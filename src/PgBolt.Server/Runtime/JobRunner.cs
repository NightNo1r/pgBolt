using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Выполнение одной задачи и классификация исхода с consumer-span'ом, scope и событиями жизненного цикла.
/// Сбой телеметрии исход не меняет; исключение вне её защиты выходит наружу, закрыв span и scope.
/// </summary>
internal sealed partial class JobRunner
{
    private readonly JobRegistry _registry;
    private readonly IServiceScopeFactory _scopes;
    private readonly FilterPipeline _filters;
    private readonly RetryDecider _retry;
    private readonly PgBoltServerOptions _options;
    private readonly MonoClock _clock;
    private readonly ILogger _jobLogger;
    private readonly ILogger _logger;
    private readonly PgBoltMetrics _metrics;
    private readonly PgBoltInstrumentation _instrumentation;
    private readonly string _serverName;
    private readonly ConcurrentDictionary<string, byte> _cappedTimeoutLogged = new(StringComparer.Ordinal);

    public JobRunner(JobRegistry registry, IServiceScopeFactory scopes, FilterPipeline filters, RetryDecider retry,
        PgBoltServerOptions options, MonoClock clock, ILoggerFactory loggers, PgBoltMetrics metrics, string serverName)
    {
        _registry = registry;
        _scopes = scopes;
        _filters = filters;
        _retry = retry;
        _options = options;
        _clock = clock;
        _jobLogger = loggers.CreateLogger("PgBolt.Job");
        _logger = loggers.CreateLogger("PgBolt.Server");
        _metrics = metrics;
        _instrumentation = metrics.Instrumentation;
        _serverName = serverName;
    }

    public async Task<JobOutcome> RunAsync(Execution execution, CancellationToken shutdown)
    {
        var job = execution.Job;
        // Сигнал остановки пришёл после проверки диспетчера — задача не запускалась; span'а и scope нет.
        if (shutdown.IsCancellationRequested)
        {
            var released = new JobOutcome.Release(Ran: false);
            LogOutcome(execution, CancelReason.None, released, null, TimeSpan.Zero);
            return released;
        }

        var startedAt = _clock.Now;
        var span = StartSpan(execution);
        var scope = BeginScope(execution);
        // Span и scope закрываются и при исключении вне защиты телеметрии (предупреждение TimeoutOf, лог RetryDecider):
        // открытый span и scope не должны пережить попытку, само исключение выходит из RunAsync.
        try
        {
            _metrics.JobWaitTime(execution.Queue, job.Type, job.WaitSeconds);
            try { Log.JobStarted(_jobLogger, job.Id, job.Type, job.Attempt, execution.Queue); }
            catch (Exception e) { _instrumentation.ReportFailure(e); }

            JobOutcome outcome;
            Exception? error = null;
            CancelReason reason;
            TimeSpan duration;
            if (!_registry.TryGet(job.Type, out var registration))
            {
                // То же исключение получают span, лог и RetryPolicy.
                var notFound = new JobExecutorNotFoundException(job.Type);
                error = notFound;
                reason = execution.FirstReason;
                duration = _clock.Elapsed(startedAt);
                outcome = OutcomeClassifier.NoExecutor(execution, notFound, _retry);
            }
            else
            {
                // Уже отменённый shutdown вызывает callback сразу: задача получит отменённый токен.
                using var onShutdown = shutdown.Register(static s => ((Execution)s!).Cancel(CancelReason.Shutdown), execution);

                var timeout = TimeoutOf(registration.Descriptor);
                using var timeoutSource = timeout is { } t ? new CancellationTokenSource(t, _clock.Time) : null;
                using var onTimeout = timeoutSource is null
                    ? default
                    : timeoutSource.Token.Register(static s => ((Execution)s!).Cancel(CancelReason.Timeout), execution);

                try { await ExecuteAsync(execution, registration); }
                catch (Exception e) { error = e; }
                // Причина и длительность снимаются до классификации: RetryPolicy — пользовательский код, его время
                // не входит в длительность попытки, а отмена во время него не меняет причину, по которой классифицирован исход.
                reason = execution.FirstReason;
                duration = _clock.Elapsed(startedAt);
                outcome = OutcomeClassifier.Classify(execution, reason, error, _retry, timeout);
            }

            // Исход фиксируется, пока span текущий и scope открыт.
            if (span is not null)
                TagOutcome(span, reason, outcome, error);
            _metrics.JobDuration(execution.Queue, job.Type, PgBoltMetrics.OutcomeName(outcome), duration);
            LogOutcome(execution, reason, outcome, error, duration);
            return outcome;
        }
        finally
        {
            if (scope is not null)
            {
                try { scope.Dispose(); }
                catch (Exception e) { _instrumentation.ReportFailure(e); }
            }
            if (span is not null)
                GuardedActivity.Stop(span, null, _instrumentation);
        }
    }

    /// <summary>
    /// Consumer-span попытки — удалённый ребёнок <c>trace_parent</c> задачи; без него или при нераспознанном значении — корень.
    /// Без слушателя источника span не создаётся, и теги не вычисляются.
    /// </summary>
    private Activity? StartSpan(Execution execution)
    {
        var job = execution.Job;
        try
        {
            // Циклы диспетчера и выполнения наследуют ExecutionContext из StartAsync хоста: без сброса задача без trace_parent
            // стала бы ребёнком чужой долгоживущей Activity. Изменение не выходит за пределы асинхронного потока RunAsync.
            Activity.Current = null;
            // Сброс Activity.Current выше нужен и без слушателя; разбор trace_parent — только когда span будет создан.
            if (!Source.HasListeners())
                return null;
            ActivityContext.TryParse(job.TraceParent, null, isRemote: true, out var parent);
            var span = Source.StartActivity(job.Type, ActivityKind.Consumer, parent);
            if (span is null)
                return null;
            span.SetTag(Attr.MessagingSystem, PgBoltTelemetry.System);
            span.SetTag(Attr.MessagingOperationType, "process");
            span.SetTag(Attr.MessagingDestinationName, execution.Queue);
            span.SetTag(Attr.MessagingMessageId, job.Id.ToString());
            span.SetTag(Attr.JobType, job.Type);
            span.SetTag(Attr.JobAttempt, job.Attempt);
            span.SetTag(Attr.JobMaxAttempts, job.MaxAttempts);
            span.SetTag(Attr.ServerName, _serverName);
            if (job.BatchId is { } batch)
                span.SetTag(Attr.BatchId, batch.ToString());
            if (WorkflowCodes.ParentJob(job.Continuation, job.ParentId) is { } parentJob)
                span.SetTag(Attr.JobParentJobId, parentJob.ToString());
            if (WorkflowCodes.ParentBatch(job.Continuation, job.ParentId) is { } parentBatch)
                span.SetTag(Attr.JobParentBatchId, parentBatch.ToString());
            if (job.RecurringId is not null)
                span.SetTag(Attr.RecurringId, job.RecurringId);
            return span;
        }
        catch (Exception e)
        {
            // Бросивший ActivityStarted уже сделал недостартовавший span текущим: задача идёт без span'а.
            Activity.Current = null;
            _instrumentation.ReportFailure(e);
            return null;
        }
    }

    private IDisposable? BeginScope(Execution execution)
    {
        try { return _jobLogger.BeginScope(new JobLogScope(execution.Job, execution.Queue)); }
        catch (Exception e)
        {
            _instrumentation.ReportFailure(e);
            return null;
        }
    }

    /// <summary>Исход, а для Retry и Fail — статус Error, error.type (причина отмены или тип исключения) и событие исключения.</summary>
    private void TagOutcome(Activity span, CancelReason reason, JobOutcome outcome, Exception? error)
    {
        try
        {
            span.SetTag(Attr.JobOutcome, PgBoltMetrics.OutcomeName(outcome));
            if (outcome is not (JobOutcome.Retry or JobOutcome.Fail) || error is null)
                return;
            span.SetStatus(ActivityStatusCode.Error);
            span.SetTag(Attr.ErrorType, reason switch
            {
                CancelReason.Timeout => "timeout",
                CancelReason.HeartbeatLost => "heartbeat_lost",
                CancelReason.LeaseLost => "lease_lost",
                _ => error.GetType().FullName,
            });
            span.AddException(error);
        }
        catch (Exception e) { _instrumentation.ReportFailure(e); }
    }

    /// <summary>Событие жизненного цикла по исходу классификатора; причина отмены — поле Reason.</summary>
    private void LogOutcome(Execution execution, CancelReason reason, JobOutcome outcome, Exception? error, TimeSpan duration)
    {
        var job = execution.Job;
        try
        {
            switch (outcome)
            {
                case JobOutcome.Ack:
                    Log.JobSucceeded(_jobLogger, job.Id, (long)duration.TotalMilliseconds);
                    break;
                case JobOutcome.Retry { Kind: JobEventKind.NoExecutor } retry:
                    Log.JobNoExecutorWillRetry(_jobLogger, error, job.Id, job.Type, retry.Delay);
                    break;
                case JobOutcome.Retry retry:
                    Log.JobFailedWillRetry(_jobLogger, error, job.Id, job.Attempt, job.MaxAttempts, reason, retry.Delay);
                    break;
                case JobOutcome.Fail { Kind: JobEventKind.NoExecutor }:
                    Log.JobNoExecutorFailed(_jobLogger, error, job.Id, job.Type);
                    break;
                case JobOutcome.Fail:
                    Log.JobFailed(_jobLogger, error, job.Id, job.Attempt, job.MaxAttempts, reason);
                    break;
                case JobOutcome.ConfirmCancel:
                    Log.JobCancelled(_jobLogger, error, job.Id);
                    break;
                case JobOutcome.Release release:
                    Log.JobReleased(_jobLogger, job.Id, release.Ran);
                    break;
            }
        }
        catch (Exception e) { _instrumentation.ReportFailure(e); }
    }

    /// <summary>
    /// TimeoutSeconds задачи > 0, иначе DefaultJobTimeout (он уже ≤ MaxJobTimeout). TimeoutSeconds больше
    /// предела таймеров .NET урезается до него: иначе CancellationTokenSource бросил бы ArgumentOutOfRangeException.
    /// Урезание пишется в лог один раз на тип задачи (для [Job] его уже отметил BOLT008 при компиляции).
    /// </summary>
    private TimeSpan? TimeoutOf(JobDescriptor descriptor)
    {
        if (descriptor.TimeoutSeconds <= 0)
            return _options.DefaultJobTimeout;
        var own = TimeSpan.FromSeconds(descriptor.TimeoutSeconds);
        if (own <= PgBoltServerOptions.MaxJobTimeout)
            return own;
        if (_cappedTimeoutLogged.TryAdd(descriptor.Name, 0))
            Log.JobTimeoutCapped(_logger, descriptor.Name, descriptor.TimeoutSeconds, PgBoltServerOptions.MaxJobTimeout);
        return PgBoltServerOptions.MaxJobTimeout;
    }

    private async Task ExecuteAsync(Execution execution, JobExecutorRegistration registration)
    {
        var job = execution.Job;
        await using var scope = _scopes.CreateAsyncScope();
        var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts,
            WorkflowCodes.ParentJob(job.Continuation, job.ParentId), job.BatchId is { } batch ? new BatchId(batch) : null, _jobLogger,
            job.ScheduledAtUtc is { } scheduled ? new DateTimeOffset(scheduled, TimeSpan.Zero) : null, job.RecurringId,
            WorkflowCodes.ParentBatch(job.Continuation, job.ParentId));
        var executing = new JobExecutingContext(context, scope.ServiceProvider, Encoding.UTF8.GetBytes(job.ArgsJson), execution.Token);
        await _filters.InvokeAsync(executing, () => InvokeExecutor(registration, executing));
    }

    /// <summary>Синхронное исключение исполнителя и null вместо Task превращаются в обычный сбой.</summary>
    private static ValueTask InvokeExecutor(JobExecutorRegistration registration, JobExecutingContext context)
    {
        Task? task;
        try
        {
            task = registration.Invoke(context.Services, context.ArgsUtf8, context.Job, context.CancellationToken);
        }
        catch (Exception e)
        {
            return ValueTask.FromException(e);
        }
        return task is null
            ? ValueTask.FromException(new InvalidOperationException(
                $"Executor {registration.JobType.FullName} of job '{registration.Descriptor.Name}' returned a null Task."))
            : new ValueTask(task);
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 3001, EventName = nameof(JobStarted), Level = LogLevel.Debug,
            Message = "PgBolt: job {JobId} ({JobType}) attempt {Attempt} started on queue {Queue}.")]
        internal static partial void JobStarted(ILogger logger, Guid jobId, string jobType, int attempt, string queue);

        [LoggerMessage(EventId = 3002, EventName = nameof(JobSucceeded), Level = LogLevel.Debug,
            Message = "PgBolt: job {JobId} succeeded in {DurationMs} ms.")]
        internal static partial void JobSucceeded(ILogger logger, Guid jobId, long durationMs);

        [LoggerMessage(EventId = 3003, EventName = nameof(JobFailedWillRetry), Level = LogLevel.Warning,
            Message = "PgBolt: job {JobId} attempt {Attempt} of {MaxAttempts} failed ({Reason}); retrying in {NextAttemptIn}.")]
        internal static partial void JobFailedWillRetry(ILogger logger, Exception? e, Guid jobId, int attempt, int maxAttempts,
            CancelReason reason, TimeSpan nextAttemptIn);

        [LoggerMessage(EventId = 3004, EventName = nameof(JobFailed), Level = LogLevel.Error,
            Message = "PgBolt: job {JobId} failed at attempt {Attempt} of {MaxAttempts} ({Reason}); no attempts left.")]
        internal static partial void JobFailed(ILogger logger, Exception? e, Guid jobId, int attempt, int maxAttempts, CancelReason reason);

        [LoggerMessage(EventId = 3005, EventName = nameof(JobNoExecutorWillRetry), Level = LogLevel.Warning,
            Message = "PgBolt: no executor is registered for job {JobId} of type {JobType}; retrying in {NextAttemptIn}.")]
        internal static partial void JobNoExecutorWillRetry(ILogger logger, Exception? e, Guid jobId, string jobType, TimeSpan nextAttemptIn);

        [LoggerMessage(EventId = 3006, EventName = nameof(JobNoExecutorFailed), Level = LogLevel.Error,
            Message = "PgBolt: no executor is registered for job {JobId} of type {JobType}; the job is failed.")]
        internal static partial void JobNoExecutorFailed(ILogger logger, Exception? e, Guid jobId, string jobType);

        [LoggerMessage(EventId = 3007, EventName = nameof(JobCancelled), Level = LogLevel.Information,
            Message = "PgBolt: job {JobId} was cancelled.")]
        internal static partial void JobCancelled(ILogger logger, Exception? e, Guid jobId);

        [LoggerMessage(EventId = 3008, EventName = nameof(JobReleased), Level = LogLevel.Debug,
            Message = "PgBolt: job {JobId} was released without spending an attempt (ran: {Ran}).")]
        internal static partial void JobReleased(ILogger logger, Guid jobId, bool ran);

        [LoggerMessage(EventId = 3101, EventName = nameof(JobTimeoutCapped), Level = LogLevel.Warning,
            Message = "PgBolt: job '{JobName}' has TimeoutSeconds = {TimeoutSeconds}, above the .NET timer limit; its timeout is capped at {MaxJobTimeout}.")]
        internal static partial void JobTimeoutCapped(ILogger logger, string jobName, int timeoutSeconds, TimeSpan maxJobTimeout);
    }
}
