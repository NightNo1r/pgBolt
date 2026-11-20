using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Jobs;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Unit;

public sealed record RunnerArgs(int N);
public sealed record TimedArgs(int N);
public sealed record LongTimedArgs(int N);

[JsonSerializable(typeof(RunnerArgs))]
[JsonSerializable(typeof(TimedArgs))]
[JsonSerializable(typeof(LongTimedArgs))]
internal sealed partial class RunnerJson : JsonSerializerContext;

/// <summary>Поведение исполнителя задаётся тестом; все вызовы и контексты записываются.</summary>
public sealed class RunnerProbe
{
    public Func<JobContext, CancellationToken, Task> Behavior { get; set; } = (_, _) => Task.CompletedTask;
    public ConcurrentQueue<string> Trace { get; } = new();
    public ConcurrentQueue<JobContext> Contexts { get; } = new();
    public ConcurrentQueue<CancellationToken> Tokens { get; } = new();
    public ConcurrentQueue<JobExecutingContext> FilterContexts { get; } = new();
    public ConcurrentQueue<object> FilterInstances { get; } = new();
    public ConcurrentQueue<bool> ResolvedFromJobScope { get; } = new();
}

public sealed class ProbeJob(RunnerProbe probe) : IJob<RunnerArgs>, IJob<TimedArgs>, IJob<LongTimedArgs>
{
    public Task ExecuteAsync(RunnerArgs args, JobContext context, CancellationToken cancellationToken) => Run(context, cancellationToken);

    public Task ExecuteAsync(TimedArgs args, JobContext context, CancellationToken cancellationToken) => Run(context, cancellationToken);

    public Task ExecuteAsync(LongTimedArgs args, JobContext context, CancellationToken cancellationToken) => Run(context, cancellationToken);

    private Task Run(JobContext context, CancellationToken ct)
    {
        probe.Contexts.Enqueue(context);
        probe.Tokens.Enqueue(ct);
        probe.Trace.Enqueue("job");
        return probe.Behavior(context, ct);
    }
}

public sealed class OuterFilter(RunnerProbe probe) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        probe.FilterContexts.Enqueue(context);
        probe.FilterInstances.Enqueue(this);
        probe.ResolvedFromJobScope.Enqueue(ReferenceEquals(context.Services.GetRequiredService<OuterFilter>(), this));
        probe.Trace.Enqueue("outer>");
        await next();
        probe.Trace.Enqueue("<outer");
    }
}

public sealed class InnerFilter(RunnerProbe probe) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        probe.Trace.Enqueue("inner>");
        await next();
        probe.Trace.Enqueue("<inner");
    }
}

public sealed class ShortCircuitFilter(RunnerProbe probe) : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        probe.Trace.Enqueue("short");
        return ValueTask.CompletedTask;
    }
}

public sealed class ThrowingFilter : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) =>
        throw new InvalidOperationException("filter failed");
}

public sealed class JobRunnerTests : IAsyncDisposable
{
    private static readonly JobDescriptor Plain = new("runner.job", "q", 3, 0, null);
    private static readonly JobDescriptor Timed = new("runner.timed", "q", 3, 0, null) { TimeoutSeconds = 5 };
    private static readonly JobDescriptor LongTimed = new("runner.long", "q", 3, 0, null) { TimeoutSeconds = int.MaxValue };   // ≈ 68 лет

    private readonly FakeTimeProvider _time = new();
    private readonly MonoClock _clock;
    private readonly RunnerProbe _probe = new();
    private readonly JobRegistry _registry = new();
    private readonly PgBoltServerOptions _options = new PgBoltServerOptions().Queue("q");
    private readonly ServiceCollection _services = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltInstrumentation _instrumentation = TestTelemetry.Instrumentation();
    private ServiceProvider? _provider;

    public JobRunnerTests()
    {
        _clock = new MonoClock(_time);
        _services.AddSingleton(_probe);
        _services.AddScoped<OuterFilter>();
        _services.AddScoped<InnerFilter>();
        _services.AddScoped<ShortCircuitFilter>();
        _services.AddScoped<ThrowingFilter>();
        var jobs = new JobsBuilder(_services, _registry);
        jobs.AddExecutor<RunnerArgs, ProbeJob>(Plain, RunnerJson.Default.RunnerArgs);
        jobs.AddExecutor<TimedArgs, ProbeJob>(Timed, RunnerJson.Default.TimedArgs);
        jobs.AddExecutor<LongTimedArgs, ProbeJob>(LongTimed, RunnerJson.Default.LongTimedArgs);
    }

    public async ValueTask DisposeAsync()
    {
        _instrumentation.Dispose();
        if (_provider is not null)
            await _provider.DisposeAsync();
    }

    private JobRunner Runner(params Type[] filters)
    {
        _provider ??= _services.BuildServiceProvider(validateScopes: true);
        var retry = new RetryDecider(RetryPolicy.Custom((attempt, _) => TimeSpan.FromSeconds(10 * attempt)), NullLogger.Instance);
        return new JobRunner(_registry, _provider.GetRequiredService<IServiceScopeFactory>(), new FilterPipeline(filters), retry,
            _options, _clock, _logs, TestTelemetry.Metrics(_instrumentation), serverName: "test");
    }

    private static Execution Exec(string type = "runner.job", string args = """{"N":1}""", int attempt = 1, int maxAttempts = 3) =>
        new(Leased.Job(type: type, args: args, attempt: attempt, maxAttempts: maxAttempts), "q", 0, ExecutionState.Running);

    private static Task WaitForCancellation(CancellationToken ct) => Task.Delay(Timeout.InfiniteTimeSpan, ct);

    [Fact]
    public async Task Recurring_fields_reach_job_context()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        var job = Leased.Job(type: "runner.job", args: """{"N":1}""") with { RecurringId = "daily", ScheduledAtUtc = at };

        await Runner().RunAsync(new Execution(job, "q", 0, ExecutionState.Running), CancellationToken.None);

        var context = Assert.Single(_probe.Contexts);
        Assert.Equal(new DateTimeOffset(at), context.ScheduledAt);
        Assert.Equal("daily", context.RecurringJobId);
    }

    [Theory]
    [InlineData((short)1, true, false)]
    [InlineData((short)2, true, false)]
    [InlineData((short)3, false, true)]
    [InlineData((short)4, false, true)]
    [InlineData((short)5, false, false)]
    public async Task Workflow_links_follow_continuation_code(short code, bool jobParent, bool batchParent)
    {
        var parent = Guid.CreateVersion7();
        var batch = Guid.CreateVersion7();
        var job = Leased.Job(type: "runner.job", args: """{"N":1}""") with { ParentId = parent, BatchId = batch, Continuation = code };

        await Runner().RunAsync(new Execution(job, "q", 0, ExecutionState.Running), CancellationToken.None);

        var context = Assert.Single(_probe.Contexts);
        Assert.Equal(jobParent ? new JobId(parent) : null, context.ParentId);
        Assert.Equal(batchParent ? new BatchId(parent) : null, context.ParentBatchId);
        Assert.Equal(new BatchId(batch), context.BatchId);
    }

    [Fact]
    public async Task Successful_job_is_acked_with_job_context()
    {
        var execution = Exec(attempt: 2, maxAttempts: 5);

        var outcome = await Runner().RunAsync(execution, CancellationToken.None);

        Assert.IsType<JobOutcome.Ack>(outcome);
        var context = Assert.Single(_probe.Contexts);
        Assert.Equal((new JobId(execution.Job.Id), "runner.job", 2, 5, (JobId?)null, (BatchId?)null),
            (context.JobId, context.JobName, context.Attempt, context.MaxAttempts, context.ParentId, context.BatchId));
        Assert.Equal(execution.Token, Assert.Single(_probe.Tokens));
    }

    [Fact]
    public async Task Already_cancelled_shutdown_releases_without_lookup_or_call()
    {
        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();

        var unknown = await Runner().RunAsync(Exec(type: "missing.job"), shutdown.Token);
        var known = await Runner().RunAsync(Exec(), shutdown.Token);

        Assert.Equal(new JobOutcome.Release(Ran: false), unknown);   // не NoExecutor: исполнитель не искался
        Assert.Equal(new JobOutcome.Release(Ran: false), known);
        Assert.Empty(_probe.Contexts);
    }

    [Fact]
    public async Task Unknown_type_goes_to_no_executor_retry()
    {
        var outcome = await Runner().RunAsync(Exec(type: "missing.job"), CancellationToken.None);

        var retry = Assert.IsType<JobOutcome.Retry>(outcome);
        Assert.Equal((TimeSpan.FromSeconds(10), JobEventKind.NoExecutor), (retry.Delay, retry.Kind));
        Assert.Contains("missing.job", retry.Message);
    }

    [Fact]
    public async Task Shutdown_between_check_and_call_releases_as_ran()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = (_, ct) =>
        {
            shutdown.Cancel();                    // отмена пришла уже после проверки сигнала остановки в начале RunAsync
            ct.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        };

        var outcome = await Runner().RunAsync(Exec(), shutdown.Token);

        Assert.Equal(new JobOutcome.Release(Ran: true), outcome);
    }

    [Fact]
    public async Task Job_stopping_on_shutdown_token_is_released_as_ran()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec();

        var run = Runner().RunAsync(execution, shutdown.Token);
        await shutdown.CancelAsync();

        Assert.Equal(new JobOutcome.Release(Ran: true), await run);
        Assert.Equal(CancelReason.Shutdown, execution.FirstReason);
    }

    [Fact]
    public async Task Non_cancellation_exception_after_shutdown_is_retried()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = async (_, ct) =>
        {
            try { await WaitForCancellation(ct); }
            catch (OperationCanceledException) { throw new IOException("cleanup failed"); }
        };

        var run = Runner().RunAsync(Exec(), shutdown.Token);
        await shutdown.CancelAsync();

        Assert.Equal("cleanup failed", Assert.IsType<JobOutcome.Retry>(await run).Message);
    }

    [Fact]
    public async Task Heartbeat_lost_then_shutdown_spends_attempt()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec();

        var run = Runner().RunAsync(execution, shutdown.Token);
        execution.Cancel(CancelReason.HeartbeatLost);
        await shutdown.CancelAsync();

        var retry = Assert.IsType<JobOutcome.Retry>(await run);
        Assert.Equal((JobEventKind.Retry, "cancelled: lease renewal not confirmed"), (retry.Kind, retry.Message));
    }

    [Fact]
    public async Task Heartbeat_lost_on_last_attempt_fails()
    {
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec(attempt: 3, maxAttempts: 3);

        var run = Runner().RunAsync(execution, CancellationToken.None);
        execution.Cancel(CancelReason.HeartbeatLost);

        Assert.Equal(JobEventKind.Failed, Assert.IsType<JobOutcome.Fail>(await run).Kind);
    }

    [Fact]
    public async Task Cancel_requested_then_any_exception_confirms_cancel()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = async (_, ct) =>
        {
            try { await WaitForCancellation(ct); }
            catch (OperationCanceledException) { throw new InvalidOperationException("rolled back"); }
        };
        var execution = Exec();

        var run = Runner().RunAsync(execution, shutdown.Token);
        execution.Cancel(CancelReason.CancelRequested);
        await shutdown.CancelAsync();

        Assert.Equal(new JobOutcome.ConfirmCancel("rolled back"), await run);
    }

    [Fact]
    public async Task Descriptor_timeout_cancels_job_and_retries_with_timeout_message()
    {
        _options.DefaultJobTimeout = TimeSpan.FromSeconds(1);   // таймаут задачи важнее
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec(type: "runner.timed");

        var run = Runner().RunAsync(execution, CancellationToken.None);
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(execution.Token.IsCancellationRequested);
        _time.Advance(TimeSpan.FromSeconds(4));

        var retry = Assert.IsType<JobOutcome.Retry>(await run);
        Assert.Equal("timeout after 5 s", retry.Message);
        Assert.Equal(CancelReason.Timeout, execution.FirstReason);
    }

    [Fact]
    public async Task Default_timeout_applies_to_job_without_own_timeout()
    {
        _options.DefaultJobTimeout = TimeSpan.FromMilliseconds(2500);
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);

        var run = Runner().RunAsync(Exec(), CancellationToken.None);
        _time.Advance(TimeSpan.FromMilliseconds(2500));

        Assert.Equal("timeout after 2.5 s", Assert.IsType<JobOutcome.Retry>(await run).Message);
    }

    [Fact]
    public async Task Descriptor_timeout_beyond_timer_limit_is_capped_at_max_job_timeout()
    {
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec(type: "runner.long");

        // int.MaxValue с таймер .NET не принимает: без урезания конструктор токена бросил бы из RunAsync.
        var run = Runner().RunAsync(execution, CancellationToken.None);
        _time.Advance(PgBoltServerOptions.MaxJobTimeout - TimeSpan.FromMilliseconds(1));
        Assert.False(execution.Token.IsCancellationRequested);
        _time.Advance(TimeSpan.FromMilliseconds(1));

        var retry = Assert.IsType<JobOutcome.Retry>(await run);
        Assert.Equal("timeout after 4294967.294 s", retry.Message);   // 2³² − 2 мс
        Assert.Equal(CancelReason.Timeout, execution.FirstReason);
    }

    [Fact]
    public async Task Capped_descriptor_timeout_is_logged_once_per_job_type()
    {
        var runner = Runner();

        await runner.RunAsync(Exec(type: "runner.long"), CancellationToken.None);
        await runner.RunAsync(Exec(type: "runner.long"), CancellationToken.None);
        await runner.RunAsync(Exec(type: "runner.timed"), CancellationToken.None);   // таймаут в пределе — без лога

        var warning = Assert.Single(_logs.At(LogLevel.Warning));
        Assert.Equal("PgBolt.Server", warning.Category);
        Assert.Contains("runner.long", warning.Message);
        Assert.Contains("2147483647", warning.Message);
    }

    [Fact]
    public async Task Job_without_timeout_is_never_cancelled()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _probe.Behavior = (_, _) => gate.Task;
        var execution = Exec();

        var run = Runner().RunAsync(execution, CancellationToken.None);
        _time.Advance(TimeSpan.FromDays(1));

        Assert.False(execution.Token.IsCancellationRequested);
        Assert.False(run.IsCompleted);
        gate.SetResult();
        Assert.IsType<JobOutcome.Ack>(await run);
    }

    [Fact]
    public async Task Synchronous_throw_before_first_await_is_a_regular_failure()
    {
        _probe.Behavior = (_, _) => throw new InvalidOperationException("sync boom");

        var outcome = await Runner().RunAsync(Exec(), CancellationToken.None);

        Assert.Equal("sync boom", Assert.IsType<JobOutcome.Retry>(outcome).Message);
    }

    [Fact]
    public async Task Null_task_from_executor_is_a_regular_failure()
    {
        _probe.Behavior = (_, _) => null!;

        var outcome = await Runner().RunAsync(Exec(), CancellationToken.None);

        var retry = Assert.IsType<JobOutcome.Retry>(outcome);
        Assert.Contains("returned a null Task", retry.Message);
        Assert.Contains(nameof(InvalidOperationException), retry.Exception);
    }

    [Theory]
    [InlineData("{")]
    [InlineData("null")]
    public async Task Broken_arguments_fail_without_retry(string args)
    {
        var outcome = await Runner().RunAsync(Exec(args: args), CancellationToken.None);

        var fail = Assert.IsType<JobOutcome.Fail>(outcome);
        Assert.Equal(JobEventKind.Failed, fail.Kind);
        Assert.Contains(nameof(JobArgumentsException), fail.Exception);
        Assert.Empty(_probe.Contexts);
    }

    [Fact]
    public async Task Job_aborted_exception_fails_without_retry()
    {
        _probe.Behavior = (_, _) => Task.FromException(new JobAbortedException("no point"));

        var fail = Assert.IsType<JobOutcome.Fail>(await Runner().RunAsync(Exec(), CancellationToken.None));

        Assert.Equal((JobEventKind.Failed, "no point"), (fail.Kind, fail.Message));
    }

    [Fact]
    public async Task Filters_run_in_list_order_first_is_outermost()
    {
        var outcome = await Runner(typeof(OuterFilter), typeof(InnerFilter)).RunAsync(Exec(), CancellationToken.None);

        Assert.IsType<JobOutcome.Ack>(outcome);
        Assert.Equal(["outer>", "inner>", "job", "<inner", "<outer"], _probe.Trace);
    }

    [Fact]
    public async Task Filter_not_calling_next_short_circuits_to_success()
    {
        var outcome = await Runner(typeof(ShortCircuitFilter), typeof(InnerFilter)).RunAsync(Exec(), CancellationToken.None);

        Assert.IsType<JobOutcome.Ack>(outcome);
        Assert.Equal(["short"], _probe.Trace);
    }

    [Fact]
    public async Task Filter_exception_is_job_exception()
    {
        var outcome = await Runner(typeof(ThrowingFilter)).RunAsync(Exec(), CancellationToken.None);

        Assert.Equal("filter failed", Assert.IsType<JobOutcome.Retry>(outcome).Message);
        Assert.Empty(_probe.Contexts);
    }

    [Fact]
    public async Task Filters_are_scoped_per_job()
    {
        var runner = Runner(typeof(OuterFilter), typeof(OuterFilter));

        await runner.RunAsync(Exec(), CancellationToken.None);
        await runner.RunAsync(Exec(), CancellationToken.None);

        var instances = _probe.FilterInstances.ToArray();
        Assert.Equal(4, instances.Length);
        Assert.Same(instances[0], instances[1]);
        Assert.Same(instances[2], instances[3]);
        Assert.NotSame(instances[0], instances[2]);
    }

    [Fact]
    public async Task Filter_context_carries_args_utf8_scope_and_token()
    {
        var execution = Exec(args: """{"N":42}""");

        await Runner(typeof(OuterFilter)).RunAsync(execution, CancellationToken.None);

        var context = Assert.Single(_probe.FilterContexts);
        Assert.Equal(Encoding.UTF8.GetBytes("""{"N":42}"""), context.ArgsUtf8.ToArray());
        Assert.Equal(execution.Token, context.CancellationToken);
        Assert.Same(Assert.Single(_probe.Contexts), context.Job);
        Assert.True(Assert.Single(_probe.ResolvedFromJobScope));   // context.Services — scope задачи
    }
}
