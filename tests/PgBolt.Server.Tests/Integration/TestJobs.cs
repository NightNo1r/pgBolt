using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization;
using Npgsql;

namespace PgBolt.Server.Tests.Integration;

public sealed record SuccessArgs(int N);
public sealed record FailArgs(string Message);
/// <summary>Бросает, пока context.Attempt ≤ FailUntilAttempt, затем успешна.</summary>
public sealed record FlakyArgs(int FailUntilAttempt);
public sealed record AbortArgs(string Reason);
/// <summary>Ждёт JobProbe.Release или отмены токена (корректно останавливается).</summary>
public sealed record BlockArgs(int N);
/// <summary>Ждёт JobProbe.Release, токен игнорирует (отмену только фиксирует в JobRun).</summary>
public sealed record StubbornArgs(int N);
public sealed record SlowArgs(int Milliseconds);
/// <summary>Тип с [Job(TimeoutSeconds = 1)]: ждёт отмены токена.</summary>
public sealed record TimeLimitedArgs(int N);
/// <summary>Держит Connections соединений пула NpgsqlDataSource хоста, пока токен не отменён.</summary>
public sealed record PoolHogArgs(int Connections);
/// <summary>Публикуется только хостом с TestJobs.RegisterWithOrphan — у остальных для него нет исполнителя.</summary>
public sealed record OrphanArgs(int N);

[JsonSerializable(typeof(SuccessArgs))]
[JsonSerializable(typeof(FailArgs))]
[JsonSerializable(typeof(FlakyArgs))]
[JsonSerializable(typeof(AbortArgs))]
[JsonSerializable(typeof(BlockArgs))]
[JsonSerializable(typeof(StubbornArgs))]
[JsonSerializable(typeof(SlowArgs))]
[JsonSerializable(typeof(TimeLimitedArgs))]
[JsonSerializable(typeof(PoolHogArgs))]
[JsonSerializable(typeof(OrphanArgs))]
internal sealed partial class TestJobsJson : JsonSerializerContext;

/// <summary>
/// Дескрипторы задач. Это единственные экземпляры на процесс: JobMetadata&lt;TArgs&gt; — статический и глобальный,
/// повторная регистрация того же args-типа с другим дескриптором бросает. Вариации (очередь, MaxAttempts, задержки
/// повтора) задаются через EnqueueOptions.
/// </summary>
public static class TestJobs
{
    public const string Queue = "q";

    public static readonly JobDescriptor Success = new("test.success", Queue, 3, 0, null);
    public static readonly JobDescriptor Fail = new("test.fail", Queue, 3, 0, null);
    public static readonly JobDescriptor Flaky = new("test.flaky", Queue, 3, 0, [0]);
    public static readonly JobDescriptor Abort = new("test.abort", Queue, 3, 0, null);
    public static readonly JobDescriptor Block = new("test.block", Queue, 3, 0, [0]);
    public static readonly JobDescriptor Stubborn = new("test.stubborn", Queue, 3, 0, [0]);
    public static readonly JobDescriptor Slow = new("test.slow", Queue, 3, 0, null);
    public static readonly JobDescriptor TimeLimited = new("test.timeout", Queue, 2, 0, [0]) { TimeoutSeconds = 1 };
    public static readonly JobDescriptor PoolHog = new("test.poolhog", Queue, 2, 0, [0]);
    public static readonly JobDescriptor Orphan = new("test.orphan", Queue, 2, 0, [0]);

    /// <summary>Все исполнители, кроме test.orphan.</summary>
    public static void Register(IPgBoltJobsBuilder b)
    {
        b.AddExecutor<SuccessArgs, SuccessJob>(Success, TestJobsJson.Default.SuccessArgs);
        b.AddExecutor<FailArgs, FailJob>(Fail, TestJobsJson.Default.FailArgs);
        b.AddExecutor<FlakyArgs, FlakyJob>(Flaky, TestJobsJson.Default.FlakyArgs);
        b.AddExecutor<AbortArgs, AbortJob>(Abort, TestJobsJson.Default.AbortArgs);
        b.AddExecutor<BlockArgs, BlockJob>(Block, TestJobsJson.Default.BlockArgs);
        b.AddExecutor<StubbornArgs, StubbornJob>(Stubborn, TestJobsJson.Default.StubbornArgs);
        b.AddExecutor<SlowArgs, SlowJob>(Slow, TestJobsJson.Default.SlowArgs);
        b.AddExecutor<TimeLimitedArgs, TimeLimitedJob>(TimeLimited, TestJobsJson.Default.TimeLimitedArgs);
        b.AddExecutor<PoolHogArgs, PoolHogJob>(PoolHog, TestJobsJson.Default.PoolHogArgs);
    }

    public static void RegisterWithOrphan(IPgBoltJobsBuilder b)
    {
        Register(b);
        b.AddExecutor<OrphanArgs, OrphanJob>(Orphan, TestJobsJson.Default.OrphanArgs);
    }
}

/// <summary>Имя ServerTestHost, на котором выполняется задача (синглтон в DI каждого хоста).</summary>
public sealed record HostTag(string Name);

public enum RunEnd { Running, Completed, Threw }

/// <summary>Одно выполнение (попытка) задачи. Метки времени — Stopwatch.GetTimestamp(), 0 — не было.</summary>
public sealed class JobRun
{
    private long _cancelledAt;
    private long _finishedAt;
    private int _end;

    internal JobRun(Guid jobId, int attempt, string host)
    {
        JobId = jobId;
        Attempt = attempt;
        Host = host;
        StartedAt = Stopwatch.GetTimestamp();
    }

    public Guid JobId { get; }
    public int Attempt { get; }
    public string Host { get; }
    public long StartedAt { get; }
    /// <summary>Момент отмены токена выполнения (любой причиной); 0 — токен не отменялся.</summary>
    public long CancelledAt => Volatile.Read(ref _cancelledAt);
    public long FinishedAt => Volatile.Read(ref _finishedAt);
    public RunEnd End => (RunEnd)Volatile.Read(ref _end);
    public bool WasCancelled => CancelledAt != 0;
    public TimeSpan Duration => Stopwatch.GetElapsedTime(StartedAt, FinishedAt);
    public DateTimeOffset? ScheduledAt { get; init; }
    public string? RecurringJobId { get; init; }
    public JobId? ParentId { get; init; }
    public BatchId? ParentBatchId { get; init; }
    public BatchId? BatchId { get; init; }

    internal void MarkCancelled() => Interlocked.CompareExchange(ref _cancelledAt, Stopwatch.GetTimestamp(), 0);

    internal void Finish(RunEnd end)
    {
        Volatile.Write(ref _finishedAt, Stopwatch.GetTimestamp());
        Volatile.Write(ref _end, (int)end);
    }
}

/// <summary>
/// Управляющий синглтон теста: общий для всех хостов теста (передаётся в ServerTestHost.StartAsync), считает выполнения
/// по job_id и держит «ворота» Block/Stubborn-задач по (job_id, attempt).
/// </summary>
public sealed class JobProbe
{
    private readonly ConcurrentDictionary<Guid, List<JobRun>> _runs = new();
    private readonly Lock _gatesLock = new();
    private readonly Dictionary<(Guid JobId, int Attempt), TaskCompletionSource> _gates = [];
    private readonly HashSet<Guid> _releasedJobs = [];
    private bool _releasedAll;
    private int _started;
    private int _completed;

    public int TotalStarted => Volatile.Read(ref _started);
    public int TotalCompleted => Volatile.Read(ref _completed);

    public IReadOnlyList<JobRun> Runs(JobId id)
    {
        if (!_runs.TryGetValue(id.Value, out var list))
            return [];
        lock (list)
            return list.ToArray();
    }

    public JobRun? Run(JobId id, int attempt) => Runs(id).FirstOrDefault(r => r.Attempt == attempt);

    public int RunCount(JobId id) => Runs(id).Count;

    public IReadOnlyDictionary<Guid, int> RunCounts() => _runs.ToDictionary(p => p.Key, p => { lock (p.Value) return p.Value.Count; });

    /// <summary>Открывает ворота задачи: attempt = null — все попытки, включая будущие.</summary>
    public void Release(JobId id, int? attempt = null)
    {
        List<TaskCompletionSource> open;
        lock (_gatesLock)
        {
            if (attempt is null)
                _releasedJobs.Add(id.Value);
            else if (!_gates.ContainsKey((id.Value, attempt.Value)))
                _gates[(id.Value, attempt.Value)] = NewGate();
            open = _gates.Where(p => p.Key.JobId == id.Value && (attempt is null || p.Key.Attempt == attempt))
                .Select(p => p.Value).ToList();
        }
        foreach (var gate in open)
            gate.TrySetResult();
    }

    /// <summary>Открывает все ворота (в том числе будущие) — вызывается при уборке теста.</summary>
    public void ReleaseAll()
    {
        List<TaskCompletionSource> open;
        lock (_gatesLock)
        {
            _releasedAll = true;
            open = _gates.Values.ToList();
        }
        foreach (var gate in open)
            gate.TrySetResult();
    }

    public Task GateAsync(Guid jobId, int attempt)
    {
        lock (_gatesLock)
        {
            if (!_gates.TryGetValue((jobId, attempt), out var gate))
            {
                gate = NewGate();
                if (_releasedAll || _releasedJobs.Contains(jobId))
                    gate.SetResult();
                _gates[(jobId, attempt)] = gate;
            }
            return gate.Task;
        }
    }

    public async Task RunAsync(JobContext context, HostTag host, CancellationToken ct, Func<JobRun, Task> body)
    {
        var run = new JobRun(context.JobId.Value, context.Attempt, host.Name)
        {
            ScheduledAt = context.ScheduledAt,
            RecurringJobId = context.RecurringJobId,
            ParentId = context.ParentId,
            ParentBatchId = context.ParentBatchId,
            BatchId = context.BatchId,
        };
        var list = _runs.GetOrAdd(run.JobId, static _ => new List<JobRun>(1));
        lock (list)
            list.Add(run);
        Interlocked.Increment(ref _started);
        using var registration = ct.Register(static state => ((JobRun)state!).MarkCancelled(), run);
        try
        {
            await body(run);
        }
        catch
        {
            // Колбэки токена идут в обратном порядке регистрации: WaitAsync(ct) тела завершает его синхронно внутри
            // Cancel(), и Dispose регистрации выше снимает MarkCancelled до вызова — отмену фиксируем здесь.
            if (ct.IsCancellationRequested)
                run.MarkCancelled();
            run.Finish(RunEnd.Threw);
            throw;
        }
        if (ct.IsCancellationRequested)
            run.MarkCancelled();
        run.Finish(RunEnd.Completed);
        Interlocked.Increment(ref _completed);
    }

    private static TaskCompletionSource NewGate() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class SuccessJob(JobProbe probe, HostTag host) : IJob<SuccessArgs>
{
    public Task ExecuteAsync(SuccessArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, static _ => Task.CompletedTask);
}

public sealed class FailJob(JobProbe probe, HostTag host) : IJob<FailArgs>
{
    public Task ExecuteAsync(FailArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => throw new InvalidOperationException(args.Message));
}

public sealed class FlakyJob(JobProbe probe, HostTag host) : IJob<FlakyArgs>
{
    public Task ExecuteAsync(FlakyArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => context.Attempt <= args.FailUntilAttempt
            ? throw new InvalidOperationException($"flaky attempt {context.Attempt}")
            : Task.CompletedTask);
}

public sealed class AbortJob(JobProbe probe, HostTag host) : IJob<AbortArgs>
{
    public Task ExecuteAsync(AbortArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => throw new JobAbortedException(args.Reason));
}

public sealed class BlockJob(JobProbe probe, HostTag host) : IJob<BlockArgs>
{
    public Task ExecuteAsync(BlockArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken,
            run => probe.GateAsync(run.JobId, run.Attempt).WaitAsync(cancellationToken));
}

public sealed class StubbornJob(JobProbe probe, HostTag host) : IJob<StubbornArgs>
{
    public Task ExecuteAsync(StubbornArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, run => probe.GateAsync(run.JobId, run.Attempt));
}

public sealed class SlowJob(JobProbe probe, HostTag host) : IJob<SlowArgs>
{
    public Task ExecuteAsync(SlowArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => Task.Delay(args.Milliseconds, cancellationToken));
}

public sealed class TimeLimitedJob(JobProbe probe, HostTag host) : IJob<TimeLimitedArgs>
{
    public Task ExecuteAsync(TimeLimitedArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken));
}

/// <summary>Берёт соединения из того же NpgsqlDataSource, что и runtime (TestHostSettings.DataSource).</summary>
public sealed class PoolHogJob(JobProbe probe, HostTag host, NpgsqlDataSource dataSource) : IJob<PoolHogArgs>
{
    public Task ExecuteAsync(PoolHogArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, async _ =>
        {
            var held = new List<NpgsqlConnection>();
            try
            {
                for (var i = 0; i < args.Connections; i++)
                    held.Add(await dataSource.OpenConnectionAsync(cancellationToken));
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            finally
            {
                foreach (var connection in held)
                    await connection.DisposeAsync();
            }
        });
}

public sealed class OrphanJob(JobProbe probe, HostTag host) : IJob<OrphanArgs>
{
    public Task ExecuteAsync(OrphanArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, static _ => Task.CompletedTask);
}
