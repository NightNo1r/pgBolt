using System.Collections.Concurrent;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Shared state for a run in one mode. Job handlers resolved from host DI and the producer write it. Window collection, sampling, and
/// correctness checks read it.
/// </summary>
/// <param name="capacity">Upper estimate of jobs in the run, used to size the execution counter's outer array.</param>
internal sealed class RunContext(long capacity, QueueProfile queues = QueueProfile.Standard)
{
    private long _executed;

    public FetchRecorder? Fetches { get; init; }
    public QueueProfile Queues { get; } = queues;
    public ExecutionCounter Counter { get; } = new(capacity);

    /// <summary>Total executions, including failed first attempts.</summary>
    public long Executed => Interlocked.Read(ref _executed);

    public LatencyRecorder Immediate { get; } = new();
    public LatencyRecorder Delayed { get; } = new();

    /// <summary><c>Seq</c> values from <c>EnqueueManyAsync</c> calls that threw.</summary>
    public ConcurrentBag<long> FailedCallSeqs { get; } = [];

    /// <summary><c>Seq</c> values of jobs that fail on their first attempt.</summary>
    public ConcurrentBag<long> FailFirstSeqs { get; } = [];

    public void CountExecution(long seq)
    {
        Counter.Increment(seq);
        Interlocked.Increment(ref _executed);
    }

    /// <summary>
    /// Counts excess executions among the first <paramref name="issued"/> <c>Seq</c> values for a run without a drain. Excess executions are
    /// duplicates even without draining. A missing execution may simply be pending. Call after stopping the hosts.
    /// </summary>
    public long DuplicatedExecutions(long issued) =>
        ExecutionReconciler.Reconcile(Counter, issued, FailFirstSeqs.ToHashSet(), FailedCallSeqs.ToHashSet()).Duplicated;
}
