using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Tests.Slo;

public sealed class ExecutionReconcilerTests
{
    private static readonly HashSet<long> None = [];

    private static ExecutionCounter Counter(params int[] runs)
    {
        var counter = new ExecutionCounter(runs.Length);
        counter.EnsureChunk(0);
        for (var seq = 0; seq < runs.Length; seq++)
            for (var i = 0; i < runs[seq]; i++) counter.Increment(seq);
        return counter;
    }

    [Fact]
    public void Complete_counter_has_no_discrepancies() =>
        Assert.Equal((0L, 0L, 0L), ExecutionReconciler.Reconcile(Counter(1, 2, 1), 3, new HashSet<long> { 1 }, None));

    [Fact]
    public void Missing_execution_is_reported() =>
        Assert.Equal((1L, 0L, 0L), ExecutionReconciler.Reconcile(Counter(1, 0, 1), 3, None, None));

    [Fact]
    public void Duplicate_execution_is_reported() =>
        Assert.Equal((0L, 1L, 0L), ExecutionReconciler.Reconcile(Counter(1, 2, 1), 3, None, None));

    [Fact]
    public void Fail_first_expects_two()
    {
        var failFirst = new HashSet<long> { 0, 1 };

        Assert.Equal((1L, 0L, 0L), ExecutionReconciler.Reconcile(Counter(1, 2), 2, failFirst, None));
    }

    [Fact]
    public void Failed_call_seq_with_0_1_2_is_excluded() =>
        Assert.Equal((0L, 0L, 3L),
            ExecutionReconciler.Reconcile(Counter(0, 1, 2, 1), 4, None, new HashSet<long> { 0, 1, 2 }));

    [Fact]
    public void Seqs_beyond_issued_are_not_checked() =>
        Assert.Equal((0L, 0L, 0L), ExecutionReconciler.Reconcile(Counter(1, 1, 0), 2, None, None));
}
