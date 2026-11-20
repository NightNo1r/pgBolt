using PgBolt.LoadTest.Run;

namespace PgBolt.LoadTest.Tests.Run;

public sealed class RunContextTests
{
    [Fact]
    public void Extra_executions_without_drain_count_only_runs_above_expected()
    {
        // Seq: 0 — выполнена раз; 1 — FailFirstAttempt, повтор ещё не дошёл; 2 — выполнена дважды; 3 — из
        // неуспешного вызова, дважды; 4 — FailFirstAttempt, трижды; 5 — не выполнена; 6 — за пределами выданных.
        var ctx = new RunContext(7);
        ctx.Counter.EnsureChunk(0);
        int[] runs = [1, 1, 2, 2, 3, 0, 5];
        for (var seq = 0; seq < runs.Length; seq++)
            for (var i = 0; i < runs[seq]; i++) ctx.CountExecution(seq);
        ctx.FailFirstSeqs.Add(1);
        ctx.FailFirstSeqs.Add(4);
        ctx.FailedCallSeqs.Add(3);

        Assert.Equal(2, ctx.DuplicatedExecutions(issued: 6));
    }
}
