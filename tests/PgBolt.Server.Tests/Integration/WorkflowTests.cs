using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Часть 5 через сервер: resolver процесса будится ack'ом своего сервера; JobContext / JobInfo / BatchInfo.</summary>
public abstract class WorkflowTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Интервал resolver'а заведомо дольше Timeout: продолжения успевают только по in-process сигналу ack / fail своего
    /// сервера (§6.3, §1 — миллисекунды), а не по таймеру.
    /// </summary>
    private static void SignalOnlyResolver(TestHostSettings s) => s.Processing = p => p.ResolverInterval = TimeSpan.FromMinutes(5);

    [Fact]
    public async Task Chain_with_fan_out_runs_through_server_and_exposes_links()   // §5.3, §12 API: JobContext
    {
        await StartHostAsync(SignalOnlyResolver);
        var first = Client.Workflow().Enqueue(new SuccessArgs(1));
        var second = first.ContinueWith(new SuccessArgs(2));
        IReadOnlyList<IJobStep> members = [];
        var fan = second.ContinueWithBatch(b => members = b.EnqueueMany(new[] { new SuccessArgs(3), new SuccessArgs(4), new SuccessArgs(5) }));
        var report = fan.ContinueWith(new SuccessArgs(6), ContinuationCondition.OnAnyFinished);
        await first.SubmitAsync(Ct);

        await WaitForStateAsync(report.Id, DbState.Succeeded, Timeout);

        // Порядок — по меткам Stopwatch одного процесса (монотонны): FinishedAt ставится до ack, StartedAt — после dequeue.
        var firstRun = Assert.Single(Probe.Runs(first.Id));
        Assert.Equal(RunEnd.Completed, firstRun.End);
        var secondRun = Assert.Single(Probe.Runs(second.Id));
        Assert.Equal(RunEnd.Completed, secondRun.End);
        Assert.True(secondRun.StartedAt >= firstRun.FinishedAt, "the continuation starts after its parent finished");
        Assert.Equal(first.Id, secondRun.ParentId);
        Assert.Null(secondRun.ParentBatchId);
        Assert.Null(secondRun.BatchId);
        Assert.Equal(3, members.Count);
        var memberRuns = new List<JobRun>();
        foreach (var member in members)
        {
            var run = Assert.Single(Probe.Runs(member.Id));
            Assert.Equal(RunEnd.Completed, run.End);
            Assert.True(run.StartedAt >= secondRun.FinishedAt, "the batch continuation starts after its parent finished");
            Assert.Equal(((JobId?)null, (BatchId?)null, (BatchId?)fan.Id), (run.ParentId, run.ParentBatchId, run.BatchId));
            memberRuns.Add(run);
        }
        var reportRun = Assert.Single(Probe.Runs(report.Id));
        Assert.All(memberRuns, run => Assert.True(reportRun.StartedAt >= run.FinishedAt, "the batch's continuation starts after every member"));
        Assert.Equal(((JobId?)null, (BatchId?)fan.Id, (BatchId?)null), (reportRun.ParentId, reportRun.ParentBatchId, reportRun.BatchId));

        var batch = (await Client.GetBatchAsync(fan.Id, Ct))!;
        Assert.Equal((BatchState.Succeeded, 3, 3), (batch.State, batch.Total, batch.Succeeded));
        Assert.Equal((second.Id, ContinuationCondition.OnSuccess), (batch.ParentId!.Value, batch.Condition!.Value));
        var info = (await Client.GetAsync(report.Id, Ct))!;
        Assert.Equal((fan.Id, ContinuationCondition.OnAnyFinished), (info.ParentBatchId!.Value, info.Continuation!.Value));
    }

    [Fact]
    public async Task Failed_member_runs_OnAnyFinished_and_holds_OnSuccess()   // §2.2, §2.3
    {
        await StartHostAsync(SignalOnlyResolver);
        var batch = Client.Workflow().StartBatch(b =>
        {
            b.Enqueue(new AbortArgs("no retry"));
            b.Enqueue(new SuccessArgs(1));
        });
        var any = batch.ContinueWith(new SuccessArgs(2), ContinuationCondition.OnAnyFinished);
        var success = batch.ContinueWith(new SuccessArgs(3));
        await batch.SubmitAsync(Ct);

        await WaitForStateAsync(any.Id, DbState.Succeeded, Timeout);

        Assert.Equal(BatchState.Failed, (await Client.GetBatchAsync(batch.Id, Ct))!.State);
        Assert.Equal(JobState.Awaiting, (await Client.GetAsync(success.Id, Ct))!.State);
    }

    [Fact]
    public async Task Cancel_batch_stops_running_member_through_heartbeat()   // §6.5, §12 CancelBatchAsync
    {
        await StartHostAsync();
        IJobStep? blocked = null, waiting = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            blocked = b.Enqueue(new BlockArgs(1));
            waiting = blocked.ContinueWith(new SuccessArgs(1));
        });
        var any = batch.ContinueWith(new SuccessArgs(2), ContinuationCondition.OnAnyFinished);
        var success = batch.ContinueWith(new SuccessArgs(3));
        await batch.SubmitAsync(Ct);
        await WaitUntilAsync(() => Probe.RunCount(blocked!.Id) == 1, Timeout, "the member runs");

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch.Id, Ct));

        await WaitForStateAsync(any.Id, DbState.Succeeded, Timeout);
        Assert.True(Assert.Single(Probe.Runs(blocked!.Id)).WasCancelled);
        Assert.Equal(DbState.Cancelled, await JobStateAsync(blocked.Id));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(waiting!.Id));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(success.Id));
        Assert.Equal(BatchState.Cancelled, (await Client.GetBatchAsync(batch.Id, Ct))!.State);
    }
}

public sealed class WorkflowTests_Direct(PostgresFixture pg) : WorkflowTests(pg, ConnectionMode.Direct);
public sealed class WorkflowTests_Bouncer(PostgresFixture pg) : WorkflowTests(pg, ConnectionMode.Bouncer);
