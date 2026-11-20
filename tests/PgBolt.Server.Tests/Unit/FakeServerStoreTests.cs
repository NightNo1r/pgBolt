using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

/// <summary>Проверка тестовой поддержки: умолчания фейка и запись вызовов, на которые опираются тесты Tasks 12–16.</summary>
public sealed class FakeServerStoreTests
{
    private static readonly Guid Server = Guid.CreateVersion7();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Defaults_and_recorded_calls()
    {
        var store = new FakeServerStore();
        var job = Leased.Job();
        var lease = new LeaseRef(job.Id, job.LeaseId);

        Assert.Empty(await store.DequeueAsync("q", 5, Server, TimeSpan.FromMinutes(1), Ct));
        Assert.Equal([job.Id], await store.AckAsync(Server, [lease], Ct));
        Assert.Equal(FailOutcome.Retried, await store.RetryAsync(job.Id, job.LeaseId, Server, TimeSpan.FromSeconds(3), JobEventKind.Retry, "m", "x", Ct));
        Assert.Equal(FailOutcome.Failed, await store.FailAsync(job.Id, job.LeaseId, Server, JobEventKind.Failed, "m", "x", Ct));
        Assert.Equal(FailOutcome.Cancelled, await store.ConfirmCancelAsync(job.Id, job.LeaseId, Server, "c", Ct));
        Assert.Equal(ReleaseOutcome.Released, await store.ReleaseAsync(job.Id, job.LeaseId, Server, true, null, Ct));
        var hb = Assert.Single((await store.HeartbeatAsync(Server, [lease], TimeSpan.FromMinutes(1), Ct)).Leases);
        Assert.Equal(new HeartbeatLease(job.Id, job.LeaseId, false), hb);
        var registration = new ServerRegistration(Server, "s", "h", ["q"], 3);
        await store.RegisterServerAsync(registration, Ct);
        await store.RemoveServerAsync(Server, Ct);
        Assert.Equal(0, (await store.PromoteDueAsync(1000, Ct)).Promoted);
        Assert.Equal(0, await store.SweepExpiredLeasesAsync(1000, Ct));
        Assert.Empty(await store.PublishJobTypesAsync([], "me", Ct));
        Assert.Empty(await store.GetJobTypeNamesByQueuesAsync(["q"], Ct));

        Assert.Equal(["Dequeue", "Ack", "Retry", "Fail", "ConfirmCancel", "Release", "Heartbeat", "RegisterServer",
            "RemoveServer", "Promote", "Sweep", "PublishJobTypes", "JobTypeNames"], store.Calls.Select(c => c.Op));
        Assert.Equal(("q", 5, Server, TimeSpan.FromMinutes(1)), Assert.Single(store.CallsOf("Dequeue")).Detail);
        Assert.Equal([lease], (LeaseRef[])Assert.Single(store.CallsOf("Ack")).Detail!);
        var retry = Assert.Single(store.CallsOf("Retry"));
        Assert.Equal((job.Id, job.LeaseId), (retry.JobId!.Value, retry.LeaseId!.Value));
        Assert.Equal((TimeSpan.FromSeconds(3), JobEventKind.Retry, (string?)"m"), retry.Detail);
        Assert.Equal((JobEventKind.Failed, (string?)"m"), Assert.Single(store.CallsOf("Fail")).Detail);
        Assert.Equal((object?)true, Assert.Single(store.CallsOf("Release")).Detail);
        Assert.Equal([lease], (LeaseRef[])Assert.Single(store.CallsOf("Heartbeat")).Detail!);
        Assert.Same(registration, Assert.Single(store.CallsOf("RegisterServer")).Detail);
    }

    [Fact]
    public async Task Delegates_override_behaviour()
    {
        var store = new FakeServerStore
        {
            OnRelease = (_, _, ran, _) => Task.FromResult(ran ? ReleaseOutcome.Cancelled : ReleaseOutcome.LeaseLost),
            OnHeartbeat = (_, _) => Task.FromResult<IReadOnlyList<HeartbeatLease>>([]),
        };
        var job = Leased.Job();

        Assert.Equal(ReleaseOutcome.Cancelled, await store.ReleaseAsync(job.Id, job.LeaseId, Server, true, null, Ct));
        Assert.Equal(ReleaseOutcome.LeaseLost, await store.ReleaseAsync(job.Id, job.LeaseId, Server, false, null, Ct));
        Assert.Empty((await store.HeartbeatAsync(Server, [new LeaseRef(job.Id, job.LeaseId)], TimeSpan.FromMinutes(1), Ct)).Leases);
    }

    [Fact]
    public void Leased_job_defaults_and_fresh_lease_per_call()
    {
        var id = Guid.CreateVersion7();
        var a = Leased.Job(id);
        var b = Leased.Job(id, type: "x.job", attempt: 2, maxAttempts: 5, retryDelays: [1, 2]);

        Assert.Equal(id, a.Id);
        Assert.Equal(id, b.Id);
        Assert.NotEqual(a.LeaseId, b.LeaseId);
        Assert.Equal(("test.job", "{}", 1, 3), (a.Type, a.ArgsJson, a.Attempt, a.MaxAttempts));
        Assert.Null(a.RetryDelaysSeconds);
        Assert.Equal(("x.job", 2, 5), (b.Type, b.Attempt, b.MaxAttempts));
        Assert.Equal([1, 2], b.RetryDelaysSeconds!);
    }
}
