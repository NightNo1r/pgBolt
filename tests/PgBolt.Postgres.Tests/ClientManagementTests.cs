using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ClientManagementTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private readonly RecordingWakeup _wakeup = new();
    private IJobClient _client = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        _client = ClientTelemetry.Client(Store, _wakeup);
    }

    [Fact]
    public async Task Cancel_delegates_to_store()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(1), ct: Ct);
        Assert.Equal(CancelResult.Cancelled, await _client.CancelAsync(r.Id!.Value, "why", Ct));
        Assert.Equal(CancelResult.AlreadyFinished, await _client.CancelAsync(r.Id!.Value, null, Ct));
    }

    [Fact]
    public async Task Trigger_signals_wakeup()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Delay = TimeSpan.FromHours(1) }, Ct);
        _wakeup.Signals.Clear();
        Assert.True(await _client.TriggerAsync(r.Id!.Value, Ct));
        Assert.Equal(new[] { "client" }, _wakeup.Signals);
        Assert.False(await _client.TriggerAsync(r.Id!.Value, Ct));
    }

    [Fact]
    public async Task Requeue_signals_wakeup_on_success()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { MaxAttempts = 1 }, Ct);
        var job = Assert.Single(await Store.DequeueAsync("client", 1, ServerA, Lease, Ct));
        await Store.FailAsync(job.Id, job.LeaseId, ServerA, JobEventKind.Failed, null, null, Ct);
        _wakeup.Signals.Clear();

        Assert.Equal(RequeueResult.Requeued, await _client.RequeueAsync(r.Id!.Value, Ct));
        Assert.Equal(new[] { "client" }, _wakeup.Signals);
        Assert.Equal(RequeueResult.NotTerminal, await _client.RequeueAsync(r.Id!.Value, Ct));
    }

    [Fact]
    public async Task Get_returns_snapshot()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(5), ct: Ct);
        var info = await _client.GetAsync(r.Id!.Value, Ct);
        Assert.NotNull(info);
        Assert.Equal((JobState.Enqueued, "client.test", 5), (info.State, info.Name, info.Args.GetProperty("Order").GetInt32()));
        Assert.Null(await _client.GetAsync(JobId.New(), Ct));
    }
}

public sealed class ClientManagementTests_Direct(PostgresFixture pg) : ClientManagementTests(pg, ConnectionMode.Direct);
public sealed class ClientManagementTests_Bouncer(PostgresFixture pg) : ClientManagementTests(pg, ConnectionMode.Bouncer);
