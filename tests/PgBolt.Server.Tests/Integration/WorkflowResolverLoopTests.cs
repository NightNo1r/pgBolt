using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Resolver с ResolverInterval = 10 с над хранилищем без ICompletionSignal: ack в нём resolver не будит.</summary>
internal static class WorkflowResolverHarness
{
    public static (ProcessingMaintenance Maintenance, PgBoltStore Store) Create(TestDb db)
    {
        var store = new PgBoltStore(db.DataSource, db.Schema, usePreparedStatements: false);
        var options = new PgBoltProcessingOptions { ResolverInterval = TimeSpan.FromSeconds(10) };
        var clock = new MonoClock(TimeProvider.System);
        var maintenance = new ProcessingMaintenance(new PgBoltServerStore(store), new InProcessWakeupSignal(), options,
            new FakeMaintenance(), new PgBoltOptions(), clock, NullLogger.Instance, TestTelemetry.Metrics(), new ClusterQueueStats(clock, options),
            new ClusterTableStats(clock, options));
        return (maintenance, store);
    }
}

/// <summary>§12 Batches, §1 «Задержка»: многошаговый путь и отмена — повторами итерации, без пауз и без in-process сигнала.</summary>
public abstract class WorkflowResolverLoopTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private (ProcessingMaintenance Maintenance, PgBoltStore Store) Resolver() => WorkflowResolverHarness.Create(Db);

    [Fact]
    public async Task Member_batch_continuation_and_its_nested_batch_resolve_in_one_iteration()
    {
        IJobStep? x = null, y = null;
        var outer = Client.Workflow().StartBatch(b => x = b.Enqueue(new SuccessArgs(1)));
        outer.ContinueWithBatch(c => c.StartBatch(n => y = n.Enqueue(new SuccessArgs(2))));
        await outer.SubmitAsync(Ct);
        var (maintenance, store) = Resolver();
        var server = Guid.NewGuid();
        var job = Assert.Single(await store.DequeueAsync(TestJobs.Queue, 10, server, TimeSpan.FromMinutes(1), Ct));
        Assert.Equal(x!.Id.Value, job.Id);
        Assert.Single(await store.AckAsync(server, [new LeaseRef(job.Id, job.LeaseId)], Ct));

        await maintenance.ResolveOnceAsync(Ct);   // член → batch → OnSuccess batch-продолжение → его вложенный batch → запуск

        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND await_condition IS NULL FROM {schema}.queue WHERE job_id = @id",
            ("id", y!.Id.Value)));
    }

    [Fact]
    public async Task Small_batch_cancel_finishes_in_one_resolver_call()   // §12 CancelBatchAsync: без пауз
    {
        var batch = (await Client.Workflow().StartBatch(b => b.EnqueueMany(new[] { new SuccessArgs(1), new SuccessArgs(2) })).SubmitAsync(Ct)).Batches[0];
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));

        await Resolver().Maintenance.ResolveOnceAsync(Ct);

        Assert.Equal(BatchState.Cancelled, (await Client.GetBatchAsync(batch, Ct))!.State);
    }
}

public sealed class WorkflowResolverLoopTests_Direct(PostgresFixture pg) : WorkflowResolverLoopTests(pg, ConnectionMode.Direct);
public sealed class WorkflowResolverLoopTests_Bouncer(PostgresFixture pg) : WorkflowResolverLoopTests(pg, ConnectionMode.Bouncer);

/// <summary>§12 CancelBatchAsync: batch с 5000 вложенными отменяется итерациями без пауз. Только Direct, Slow.</summary>
[Trait("Category", "Slow")]
public sealed class WorkflowResolverLoadTests(PostgresFixture pg) : ServerIntegrationTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Batch_with_5000_nested_batches_is_cancelled_in_one_resolver_call()
    {
        var batch = (await Client.Workflow().StartBatch(b =>
        {
            for (var i = 0; i < 5000; i++)
            {
                var n = i;
                b.StartBatch(nested => nested.Enqueue(new SuccessArgs(n)));
            }
        }).SubmitAsync(Ct)).Batches[0];
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));

        await WorkflowResolverHarness.Create(Db).Maintenance.ResolveOnceAsync(Ct);

        var info = (await Client.GetBatchAsync(batch, Ct))!;
        Assert.Equal((BatchState.Cancelled, 5000), (info.State, info.Cancelled));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions"));
    }
}
