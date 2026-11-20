using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Data;

public abstract class RelatedStoreTests(PostgresFixture pg, ConnectionMode mode) : DashboardTestBase(pg, mode)
{
    private static readonly JsonElement Args = JsonDocument.Parse("{}").RootElement;
    private static readonly Guid Server = Guid.Parse("00000000-0000-0000-0000-0000000000f2");

    private async Task<DateTimeOffset> JobCreatedAsync(JobId id) => (await Client.GetAsync(id, Ct))!.CreatedAt;

    private async Task<DateTimeOffset> BatchCreatedAsync(BatchId id) => (await Client.GetBatchAsync(id, Ct))!.CreatedAt;

    private Task<Page<RelatedJobRow>> JobContinuationsAsync(JobId parent, DateTimeOffset created, string? after = null) =>
        Store.ListContinuationsAsync(parent.Value, batchParent: false, created, after, Ct);

    [Fact]
    public async Task Job_continuations_include_launched_ones_with_current_state()
    {
        await PublishTypeAsync();
        var wf = Client.Workflow();
        var parent = wf.Enqueue("dash.job", Args);
        parent.ContinueWith("dash.job", Args, ContinuationCondition.OnSuccess);
        parent.ContinueWith("dash.job", Args, ContinuationCondition.OnAnyFinished);
        await wf.SubmitAsync(Ct);
        var created = await JobCreatedAsync(parent.Id);

        var before = (await JobContinuationsAsync(parent.Id, created)).Rows;
        Assert.Equal([JobState.Awaiting, JobState.Awaiting], before.Select(r => r.State));
        Assert.Equal([(short)1, (short)2], before.OrderBy(r => r.Continuation).Select(r => r.Continuation!.Value));

        var leased = await LeaseAsync();
        Assert.Equal(parent.Id.Value, leased.Id);
        await Core.AckAsync(Server, [new LeaseRef(leased.Id, leased.LeaseId)], Ct);
        await ResolveAllAsync();

        var after = (await JobContinuationsAsync(parent.Id, created)).Rows;
        Assert.Equal([JobState.Enqueued, JobState.Enqueued], after.Select(r => r.State));
        Assert.Equal(before.Select(r => r.Id), after.Select(r => r.Id));
    }

    [Fact]
    public async Task Batch_continuation_members_are_not_continuations_of_parent()
    {
        await PublishTypeAsync();
        var wf = Client.Workflow();
        var parent = wf.Enqueue("dash.job", Args);
        var batchCont = parent.ContinueWithBatch(b =>
        {
            b.Enqueue("dash.job", Args);
            b.Enqueue("dash.job", Args);
        });
        await wf.SubmitAsync(Ct);
        var created = await BatchCreatedAsync(batchCont.Id);

        Assert.Empty((await JobContinuationsAsync(parent.Id, await JobCreatedAsync(parent.Id))).Rows);
        Assert.Equal(batchCont.Id.Value, Assert.Single((await Store.ListBatchContinuationsAsync(parent.Id.Value, null, Ct)).Rows).Id);
        Assert.Equal(2L, await Db.ScalarAsync<long>(
            "SELECT count(*) FROM {schema}.jobs WHERE continuation = 5 AND parent_id = @b", ("b", batchCont.Id.Value)));

        Assert.Empty((await Store.ListContinuationsAsync(batchCont.Id.Value, batchParent: true, created, null, Ct)).Rows);
        Assert.Equal(2, (await Store.ListBatchJobsAsync(batchCont.Id.Value, created, null, Ct)).Rows.Count);
    }

    [Fact]
    public async Task Batch_card_lists_members_nested_and_continuations()
    {
        await PublishTypeAsync();
        var wf = Client.Workflow();
        IBatchStep nested = null!;
        var batch = wf.StartBatch(b =>
        {
            b.Enqueue("dash.job", Args);
            b.Enqueue("dash.job", Args);
            nested = b.StartBatch(n => n.Enqueue("dash.job", Args));
        });
        var continuation = batch.ContinueWith("dash.job", Args);
        var batchCont = batch.ContinueWithBatch(c => c.Enqueue("dash.job", Args));
        await wf.SubmitAsync(Ct);
        var id = batch.Id.Value;
        var created = await BatchCreatedAsync(batch.Id);

        Assert.Equal(2, (await Store.ListBatchJobsAsync(id, created, null, Ct)).Rows.Count);
        Assert.Equal(nested.Id.Value, Assert.Single((await Store.ListNestedBatchesAsync(id, null, Ct)).Rows).Id);
        var row = Assert.Single((await Store.ListContinuationsAsync(id, batchParent: true, created, null, Ct)).Rows);
        Assert.Equal((continuation.Id.Value, (short?)3), (row.Id, row.Continuation));
        Assert.Equal(batchCont.Id.Value, Assert.Single((await Store.ListBatchContinuationsAsync(id, null, Ct)).Rows).Id);
    }

    [Fact]
    public async Task Batch_progress_is_never_negative()
    {
        await PublishTypeAsync();
        var batch = Client.Workflow().StartBatch(b => b.Enqueue("dash.job", Args));
        await batch.SubmitAsync(Ct);
        await Db.ExecAsync("UPDATE {schema}.batches SET succeeded = -2, failed = -1");

        var plain = Assert.Single((await Store.ListBatchesAsync(null, null, Ct)).Rows);
        var filtered = Assert.Single((await Store.ListBatchesAsync(BatchState.Active, null, Ct)).Rows);

        Assert.All(new[] { plain, filtered }, r => Assert.Equal((batch.Id.Value, 0, 0), (r.Id, r.Succeeded, r.Failed)));
    }

    [Fact]
    public async Task Batches_list_newest_first_and_filter_by_state_in_portions()
    {
        await PublishTypeAsync();
        var ids = new List<BatchId>();
        for (var i = 0; i < 3; i++)
        {
            var batch = Client.Workflow().StartBatch(b => b.Enqueue("dash.job", Args));
            await batch.SubmitAsync(Ct);
            ids.Add(batch.Id);
        }
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(ids[1], Ct));
        await ResolveAllAsync();

        var all = await Store.ListBatchesAsync(null, null, Ct);
        var cancelled = await Store.ListBatchesAsync(BatchState.Cancelled, null, Ct);

        Assert.Equal(ids.Select(b => b.Value).OrderDescending(), all.Rows.Select(r => r.Id));
        Assert.Null(all.Next);
        var row = Assert.Single(cancelled.Rows);
        Assert.Equal((ids[1].Value, BatchState.Cancelled), (row.Id, row.State));
        Assert.Null(cancelled.Next);
    }

    [Fact]
    public async Task Continuations_page_by_fifty()
    {
        await PublishTypeAsync();
        var wf = Client.Workflow();
        var parent = wf.Enqueue("dash.job", Args);
        for (var i = 0; i < 55; i++)
            parent.ContinueWith("dash.job", Args);
        var result = await wf.SubmitAsync(Ct);
        var created = await JobCreatedAsync(parent.Id);

        var first = await JobContinuationsAsync(parent.Id, created);
        var second = await JobContinuationsAsync(parent.Id, created, first.Next);

        Assert.Equal(50, first.Rows.Count);
        Assert.NotNull(first.Next);
        Assert.Equal(5, second.Rows.Count);
        Assert.Null(second.Next);
        Assert.Equal(result.Jobs.Skip(1).Select(j => j.Value).Order(), first.Rows.Concat(second.Rows).Select(r => r.Id));
    }

    [Fact]
    public async Task Recurring_list_by_id_with_text_cursor()
    {
        await PublishTypeAsync();
        var manager = Services.GetRequiredService<IRecurringJobManager>();
        string[] special = ["a/b", "отчёт", "Z-last", "имя/с/слешами"];
        foreach (var id in special.Concat(Enumerable.Range(0, 48).Select(i => $"job-{i:D2}")))
            await manager.AddOrUpdateAsync(id, "dash.job", "0 0 * * *", Args, ct: Ct);
        var expected = await Db.ScalarAsync<string[]>("SELECT array_agg(id ORDER BY id) FROM {schema}.recurring_jobs");

        var first = await Store.ListRecurringAsync(null, Ct);
        var second = await Store.ListRecurringAsync(first.Next, Ct);

        Assert.Equal(50, first.Rows.Count);
        Assert.Equal(2, second.Rows.Count);
        Assert.Null(second.Next);
        Assert.Equal(expected, first.Rows.Concat(second.Rows).Select(r => r.Id));
        Assert.All(first.Rows, r => Assert.Equal(("dash.job", "0 0 * * *"), (r.Type, r.Cron)));
    }

    [Fact]
    public async Task Recurring_runs_are_newest_first_with_derived_state()
    {
        await PublishTypeAsync();
        var manager = Services.GetRequiredService<IRecurringJobManager>();
        await manager.AddOrUpdateAsync("nightly", "dash.job", "0 0 * * *", Args, new RecurringJobOptions { Overlap = OverlapPolicy.Allow }, Ct);
        for (var i = 0; i < 3; i++)
            Assert.NotNull(await manager.TriggerAsync("nightly", Ct));
        var leased = await LeaseAsync();

        var rows = (await Store.ListRecurringRunsAsync("nightly", null, Ct)).Rows;

        Assert.Equal(3, rows.Count);
        Assert.Equal(rows.Select(r => r.Id).OrderDescending(), rows.Select(r => r.Id));
        Assert.Equal([JobState.Enqueued, JobState.Enqueued, JobState.Processing], rows.Select(r => r.State));
        Assert.Equal(leased.Id, rows[2].Id);
    }
}

public sealed class RelatedStoreTests_Direct(PostgresFixture pg) : RelatedStoreTests(pg, ConnectionMode.Direct);
public sealed class RelatedStoreTests_Bouncer(PostgresFixture pg) : RelatedStoreTests(pg, ConnectionMode.Bouncer);
