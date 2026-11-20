using PgBolt.Dashboard.Tests.Support;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Data;

public abstract class TerminalListsTests(PostgresFixture pg, ConnectionMode mode) : DashboardTestBase(pg, mode)
{
    private static readonly Guid Server = Guid.Parse("00000000-0000-0000-0000-0000000000f1");

    private Task<Page<JobListRow>> ListAsync(JobState kind, string? type = null, string? after = null) =>
        Store.ListJobsAsync(kind, null, type, after, Ct);

    /// <summary>Задача типа dash.job через IJobClient (публикация типа — идемпотентный upsert).</summary>
    private async Task<Guid> EnqueuePublishedAsync(TimeSpan? delay = null, int maxAttempts = 3)
    {
        await PublishTypeAsync();
        return await EnqueueAsync(delay: delay, maxAttempts: maxAttempts);
    }

    private async Task<LeasedJob> EnqueueAndLeaseAsync(int maxAttempts = 3)
    {
        await EnqueuePublishedAsync(maxAttempts: maxAttempts);
        return await LeaseAsync();
    }

    private Task<FailOutcome> FailAsync(LeasedJob job, JobEventKind kind = JobEventKind.Failed, string? message = "boom") =>
        Core.FailAsync(job.Id, job.LeaseId, Server, kind, message, null, Ct);

    private async Task ExpireAndSweepAsync(Guid id)
    {
        await Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() - interval '1 second' WHERE job_id = @id", ("id", id));
        await Core.SweepExpiredLeasesAsync(100, Ct);
    }

    private async Task RequeueAsync(Guid id) =>
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(new JobId(id), Ct));

    [Fact]
    public async Task Succeeded_is_newest_first()
    {
        var ids = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            var job = await EnqueueAndLeaseAsync();
            await Core.AckAsync(Server, [new LeaseRef(job.Id, job.LeaseId)], Ct);
            ids.Add(job.Id);
        }

        var page = await ListAsync(JobState.Succeeded);

        Assert.Equal(ids.OrderDescending(), page.Rows.Select(r => r.Id));
        Assert.All(page.Rows, r =>
        {
            Assert.NotNull(r.FinishedAt);
            Assert.NotNull(r.StartedAt);
            Assert.Equal(Server, r.ServerId);
        });
        Assert.Null(page.Next);
    }

    [Fact]
    public async Task Failed_from_worker_no_executor_and_sweeper_are_listed()
    {
        var failed = await EnqueueAndLeaseAsync();
        await FailAsync(failed);
        var noExecutor = await EnqueueAndLeaseAsync();
        await FailAsync(noExecutor, JobEventKind.NoExecutor, null);
        var swept = await EnqueueAndLeaseAsync(maxAttempts: 1);
        await ExpireAndSweepAsync(swept.Id);

        var rows = (await ListAsync(JobState.Failed)).Rows;

        Assert.Equal([swept.Id, noExecutor.Id, failed.Id], rows.Select(r => r.Id));
        Assert.Equal([JobEventKind.LeaseExpired, JobEventKind.NoExecutor, JobEventKind.Failed], rows.Select(r => r.EventKind!.Value));
        Assert.Equal("boom", rows[2].Message);
        Assert.Null(rows[0].Message);
    }

    [Fact]
    public async Task Non_terminal_events_do_not_list()
    {
        var swept = await EnqueueAndLeaseAsync();
        await ExpireAndSweepAsync(swept.Id);
        var retried = await EnqueueAndLeaseAsync();
        await Core.RetryAsync(retried.Id, retried.LeaseId, Server, TimeSpan.FromMinutes(1), JobEventKind.NoExecutor, null, null, Ct);

        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.job_events WHERE kind IN (3, 7)"));
        Assert.Empty((await ListAsync(JobState.Failed)).Rows);
    }

    [Fact]
    public async Task Requeue_removes_job_from_failed()
    {
        var job = await EnqueueAndLeaseAsync();
        await FailAsync(job);
        Assert.Equal(job.Id, Assert.Single((await ListAsync(JobState.Failed)).Rows).Id);

        await RequeueAsync(job.Id);

        Assert.Empty((await ListAsync(JobState.Failed)).Rows);
    }

    [Fact]
    public async Task Repeated_failure_after_requeue_is_one_row()
    {
        var job = await EnqueueAndLeaseAsync();
        await FailAsync(job);
        await RequeueAsync(job.Id);
        await FailAsync(await LeaseAsync());

        Assert.Equal(job.Id, Assert.Single((await ListAsync(JobState.Failed)).Rows).Id);
    }

    [Fact]
    public async Task Repeated_failure_through_sweeper_is_one_row()
    {
        var job = await EnqueueAndLeaseAsync(maxAttempts: 1);
        await FailAsync(job);
        await RequeueAsync(job.Id);
        await LeaseAsync();
        await ExpireAndSweepAsync(job.Id);

        var row = Assert.Single((await ListAsync(JobState.Failed)).Rows);
        Assert.Equal((job.Id, JobEventKind.LeaseExpired), (row.Id, row.EventKind));
    }

    [Fact]
    public async Task Sweeper_with_cancel_request_lists_only_in_cancelled()
    {
        var job = await EnqueueAndLeaseAsync();
        Assert.Equal(CancelResult.CancellationRequested, await Client.CancelAsync(new JobId(job.Id), "stop", Ct));
        await ExpireAndSweepAsync(job.Id);

        var row = Assert.Single((await ListAsync(JobState.Cancelled)).Rows);
        Assert.Equal((job.Id, JobEventKind.Cancelled), (row.Id, row.EventKind));
        Assert.Empty((await ListAsync(JobState.Failed)).Rows);
    }

    [Fact]
    public async Task Cancelled_lists_cancelled_jobs()
    {
        var id = await EnqueuePublishedAsync(TimeSpan.FromHours(1));
        Assert.Equal(CancelResult.Cancelled, await Client.CancelAsync(new JobId(id), "not needed", Ct));

        var row = Assert.Single((await ListAsync(JobState.Cancelled)).Rows);
        Assert.Equal((id, JobEventKind.Cancelled, "not needed"), (row.Id, row.EventKind, row.Message));
        Assert.NotNull(row.FinishedAt);
    }

    [Fact]
    public async Task Events_of_missing_jobs_give_no_rows()
    {
        await Db.ExecAsync("INSERT INTO {schema}.job_events (job_id, at, kind) VALUES (gen_random_uuid(), now(), 2)");

        Assert.Empty((await ListAsync(JobState.Failed)).Rows);
    }

    [Fact]
    public async Task Failed_type_filter_and_budget_progress()
    {
        Time.Step = TimeSpan.FromSeconds(1);
        var needles = await SeedJobsAsync(3, "needle", "default", 3, "none");
        await SeedJobsAsync(4500, "noise", "default", 3, "none");
        await Db.ExecAsync("UPDATE {schema}.jobs SET finished_at = finished_at + interval '1 minute' WHERE type = 'noise'");
        await Db.ExecAsync("INSERT INTO {schema}.job_events (job_id, at, kind) SELECT id, finished_at, 2 FROM {schema}.jobs WHERE state = 3");

        var page = await ListAsync(JobState.Failed, type: "needle");

        Assert.Empty(page.Rows);
        Assert.NotNull(page.Next);
        var lastScanned = await Db.ScalarAsync<DateTime>(
            "SELECT at FROM {schema}.job_events ORDER BY at DESC, job_id DESC OFFSET 3999 LIMIT 1");
        Assert.Equal(lastScanned, page.ScannedTo!.Value.UtcDateTime);

        page = await ListAsync(JobState.Failed, type: "needle", after: page.Next);

        Assert.Equal(needles.Reverse(), page.Rows.Select(r => r.Id));
        Assert.Null(page.ScannedTo);
        Assert.Null(page.Next);
    }

    [Fact]
    public async Task Succeeded_portions_skip_other_states()
    {
        var succeeded = await SeedJobsAsync(2, "dash.job", "default", 2, "none");
        await SeedJobsAsync(2500, "dash.job", "default", 3, "none");

        var page = await ListAsync(JobState.Succeeded);

        Assert.Equal(succeeded.Reverse(), page.Rows.Select(r => r.Id));
        Assert.Null(page.Next);
        Assert.Null(page.ScannedTo);
    }
}

public sealed class TerminalListsTests_Direct(PostgresFixture pg) : TerminalListsTests(pg, ConnectionMode.Direct);
public sealed class TerminalListsTests_Bouncer(PostgresFixture pg) : TerminalListsTests(pg, ConnectionMode.Bouncer);
