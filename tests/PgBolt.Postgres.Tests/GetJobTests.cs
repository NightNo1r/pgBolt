using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class GetJobTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Unknown_job_is_null()
    {
        Assert.Null(await Store.GetJobAsync(Guid.CreateVersion7(), Ct));
    }

    [Fact]
    public async Task Derives_scheduled_enqueued_processing()
    {
        var scheduled = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        var enqueued = await EnqueueAsync(args: """{"x":42}""");
        Assert.Equal(JobState.Scheduled, (await Store.GetJobAsync(scheduled, Ct))!.State);

        var info = (await Store.GetJobAsync(enqueued, Ct))!;
        Assert.Equal(JobState.Enqueued, info.State);
        Assert.Equal(42, info.Args.GetProperty("x").GetInt32());
        Assert.Equal("test.job", info.Name);
        Assert.NotNull(info.RunAt);

        var job = await LeaseOneAsync();
        var processing = (await Store.GetJobAsync(job.Id, Ct))!;
        Assert.Equal(JobState.Processing, processing.State);
        Assert.Equal(1, processing.Attempt);
        Assert.Equal(ServerA, processing.ServerId);
        Assert.NotNull(processing.StartedAt);
    }

    [Fact]
    public async Task Terminal_job_reads_jobs_columns_and_events()
    {
        var id = await EnqueueAsync(maxAttempts: 2);
        var first = await LeaseOneAsync();
        await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "oops", "trace", Ct);
        var second = await LeaseOneAsync();
        await Store.AckAsync(ServerA, [new LeaseRef(id, second.LeaseId)], Ct);

        var info = (await Store.GetJobAsync(id, Ct))!;
        Assert.Equal(JobState.Succeeded, info.State);
        Assert.Equal(2, info.Attempt);
        Assert.Null(info.RunAt);
        Assert.NotNull(info.FinishedAt);
        var ev = Assert.Single(info.Events);
        Assert.Equal((JobEventKind.Retry, "oops", "trace"), (ev.Kind, ev.Message, ev.Exception));
    }

    [Fact]
    public async Task Cancellation_request_is_visible()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        await Store.CancelAsync(id, null, Ct);
        Assert.True((await Store.GetJobAsync(id, Ct))!.CancellationRequested);
    }

    [Fact]
    public async Task Get_returns_recurring_columns()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id) with { RecurringIds = ["daily"], ScheduledAtUtc = [at] }, null, Ct);

        var info = await Store.GetJobAsync(id, Ct);

        Assert.Equal("daily", info!.RecurringJobId);
        Assert.Equal(new DateTimeOffset(at), info.ScheduledAt);
    }
}

public sealed class GetJobTests_Direct(PostgresFixture pg) : GetJobTests(pg, ConnectionMode.Direct);
public sealed class GetJobTests_Bouncer(PostgresFixture pg) : GetJobTests(pg, ConnectionMode.Bouncer);
