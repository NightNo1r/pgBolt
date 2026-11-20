using System.Diagnostics;
using Microsoft.Extensions.Logging;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class BasicScenarioTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Enqueued_job_is_executed_and_succeeds()
    {
        var host = await StartHostAsync();
        var id = await EnqueueAsync(new SuccessArgs(1), client: host.Client);

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Equal(host.ServerId, await JobServerIdAsync(id));
        Assert.Null(await QueueRowAsync(id));
        Assert.Empty(await EventsAsync(id));
        var run = Assert.Single(Probe.Runs(id));
        Assert.Equal(RunEnd.Completed, run.End);
        Assert.Equal("host", run.Host);
    }

    [Fact]
    public async Task Five_hundred_jobs_are_executed_once_and_acked_in_batches()
    {
        var host = await StartHostAsync();
        var results = await host.Client.EnqueueManyAsync(
            Enumerable.Range(0, 500).Select(n => new SuccessArgs(n)).ToList(), ct: Ct);
        Assert.All(results, r => Assert.True(r.Created));

        await WaitUntilAsync(async () => await CountAsync("SELECT count(*) FROM {schema}.jobs WHERE state = 2") == 500,
            TimeSpan.FromSeconds(30), "all 500 jobs succeed");

        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.queue"));
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.job_events"));
        Assert.All(results, r => Assert.Equal(1, Probe.RunCount(r.Id ?? throw new InvalidOperationException())));
        // Один ack-statement — одно now(): число различных finished_at = число пакетов ack.
        var batches = await CountAsync("SELECT count(DISTINCT finished_at) FROM {schema}.jobs");
        Assert.True(batches < 250, $"acks must be batched: {batches} distinct ack statements for 500 jobs");
    }

    [Fact]
    public async Task Retry_with_delay_waits_for_promotion_and_succeeds_on_second_attempt()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new FlakyArgs(1), new EnqueueOptions { RetryDelays = [TimeSpan.FromSeconds(1)] });

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        var retry = Assert.Single(await EventsAsync(id));
        Assert.Equal(JobEventKind.Retry, retry.Kind);
        Assert.Equal(1, retry.Attempt);
        Assert.Contains("flaky attempt 1", retry.Message);
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        var gap = Stopwatch.GetElapsedTime(runs[0].FinishedAt, runs[1].StartedAt);
        Assert.True(gap >= TimeSpan.FromMilliseconds(900), $"second attempt must wait for run_at and promotion, gap {gap}");
    }

    [Fact]
    public async Task Exhausted_retries_end_in_failed_with_retry_events()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new FailArgs("boom"), ImmediateRetry);

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(15));

        Assert.Equal(3, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.Retry, JobEventKind.Retry, JobEventKind.Failed }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Equal("boom", e.Message));
        Assert.Equal(new int?[] { 1, 2, 3 }, events.Select(e => e.Attempt));
        Assert.Equal(3, Probe.RunCount(id));
        Assert.Null(await QueueRowAsync(id));
    }

    [Fact]
    public async Task JobAbortedException_fails_immediately()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new AbortArgs("pointless"));

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        var failed = Assert.Single(await EventsAsync(id));
        Assert.Equal(JobEventKind.Failed, failed.Kind);
        Assert.Equal("pointless", failed.Message);
        Assert.Equal(1, Probe.RunCount(id));
    }

    [Fact]
    public async Task Malformed_arguments_fail_immediately_without_invoking_the_job()
    {
        await StartHostAsync(); // публикует job_types — нужен для enqueue по имени
        var id = await EnqueueByNameAsync("test.success", """{"N":"not-a-number"}""");

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.Failed }, await EventKindsAsync(id));
        Assert.Equal(0, Probe.RunCount(id));
    }

    [Fact]
    public async Task Cancelling_a_running_job_is_delivered_by_heartbeat()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new BlockArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "job starts");

        Assert.Equal(CancelResult.CancellationRequested, await Client.CancelAsync(id, "test", Ct));
        await WaitForStateAsync(id, DbState.Cancelled, ServerTestHost.HeartbeatInterval + TimeSpan.FromSeconds(2));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Contains(JobEventKind.Cancelled, await EventKindsAsync(id));
        var run = Assert.Single(Probe.Runs(id));
        Assert.True(run.WasCancelled);
        Assert.Equal(RunEnd.Threw, run.End);
    }

    [Fact]
    public async Task Timeout_cancels_the_job_and_retries_with_timeout_message()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new TimeLimitedArgs(1)); // [Job(TimeoutSeconds = 1)], MaxAttempts = 2, retry_delays = [0]

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.Retry, JobEventKind.Failed }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Contains("timeout after 1 s", e.Message));
        Assert.All(Probe.Runs(id), r =>
        {
            Assert.True(r.WasCancelled);
            Assert.InRange(r.Duration, TimeSpan.FromMilliseconds(900), TimeSpan.FromSeconds(5));
        });
    }

    [Fact]
    public async Task Job_without_timeout_runs_longer_than_two_lease_durations()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new SlowArgs(7000)); // 7 с > 2 × LeaseDuration (3 с)

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(20));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Empty(await EventsAsync(id));
        var run = Assert.Single(Probe.Runs(id));
        Assert.False(run.WasCancelled);
        Assert.True(run.Duration >= 2 * ServerTestHost.LeaseDuration, $"run took {run.Duration}");
    }

    [Fact]
    public async Task Job_type_without_executor_is_warned_at_start_retried_as_NoExecutor_and_failed()
    {
        // «Другой процесс» публикует test.orphan (default_queue = "q"), сам его очередь не обслуживает.
        var publisher = await StartHostAsync(h =>
        {
            h.Name = "P";
            h.Jobs = TestJobs.RegisterWithOrphan;
            h.AddServer("publisher", s => s.Queue("p-idle", ServerTestHost.FastQueue));
        });
        await publisher.StopAsync();

        var executor = await StartHostAsync(h => h.Name = "E");
        Assert.Contains(executor.Logs.Entries,
            e => e.Level == LogLevel.Warning && e.Message.Contains("test.orphan", StringComparison.Ordinal));

        var id = await EnqueueByNameAsync("test.orphan", """{"N":1}"""); // max_attempts = 2, retry_delays = [0] из job_types

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.NoExecutor, JobEventKind.NoExecutor }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Contains("test.orphan", e.Message));
        Assert.Equal(0, Probe.RunCount(id));
    }
}

public sealed class BasicScenarioTests_Direct(PostgresFixture pg) : BasicScenarioTests(pg, ConnectionMode.Direct);
public sealed class BasicScenarioTests_Bouncer(PostgresFixture pg) : BasicScenarioTests(pg, ConnectionMode.Bouncer);
