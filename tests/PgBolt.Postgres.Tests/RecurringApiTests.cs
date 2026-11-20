using System.Text.Json;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RecurringApiTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private readonly RecordingWakeup _wakeup = new();
    private IRecurringJobManager _manager = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        RecurringTestSupport.EnsureRegistered();
        await RecurringTestSupport.PublishAsync(Store, Ct);
        _manager = RecurringTestSupport.Manager(Store, _wakeup);
    }

    private Task<bool> RowAsync(string condition, string id = "a") =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.recurring_jobs WHERE id = @id", ("id", id));

    [Fact]
    public async Task Add_creates_api_row_with_cursor_after_now()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);

        var info = await _manager.GetAsync("a", Ct);
        Assert.NotNull(info);
        Assert.Equal(("rec.job", "0 3 * * *", "UTC", (string?)null, false), (info.JobName, info.Cron, info.TimeZone, info.Owner, info.Paused));
        Assert.Equal(1, info.Args.GetProperty("N").GetInt32());
        Assert.True(info.NextFireAt > DateTimeOffset.UtcNow);
        Assert.True(await RowAsync("wake_at = next_fire_at AND (next_fire_at AT TIME ZONE 'UTC')::time = '03:00' AND last_fire_at IS NULL"));
    }

    [Fact]
    public async Task Update_keeps_cursor_unless_cron_changes()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = '2099-01-01', wake_at = '2099-01-01' WHERE id = 'a'");

        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(2), new RecurringJobOptions { Queue = "other" }, Ct);
        Assert.True(await RowAsync("""next_fire_at = '2099-01-01' AND args = '{"N":2}' AND queue = 'other'"""));

        await _manager.AddOrUpdateAsync("a", "0 4 * * *", new RecArgs(2), ct: Ct);
        Assert.True(await RowAsync(
            "next_fire_at < '2099-01-01' AND wake_at = next_fire_at AND (next_fire_at AT TIME ZONE 'UTC')::time = '04:00' AND queue IS NULL"));
    }

    [Fact]
    public async Task Cron_change_after_tick_does_not_repeat_fired_time()     // Review Focus 3
    {
        await _manager.AddOrUpdateAsync("a", "* * * * *", new RecArgs(1), ct: Ct);
        // тик уже поставил срабатывание позже момента, от которого API считает курсор (db_now до ожидания блокировки)
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET last_fire_at = date_trunc('minute', now()) + interval '2 minutes' WHERE id = 'a'");

        await _manager.AddOrUpdateAsync("a", "*/2 * * * *", new RecArgs(1), ct: Ct);

        Assert.True(await RowAsync("next_fire_at > last_fire_at"));
    }

    [Fact]
    public async Task Paused_survives_update()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        Assert.True(await _manager.PauseAsync("a", Ct));

        await _manager.AddOrUpdateAsync("a", "0 5 * * *", new RecArgs(1), ct: Ct);

        Assert.True((await _manager.GetAsync("a", Ct))!.Paused);
    }

    [Fact]
    public async Task Code_first_row_allows_only_pause_resume_trigger_get()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET owner = 'svc' WHERE id = 'a'");

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await _manager.AddOrUpdateAsync("a", "0 4 * * *", new RecArgs(2), ct: Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await _manager.RemoveAsync("a", Ct));
        Assert.True(await _manager.PauseAsync("a", Ct));
        Assert.True(await _manager.ResumeAsync("a", Ct));
        Assert.NotNull(await _manager.TriggerAsync("a", Ct));
        Assert.Equal("svc", (await _manager.GetAsync("a", Ct))!.Owner);
        Assert.True(await RowAsync("""cron = '0 3 * * *' AND args = '{"N":1}'"""));
    }

    [Fact]
    public async Task Missing_rows_and_remove()
    {
        Assert.False(await _manager.RemoveAsync("a", Ct));
        Assert.False(await _manager.PauseAsync("a", Ct));
        Assert.False(await _manager.ResumeAsync("a", Ct));
        Assert.Null(await _manager.TriggerAsync("a", Ct));
        Assert.Null(await _manager.GetAsync("a", Ct));

        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);

        Assert.True(await _manager.RemoveAsync("a", Ct));
        Assert.Null(await _manager.GetAsync("a", Ct));
    }

    [Fact]
    public async Task Resume_wakes_now_and_keeps_null_cursor()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await _manager.PauseAsync("a", Ct);

        await _manager.ResumeAsync("a", Ct);
        Assert.True(await RowAsync("NOT paused AND wake_at <= now() AND next_fire_at > now()"));

        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = NULL, wake_at = NULL, paused = true WHERE id = 'a'");
        await _manager.ResumeAsync("a", Ct);
        Assert.True(await RowAsync("wake_at IS NULL"));
    }

    [Fact]
    public async Task Trigger_enqueues_recurring_job_and_respects_overlap()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);

        var first = (await _manager.TriggerAsync("a", Ct))!.Value;
        var second = (await _manager.TriggerAsync("a", Ct))!.Value;

        Assert.True(first.Created);
        Assert.Equal(new EnqueueResult(first.Id, false), second);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT j.recurring_id = 'a' AND j.scheduled_at = j.created_at AND j.queue = 'rq' AND j.priority = 1
                   AND j.unique_key = 'pgbolt:recurring:a' AND j.args = '{"N":1}'
            FROM {schema}.jobs j WHERE j.id = @id
            """, ("id", first.Id!.Value.Value)));
        Assert.Equal(new[] { "rq" }, _wakeup.Signals);
        Assert.True(await RowAsync("last_fire_at IS NULL AND last_job_id IS NULL"));   // Trigger строку не меняет (§5.5)
    }

    [Fact]
    public async Task Trigger_with_overlap_allow_enqueues_every_time()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), new RecurringJobOptions { Overlap = OverlapPolicy.Allow }, Ct);

        Assert.True((await _manager.TriggerAsync("a", Ct))!.Value.Created);
        Assert.True((await _manager.TriggerAsync("a", Ct))!.Value.Created);

        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a' AND unique_key IS NULL"));
    }

    [Fact]
    public async Task Requeue_of_failed_run_conflicts_while_new_run_is_alive()   // core §5.9: ключ восстанавливается из jobs
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        var failed = (await _manager.TriggerAsync("a", Ct))!.Value.Id!.Value.Value;
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = 3, finished_at = now() WHERE id = @id",
            ("id", failed));
        var live = (await _manager.TriggerAsync("a", Ct))!.Value.Id!.Value.Value;

        Assert.Equal(RequeueResult.Conflict, (await Store.RequeueAsync(failed, Ct)).Result);

        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = 2, finished_at = now() WHERE id = @id",
            ("id", live));
        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(failed, Ct)).Result);
    }

    [Fact]
    public async Task Unknown_job_type_is_rejected_by_add_and_trigger()
    {
        using var args = JsonDocument.Parse("""{"N":1}""");
        await Assert.ThrowsAsync<UnknownJobException>(async () =>
            await _manager.AddOrUpdateAsync("a", "nope", "0 3 * * *", args.RootElement, ct: Ct));

        await _manager.AddOrUpdateAsync("b", "rec.job", "0 3 * * *", args.RootElement, ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET type = 'nope' WHERE id = 'b'");

        await Assert.ThrowsAsync<UnknownJobException>(async () => await _manager.TriggerAsync("b", Ct));
    }

    [Fact]
    public async Task Get_reports_unreadable_catch_up_window_and_trigger_does_not_need_it()   // строка записана SQL'ем
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET catch_up_window = interval '1 month' WHERE id = 'a'");

        var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await _manager.GetAsync("a", Ct));
        Assert.Contains("'a'", e.Message);
        Assert.Contains("catch_up_window", e.Message);
        Assert.Contains("AddOrUpdate", e.Message);
        Assert.True((await _manager.TriggerAsync("a", Ct))!.Value.Created);   // §4.2: окно Trigger'у не нужно

        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);   // исправление из сообщения
        Assert.Equal(TimeSpan.FromDays(1), (await _manager.GetAsync("a", Ct))!.CatchUpWindow);
    }

    [Fact]
    public async Task Get_and_trigger_report_null_retry_delay()             // строка записана SQL'ем
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET retry_delays = ARRAY[1, NULL]::int[] WHERE id = 'a'");

        var get = await Assert.ThrowsAsync<InvalidOperationException>(async () => await _manager.GetAsync("a", Ct));
        var trigger = await Assert.ThrowsAsync<InvalidOperationException>(async () => await _manager.TriggerAsync("a", Ct));
        foreach (var e in new[] { get, trigger })
        {
            Assert.Contains("'a'", e.Message);
            Assert.Contains("retry_delays", e.Message);
            Assert.Contains("AddOrUpdate", e.Message);
        }
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a'"));

        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), new RecurringJobOptions { RetryDelays = [TimeSpan.FromSeconds(3)] }, Ct);
        Assert.Equal([TimeSpan.FromSeconds(3)], (await _manager.GetAsync("a", Ct))!.RetryDelays!);
        Assert.True((await _manager.TriggerAsync("a", Ct))!.Value.Created);
    }

    [Fact]
    public async Task Invalid_definition_is_rejected_before_database()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await _manager.AddOrUpdateAsync("a", "bad", new RecArgs(1), ct: Ct));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs"));
    }

    [Fact]
    public async Task Parallel_add_of_new_id_creates_one_row()
    {
        await Task.WhenAll(Enumerable.Range(0, 8).Select(i => _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(i), ct: Ct).AsTask()));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs"));
    }

    [Fact]
    public async Task Get_returns_normalized_definition()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), new RecurringJobOptions
        {
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow"), Misfire = MisfirePolicy.CatchUp,
            CatchUpWindow = TimeSpan.FromHours(6), RetryDelays = [TimeSpan.FromSeconds(1.5)], MaxAttempts = 2, Priority = 4,
        }, Ct);

        var info = (await _manager.GetAsync("a", Ct))!;

        Assert.Equal(("Europe/Moscow", MisfirePolicy.CatchUp, TimeSpan.FromHours(6), OverlapPolicy.Skip),
            (info.TimeZone, info.Misfire, info.CatchUpWindow, info.Overlap));
        Assert.Equal([TimeSpan.FromSeconds(2)], info.RetryDelays!);
        Assert.Equal(((short?)4, (int?)2, (string?)null), (info.Priority, info.MaxAttempts, info.Queue));
    }
}

public sealed class RecurringApiTests_Direct(PostgresFixture pg) : RecurringApiTests(pg, ConnectionMode.Direct);
public sealed class RecurringApiTests_Bouncer(PostgresFixture pg) : RecurringApiTests(pg, ConnectionMode.Bouncer);
