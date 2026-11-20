using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RecurringTickTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly TimeSpan Threshold = TimeSpan.FromMinutes(1);
    private const string LastMinute = "date_trunc('minute', now()) - interval '1 minute'";

    // Окно с большим запасом (~274 года): курсоры с фиксированной датой не выходят из окна со временем. Вставка идёт SQL'ем
    // в обход API, лимит API на окно здесь не действует.
    private const string WideWindow = "100000 days";

    // Ежегодный cron на месяц, отстоящий от текущего на 6 месяцев: последнее due никогда не попадает в порог Skip (§5.3: last ≥ db_now − T),
    // тест не зависит от того, когда запущен (например, 00:00 UTC 1 января). Часы клиента нужны только для выбора месяца
    // и на курсор не влияют; запас в месяцы покрывает любое расхождение часов клиента и БД.
    private static readonly int FarMonth = (DateTime.UtcNow.Month + 5) % 12 + 1;
    private static readonly string FarYearlyCron = $"0 0 1 {FarMonth} *";
    // Курсор — срабатывание этого cron в далёком прошлом (как 2020-01-01 для «0 0 1 1 *»); тик отбрасывает его как устаревшее.
    private static readonly string FarCursorSql = $"'2020-{FarMonth:D2}-01 00:00:00+00'";
    private static readonly DateTime FarCursor = new(2020, FarMonth, 1, 0, 0, 0, DateTimeKind.Utc);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        RecurringTestSupport.EnsureRegistered();
        await RecurringTestSupport.PublishAsync(Store, Ct);
    }

    private Task<RecurringTickResult> TickAsync(int batchSize = 100, int maxFires = 1000) =>
        Store.TickRecurringAsync(batchSize, maxFires, Threshold, Ct);

    /// <summary>Строка в обход API: курсор и пробуждение — SQL-выражения от now().</summary>
    private Task InsertAsync(string id, string cron = "* * * * *", string next = LastMinute,
        MisfirePolicy misfire = MisfirePolicy.FireOnce, OverlapPolicy overlap = OverlapPolicy.Allow, string window = "1 day",
        string type = "rec.job", string timeZone = "UTC", string? wake = null) =>
        Db.ExecAsync($$"""
            INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
                                                 next_fire_at, wake_at, created_at, updated_at)
            VALUES (@id, @type, '{"N":1}', @cron, @tz, @misfire, interval '{{window}}', @overlap,
                    {{next}}, {{wake ?? next}}, now(), now())
            """, ("id", id), ("type", type), ("cron", cron), ("tz", timeZone), ("misfire", (short)misfire), ("overlap", (short)overlap));

    private Task<bool> RowAsync(string id, string condition) =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.recurring_jobs WHERE id = @id", ("id", id));

    private Task<long> JobsAsync(string id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = @id", ("id", id));

    [Fact]
    public async Task Due_fire_once_row_enqueues_one_job_and_moves_cursor()
    {
        await InsertAsync("a", next: "date_trunc('minute', now()) - interval '3 minutes'");

        var result = await TickAsync();

        Assert.Equal((1, 1, false), (result.Claimed, result.Fired, result.Full));
        Assert.Equal(new[] { "rq" }, result.Queues);
        Assert.Equal(1L, await JobsAsync("a"));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT j.scheduled_at = r.last_fire_at AND j.id = r.last_job_id AND j.queue = 'rq' AND j.priority = 1
                   AND j.unique_key IS NULL AND j.args = '{"N":1}'
                   AND r.next_fire_at > now() AND r.wake_at = r.next_fire_at AND r.last_error IS NULL
                   AND r.last_fire_at >= date_trunc('minute', now()) - interval '1 minute'
            FROM {schema}.recurring_jobs r JOIN {schema}.jobs j ON j.recurring_id = r.id WHERE r.id = 'a'
            """));
    }

    [Fact]
    public async Task Future_and_paused_rows_are_not_claimed()
    {
        await InsertAsync("future", next: "now() + interval '1 hour'");
        await InsertAsync("paused");
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET paused = true WHERE id = 'paused'");

        var result = await TickAsync();

        Assert.Equal(0, result.Claimed);
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Overlap_skip_fire_once_skips_while_previous_is_alive()
    {
        await InsertAsync("a", overlap: OverlapPolicy.Skip);
        Assert.Equal(1, (await TickAsync()).Fired);
        await Db.ExecAsync($"UPDATE {{schema}}.recurring_jobs SET next_fire_at = {LastMinute}, wake_at = now() WHERE id = 'a'");

        var result = await TickAsync();

        Assert.Equal(0, result.Fired);
        Assert.Equal(new[] { "a" }, result.OverlapSkipped);
        Assert.Equal(1L, await JobsAsync("a"));
        Assert.True(await RowAsync("a", "next_fire_at > now()"));
    }

    [Fact]
    public async Task CatchUp_skip_waits_for_running_job_and_keeps_order()   // Review Focus 2
    {
        // фиксированные моменты в прошлом: тест не зависит от границы минуты
        await InsertAsync("a", next: "'2026-01-01 00:00:00+00'", misfire: MisfirePolicy.CatchUp, overlap: OverlapPolicy.Skip,
            window: WideWindow);

        Assert.Equal(1, (await TickAsync()).Fired);
        var blocked = await TickAsync();                                      // первый запуск жив: курсор стоит

        Assert.Equal(new[] { "a" }, blocked.Waiting);
        Assert.True(await RowAsync("a", "next_fire_at = '2026-01-01 00:01:00+00' AND wake_at > now() + interval '4 seconds'"));

        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE unique_key = 'pgbolt:recurring:a'");   // запуск завершился
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'a'");
        Assert.Equal(1, (await TickAsync()).Fired);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT array_agg(scheduled_at ORDER BY id) = ARRAY['2026-01-01 00:00:00+00', '2026-01-01 00:01:00+00']::timestamptz[]
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));                                                                 // UUIDv7: порядок id = порядок постановки
    }

    [Fact]
    public async Task CatchUp_allow_catches_up_in_chunks_of_100()
    {
        await InsertAsync("a", next: "date_trunc('minute', now()) - interval '250 minutes'", misfire: MisfirePolicy.CatchUp);

        var first = await TickAsync();
        Assert.True(await RowAsync("a", "wake_at <= now()"));                  // догон встаёт в общую очередь
        var second = await TickAsync();
        var third = await TickAsync();

        Assert.Equal((100, 100), (first.Fired, second.Fired));
        Assert.InRange(third.Fired, 50, 52);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) = count(DISTINCT scheduled_at)
                   AND max(scheduled_at) - min(scheduled_at) = (count(*) - 1) * interval '1 minute'
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));
    }

    [Fact]
    public async Task Batch_fire_budget_leaves_rest_untouched()
    {
        for (var i = 0; i < 15; i++)
            await InsertAsync($"r{i:D2}", next: "date_trunc('minute', now()) - interval '150 minutes'", misfire: MisfirePolicy.CatchUp,
                wake: $"now() - interval '{100 - i} seconds'");

        var result = await TickAsync();

        Assert.Equal((1000, true), (result.Fired, result.Full));
        Assert.Equal(new[] { "r10", "r11", "r12", "r13", "r14" }, await Db.ScalarAsync<string[]>(   // лимит: курсоры не тронуты
            "SELECT array_agg(id ORDER BY id) FROM {schema}.recurring_jobs WHERE last_fire_at IS NULL"));
    }

    [Fact]
    public async Task Catch_up_rows_do_not_starve_due_rows()                  // §5.3: wake_at = max(next_fire_at, db_now)
    {
        for (var i = 0; i < 15; i++)
            await InsertAsync($"c{i:D2}", cron: "* * * * * *", next: "date_trunc('second', now()) - interval '1 day'",
                misfire: MisfirePolicy.CatchUp, window: "2 days");
        await InsertAsync("normal", wake: "now() - interval '1 second'");

        await TickAsync();                                                     // лимит исчерпан на 10-й догоняющей строке
        Assert.Equal(0L, await JobsAsync("normal"));
        await TickAsync();                                                     // 5 старых догоняющих, затем normal раньше сдвинутых

        Assert.Equal(1L, await JobsAsync("normal"));
    }

    [Fact]
    public async Task Broken_rows_do_not_block_the_batch()                    // Review Focus 1
    {
        await InsertAsync("unknown", type: "nope", wake: "now() - interval '4 seconds'");
        await InsertAsync("tz", timeZone: "Mars/Olympus", wake: "now() - interval '3 seconds'");
        await InsertAsync("cron", cron: "not a cron", wake: "now() - interval '2 seconds'");
        // Данные, которые не читаются в TimeSpan или не проходят проверку EnqueueBatch, — тоже ошибки строки, не откат тика.
        await InsertAsync("window", window: "1 month", wake: "now() - interval '8 seconds'");
        await InsertAsync("window-zero", misfire: MisfirePolicy.CatchUp, window: "0 seconds", wake: "now() - interval '10 seconds'");
        await InsertAsync("window-negative", misfire: MisfirePolicy.CatchUp, window: "-1 hours", wake: "now() - interval '9 seconds'");
        await InsertAsync("", wake: "now() - interval '7 seconds'");
        await InsertAsync("attempts", wake: "now() - interval '6 seconds'");
        await InsertAsync("delays", wake: "now() - interval '5 seconds'");
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET max_attempts = 0 WHERE id = 'attempts'");
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET retry_delays = ARRAY[1, NULL]::int[] WHERE id = 'delays'");
        await InsertAsync("good", wake: "now() - interval '1 second'");

        var result = await TickAsync();

        string[] broken = ["", "attempts", "cron", "delays", "tz", "unknown", "window", "window-negative", "window-zero"];
        Assert.Equal(broken.Order(StringComparer.Ordinal), result.Errors.Select(e => e.Id).Order(StringComparer.Ordinal));
        Assert.Equal(1, result.Fired);
        Assert.Equal(1L, await JobsAsync("good"));
        foreach (var id in broken)
        {
            Assert.True(await RowAsync(id, "last_error IS NOT NULL AND wake_at > now() + interval '50 seconds' " +
                                           "AND wake_at <= now() + interval '1 minute' AND next_fire_at < now()"));
            Assert.Equal(0L, await JobsAsync(id));
        }
        Assert.Contains("nope", result.Errors.Single(e => e.Id == "unknown").Error);
        Assert.Equal(0, (await TickAsync()).Claimed);                           // битые строки спят, хорошая сдвинута
    }

    private Task SetLastErrorAsync(string id) =>
        Db.ExecAsync("UPDATE {schema}.recurring_jobs SET last_error = 'old error' WHERE id = @id", ("id", id));

    [Fact]
    public async Task Successful_fire_clears_last_error()                   // §3, §7: очищается успешным срабатыванием
    {
        await InsertAsync("a");
        await SetLastErrorAsync("a");

        Assert.Equal(1, (await TickAsync()).Fired);

        Assert.True(await RowAsync("a", "last_error IS NULL AND last_fire_at IS NOT NULL"));
    }

    [Theory]                                                                // §7: пропуски в last_error не пишутся и его не стирают
    [InlineData(MisfirePolicy.FireOnce)]
    [InlineData(MisfirePolicy.Skip)]
    public async Task Overlap_skip_keeps_last_error(MisfirePolicy misfire)
    {
        await InsertAsync("a", misfire: misfire, overlap: OverlapPolicy.Skip);
        Assert.Equal(1, (await TickAsync()).Fired);                          // запуск жив: ключ pgbolt:recurring:a занят
        await SetLastErrorAsync("a");
        await Db.ExecAsync($"UPDATE {{schema}}.recurring_jobs SET next_fire_at = {LastMinute}, wake_at = now() WHERE id = 'a'");

        var result = await TickAsync();

        Assert.Equal(new[] { "a" }, result.OverlapSkipped);
        Assert.True(await RowAsync("a", "last_error = 'old error' AND next_fire_at > now()"));   // курсор сдвинут, ошибка цела
    }

    [Fact]
    public async Task Skip_misfire_keeps_last_error()
    {
        await InsertAsync("stale", cron: FarYearlyCron, next: FarCursorSql, misfire: MisfirePolicy.Skip);
        await SetLastErrorAsync("stale");

        Assert.Single((await TickAsync()).Missed);

        Assert.True(await RowAsync("stale", "last_error = 'old error' AND next_fire_at > now()"));
    }

    [Fact]
    public async Task CatchUp_skip_waiting_keeps_last_error()
    {
        await InsertAsync("a", next: "'2026-01-01 00:00:00+00'", misfire: MisfirePolicy.CatchUp, overlap: OverlapPolicy.Skip,
            window: WideWindow);
        Assert.Equal(1, (await TickAsync()).Fired);
        await SetLastErrorAsync("a");
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'a'");

        Assert.Equal(new[] { "a" }, (await TickAsync()).Waiting);

        Assert.True(await RowAsync("a", "last_error = 'old error' AND wake_at > now() + interval '4 seconds'"));
    }

    [Fact]
    public async Task Defaults_come_from_job_types_at_fire_time()
    {
        await InsertAsync("a");
        await Db.ExecAsync("UPDATE {schema}.job_types SET default_queue = 'rq2', default_max_attempts = 7 WHERE name = 'rec.job'");
        await TickAsync();
        Assert.True(await Db.ScalarAsync<bool>("SELECT queue = 'rq2' AND max_attempts = 7 FROM {schema}.jobs WHERE recurring_id = 'a'"));

        await Db.ExecAsync($"UPDATE {{schema}}.recurring_jobs SET queue = 'own', next_fire_at = {LastMinute}, wake_at = now() WHERE id = 'a'");
        await TickAsync();
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a' AND queue = 'own'"));
    }

    [Fact]
    public async Task Skip_misfire_drops_stale_fire_and_reports_it()
    {
        await InsertAsync("stale", cron: FarYearlyCron, next: FarCursorSql, misfire: MisfirePolicy.Skip);
        await InsertAsync("fresh", misfire: MisfirePolicy.Skip);

        var result = await TickAsync();

        Assert.Equal(1, result.Fired);
        Assert.Equal(1L, await JobsAsync("fresh"));
        var missed = Assert.Single(result.Missed);
        Assert.Equal(("stale", FarCursor), (missed.Id, missed.FromUtc));
        Assert.True(await RowAsync("stale", "next_fire_at > now() AND last_fire_at IS NULL"));
    }

    [Fact]
    public async Task Parallel_tickers_create_exactly_one_job_per_second()
    {
        await InsertAsync("a", cron: "* * * * * *", next: "date_trunc('second', now())", misfire: MisfirePolicy.CatchUp);
        var until = DateTime.UtcNow.AddSeconds(10);                         // §10: 8 тикеров, 10 с

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            while (DateTime.UtcNow < until)
            {
                await TickAsync();
                await Task.Delay(50, Ct);
            }
        }, Ct)));

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) >= 9 AND count(*) = count(DISTINCT scheduled_at)
                   AND max(scheduled_at) - min(scheduled_at) = (count(*) - 1) * interval '1 second'
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));
    }

    [Fact]
    public async Task Thousand_rows_due_together_fire_in_batches_of_100()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
                                                 next_fire_at, wake_at, created_at, updated_at)
            SELECT 'r' || g, 'rec.job', '{}', '* * * * *', 'UTC', 0, interval '1 day', 1,
                   date_trunc('minute', now()), date_trunc('minute', now()), now(), now()
            FROM generate_series(1, 1000) g
            """);

        var fired = new List<int>();
        RecurringTickResult r;
        do
        {
            r = await TickAsync();
            fired.Add(r.Fired);
        }
        while (r.Full);

        Assert.All(fired, f => Assert.True(f <= 100));
        Assert.Equal(1000, fired.Sum());
        Assert.Equal(1000L, await Db.ScalarAsync<long>("SELECT count(DISTINCT recurring_id) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Trigger_and_tick_on_same_id_do_not_deadlock_or_duplicate()
    {
        await InsertAsync("a", cron: "* * * * * *", next: "date_trunc('second', now())", overlap: OverlapPolicy.Skip);
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var until = DateTime.UtcNow.AddSeconds(3);

        await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
        {
            while (DateTime.UtcNow < until)
            {
                if (i % 2 == 0)
                    await TickAsync();
                else
                    await manager.TriggerAsync("a", Ct);
                await Db.ExecAsync("DELETE FROM {schema}.queue WHERE unique_key = 'pgbolt:recurring:a' AND random() < 0.3");
            }
        }, Ct)));

        Assert.True(await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE unique_key = 'pgbolt:recurring:a'") <= 1);
    }

    /// <summary>Задач расписания на момент его последнего поставленного срабатывания (last_fire_at).</summary>
    private Task<long> JobsAtLastFireAsync(string id) =>
        Db.ScalarAsync<long>("""
            SELECT count(*) FROM {schema}.jobs j JOIN {schema}.recurring_jobs r ON r.last_fire_at = j.scheduled_at
            WHERE j.recurring_id = @id AND r.id = @id
            """, ("id", id));

    [Fact]
    public async Task Api_cron_change_while_tick_holds_row_fires_once()      // §10: конкуренция API и тика; Review Focus 3
    {
        await InsertAsync("a");                                              // "* * * * *", FireOnce, Allow, курсор в прошлом
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);     // тик поставил срабатывание X, COMMIT не выполнен

        // Новый cron с тем же множеством моментов (каждую минуту в 0 с): X совпадает с его срабатыванием.
        var update = manager.AddOrUpdateAsync("a", "0 * * * * *", new RecArgs(1), new RecurringJobOptions { Overlap = OverlapPolicy.Allow }, Ct)
            .AsTask();
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);        // AddOrUpdate ждёт FOR UPDATE строки
        Assert.False(update.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        await update;

        Assert.True(await RowAsync("a", "cron = '0 * * * * *' AND last_fire_at IS NOT NULL AND last_job_id IS NOT NULL " +
                                        "AND next_fire_at > last_fire_at AND wake_at = next_fire_at"));
        Assert.Equal(1L, await JobsAtLastFireAsync("a"));
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'a'");
        await TickAsync();                                                   // курсор после X: X второй раз не ставится
        Assert.Equal(1L, await Db.ScalarAsync<long>("""
            SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a'
              AND scheduled_at = (SELECT min(scheduled_at) FROM {schema}.jobs WHERE recurring_id = 'a')
            """));
    }

    [Fact]
    public async Task Remove_while_tick_holds_row_lets_tick_finish()        // §5.5, §10
    {
        await InsertAsync("a");
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);

        var remove = manager.RemoveAsync("a", Ct).AsTask();
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);
        Assert.False(remove.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        Assert.True(await remove);

        Assert.Equal(1L, await JobsAsync("a"));                              // задача тика создана
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs WHERE id = 'a'"));
        Assert.Equal(0, (await TickAsync()).Claimed);
    }

    [Fact]
    public async Task Pause_while_tick_holds_row_lets_tick_finish_and_stops_next_ticks()   // §5.5, §10
    {
        await InsertAsync("a");
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);

        var pause = manager.PauseAsync("a", Ct).AsTask();
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);
        Assert.False(pause.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        Assert.True(await pause);

        Assert.Equal(1L, await JobsAsync("a"));
        Assert.True(await RowAsync("a", "paused AND last_fire_at IS NOT NULL AND next_fire_at > last_fire_at"));   // курсор тика сохранён
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() - interval '1 second' WHERE id = 'a'");
        Assert.Equal(0, (await TickAsync()).Claimed);                        // на паузе строку не берут
        Assert.Equal(1L, await JobsAsync("a"));
    }

    /// <summary>
    /// §4.2 Resume, §10: строка на паузе с курсором в прошлом (срабатывания пропущены за время паузы). Тик на паузе строку не
    /// берёт; после ResumeAsync — возвращает результат следующего тика.
    /// </summary>
    private async Task<RecurringTickResult> PauseTickResumeTickAsync(string id)
    {
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        Assert.True(await manager.PauseAsync(id, Ct));

        Assert.Equal(0, (await TickAsync()).Claimed);                        // на паузе строку не берут, хотя wake_at в прошлом
        Assert.Equal(0L, await JobsAsync(id));
        Assert.True(await RowAsync(id, "paused AND wake_at <= now() AND last_fire_at IS NULL"));

        Assert.True(await manager.ResumeAsync(id, Ct));
        return await TickAsync();
    }

    [Fact]
    public async Task Resume_fire_once_fires_latest_time_missed_during_pause()
    {
        await InsertAsync("a", cron: "0 * * * *", next: "date_trunc('hour', now()) - interval '5 hours'");
        var beforeTick = await Db.ScalarAsync<DateTime>("SELECT now()");

        var result = await PauseTickResumeTickAsync("a");

        Assert.Equal(1, result.Fired);                                         // одно срабатывание за 5+ пропущенных часов
        Assert.Empty(result.Missed);
        // курсор = after(db_now) > db_now ≥ beforeTick; поставлено last_fire_at = курсор − 1 ч — последнее из due
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT NOT paused AND next_fire_at > @t AND next_fire_at = last_fire_at + interval '1 hour' AND wake_at = next_fire_at
                   AND last_fire_at = (SELECT scheduled_at FROM {schema}.jobs WHERE recurring_id = 'a')
            FROM {schema}.recurring_jobs WHERE id = 'a'
            """, ("t", beforeTick)));
    }

    [Fact]
    public async Task Resume_skip_drops_times_missed_during_pause()
    {
        await InsertAsync("a", cron: FarYearlyCron, next: FarCursorSql, misfire: MisfirePolicy.Skip);

        var result = await PauseTickResumeTickAsync("a");

        Assert.Equal(0, result.Fired);
        var missed = Assert.Single(result.Missed);                             // Warning §7: отброшено с курсора паузы
        Assert.Equal(("a", FarCursor), (missed.Id, missed.FromUtc));
        Assert.Equal(0L, await JobsAsync("a"));
        Assert.True(await RowAsync("a", "NOT paused AND next_fire_at > now() AND wake_at = next_fire_at AND last_fire_at IS NULL"));
    }

    [Fact]
    public async Task Resume_catch_up_catches_up_from_cursor_of_pause()
    {
        // окно накрывает всю паузу: догон с курсора паузы, по 100 срабатываний за тик
        await InsertAsync("a", cron: "0 * * * *", next: "'2026-01-01 00:00:00+00'", misfire: MisfirePolicy.CatchUp, window: WideWindow);

        var result = await PauseTickResumeTickAsync("a");

        Assert.Equal(100, result.Fired);
        Assert.Empty(result.Missed);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) = 100 AND count(DISTINCT scheduled_at) = 100
                   AND min(scheduled_at) = '2026-01-01 00:00:00+00' AND max(scheduled_at) = '2026-01-05 03:00:00+00'
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));
        Assert.True(await RowAsync("a", "NOT paused AND next_fire_at = '2026-01-05 04:00:00+00' AND wake_at <= now() " +
                                        "AND last_fire_at = '2026-01-05 03:00:00+00'"));   // остаток догоняют следующие тики
    }

    [Fact]
    public async Task Resume_catch_up_drops_times_older_than_window()
    {
        await InsertAsync("a", cron: "0 * * * *", next: "'2026-01-01 00:00:00+00'", misfire: MisfirePolicy.CatchUp, window: "3 hours");

        var beforeTick = await Db.ScalarAsync<DateTime>("SELECT now()");

        var result = await PauseTickResumeTickAsync("a");

        var missed = Assert.Single(result.Missed);                             // пропуск старше окна — Warning §7
        Assert.Equal(("a", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)), (missed.Id, missed.FromUtc));
        Assert.InRange(result.Fired, 3, 4);                                    // ежечасные срабатывания в [db_now − 3 ч, db_now]
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT min(scheduled_at) >= now() - interval '3 hours 1 minute' AND count(DISTINCT scheduled_at) = count(*)
                   AND max(scheduled_at) = (SELECT last_fire_at FROM {schema}.recurring_jobs WHERE id = 'a')
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT NOT paused AND next_fire_at = last_fire_at + interval '1 hour' AND next_fire_at > @t FROM {schema}.recurring_jobs WHERE id = 'a'",
            ("t", beforeTick)));
    }

    /// <summary>
    /// §4.3, §5.2, §10: code-first строка типа, которого ещё нет в job_types (исполнитель не опубликовал). Тик — ошибка строки
    /// (last_error, wake_at ≈ now() + 1 мин, курсор стоит); после публикации типа и истечения backoff — результат следующего тика.
    /// </summary>
    private async Task<(RecurringTickResult Result, DateTime Cursor)> UnknownCodeFirstTypeThenPublishedAsync(string cron, string next, MisfirePolicy misfire,
        TimeSpan? window = null)
    {
        var options = new RecurringJobOptions { Misfire = misfire, Overlap = OverlapPolicy.Allow, CatchUpWindow = window ?? TimeSpan.FromDays(1) };
        var sync = await Store.SyncRecurringAsync("svc",
            [RecurringDefinition.Create("cf", "late.job", """{"N":1}""", cron, options)], NullLogger.Instance, Ct);
        Assert.Equal(new[] { "cf" }, sync.Inserted);                           // синхронизация тип по job_types не проверяет
        await Db.ExecAsync($"UPDATE {{schema}}.recurring_jobs SET next_fire_at = {next}, wake_at = now() - interval '1 second' WHERE id = 'cf'");
        var cursor = await Db.ScalarAsync<DateTime>("SELECT next_fire_at FROM {schema}.recurring_jobs WHERE id = 'cf'");

        var failed = await TickAsync();

        var error = Assert.Single(failed.Errors);
        Assert.Equal("cf", error.Id);
        Assert.Contains(nameof(UnknownJobException), error.Error);
        Assert.Contains("late.job", error.Error);
        Assert.Equal(0, failed.Fired);
        Assert.Equal(0L, await JobsAsync("cf"));
        Assert.True(await RowAsync("cf", "last_error LIKE '%late.job%' AND wake_at > now() + interval '50 seconds' " +
                                         "AND wake_at <= now() + interval '1 minute' AND last_fire_at IS NULL"));
        Assert.Equal(cursor, await Db.ScalarAsync<DateTime>("SELECT next_fire_at FROM {schema}.recurring_jobs WHERE id = 'cf'"));
        Assert.Equal(0, (await TickAsync()).Claimed);                           // до истечения backoff строку не берут

        await Store.PublishJobTypesAsync([new JobTypeRecord("late.job", "{}", "rq", 3, 1, null)], "executor", Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'cf'");   // backoff истёк
        return (await TickAsync(), cursor);
    }

    [Fact]
    public async Task Code_first_unknown_type_fires_once_after_type_is_published()
    {
        var beforeTicks = await Db.ScalarAsync<DateTime>("SELECT now()");
        var (result, _) = await UnknownCodeFirstTypeThenPublishedAsync("0 * * * *", "date_trunc('hour', now()) - interval '5 hours'",
            MisfirePolicy.FireOnce);

        Assert.Equal((1, 0), (result.Fired, result.Errors.Count));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT last_error IS NULL AND next_fire_at > @t AND next_fire_at = last_fire_at + interval '1 hour'
                   AND last_fire_at = (SELECT scheduled_at FROM {schema}.jobs WHERE recurring_id = 'cf' AND type = 'late.job' AND queue = 'rq')
            FROM {schema}.recurring_jobs WHERE id = 'cf'
            """, ("t", beforeTicks)));                                         // последнее из due; курсор = after(db_now)
    }

    [Fact]
    public async Task Code_first_unknown_type_catches_up_from_kept_cursor_after_type_is_published()
    {
        var (result, cursor) = await UnknownCodeFirstTypeThenPublishedAsync("0 * * * *", "date_trunc('hour', now()) - interval '200 days'",
            MisfirePolicy.CatchUp, TimeSpan.FromDays(366));

        Assert.Equal((100, 0), (result.Fired, result.Errors.Count));            // курсор не сдвинут ошибкой: догон с него
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) = 100 AND min(scheduled_at) = @c AND max(scheduled_at) = @c + interval '99 hours'
            FROM {schema}.jobs WHERE recurring_id = 'cf'
            """, ("c", cursor)));
        Assert.True(await RowAsync("cf", "last_error IS NULL AND next_fire_at = last_fire_at + interval '1 hour' AND next_fire_at < now()"));
    }

    [Fact]
    public async Task Code_first_unknown_type_skip_drops_stale_time_after_type_is_published()
    {
        var (result, _) = await UnknownCodeFirstTypeThenPublishedAsync(FarYearlyCron, FarCursorSql, MisfirePolicy.Skip);

        Assert.Equal((0, 0), (result.Fired, result.Errors.Count));
        Assert.Equal("cf", Assert.Single(result.Missed).Id);
        Assert.Equal(0L, await JobsAsync("cf"));
        Assert.True(await RowAsync("cf", "next_fire_at > now() AND wake_at = next_fire_at AND last_fire_at IS NULL"));
    }
}

public sealed class RecurringTickTests_Direct(PostgresFixture pg) : RecurringTickTests(pg, ConnectionMode.Direct);
public sealed class RecurringTickTests_Bouncer(PostgresFixture pg) : RecurringTickTests(pg, ConnectionMode.Bouncer);
