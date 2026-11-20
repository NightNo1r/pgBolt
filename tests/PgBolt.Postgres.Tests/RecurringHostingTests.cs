using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres.Hosting;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed record UnregisteredRecurringArgs(int N);

internal sealed class TestHostEnvironment(string applicationName) : IHostEnvironment
{
    public string EnvironmentName { get; set; } = "Test";
    public string ApplicationName { get; set; } = applicationName;
    public string ContentRootPath { get; set; } = "/";
    public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
}

public abstract class RecurringHostingTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static JsonElement Args => JsonDocument.Parse("""{"N":1}""").RootElement.Clone();

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private ServiceProvider Build(Action<PgBoltOptions> configure, IHostEnvironment? environment = null, ILogger<PgBoltInitializer>? logger = null)
    {
        var services = new ServiceCollection();
        if (environment is not null)
            services.AddSingleton(environment);
        if (logger is not null)
            services.AddSingleton(logger);
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            configure(o);
        });
        return services.BuildServiceProvider();
    }

    private static Task StartAsync(ServiceProvider sp) => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

    private Task<string[]> IdsAsync(string owner) =>
        _db.ScalarAsync<string[]>("SELECT coalesce(array_agg(id ORDER BY id), '{}') FROM {schema}.recurring_jobs WHERE owner = @o", ("o", owner));

    [Fact]
    public async Task Start_syncs_declarations_before_ready_without_job_types()
    {
        var log = new ListLogger<PgBoltInitializer>();
        await using var sp = Build(o =>
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("h.daily", "hosting.job", "0 3 * * *", Args);   // тип ещё не опубликован исполнителем — старт не падает
        }, logger: log);

        await StartAsync(sp);

        Assert.Equal(new[] { "h.daily" }, await IdsAsync("svc"));
        Assert.True(await _db.ScalarAsync<bool>(
            """SELECT type = 'hosting.job' AND args = '{"N":1}' AND next_fire_at > now() FROM {schema}.recurring_jobs WHERE id = 'h.daily'"""));
        Assert.Contains(log.Entries, e => e.Level == LogLevel.Information && e.Message.Contains("h.daily") && e.Message.Contains("inserted"));
        Assert.NotNull(sp.GetRequiredService<IRecurringJobManager>());
    }

    [Fact]
    public async Task Typed_declaration_uses_job_metadata()
    {
        RecurringTestSupport.EnsureRegistered();
        await using var sp = Build(o =>
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("typed", "0 3 * * *", new RecArgs(5), new RecurringJobOptions { Overlap = OverlapPolicy.Allow });
        });

        await StartAsync(sp);

        Assert.True(await _db.ScalarAsync<bool>(
            """SELECT type = 'rec.job' AND args = '{"N":5}' AND overlap = 1 FROM {schema}.recurring_jobs WHERE id = 'typed'"""));
    }

    [Fact]
    public async Task Default_owner_is_application_name()
    {
        await using var sp = Build(o => o.AddRecurring("a", "hosting.job", "0 3 * * *", Args), new TestHostEnvironment("orders"));
        await StartAsync(sp);
        Assert.Equal(new[] { "a" }, await IdsAsync("orders"));
    }

    [Fact]
    public async Task Missing_owner_fails_start()
    {
        await using var sp = Build(o => o.AddRecurring("a", "hosting.job", "0 3 * * *", Args));
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => StartAsync(sp));
        Assert.Contains("RecurringOwner", e.Message);
    }

    [Fact]
    public async Task Unregistered_typed_args_fail_start()
    {
        await using var sp = Build(o => { o.RecurringOwner = "svc"; o.AddRecurring("a", "0 3 * * *", new UnregisteredRecurringArgs(1)); });
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => StartAsync(sp));
        Assert.Contains("No PgBolt metadata", e.Message);
    }

    [Fact]
    public async Task Clear_deletes_owner_rows_and_absent_declarations_do_not()
    {
        await using (var first = Build(o => { o.RecurringOwner = "svc"; o.AddRecurring("a", "hosting.job", "0 3 * * *", Args); }))
            await StartAsync(first);

        await using (var api = Build(o => o.RecurringOwner = "svc"))           // api-процесс того же владельца без объявлений
            await StartAsync(api);
        Assert.Equal(new[] { "a" }, await IdsAsync("svc"));

        await using (var cleared = Build(o => { o.RecurringOwner = "svc"; o.ClearRecurring(); }))
            await StartAsync(cleared);
        Assert.Empty(await IdsAsync("svc"));
    }

    [Fact]
    public void Invalid_declarations_are_rejected_at_registration()
    {
        Assert.Throws<ArgumentException>(() => Build(o => o.AddRecurring("a", "hosting.job", "bad cron", Args)));
        Assert.Throws<InvalidOperationException>(() => Build(o =>
        {
            o.AddRecurring("a", "hosting.job", "0 3 * * *", Args);
            o.ClearRecurring();
        }));
        Assert.Throws<InvalidOperationException>(() => Build(o =>
        {
            o.AddRecurring("a", "hosting.job", "0 3 * * *", Args);
            o.AddRecurring("a", "hosting.job", "0 4 * * *", Args);
        }));
        Assert.Throws<InvalidOperationException>(() => Build(o => o.RecurringOwner = ""));
    }

    private static IEnumerable<LogEntry> SyncEntries(ListLogger<PgBoltInitializer> log) =>
        log.Entries.Where(e => e.Message.Contains("PgBolt: recurring job"));

    private static bool Logged(ListLogger<PgBoltInitializer> log, LogLevel level, string id, string what) =>
        SyncEntries(log).Any(e => e.Level == level && e.Message.Contains($"recurring job {id} ") && e.Message.Contains(what));

    [Fact]
    public async Task Alternating_sets_of_one_owner_converge_to_the_last_one()   // §6.2 rolling deploy, §10
    {
        static void Old(PgBoltOptions o)
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("keep", "hosting.job", "0 3 * * *", Args);
            o.AddRecurring("old.only", "hosting.job", "0 3 * * *", Args);
        }
        static void New(PgBoltOptions o)
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("keep", "hosting.job", "0 4 * * *", Args);
            o.AddRecurring("new.only", "hosting.job", "0 4 * * *", Args);
        }

        async Task<ListLogger<PgBoltInitializer>> StartPodAsync(Action<PgBoltOptions> set)
        {
            var log = new ListLogger<PgBoltInitializer>();
            await using var sp = Build(set, logger: log);
            await StartAsync(sp);
            return log;
        }

        await StartPodAsync(Old);                                                // до выкатки
        for (var i = 0; i < 2; i++)                                              // выкатка: старые и новые поды чередуются
        {
            var toNew = await StartPodAsync(New);
            Assert.True(Logged(toNew, LogLevel.Information, "keep", "updated (cron)"));
            Assert.True(Logged(toNew, LogLevel.Information, "new.only", "inserted"));
            Assert.True(Logged(toNew, LogLevel.Information, "old.only", "deleted"));
            Assert.Equal(new[] { "keep", "new.only" }, await IdsAsync("svc"));

            var toOld = await StartPodAsync(Old);
            Assert.True(Logged(toOld, LogLevel.Information, "keep", "updated (cron)"));
            Assert.True(Logged(toOld, LogLevel.Information, "old.only", "inserted"));
            Assert.True(Logged(toOld, LogLevel.Information, "new.only", "deleted"));
            Assert.Equal(new[] { "keep", "old.only" }, await IdsAsync("svc"));
            Assert.DoesNotContain(SyncEntries(toOld), e => e.Level >= LogLevel.Warning);
        }

        var last = await StartPodAsync(New);                                     // старые поды ушли
        Assert.Equal(3, SyncEntries(last).Count());
        var settled = await StartPodAsync(New);                                  // дальше — проходы без разницы

        Assert.Empty(SyncEntries(settled));
        Assert.Equal(new[] { "keep", "new.only" }, await IdsAsync("svc"));
        Assert.True(await _db.ScalarAsync<bool>(
            "SELECT bool_and(cron = '0 4 * * *') FROM {schema}.recurring_jobs WHERE owner = 'svc'"));
    }

    [Fact]
    public async Task Api_insert_of_same_id_during_start_sync_is_a_logged_collision()   // §6.2, §10
    {
        RecurringTestSupport.EnsureRegistered();
        await using (var before = Build(o => { o.RecurringOwner = "svc"; o.AddRecurring("a", "hosting.job", "0 3 * * *", Args); }))
            await StartAsync(before);

        // Смена cron "a" требует FOR UPDATE строки: держим её, чтобы синхронизация встала между чтением и вставкой "x".
        await using var holder = await _db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await holder.BeginTransactionAsync(Ct);
        await using (var lockRow = new NpgsqlCommand(_db.Sql("SELECT 1 FROM {schema}.recurring_jobs WHERE id = 'a' FOR UPDATE"), holder, tx))
            await lockRow.ExecuteNonQueryAsync(Ct);

        var log = new ListLogger<PgBoltInitializer>();
        await using var sp = Build(o =>
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("a", "hosting.job", "0 4 * * *", Args);
            o.AddRecurring("x", "hosting.job", "0 5 * * *", Args);
        }, logger: log);
        var start = StartAsync(sp);
        await RecurringTestSupport.WaitForRowLockWaiterAsync(_db, Ct);           // синхронизация прочитала набор, "x" ещё не вставлен

        // Параллельный AddOrUpdate того же id через API: строки нет — вставка owner = NULL.
        await sp.GetRequiredService<IRecurringJobManager>().AddOrUpdateAsync("x", "0 6 * * *", new RecArgs(7), ct: Ct);
        Assert.False(start.IsCompleted);
        await tx.CommitAsync(Ct);

        await start;                                                             // старт не падает

        Assert.True(Logged(log, LogLevel.Error, "x", "belongs to another owner or was created through the API"));
        Assert.False(Logged(log, LogLevel.Information, "x", "inserted"));
        Assert.True(Logged(log, LogLevel.Information, "a", "updated (cron)"));
        Assert.True(await _db.ScalarAsync<bool>(
            """SELECT owner IS NULL AND cron = '0 6 * * *' AND args = '{"N":7}' FROM {schema}.recurring_jobs WHERE id = 'x'"""));
        Assert.Equal(new[] { "a" }, await IdsAsync("svc"));                     // каждый id — у одного владельца
        Assert.Equal(2L, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs"));
    }

    [Fact]
    public async Task Default_owner_without_declarations_does_not_touch_its_rows()   // §4.3, §6.1, §10
    {
        await using (var worker = Build(o => o.AddRecurring("a", "hosting.job", "0 3 * * *", Args), new TestHostEnvironment("orders")))
        {
            await StartAsync(worker);
            Assert.True(await worker.GetRequiredService<IRecurringJobManager>().PauseAsync("a", Ct));
        }
        await _db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = '2099-01-01', wake_at = '2099-01-01' WHERE id = 'a'");
        var updatedAt = await _db.ScalarAsync<DateTime>("SELECT updated_at FROM {schema}.recurring_jobs WHERE id = 'a'");

        var log = new ListLogger<PgBoltInitializer>();
        await using (var api = Build(_ => { }, new TestHostEnvironment("orders"), log))   // тот же ApplicationName, без объявлений
            await StartAsync(api);

        Assert.Equal(new[] { "a" }, await IdsAsync("orders"));
        Assert.True(await _db.ScalarAsync<bool>("""
            SELECT paused AND next_fire_at = '2099-01-01' AND wake_at = '2099-01-01' AND cron = '0 3 * * *' AND updated_at = @u
            FROM {schema}.recurring_jobs WHERE id = 'a'
            """, ("u", updatedAt)));
        Assert.Empty(SyncEntries(log));
    }
}

public sealed class RecurringHostingTests_Direct(PostgresFixture pg) : RecurringHostingTests(pg, ConnectionMode.Direct);
public sealed class RecurringHostingTests_Bouncer(PostgresFixture pg) : RecurringHostingTests(pg, ConnectionMode.Bouncer);
