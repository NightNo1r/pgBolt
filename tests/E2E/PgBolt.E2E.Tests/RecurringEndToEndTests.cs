using Microsoft.Extensions.DependencyInjection;
using PgBolt.E2E.Contracts;
using PgBolt.E2E.JobsA;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

public abstract class RecurringEndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Typed_code_first_schedule_reaches_executor_with_schedule_fields()
    {
        // Метка прогона: PingProbe.Messages — статическая очередь, общая для _Direct/_Bouncer и EndToEndTests.
        var marker = $"scheduled-{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register);
            o.RecurringOwner = "e2e";
            o.AddRecurring("e2e.ping.daily", "0 3 * * *", new PingArgs(marker));
        });
        await using var sp = services.BuildServiceProvider();
        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);
        var store = sp.GetRequiredService<PgBoltStore>();
        var registry = sp.GetRequiredService<JobRegistry>();
        await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), "e2e-server", Ct);
        await _db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = '2026-01-01 03:00:00+00', wake_at = now()");

        Assert.Equal(1, (await store.TickRecurringAsync(100, 1000, TimeSpan.FromMinutes(1), Ct)).Fired);

        var job = Assert.Single(await store.DequeueAsync("e2e", 10, Guid.NewGuid(), TimeSpan.FromMinutes(1), Ct));
        Assert.Equal("e2e.ping.daily", job.RecurringId);
        Assert.Equal(await _db.ScalarAsync<DateTime>("SELECT last_fire_at FROM {schema}.recurring_jobs"), job.ScheduledAtUtc);
        var info = await sp.GetRequiredService<IRecurringJobManager>().GetAsync("e2e.ping.daily", Ct);   // §10: GetAsync в E2E
        Assert.NotNull(info);
        Assert.Equal(new DateTimeOffset(job.ScheduledAtUtc!.Value, TimeSpan.Zero), info.LastFireAt);
        Assert.Equal(new JobId(job.Id), info.LastJobId);
        Assert.Equal("e2e", info.Owner);
        Assert.DoesNotContain(marker, PingProbe.Messages);
        await E2EProcessing.InvokeAsync(sp, job, Ct);
        Assert.Contains(marker, PingProbe.Messages);
    }
}

public sealed class RecurringEndToEndTests_Direct(PostgresFixture pg) : RecurringEndToEndTests(pg, ConnectionMode.Direct);
public sealed class RecurringEndToEndTests_Bouncer(PostgresFixture pg) : RecurringEndToEndTests(pg, ConnectionMode.Bouncer);
