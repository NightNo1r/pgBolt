using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Dashboard.Data;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Support;

/// <summary>Схема TestDb, core и dashboard из DI (TimeProvider — <see cref="StepTimeProvider"/>), засев и проходы resolver'а.</summary>
public abstract class DashboardTestBase(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private static readonly JsonElement EmptyArgs = JsonDocument.Parse("{}").RootElement;

    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected TestDb Db { get; private set; } = null!;
    protected PgBoltStore Core { get; private set; } = null!;
    protected IJobClient Client { get; private set; } = null!;
    protected ServiceProvider Services { get; private set; } = null!;
    protected StepTimeProvider Time { get; } = new();
    protected JobSeeder Seeder { get; private set; } = null!;

    /// <summary>internal: DashboardStore — internal-тип, а базовый класс public (CS0053).</summary>
    internal DashboardStore Store { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        Db = await TestDb.CreateAsync(pg, mode);
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(Time);
        services.AddPgBolt(o =>
        {
            o.DataSource = Db.DataSource;
            o.Schema = Db.Schema;
            o.AutoMigrate = false;
        });
        services.AddPgBoltDashboard(o => o.ScanBudget = TimeSpan.FromSeconds(2));
        Services = services.BuildServiceProvider();
        Core = Services.GetRequiredService<PgBoltStore>();
        Client = Services.GetRequiredService<IJobClient>();
        Store = Services.GetRequiredService<DashboardStore>();
        Seeder = new JobSeeder(Db, Core);
    }

    public async ValueTask DisposeAsync()
    {
        await Services.DisposeAsync();
        await Db.DisposeAsync();
    }

    protected Task PublishTypeAsync(string name = "dash.job", string queue = "default", string schemaJson = """{"type":"object"}""") =>
        Core.PublishJobTypesAsync([new JobTypeRecord(name, schemaJson, queue, 3, 0, null)], "tests", Ct);

    protected async Task<Guid> EnqueueAsync(string type = "dash.job", string queue = "default", TimeSpan? delay = null,
        short priority = 0, int maxAttempts = 3)
    {
        var result = await Client.EnqueueAsync(type, EmptyArgs,
            new EnqueueOptions { Queue = queue, Delay = delay, Priority = priority, MaxAttempts = maxAttempts }, Ct);
        return result.Id!.Value.Value;
    }

    protected Task<Guid[]> SeedJobsAsync(int count, string type, string queue, short state, string queueRow,
        DateTime? runAtUtc = null, short priority = 0, TimeSpan? runAtStep = null) =>
        Seeder.SeedJobsAsync(count, type, queue, state, queueRow, runAtUtc, priority, runAtStep);

    protected Task<LeasedJob> LeaseAsync(string queue = "default") => Seeder.LeaseAsync(queue);

    protected Task ResolveAllAsync() => ResolverPasses.SettleAsync(Core, cancelMembers: true, Ct);
}
