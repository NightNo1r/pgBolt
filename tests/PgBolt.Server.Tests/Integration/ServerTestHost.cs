using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres;
using PgBolt.Server.Runtime;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Коды jobs.state.</summary>
public static class DbState
{
    public const short Pending = 0;
    public const short Succeeded = 2;
    public const short Failed = 3;
    public const short Cancelled = 4;
}

public sealed record QueueRow(int Attempt, Guid? LeaseOwner, Guid? LeaseId, DateTime? LeaseUntil, bool Ready, bool CancelRequested);

public sealed record EventRow(JobEventKind Kind, int? Attempt, Guid? ServerId, string? Message);

public sealed class TestHostSettings
{
    internal List<(string Name, Action<PgBoltServerOptions> Configure)> Servers { get; } = [];

    /// <summary>Имя хоста — попадает в JobRun.Host.</summary>
    public string Name { get; set; } = "host";

    /// <summary>
    /// Подключение runtime через прокси. Cancellation Timeout = -1: после попытки CancelRequest Npgsql рвёт соединение,
    /// не дожидаясь ответа; на паузе прокси сбрасывает соединение CancelRequest сразу, так что отменённый запрос рвёт
    /// соединение немедленно (см. <see cref="TcpProxy"/>).
    /// </summary>
    public TcpProxy? Proxy { get; set; }

    /// <summary>Пользовательский источник данных (o.DataSource); регистрируется и в DI — для PoolHogJob. Важнее Proxy.</summary>
    public NpgsqlDataSource? DataSource { get; set; }

    public Action<IPgBoltJobsBuilder> Jobs { get; set; } = TestJobs.Register;

    public bool RunMaintenance { get; set; } = true;

    /// <summary>PgBoltProcessingOptions.RunScheduler (часть 4); сценарии без фонового трафика выключают его вместе с RunMaintenance.</summary>
    public bool RunScheduler { get; set; } = true;

    /// <summary>Дополнительная настройка core (после Schema и AddJobs).</summary>
    public Action<PgBoltOptions>? Core { get; set; }

    /// <summary>Дополнительная настройка обработки (после RunMaintenance и интервалов promoter/sweeper).</summary>
    public Action<PgBoltProcessingOptions>? Processing { get; set; }

    /// <summary>Применяется к каждому серверу после Fast и до его собственной настройки. Очереди здесь не добавлять.</summary>
    public Action<PgBoltServerOptions>? Configure { get; set; }

    /// <summary>Виртуальный сервер; без вызова — один сервер "s1" с очередью TestJobs.Queue.</summary>
    public TestHostSettings AddServer(string name, Action<PgBoltServerOptions> configure)
    {
        Servers.Add((name, configure));
        return this;
    }
}

/// <summary>
/// «Процесс» с PgBolt-сервером: свой ServiceProvider, hosted services, пул соединений и IWakeupSignal. Несколько хостов
/// в одном тесте работают на одной схеме как разные процессы.
/// </summary>
public sealed class ServerTestHost : IAsyncDisposable
{
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(3);
    public static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(2);

    private readonly ServiceProvider _provider;
    private readonly List<IHostedService> _started = [];
    private bool _stopped;

    private ServerTestHost(string name, ServiceProvider provider, ListLogger logs)
    {
        Name = name;
        _provider = provider;
        Logs = logs;
        HostedServices = provider.GetServices<IHostedService>().ToList();
        Processing = HostedServices.OfType<PgBoltProcessingService>().Single();
    }

    public string Name { get; }
    public IServiceProvider Services => _provider;
    public IJobClient Client => _provider.GetRequiredService<IJobClient>();
    public ListLogger Logs { get; }
    public IReadOnlyList<IHostedService> HostedServices { get; }
    internal PgBoltProcessingService Processing { get; }

    /// <summary>server_id виртуальных серверов (после StartAsync).</summary>
    public IReadOnlyList<Guid> ServerIds => Processing.Servers.Select(s => s.Id).ToList();

    public Guid ServerId => Assert.Single(ServerIds);

    public static void Fast(PgBoltServerOptions s)
    {
        s.Workers = 8;
        s.LeaseDuration = LeaseDuration;
        s.HeartbeatInterval = HeartbeatInterval;
        s.ShutdownTimeout = ShutdownTimeout;
        s.AckFlushInterval = TimeSpan.FromMilliseconds(10);
        s.RetryPolicy = RetryPolicy.Exponential(TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1), 0);
    }

    public static void FastQueue(QueueOptions q) => q.MaxPollInterval = TimeSpan.FromMilliseconds(200);

    /// <summary>L = 30 с, H = 10 с: heartbeat гарантированно не срабатывает в окне сценария.</summary>
    public static void SlowHeartbeat(PgBoltServerOptions s)
    {
        s.LeaseDuration = TimeSpan.FromSeconds(30);
        s.HeartbeatInterval = TimeSpan.FromSeconds(10);
    }

    public static async Task<ServerTestHost> StartAsync(TestDb db, JobProbe probe, Action<TestHostSettings>? configure,
        CancellationToken ct)
    {
        var settings = new TestHostSettings();
        configure?.Invoke(settings);
        if (settings.Servers.Count == 0)
            settings.AddServer("s1", s => s.Queue(TestJobs.Queue, FastQueue));

        var logs = new ListLogger();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Information).AddProvider(logs));
        services.AddSingleton(probe);
        services.AddSingleton(new HostTag(settings.Name));
        if (settings.DataSource is { } dataSource)
            services.AddSingleton(dataSource);

        var connectionString = settings.Proxy is { } proxy
            ? new NpgsqlConnectionStringBuilder(proxy.ConnectionStringVia(db.ConnectionString)) { CancellationTimeout = -1 }.ConnectionString
            : db.ConnectionString;
        services.AddPgBolt(o =>
        {
            if (settings.DataSource is { } source)
                o.DataSource = source;
            else
                o.ConnectionString = connectionString;
            o.Schema = db.Schema;
            o.AddJobs(settings.Jobs);
            settings.Core?.Invoke(o);
        });
        foreach (var (name, configureServer) in settings.Servers)
            services.AddPgBoltServer(name, s =>
            {
                Fast(s);
                settings.Configure?.Invoke(s);
                configureServer(s);
            });
        services.ConfigurePgBoltProcessing(p =>
        {
            p.RunMaintenance = settings.RunMaintenance;
            p.RunScheduler = settings.RunScheduler;
            p.PromoterInterval = TimeSpan.FromMilliseconds(200);
            p.SweeperInterval = TimeSpan.FromMilliseconds(500);
            settings.Processing?.Invoke(p);
        });

        var host = new ServerTestHost(settings.Name, services.BuildServiceProvider(), logs);
        try
        {
            foreach (var hosted in host.HostedServices)
            {
                await hosted.StartAsync(ct);
                host._started.Add(hosted);
            }
        }
        catch
        {
            await host.DisposeAsync();
            throw;
        }
        return host;
    }

    /// <summary>Останавливает hosted services в обратном порядке (как хост). Идемпотентно. Возвращает длительность.</summary>
    public async Task<TimeSpan> StopAsync()
    {
        if (_stopped)
            return TimeSpan.Zero;
        _stopped = true;
        var clock = Stopwatch.StartNew();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        for (var i = _started.Count - 1; i >= 0; i--)
            await _started[i].StopAsync(deadline.Token);
        return clock.Elapsed;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _provider.DisposeAsync();
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TimingSensitiveCollection
{
    public const string Name = "timing-sensitive";
}

public abstract class ServerIntegrationTestBase(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private readonly List<ServerTestHost> _hosts = [];
    private readonly List<TcpProxy> _proxies = [];
    private readonly List<IAsyncDisposable> _tracked = [];
    private ServiceProvider _clientServices = null!;

    /// <summary>retry_delays = [0]: повтор сразу (ready = true), без promoter.</summary>
    protected static readonly EnqueueOptions ImmediateRetry = new() { RetryDelays = [TimeSpan.Zero] };

    protected PostgresFixture Pg { get; } = pg;
    protected ConnectionMode Mode { get; } = mode;
    protected TestDb Db { get; private set; } = null!;
    protected JobProbe Probe { get; } = new();
    /// <summary>Клиент без сервера, в обход прокси: enqueue работает, даже когда прокси хоста на паузе.</summary>
    protected IJobClient Client { get; private set; } = null!;
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    public virtual async ValueTask InitializeAsync()
    {
        Db = await TestDb.CreateAsync(Pg, Mode);
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = Db.ConnectionString;
            o.Schema = Db.Schema;
            o.AddJobs(TestJobs.RegisterWithOrphan);
        });
        _clientServices = services.BuildServiceProvider();
        Client = _clientServices.GetRequiredService<IJobClient>();
    }

    public virtual async ValueTask DisposeAsync()
    {
        Probe.ReleaseAll();
        foreach (var proxy in _proxies)
            proxy.Resume();
        for (var i = _hosts.Count - 1; i >= 0; i--)
            await _hosts[i].DisposeAsync();
        foreach (var tracked in _tracked)
            await tracked.DisposeAsync();
        foreach (var proxy in _proxies)
            await proxy.DisposeAsync();
        await _clientServices.DisposeAsync();
        await Db.DisposeAsync();
    }

    protected async Task<ServerTestHost> StartHostAsync(Action<TestHostSettings>? configure = null)
    {
        var host = await ServerTestHost.StartAsync(Db, Probe, configure, Ct);
        _hosts.Add(host);
        return host;
    }

    /// <summary>Прокси к адресу текущего режима (PostgreSQL или PgBouncer).</summary>
    protected TcpProxy StartProxy()
    {
        var proxy = TcpProxy.For(Pg.ConnectionString(Mode));
        _proxies.Add(proxy);
        return proxy;
    }

    /// <summary>Освобождается после остановки хостов (например, NpgsqlDataSource, отданный хосту).</summary>
    protected T Track<T>(T disposable) where T : IAsyncDisposable
    {
        _tracked.Add(disposable);
        return disposable;
    }

    protected async Task<JobId> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, IJobClient? client = null)
    {
        var result = await (client ?? Client).EnqueueAsync(args, options, Ct);
        Assert.True(result.Created);
        return result.Id ?? throw new InvalidOperationException("Created job without id.");
    }

    protected async Task<JobId> EnqueueByNameAsync(string jobName, string argsJson, EnqueueOptions? options = null)
    {
        using var doc = JsonDocument.Parse(argsJson);
        var result = await Client.EnqueueAsync(jobName, doc.RootElement, options, Ct);
        Assert.True(result.Created);
        return result.Id ?? throw new InvalidOperationException("Created job without id.");
    }

    protected static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string because)
    {
        var started = Stopwatch.GetTimestamp();
        while (!await condition())
        {
            if (Stopwatch.GetElapsedTime(started) > timeout)
                Assert.Fail($"Timed out after {timeout.TotalSeconds:0.##} s waiting for: {because}");
            await Task.Delay(25, Ct);
        }
    }

    protected static Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string because) =>
        WaitUntilAsync(() => Task.FromResult(condition()), timeout, because);

    protected async Task WaitForStateAsync(JobId id, short state, TimeSpan timeout)
    {
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            var current = await Db.ScalarAsync<short?>("SELECT state FROM {schema}.jobs WHERE id = @id", ("id", id.Value));
            if (current == state)
                return;
            if (Stopwatch.GetElapsedTime(started) > timeout)
                Assert.Fail($"Job {id} did not reach state {state} within {timeout.TotalSeconds:0.##} s (last: {current?.ToString() ?? "no row"})");
            await Task.Delay(25, Ct);
        }
    }

    protected async Task<short> JobStateAsync(JobId id) =>
        await Db.ScalarAsync<short?>("SELECT state FROM {schema}.jobs WHERE id = @id", ("id", id.Value))
        ?? throw new InvalidOperationException($"No jobs row for {id}.");

    protected async Task<int> AttemptAsync(JobId id) =>
        await Db.ScalarAsync<int?>("SELECT attempt FROM {schema}.jobs WHERE id = @id", ("id", id.Value))
        ?? throw new InvalidOperationException($"No jobs row for {id}.");

    protected Task<Guid?> JobServerIdAsync(JobId id) =>
        Db.ScalarAsync<Guid?>("SELECT server_id FROM {schema}.jobs WHERE id = @id", ("id", id.Value));

    protected async Task<QueueRow?> QueueRowAsync(JobId id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql(
            "SELECT attempt, lease_owner, lease_id, lease_until, ready, cancel_requested FROM {schema}.queue WHERE job_id = @id"));
        cmd.Parameters.AddWithValue("id", id.Value);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        if (!await r.ReadAsync(Ct))
            return null;
        return new QueueRow(
            r.GetInt32(0),
            r.IsDBNull(1) ? null : r.GetGuid(1),
            r.IsDBNull(2) ? null : r.GetGuid(2),
            r.IsDBNull(3) ? null : r.GetDateTime(3),
            r.GetBoolean(4),
            r.GetBoolean(5));
    }

    protected async Task<List<EventRow>> EventsAsync(JobId id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql(
            "SELECT kind, attempt, server_id, message FROM {schema}.job_events WHERE job_id = @id ORDER BY at, kind"));
        cmd.Parameters.AddWithValue("id", id.Value);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var list = new List<EventRow>();
        while (await r.ReadAsync(Ct))
            list.Add(new EventRow(
                (JobEventKind)r.GetInt16(0),
                r.IsDBNull(1) ? null : r.GetInt32(1),
                r.IsDBNull(2) ? null : r.GetGuid(2),
                r.IsDBNull(3) ? null : r.GetString(3)));
        return list;
    }

    protected async Task<List<JobEventKind>> EventKindsAsync(JobId id) =>
        (await EventsAsync(id)).Select(e => e.Kind).ToList();

    protected Task<long> CountAsync(string sql, params (string Name, object? Value)[] ps) => Db.ScalarAsync<long>(sql, ps);

    protected Task<bool> ServerRowExistsAsync(Guid serverId) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.servers WHERE id = @id)", ("id", serverId));

    protected Task<DateTime> HeartbeatAtAsync(Guid serverId) =>
        Db.ScalarAsync<DateTime>("SELECT heartbeat_at FROM {schema}.servers WHERE id = @id", ("id", serverId));

    protected Task<DateTime> DbNowAsync() => Db.ScalarAsync<DateTime>("SELECT clock_timestamp()");

    /// <summary>Ждёт момента target по часам БД (явная задержка сценария).</summary>
    protected async Task DelayUntilDbTimeAsync(DateTime target)
    {
        var seconds = await Db.ScalarAsync<double>(
            "SELECT GREATEST(0, EXTRACT(EPOCH FROM (@t - clock_timestamp())))::float8", ("t", target));
        await Task.Delay(TimeSpan.FromSeconds(seconds), Ct);
    }

    /// <summary>Dequeue (§5.2 core), ждущий блокировку таблицы queue: виден в pg_stat_activity по тексту statement'а.</summary>
    protected Task<long> ParkedDequeueCountAsync() =>
        Db.ScalarAsync<long>(
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p",
            ("p", $"%{Db.Schema}.queue%attempt = q.attempt + 1%"));

    protected Task WaitForParkedDequeueAsync(TimeSpan timeout) =>
        WaitUntilAsync(async () => await ParkedDequeueCountAsync() > 0, timeout, "a dequeue parks on the queue table lock");
}
