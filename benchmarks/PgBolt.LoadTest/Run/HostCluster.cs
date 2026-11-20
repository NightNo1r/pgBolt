using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Run;

/// <summary>Два хоста PgBolt с одним виртуальным сервером на 32 воркера, очереди <c>default</c> и <c>critical</c>.</summary>
internal sealed class HostCluster : IAsyncDisposable
{
    public static readonly IReadOnlyList<string> ApplicationNames = ["pgbolt-load-host-1", "pgbolt-load-host-2"];

    private readonly List<IHost> _hosts = [];

    private HostCluster()
    {
    }

    /// <param name="connectionString">Строка режима: напрямую или через PgBouncer.</param>
    public static async Task<HostCluster> StartAsync(string connectionString, string schema, LoadMode mode,
        RunContext ctx, CancellationToken ct)
    {
        var cluster = new HostCluster();
        try
        {
            foreach (var name in ApplicationNames)
            {
                var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
                builder.Services.AddSingleton(ctx);
                builder.Services.AddPgBolt(o => Configure(o, connectionString, 20, name, schema, mode));
                builder.Services.AddPgBoltServer("load", s =>
                {
                    s.Workers = 32;
                    if (ctx.Fetches is { } fetches) { s.FetchMeasured = fetches.Fetch; }
                    if (ctx.Queues == QueueProfile.Limited)
                    {
                        s.Queue("default", q => q.ClusterMaxConcurrency = s.Workers);
                    }
                    else
                    {
                        s.Queue("default").Queue("critical");
                    }
                });
                var host = builder.Build();
                cluster._hosts.Add(host);
                if (ctx.Fetches is { } recorder)
                {
                    var store = host.Services.GetRequiredService<Postgres.Storage.PgBoltStore>();
                    store.DequeueExecuting = recorder.Sent;
                    if (ctx.Queues == QueueProfile.Limited) { store.QueueLockMeasured = recorder.QueueLock; }
                }
                await host.StartAsync(ct);
            }

            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Пул задаётся строкой подключения, а не своим <c>NpgsqlDataSource</c>: тогда PgBolt сам выставляет значения
    /// для PgBouncer. В режимах <c>*-prepared</c> statement'ы подготавливаются.
    /// </summary>
    internal static void Configure(Postgres.PgBoltOptions o, string connectionString, int maxPoolSize,
        string applicationName, string schema, LoadMode mode)
    {
        o.ConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = maxPoolSize,
            ApplicationName = applicationName,
        }.ConnectionString;
        o.Schema = schema;
        o.UsePreparedStatements = mode is LoadMode.DirectPrepared or LoadMode.BouncerPrepared;
        o.AddJobs(LoadJob.Register);
    }

    /// <summary>
    /// Останавливает хосты; пулы соединений закрываются вместе с их контейнерами DI. Сбой одного хоста не мешает
    /// остановить остальные: первое исключение бросается после всех.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        var errors = new List<Exception>();
        foreach (var host in _hosts)
        {
            try
            {
                try
                {
                    await host.StopAsync(CancellationToken.None);
                }
                finally
                {
                    await ((IAsyncDisposable)host).DisposeAsync();
                }
            }
            catch (Exception e)
            {
                errors.Add(e);
            }
        }

        if (errors.Count > 0) ExceptionDispatchInfo.Throw(errors[0]);
    }
}
