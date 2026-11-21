using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Run;

/// <summary>Two PgBolt hosts, each with one virtual server and 32 workers serving <c>default</c> and <c>critical</c>.</summary>
internal sealed class HostCluster : IAsyncDisposable
{
    public static readonly IReadOnlyList<string> ApplicationNames = ["pgbolt-load-host-1", "pgbolt-load-host-2"];

    private readonly List<IHost> _hosts = [];

    private HostCluster()
    {
    }

    /// <param name="connectionString">Connection string for the run mode, direct or through PgBouncer.</param>
    public static async Task<HostCluster> StartAsync(string connectionString, string schema, LoadMode mode,
        RunContext ctx, CancellationToken ct)
    {
        var cluster = new HostCluster();
        try
        {
            foreach (var name in ApplicationNames)
            {
                // У хоста один диспетчер. Все попытки одного FetchAsync учитываются в окне его завершения.
                var sends = 0;
                var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
                builder.Services.AddSingleton(ctx);
                builder.Services.AddPgBolt(o => Configure(o, connectionString, 20, name, schema, mode));
                builder.Services.AddPgBoltServer("load", s =>
                {
                    s.Workers = 32;
                    if (ctx.Fetches is { } fetches)
                    {
                        s.FetchMeasured = (elapsed, jobs) =>
                        {
                            fetches.Fetch(elapsed, jobs, sends);
                            sends = 0;
                        };
                    }
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
                    store.DequeueExecuting = () => sends++;
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
    /// Configures pooling through the connection string so PgBolt sets the PgBouncer-compatible defaults itself instead of receiving a custom
    /// <c>NpgsqlDataSource</c>. Modes ending in <c>*-prepared</c> prepare statements.
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
    /// Stops the hosts and closes connection pools when their DI containers are disposed. A host failure never prevents stopping the rest. Rethrows
    /// the first exception after all have stopped.
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
