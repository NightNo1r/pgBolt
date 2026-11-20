using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.Postgres.Hosting;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Подготовка схемы прогона до старта хостов: миграция стартом короткоживущего хоста PgBolt, индексы набора
/// <c>extra</c> на родителе <c>jobs</c> и партиции. Партиции создаёт продуктовый код.
/// </summary>
internal static class SchemaPreparer
{
    // Все партиции одного вызова создаются одной транзакцией: при сотнях партиций это тысячи блокировок, сравнимые
    // с ёмкостью общей таблицы блокировок, поэтому горизонт растёт шагами.
    private const int DaysStep = 50;

    /// <param name="partitions">Число партиций <c>jobs</c>: от сегодня до сегодня + partitions − 1.</param>
    public static async Task PrepareAsync(string connectionString, string schema, int partitions, IndexSet indexes,
        CancellationToken ct)
    {
        var standConnection = StandConnection.For(connectionString, "prepare").ConnectionString;
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddPgBolt(o =>
        {
            o.ConnectionString = standConnection;
            o.Schema = schema;
        });
        var host = builder.Build();
        try
        {
            await host.StartAsync(ct);
            if (indexes == IndexSet.Extra)
            {
                await using var conn = new NpgsqlConnection(standConnection);
                await conn.OpenAsync(ct);
                // Индекс партиционированного родителя создаётся и на существующих партициях, новые получают его при ATTACH.
                await using var cmd = new NpgsqlCommand($"""
                    CREATE INDEX jobs_type ON {schema}.jobs (type, id);
                    CREATE INDEX jobs_args ON {schema}.jobs USING gin (args jsonb_path_ops);
                    """, conn);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            var maintenance = host.Services.GetRequiredService<IPgBoltMaintenance>();
            for (var days = 0; days < partitions - 1;)
            {
                days = Math.Min(days + DaysStep, partitions - 1);
                if ((await maintenance.EnsurePartitionsAsync(days, ct)).TimedOut)
                    throw new InvalidOperationException($"Партиции {schema}.jobs на {days} суток вперёд не созданы: lock_timeout.");
            }
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            await ((IAsyncDisposable)host).DisposeAsync();
        }
    }
}
