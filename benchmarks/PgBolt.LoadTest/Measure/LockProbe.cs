using System.Text.RegularExpressions;
using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.Postgres.Storage;

namespace PgBolt.LoadTest.Measure;

/// <summary>Блокировки отношений одного statement'а: вне fast-path (в общей таблице блокировок) и всего.</summary>
internal sealed record LockCount(long NonFastPath, long Total);

/// <summary>План, который PostgreSQL выбрал для подготовленного statement'а.</summary>
internal enum PlanKind { Custom, Generic }

/// <summary>
/// Блокировки statement'ов горячего пути без нагрузки, на отдельном прямом соединении. Планировщик блокирует
/// отношения, оставшиеся после отсечения партиций, и их индексы до конца транзакции, поэтому <c>EXPLAIN</c> в
/// транзакции показывает блокировки планирования ack/enqueue. Dequeue исполняется с реальной пачкой до отката.
/// </summary>
internal sealed class LockProbe(string directConnectionString)
{
    /// <summary>
    /// Fast-path слотов блокировок у backend'а PostgreSQL 17: слабые блокировки отношений сверх них идут в общую таблицу.
    /// </summary>
    public const int FastPathSlots = 16;

    /// <summary>Назначение служебного соединения пробы в <see cref="StandConnection.For"/>.</summary>
    public const string ConnectionPurpose = "locks";

    private const string PreparedName = "pgbolt_lock_probe";
    private const string ProbeQueue = "pgbolt_lock_probe";
    private const int BatchSize = 32;

    private static readonly Regex NamedParameter = new(@"(?<![@\w])@(\w+)", RegexOptions.CultureInvariant);

    /// <summary>
    /// Dequeue, ack и enqueue схемы <paramref name="store"/> с параметрами горячего пути: очередь <c>default</c>,
    /// 32 задачи с id текущих суток. Параметры связывает продуктовый код.
    /// </summary>
    public static IReadOnlyList<(string Statement, string Sql, Action<NpgsqlParameterCollection> Bind)> HotPath(
        PgBoltStore store, QueueProfile queues = QueueProfile.Standard)
    {
        var server = Guid.CreateVersion7();
        var ids = Enumerable.Range(0, BatchSize).Select(_ => Guid.CreateVersion7()).ToArray();
        var leaseIds = Enumerable.Range(0, BatchSize).Select(_ => Guid.NewGuid()).ToArray();
        var batch = new EnqueueBatch("load", "default", 0, 3, null, null, TimeSpan.Zero, ids,
            [.. Enumerable.Repeat("{}", BatchSize)], new DateTime?[BatchSize], new string?[BatchSize]);
        return
        [
            (queues == QueueProfile.Limited ? StatementClasses.DequeueLimited : StatementClasses.Dequeue,
                queues == QueueProfile.Limited ? store.DequeueLimitedSql : store.DequeueSql,
                ps => PgBoltStore.BindDequeue(ps, ProbeQueue, BatchSize, server, TimeSpan.FromMinutes(1))),
            (StatementClasses.Ack, store.AckSql, ps => PgBoltStore.BindAck(ps, ids, leaseIds, server)),
            (StatementClasses.Enqueue, store.EnqueueSql, ps => PgBoltStore.BindEnqueue(ps, batch)),
        ];
    }

    /// <summary>
    /// Блокировки custom plan: <c>EXPLAIN</c> безымянного statement'а с параметрами и счёт <c>pg_locks</c> своего
    /// backend'а в той же транзакции. Блокировка самого <c>pg_locks</c> при занятых fast-path слотах попала бы в счёт
    /// «вне fast-path», поэтому исключена.
    /// </summary>
    public async Task<LockCount> MeasureAsync(string sql, Action<NpgsqlParameterCollection> bind, CancellationToken ct)
    {
        await using var conn = await StandConnection.OpenAsync(directConnectionString, ConnectionPurpose, ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var explain = new NpgsqlCommand("EXPLAIN " + sql, conn, tx))
        {
            bind(explain.Parameters);
            await explain.ExecuteNonQueryAsync(ct);
        }

        return await CountLocksAsync(conn, tx, ct);
    }

    // Определения читаются только при выполнении. Подготовка пачки на другом соединении сохраняет чистоту pg_locks.
    public async Task<LockCount> MeasureDequeueAsync(PgBoltStore store, string sql, Action<NpgsqlParameterCollection> bind,
        PlanKind? plan, CancellationToken ct)
    {
        var ids = Enumerable.Range(0, BatchSize).Select(_ => Guid.CreateVersion7()).ToArray();
        var seed = new EnqueueBatch("load", ProbeQueue, 0, 3, null, null, TimeSpan.Zero, ids,
            [.. Enumerable.Repeat("{}", BatchSize)], new DateTime?[BatchSize], new string?[BatchSize]);
        await using var conn = await StandConnection.OpenAsync(directConnectionString, ConnectionPurpose, ct);
        await store.EnqueueAsync(seed, null, ct);
        try
        {
            if (sql == store.DequeueLimitedSql)
            {
                await store.SetConcurrencyOverrideAsync(ProbeQueue, BatchSize, null, ct);
            }
            await using var read = new NpgsqlCommand(sql, conn);
            bind(read.Parameters);
            if (plan.HasValue)
            {
                await read.PrepareAsync(ct);
            }

            // Первое выполнение строит план. Повтор в новой транзакции показывает блокировки готового плана.
            for (var pass = 0; ; pass++)
            {
                await using var tx = await conn.BeginTransactionAsync(ct);
                if (plan.HasValue)
                {
                    await ExecuteAsync(conn, plan == PlanKind.Generic ? "SET LOCAL plan_cache_mode = force_generic_plan"
                        : "SET LOCAL plan_cache_mode = force_custom_plan", ct);
                }
                var rows = 0;
                await using (var reader = await read.ExecuteReaderAsync(ct))
                {
                    var id = reader.GetOrdinal("id");
                    while (await reader.ReadAsync(ct))
                    {
                        if (!reader.IsDBNull(id)) rows++;
                    }
                }
                if (rows != BatchSize)
                {
                    throw new InvalidOperationException($"Проба выдала {rows} задач из {BatchSize}.");
                }
                if (!plan.HasValue || pass > 0)
                {
                    return await CountLocksAsync(conn, tx, ct);
                }
                await tx.RollbackAsync(ct);
            }
        }
        finally
        {
            // Обе строки каждой пробной задачи удаляются одной неявной транзакцией до прогрева нагрузки.
            await using var clean = new NpgsqlCommand($"DELETE FROM {store.Schema}.queue WHERE job_id = ANY(@ids); " +
                $"DELETE FROM {store.Schema}.jobs WHERE id = ANY(@ids); " +
                $"DELETE FROM {store.Schema}.queue_settings WHERE queue = @queue", conn);
            clean.Parameters.AddWithValue("ids", ids);
            clean.Parameters.AddWithValue("queue", ProbeQueue);
            await clean.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    private static async Task<LockCount> CountLocksAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand("""
            SELECT count(*) FILTER (WHERE NOT fastpath), count(*) FROM pg_locks
            WHERE pid = pg_backend_pid() AND locktype = 'relation' AND relation <> 'pg_locks'::regclass
            """, conn, tx);
        await using var r = await count.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        var result = new LockCount(r.GetInt64(0), r.GetInt64(1));
        await r.CloseAsync();
        return result;
    }

    /// <summary>
    /// План подготовленного statement'а: <c>PREPARE</c> с позиционными параметрами и шесть <c>EXPLAIN EXECUTE</c> —
    /// после пяти custom plan PostgreSQL при <c>plan_cache_mode = auto</c> выбирает план по стоимости; затем
    /// <c>pg_prepared_statements</c> и <c>DEALLOCATE</c>. Аргументы <c>EXECUTE</c> — литералы: параметры протокола
    /// в них PostgreSQL не принимает. Литералы строит сам сервер из значений, связанных продуктовым кодом.
    /// </summary>
    public async Task<PlanKind> DetectPlanAsync(string sql, Action<NpgsqlParameterCollection> bind, CancellationToken ct)
    {
        await using var conn = await StandConnection.OpenAsync(directConnectionString, ConnectionPurpose, ct);
        await using var quote = new NpgsqlCommand { Connection = conn };
        bind(quote.Parameters);
        var positions = quote.Parameters.Select((p, i) => (p.ParameterName, i + 1)).ToDictionary();
        var positional = NamedParameter.Replace(sql, m => "$" + positions[m.Groups[1].Value]);

        quote.CommandText = "SELECT " + string.Join(", ", positions.Values.Select(i => $"quote_nullable(${i})"));
        foreach (NpgsqlParameter p in quote.Parameters) p.ParameterName = "";
        string arguments;
        await using (var r = await quote.ExecuteReaderAsync(ct))
        {
            await r.ReadAsync(ct);
            arguments = string.Join(", ", Enumerable.Range(0, r.FieldCount).Select(r.GetString));
        }

        await ExecuteAsync(conn, $"PREPARE {PreparedName} AS {positional}", ct);
        try
        {
            for (var i = 0; i < 6; i++)
                await ExecuteAsync(conn, $"EXPLAIN EXECUTE {PreparedName}({arguments})", ct);

            await using var plans = new NpgsqlCommand(
                $"SELECT generic_plans FROM pg_prepared_statements WHERE name = '{PreparedName}'", conn);
            return (long)(await plans.ExecuteScalarAsync(ct))! == 0 ? PlanKind.Custom : PlanKind.Generic;
        }
        finally
        {
            await ExecuteAsync(conn, $"DEALLOCATE {PreparedName}", CancellationToken.None);
        }
    }

    /// <summary>Для ack и enqueue generic plan открывает все дочерние таблицы и PK переживших отсечение.</summary>
    public static long GenericLowerBound(int partitions, int otherRelations, int survivingPartitions) =>
        partitions + otherRelations + survivingPartitions;

    /// <summary>
    /// Ёмкость общей таблицы блокировок по параметрам сервера:
    /// <c>max_locks_per_transaction × (MaxBackends + max_prepared_transactions)</c>, где MaxBackends —
    /// <c>max_connections + autovacuum_max_workers + 1 + max_worker_processes + max_wal_senders</c>.
    /// </summary>
    public async Task<long> LockTableCapacityAsync(CancellationToken ct)
    {
        await using var conn = await StandConnection.OpenAsync(directConnectionString, ConnectionPurpose, ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT current_setting('max_locks_per_transaction')::bigint * (
                current_setting('max_connections')::int + current_setting('autovacuum_max_workers')::int + 1
                + current_setting('max_worker_processes')::int + current_setting('max_wal_senders')::int
                + current_setting('max_prepared_transactions')::int)
            """, conn);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecuteAsync(NpgsqlConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
