using System.Text.RegularExpressions;
using Npgsql;
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
/// транзакции показывает блокировки планирования, не выполняя statement. Блокировки исполнителя (маршрутизация
/// вставки в партицию) в счёт не входят.
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
    private const int BatchSize = 32;

    private static readonly Regex NamedParameter = new(@"(?<![@\w])@(\w+)", RegexOptions.CultureInvariant);

    /// <summary>
    /// Dequeue, ack и enqueue схемы <paramref name="store"/> с параметрами горячего пути: очередь <c>default</c>,
    /// 32 задачи с id текущих суток. Параметры связывает продуктовый код.
    /// </summary>
    public static IReadOnlyList<(string Statement, string Sql, Action<NpgsqlParameterCollection> Bind)> HotPath(
        PgBoltStore store)
    {
        var server = Guid.CreateVersion7();
        var ids = Enumerable.Range(0, BatchSize).Select(_ => Guid.CreateVersion7()).ToArray();
        var leaseIds = Enumerable.Range(0, BatchSize).Select(_ => Guid.NewGuid()).ToArray();
        var batch = new EnqueueBatch("load", "default", 0, 3, null, null, TimeSpan.Zero, ids,
            [.. Enumerable.Repeat("{}", BatchSize)], new DateTime?[BatchSize], new string?[BatchSize]);
        return
        [
            (StatementClasses.Dequeue, store.DequeueSql,
                ps => PgBoltStore.BindDequeue(ps, "default", BatchSize, server, TimeSpan.FromMinutes(1))),
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

        await using var count = new NpgsqlCommand("""
            SELECT count(*) FILTER (WHERE NOT fastpath), count(*) FROM pg_locks
            WHERE pid = pg_backend_pid() AND locktype = 'relation' AND relation <> 'pg_locks'::regclass
            """, conn, tx);
        await using var r = await count.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        var result = new LockCount(r.GetInt64(0), r.GetInt64(1));
        await r.CloseAsync();
        await tx.RollbackAsync(ct);
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

    /// <summary>
    /// Нижняя оценка блокировок generic plan. Перед выполнением он блокирует все отношения range table — все
    /// <paramref name="partitions"/> партиций <c>jobs</c> и <paramref name="otherRelations"/> прочих таблиц, — затем
    /// индексы index scan'ов в партициях, переживших начальное отсечение: у ack и enqueue — PK
    /// <paramref name="survivingPartitions"/> партиций; у dequeue начального отсечения нет (ключ партиции известен
    /// только при выполнении), поэтому PK всех партиций. Индексы, которые открывает изменение строк, не учтены.
    /// </summary>
    public static long GenericLowerBound(string statement, int partitions, int otherRelations, int survivingPartitions) =>
        partitions + otherRelations + (statement == StatementClasses.Dequeue ? partitions : survivingPartitions);

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
