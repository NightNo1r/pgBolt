using System.Text.RegularExpressions;
using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.Postgres.Storage;

namespace PgBolt.LoadTest.Measure;

/// <summary>Relation locks for one statement, both outside the fast path (shared lock table) and in total.</summary>
internal sealed record LockCount(long NonFastPath, long Total);

/// <summary>Plan PostgreSQL chose for a prepared statement.</summary>
internal enum PlanKind { Custom, Generic }

/// <summary>
/// Probes hot-path statement locks without load on a separate direct connection. The planner holds locks on relations surviving partition
/// pruning and their indexes until transaction end. <c>EXPLAIN</c> in a transaction therefore exposes ack and enqueue planning locks. Dequeue
/// runs with a real batch before rollback.
/// </summary>
internal sealed class LockProbe(string directConnectionString)
{
    /// <summary>Fast-path lock slots per PostgreSQL 17 backend. Additional weak relation locks go into the shared lock table.</summary>
    public const int FastPathSlots = 16;

    /// <summary>Purpose passed to <see cref="StandConnection.For"/> for the probe connection.</summary>
    public const string ConnectionPurpose = "locks";

    private const string PreparedName = "pgbolt_lock_probe";
    private const string ProbeQueue = "pgbolt_lock_probe";
    private const int BatchSize = 32;

    private static readonly Regex NamedParameter = new(@"(?<![@\w])@(\w+)", RegexOptions.CultureInvariant);

    /// <summary>
    /// Dequeue, ack, and enqueue in the <paramref name="store"/> schema with hot-path parameters: the <c>default</c> queue and 32 job IDs from the
    /// current UTC day. Production code binds the parameters.
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
    /// Measures custom-plan locks with <c>EXPLAIN</c> on an unnamed parameterized statement, then counts the backend's <c>pg_locks</c> in the same
    /// transaction. Excludes the lock on <c>pg_locks</c> itself, which would count as a non-fast-path lock if all fast-path slots were occupied.
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
                    throw new InvalidOperationException($"Probe returned {rows} of {BatchSize} jobs.");
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
    /// Uses <c>PREPARE</c> with positional parameters and six <c>EXPLAIN EXECUTE</c> calls. After five custom plans PostgreSQL chooses by cost when
    /// <c>plan_cache_mode = auto</c>. Reads <c>pg_prepared_statements</c> and runs <c>DEALLOCATE</c>. PostgreSQL requires literals in <c>EXECUTE</c>
    /// arguments, so the server constructs them from the values bound by production code.
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

    /// <summary>For ack and enqueue, a generic plan opens all child tables and the primary keys of those surviving pruning.</summary>
    public static long GenericLowerBound(int partitions, int otherRelations, int survivingPartitions) =>
        partitions + otherRelations + survivingPartitions;

    /// <summary>
    /// Shared lock table capacity from server settings: <c>max_locks_per_transaction × (MaxBackends + max_prepared_transactions)</c>, where
    /// MaxBackends is <c>max_connections + autovacuum_max_workers + 1 + max_worker_processes + max_wal_senders</c>.
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
