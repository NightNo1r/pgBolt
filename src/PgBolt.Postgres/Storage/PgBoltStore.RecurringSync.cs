using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Synchronizes one owner's code-defined schedules (Part 4 §6.2). Takes the owner's transaction-level advisory lock, reads rows, computes
/// changes in .NET, and writes one NpgsqlBatch only when needed. SQL compares args as jsonb so JSON formatting alone causes no writes. Other
/// fields use normalized values (§4.2). Inserts use ordinal ID order with ON CONFLICT DO NOTHING. Owners with overlapping IDs wait in the same
/// order, and a skipped ID is a collision. Unlike the single INSERT … ORDER BY d.id using database collation in §6.2, the batch sends separate
/// statements using the same .NET ordinal comparer in every process, preserving deadlock avoidance. A 40P01 from a double collision triggers up
/// to three transaction retries with a warning (§7) instead of silent RunAsync/DbRetry handling.
/// </summary>
public sealed partial class PgBoltStore
{
    /// <summary>
    /// Retries the entire synchronization transaction on 40P01 for at most maxAttempts attempts, logging a warning per retry (§6.2, §7). Exhaustion
    /// and other errors propagate. Startup fails, while the periodic loop logs an error and waits for the next interval.
    /// </summary>
    internal static async Task<T> RetrySyncDeadlockAsync<T>(Func<CancellationToken, Task<T>> op, int maxAttempts, ILogger logger,
        string owner, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await op(ct);
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.DeadlockDetected && attempt < maxAttempts)
            {
                RecurringSyncLog.RecurringSyncDeadlockRetry(logger, e, owner, attempt + 1, maxAttempts);
                await Task.Delay(TimeSpan.FromMilliseconds(5 * attempt + Random.Shared.Next(10)), ct);
            }
        }
    }

    private const string SyncLockSql = "SELECT pg_advisory_xact_lock(hashtext(@key))";

    private const string SyncReadTemplate = """
        SELECT r.id, r.owner, r.type, r.args = d.args, r.cron, r.time_zone, r.misfire, r.catch_up_window, r.overlap,
               r.queue, r.priority, r.max_attempts, r.retry_delays
        FROM {schema}.recurring_jobs r
        LEFT JOIN unnest(@ids::text[], @args::jsonb[]) AS d(id, args) ON d.id = r.id
        WHERE r.owner = @owner OR r.id = ANY(@ids)
        """;

    private const string SyncLockRowsTemplate = """
        SELECT id, last_fire_at FROM {schema}.recurring_jobs WHERE id = ANY(@ids) AND owner = @owner ORDER BY id FOR UPDATE
        """;

    private const string SyncDeleteTemplate = """
        DELETE FROM {schema}.recurring_jobs WHERE owner = @owner AND id = ANY(@ids)
        """;

    private string SyncReadSql => field ??= Render(SyncReadTemplate);
    private string SyncLockRowsSql => field ??= Render(SyncLockRowsTemplate);
    private string SyncDeleteSql => field ??= Render(SyncDeleteTemplate);

    /// <summary>
    /// Stored row used for comparison. Reads catch_up_window and retry_delays defensively because direct SQL can store months or NULL array
    /// elements. An unrepresentable value, e.g. a null window, differs from the declaration and is overwritten (§6.2). Such values in rows owned by
    /// the API or another owner do not interrupt synchronization.
    /// </summary>
    private sealed record SyncRow(string? Owner, string Type, bool ArgsEqual, string Cron, string TimeZone, MisfirePolicy Misfire,
        TimeSpan? CatchUpWindow, OverlapPolicy Overlap, string? Queue, short? Priority, int? MaxAttempts, int?[]? RetryDelaysSeconds);

    internal Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(owner);
        ArgumentNullException.ThrowIfNull(logger);
        var declared = definitions.OrderBy(d => d.Id, StringComparer.Ordinal).ToList();
        var ids = declared.Select(d => d.Id).ToArray();
        var args = declared.Select(d => d.ArgsJson).ToArray();

        // retryOnConflict: false (тесты отсутствия deadlock) — одна попытка: 40P01 выходит исключением.
        return RetrySyncDeadlockAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);

            DateTime dbNow;
            var existing = new Dictionary<string, SyncRow>(StringComparer.Ordinal);
            await using (var read = new NpgsqlBatch(conn, tx)
                         {
                             BatchCommands = { new NpgsqlBatchCommand(SyncLockSql), new NpgsqlBatchCommand("SELECT now()"), new NpgsqlBatchCommand(SyncReadSql) },
                         })
            {
                P.Add(read.BatchCommands[0].Parameters, "key", $"{Schema}:recurring:{owner}");
                P.Add(read.BatchCommands[2].Parameters, "ids", ids);
                P.Add(read.BatchCommands[2].Parameters, "args", args, NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
                P.Add(read.BatchCommands[2].Parameters, "owner", owner);
                await using var r = await read.ExecuteReaderAsync(c);
                await r.NextResultAsync(c);                                   // результат pg_advisory_xact_lock
                await r.ReadAsync(c);
                dbNow = r.GetDateTime(0);
                await r.NextResultAsync(c);
                while (await r.ReadAsync(c))
                    existing[r.GetString(0)] = new SyncRow(r.GetNullable<string>(1), r.GetString(2), r.GetNullableValue<bool>(3) ?? false,
                        r.GetString(4), r.GetString(5), (MisfirePolicy)r.GetInt16(6), r.GetFieldValue<NpgsqlInterval>(7).ToTimeSpanOrNull(),
                        (OverlapPolicy)r.GetInt16(8), r.GetNullable<string>(9), r.GetNullableValue<short>(10),
                        r.GetNullableValue<int>(11), r.GetNullable<int?[]>(12));
            }

            var inserts = new List<RecurringDefinition>();
            var updates = new List<(RecurringDefinition Definition, List<string> Fields, bool Recalc)>();
            var collisions = new List<string>();
            foreach (var d in declared)
            {
                if (!existing.TryGetValue(d.Id, out var row))
                    inserts.Add(d);
                else if (row.Owner != owner)
                    collisions.Add(d.Id);
                else if (Diff(row, d) is { Count: > 0 } fields)
                    updates.Add((d, fields, fields.Contains("cron") || fields.Contains("time_zone")));
            }
            var declaredIds = ids.ToHashSet(StringComparer.Ordinal);
            var deletes = existing.Where(e => e.Value.Owner == owner && !declaredIds.Contains(e.Key))
                .Select(e => e.Key).Order(StringComparer.Ordinal).ToArray();

            if (inserts.Count == 0 && updates.Count == 0 && deletes.Length == 0)
            {
                await tx.CommitAsync(c);
                return new RecurringSyncResult([], [], [], collisions);
            }

            // Смена cron / таймзоны: курсор от last_fire_at под FOR UPDATE, как в AddOrUpdate (§4.2). Остальные строки не блокируются.
            var lastFire = new Dictionary<string, DateTime?>(StringComparer.Ordinal);
            var recalcIds = updates.Where(u => u.Recalc).Select(u => u.Definition.Id).ToArray();
            if (recalcIds.Length > 0)
            {
                await using var lockRows = new NpgsqlCommand(SyncLockRowsSql, conn, tx);
                P.Add(lockRows.Parameters, "ids", recalcIds);
                P.Add(lockRows.Parameters, "owner", owner);
                await using var r = await lockRows.ExecuteReaderAsync(c);
                while (await r.ReadAsync(c))
                    lastFire[r.GetString(0)] = r.GetNullableValue<DateTime>(1);
            }

            var inserted = new List<string>();
            await using (var write = new NpgsqlBatch(conn, tx))
            {
                foreach (var d in inserts)
                    write.BatchCommands.Add(InsertRecurringCommand(d, owner, d.Schedule().After(dbNow)));
                foreach (var (d, _, recalc) in updates)
                    write.BatchCommands.Add(UpdateRecurringCommand(d, owner, recalc,
                        recalc ? d.Schedule().After(Later(dbNow, lastFire.GetValueOrDefault(d.Id))) : null));
                if (deletes.Length > 0)
                {
                    var delete = new NpgsqlBatchCommand(SyncDeleteSql);
                    P.Add(delete.Parameters, "owner", owner);
                    P.Add(delete.Parameters, "ids", deletes);
                    write.BatchCommands.Add(delete);
                }

                await using var r = await write.ExecuteReaderAsync(c);
                foreach (var d in inserts)
                {
                    if (await r.ReadAsync(c))
                        inserted.Add(d.Id);
                    else
                        collisions.Add(d.Id);                                // параллельно создал API или другой владелец
                    await r.NextResultAsync(c);
                }
            }

            await tx.CommitAsync(c);
            return new RecurringSyncResult(inserted, updates.Select(u => new RecurringFieldChange(u.Definition.Id, u.Fields)).ToList(),
                deletes, collisions);
        }, _retryOnConflict ? DbRetry.MaxAttempts : 1, logger, owner, ct);
    }

    private static List<string> Diff(SyncRow row, RecurringDefinition d)
    {
        var fields = new List<string>();
        if (row.Type != d.Type) fields.Add("type");
        if (!row.ArgsEqual) fields.Add("args");
        if (row.Cron != d.Cron) fields.Add("cron");
        if (row.TimeZone != d.TimeZone) fields.Add("time_zone");
        if (row.Misfire != d.Misfire) fields.Add("misfire");
        if (row.CatchUpWindow != d.CatchUpWindow) fields.Add("catch_up_window");
        if (row.Overlap != d.Overlap) fields.Add("overlap");
        if (row.Queue != d.Queue) fields.Add("queue");
        if (row.Priority != d.Priority) fields.Add("priority");
        if (row.MaxAttempts != d.MaxAttempts) fields.Add("max_attempts");
        if (row.RetryDelaysSeconds is { } stored && Array.IndexOf(stored, null) >= 0
            || !RecurringDefinition.SameDelays(row.RetryDelaysSeconds?.Select(v => v.GetValueOrDefault()).ToArray(), d.RetryDelaysSeconds))
            fields.Add("retry_delays");
        return fields;
    }
}
