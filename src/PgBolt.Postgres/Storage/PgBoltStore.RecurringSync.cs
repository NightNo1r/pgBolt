using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Синхронизация code-first набора владельца (часть 4 §6.2): advisory xact lock владельца, чтение, дифф в .NET, запись одним
/// NpgsqlBatch только при разнице. args сравниваются в SQL как jsonb (форматирование JSON не даёт записей), остальные поля —
/// в нормализованной форме (§4.2). Вставки — в порядке id (Ordinal) с ON CONFLICT DO NOTHING: разные владельцы с
/// пересекающимися id ждут друг друга в одном порядке; невставленный id — коллизия. Отклонение от текста §6.2 (один
/// INSERT … ORDER BY d.id с collation базы): порядок задаёт ordinal-сортировка в .NET, statement'ы идут в batch по одному —
/// для отсутствия deadlock это эквивалентно, так как все процессы используют один компаратор. 40P01 (двойная коллизия, §6.2) —
/// повтор транзакции до 3 раз с Warning (§7), не молча через RunAsync/DbRetry.
/// </summary>
public sealed partial class PgBoltStore
{
    /// <summary>
    /// §6.2, §7: 40P01 синхронизации — повтор всей транзакции, не больше maxAttempts попыток, Warning на каждый повтор.
    /// Исчерпание и любые другие ошибки — исключение наружу (на старте — падение, в цикле — Error и следующий интервал).
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
    /// Хранимая строка для диффа. catch_up_window и retry_delays читаются без исключений (interval с месяцами, NULL в массиве —
    /// строка, записанная SQL'ем в обход API): нечитаемое значение (null окна) отличается от объявленного и перезаписывается
    /// им (§6.2), а чужая или API-строка с таким значением не роняет синхронизацию.
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
