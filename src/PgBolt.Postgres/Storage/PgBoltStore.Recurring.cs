using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Storage;

/// <summary>Runtime schedule API (Part 4 §4.2) and shared insert and update commands used by synchronization (§6.2).</summary>
public sealed partial class PgBoltStore
{
    private const string InsertRecurringTemplate = """
        INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
                                             queue, priority, max_attempts, retry_delays, owner, next_fire_at, wake_at,
                                             created_at, updated_at)
        VALUES (@id, @type, @args, @cron, @time_zone, @misfire, @catch_up_window, @overlap,
                @queue, @priority, @max_attempts, @retry_delays, @owner, @cursor, @cursor, now(), now())
        ON CONFLICT (id) DO NOTHING
        RETURNING id
        """;

    private const string UpdateRecurringTemplate = """
        UPDATE {schema}.recurring_jobs
        SET type = @type, args = @args, cron = @cron, time_zone = @time_zone, misfire = @misfire,
            catch_up_window = @catch_up_window, overlap = @overlap, queue = @queue, priority = @priority,
            max_attempts = @max_attempts, retry_delays = @retry_delays, updated_at = now(),
            next_fire_at = CASE WHEN @recalc THEN @cursor ELSE next_fire_at END,
            wake_at      = CASE WHEN @recalc THEN @cursor ELSE wake_at END
        WHERE id = @id AND owner IS NOT DISTINCT FROM @owner
        """;

    private const string LockRecurringTemplate = """
        SELECT owner, cron, time_zone, last_fire_at FROM {schema}.recurring_jobs WHERE id = @id FOR UPDATE
        """;

    private const string RemoveRecurringTemplate = """
        WITH d AS (DELETE FROM {schema}.recurring_jobs WHERE id = @id AND owner IS NULL RETURNING 1)
        SELECT EXISTS (SELECT 1 FROM d), (SELECT owner FROM {schema}.recurring_jobs WHERE id = @id)
        """;

    private const string PauseRecurringTemplate = """
        UPDATE {schema}.recurring_jobs SET paused = true WHERE id = @id
        """;

    private const string ResumeRecurringTemplate = """
        UPDATE {schema}.recurring_jobs
        SET paused = false, wake_at = CASE WHEN wake_at IS NULL THEN NULL ELSE LEAST(wake_at, now()) END
        WHERE id = @id
        """;

    private const string GetRecurringTemplate = """
        SELECT id, type, args, cron, time_zone, misfire, catch_up_window, overlap, queue, priority, max_attempts, retry_delays,
               owner, paused, next_fire_at, last_fire_at, last_job_id, last_error, created_at, updated_at
        FROM {schema}.recurring_jobs WHERE id = @id
        """;

    private const string GetRecurringForTriggerTemplate = """
        SELECT r.type, r.args,
               COALESCE(r.queue,        t.default_queue),
               COALESCE(r.priority,     t.default_priority),
               COALESCE(r.max_attempts, t.default_max_attempts),
               COALESCE(r.retry_delays, t.default_retry_delays),
               r.overlap, t.name IS NULL
        FROM {schema}.recurring_jobs r
        LEFT JOIN {schema}.job_types t ON t.name = r.type
        WHERE r.id = @id
        """;

    private string InsertRecurringSql => field ??= Render(InsertRecurringTemplate);
    private string UpdateRecurringSql => field ??= Render(UpdateRecurringTemplate);
    private string LockRecurringSql => field ??= Render(LockRecurringTemplate);
    private string RemoveRecurringSql => field ??= Render(RemoveRecurringTemplate);
    private string PauseRecurringSql => field ??= Render(PauseRecurringTemplate);
    private string ResumeRecurringSql => field ??= Render(ResumeRecurringTemplate);
    private string GetRecurringSql => field ??= Render(GetRecurringTemplate);
    private string GetRecurringForTriggerSql => field ??= Render(GetRecurringForTriggerTemplate);

    /// <summary>
    /// AddOrUpdate takes two round trips in one transaction (§4.2). Under FOR UPDATE, sets the cursor to after(max(db_now, last_fire_at)) so an
    /// occurrence already enqueued under the old cron is not repeated. An empty INSERT RETURNING means a concurrent insert won. Restart to see the
    /// row under lock.
    /// </summary>
    internal Task<RecurringUpsertResult> UpsertRecurringAsync(RecurringDefinition d, CancellationToken ct)
    {
        var schedule = d.Schedule();
        return RunAsync(async c =>
        {
            while (true)
            {
                await using var conn = await _dataSource.OpenConnectionAsync(c);
                await using var tx = await conn.BeginTransactionAsync(c);
                var current = await LockRecurringAsync(conn, tx, d.Id, c);
                if (current.Exists && current.Owner is not null)
                {
                    await tx.RollbackAsync(c);
                    return RecurringUpsertResult.OwnedByCodeFirst;
                }

                var cursor = schedule.After(Later(current.DbNow, current.LastFireAt));
                if (!current.Exists)
                {
                    await using var insert = new NpgsqlBatch(conn, tx) { BatchCommands = { InsertRecurringCommand(d, owner: null, cursor) } };
                    if (await insert.ExecuteScalarAsync(c) is null)
                    {
                        await tx.RollbackAsync(c);
                        continue;
                    }
                    await tx.CommitAsync(c);
                    return RecurringUpsertResult.Inserted;
                }

                var recalc = current.Cron != d.Cron || current.TimeZone != d.TimeZone;
                await using (var update = new NpgsqlBatch(conn, tx) { BatchCommands = { UpdateRecurringCommand(d, owner: null, recalc, cursor) } })
                    await update.ExecuteNonQueryAsync(c);
                await tx.CommitAsync(c);
                return RecurringUpsertResult.Updated;
            }
        }, ct);
    }

    internal Task<RecurringRemoveResult> RemoveRecurringAsync(string id, CancellationToken ct) =>
        QueryAsync(RemoveRecurringSql, ps => P.Add(ps, "id", id), async (r, c) =>
        {
            await r.ReadAsync(c);
            if (r.GetBoolean(0))
                return RecurringRemoveResult.Removed;
            return r.IsDBNull(1) ? RecurringRemoveResult.NotFound : RecurringRemoveResult.OwnedByCodeFirst;
        }, ct);

    internal async Task<bool> PauseRecurringAsync(string id, CancellationToken ct) =>
        await NonQueryAsync(PauseRecurringSql, ps => P.Add(ps, "id", id), ct) == 1;

    /// <summary>Sets wake_at = LEAST(wake_at, now()) except when NULL (§4.2). LEAST ignores NULL and would reactivate a row without a
    /// cursor.</summary>
    internal async Task<bool> ResumeRecurringAsync(string id, CancellationToken ct) =>
        await NonQueryAsync(ResumeRecurringSql, ps => P.Add(ps, "id", id), ct) == 1;

    internal Task<RecurringJobInfo?> GetRecurringAsync(string id, CancellationToken ct) =>
        QueryAsync<RecurringJobInfo?>(GetRecurringSql, ps => P.Add(ps, "id", id), async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return null;
            var rowId = r.GetString(0);
            using var args = JsonDocument.Parse(r.GetString(2));
            return new RecurringJobInfo(
                rowId, r.GetString(1), args.RootElement.Clone(), r.GetString(3), r.GetString(4),
                (MisfirePolicy)r.GetInt16(5), ReadCatchUpWindow(rowId, r.GetFieldValue<NpgsqlInterval>(6)), (OverlapPolicy)r.GetInt16(7),
                r.GetNullable<string>(8), r.GetNullableValue<short>(9), r.GetNullableValue<int>(10),
                ReadRetryDelays(rowId, r.GetNullable<int?[]>(11))?.Select(s => TimeSpan.FromSeconds(s)).ToArray(),
                r.GetNullable<string>(12), r.GetBoolean(13),
                r.GetNullableValue<DateTimeOffset>(14), r.GetNullableValue<DateTimeOffset>(15),
                r.GetNullableValue<Guid>(16) is { } job ? new JobId(job) : null, r.GetNullable<string>(17),
                r.GetFieldValue<DateTimeOffset>(18), r.GetFieldValue<DateTimeOffset>(19));
        }, ct);

    /// <summary>
    /// Trigger skips catch_up_window, cron, and time zone because it uses none of them (§4.2). It needs retry_delays to enqueue, so an unreadable
    /// value throws InvalidOperationException as in GetAsync. The scheduler tick also skips the invalid row (§5.2, step 2).
    /// </summary>
    internal Task<RecurringTriggerRow?> GetRecurringForTriggerAsync(string id, CancellationToken ct) =>
        QueryAsync<RecurringTriggerRow?>(GetRecurringForTriggerSql, ps => P.Add(ps, "id", id), async (r, c) =>
            await r.ReadAsync(c)
                ? new RecurringTriggerRow(r.GetString(0), r.GetString(1), r.GetNullable<string>(2), r.GetNullableValue<short>(3),
                    r.GetNullableValue<int>(4), ReadRetryDelays(id, r.GetNullable<int?[]>(5)), (OverlapPolicy)r.GetInt16(6), r.GetBoolean(7))
                : null, ct);

    /// <summary>INSERT … ON CONFLICT (id) DO NOTHING RETURNING id returns a row only after a successful insert.</summary>
    internal NpgsqlBatchCommand InsertRecurringCommand(RecurringDefinition d, string? owner, DateTime? cursor)
    {
        var cmd = new NpgsqlBatchCommand(InsertRecurringSql);
        BindDefinition(cmd.Parameters, d, owner, cursor);
        return cmd;
    }

    /// <summary>
    /// Updates definition fields for this owner's row. When cron or time zone changes, recalc resets the cursor and sets wake_at to it.
    /// </summary>
    internal NpgsqlBatchCommand UpdateRecurringCommand(RecurringDefinition d, string? owner, bool recalc, DateTime? cursor)
    {
        var cmd = new NpgsqlBatchCommand(UpdateRecurringSql);
        BindDefinition(cmd.Parameters, d, owner, cursor);
        P.Add(cmd.Parameters, "recalc", recalc);
        return cmd;
    }

    internal static DateTime Later(DateTime a, DateTime? b) => b is { } v && v > a ? v : a;

    /// <summary>
    /// Direct SQL writes may store values outside the contract, such as intervals containing months or exceeding TimeSpan, or NULL elements in
    /// retry_delays (§5.2, step 2). Reports a descriptive error in place of an Npgsql InvalidCastException.
    /// </summary>
    private static TimeSpan ReadCatchUpWindow(string id, NpgsqlInterval value) =>
        value.ToTimeSpanOrNull() ?? throw Unreadable(id, "catch_up_window",
            $"({value.Months} months, {value.Days} days, {value.Time} µs) has months or does not fit TimeSpan");

    private static int[]? ReadRetryDelays(string id, int?[]? value)
    {
        if (value is null)
            return null;
        var delays = new int[value.Length];
        for (var i = 0; i < value.Length; i++)
            delays[i] = value[i] ?? throw Unreadable(id, "retry_delays", "contains NULL elements");
        return delays;
    }

    private static InvalidOperationException Unreadable(string id, string column, string problem) =>
        new($"Recurring job '{id}': {column} {problem}; the row was probably written by SQL bypassing the API. " +
            "Rewrite the definition: IRecurringJobManager.AddOrUpdateAsync for an API row, the owner's code-first " +
            $"synchronization (AddRecurring) for a code-first row, or an UPDATE of {column} in SQL.");

    private static void BindDefinition(NpgsqlParameterCollection ps, RecurringDefinition d, string? owner, DateTime? cursor)
    {
        P.Add(ps, "id", d.Id);
        P.Add(ps, "type", d.Type);
        P.Add(ps, "args", d.ArgsJson, NpgsqlDbType.Jsonb);
        P.Add(ps, "cron", d.Cron);
        P.Add(ps, "time_zone", d.TimeZone);
        P.Add(ps, "misfire", (short)d.Misfire);
        P.Add(ps, "catch_up_window", d.CatchUpWindow, NpgsqlDbType.Interval);
        P.Add(ps, "overlap", (short)d.Overlap);
        P.AddNullable(ps, "queue", d.Queue, NpgsqlDbType.Text);
        P.AddValue(ps, "priority", d.Priority, NpgsqlDbType.Smallint);
        P.AddValue(ps, "max_attempts", d.MaxAttempts, NpgsqlDbType.Integer);
        P.AddNullable(ps, "retry_delays", d.RetryDelaysSeconds, NpgsqlDbType.Array | NpgsqlDbType.Integer);
        P.AddNullable(ps, "owner", owner, NpgsqlDbType.Text);
        P.AddValue(ps, "cursor", cursor, NpgsqlDbType.TimestampTz);
    }

    private readonly record struct LockedRecurring(DateTime DbNow, bool Exists, string? Owner, string? Cron, string? TimeZone, DateTime? LastFireAt);

    private async Task<LockedRecurring> LockRecurringAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string id, CancellationToken ct)
    {
        await using var batch = new NpgsqlBatch(conn, tx)
        {
            BatchCommands = { new NpgsqlBatchCommand("SELECT now()"), new NpgsqlBatchCommand(LockRecurringSql) },
        };
        P.Add(batch.BatchCommands[1].Parameters, "id", id);
        await using var r = await batch.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        var dbNow = r.GetDateTime(0);
        await r.NextResultAsync(ct);
        return await r.ReadAsync(ct)
            ? new LockedRecurring(dbNow, true, r.GetNullable<string>(0), r.GetString(1), r.GetString(2), r.GetNullableValue<DateTime>(3))
            : new LockedRecurring(dbNow, false, null, null, null, null);
    }
}
