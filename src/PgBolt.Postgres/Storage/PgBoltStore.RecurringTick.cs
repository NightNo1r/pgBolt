using Npgsql;
using NpgsqlTypes;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Тик расписаний (часть 4 §5.2): захват FOR UPDATE SKIP LOCKED, планирование в .NET, enqueue групп одним NpgsqlBatch и
/// сдвиг курсоров — в одной транзакции, поэтому на срабатывание в кластере ставится не больше одной задачи.
/// </summary>
public sealed partial class PgBoltStore
{
    private const string TickClaimTemplate = """
        SELECT r.id, r.type, r.args, r.cron, r.time_zone, r.misfire, r.catch_up_window, r.overlap, r.next_fire_at,
               COALESCE(r.queue,        t.default_queue),
               COALESCE(r.priority,     t.default_priority),
               COALESCE(r.max_attempts, t.default_max_attempts),
               COALESCE(r.retry_delays, t.default_retry_delays),
               t.name IS NULL,
               now()
        FROM {schema}.recurring_jobs r
        LEFT JOIN {schema}.job_types t ON t.name = r.type
        WHERE r.wake_at <= now() AND NOT r.paused
        ORDER BY r.wake_at
        LIMIT @n
        FOR UPDATE OF r SKIP LOCKED
        """;

    private const string TickUpdateTemplate = """
        UPDATE {schema}.recurring_jobs r
        SET next_fire_at = u.next_fire_at, wake_at = u.wake_at,
            last_fire_at = COALESCE(u.last_fire_at, r.last_fire_at),
            last_job_id  = COALESCE(u.last_job_id,  r.last_job_id),
            -- §3, §7: ошибка строки пишется, успешное срабатывание очищает; пропуск и ожидание без срабатывания не трогают
            last_error   = CASE WHEN u.last_error IS NOT NULL OR u.last_fire_at IS NOT NULL THEN u.last_error ELSE r.last_error END
        FROM unnest(@ids::text[], @next::timestamptz[], @wake::timestamptz[],
                    @fired::timestamptz[], @job_ids::uuid[], @errors::text[])
             AS u(id, next_fire_at, wake_at, last_fire_at, last_job_id, last_error)
        WHERE r.id = u.id
        """;

    private string TickClaimSql => field ??= Render(TickClaimTemplate);
    private string TickUpdateSql => field ??= Render(TickUpdateTemplate);

    /// <summary>
    /// Строка захвата как есть: значения, которые могут не читаться в .NET-типы или не проходить проверку enqueue (строка
    /// записана SQL'ем в обход API), читаются в «сырые» типы и проверяются в <see cref="ToTickRow"/> — внутри обработки строки,
    /// чтобы такая строка была ошибкой строки, а не откатом тика (§2 «Ошибка строки», §5.2 п. 2).
    /// </summary>
    private sealed record ClaimedRow(string Id, string Type, string ArgsJson, string Cron, string TimeZone, short Misfire,
        NpgsqlInterval CatchUpWindow, short Overlap, DateTime? NextFireAt, string? Queue, short? Priority, int? MaxAttempts,
        int?[]? RetryDelays, bool UnknownType)
    {
        /// <summary>Исключение — ошибка строки: после проверки EnqueueBatch.Validate не может бросить из-за данных этой строки.</summary>
        public TickRow ToTickRow()
        {
            if (UnknownType)
                throw new UnknownJobException(Type);
            if (Id.Length == 0)
                throw Invalid("id is empty");
            var misfire = (MisfirePolicy)Misfire;
            if (!Enum.IsDefined(misfire))
                throw Invalid($"unknown misfire policy {Misfire}");
            var overlap = (OverlapPolicy)Overlap;
            if (!Enum.IsDefined(overlap))
                throw Invalid($"unknown overlap policy {Overlap}");
            var window = CatchUpWindow.ToTimeSpanOrNull()
                         ?? throw Invalid($"catch_up_window ({CatchUpWindow.Months} months, {CatchUpWindow.Days} days, " +
                                          $"{CatchUpWindow.Time} µs) has months or does not fit TimeSpan");
            // §4.2: окно в (0, 366 суток]. Верхняя граница не проверяется: большое окно корректно, а нулевое или
            // отрицательное CatchUp молча превращало бы в Skip.
            if (window <= TimeSpan.Zero)
                throw Invalid($"catch_up_window {window} must be positive");
            if (string.IsNullOrEmpty(Queue))
                throw Invalid("queue is empty");
            if (Priority is not { } priority)
                throw Invalid("priority is null");
            if (MaxAttempts is not { } maxAttempts || maxAttempts < 1)
                throw Invalid($"max_attempts {MaxAttempts?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "NULL"} must be >= 1");
            int[]? delays = null;
            if (RetryDelays is { } raw)
            {
                delays = new int[raw.Length];
                for (var i = 0; i < raw.Length; i++)
                    delays[i] = raw[i] is { } d and >= 0 ? d : throw Invalid("retry_delays must contain only non-null, non-negative values");
            }
            return new TickRow(Id, Type, ArgsJson, Cron, TimeZone, misfire, window, overlap, NextFireAt, Queue, priority, maxAttempts, delays);
        }

        private InvalidDataException Invalid(string reason) => new($"Recurring job '{Id}': {reason}.");
    }

    private sealed record TickRow(string Id, string Type, string ArgsJson, string Cron, string TimeZone, MisfirePolicy Misfire,
        TimeSpan CatchUpWindow, OverlapPolicy Overlap, DateTime? NextFireAt, string Queue, short Priority, int MaxAttempts,
        int[]? RetryDelaysSeconds);

    private sealed record PlannedRow(TickRow Row, PlanInput Input, FirePlan Plan, Guid[] JobIds);

    private readonly record struct CursorUpdate(string Id, DateTime? Next, DateTime? Wake, DateTime? FiredAt, Guid? JobId, string? Error);

    /// <summary>
    /// Только для тестов (§10): вызывается в транзакции тика с захваченными строками — после enqueue и обновления курсоров,
    /// перед COMMIT. Позволяет удержать тик и проверить конкуренцию с API и синхронизацией. В рабочем коде — null.
    /// </summary>
    internal Func<CancellationToken, Task>? TickBeforeCommit { get; set; }

    internal Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxFires);
        return RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);

            var (rows, dbNow) = await ClaimRecurringAsync(conn, tx, batchSize, c);
            if (rows.Count == 0)
            {
                await tx.CommitAsync(c);
                return RecurringTickResult.Empty;
            }

            // 2. Планирование: ошибка строки не откатывает пакет; строка, не поместившаяся в лимит, и все следующие не трогаются.
            var full = rows.Count == batchSize;
            var budget = maxFires;
            var planned = new List<PlannedRow>();
            var updates = new List<CursorUpdate>();
            var errors = new List<RecurringRowError>();
            foreach (var claimed in rows)
            {
                TickRow row;
                PlanInput input;
                FirePlan plan;
                try
                {
                    row = claimed.ToTickRow();
                    input = new PlanInput(CronSchedule.Parse(row.Cron, row.TimeZone), row.Misfire, row.CatchUpWindow, row.Overlap, row.NextFireAt);
                    plan = RecurringPlanner.Plan(input, dbNow, misfireThreshold);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    var error = $"{e.GetType().Name}: {e.Message}";
                    errors.Add(new RecurringRowError(claimed.Id, error));
                    updates.Add(new CursorUpdate(claimed.Id, claimed.NextFireAt,
                        claimed.NextFireAt is null ? null : dbNow + RecurringPlanner.RowErrorDelay, null, null, error));
                    continue;
                }
                if (plan.Fires.Count > budget)
                {
                    full = true;
                    break;
                }
                budget -= plan.Fires.Count;
                planned.Add(new PlannedRow(row, input, plan, plan.Fires.Select(_ => Guid.CreateVersion7()).ToArray()));
            }

            // 3. Enqueue: одна команда на группу (type, queue, priority, max_attempts, retry_delays), все — одним NpgsqlBatch.
            var created = await EnqueueFiresAsync(conn, tx, planned, c);

            // 4. Курсоры: при CatchUp + Skip курсор зависит от конфликта по ключу, поэтому после enqueue.
            var queues = new HashSet<string>(StringComparer.Ordinal);
            var missed = new List<RecurringMissed>();
            var overlapSkipped = new List<string>();
            var waiting = new List<string>();
            var exhausted = new List<string>();
            var fired = 0;
            foreach (var p in planned)
            {
                DateTime? lastFired = null;
                Guid? lastJob = null;
                var conflict = false;
                for (var i = 0; i < p.JobIds.Length; i++)
                {
                    if (created.GetValueOrDefault(p.JobIds[i]))
                    {
                        lastFired = p.Plan.Fires[i];
                        lastJob = p.JobIds[i];
                        fired++;
                        queues.Add(p.Row.Queue);
                    }
                    else
                    {
                        conflict = true;
                    }
                }

                if (conflict && p.Row.Misfire == MisfirePolicy.CatchUp && p.Row.Overlap == OverlapPolicy.Skip)
                {
                    var wait = RecurringPlanner.OnConflict(p.Input, dbNow);
                    updates.Add(new CursorUpdate(p.Row.Id, wait.NextFireAt, wait.WakeAt, null, null, null));
                    waiting.Add(p.Row.Id);
                    continue;
                }
                if (conflict)
                    overlapSkipped.Add(p.Row.Id);
                if (p.Plan.Missed is { } m)
                    missed.Add(new RecurringMissed(p.Row.Id, m.FromUtc, m.ToUtc));
                if (p.Plan.NextFireAt is null && p.Row.NextFireAt is not null)
                    exhausted.Add(p.Row.Id);
                updates.Add(new CursorUpdate(p.Row.Id, p.Plan.NextFireAt, p.Plan.WakeAt, lastFired, lastJob, null));
            }

            if (updates.Count > 0)
                await UpdateCursorsAsync(conn, tx, updates, c);
            if (TickBeforeCommit is { } hook)
                await hook(c);
            await tx.CommitAsync(c);
            return new RecurringTickResult(rows.Count, fired, full, queues, errors, missed, overlapSkipped, waiting, exhausted);
        }, ct);
    }

    private async Task<(List<ClaimedRow> Rows, DateTime DbNow)> ClaimRecurringAsync(NpgsqlConnection conn, NpgsqlTransaction tx, int batchSize,
        CancellationToken ct)
    {
        var rows = new List<ClaimedRow>();
        DateTime dbNow = default;
        await using var cmd = new NpgsqlCommand(TickClaimSql, conn, tx);
        P.Add(cmd.Parameters, "n", batchSize);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            rows.Add(new ClaimedRow(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4),
                r.GetInt16(5), r.GetFieldValue<NpgsqlInterval>(6), r.GetInt16(7),
                r.GetNullableValue<DateTime>(8), r.GetNullable<string>(9), r.GetNullableValue<short>(10),
                r.GetNullableValue<int>(11), r.GetNullable<int?[]>(12), r.GetBoolean(13)));
            dbNow = r.GetDateTime(14);
        }
        return (rows, dbNow);
    }

    /// <summary>id → created. Id свежие, поэтому !created — конфликт по unique_key (жив предыдущий запуск), без запроса разрешения.</summary>
    private async Task<Dictionary<Guid, bool>> EnqueueFiresAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<PlannedRow> planned,
        CancellationToken ct)
    {
        var created = new Dictionary<Guid, bool>();
        var groups = planned
            .Where(p => p.JobIds.Length > 0)
            .GroupBy(p => (p.Row.Type, p.Row.Queue, p.Row.Priority, p.Row.MaxAttempts,
                Retry: p.Row.RetryDelaysSeconds is { } d ? string.Join(',', d) : "-"))
            .ToList();
        if (groups.Count == 0)
            return created;

        await using var batch = new NpgsqlBatch(conn, tx);
        foreach (var g in groups)
        {
            var fires = g.SelectMany(p => p.Plan.Fires.Select((at, i) => (p.Row, At: at, Id: p.JobIds[i]))).ToList();
            batch.BatchCommands.Add(CreateEnqueueCommand(new EnqueueBatch(g.Key.Type, g.Key.Queue, g.Key.Priority, g.Key.MaxAttempts,
                g.First().Row.RetryDelaysSeconds, TraceParent: null, TimeSpan.Zero,
                fires.Select(f => f.Id).ToArray(), fires.Select(f => f.Row.ArgsJson).ToArray(), new DateTime?[fires.Count],
                fires.Select(f => f.Row.Overlap == OverlapPolicy.Skip ? ReservedUniqueKeys.Recurring(f.Row.Id) : null).ToArray())
            {
                RecurringIds = fires.Select(f => f.Row.Id).ToArray(),
                ScheduledAtUtc = fires.Select(f => (DateTime?)f.At).ToArray(),
            }));
        }

        await using var r = await batch.ExecuteReaderAsync(ct);
        do
        {
            while (await r.ReadAsync(ct))
                created[r.GetGuid(0)] = r.GetBoolean(1);
        }
        while (await r.NextResultAsync(ct));
        return created;
    }

    private async Task UpdateCursorsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<CursorUpdate> updates, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(TickUpdateSql, conn, tx);
        var ps = cmd.Parameters;
        P.Add(ps, "ids", updates.Select(u => u.Id).ToArray());
        P.Add(ps, "next", updates.Select(u => u.Next).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "wake", updates.Select(u => u.Wake).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "fired", updates.Select(u => u.FiredAt).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "job_ids", updates.Select(u => u.JobId).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "errors", updates.Select(u => u.Error).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Text);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
