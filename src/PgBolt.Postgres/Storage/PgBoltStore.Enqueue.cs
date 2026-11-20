using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    /// <summary>
    /// Общие части вариантов enqueue (§5.1): строки, уже записанные по id (повтор), и вход без них; строки queue без колонок
    /// графа; итог по строкам src. Шаблоны собираются из них без изменения текста statement'ов.
    /// </summary>
    private const string EnqueueInputCtes = """
        existing AS (
            SELECT id FROM {schema}.jobs WHERE id = ANY(@ids)
        ),
        input AS (
            SELECT * FROM src WHERE id NOT IN (SELECT id FROM existing)
        ),
        """;

    private const string EnqueueQueueCte = """
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key)
            SELECT id, @queue, @priority, r.run_at, r.run_at <= now(), @max_attempts, unique_key
            FROM input CROSS JOIN LATERAL (SELECT COALESCE(input.run_at, now() + @delay) AS run_at) r
            ORDER BY unique_key, id
            ON CONFLICT DO NOTHING
            RETURNING job_id
        ),
        """;

    private const string EnqueueResultSelect = """
        SELECT s.id,
               q.job_id IS NOT NULL AS created,
               e.id     IS NOT NULL AS existed
        FROM src s
        LEFT JOIN q        ON q.job_id = s.id
        LEFT JOIN existing e ON e.id = s.id
        """;

    private const string EnqueueTemplate = $$"""
        WITH src AS (
            SELECT * FROM unnest(@ids::uuid[], @args::jsonb[], @run_at::timestamptz[], @unique_keys::text[])
                     AS t(id, args, run_at, unique_key)
        ),
        {{EnqueueInputCtes}}
        {{EnqueueQueueCte}}
        j AS (
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, retry_delays,
                                       unique_key, trace_parent, created_at)
            SELECT i.id, @type, @queue, @priority, i.args, 0, @max_attempts, @retry_delays,
                   i.unique_key, @trace_parent, now()
            FROM input i JOIN q ON q.job_id = i.id
        )
        {{EnqueueResultSelect}}
        """;

    /// <summary>
    /// Часть 4 §8: вариант EnqueueTemplate для тика и Trigger — дополнительные массивы scheduled_at и recurring_id;
    /// scheduled_at = COALESCE(i.scheduled_at, now()) (Trigger передаёт NULL — время БД, §5.4). Клиент им не пользуется.
    /// </summary>
    private const string RecurringEnqueueTemplate = $$"""
        WITH src AS (
            SELECT * FROM unnest(@ids::uuid[], @args::jsonb[], @run_at::timestamptz[], @unique_keys::text[],
                                 @scheduled_at::timestamptz[], @recurring_ids::text[])
                     AS t(id, args, run_at, unique_key, scheduled_at, recurring_id)
        ),
        {{EnqueueInputCtes}}
        {{EnqueueQueueCte}}
        j AS (
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, retry_delays,
                                       unique_key, trace_parent, created_at, recurring_id, scheduled_at)
            SELECT i.id, @type, @queue, @priority, i.args, 0, @max_attempts, @retry_delays,
                   i.unique_key, @trace_parent, now(), i.recurring_id, COALESCE(i.scheduled_at, now())
            FROM input i JOIN q ON q.job_id = i.id
        )
        {{EnqueueResultSelect}}
        """;

    private string RecurringEnqueueSql => field ??= Render(RecurringEnqueueTemplate);

    /// <summary>
    /// Часть 5 §6.1 п. 2: вариант EnqueueTemplate для графа — поэлементные parent_id, continuation (= await_condition), notify,
    /// batch_id (в queue и jobs). Строка с continuation пишется Awaiting: ready = false, run_at = 'infinity' (инвариант §4).
    /// </summary>
    private const string WorkflowEnqueueTemplate = $$"""
        WITH src AS (
            SELECT * FROM unnest(@ids::uuid[], @args::jsonb[], @run_at::timestamptz[], @unique_keys::text[],
                                 @parent_ids::uuid[], @continuations::smallint[], @notify::boolean[], @batch_ids::uuid[])
                     AS t(id, args, run_at, unique_key, parent_id, continuation, notify, batch_id)
        ),
        {{EnqueueInputCtes}}
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key,
                                        parent_id, await_condition, batch_id, notify)
            SELECT id, @queue, @priority, r.run_at, input.continuation IS NULL AND r.run_at <= now(), @max_attempts, unique_key,
                   parent_id, continuation, batch_id, notify
            FROM input CROSS JOIN LATERAL (
                SELECT CASE WHEN input.continuation IS NULL THEN COALESCE(input.run_at, now() + @delay)
                            ELSE 'infinity'::timestamptz END AS run_at) r
            ORDER BY unique_key, id
            ON CONFLICT DO NOTHING
            RETURNING job_id
        ),
        j AS (
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, retry_delays,
                                       unique_key, trace_parent, created_at, parent_id, batch_id, continuation, notify)
            SELECT i.id, @type, @queue, @priority, i.args, 0, @max_attempts, @retry_delays,
                   i.unique_key, @trace_parent, now(), i.parent_id, i.batch_id, i.continuation, i.notify
            FROM input i JOIN q ON q.job_id = i.id
        )
        {{EnqueueResultSelect}}
        """;

    private string WorkflowEnqueueSql => field ??= Render(WorkflowEnqueueTemplate);

    /// <summary>§8 части 4, §6.1 части 5: массивы графа — вариант с колонками workflow; RecurringIds — с колонками расписания; иначе прежний SQL клиента.</summary>
    internal string EnqueueSqlFor(EnqueueBatch batch) =>
        batch.ParentIds is not null ? WorkflowEnqueueSql : batch.RecurringIds is null ? EnqueueSql : RecurringEnqueueSql;

    private const string ResolveTemplate = """
        SELECT s.id, j.id IS NOT NULL AS exists_by_id, q.job_id AS existing_id
        FROM unnest(@ids::uuid[], @unique_keys::text[]) AS s(id, unique_key)
        LEFT JOIN {schema}.jobs  j ON j.id = s.id AND j.id = ANY(@ids)
        LEFT JOIN {schema}.queue q ON q.unique_key = s.unique_key
        """;

    internal string EnqueueSql => field ??= Render(EnqueueTemplate);
    private string ResolveSql => field ??= Render(ResolveTemplate);

    public async Task<IReadOnlyList<EnqueueRowResult>> EnqueueAsync(EnqueueBatch batch, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        batch.Validate();

        if (transaction is not null)
        {
            // Внутри пользовательской транзакции повторов нет (§9).
            var conn = transaction.Connection ?? throw new InvalidOperationException("The transaction is already completed.");
            return await EnqueueOnceAsync(conn, transaction, batch, ct);
        }

        var transientRetried = false;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var conn = await _dataSource.OpenConnectionAsync(ct);
                return await EnqueueOnceAsync(conn, null, batch, ct);
            }
            catch (PostgresException e) when (attempt < MaxStatementAttempts
                                              && (IsJobsPrimaryKeyViolation(e) || (_retryOnConflict && DbRetry.IsRetryable(e))))
            {
                // 23505 по PK jobs: исходная попытка закоммитилась между снимком existing и вставкой — повтор увидит её (§5.1).
            }
            catch (NpgsqlException e) when (e is not PostgresException && e.IsTransient && !transientRetried)
            {
                transientRetried = true;
            }
        }
    }

    private static bool IsJobsPrimaryKeyViolation(PostgresException e) =>
        e.SqlState == PostgresErrorCodes.UniqueViolation
        && e.TableName is { } table
        && (table == "jobs" || table.StartsWith("jobs_", StringComparison.Ordinal));

    private async Task<IReadOnlyList<EnqueueRowResult>> EnqueueOnceAsync(
        NpgsqlConnection conn, NpgsqlTransaction? tx, EnqueueBatch b, CancellationToken ct)
    {
        var index = new Dictionary<Guid, int>(b.Ids.Length);
        for (var i = 0; i < b.Ids.Length; i++)
            index[b.Ids[i]] = i;

        var created = new bool[b.Ids.Length];
        var existed = new bool[b.Ids.Length];
        await using (var cmd = new NpgsqlCommand(EnqueueSqlFor(b), conn, tx))
        {
            BindEnqueue(cmd.Parameters, b);
            await PrepareAsync(cmd, ct);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var i = index[r.GetGuid(0)];
                created[i] = r.GetBoolean(1);
                existed[i] = r.GetBoolean(2);
            }
        }

        var results = new EnqueueRowResult[b.Ids.Length];
        var unresolved = new List<int>();
        for (var i = 0; i < results.Length; i++)
        {
            if (created[i] || existed[i])
                results[i] = new EnqueueRowResult(b.Ids[i], true);
            else
                unresolved.Add(i);
        }
        if (unresolved.Count == 0)
            return results;

        // Конфликт в queue: по unique_key или по PK с ещё не видимой исходной транзакцией. Новый снимок (§5.1).
        await using (var cmd = new NpgsqlCommand(ResolveSql, conn, tx))
        {
            P.Add(cmd.Parameters, "ids", unresolved.Select(i => b.Ids[i]).ToArray());
            P.Add(cmd.Parameters, "unique_keys", unresolved.Select(i => b.UniqueKeys[i]).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Text);
            await PrepareAsync(cmd, ct);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var i = index[r.GetGuid(0)];
                var existingId = r.GetNullableValue<Guid>(2);
                results[i] = r.GetBoolean(1)
                    ? new EnqueueRowResult(b.Ids[i], true)
                    : new EnqueueRowResult(existingId, false);
            }
        }
        return results;
    }

    internal static void BindEnqueue(NpgsqlParameterCollection ps, EnqueueBatch b)
    {
        P.Add(ps, "ids", b.Ids);
        P.Add(ps, "args", b.ArgsJson, NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
        P.Add(ps, "run_at", b.RunAtUtc, NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "unique_keys", b.UniqueKeys, NpgsqlDbType.Array | NpgsqlDbType.Text);
        if (b.RecurringIds is { } recurringIds)
        {
            P.Add(ps, "scheduled_at", b.ScheduledAtUtc ?? new DateTime?[b.Ids.Length], NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
            P.Add(ps, "recurring_ids", recurringIds, NpgsqlDbType.Array | NpgsqlDbType.Text);
        }
        if (b.ParentIds is { } parents)
        {
            P.Add(ps, "parent_ids", parents, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
            P.Add(ps, "continuations", b.Continuations!, NpgsqlDbType.Array | NpgsqlDbType.Smallint);
            P.Add(ps, "notify", b.Notify!);
            P.Add(ps, "batch_ids", b.BatchIds!, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        }
        P.Add(ps, "type", b.Type);
        P.Add(ps, "queue", b.Queue);
        P.Add(ps, "priority", b.Priority);
        P.Add(ps, "max_attempts", b.MaxAttempts);
        P.AddNullable(ps, "retry_delays", b.RetryDelaysSeconds, NpgsqlDbType.Array | NpgsqlDbType.Integer);
        P.AddNullable(ps, "trace_parent", b.TraceParent, NpgsqlDbType.Text);
        P.Add(ps, "delay", b.Delay);
    }

    /// <summary>
    /// Часть 4 §5.2: statement enqueue для NpgsqlBatch тика — в его транзакции, без запроса разрешения конфликта и без
    /// повторов. Строки результата — (id, created, existed); id свежие, поэтому !created — конфликт по unique_key.
    /// </summary>
    internal NpgsqlBatchCommand CreateEnqueueCommand(EnqueueBatch batch)
    {
        batch.Validate();
        var cmd = new NpgsqlBatchCommand(EnqueueSqlFor(batch));
        BindEnqueue(cmd.Parameters, batch);
        return cmd;
    }
}
