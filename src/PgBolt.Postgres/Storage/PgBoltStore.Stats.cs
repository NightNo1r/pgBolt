using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string FlushOutcomeStatsTemplate = """
        INSERT INTO {schema}.stats_minutes AS s (bucket, server_id, queue, succeeded, failed, retried)
        SELECT * FROM unnest(@buckets::timestamptz[], @servers::uuid[], @queues::text[], @succeeded::int[], @failed::int[], @retried::int[])
        ON CONFLICT (bucket, server_id, queue) DO UPDATE
            SET succeeded = s.succeeded + EXCLUDED.succeeded,
                failed    = s.failed    + EXCLUDED.failed,
                retried   = s.retried   + EXCLUDED.retried
        """;

    private const string SnapshotQueueStatsTemplate = """
        WITH s AS (
            SELECT queue,
                   count(*) FILTER (WHERE lease_owner IS NULL AND NOT ready AND await_condition IS NULL) AS scheduled,
                   count(*) FILTER (WHERE lease_owner IS NULL AND ready)                                 AS enqueued,
                   count(*) FILTER (WHERE lease_owner IS NOT NULL)                                       AS processing,
                   count(*) FILTER (WHERE await_condition IS NOT NULL)                                   AS awaiting,
                   extract(epoch FROM now() - min(run_at) FILTER (WHERE lease_owner IS NULL AND ready))::double precision
                                                                                                         AS oldest_ready_seconds
            FROM {schema}.queue GROUP BY queue
        ), up AS (
            INSERT INTO {schema}.queue_stats (queue, scheduled, enqueued, processing, awaiting, oldest_ready_seconds, measured_at)
            SELECT s.*, now() FROM s ORDER BY s.queue
            ON CONFLICT (queue) DO UPDATE SET scheduled = EXCLUDED.scheduled, enqueued = EXCLUDED.enqueued,
                processing = EXCLUDED.processing, awaiting = EXCLUDED.awaiting,
                oldest_ready_seconds = EXCLUDED.oldest_ready_seconds, measured_at = EXCLUDED.measured_at
        )
        DELETE FROM {schema}.queue_stats q WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.queue = q.queue)
        """;

    // Один statement: строки queue_stats и строка stats берутся из одного MVCC-представления. Момент снимка — последний успешный:
    // проход без ошибки (last_finished_at) либо measured_at закоммиченных строк, если проход упал уже после коммита снимка.
    // greatest пропускает NULL, а greatest(0, ...) применяется только к известному моменту. Пустой queue_stats даёт одну строку
    // с queue = NULL, из неё читается только возраст.
    private const string ReadQueueStatsTemplate = """
        WITH f AS (
            SELECT greatest(CASE WHEN t.last_error IS NULL THEN t.last_finished_at END,
                            (SELECT max(measured_at) FROM {schema}.queue_stats)) AS at
            FROM (SELECT 1) one LEFT JOIN {schema}.maintenance_tasks t ON t.name = 'stats'
        )
        SELECT q.queue, q.scheduled, q.enqueued, q.processing, q.awaiting, q.oldest_ready_seconds,
               CASE WHEN f.at IS NOT NULL THEN greatest(0, extract(epoch FROM now() - f.at))::double precision END AS age_seconds
        FROM f LEFT JOIN {schema}.queue_stats q ON true
        """;

    private const string ReadTableStatsSql = """
        SELECT c.relname, coalesce(s.n_dead_tup, 0)::bigint, pg_total_relation_size(c.oid)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE n.nspname = @schema AND c.relname IN ('queue', 'completions')
        """;

    private const string TrimStatsTemplate = """
        DELETE FROM {schema}.stats_minutes
        WHERE ctid IN (SELECT ctid FROM {schema}.stats_minutes WHERE bucket < now() - @retention LIMIT @limit)
        """;

    private string FlushOutcomeStatsSql => field ??= Render(FlushOutcomeStatsTemplate);
    private string SnapshotQueueStatsSql => field ??= Render(SnapshotQueueStatsTemplate);
    private string ReadQueueStatsSql => field ??= Render(ReadQueueStatsTemplate);
    private string TrimStatsSql => field ??= Render(TrimStatsTemplate);

    /// <summary>Сброс счётчиков исходов одним upsert'ом (часть 6 §4.1); пустой список — без обращения к БД.</summary>
    /// <remarks>
    /// Предусловия: ключ (BucketUtc, ServerId, Queue) уникален в пакете (иначе 21000 — ON CONFLICT дважды по одной строке),
    /// BucketUtc — UTC (иначе Npgsql отклоняет параметр timestamptz). Такой пакет не запишется ни при каком повторе:
    /// вызывающий, возвращающий строки на повтор, повторял бы ошибку бесконечно.
    /// </remarks>
    internal Task FlushOutcomeStatsAsync(IReadOnlyList<OutcomeStatsRow> rows, CancellationToken ct)
    {
        var n = rows.Count;
        if (n == 0)
            return Task.CompletedTask;
        var (buckets, servers, queues) = (new DateTime[n], new Guid[n], new string[n]);
        var (succeeded, failed, retried) = (new int[n], new int[n], new int[n]);
        for (var i = 0; i < n; i++)
            (buckets[i], servers[i], queues[i], succeeded[i], failed[i], retried[i]) = rows[i];
        return NonQueryAsync(FlushOutcomeStatsSql, ps =>
        {
            P.Add(ps, "buckets", buckets, NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
            P.Add(ps, "servers", servers, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
            P.Add(ps, "queues", queues, NpgsqlDbType.Array | NpgsqlDbType.Text);
            P.Add(ps, "succeeded", succeeded, NpgsqlDbType.Array | NpgsqlDbType.Integer);
            P.Add(ps, "failed", failed, NpgsqlDbType.Array | NpgsqlDbType.Integer);
            P.Add(ps, "retried", retried, NpgsqlDbType.Array | NpgsqlDbType.Integer);
        }, ct);
    }

    /// <summary>Снимок живых состояний по очередям в queue_stats, исчезнувшие очереди удаляются (часть 6 §4.2 п. 1).</summary>
    internal Task SnapshotQueueStatsAsync(CancellationToken ct) => NonQueryAsync(SnapshotQueueStatsSql, _ => { }, ct);

    /// <summary>Строки queue_stats и возраст последнего успешного снимка одним запросом.</summary>
    internal Task<QueueStatsSnapshot> ReadQueueStatsAsync(CancellationToken ct) =>
        QueryAsync(ReadQueueStatsSql, _ => { }, async (r, c) =>
        {
            var rows = new List<QueueStatsRow>();
            double? age = null;
            while (await r.ReadAsync(c))
            {
                age = r.GetNullableValue<double>(6);
                if (!r.IsDBNull(0))
                    rows.Add(new QueueStatsRow(r.GetString(0), r.GetInt64(1), r.GetInt64(2), r.GetInt64(3), r.GetInt64(4),
                        r.GetNullableValue<double>(5)));
            }
            return new QueueStatsSnapshot(rows, age);
        }, ct);

    /// <summary>Мёртвые строки и размер таблиц queue и completions своей схемы (признаки bloat).</summary>
    internal Task<IReadOnlyList<TableStatsRow>> ReadTableStatsAsync(CancellationToken ct) =>
        QueryAsync<IReadOnlyList<TableStatsRow>>(ReadTableStatsSql, ps => P.Add(ps, "schema", Schema),
            async (r, c) => await r.ReadAllAsync(static r => new TableStatsRow(r.GetString(0), r.GetInt64(1), r.GetInt64(2)), c), ct);

    /// <summary>Одна порция retention stats_minutes (часть 6 §4.2 п. 2); возвращает число удалённых строк.</summary>
    internal Task<int> TrimStatsAsync(TimeSpan retention, int limit, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(retention, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        return NonQueryAsync(TrimStatsSql, ps =>
        {
            P.Add(ps, "retention", retention);
            P.Add(ps, "limit", limit);
        }, ct);
    }
}
