using Npgsql;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres.Storage;

namespace PgBolt.Dashboard.Data;

internal sealed partial class DashboardStore
{
    // §6.1, §6.4: суммы снимка, живые серверы, строка stats и now() БД.
    private const string OverviewTemplate = """
        SELECT (SELECT coalesce(sum(scheduled), 0) FROM {schema}.queue_stats)::bigint,
               (SELECT coalesce(sum(enqueued), 0) FROM {schema}.queue_stats)::bigint,
               (SELECT coalesce(sum(processing), 0) FROM {schema}.queue_stats)::bigint,
               (SELECT coalesce(sum(awaiting), 0) FROM {schema}.queue_stats)::bigint,
               (SELECT count(*) FROM {schema}.servers WHERE heartbeat_at > now() - @stale)::int,
               t.next_run_at, t.last_run_at, t.last_error, now()
        FROM (SELECT 1) one LEFT JOIN {schema}.maintenance_tasks t ON t.name = 'stats'
        """;

    // 60 поминутных точек; без данных — одна строка с NULL.
    private const string HourTemplate = """
        SELECT c.current, s.bucket, s.succeeded, s.failed, s.retried
        FROM (SELECT date_trunc('minute', now()) AS current) c
        LEFT JOIN LATERAL (
            SELECT bucket, sum(succeeded)::bigint AS succeeded, sum(failed)::bigint AS failed, sum(retried)::bigint AS retried
            FROM {schema}.stats_minutes WHERE bucket > c.current - interval '60 minutes'
            GROUP BY bucket
        ) s ON true
        ORDER BY s.bucket
        """;

    // 96 точек по 15 минут.
    private const string DayTemplate = """
        SELECT c.current, s.slot, s.succeeded, s.failed, s.retried
        FROM (SELECT date_bin('15 minutes', now(), timestamptz '2000-01-01') AS current) c
        LEFT JOIN LATERAL (
            SELECT date_bin('15 minutes', bucket, timestamptz '2000-01-01') AS slot,
                   sum(succeeded)::bigint AS succeeded, sum(failed)::bigint AS failed, sum(retried)::bigint AS retried
            FROM {schema}.stats_minutes WHERE bucket >= c.current - interval '1425 minutes'
            GROUP BY 1
        ) s ON true
        ORDER BY s.slot
        """;

    // Очереди: снимок ∪ очереди живых серверов ∪ очереди с переопределением лимита или веса — иначе переопределение
    // очереди без задач и серверов не видно и не снимается из UI; строки только со значениями из кода (очередь могли убрать
    // из кода) список не пополняют.
    private const string QueuesTemplate = """
        WITH live AS (
            SELECT u.queue, count(*)::int AS servers
            FROM {schema}.servers s CROSS JOIN LATERAL unnest(s.queues) AS u(queue)
            WHERE s.heartbeat_at > now() - @stale
            GROUP BY u.queue
        ),
        names AS (
            SELECT queue FROM {schema}.queue_stats
            UNION SELECT queue FROM live
            UNION SELECT queue FROM {schema}.queue_settings WHERE override_concurrency_set OR override_weight_set
        )
        SELECT n.queue, coalesce(q.scheduled, 0), coalesce(q.enqueued, 0), coalesce(q.processing, 0),
               coalesce(q.awaiting, 0), coalesce(l.servers, 0), q.measured_at
        FROM names n LEFT JOIN {schema}.queue_stats q ON q.queue = n.queue LEFT JOIN live l ON l.queue = n.queue
        ORDER BY 1
        """;

    // Живое число взятых строк по индексу queue_leased; колонка Processing снимка остаётся согласованной со снимком.
    private const string LeasedTemplate = """
        SELECT queue, count(*)::int FROM {schema}.queue WHERE lease_owner IS NOT NULL AND queue = ANY(@queues) GROUP BY queue
        """;

    private const string ServersTemplate = """
        SELECT s.id, s.name, s.host, s.queues, s.capacity, s.started_at, s.heartbeat_at, s.heartbeat_at <= now() - @stale,
               coalesce(st.succeeded, 0), coalesce(st.failed, 0), coalesce(st.retried, 0)
        FROM {schema}.servers s
        LEFT JOIN (SELECT server_id, sum(succeeded)::bigint AS succeeded, sum(failed)::bigint AS failed, sum(retried)::bigint AS retried
                   FROM {schema}.stats_minutes WHERE bucket > now() - interval '1 hour' GROUP BY server_id) st ON st.server_id = s.id
        ORDER BY s.name, s.id
        """;

    // Часть 2 §5.7: идёт ли запуск.
    private const string MaintenanceTasksTemplate = """
        SELECT name, next_run_at, last_run_at, last_finished_at, last_error,
               last_run_at IS NOT NULL AND (last_finished_at IS NULL OR last_finished_at < last_run_at) AND next_run_at > now()
        FROM {schema}.maintenance_tasks ORDER BY name
        """;

    private const string JobTypeColumns =
        "name, args_schema::text, default_queue, default_max_attempts, default_priority, default_retry_delays, published_by, last_seen_at";

    private const string JobTypesTemplate = $"SELECT {JobTypeColumns} FROM {{schema}}.job_types ORDER BY name";
    private const string JobTypeTemplate = $"SELECT {JobTypeColumns} FROM {{schema}}.job_types WHERE name = @name";

    private string OverviewSql => field ??= Render(OverviewTemplate);
    private string HourSql => field ??= Render(HourTemplate);
    private string DaySql => field ??= Render(DayTemplate);
    private string QueuesSql => field ??= Render(QueuesTemplate);
    private string LeasedSql => field ??= Render(LeasedTemplate);
    private string ServersSql => field ??= Render(ServersTemplate);
    private string MaintenanceTasksSql => field ??= Render(MaintenanceTasksTemplate);
    private string JobTypesSql => field ??= Render(JobTypesTemplate);
    private string JobTypeSql => field ??= Render(JobTypeTemplate);

    private void BindStale(NpgsqlParameterCollection ps) => P.Add(ps, "stale", options.ServerStaleAfter);

    public async Task<OverviewData> GetOverviewAsync(CancellationToken ct)
    {
        var (totals, live, stats, dbNow) = await ReadAsync(OverviewSql, BindStale, static async (r, c) =>
        {
            await r.ReadAsync(c);
            var stats = r.IsDBNull(5) ? null : new StatsTaskRow(r.GetDateTime(5), r.GetNullableValue<DateTime>(6), r.GetNullable<string>(7));
            return (new QueueTotals(r.GetInt64(0), r.GetInt64(1), r.GetInt64(2), r.GetInt64(3)), r.GetInt32(4), stats, r.GetDateTime(8));
        }, ct);
        var lastHour = await ReadThroughputAsync(HourSql, TimeSpan.FromMinutes(1), 60, ct);
        return new OverviewData(totals, live, StatsFreshness.Evaluate(stats, dbNow, options.StatsStaleAfter), lastHour);
    }

    public Task<IReadOnlyList<ThroughputPoint>> GetThroughputDayAsync(CancellationToken ct) =>
        ReadThroughputAsync(DaySql, TimeSpan.FromMinutes(15), 96, ct);

    /// <summary>count точек шага step, последняя — текущая (столбец 0); бакеты без строк — нули.</summary>
    private Task<IReadOnlyList<ThroughputPoint>> ReadThroughputAsync(string sql, TimeSpan step, int count, CancellationToken ct) =>
        ReadAsync<IReadOnlyList<ThroughputPoint>>(sql, static _ => { }, async (r, c) =>
        {
            var current = default(DateTime);
            var buckets = new Dictionary<DateTime, ThroughputPoint>();
            while (await r.ReadAsync(c))
            {
                current = r.GetDateTime(0);
                if (!r.IsDBNull(1) && r.GetDateTime(1) is var bucket)
                    buckets[bucket] = new ThroughputPoint(bucket, r.GetInt64(2), r.GetInt64(3), r.GetInt64(4));
            }
            var points = new ThroughputPoint[count];
            for (var i = 0; i < count; i++)
            {
                var bucket = current - step * (count - 1 - i);
                points[i] = buckets.GetValueOrDefault(bucket) ?? new ThroughputPoint(bucket, 0, 0, 0);
            }
            return points;
        }, ct);

    public Task<IReadOnlyList<QueueRow>> ListQueuesAsync(CancellationToken ct) =>
        ReadAsync<IReadOnlyList<QueueRow>>(QueuesSql, BindStale, static async (r, c) => await r.ReadAllAsync(static r => new QueueRow(
            r.GetString(0), r.GetInt64(1), r.GetInt64(2), r.GetInt64(3), r.GetInt64(4), r.GetInt32(5), r.GetNullableValue<DateTime>(6)), c), ct);

    /// <summary>Взятые строки по очередям queues; очереди без взятых строк в ответе нет.</summary>
    public Task<IReadOnlyDictionary<string, int>> CountLeasedAsync(string[] queues, CancellationToken ct) =>
        ReadAsync<IReadOnlyDictionary<string, int>>(LeasedSql, ps => P.Add(ps, "queues", queues),
            static async (r, c) => (await r.ReadAllAsync(static r => (Queue: r.GetString(0), Count: r.GetInt32(1)), c))
                .ToDictionary(static x => x.Queue, static x => x.Count, StringComparer.Ordinal), ct);

    public async Task<ServersData> GetServersAsync(CancellationToken ct)
    {
        var servers = await ReadAsync(ServersSql, BindStale, static async (r, c) => await r.ReadAllAsync(static r => new ServerRow(
            r.GetGuid(0), r.GetString(1), r.GetString(2), r.GetFieldValue<string[]>(3), r.GetInt32(4), r.GetDateTime(5), r.GetDateTime(6),
            r.GetBoolean(7), r.GetInt64(8), r.GetInt64(9), r.GetInt64(10)), c), ct);
        var tasks = await ReadAsync(MaintenanceTasksSql, static _ => { }, static async (r, c) => await r.ReadAllAsync(static r => new MaintenanceTaskRow(
            r.GetString(0), r.GetDateTime(1), r.GetNullableValue<DateTime>(2), r.GetNullableValue<DateTime>(3), r.GetNullable<string>(4),
            r.GetBoolean(5)), c), ct);
        return new ServersData(servers, tasks);
    }

    public Task<IReadOnlyList<JobTypeRow>> ListJobTypesAsync(CancellationToken ct) =>
        ReadAsync<IReadOnlyList<JobTypeRow>>(JobTypesSql, static _ => { }, static async (r, c) => await r.ReadAllAsync(ReadJobType, c), ct);

    public Task<JobTypeRow?> GetJobTypeAsync(string name, CancellationToken ct) =>
        ReadAsync(JobTypeSql, ps => P.Add(ps, "name", name), static async (r, c) => await r.ReadAsync(c) ? ReadJobType(r) : null, ct);

    private static JobTypeRow ReadJobType(NpgsqlDataReader r) => new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetInt32(3),
        r.GetInt16(4), r.GetNullable<int[]>(5), r.GetString(6), r.GetDateTime(7));
}
