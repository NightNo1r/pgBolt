using Npgsql;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    // Общие фрагменты dequeue и dequeue с лимитом: lease взятой строки queue q и итоговый SELECT задач по CTE l
    // (столбцы в порядке ReadLeasedJob).
    private const string LeaseSet = """
        lease_owner = @server, lease_id = gen_random_uuid(), lease_until = now() + @lease,
                leased_at = now(), attempt = q.attempt + 1
        """;

    // Сколько задача ждала готовности: от run_at, а не от создания (отложенная задача не ждёт до своего срока). Именно
    // clock_timestamp(): now() — начало транзакции, а dequeue с лимитом может долго ждать advisory lock внутри неё.
    private const string WaitSecondsExpr =
        "greatest(0, extract(epoch FROM clock_timestamp() - q.run_at))::double precision AS wait_seconds";

    // При повреждённой паре queue/jobs ошибка внутри statement откатывает lease всей пачки.
    private const string LeasedColumns = """
        coalesce(j.id, {schema}.missing_job(l.job_id)) AS id,
                   l.lease_id, j.type, j.args, l.attempt, j.max_attempts, j.retry_delays,
                   j.parent_id, j.batch_id, j.trace_parent, j.recurring_id, j.scheduled_at, j.continuation, l.wait_seconds
        """;

    private const string JoinDefinitions = """
        LEFT JOIN {schema}.read_jobs((SELECT array_agg(job_id) FROM l)) j ON j.id = l.job_id
        """;

    private const string DequeueTemplate = $$"""
        WITH c AS (
            SELECT job_id FROM {schema}.queue
            WHERE queue = @queue AND lease_owner IS NULL AND ready
            ORDER BY priority DESC, run_at
            LIMIT @n
            FOR UPDATE SKIP LOCKED
        ),
        l AS (
            UPDATE {schema}.queue q
            SET {{LeaseSet}}
            FROM c WHERE q.job_id = c.job_id
            RETURNING q.job_id, q.lease_id, q.attempt, q.priority, q.run_at, {{WaitSecondsExpr}}
        )
        SELECT {{LeasedColumns}} FROM l {{JoinDefinitions}}
        ORDER BY l.priority DESC, l.run_at
        """;

    internal string DequeueSql => field ??= Render(DequeueTemplate);

    /// <summary>Берёт до <paramref name="max"/> готовых задач очереди в lease. max = число свободных слотов (без prefetch).</summary>
    public Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(max);
        return QueryAsync<IReadOnlyList<LeasedJob>>(DequeueSql, ps => BindDequeue(ps, queue, max, serverId, lease),
            async (r, c) => await r.ReadAllAsync(static r => ReadLeasedJob(r, 0), c), ct, DequeueExecuting);
    }

    internal static void BindDequeue(NpgsqlParameterCollection ps, string queue, int max, Guid serverId, TimeSpan lease)
    {
        P.Add(ps, "queue", queue);
        P.Add(ps, "n", max);
        P.Add(ps, "server", serverId);
        P.Add(ps, "lease", lease);
    }

    /// <summary>Столбцы задачи из итогового SELECT dequeue, начиная с <paramref name="o"/>.</summary>
    private static LeasedJob ReadLeasedJob(NpgsqlDataReader r, int o) => new(
        r.GetGuid(o), r.GetGuid(o + 1), r.GetString(o + 2), r.GetString(o + 3), r.GetInt32(o + 4), r.GetInt32(o + 5),
        r.GetNullable<int[]>(o + 6), r.GetNullableValue<Guid>(o + 7), r.GetNullableValue<Guid>(o + 8), r.GetNullable<string>(o + 9),
        r.GetNullable<string>(o + 10), r.GetNullableValue<DateTime>(o + 11), r.GetNullableValue<short>(o + 12),
        r.GetDouble(o + 13));
}
