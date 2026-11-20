using Npgsql;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string AckTemplate = """
        WITH l AS MATERIALIZED (
            SELECT job_id FROM {schema}.queue
            WHERE job_id = ANY(@ids) AND lease_id = ANY(@lease_ids)
            ORDER BY job_id
            FOR UPDATE
        ),
        d AS (
            DELETE FROM {schema}.queue q USING l
            WHERE q.job_id = l.job_id
            RETURNING q.job_id, q.attempt, q.leased_at, q.notify
        ),
        j AS (
            UPDATE {schema}.jobs j
            SET state = 2, attempt = d.attempt, started_at = d.leased_at, finished_at = now(), server_id = @server,
                notify = j.notify OR d.notify
            FROM d
            WHERE j.id = d.job_id AND j.id = ANY(@ids)
            RETURNING j.id, j.batch_id, d.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 2, batch_id FROM j WHERE notify
            RETURNING 1
        )
        SELECT array(SELECT id FROM j) AS acked, (SELECT count(*) FROM s) AS signals
        """;

    internal string AckSql => field ??= Render(AckTemplate);

    /// <summary>
    /// Пакетный ack (§5.3). Задачи с потерянным lease игнорируются. Возвращает id подтверждённых задач (часть 6 §4.1);
    /// сигналы — часть 5 §6.3.
    /// </summary>
    public Task<IReadOnlyList<Guid>> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct)
    {
        if (leases.Count == 0)
            return Task.FromResult<IReadOnlyList<Guid>>([]);
        var ids = new Guid[leases.Count];
        var leaseIds = new Guid[leases.Count];
        for (var i = 0; i < leases.Count; i++)
            (ids[i], leaseIds[i]) = (leases[i].JobId, leases[i].LeaseId);

        return QueryAndSignalAsync<IReadOnlyList<Guid>>(AckSql, ps => BindAck(ps, ids, leaseIds, serverId), async (r, c) =>
        {
            await r.ReadAsync(c);
            return (r.GetFieldValue<Guid[]>(0), r.GetInt64(1));
        }, ct);
    }

    internal static void BindAck(NpgsqlParameterCollection ps, Guid[] ids, Guid[] leaseIds, Guid serverId)
    {
        P.Add(ps, "ids", ids);
        P.Add(ps, "lease_ids", leaseIds);
        P.Add(ps, "server", serverId);
    }
}
