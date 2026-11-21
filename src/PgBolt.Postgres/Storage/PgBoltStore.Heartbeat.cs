namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string HeartbeatTemplate = """
        WITH s AS (
            UPDATE {schema}.servers SET heartbeat_at = now() WHERE id = @server
            RETURNING 1
        ),
        l AS MATERIALIZED (
            SELECT job_id FROM {schema}.queue
            WHERE job_id = ANY(@ids) AND lease_id = ANY(@lease_ids) AND lease_owner = @server
            ORDER BY job_id                                -- порядок захвата core §5.3 не меняется
            FOR UPDATE
        ),
        u AS (
            UPDATE {schema}.queue q SET lease_until = now() + @lease
            FROM l WHERE q.job_id = l.job_id
            RETURNING q.job_id, q.lease_id, q.cancel_requested
        )
        SELECT EXISTS (SELECT 1 FROM s) AS server_present, u.job_id, u.lease_id, u.cancel_requested
        FROM (VALUES (1)) AS one(x) LEFT JOIN u ON true
        """;

    internal string HeartbeatSql => field ??= Render(HeartbeatTemplate);

    /// <summary>
    /// Renews listed leases matching this server and lease_id, updates servers.heartbeat_at, and delivers cancellation requests (Part 3 §8.1). A
    /// requested lease missing from the response has been lost. ServerPresent = false means cleanup removed the server row and the server must
    /// register again (Part 2 §9.2).
    /// </summary>
    public Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(leases);
        var ids = new Guid[leases.Count];
        var leaseIds = new Guid[leases.Count];
        var i = 0;
        foreach (var l in leases)
        {
            ids[i] = l.JobId;
            leaseIds[i] = l.LeaseId;
            i++;
        }

        return QueryAsync(HeartbeatSql, ps =>
        {
            P.Add(ps, "server", serverId);
            P.Add(ps, "ids", ids);
            P.Add(ps, "lease_ids", leaseIds);
            P.Add(ps, "lease", lease);
        }, static async (r, c) =>
        {
            var present = false;
            var alive = new List<HeartbeatLease>();
            while (await r.ReadAsync(c))
            {
                present = r.GetBoolean(0);
                if (!r.IsDBNull(1))
                    alive.Add(new HeartbeatLease(r.GetGuid(1), r.GetGuid(2), r.GetBoolean(3)));
            }
            return new HeartbeatResult(present, alive);
        }, ct);
    }
}
