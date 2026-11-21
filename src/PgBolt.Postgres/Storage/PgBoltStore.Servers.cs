namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string RegisterServerTemplate = """
        INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
        VALUES (@id, @name, @host, @queues, @capacity, now(), now())
        ON CONFLICT (id) DO UPDATE
            SET name = EXCLUDED.name, host = EXCLUDED.host, queues = EXCLUDED.queues,
                capacity = EXCLUDED.capacity, heartbeat_at = now();
        """;

    private const string RemoveServerTemplate = """
        DELETE FROM {schema}.servers WHERE id = @id;
        """;

    private string RegisterServerSql => field ??= Render(RegisterServerTemplate);
    private string RemoveServerSql => field ??= Render(RemoveServerTemplate);

    /// <summary>
    /// Registers a virtual server or restores its row with an upsert, preserving an existing started_at (Part 3 §7.2, Part 2 §9.2). The host is
    /// Environment.MachineName.
    /// </summary>
    public Task RegisterServerAsync(Guid serverId, string name, string host, string[] queues, int capacity, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(queues);
        return NonQueryAsync(RegisterServerSql, ps =>
        {
            P.Add(ps, "id", serverId);
            P.Add(ps, "name", name);
            P.Add(ps, "host", host);
            P.Add(ps, "queues", queues);
            P.Add(ps, "capacity", capacity);
        }, ct);
    }

    /// <summary>Removes the server row at shutdown (Part 3 §6.4, step 6). A missing row is harmless.</summary>
    public Task RemoveServerAsync(Guid serverId, CancellationToken ct) =>
        NonQueryAsync(RemoveServerSql, ps => P.Add(ps, "id", serverId), ct);
}
