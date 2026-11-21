namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string TriggerTemplate = """
        WITH t AS (
            UPDATE {schema}.queue SET run_at = now(), ready = true
            WHERE job_id = @id AND lease_owner IS NULL AND NOT ready AND await_condition IS NULL
            RETURNING job_id, attempt, queue
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, attempt)
            SELECT job_id, now(), @kind_triggered, attempt FROM t
        )
        SELECT queue FROM t
        """;

    private string TriggerSql => field ??= Render(TriggerTemplate);

    /// <summary>Moves a Scheduled job to Enqueued immediately (§5.8). Returns the queue to wake, or null.</summary>
    public Task<string?> TriggerAsync(Guid jobId, CancellationToken ct) =>
        QueryAsync(TriggerSql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "kind_triggered", (short)JobEventKind.Triggered);
        }, async (r, c) => await r.ReadAsync(c) ? r.GetString(0) : null, ct);
}
