namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string PromoteTemplate = """
        WITH p AS (
            SELECT job_id FROM {schema}.queue
            WHERE NOT ready AND run_at <= now()
            ORDER BY run_at
            LIMIT @batch
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {schema}.queue q SET ready = true
        FROM p WHERE q.job_id = p.job_id
        RETURNING q.queue
        """;

    internal string PromoteSql => field ??= Render(PromoteTemplate);

    /// <summary>Scheduled → Enqueued для наступивших run_at (§5.11). Повторяет пакеты, пока пакет полный.</summary>
    public async Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        var queues = new HashSet<string>(StringComparer.Ordinal);
        var total = 0;
        while (true)
        {
            var batch = await QueryAsync(PromoteSql, ps => P.Add(ps, "batch", batchSize),
                async (r, c) => await r.ReadAllAsync(static r => r.GetString(0), c), ct);
            total += batch.Count;
            queues.UnionWith(batch);
            if (batch.Count < batchSize)
                return new PromoteResult(total, queues);
        }
    }
}
