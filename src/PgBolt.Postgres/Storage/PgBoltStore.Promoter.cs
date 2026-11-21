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
        ), promoted AS (
            UPDATE {schema}.queue q SET ready = true
            FROM p WHERE q.job_id = p.job_id
            RETURNING q.queue
        )
        SELECT count(*)::int, coalesce(array_agg(DISTINCT queue COLLATE "C"), '{}'::text[])
        FROM promoted
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
            // Число изменённых строк определяет повтор, имена нужны только для пробуждения очередей.
            var (promoted, names) = await QueryAsync(PromoteSql, ps => P.Add(ps, "batch", batchSize), static async (r, c) =>
            {
                await r.ReadAsync(c);
                return (r.GetInt32(0), r.GetFieldValue<string[]>(1));
            }, ct);
            total += promoted;
            queues.UnionWith(names);
            if (promoted < batchSize)
            {
                return new PromoteResult(total, queues);
            }
        }
    }
}
