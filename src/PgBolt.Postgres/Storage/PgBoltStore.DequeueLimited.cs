using Npgsql;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    // Транзакционный lock: снимается на коммите батча; сессионный остался бы на серверном соединении PgBouncer.
    private const string QueueLockTemplate = """
        SELECT pg_advisory_xact_lock(hashtextextended('pgbolt:{schema}:' || @queue, 0))
        """;

    // Отдельная команда после lock: snapshot берётся уже после коммита предыдущего держателя, и подсчёт видит все взятые им
    // строки (в одном statement'е с lock snapshot был бы старше ожидания и лимит превышался бы).
    // LIMIT — параметр, отсечение по room — в l: LIMIT с подзапросом планировщик оценивает в 10 % очереди и уходит в Seq Scan
    // и Hash Join. Лишние кандидаты c остаются заблокированы до коммита батча сразу после этой команды.
    private const string DequeueLimitedTemplate = $$"""
        WITH r AS (
            SELECT (SELECT {{EffectiveLimit}} FROM {schema}.queue_settings WHERE queue = @queue)
                 - (SELECT count(*) FROM {schema}.queue WHERE queue = @queue AND lease_owner IS NOT NULL) AS room
        ),
        c AS (
            SELECT job_id, row_number() OVER (ORDER BY priority DESC, run_at) AS rn
            FROM (
                SELECT job_id, priority, run_at FROM {schema}.queue
                WHERE queue = @queue AND lease_owner IS NULL AND ready
                  AND coalesce((SELECT room FROM r) > 0, true)
                ORDER BY priority DESC, run_at
                LIMIT @n
                FOR UPDATE SKIP LOCKED
            ) t
        ),
        l AS (
            UPDATE {schema}.queue q
            SET {{LeaseSet}}
            FROM c, r
            WHERE q.job_id = c.job_id AND (r.room IS NULL OR c.rn <= r.room)
            RETURNING q.job_id, q.lease_id, q.attempt, q.priority, q.run_at, {{WaitSecondsExpr}}
        )
        SELECT r.room, {{LeasedColumns}} FROM r LEFT JOIN l ON true {{JoinDefinitions}}
        ORDER BY l.priority DESC, l.run_at
        """;

    private string QueueLockSql => field ??= Render(QueueLockTemplate);
    internal string DequeueLimitedSql => field ??= Render(DequeueLimitedTemplate);

    /// <summary>
    /// Dequeue очереди с кластерным лимитом: берёт до <paramref name="max"/> задач, но не больше свободных мест лимита
    /// (лимит из queue_settings минус взятые строки очереди). Нет лимита — как <see cref="DequeueAsync"/>.
    /// LimitReached — после этого dequeue мест в лимите нет (в том числе пауза).
    /// </summary>
    public Task<LimitedDequeueResult> DequeueLimitedAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(max);
        return RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            // Две команды одного батча без BeginTransaction — одна неявная транзакция и один round trip.
            await using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands = { new NpgsqlBatchCommand(QueueLockSql), new NpgsqlBatchCommand(DequeueLimitedSql) },
            };
            P.Add(batch.BatchCommands[0].Parameters, "queue", queue);
            var ps = batch.BatchCommands[1].Parameters;
            P.Add(ps, "queue", queue);
            P.Add(ps, "n", max);
            P.Add(ps, "server", serverId);
            P.Add(ps, "lease", lease);
            if (_prepare)
                await batch.PrepareAsync(c);

            await using var r = await batch.ExecuteReaderAsync(c);
            await r.NextResultAsync(c);
            long? room = null;
            var jobs = new List<LeasedJob>();
            while (await r.ReadAsync(c))
            {
                room = r.GetNullableValue<long>(0);
                if (!r.IsDBNull(1))
                    jobs.Add(ReadLeasedJob(r, 1));
            }
            return new LimitedDequeueResult(jobs, room is { } left && jobs.Count >= left);
        }, ct);
    }
}
