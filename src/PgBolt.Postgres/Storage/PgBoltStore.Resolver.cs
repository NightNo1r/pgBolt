using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Resolver части 5 (§6.4). Т1 — один statement: захват, счётчики batches, сигналы исходов batches, отсев сигналов без детей.
/// Т2 — одна транзакция с бюджетом строк: statement на этап (задачи, затем batches), а не на узел; исход узла перечитывается в
/// каждом statement'е выборки детей; строки queue / batches — SKIP LOCKED, остаток за чужими блокировками — retry_after.
/// </summary>
public sealed partial class PgBoltStore
{
    private const int MaxWalkPortion = 1000;

    /// <summary>Коды ожиданий детей-задач, которые запускает исход узла (§2.2): Failed — только OnAnyFinished, Activated — члены.</summary>
    private const string JobChildConditions =
        "WHEN 3 THEN '{2,4}'::smallint[] WHEN 5 THEN '{5}'::smallint[] ELSE '{1,2,3,4}'::smallint[] END";

    /// <summary>То же для batch-продолжений (кода 5 у batches.condition нет).</summary>
    private const string BatchChildConditions = "WHEN 3 THEN '{2,4}'::smallint[] ELSE '{1,2,3,4}'::smallint[] END";

    /// <summary>Готовый к захвату Т1 сигнал (частичный индекс completions_count).</summary>
    private const string CountReadyPredicate = "kind <> 6 AND NOT counted AND (retry_after IS NULL OR retry_after <= now())";

    /// <summary>Готовый к захвату Т2 сигнал (частичный индекс completions_walk).</summary>
    private const string WalkReadyPredicate = "counted AND (retry_after IS NULL OR retry_after <= now())";

    /// <summary>Готовый к захвату CancelMembers сигнал (частичный индекс completions_cancel).</summary>
    private const string CancelReadyPredicate = "kind = 6 AND (retry_after IS NULL OR retry_after <= now())";

    /// <summary>
    /// Т1. walk_ready / cancel_ready — снимок statement'а: сигналы, переданные Т2 этим же statement'ом (walk), в нём не видны —
    /// их число возвращает walking.
    /// </summary>
    private const string CountTemplate = $$"""
        WITH c AS MATERIALIZED (
            SELECT id, node_id, kind, batch_id FROM {schema}.completions
            WHERE {{CountReadyPredicate}}
            ORDER BY id
            LIMIT @n
            FOR UPDATE SKIP LOCKED
        ),
        dl AS (
            SELECT batch_id,
                   (count(*) FILTER (WHERE kind = 2))::int AS s,
                   (count(*) FILTER (WHERE kind = 3))::int AS f,
                   (count(*) FILTER (WHERE kind = 4))::int AS x
            FROM c
            WHERE batch_id IS NOT NULL AND kind IN (2, 3, 4)
            GROUP BY batch_id
        ),
        lb AS MATERIALIZED (
            SELECT b.id, b.state, b.total, b.succeeded, b.failed, b.cancelled, b.parent_batch_id
            FROM {schema}.batches b
            WHERE b.id IN (SELECT batch_id FROM dl)
            ORDER BY b.id
            FOR NO KEY UPDATE
        ),
        n AS (
            SELECT lb.id, lb.parent_batch_id, lb.state AS old_state,
                   lb.succeeded + dl.s AS s, lb.failed + dl.f AS f, lb.cancelled + dl.x AS x,
                   (CASE WHEN lb.state IN (0, 1) AND lb.succeeded + dl.s + lb.failed + dl.f + lb.cancelled + dl.x = lb.total
                         THEN CASE WHEN lb.cancelled + dl.x > 0 THEN 4 WHEN lb.failed + dl.f > 0 THEN 3 ELSE 2 END
                         ELSE lb.state END)::smallint AS new_state
            FROM lb JOIN dl ON dl.batch_id = lb.id
        ),
        ub AS (
            UPDATE {schema}.batches b
            SET succeeded = n.s, failed = n.f, cancelled = n.x, state = n.new_state,
                finished_at = CASE WHEN n.new_state <> n.old_state THEN now() ELSE b.finished_at END
            FROM n
            WHERE b.id = n.id
        ),
        fin AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, new_state, parent_batch_id FROM n WHERE new_state <> old_state
            RETURNING 1
        ),
        k AS (
            SELECT c.id,
                   EXISTS (SELECT 1 FROM {schema}.queue q
                           WHERE q.parent_id = c.node_id AND q.await_condition IS NOT NULL
                             AND q.await_condition = ANY (CASE c.kind {{JobChildConditions}}))
                   OR (c.kind <> 5 AND EXISTS (
                           SELECT 1 FROM {schema}.batches b
                           WHERE b.parent_id = c.node_id AND b.parent_id IS NOT NULL AND b.state = 1 AND NOT b.cancel_requested
                             AND b.condition = ANY (CASE c.kind {{BatchChildConditions}})))
                   OR (c.kind = 5 AND EXISTS (
                           SELECT 1 FROM {schema}.batches b
                           WHERE b.parent_batch_id = c.node_id AND b.parent_batch_id IS NOT NULL AND b.parent_id IS NULL
                             AND b.state = 1 AND NOT b.cancel_requested)) AS has_children
            FROM c
        ),
        del AS (
            DELETE FROM {schema}.completions t USING k WHERE t.id = k.id AND NOT k.has_children
        ),
        walk AS (
            UPDATE {schema}.completions t SET counted = true FROM k WHERE t.id = k.id AND k.has_children
            RETURNING 1
        )
        SELECT (SELECT count(*) FROM c)::int AS captured,
               (SELECT count(*) FROM fin)::int AS finished,
               (SELECT count(*) FROM walk)::int AS walking,
               ARRAY(SELECT batch_id FROM dl WHERE batch_id NOT IN (SELECT id FROM lb)) AS missing,
               EXISTS (SELECT 1 FROM {schema}.completions WHERE {{WalkReadyPredicate}}) AS walk_ready,
               EXISTS (SELECT 1 FROM {schema}.completions WHERE {{CancelReadyPredicate}}) AS cancel_ready
        """;

    private const string WalkCaptureTemplate = $$"""
        SELECT id, node_id, kind, stage, cursor FROM {schema}.completions
        WHERE {{WalkReadyPredicate}}
        ORDER BY id
        LIMIT @n
        FOR UPDATE SKIP LOCKED
        """;

    /// <summary>Исход узла в снимке statement'а (§6.4 Т2.2): 2/3/4 — если состояние узла равно kind; 7 — терминальный batches.state; 5 — всегда.</summary>
    private const string WalkNodesCte = $$"""
        n AS MATERIALIZED (
            SELECT u.ord, u.node, COALESCE(u.cur, '00000000-0000-0000-0000-000000000000'::uuid) AS cur, o.outcome,
                   CASE o.outcome {{JobChildConditions}} AS conds
            FROM unnest(@nodes::uuid[], @kinds::smallint[], @cursors::uuid[]) WITH ORDINALITY AS u(node, kind, cur, ord)
            CROSS JOIN LATERAL (
                SELECT CASE
                    WHEN u.kind = 5 THEN 5::smallint
                    WHEN u.kind = 7 THEN (SELECT b.state FROM {schema}.batches b WHERE b.id = u.node AND b.state IN (2, 3, 4))
                    WHEN u.kind = COALESCE((SELECT j.state FROM {schema}.jobs j WHERE j.id = u.node AND j.id = ANY(@nodes)),
                                           (SELECT b.state FROM {schema}.batches b WHERE b.id = u.node)) THEN u.kind
                END AS outcome
            ) o
        )
        """;

    /// <summary>
    /// §6.4 Т2.3: дети-задачи всех узлов этапа. Запуск — UPDATE queue; отмена (Cancelled-родитель с OnSuccess или отмена ветки
    /// по cancel_requested batch ребёнка и внешних) — statement cancel core §5.7 с дополнением §6.3.
    /// </summary>
    private const string WalkJobsTemplate = "WITH RECURSIVE " + WalkNodesCte + """
        ,
        c AS MATERIALIZED (
            SELECT n.ord, n.node, n.outcome, q.job_id, q.await_condition, q.batch_id
            FROM n CROSS JOIN LATERAL (
                SELECT job_id, await_condition, batch_id FROM {schema}.queue
                WHERE parent_id = n.node AND await_condition IS NOT NULL AND await_condition = ANY(n.conds) AND job_id >= n.cur
                ORDER BY job_id
                LIMIT @lim
                FOR UPDATE SKIP LOCKED
            ) q
            WHERE n.outcome IS NOT NULL
        ),
        up(start, id, depth) AS (
            SELECT DISTINCT batch_id, batch_id, 0 FROM c WHERE batch_id IS NOT NULL
            UNION
            SELECT up.start, b.parent_batch_id, up.depth + 1
            FROM up JOIN {schema}.batches b ON b.id = up.id WHERE b.parent_batch_id IS NOT NULL
        ),
        br AS (
            -- Ближайший к ребёнку batch цепочки с запрошенной отменой (свой или внешний) — он и назван в сообщении.
            SELECT up.start AS batch_id,
                   (array_agg(up.id ORDER BY up.depth) FILTER (WHERE b.cancel_requested))[1] AS cancelled_by
            FROM up JOIN {schema}.batches b ON b.id = up.id
            GROUP BY up.start
        ),
        d AS MATERIALIZED (
            SELECT c.job_id,
                   CASE WHEN br.cancelled_by IS NOT NULL THEN 'batch ' || br.cancelled_by || ' cancelled'
                        WHEN c.outcome = 4 AND c.await_condition IN (1, 3) THEN 'parent ' || c.node || ' cancelled'
                   END AS cancel_reason
            FROM c LEFT JOIN br ON br.batch_id = c.batch_id
        ),
        l AS (
            UPDATE {schema}.queue q SET await_condition = NULL, run_at = now(), ready = true
            FROM d WHERE q.job_id = d.job_id AND d.cancel_reason IS NULL
            RETURNING q.queue
        ),
        x AS (
            DELETE FROM {schema}.queue q USING d
            WHERE q.job_id = d.job_id AND d.cancel_reason IS NOT NULL
            RETURNING q.job_id, q.attempt, q.notify, d.cancel_reason
        ),
        xj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = x.attempt, finished_at = now(), notify = j.notify OR x.notify
            FROM x WHERE j.id = x.job_id
            RETURNING j.id, j.batch_id, x.notify
        ),
        xe AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
            SELECT job_id, now(), @kind_cancelled, attempt, cancel_reason FROM x
        ),
        xs AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 4, batch_id FROM xj WHERE notify
            RETURNING 1
        )
        SELECT n.ord, n.outcome, COALESCE(p.taken, 0) AS taken, p.last_id,
               ARRAY(SELECT DISTINCT queue FROM l) AS queues,
               (SELECT count(*) FROM xs)::int AS signals
        FROM n LEFT JOIN (SELECT ord, count(*)::int AS taken, (array_agg(job_id ORDER BY job_id DESC))[1] AS last_id
                          FROM c GROUP BY ord) p ON p.ord = n.ord
        ORDER BY n.ord
        """;

    /// <summary>
    /// §6.4 Т2.4: дети-batches. 2/3/4/7 — batch-продолжения узла (batches_awaiting); 5 — вложенные без родителя (batches_nested).
    /// Запуск — state = 0 и Activated; отмена (Cancelled-родитель с OnSuccess или отмена ветки от parent_batch_id) —
    /// cancel_requested и CancelMembers; batch с собственным cancel_requested не трогается.
    /// </summary>
    private const string WalkBatchesTemplate = "WITH RECURSIVE " + WalkNodesCte + """
        ,
        c AS MATERIALIZED (
            SELECT n.ord, n.outcome, b.id, b.condition, b.parent_batch_id
            FROM n CROSS JOIN LATERAL (
                SELECT id, condition, parent_batch_id FROM {schema}.batches
                WHERE parent_id = n.node AND parent_id IS NOT NULL AND state = 1 AND NOT cancel_requested
                  AND condition = ANY(n.conds) AND id >= n.cur
                ORDER BY id
                LIMIT @lim
                FOR NO KEY UPDATE SKIP LOCKED
            ) b
            WHERE n.outcome IS NOT NULL AND n.outcome <> 5
            UNION ALL
            SELECT n.ord, n.outcome, b.id, b.condition, b.parent_batch_id
            FROM n CROSS JOIN LATERAL (
                SELECT id, condition, parent_batch_id FROM {schema}.batches
                WHERE parent_batch_id = n.node AND parent_batch_id IS NOT NULL AND parent_id IS NULL AND state = 1
                  AND NOT cancel_requested AND id >= n.cur
                ORDER BY id
                LIMIT @lim
                FOR NO KEY UPDATE SKIP LOCKED
            ) b
            WHERE n.outcome = 5
        ),
        up(start, id) AS (
            SELECT DISTINCT parent_batch_id, parent_batch_id FROM c WHERE parent_batch_id IS NOT NULL
            UNION
            SELECT up.start, b.parent_batch_id FROM up JOIN {schema}.batches b ON b.id = up.id WHERE b.parent_batch_id IS NOT NULL
        ),
        br AS (
            SELECT up.start AS batch_id, bool_or(b.cancel_requested) AS cancelled
            FROM up JOIN {schema}.batches b ON b.id = up.id
            GROUP BY up.start
        ),
        d AS MATERIALIZED (
            SELECT c.id, COALESCE(br.cancelled, false) OR (c.outcome = 4 AND c.condition IN (1, 3)) AS cancel
            FROM c LEFT JOIN br ON br.batch_id = c.parent_batch_id
        ),
        u AS (
            UPDATE {schema}.batches b
            SET state = CASE WHEN d.cancel THEN b.state ELSE 0 END, cancel_requested = d.cancel
            FROM d WHERE b.id = d.id
            RETURNING b.id, d.cancel
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind)
            SELECT id, CASE WHEN cancel THEN 6 ELSE 5 END FROM u
            RETURNING 1
        )
        SELECT n.ord, n.outcome, COALESCE(p.taken, 0) AS taken, p.last_id,
               '{}'::text[] AS queues,
               (SELECT count(*) FROM s)::int AS signals
        FROM n LEFT JOIN (SELECT ord, count(*)::int AS taken, (array_agg(id ORDER BY id DESC))[1] AS last_id
                          FROM c GROUP BY ord) p ON p.ord = n.ord
        ORDER BY n.ord
        """;

    /// <summary>§6.4 Т2.3: остаток узлов, дошедших до конца, — первая строка тем же предикатом без курсора и без блокировки.</summary>
    private const string WalkJobsTailTemplate = $$"""
        SELECT u.ord,
               (SELECT q.job_id FROM {schema}.queue q
                WHERE q.parent_id = u.node AND q.await_condition IS NOT NULL
                  AND q.await_condition = ANY (CASE u.outcome {{JobChildConditions}})
                ORDER BY q.job_id
                LIMIT 1) AS first_id
        FROM unnest(@nodes::uuid[], @outcomes::smallint[]) WITH ORDINALITY AS u(node, outcome, ord)
        """;

    private const string WalkBatchesTailTemplate = $$"""
        SELECT u.ord,
               CASE WHEN u.outcome = 5 THEN
                   (SELECT b.id FROM {schema}.batches b
                    WHERE b.parent_batch_id = u.node AND b.parent_batch_id IS NOT NULL AND b.parent_id IS NULL AND b.state = 1
                      AND NOT b.cancel_requested
                    ORDER BY b.id LIMIT 1)
               ELSE
                   (SELECT b.id FROM {schema}.batches b
                    WHERE b.parent_id = u.node AND b.parent_id IS NOT NULL AND b.state = 1 AND NOT b.cancel_requested
                      AND b.condition = ANY (CASE u.outcome {{BatchChildConditions}})
                    ORDER BY b.id LIMIT 1)
               END AS first_id
        FROM unnest(@nodes::uuid[], @outcomes::smallint[]) WITH ORDINALITY AS u(node, outcome, ord)
        """;

    /// <summary>§6.4 Т2.5: прошедшие оба этапа без остатка удаляются; остальные — stage, cursor, retry_after.</summary>
    private const string WalkFinishTemplate = """
        WITH f AS (
            SELECT * FROM unnest(@ids::bigint[], @done::boolean[], @stages::smallint[], @cursors::uuid[], @defer::boolean[])
                     AS f(id, done, stage, cur, defer)
        ),
        del AS (
            DELETE FROM {schema}.completions c USING f WHERE c.id = f.id AND f.done
        )
        UPDATE {schema}.completions c
        SET stage = f.stage, cursor = f.cur, retry_after = CASE WHEN f.defer THEN now() + @retry ELSE NULL END
        FROM f
        WHERE c.id = f.id AND NOT f.done
        """;

    private const string OldestCompletionTemplate = "SELECT now() - created_at FROM {schema}.completions ORDER BY id LIMIT 1";

    /// <summary>
    /// Есть ли сигнал, который взял бы захват Т1, Т2 или CancelMembers: те же предикаты, каждый — по своему частичному индексу
    /// (completions_count, completions_walk, completions_cancel; вместе они покрывают все строки).
    /// </summary>
    private const string ReadyCompletionsTemplate = $$"""
        SELECT EXISTS (SELECT 1 FROM {schema}.completions WHERE {{CountReadyPredicate}})
            OR EXISTS (SELECT 1 FROM {schema}.completions WHERE {{WalkReadyPredicate}})
            OR EXISTS (SELECT 1 FROM {schema}.completions WHERE {{CancelReadyPredicate}})
        """;

    internal string CountSql => field ??= Render(CountTemplate);
    internal string WalkCaptureSql => field ??= Render(WalkCaptureTemplate);
    internal string WalkJobsSql => field ??= Render(WalkJobsTemplate);
    internal string WalkBatchesSql => field ??= Render(WalkBatchesTemplate);
    private string WalkJobsTailSql => field ??= Render(WalkJobsTailTemplate);
    private string WalkBatchesTailSql => field ??= Render(WalkBatchesTailTemplate);
    private string WalkFinishSql => field ??= Render(WalkFinishTemplate);
    private string OldestCompletionSql => field ??= Render(OldestCompletionTemplate);
    internal string ReadyCompletionsSql => field ??= Render(ReadyCompletionsTemplate);

    /// <summary>Только для тестов: вызывается перед COMMIT транзакции Т2 (Walked).</summary>
    internal Func<ResolverStep, CancellationToken, Task>? ResolverHook { get; set; }

    /// <summary>Т1 (§6.4): один statement; повтор целиком при 40P01/40001.</summary>
    internal Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        return QueryAsync(CountSql, ps => P.Add(ps, "n", batchSize), async (r, c) =>
        {
            await r.ReadAsync(c);
            return new CompletionCountResult(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2), r.GetFieldValue<Guid[]>(3), r.GetBoolean(4),
                r.GetBoolean(5));
        }, ct);
    }

    /// <summary>Возраст старейшего сигнала (§8: Warning при лаге resolver'а); null — сигналов нет.</summary>
    internal Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct) =>
        QueryAsync<TimeSpan?>(OldestCompletionSql, _ => { }, async (r, c) => await r.ReadAsync(c) ? r.GetFieldValue<TimeSpan>(0) : null, ct);

    /// <summary>Проверка простоя resolver'а по таймеру (§6.4): один дешёвый запрос вместо транзакций Т1, Т2 и CancelMembers.</summary>
    internal Task<bool> HasReadyCompletionsAsync(CancellationToken ct) =>
        QueryAsync(ReadyCompletionsSql, _ => { }, async (r, c) => await r.ReadAsync(c) && r.GetBoolean(0), ct);

    private sealed class WalkNode(long id, Guid node, short kind, short stage, Guid? cursor)
    {
        public long Id { get; } = id;
        public Guid Node { get; } = node;
        public short Kind { get; } = kind;
        public short Stage { get; set; } = stage;
        public Guid? Cursor { get; set; } = cursor;
        public short? Outcome { get; set; }
        public bool Done { get; set; }        // сигнал удаляется: обход завершён или сигнал устарел
        public bool Ended { get; set; }       // порция меньше лимита — этап пройден, нужна проверка остатка
        public bool Deferred { get; set; }    // остаток заблокирован другими — retry_after
    }

    private sealed class WalkProgress(int budget)
    {
        public int Left { get; set; } = budget;
        public int Statements { get; set; } = 1;   // захват
        public int MaxPortion { get; set; }
        public int NewSignals { get; set; }
        public HashSet<string> Queues { get; } = new(StringComparer.Ordinal);
    }

    /// <summary>Т2 (§6.4): одна транзакция; повтор целиком при 40P01/40001.</summary>
    internal Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowBudget);
        return RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);
            var nodes = await CaptureWalkAsync(conn, tx, batchSize, c);
            if (nodes.Count == 0)
            {
                await tx.CommitAsync(c);
                return ResolveResult.Empty;
            }

            var walk = new WalkProgress(rowBudget);
            foreach (var stage in new short[] { 0, 1 })
            {
                while (walk.Left > 0)
                {
                    var active = nodes.Where(n => n.Stage == stage && !n.Done && !n.Deferred && !n.Ended).ToList();
                    if (active.Count == 0)
                        break;
                    var lim = Math.Min(MaxWalkPortion, Math.Max(1, walk.Left / active.Count));
                    await WalkStageAsync(conn, tx, stage, active, lim, walk, c);
                }

                var ended = nodes.Where(n => n.Stage == stage && n.Ended && !n.Done).ToList();
                if (ended.Count == 0)
                    continue;
                var rest = await WalkTailAsync(conn, tx, stage, ended, c);
                walk.Statements++;
                foreach (var n in ended)
                {
                    n.Ended = false;
                    if (rest.TryGetValue(n.Id, out var first))
                    {
                        n.Cursor = first;
                        n.Deferred = true;
                    }
                    else if (stage == 0)
                    {
                        n.Stage = 1;
                        n.Cursor = null;
                    }
                    else
                    {
                        n.Done = true;
                    }
                }
            }

            await FinishWalkAsync(conn, tx, nodes, retryDelay, c);
            walk.Statements++;
            if (ResolverHook is { } hook)
                await hook(ResolverStep.Walked, c);
            await tx.CommitAsync(c);
            return new ResolveResult(nodes.Count, rowBudget - walk.Left, walk.Statements, walk.MaxPortion,
                BudgetExhausted: nodes.Any(n => !n.Done && !n.Deferred), walk.NewSignals, nodes.Count(n => n.Deferred), walk.Queues);
        }, ct);
    }

    private async Task<List<WalkNode>> CaptureWalkAsync(NpgsqlConnection conn, NpgsqlTransaction tx, int batchSize, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(WalkCaptureSql, conn, tx);
        P.Add(cmd.Parameters, "n", batchSize);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAllAsync(static r => new WalkNode(r.GetInt64(0), r.GetGuid(1), r.GetInt16(2), r.GetInt16(3),
            r.GetNullableValue<Guid>(4)), ct);
    }

    private async Task WalkStageAsync(NpgsqlConnection conn, NpgsqlTransaction tx, short stage, List<WalkNode> active, int lim,
        WalkProgress walk, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(stage == 0 ? WalkJobsSql : WalkBatchesSql, conn, tx);
        var ps = cmd.Parameters;
        P.Add(ps, "nodes", active.Select(n => n.Node).ToArray());
        P.Add(ps, "kinds", active.Select(n => n.Kind).ToArray());
        P.Add(ps, "cursors", active.Select(n => n.Cursor).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "lim", lim);
        if (stage == 0)
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
        walk.Statements++;
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var first = true;
        while (await r.ReadAsync(ct))
        {
            if (first)
            {
                foreach (var queue in r.GetFieldValue<string[]>(4))
                    walk.Queues.Add(queue);
                walk.NewSignals += r.GetInt32(5);
                first = false;
            }
            var n = active[(int)r.GetInt64(0) - 1];
            if (r.IsDBNull(1))
            {
                n.Done = true;   // §6.4 Т2.2: устаревший сигнал (requeue, строки нет, batch не терминален) — к детям не применяется
                continue;
            }
            n.Outcome = r.GetInt16(1);
            var taken = r.GetInt32(2);
            walk.Left -= taken;
            walk.MaxPortion = Math.Max(walk.MaxPortion, taken);
            if (taken < lim)
                n.Ended = true;
            else
                n.Cursor = r.GetGuid(3);   // курсор включающий: обработанные строки предикату больше не удовлетворяют
        }
    }

    private async Task<Dictionary<long, Guid>> WalkTailAsync(NpgsqlConnection conn, NpgsqlTransaction tx, short stage, List<WalkNode> ended,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(stage == 0 ? WalkJobsTailSql : WalkBatchesTailSql, conn, tx);
        P.Add(cmd.Parameters, "nodes", ended.Select(n => n.Node).ToArray());
        P.Add(cmd.Parameters, "outcomes", ended.Select(n => n.Outcome!.Value).ToArray());
        var rest = new Dictionary<long, Guid>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            if (!r.IsDBNull(1))
                rest[ended[(int)r.GetInt64(0) - 1].Id] = r.GetGuid(1);
        return rest;
    }

    private async Task FinishWalkAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<WalkNode> nodes, TimeSpan retryDelay,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(WalkFinishSql, conn, tx);
        var ps = cmd.Parameters;
        P.Add(ps, "ids", nodes.Select(n => n.Id).ToArray());
        P.Add(ps, "done", nodes.Select(n => n.Done).ToArray());
        P.Add(ps, "stages", nodes.Select(n => n.Stage).ToArray());
        P.Add(ps, "cursors", nodes.Select(n => n.Cursor).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "defer", nodes.Select(n => n.Deferred).ToArray());
        P.Add(ps, "retry", retryDelay);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
