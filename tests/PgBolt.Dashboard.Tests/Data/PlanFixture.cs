using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Data;

/// <summary>
/// Заполненная схема для контракта планов (§12, отклонение 19): все присоединённые партиции jobs и job_events, queue,
/// batches, recurring_jobs, servers, stats_minutes, queue_stats; затем ANALYZE. Одна на класс тестов (~10 с).
/// </summary>
public sealed class PlanFixture(PostgresFixture pg) : IAsyncLifetime
{
    // Сутки присоединённых партиций jobs; задача g — в сутках 1 + g % n, id и время — внутри суток партиции.
    private const string Source = """
        WITH days AS (
            SELECT row_number() OVER (ORDER BY d) AS n, d
            FROM (SELECT to_date(right(c.relname, 8), 'YYYYMMDD') AS d
                  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                  WHERE i.inhparent = to_regclass('{schema}.jobs')) x
        ),
        src AS MATERIALIZED (
            SELECT g, x.t,
                   (lpad(to_hex((extract(epoch FROM x.t) * 1000)::bigint), 12, '0') || substr(md5(g::text), 1, 20))::uuid AS id
            FROM generate_series(1, 200000) g
            JOIN days ON days.n = 1 + g % (SELECT max(n) FROM days)
            CROSS JOIN LATERAL (SELECT (days.d::timestamp AT TIME ZONE 'UTC') + ((g * 7919) % 80000000) * interval '1 ms' AS t) x
        )
        """;

    // jobs: g % 10 — 0..5 Succeeded, 6..7 Failed, 8 Cancelled, 9 Enqueued; продолжения — 50 родителей md5('p' || k)
    // (по 200 задач, коды 1–4), члены — 1000 batches md5('b' || k) (batch 1 — ~110 членов), запуски — 'r1'..'r50' (по 80).
    // queue: g 1..20000 Scheduled, 20001..40000 Awaiting (run_at = infinity), 40001..45000 Enqueued (у чётных run_at равны),
    // 45001..45500 Processing (lease_until = leased_at + 5 мин: одинаковый у всех строк срок делал queue_expiry идеально
    // упорядоченным по heap, и планировщик считал взятые строки по нему, а не по queue_leased). job_events: терминальные
    // события at = finished_at, шумовые kind 1 / 5 — at = created_at + 2 с.
    private const string JobsSql = Source + """
        ,
        j AS (
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, started_at, finished_at,
                                       server_id, parent_id, continuation, batch_id, recurring_id)
            SELECT id, 'type' || (g / 10) % 20, 'q' || (g / 7) % 5, (g % 3)::smallint, '{}',
                   (CASE WHEN g % 10 < 6 THEN 2 WHEN g % 10 < 8 THEN 3 WHEN g % 10 = 8 THEN 4 ELSE 0 END)::smallint, 3, t,
                   CASE WHEN g % 10 < 9 THEN t + interval '500 ms' END,
                   CASE WHEN g % 10 < 9 THEN t + interval '1 second' END,
                   CASE WHEN g % 10 < 9 THEN md5('s' || g % 3)::uuid END,
                   CASE WHEN g % 20 = 3 THEN md5('p' || (g / 20) % 50)::uuid END,
                   CASE WHEN g % 20 = 3 THEN (1 + (g / 1000) % 4)::smallint END,
                   CASE WHEN g % 20 = 7 THEN md5('b' || CASE WHEN (g / 20) % 100 = 0 THEN 1 ELSE 1 + (g / 20) % 1000 END)::uuid END,
                   CASE WHEN g % 50 = 1 THEN 'r' || (1 + (g / 50) % 50) END
            FROM src
        ),
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, lease_owner, lease_id, lease_until,
                                        leased_at, await_condition, parent_id)
            SELECT id, 'q' || (g / 7) % 5, (g % 3)::smallint,
                   CASE WHEN g <= 20000 THEN now() + g * interval '1 second'
                        WHEN g <= 40000 THEN 'infinity'
                        WHEN g <= 45000 THEN now() - CASE WHEN g % 2 = 0 THEN interval '1 hour' ELSE g * interval '1 ms' END
                        ELSE now() - interval '1 minute' END,
                   g > 40000, 3,
                   CASE WHEN g > 45000 THEN md5('s' || g % 3)::uuid END,
                   CASE WHEN g > 45000 THEN md5('l' || g)::uuid END,
                   CASE WHEN g > 45000 THEN now() + interval '5 minutes' - (g % 60) * interval '1 second' END,
                   CASE WHEN g > 45000 THEN now() - (g % 60) * interval '1 second' END,
                   CASE WHEN g BETWEEN 20001 AND 40000 THEN 1 END,
                   CASE WHEN g BETWEEN 20001 AND 40000 THEN md5('a' || g)::uuid END
            FROM src WHERE g <= 45500
        )
        INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
        SELECT id, t + interval '1 second', (CASE WHEN g % 10 = 8 THEN 4 ELSE (ARRAY[2, 3, 7])[1 + (g / 10) % 3] END)::smallint, 1, 'boom'
        FROM src WHERE g % 10 IN (6, 7, 8)
        UNION ALL
        SELECT id, t + interval '2 seconds', (CASE WHEN g % 4 = 0 THEN 1 ELSE 5 END)::smallint, 1, NULL
        FROM src WHERE g % 2 = 0
        """;

    // batches: 1000 batch-продолжений (3001..4000; у md5('p0') — ~120), 1000 вложенных (4001..5000; у batch 1 — 100).
    private const string RestSql = """
        INSERT INTO {schema}.batches (id, parent_batch_id, parent_id, condition, state, total, created_at, finished_at)
        SELECT md5('b' || b)::uuid,
               CASE WHEN b > 4000 THEN md5('b' || CASE WHEN b <= 4100 THEN 1 ELSE b - 4000 END)::uuid END,
               CASE WHEN b BETWEEN 3001 AND 4000 THEN md5('p' || CASE WHEN b <= 3100 THEN 0 ELSE b % 50 END)::uuid END,
               CASE WHEN b BETWEEN 3001 AND 4000 THEN (1 + b % 4)::smallint END,
               (b % 5)::smallint, 10, @start + b * interval '1 second',
               CASE WHEN b % 5 >= 2 THEN @start + b * interval '1 second' + interval '1 minute' END
        FROM generate_series(1, 5000) b;
        INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap, next_fire_at, wake_at,
                                             created_at, updated_at)
        SELECT 'r' || r, 'type' || r % 20, '{}', '* * * * *', 'UTC', 0, interval '0', 0, now() + interval '1 minute',
               now() + interval '1 minute', now(), now()
        FROM generate_series(1, 50) r;
        INSERT INTO {schema}.servers
        SELECT md5('s' || s)::uuid, 'server' || s, 'host' || s, '{q0,q1,q2,q3,q4}', 10, now() - interval '1 hour', now()
        FROM generate_series(0, 2) s;
        INSERT INTO {schema}.stats_minutes
        SELECT date_trunc('minute', now()) - m * interval '1 minute', md5('s' || s)::uuid, 'q' || qn, 3, 1, 1
        FROM generate_series(0, 1439) m, generate_series(0, 2) s, generate_series(0, 4) qn;
        INSERT INTO {schema}.queue_stats SELECT 'q' || qn, 4000, 1000, 100, 4000, now() FROM generate_series(0, 4) qn;
        """;

    public TestDb Db { get; private set; } = null!;
    public Guid ParentId { get; private set; }
    public DateTimeOffset ParentCreatedAt { get; private set; }
    public Guid BatchId { get; private set; }
    public DateTimeOffset BatchCreatedAt { get; private set; }
    public string RecurringId { get; } = "r1";
    public string[] Queues { get; } = [.. Enumerable.Range(0, 5).Select(i => $"q{i}")];
    public string[] Types { get; } = [.. Enumerable.Range(0, 20).Select(i => $"type{i}")];

    public async ValueTask InitializeAsync()
    {
        Db = await TestDb.CreateAsync(pg, ConnectionMode.Direct);
        var start = await Db.ScalarAsync<DateTime>("""
            SELECT min(to_date(right(c.relname, 8), 'YYYYMMDD'))::timestamp AT TIME ZONE 'UTC'
            FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = to_regclass('{schema}.jobs')
            """);
        await Db.ExecAsync(JobsSql);
        await Db.ExecAsync(RestSql, ("start", start));
        var tables = await Db.ScalarAsync<string>("""
            SELECT string_agg(format('%I.%I', n.nspname, c.relname), ', ')
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = @s AND c.relkind IN ('r', 'p') AND NOT c.relispartition
            """, ("s", Db.Schema));
        await Db.ExecAsync("ANALYZE " + tables);
        var empty = await Db.ScalarAsync<long>("""
            SELECT count(*) FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid
            WHERE i.inhparent IN (to_regclass('{schema}.jobs'), to_regclass('{schema}.job_events')) AND c.relpages = 0
            """);
        if (empty != 0)
            throw new InvalidOperationException($"{empty} partitions of jobs / job_events are empty after ANALYZE.");

        ParentId = await Db.ScalarAsync<Guid>("SELECT md5('p0')::uuid");
        ParentCreatedAt = new DateTimeOffset(start);
        BatchId = await Db.ScalarAsync<Guid>("SELECT md5('b1')::uuid");
        BatchCreatedAt = new DateTimeOffset(start.AddSeconds(1));
    }

    public async ValueTask DisposeAsync()
    {
        if (Db is not null)
            await Db.DisposeAsync();
    }
}
