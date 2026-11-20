using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Support;

/// <summary>Засев задач одним statement'ом (в обход API) и lease через core.</summary>
public sealed class JobSeeder(TestDb db, PgBoltStore core)
{
    private static readonly Guid Server = Guid.Parse("00000000-0000-0000-0000-00000000d0a5");

    private const string SeedSql = """
        WITH k AS (
            SELECT id, ord - 1 AS i FROM unnest(@ids::uuid[]) WITH ORDINALITY AS u(id, ord)
        ),
        j AS (
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, started_at, finished_at)
            SELECT id, @type, @queue, @priority::smallint, '{}', @state::smallint, 3, now(),
                   CASE WHEN @state::smallint IN (2, 3, 4) THEN clock_timestamp() + i * interval '1 millisecond' END,
                   CASE WHEN @state::smallint IN (2, 3, 4) THEN clock_timestamp() + i * interval '1 millisecond' END
            FROM k
        )
        INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, await_condition, parent_id)
        SELECT id, @queue, @priority::smallint,
               CASE @row
                   WHEN 'scheduled' THEN coalesce(@runAt::timestamptz, now() + interval '1 hour') + i * coalesce(@step::interval, '0')
                   WHEN 'enqueued' THEN coalesce(@runAt::timestamptz, now()) + i * coalesce(@step::interval, '0')
                   ELSE 'infinity'
               END,
               @row = 'enqueued', 3,
               CASE WHEN @row = 'awaiting' THEN 1 END,
               CASE WHEN @row = 'awaiting' THEN gen_random_uuid() END
        FROM k WHERE @row <> 'none'
        """;

    /// <summary>
    /// count задач в jobs (state) и, по queueRow ("none" | "scheduled" | "enqueued" | "awaiting"), в queue с инвариантами
    /// состояния; id — UUIDv7 по возрастанию. run_at = (runAtUtc ?? по умолчанию) + i × runAtStep.
    /// </summary>
    public async Task<Guid[]> SeedJobsAsync(int count, string type, string queue, short state, string queueRow,
        DateTime? runAtUtc = null, short priority = 0, TimeSpan? runAtStep = null)
    {
        if (queueRow is not ("none" or "scheduled" or "enqueued" or "awaiting"))
            throw new ArgumentOutOfRangeException(nameof(queueRow), queueRow, null);
        var ids = Enumerable.Range(0, count).Select(_ => Guid.CreateVersion7()).Order().ToArray();
        await db.ExecAsync(SeedSql, ("ids", ids), ("type", type), ("queue", queue), ("priority", priority), ("state", state),
            ("row", queueRow), ("runAt", runAtUtc), ("step", runAtStep));
        return ids;
    }

    /// <summary>Один lease в очереди (самый ранний run_at среди задач высшего приоритета).</summary>
    public async Task<LeasedJob> LeaseAsync(string queue = "default") =>
        (await core.DequeueAsync(queue, 1, Server, TimeSpan.FromMinutes(5), TestContext.Current.CancellationToken)).Single();
}
