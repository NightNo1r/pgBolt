using PgBolt.Dashboard.Views;

namespace PgBolt.Dashboard.Data;

internal sealed partial class DashboardStore
{
    // Succeeded (§6.1): ключи — PK партиций jobs, id DESC; state не в индексе — всегда порциями (§6.3).
    private const string SucceededKeysTemplate = """
        SELECT j.id, j.type, j.queue, j.priority, j.attempt, j.max_attempts, j.created_at, j.started_at, j.server_id, j.finished_at, j.state
        FROM {schema}.jobs j {after} ORDER BY j.id DESC LIMIT {portion}
        """;

    private const string SucceededAfter = "WHERE j.id < @after_id";

    // m без соединения: jobs уже прочитана в k.
    private const string SucceededMatches = """
        SELECT k.id, k.type, k.queue, k.priority, k.attempt, k.max_attempts, k.created_at, NULL::timestamptz, k.started_at, k.server_id,
               k.finished_at, false, NULL::smallint, NULL::text, k.id AS o1
        FROM k
        WHERE k.state = 2 AND (@queue::text IS NULL OR k.queue = @queue) AND (@type::text IS NULL OR k.type = @type)
        ORDER BY k.id DESC LIMIT @need
        """;

    // Failed и Cancelled (§6.1): ключи — job_events_failed / job_events_cancelled, (at, job_id) DESC. {kinds} и {state} —
    // литералы (предикат совпадает с частичным индексом); e.at <= @after_at — простое условие для plan-time pruning партиций.
    private const string TerminalEventsKeysTemplate = """
        SELECT e.at, e.job_id, e.kind, e.message FROM {schema}.job_events e
        WHERE e.kind {kinds} {after}
        ORDER BY e.at DESC, e.job_id DESC LIMIT {portion}
        """;

    private const string TerminalEventsAfter = "AND e.at <= @after_at AND (e.at, e.job_id) < (@after_at, @after_id)";

    // Событие — текущее терминальное состояние задачи, только если jobs.finished_at = at (повтор после requeue — одна строка).
    private const string TerminalEventsTemplate = """
        SELECT j.id, j.type, j.queue, j.priority, j.attempt, j.max_attempts, j.created_at, NULL::timestamptz, j.started_at, j.server_id,
               j.finished_at, false, k.kind, k.message, k.at AS o1, k.job_id AS o2
        FROM k CROSS JOIN LATERAL (
            SELECT j.id, j.type, j.queue, j.priority, j.attempt, j.max_attempts, j.created_at, j.started_at, j.server_id, j.finished_at
            FROM {schema}.jobs j
            WHERE j.id = k.job_id AND j.state = {state} AND j.finished_at = k.at
              AND (@queue::text IS NULL OR j.queue = @queue) AND (@type::text IS NULL OR j.type = @type)
            OFFSET 0
        ) j
        ORDER BY k.at DESC, k.job_id DESC LIMIT @need
        """;

    private string SucceededPortion(string after) => Portion(SucceededKeysTemplate.Replace("{after}", after, StringComparison.Ordinal),
        last: "id", desc: "id", m: SucceededMatches, morder: "m.o1 DESC");

    /// <summary>Failed rows match events 2, 3 or 7 and jobs.state = 3. Cancelled rows match event 4 and state = 4. These predicates match the partial indexes.</summary>
    private string TerminalEventsPortion(bool failed, string after) => Portion(
        TerminalEventsKeysTemplate.Replace("{kinds}", failed ? "IN (2, 3, 7)" : "= 4", StringComparison.Ordinal)
            .Replace("{after}", after, StringComparison.Ordinal),
        last: "at, job_id", desc: "at, job_id", m: TerminalEventsTemplate.Replace("{state}", failed ? "3" : "4", StringComparison.Ordinal),
        morder: "m.o1 DESC, m.o2 DESC");

    private string SucceededPortionSql => field ??= SucceededPortion("");
    private string SucceededAfterPortionSql => field ??= SucceededPortion(SucceededAfter);
    private string FailedPortionSql => field ??= TerminalEventsPortion(failed: true, "");
    private string FailedAfterPortionSql => field ??= TerminalEventsPortion(failed: true, TerminalEventsAfter);
    private string CancelledPortionSql => field ??= TerminalEventsPortion(failed: false, "");
    private string CancelledAfterPortionSql => field ??= TerminalEventsPortion(failed: false, TerminalEventsAfter);

    /// <summary>Key for a failed or cancelled row: (at, job_id), where the event timestamp equals jobs.finished_at.</summary>
    private static ListKey TerminalEventKey(JobListRow r) => new(r.Id, r.FinishedAt);
}
