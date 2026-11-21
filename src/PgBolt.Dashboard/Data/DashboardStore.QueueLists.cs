using System.Globalization;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres.Storage;

namespace PgBolt.Dashboard.Data;

internal sealed partial class DashboardStore
{
    // Колонки JobListRow для списков из queue.
    private const string QueueRowColumns = """
        j.id, j.type, j.queue, j.priority, q.attempt, q.max_attempts, j.created_at, CASE WHEN q.run_at < 'infinity' THEN q.run_at END,
        q.leased_at, q.lease_owner, NULL::timestamptz, q.cancel_requested, NULL::smallint, NULL::text
        """;

    // Scheduled (§6.1): queue_delayed; run_at < 'infinity' — условие индекса.
    private const string ScheduledFilter = "q.lease_owner IS NULL AND NOT q.ready AND q.run_at < 'infinity' AND q.await_condition IS NULL";
    private const string ScheduledAfter = "AND q.run_at >= @after_at AND (q.run_at, q.job_id) > (@after_at, @after_id)";

    private const string ScheduledTemplate = $$"""
        SELECT {{QueueRowColumns}} FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id
        WHERE {{ScheduledFilter}} {after}
        ORDER BY q.run_at, q.job_id LIMIT @limit
        """;

    private const string ScheduledKeysTemplate = $$"""
        SELECT q.* FROM {schema}.queue q WHERE {{ScheduledFilter}} {after} ORDER BY q.run_at, q.job_id LIMIT {portion}
        """;

    // Enqueued (§6.1): queue_fetch.
    private const string EnqueuedFilter = "q.queue = @queue AND q.lease_owner IS NULL AND q.ready";

    private const string EnqueuedTemplate = $$"""
        SELECT {{QueueRowColumns}} FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id
        WHERE {{EnqueuedFilter}}
        ORDER BY q.priority DESC, q.run_at, q.job_id LIMIT @limit
        """;

    private const string EnqueuedKeysTemplate = $$"""
        SELECT q.* FROM {schema}.queue q WHERE {{EnqueuedFilter}} ORDER BY q.priority DESC, q.run_at, q.job_id LIMIT {portion}
        """;

    // Enqueued после курсора (§6.3): UNION ALL двух ветвей, каждая со своим ORDER BY … LIMIT по queue_fetch.
    private const string EnqueuedBranches = """
        (SELECT * FROM {schema}.queue
         WHERE queue = @queue AND lease_owner IS NULL AND ready AND priority = @after_priority
           AND run_at >= @after_at AND (run_at, job_id) > (@after_at, @after_id)
         ORDER BY run_at, job_id LIMIT {limit})
        UNION ALL
        (SELECT * FROM {schema}.queue
         WHERE queue = @queue AND lease_owner IS NULL AND ready AND priority < @after_priority
         ORDER BY priority DESC, run_at, job_id LIMIT {limit})
        """;

    private const string EnqueuedAfterTemplate = $$"""
        SELECT {{QueueRowColumns}} FROM (
        {{EnqueuedBranches}}
        ) q JOIN {schema}.jobs j ON j.id = q.job_id
        ORDER BY q.priority DESC, q.run_at, q.job_id LIMIT {limit}
        """;

    private const string EnqueuedAfterKeysTemplate = $$"""
        SELECT q.* FROM (
        {{EnqueuedBranches}}
        ) q ORDER BY q.priority DESC, q.run_at, q.job_id LIMIT {limit}
        """;

    // Processing (§6.1): все взятые строки (≤ ёмкости серверов) — сортировка и курсор (leased_at, job_id) в памяти.
    private const string ProcessingTemplate = $$"""
        SELECT {{QueueRowColumns}} FROM {schema}.queue q CROSS JOIN LATERAL (
            SELECT j.id, j.type, j.queue, j.priority, j.created_at FROM {schema}.jobs j
            WHERE j.id = q.job_id AND (@type::text IS NULL OR j.type = @type)
            OFFSET 0
        ) j
        WHERE q.lease_owner IS NOT NULL AND (@queue::text IS NULL OR q.queue = @queue)
        """;

    // Awaiting (§6.1): queue_awaiting_list.
    private const string AwaitingAfter = "AND q.job_id < @after_id";

    private const string AwaitingTemplate = $$"""
        SELECT {{QueueRowColumns}} FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id
        WHERE q.await_condition IS NOT NULL {after}
        ORDER BY q.job_id DESC LIMIT @limit
        """;

    private const string AwaitingKeysTemplate = """
        SELECT q.* FROM {schema}.queue q WHERE q.await_condition IS NOT NULL {after} ORDER BY q.job_id DESC LIMIT {portion}
        """;

    // Порция (§6.3): {keys} — ChunkedScan.PortionSize ключей списка после курсора (LIMIT {portion}), {m} — до @need подходящих
    // строк JobListRow с ключом порядка; строка с m.id IS NULL — порция без совпадений.
    private const string PortionTemplate = """
        WITH k AS MATERIALIZED ({keys}),
        s AS (
            SELECT (SELECT count(*) FROM k)::int AS scanned, l.*
            FROM (SELECT 1) one LEFT JOIN LATERAL (SELECT {last} FROM k ORDER BY {desc} LIMIT 1) l ON true
        ),
        m AS ({m})
        SELECT s.*, m.* FROM s LEFT JOIN m ON true ORDER BY {morder}
        """;

    private const string QueuePortionMatches = $$"""
        SELECT {{QueueRowColumns}}, {keycols}
        FROM k q CROSS JOIN LATERAL (
            SELECT j.id, j.type, j.queue, j.priority, j.created_at FROM {schema}.jobs j
            WHERE j.id = q.job_id AND (@type::text IS NULL OR j.type = @type)
            OFFSET 0
        ) j
        WHERE (@queue::text IS NULL OR q.queue = @queue)
        ORDER BY {order} LIMIT @need
        """;

    private string ScheduledPortion(string after) => Portion(ScheduledKeysTemplate.Replace("{after}", after, StringComparison.Ordinal),
        last: "run_at, job_id", desc: "run_at DESC, job_id DESC", keycols: "q.run_at AS o1, q.job_id AS o2",
        order: "q.run_at, q.job_id", morder: "m.o1, m.o2");

    private string EnqueuedPortion(string keys) => Portion(keys,
        last: "priority, run_at, job_id", desc: "priority, run_at DESC, job_id DESC", keycols: "q.priority AS o1, q.run_at AS o2, q.job_id AS o3",
        order: "q.priority DESC, q.run_at, q.job_id", morder: "m.o1 DESC, m.o2, m.o3");

    private string AwaitingPortion(string after) => Portion(AwaitingKeysTemplate.Replace("{after}", after, StringComparison.Ordinal),
        last: "job_id", desc: "job_id", keycols: "q.job_id AS o1", order: "q.job_id DESC", morder: "m.o1 DESC");

    private string ScheduledSql => field ??= WithAfter(ScheduledTemplate, "");
    private string ScheduledAfterSql => field ??= WithAfter(ScheduledTemplate, ScheduledAfter);
    private string ScheduledPortionSql => field ??= ScheduledPortion("");
    private string ScheduledAfterPortionSql => field ??= ScheduledPortion(ScheduledAfter);
    private string EnqueuedSql => field ??= Render(EnqueuedTemplate);
    private string EnqueuedAfterSql => field ??= Render(EnqueuedAfterTemplate.Replace("{limit}", "@limit", StringComparison.Ordinal));
    private string EnqueuedPortionSql => field ??= EnqueuedPortion(EnqueuedKeysTemplate);
    private string EnqueuedAfterPortionSql => field ??= EnqueuedPortion(EnqueuedAfterKeysTemplate.Replace("{limit}", "{portion}", StringComparison.Ordinal));
    private string ProcessingSql => field ??= Render(ProcessingTemplate);
    private string AwaitingSql => field ??= WithAfter(AwaitingTemplate, "");
    private string AwaitingAfterSql => field ??= WithAfter(AwaitingTemplate, AwaitingAfter);
    private string AwaitingPortionSql => field ??= AwaitingPortion("");
    private string AwaitingAfterPortionSql => field ??= AwaitingPortion(AwaitingAfter);

    /// <summary>Chunk query over queue: keycols and order define the key and sort order of m.</summary>
    private string Portion(string keys, string last, string desc, string keycols, string order, string morder) => Portion(keys, last, desc,
        QueuePortionMatches.Replace("{keycols}", keycols, StringComparison.Ordinal).Replace("{order}", order, StringComparison.Ordinal), morder);

    // Признак «данные кончились» в ChunkedScan — Scanned < PortionSize: LIMIT порции берётся из той же константы.
    private static readonly string PortionLimit = ChunkedScan.PortionSize.ToString(CultureInfo.InvariantCulture);

    /// <summary>Chunk query over keys: last and desc define the key and reverse order in k. m and morder select and order matching rows.</summary>
    private string Portion(string keys, string last, string desc, string m, string morder) => Render(PortionTemplate
        .Replace("{keys}", keys, StringComparison.Ordinal).Replace("{portion}", PortionLimit, StringComparison.Ordinal)
        .Replace("{last}", last, StringComparison.Ordinal)
        .Replace("{desc}", desc, StringComparison.Ordinal).Replace("{m}", m, StringComparison.Ordinal)
        .Replace("{morder}", morder, StringComparison.Ordinal));

    /// <summary>
    /// Job lists (§6.1). Uses a queue index and LIMIT when the index covers the predicate. Scans chunks for Scheduled or
    /// Awaiting with queue or type filters, Enqueued with a type filter, and all terminal states
    /// (DashboardStore.TerminalLists.cs).
    /// </summary>
    public Task<Page<JobListRow>> ListJobsAsync(JobState kind, string? queue, string? type, string? after, CancellationToken ct)
    {
        switch (kind)
        {
            case JobState.Scheduled:
                return queue is null && type is null
                    ? KeysetAsync(ScheduledSql, ScheduledAfterSql, after, CursorShape.AtId, static _ => { }, ReadJobListRow, ScheduledKey, ct)
                    : PortionListAsync(ScheduledPortionSql, ScheduledAfterPortionSql, after, CursorShape.AtId, queue, type, ScheduledKey, ct);
            case JobState.Enqueued:
                if (queue is null)
                    throw new DashboardBadRequestException("queue is required for Enqueued");
                return type is null
                    ? KeysetAsync(EnqueuedSql, EnqueuedAfterSql, after, CursorShape.PriorityAtId, ps => P.Add(ps, "queue", queue),
                        ReadJobListRow, EnqueuedKey, ct)
                    : PortionListAsync(EnqueuedPortionSql, EnqueuedAfterPortionSql, after, CursorShape.PriorityAtId, queue, type, EnqueuedKey, ct);
            case JobState.Processing:
                return ProcessingAsync(queue, type, after, ct);
            case JobState.Awaiting:
                return queue is null && type is null
                    ? KeysetAsync(AwaitingSql, AwaitingAfterSql, after, CursorShape.Id, static _ => { }, ReadJobListRow, static r => new ListKey(r.Id), ct)
                    : PortionListAsync(AwaitingPortionSql, AwaitingAfterPortionSql, after, CursorShape.Id, queue, type,
                        static r => new ListKey(r.Id), ct);
            case JobState.Succeeded:
                return PortionListAsync(SucceededPortionSql, SucceededAfterPortionSql, after, CursorShape.Id, queue, type,
                    static r => new ListKey(r.Id), ct);
            case JobState.Failed:
                return PortionListAsync(FailedPortionSql, FailedAfterPortionSql, after, CursorShape.AtId, queue, type, TerminalEventKey, ct);
            case JobState.Cancelled:
                return PortionListAsync(CancelledPortionSql, CancelledAfterPortionSql, after, CursorShape.AtId, queue, type, TerminalEventKey, ct);
            default:
                throw new NotSupportedException($"Job list {kind} is not supported.");
        }
    }

    private static ListKey ScheduledKey(JobListRow r) => new(r.Id, r.RunAt);

    private static ListKey EnqueuedKey(JobListRow r) => new(r.Id, r.RunAt, r.Priority);

    private Task<Page<JobListRow>> PortionListAsync(string firstSql, string afterSql, string? after, CursorShape shape, string? queue,
        string? type, Func<JobListRow, ListKey> keyOf, CancellationToken ct) =>
        PortionPageAsync(firstSql, afterSql, after, shape, ps =>
        {
            P.AddNullable(ps, "queue", queue, NpgsqlDbType.Text);
            P.AddNullable(ps, "type", type, NpgsqlDbType.Text);
        }, ReadJobListRow, keyOf, ct);

    /// <summary>
    /// Chunked scan (§6.3). Bind filters, then @need and the cursor key. Results contain the number of keys scanned and
    /// the last key, followed by a matching row or NULL.
    /// </summary>
    private Task<Page<T>> PortionPageAsync<T>(string firstSql, string afterSql, string? after, CursorShape shape,
        Action<NpgsqlParameterCollection> bind, Func<NpgsqlDataReader, int, T> readRow, Func<T, ListKey> keyOf, CancellationToken ct) =>
        ScanPageAsync(after, shape, (cursor, need, c) => ReadAsync(cursor is null ? firstSql : afterSql, ps =>
        {
            bind(ps);
            P.Add(ps, "need", need);
            BindAfter(ps, cursor);
        }, (r, c2) => ReadPortionAsync(r, shape, readRow, c2), c), keyOf, ct);

    /// <summary>
    /// Reads the scanned key count and last key (columns determined by shape). The matching row starts after the count and
    /// key. NULL indicates a chunk without matches.
    /// </summary>
    private static async Task<Portion<T>> ReadPortionAsync<T>(NpgsqlDataReader r, CursorShape shape, Func<NpgsqlDataReader, int, T> readRow,
        CancellationToken ct)
    {
        var offset = 1 + shape switch { CursorShape.Id => 1, CursorShape.AtId => 2, _ => 3 };
        var matches = new List<T>();
        var scanned = 0;
        ListKey? last = null;
        while (await r.ReadAsync(ct))
        {
            scanned = r.GetInt32(0);
            if (scanned > 0)
                last = ReadKey(r, shape);
            if (!r.IsDBNull(offset))
                matches.Add(readRow(r, offset));
        }
        return new Portion<T>(matches, scanned, last);
    }

    /// <summary>For Processing jobs, reads all leased rows and applies ordering by (leased_at, job_id) and the cursor in memory.</summary>
    private async Task<Page<JobListRow>> ProcessingAsync(string? queue, string? type, string? after, CancellationToken ct)
    {
        var key = Cursor.Decode(after, CursorShape.AtId);
        var rows = await ReadAsync(ProcessingSql, ps =>
        {
            P.AddNullable(ps, "queue", queue, NpgsqlDbType.Text);
            P.AddNullable(ps, "type", type, NpgsqlDbType.Text);
        }, static async (r, c) => await r.ReadAllAsync(ReadJobListRow, c), ct);
        var fetched = rows
            .Where(r => key is not { } k || r.StartedAt > k.At || (r.StartedAt == k.At && r.Id.CompareTo(k.Id) > 0))
            .OrderBy(r => r.StartedAt).ThenBy(r => r.Id)
            .Take(PageSize + 1)
            .ToList();
        return LimitPage(fetched, static r => new ListKey(r.Id, r.StartedAt), CursorShape.AtId);
    }

    private static void BindAfter(NpgsqlParameterCollection ps, ListKey? key)
    {
        if (key is not { } k)
            return;
        P.Add(ps, "after_id", k.Id);
        if (k.At is { } at)
            P.Add(ps, "after_at", at, NpgsqlDbType.TimestampTz);
        if (k.Priority is { } priority)
            P.Add(ps, "after_priority", priority);
    }

    private static ListKey ReadKey(NpgsqlDataReader r, CursorShape shape) => shape switch
    {
        CursorShape.Id => new ListKey(r.GetGuid(1)),
        CursorShape.AtId => new ListKey(r.GetGuid(2), r.GetDateTime(1)),
        _ => new ListKey(r.GetGuid(3), r.GetDateTime(2), r.GetInt16(1)),
    };

    private static JobListRow ReadJobListRow(NpgsqlDataReader r) => ReadJobListRow(r, 0);

    /// <summary>Reads a JobListRow starting at column offset.</summary>
    private static JobListRow ReadJobListRow(NpgsqlDataReader r, int o) => new(r.GetGuid(o), r.GetString(o + 1), r.GetString(o + 2),
        r.GetInt16(o + 3), r.GetInt32(o + 4), r.GetInt32(o + 5), r.GetDateTime(o + 6), r.GetNullableValue<DateTime>(o + 7),
        r.GetNullableValue<DateTime>(o + 8), r.GetNullableValue<Guid>(o + 9), r.GetNullableValue<DateTime>(o + 10), r.GetBoolean(o + 11),
        (JobEventKind?)r.GetNullableValue<short>(o + 12), r.GetNullable<string>(o + 13));
}
