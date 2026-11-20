using Npgsql;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres;
using PgBolt.Postgres.Storage;

namespace PgBolt.Dashboard.Data;

internal sealed partial class DashboardStore
{
    // RelatedJobRow: строка jobs и производное состояние из queue (PgBoltStore.Derive).
    private const string RelatedJobColumns = """
        j.id, j.type, j.queue, j.state, j.continuation, j.created_at, j.finished_at,
        q.job_id IS NOT NULL, coalesce(q.ready, false), q.lease_owner, q.await_condition IS NOT NULL
        """;

    // Прогресс — счётчики, ограниченные снизу нулём (часть 5 §10); имена — для ссылок на колонки порции.
    private const string BatchColumns = """
        id, state, total, greatest(0, succeeded) AS succeeded, greatest(0, failed) AS failed, greatest(0, cancelled) AS cancelled,
        condition, cancel_requested, created_at, finished_at
        """;

    private const string JobIdAfter = "AND j.id > @after_id";
    private const string BatchIdAfter = "AND id > @after_id";

    // Продолжения-задачи (§6.2): jobs_parent; {codes} — литерал (1, 2) или (3, 4); @lower отсекает партиции.
    private const string ContinuationsTemplate = $$"""
        SELECT {{RelatedJobColumns}} FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
        WHERE j.parent_id = @parent AND j.continuation IN {codes} AND j.id >= @lower {after}
        ORDER BY j.id LIMIT @limit
        """;

    // Члены batch (§6.2, отклонение 10): jobs_batch.
    private const string BatchJobsTemplate = $$"""
        SELECT {{RelatedJobColumns}} FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
        WHERE j.batch_id = @batch AND j.id >= @lower {after}
        ORDER BY j.id LIMIT @limit
        """;

    // Batch-продолжения узла: batches_parent.
    private const string BatchContinuationsTemplate = $$"""
        SELECT {{BatchColumns}} FROM {schema}.batches WHERE parent_id = @parent {after} ORDER BY id LIMIT @limit
        """;

    // Вложенные batches: batches_nested.
    private const string NestedBatchesTemplate = $$"""
        SELECT {{BatchColumns}} FROM {schema}.batches WHERE parent_batch_id = @batch {after} ORDER BY id LIMIT @limit
        """;

    // Batches (§6.1): PK id DESC; с фильтром состояния — порциями (§6.3).
    private const string BatchesAfter = "WHERE id < @after_id";

    private const string BatchesTemplate = $$"""
        SELECT {{BatchColumns}} FROM {schema}.batches {after} ORDER BY id DESC LIMIT @limit
        """;

    private const string BatchesKeysTemplate = $$"""
        SELECT {{BatchColumns}} FROM {schema}.batches {after} ORDER BY id DESC LIMIT {portion}
        """;

    private const string BatchesMatches = """
        SELECT k.*, k.id AS o1 FROM k WHERE k.state = @state ORDER BY k.id DESC LIMIT @need
        """;

    // Recurring (форма курсора Text) и история запусков (часть 4 §9): jobs_recurring.
    private const string RecurringTemplate = """
        SELECT id, type, cron, time_zone, owner, paused, next_fire_at, last_fire_at, last_job_id, last_error
        FROM {schema}.recurring_jobs {after} ORDER BY id LIMIT @limit
        """;

    private const string RecurringRunsTemplate = $$"""
        SELECT {{RelatedJobColumns}} FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
        WHERE j.recurring_id = @id {after} ORDER BY j.id DESC LIMIT @limit
        """;

    private string WithAfter(string template, string after) => Render(template.Replace("{after}", after, StringComparison.Ordinal));

    private string BatchesPortion(string after) => Portion(BatchesKeysTemplate.Replace("{after}", after, StringComparison.Ordinal),
        last: "id", desc: "id", m: BatchesMatches, morder: "m.o1 DESC");

    private string JobContinuationsSql => field ??= WithAfter(ContinuationsTemplate.Replace("{codes}", "(1, 2)", StringComparison.Ordinal), "");
    private string JobContinuationsAfterSql => field ??= WithAfter(ContinuationsTemplate.Replace("{codes}", "(1, 2)", StringComparison.Ordinal), JobIdAfter);
    private string BatchParentContinuationsSql => field ??= WithAfter(ContinuationsTemplate.Replace("{codes}", "(3, 4)", StringComparison.Ordinal), "");
    private string BatchParentContinuationsAfterSql => field ??= WithAfter(ContinuationsTemplate.Replace("{codes}", "(3, 4)", StringComparison.Ordinal), JobIdAfter);
    private string BatchJobsSql => field ??= WithAfter(BatchJobsTemplate, "");
    private string BatchJobsAfterSql => field ??= WithAfter(BatchJobsTemplate, JobIdAfter);
    private string BatchContinuationsSql => field ??= WithAfter(BatchContinuationsTemplate, "");
    private string BatchContinuationsAfterSql => field ??= WithAfter(BatchContinuationsTemplate, BatchIdAfter);
    private string NestedBatchesSql => field ??= WithAfter(NestedBatchesTemplate, "");
    private string NestedBatchesAfterSql => field ??= WithAfter(NestedBatchesTemplate, BatchIdAfter);
    private string BatchesSql => field ??= WithAfter(BatchesTemplate, "");
    private string BatchesAfterSql => field ??= WithAfter(BatchesTemplate, BatchesAfter);
    private string BatchesPortionSql => field ??= BatchesPortion("");
    private string BatchesAfterPortionSql => field ??= BatchesPortion(BatchesAfter);
    private string RecurringSql => field ??= WithAfter(RecurringTemplate, "");
    private string RecurringAfterSql => field ??= WithAfter(RecurringTemplate, "WHERE id > @after_text");
    private string RecurringRunsSql => field ??= WithAfter(RecurringRunsTemplate, "");
    private string RecurringRunsAfterSql => field ??= WithAfter(RecurringRunsTemplate, "AND j.id < @after_id");

    /// <summary>Продолжения-задачи (§6.2): родитель-задача — коды 1, 2, родитель-batch — 3, 4; член batch-продолжения (5) — нет.</summary>
    public Task<Page<RelatedJobRow>> ListContinuationsAsync(Guid parentId, bool batchParent, DateTimeOffset parentCreatedAt, string? after,
        CancellationToken ct) =>
        KeysetAsync(batchParent ? BatchParentContinuationsSql : JobContinuationsSql,
            batchParent ? BatchParentContinuationsAfterSql : JobContinuationsAfterSql, after, CursorShape.Id, ps =>
            {
                P.Add(ps, "parent", parentId);
                P.Add(ps, "lower", LowerBound(parentCreatedAt));
            }, ReadRelatedJobRow, static r => new ListKey(r.Id), ct);

    public Task<Page<BatchRow>> ListBatchContinuationsAsync(Guid parentId, string? after, CancellationToken ct) =>
        KeysetAsync(BatchContinuationsSql, BatchContinuationsAfterSql, after, CursorShape.Id, ps => P.Add(ps, "parent", parentId),
            static r => ReadBatchRow(r, 0), static r => new ListKey(r.Id), ct);

    public Task<Page<RelatedJobRow>> ListBatchJobsAsync(Guid batchId, DateTimeOffset batchCreatedAt, string? after, CancellationToken ct) =>
        KeysetAsync(BatchJobsSql, BatchJobsAfterSql, after, CursorShape.Id, ps =>
        {
            P.Add(ps, "batch", batchId);
            P.Add(ps, "lower", LowerBound(batchCreatedAt));
        }, ReadRelatedJobRow, static r => new ListKey(r.Id), ct);

    public Task<Page<BatchRow>> ListNestedBatchesAsync(Guid batchId, string? after, CancellationToken ct) =>
        KeysetAsync(NestedBatchesSql, NestedBatchesAfterSql, after, CursorShape.Id, ps => P.Add(ps, "batch", batchId),
            static r => ReadBatchRow(r, 0), static r => new ListKey(r.Id), ct);

    /// <summary>Batches (§6.1): без фильтра — PK и LIMIT, с фильтром состояния — порциями (§6.3).</summary>
    public Task<Page<BatchRow>> ListBatchesAsync(BatchState? state, string? after, CancellationToken ct)
    {
        if (state is not { } filter)
            return KeysetAsync(BatchesSql, BatchesAfterSql, after, CursorShape.Id, static _ => { },
                static r => ReadBatchRow(r, 0), static r => new ListKey(r.Id), ct);
        return PortionPageAsync(BatchesPortionSql, BatchesAfterPortionSql, after, CursorShape.Id, ps => P.Add(ps, "state", (short)filter),
            ReadBatchRow, static r => new ListKey(r.Id), ct);
    }

    public Task<Page<RecurringRow>> ListRecurringAsync(string? after, CancellationToken ct) =>
        KeysetAsync(RecurringSql, RecurringAfterSql, after, CursorShape.Text, static _ => { }, static r => new RecurringRow(
                r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetNullable<string>(4), r.GetBoolean(5),
                r.GetNullableValue<DateTime>(6), r.GetNullableValue<DateTime>(7), r.GetNullableValue<Guid>(8), r.GetNullable<string>(9)),
            static r => new ListKey(Guid.Empty, Text: r.Id), ct);

    /// <summary>История запусков расписания: id DESC, производное состояние.</summary>
    public Task<Page<RelatedJobRow>> ListRecurringRunsAsync(string recurringId, string? after, CancellationToken ct) =>
        KeysetAsync(RecurringRunsSql, RecurringRunsAfterSql, after, CursorShape.Id, ps => P.Add(ps, "id", recurringId),
            ReadRelatedJobRow, static r => new ListKey(r.Id), ct);

    /// <summary>§6.2: UUIDv7 от времени создания минус сутки — запас на расхождение часов клиентов.</summary>
    private static Guid LowerBound(DateTimeOffset createdAt) => UuidV7.LowerBound(createdAt.AddDays(-1));

    /// <summary>Keyset-список: до PageSize + 1 строк после курсора (ключ формы shape или after_text для формы Text).</summary>
    private async Task<Page<T>> KeysetAsync<T>(string firstSql, string afterSql, string? after, CursorShape shape,
        Action<NpgsqlParameterCollection> bind, Func<NpgsqlDataReader, T> readRow, Func<T, ListKey> keyOf, CancellationToken ct)
    {
        var key = Cursor.Decode(after, shape);
        var fetched = await ReadAsync(key is null ? firstSql : afterSql, ps =>
        {
            P.Add(ps, "limit", PageSize + 1);
            bind(ps);
            if (shape != CursorShape.Text)
                BindAfter(ps, key);
            else if (key is { } k)
                P.Add(ps, "after_text", k.Text!);
        }, (r, c) => r.ReadAllAsync(readRow, c), ct);
        return LimitPage(fetched, keyOf, shape);
    }

    private static RelatedJobRow ReadRelatedJobRow(NpgsqlDataReader r) => new(r.GetGuid(0), r.GetString(1), r.GetString(2),
        PgBoltStore.Derive(r.GetInt16(3), r.GetBoolean(7), r.GetBoolean(8), r.GetNullableValue<Guid>(9), r.GetBoolean(10)),
        r.GetNullableValue<short>(4), r.GetDateTime(5), r.GetNullableValue<DateTime>(6));

    private static BatchRow ReadBatchRow(NpgsqlDataReader r, int o) => new(r.GetGuid(o), (BatchState)r.GetInt16(o + 1), r.GetInt32(o + 2),
        r.GetInt32(o + 3), r.GetInt32(o + 4), r.GetInt32(o + 5), r.GetNullableValue<short>(o + 6), r.GetBoolean(o + 7),
        r.GetDateTime(o + 8), r.GetNullableValue<DateTime>(o + 9));
}
