using System.Text.Json;
using Npgsql;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string GetJobTemplate = """
        SELECT j.id, j.type, j.queue, j.priority, j.args, j.state, j.attempt, j.max_attempts, j.created_at,
               j.started_at, j.finished_at, j.server_id, j.unique_key,
               q.job_id IS NOT NULL, q.run_at, q.ready, q.lease_owner, q.attempt, q.leased_at, q.cancel_requested,
               j.recurring_id, j.scheduled_at, q.await_condition, j.parent_id, j.batch_id, j.continuation
        FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
        WHERE j.id = @id
        """;

    private const string GetEventsTemplate = """
        SELECT kind, at, server_id, attempt, message, exception
        FROM {schema}.job_events
        WHERE job_id = @id AND at >= @created_from
        ORDER BY at DESC
        LIMIT 50
        """;

    private string GetJobSql => field ??= Render(GetJobTemplate);
    private string GetEventsSql => field ??= Render(GetEventsTemplate);

    public Task<JobInfo?> GetJobAsync(Guid jobId, CancellationToken ct) =>
        RunAsync<JobInfo?>(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands = { new NpgsqlBatchCommand(GetJobSql), new NpgsqlBatchCommand(GetEventsSql) },
            };
            P.Add(batch.BatchCommands[0].Parameters, "id", jobId);
            P.Add(batch.BatchCommands[1].Parameters, "id", jobId);
            P.Add(batch.BatchCommands[1].Parameters, "created_from", UuidV7.EventsLowerBound(jobId));
            if (_prepare)
                await batch.PrepareAsync(c);

            await using var r = await batch.ExecuteReaderAsync(c);
            if (!await r.ReadAsync(c))
                return null;

            var stored = r.GetInt16(5);
            var inQueue = r.GetBoolean(13);
            var ready = inQueue && r.GetBoolean(15);
            var leaseOwner = r.GetNullableValue<Guid>(16);
            var awaiting = inQueue && !r.IsDBNull(22);
            var state = Derive(stored, inQueue, ready, leaseOwner, awaiting);
            using var args = JsonDocument.Parse(r.GetString(4));
            var id = r.GetGuid(0);
            var type = r.GetString(1);
            var queue = r.GetString(2);
            var priority = r.GetInt16(3);
            var attempt = inQueue ? r.GetInt32(17) : r.GetInt32(6);
            var maxAttempts = r.GetInt32(7);
            var createdAt = r.GetFieldValue<DateTimeOffset>(8);
            var startedAt = state == JobState.Processing ? r.GetNullableValue<DateTimeOffset>(18) : r.GetNullableValue<DateTimeOffset>(9);
            var finishedAt = r.GetNullableValue<DateTimeOffset>(10);
            var serverId = state == JobState.Processing ? leaseOwner : r.GetNullableValue<Guid>(11);
            var uniqueKey = r.GetNullable<string>(12);
            var runAt = inQueue && !awaiting ? r.GetFieldValue<DateTimeOffset>(14) : (DateTimeOffset?)null;
            var cancelRequested = inQueue && r.GetBoolean(19);
            var argsElement = args.RootElement.Clone();

            var recurringId = r.GetNullable<string>(20);
            var scheduledAt = r.GetNullableValue<DateTimeOffset>(21);
            var parentId = r.GetNullableValue<Guid>(23);
            var batchId = r.GetNullableValue<Guid>(24);
            var continuation = r.GetNullableValue<short>(25);

            await r.NextResultAsync(c);
            var events = await r.ReadAllAsync(static e => new JobEventInfo(
                (JobEventKind)e.GetInt16(0), e.GetFieldValue<DateTimeOffset>(1), e.GetNullableValue<Guid>(2),
                e.GetNullableValue<int>(3), e.GetNullable<string>(4), e.GetNullable<string>(5)), c);

            return new JobInfo(new JobId(id), type, queue, priority, state, attempt, maxAttempts, createdAt, runAt,
                startedAt, finishedAt, serverId, uniqueKey, cancelRequested, argsElement, events, recurringId, scheduledAt,
                WorkflowCodes.ParentJob(continuation, parentId), WorkflowCodes.ParentBatch(continuation, parentId),
                batchId is { } b ? new BatchId(b) : null, WorkflowCodes.Condition(continuation));
        }, ct);

    /// <summary>
    /// Client state mapping (core §2, Part 5). Awaiting has a queue row with await_condition and jobs.state = 0. Scheduled has no lease, is not
    /// ready, and is not Awaiting. jobs.state code 1 is unused.
    /// </summary>
    internal static JobState Derive(short stored, bool inQueue, bool ready, Guid? leaseOwner, bool awaiting) => stored switch
    {
        0 when leaseOwner is not null => JobState.Processing,
        0 when awaiting => JobState.Awaiting,
        0 when inQueue && !ready => JobState.Scheduled,
        0 => JobState.Enqueued,
        2 => JobState.Succeeded,
        3 => JobState.Failed,
        4 => JobState.Cancelled,
        _ => throw new InvalidOperationException($"Unknown stored job state {stored}."),
    };
}
