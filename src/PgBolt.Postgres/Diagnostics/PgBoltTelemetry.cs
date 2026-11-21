using System.Diagnostics;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>PgBolt trace source, <c>messaging.system</c> value, and span and metric attribute names.</summary>
internal static class PgBoltTelemetry
{
    public const string System = "pgbolt";

    public static ActivitySource Source { get; } = new(PgBoltDiagnostics.ActivitySourceName, PgBoltInstrumentation.Version);

    /// <summary>
    /// Current span ID for <c>trace_parent</c>, stored only in W3C format. A hierarchical parent produces a hierarchical child whose ID is omitted.
    /// </summary>
    public static string? CurrentTraceParent() =>
        Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity ? activity.Id : null;

    public static class Attr
    {
        public const string MessagingSystem = "messaging.system";
        public const string MessagingOperationType = "messaging.operation.type";
        public const string MessagingDestinationName = "messaging.destination.name";
        public const string MessagingMessageId = "messaging.message.id";
        public const string MessagingBatchMessageCount = "messaging.batch.message_count";
        public const string ErrorType = "error.type";

        public const string Queue = "pgbolt.queue";
        public const string JobType = "pgbolt.job.type";
        public const string JobAttempt = "pgbolt.job.attempt";
        public const string JobMaxAttempts = "pgbolt.job.max_attempts";
        public const string JobParentJobId = "pgbolt.job.parent_job_id";
        public const string JobParentBatchId = "pgbolt.job.parent_batch_id";
        public const string JobOutcome = "pgbolt.job.outcome";
        public const string JobState = "pgbolt.job.state";
        public const string BatchId = "pgbolt.batch.id";
        public const string RecurringId = "pgbolt.recurring.id";
        public const string ServerName = "pgbolt.server.name";
        public const string EnqueueResult = "pgbolt.enqueue.result";
        public const string DropReason = "pgbolt.drop.reason";
        public const string StorageOperation = "pgbolt.storage.operation";
        public const string MaintenanceTask = "pgbolt.maintenance.task";
        public const string MaintenanceResult = "pgbolt.maintenance.result";
        public const string RetentionReason = "pgbolt.retention.reason";
        public const string Table = "pgbolt.table";
    }
}
