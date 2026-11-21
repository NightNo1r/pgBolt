using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Recurring;

/// <summary>Logs synchronization changes at Information and collisions at Error. A pass with no differences logs nothing (Part 4 §7).</summary>
internal static partial class RecurringSyncLog
{
    public static void Write(ILogger logger, string owner, RecurringSyncResult result)
    {
        foreach (var id in result.Inserted)
            RecurringJobInserted(logger, id, owner);
        foreach (var change in result.Updated)
            RecurringJobUpdated(logger, change.Id, owner, string.Join(", ", change.Fields));
        foreach (var id in result.Deleted)
            RecurringJobDeleted(logger, id, owner);
        foreach (var id in result.Collisions)
            RecurringJobOwnerCollision(logger, id, owner);
    }

    [LoggerMessage(EventId = 6100, EventName = nameof(RecurringJobInserted), Level = LogLevel.Information,
        Message = "PgBolt: recurring job {Id} of owner {Owner} inserted.")]
    private static partial void RecurringJobInserted(ILogger logger, string id, string owner);

    [LoggerMessage(EventId = 6101, EventName = nameof(RecurringJobUpdated), Level = LogLevel.Information,
        Message = "PgBolt: recurring job {Id} of owner {Owner} updated ({Fields}).")]
    private static partial void RecurringJobUpdated(ILogger logger, string id, string owner, string fields);

    [LoggerMessage(EventId = 6102, EventName = nameof(RecurringJobDeleted), Level = LogLevel.Information,
        Message = "PgBolt: recurring job {Id} of owner {Owner} deleted (no longer declared).")]
    private static partial void RecurringJobDeleted(ILogger logger, string id, string owner);

    [LoggerMessage(EventId = 6103, EventName = nameof(RecurringJobOwnerCollision), Level = LogLevel.Error,
        Message = "PgBolt: recurring job {Id} declared by owner {Owner} belongs to another owner or was created through the API; left untouched.")]
    private static partial void RecurringJobOwnerCollision(ILogger logger, string id, string owner);

    /// <summary>Called by <see cref="PgBoltStore"/> when retrying a synchronization transaction after a deadlock.</summary>
    [LoggerMessage(EventId = 6104, EventName = nameof(RecurringSyncDeadlockRetry), Level = LogLevel.Warning,
        Message = "PgBolt: recurring sync of owner {Owner} hit a deadlock (40P01); retrying the transaction (attempt {Attempt} of {MaxAttempts}).")]
    internal static partial void RecurringSyncDeadlockRetry(ILogger logger, Exception e, string owner, int attempt, int maxAttempts);
}
