using System.Reflection;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard;
using PgBolt.Postgres.Hosting;
using PgBolt.Server;

namespace PgBolt.E2E.Tests;

/// <summary>
/// Контракт идентификаторов событий логов: набор (Id, Name, Level) зафиксирован, Id и имена уникальны
/// и лежат в диапазоне компонента. Подписчики на логи (алерты, фильтры) ссылаются на эти значения.
/// </summary>
public sealed class EventIdContractTests
{
    private static readonly Assembly[] Assemblies =
    [
        typeof(PgBoltInitializer).Assembly,
        typeof(PgBoltServerOptions).Assembly,
        typeof(PgBoltDashboardOptions).Assembly,
    ];

    /// <summary>Диапазон Id по полному имени класса, объявляющего события.</summary>
    private static readonly Dictionary<string, (int From, int To)> Ranges = new()
    {
        ["PgBolt.Server.Runtime.PgBoltProcessingService+Log"] = (1000, 1099),
        ["PgBolt.Server.Runtime.PgBoltServer+Log"] = (1100, 1199),
        ["PgBolt.Server.Runtime.QueueSettingsCache+Log"] = (1200, 1299),
        ["PgBolt.Server.Runtime.Dispatcher+Log"] = (2000, 2999),
        ["PgBolt.Server.Runtime.JobRunner+Log"] = (3000, 3999),
        ["PgBolt.Server.Runtime.RetryDecider+Log"] = (3000, 3999),
        ["PgBolt.Server.Runtime.CompletionSink+Log"] = (4000, 4099),
        ["PgBolt.Server.Runtime.LeaseKeeper+Log"] = (4100, 4199),
        ["PgBolt.Server.Runtime.OutcomeStatsFlusher+Log"] = (4200, 4299),
        ["PgBolt.Server.Runtime.ProcessingMaintenance+Log"] = (5000, 5099),
        ["PgBolt.Server.Runtime.RetentionReport+Log"] = (5100, 5199),
        ["PgBolt.Postgres.Hosting.PgBoltInitializer+Log"] = (6000, 6099),
        ["PgBolt.Postgres.Recurring.RecurringSyncLog"] = (6100, 6199),
        ["PgBolt.Dashboard.Security.DashboardStartupCheck+Log"] = (7000, 7099),
        ["PgBolt.Dashboard.Endpoints.DashboardActionLog"] = (7100, 7199),
        ["PgBolt.Dashboard.Endpoints.DashboardErrorFilter+Log"] = (7100, 7199),
        ["PgBolt.Postgres.Diagnostics.PgBoltInstrumentation+Log"] = (8000, 8999),
    };

    private static readonly (int Id, string Name, LogLevel Level)[] Expected =
    [
        (1000, "JobTypeSchemaMismatch", LogLevel.Warning),
        (1001, "ProcessingStartFailed", LogLevel.Critical),
        (1002, "HostStopTimeoutServers", LogLevel.Warning),
        (1003, "HostStopTimeoutMaintenance", LogLevel.Warning),
        (1004, "QueueDefaultsConflict", LogLevel.Warning),
        (1005, "HostShutdownTimeoutTooShort", LogLevel.Warning),
        (1006, "PartitionDaysAheadTooShort", LogLevel.Warning),
        (1007, "DeadServerTimeoutTooShort", LogLevel.Warning),
        (1100, "ServerStarted", LogLevel.Information),
        (1101, "ExecutionsWaitCancelled", LogLevel.Warning),
        (1102, "ServerStopCancelled", LogLevel.Warning),
        (1103, "ServerRowRemoveFailed", LogLevel.Error),
        (1104, "ServerStopped", LogLevel.Information),
        (1105, "DispatcherFailed", LogLevel.Critical),
        (1106, "CancellationCallbackFailed", LogLevel.Warning),
        (1107, "JobTypesWithoutExecutor", LogLevel.Warning),
        (1108, "MissingExecutorsCheckFailed", LogLevel.Warning),
        (1109, "JobFinishFailed", LogLevel.Critical),
        (1110, "JobAbandoned", LogLevel.Warning),
        (1111, "JobIgnoresCancellation", LogLevel.Warning),
        (1112, "SlotReleaseFailed", LogLevel.Error),
        (1200, "QueueSettingsRefreshFailed", LogLevel.Warning),
        (2000, "DequeueFailed", LogLevel.Error),
        (2001, "DequeueTimedOutJobsReleased", LogLevel.Warning),
        (2002, "DispatcherIterationFailed", LogLevel.Critical),
        (2003, "WakeupSubscriptionDisposeFailed", LogLevel.Warning),
        (2004, "WakeupSubscriptionFailed", LogLevel.Error),
        (2005, "StartRollbackSlotReleaseFailed", LogLevel.Error),
        (2006, "JobStartRolledBack", LogLevel.Warning),
        (3001, "JobStarted", LogLevel.Debug),
        (3002, "JobSucceeded", LogLevel.Debug),
        (3003, "JobFailedWillRetry", LogLevel.Warning),
        (3004, "JobFailed", LogLevel.Error),
        (3005, "JobNoExecutorWillRetry", LogLevel.Warning),
        (3006, "JobNoExecutorFailed", LogLevel.Error),
        (3007, "JobCancelled", LogLevel.Information),
        (3008, "JobReleased", LogLevel.Debug),
        (3100, "RetryPolicyFailed", LogLevel.Error),
        (3101, "JobTimeoutCapped", LogLevel.Warning),
        (4000, "OutcomeDiscardedSinkStopped", LogLevel.Warning),
        (4001, "OutcomeDiscardedNotRunning", LogLevel.Warning),
        (4002, "SinkStopCancelled", LogLevel.Warning),
        (4003, "AckDiscardedStopCancelled", LogLevel.Warning),
        (4004, "AckPartialAfterRetry", LogLevel.Information),
        (4005, "AckPartialLeaseLost", LogLevel.Warning),
        (4006, "OutcomeLeaseNotFoundAfterRetry", LogLevel.Information),
        (4007, "OutcomeDiscardedLeaseLost", LogLevel.Warning),
        (4008, "OutcomeNotSentExecutionState", LogLevel.Debug),
        (4009, "LeaseFreedNotificationFailed", LogLevel.Error),
        (4010, "OutcomeSendFailedRetrying", LogLevel.Warning),
        (4011, "OutcomeNotSentStopCancelled", LogLevel.Warning),
        (4012, "OutcomeSendBudgetExceeded", LogLevel.Error),
        (4100, "LeaseRenewalNotConfirmed", LogLevel.Warning),
        (4101, "HeartbeatTimedOut", LogLevel.Warning),
        (4102, "HeartbeatFailed", LogLevel.Warning),
        (4103, "JobCancellationRequested", LogLevel.Information),
        (4104, "LeaseLostExecutionCancelled", LogLevel.Warning),
        (4105, "LeaseLostOutcomeDropped", LogLevel.Warning),
        (4106, "HeartbeatRecovered", LogLevel.Information),
        (4107, "HeartbeatRecoveryCallbackFailed", LogLevel.Error),
        (4108, "ServerReRegistered", LogLevel.Warning),
        (4109, "ServerReRegistrationFailed", LogLevel.Warning),
        (4110, "LeaseDeadlineCheckFailed", LogLevel.Error),
        (4200, "OutcomeStatsFlushFailed", LogLevel.Warning),
        (5000, "BatchCompletionNotCounted", LogLevel.Debug),
        (5001, "ResolverLagging", LogLevel.Warning),
        (5002, "RecurringFireFailed", LogLevel.Error),
        (5003, "RecurringMisfireSkipped", LogLevel.Warning),
        (5004, "RecurringOverlapSkipped", LogLevel.Debug),
        (5005, "RecurringWaitingForPrevious", LogLevel.Debug),
        (5006, "RecurringExhausted", LogLevel.Warning),
        (5007, "MaintenanceTaskFailed", LogLevel.Error),
        (5008, "FuturePartitionsLockTimeout", LogLevel.Warning),
        (5009, "TodayPartitionsMissing", LogLevel.Error),
        (5010, "DeadServersRemoved", LogLevel.Information),
        (5011, "MaintenanceLoopIterationFailed", LogLevel.Error),
        (5012, "QueueStatsReadFailed", LogLevel.Warning),
        (5013, "TableStatsReadFailed", LogLevel.Warning),
        (5100, "RetentionPartitionsDropped", LogLevel.Information),
        (5101, "RetentionBatchesDeleted", LogLevel.Information),
        (5102, "RetentionPinnedPartitions", LogLevel.Information),
        (5103, "RetentionPartitionTooManyLiveJobs", LogLevel.Warning),
        (5104, "RetentionCompactionSkippedReplication", LogLevel.Warning),
        (5105, "RetentionTooManyAttachedPartitions", LogLevel.Warning),
        (5106, "RetentionHoldTimeout", LogLevel.Warning),
        // Уровень задаётся при вызове, в атрибуте его нет.
        (5107, "RetentionDeferredByLockTimeout", LogLevel.None),
        (5108, "RetentionRemnantDropFailed", LogLevel.Error),
        (6000, "SchemaVersionNewer", LogLevel.Warning),
        (6001, "StartupFuturePartitionsLockTimeout", LogLevel.Warning),
        (6002, "BackgroundPartitionCreationFailed", LogLevel.Error),
        (6100, "RecurringJobInserted", LogLevel.Information),
        (6101, "RecurringJobUpdated", LogLevel.Information),
        (6102, "RecurringJobDeleted", LogLevel.Information),
        (6103, "RecurringJobOwnerCollision", LogLevel.Error),
        (6104, "RecurringSyncDeadlockRetry", LogLevel.Warning),
        (7000, "ActionsPolicyNotRegistered", LogLevel.Error),
        (7001, "AnonymousRemoteAccessAllowed", LogLevel.Warning),
        (7002, "LocalRequestsOnly", LogLevel.Warning),
        (7100, "DashboardActionPerformed", LogLevel.Information),
        (7101, "DashboardRequestFailed", LogLevel.Error),
        (8001, "TelemetryFailed", LogLevel.Warning),
        (8002, "MetricCallbackFailed", LogLevel.Warning),
    ];

    private const BindingFlags AnyMethod = BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic
        | BindingFlags.DeclaredOnly;

    private static List<(MethodInfo Method, LoggerMessageAttribute Attribute)> Events() => Assemblies
        .SelectMany(a => a.GetTypes())
        .SelectMany(t => t.GetMethods(AnyMethod))
        .Select(m => (Method: m, Attribute: m.GetCustomAttribute<LoggerMessageAttribute>()))
        .Where(x => x.Attribute is not null)
        .Select(x => (x.Method, x.Attribute!))
        .ToList();

    [Fact]
    public void EventIds_are_unique_explicit_and_in_range()
    {
        var events = Events();

        foreach (var (method, attribute) in events)
        {
            var owner = method.DeclaringType!.FullName!;
            Assert.True(attribute.EventId > 0, $"{owner}.{method.Name}: EventId не задан");
            Assert.Equal(method.Name, attribute.EventName);
            Assert.True(Ranges.TryGetValue(owner, out var range), $"{owner}: нет диапазона Id");
            Assert.InRange(attribute.EventId, range.From, range.To);
        }

        Assert.Equal(events.Count, events.Select(e => e.Attribute.EventId).Distinct().Count());
        Assert.Equal(events.Count, events.Select(e => e.Attribute.EventName).Distinct().Count());

        var actual = events.Select(e => (e.Attribute.EventId, e.Attribute.EventName!, e.Attribute.Level))
            .OrderBy(e => e.EventId).ToArray();
        Assert.Equal(Expected.OrderBy(e => e.Id).ToArray(), actual);
    }
}
