using Microsoft.Extensions.Logging;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>
/// Operator action log. Kept separate because <see cref="ActionEndpoints"/> already defines a <c>Log</c> method and cannot contain a nested type with the same name.
/// </summary>
internal static partial class DashboardActionLog
{
    [LoggerMessage(EventId = 7100, EventName = nameof(DashboardActionPerformed), Level = LogLevel.Information,
        Message = "PgBolt dashboard: {User} {Action} {Target}: {Result}.")]
    internal static partial void DashboardActionPerformed(ILogger logger, string user, string action, string target, string result);
}
