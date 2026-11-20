using Microsoft.Extensions.Logging;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>
/// Запись о действии оператора. Отдельный класс: у <see cref="ActionEndpoints"/> уже есть метод <c>Log</c>,
/// вложенный тип с тем же именем не компилируется.
/// </summary>
internal static partial class DashboardActionLog
{
    [LoggerMessage(EventId = 7100, EventName = nameof(DashboardActionPerformed), Level = LogLevel.Information,
        Message = "PgBolt dashboard: {User} {Action} {Target}: {Result}.")]
    internal static partial void DashboardActionPerformed(ILogger logger, string user, string action, string target, string result);
}
