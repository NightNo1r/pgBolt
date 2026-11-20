using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard.Endpoints;

namespace PgBolt.Dashboard.Security;

/// <summary>
/// §5.3: conventions группы применяются после MapPgBoltDashboard, поэтому авторизация проверяется по ApplicationStarted обходом
/// EndpointDataSource — только endpoint'ов своей группы (метка marker). Не больше одного предупреждения за вызов. ActionsPolicy —
/// опция хоста, а не группы: незарегистрированную (иначе каждый запрос dashboard падает в фильтре доступа) сообщает одна группа —
/// та, чей endpoint первый в EndpointDataSource. Статического состояния нет (несколько хостов в одном процессе).
/// </summary>
internal static partial class DashboardStartupCheck
{
    public static void Register(IServiceProvider services, object marker) =>
        services.GetRequiredService<IHostApplicationLifetime>().ApplicationStarted.Register(() => Check(services, marker));

    private static void Check(IServiceProvider services, object marker)
    {
        var options = services.GetRequiredService<PgBoltDashboardOptions>();
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(DashboardStartupCheck));
        var policyProvider = services.GetService<IAuthorizationPolicyProvider>();
        // Провайдер по умолчанию возвращает завершённые задачи; callback ApplicationStarted синхронный.
        var endpoints = services.GetRequiredService<EndpointDataSource>().Endpoints;
        if (options is { ReadOnly: false, ActionsPolicy: { } policy }
            && endpoints.FirstOrDefault(e => e.Metadata.GetMetadata<DashboardEndpoint>() is not null)?.Metadata.Contains(marker) == true
            && policyProvider?.GetPolicyAsync(policy).GetAwaiter().GetResult() is null)
            Log.ActionsPolicyNotRegistered(logger, policy);
        var hasFallback = policyProvider is not null && policyProvider.GetFallbackPolicyAsync().GetAwaiter().GetResult() is not null;
        var unauthorized = endpoints.Any(e => e.Metadata.Contains(marker) && !DashboardAccess.HasAuthorization(e.Metadata, hasFallback));
        if (!unauthorized)
            return;
        if (options.AllowAnonymousRemote)
            Log.AnonymousRemoteAccessAllowed(logger);
        else
            Log.LocalRequestsOnly(logger);
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 7000, EventName = nameof(ActionsPolicyNotRegistered), Level = LogLevel.Error,
            Message = "PgBolt dashboard: ActionsPolicy '{Policy}' is not registered: every dashboard request fails until AddAuthorization defines it.")]
        internal static partial void ActionsPolicyNotRegistered(ILogger logger, string policy);

        [LoggerMessage(EventId = 7001, EventName = nameof(AnonymousRemoteAccessAllowed), Level = LogLevel.Warning,
            Message = "PgBolt dashboard allows anonymous remote access (AllowAnonymousRemote = true).")]
        internal static partial void AnonymousRemoteAccessAllowed(ILogger logger);

        [LoggerMessage(EventId = 7002, EventName = nameof(LocalRequestsOnly), Level = LogLevel.Warning,
            Message = "PgBolt dashboard is reachable only from local requests: add RequireAuthorization(...) to the dashboard group or set AllowAnonymousRemote.")]
        internal static partial void LocalRequestsOnly(ILogger logger);
    }
}
