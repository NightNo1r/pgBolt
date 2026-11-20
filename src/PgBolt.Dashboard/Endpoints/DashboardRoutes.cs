using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>
/// Метаданные каждого endpoint'а dashboard: маркер и число сегментов локального шаблона (базовый путь); Asset — статика пакета:
/// доступ проверяется, политика действий не вычисляется.
/// </summary>
internal sealed record DashboardEndpoint(int LocalSegments, bool Asset = false);

internal static class DashboardRoutes
{
    public static RouteHandlerBuilder Get(RouteGroupBuilder g, string pattern, Delegate handler, bool asset = false) =>
        g.MapGet(pattern, handler).WithMetadata(new DashboardEndpoint(Segments(pattern), asset));

    public static RouteHandlerBuilder Post(RouteGroupBuilder g, string pattern, Delegate handler) =>
        g.MapPost(pattern, handler).WithMetadata(new DashboardEndpoint(Segments(pattern)));

    /// <summary>Необязательный параметр-перечисление: пусто — null, неизвестное значение — 400.</summary>
    public static T? OptionalEnum<T>(string? value, string what) where T : struct, Enum =>
        string.IsNullOrEmpty(value) ? null : ParseEnum<T>(value) ?? throw new DashboardBadRequestException($"invalid {what}: {value}");

    /// <summary>Член перечисления по имени без учёта регистра; неизвестное имя или число вне членов — null.</summary>
    public static T? ParseEnum<T>(string value) where T : struct, Enum =>
        Enum.TryParse<T>(value, ignoreCase: true, out var parsed) && Enum.IsDefined(parsed) ? parsed : null;

    private static int Segments(string pattern) => pattern.Split('/', StringSplitOptions.RemoveEmptyEntries).Length;
}
