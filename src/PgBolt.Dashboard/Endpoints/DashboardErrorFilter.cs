using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Maps dashboard endpoint exceptions to an error page or fragment with the appropriate status (§10).</summary>
internal sealed partial class DashboardErrorFilter(ILogger<DashboardErrorFilter> logger) : IEndpointFilter
{
    private const string Unavailable = "database unavailable";

    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var http = context.HttpContext;
        try
        {
            return await next(context);
        }
        catch (Exception e) when (!(e is OperationCanceledException && http.RequestAborted.IsCancellationRequested))
        {
            var (status, message) = Classify(e);
            if (status >= 500)
                Log.DashboardRequestFailed(logger, e, http.Request.Method, http.Request.Path);
            return await DashboardView.ErrorAsync(http, status, message);
        }
    }

    private static (int Status, string Message) Classify(Exception e) => e switch
    {
        DashboardNotFoundException => (404, e.Message),
        DashboardBadRequestException or BadHttpRequestException => (400, e.Message),
        PostgresException { SqlState: "57014" } => (503, "Query exceeded QueryTimeout. Narrow the filter."),
        PostgresException { SqlState: var state } when state.StartsWith("08", StringComparison.Ordinal)
            || state.StartsWith("57P0", StringComparison.Ordinal) || state == "53300" => (503, Unavailable),
        PostgresException => (500, Internal(e)),
        NpgsqlException => (503, Unavailable),
        _ => (500, Internal(e)),
    };

    private static string Internal(Exception e) => $"internal error: {e.GetType().Name}";

    private static partial class Log
    {
        [LoggerMessage(EventId = 7101, EventName = nameof(DashboardRequestFailed), Level = LogLevel.Error,
            Message = "PgBolt dashboard: {Method} {Path} failed.")]
        internal static partial void DashboardRequestFailed(ILogger logger, Exception e, string method, PathString path);
    }
}
