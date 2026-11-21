namespace PgBolt.Dashboard.Endpoints;

/// <summary>Invalid cursor or query parameter (400).</summary>
internal sealed class DashboardBadRequestException(string message) : Exception(message);

/// <summary>Missing job, batch or schedule (404).</summary>
internal sealed class DashboardNotFoundException(string message) : Exception(message);
