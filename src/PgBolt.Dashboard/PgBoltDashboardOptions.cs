namespace PgBolt.Dashboard;

/// <summary>Dashboard options configured through <c>services.AddPgBoltDashboard(o => …)</c>.</summary>
public sealed class PgBoltDashboardOptions
{
    private static readonly TimeSpan MaxInterval = TimeSpan.FromDays(1);

    /// <summary>Authorization policy for actions and enqueueing. With null, anyone who can view the dashboard can perform actions.</summary>
    public string? ActionsPolicy { get; set; }

    /// <summary>Hides actions and rejects POST requests with 403.</summary>
    public bool ReadOnly { get; set; }

    /// <summary>
    /// Allows remote access without authorization on the dashboard route group.
    /// </summary>
    /// <remarks>By default, a group without authorization accepts only loopback <c>RemoteIpAddress</c> requests with no
    /// <c>Forwarded</c>, <c>X-Forwarded-For</c> or <c>X-Real-IP</c> headers. A service mesh sidecar such as Envoy or Istio
    /// can appear local (127.0.0.6 or 127.0.0.1) when it omits these headers. Configure group authorization with
    /// <c>RequireAuthorization</c> in a mesh.</remarks>
    public bool AllowAnonymousRemote { get; set; }

    /// <summary>Sets <c>SET LOCAL statement_timeout</c> for each dashboard statement.</summary>
    public TimeSpan QueryTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Time budget for chunked scans, including the Succeeded, Failed and Cancelled lists.</summary>
    public TimeSpan ScanBudget { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Poll interval for the hourly overview and queue counts.</summary>
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Marks a server unresponsive when its heartbeat is older than this threshold.</summary>
    public TimeSpan ServerStaleAfter { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Shows the stale statistics banner when the <c>stats</c> task is past <c>next_run_at</c> by this threshold plus the interval allowance.</summary>
    public TimeSpan StatsStaleAfter { get; set; } = TimeSpan.FromMinutes(1);

    internal void Validate()
    {
        if (QueryTimeout < TimeSpan.FromMilliseconds(1) || QueryTimeout > MaxInterval)
            throw new InvalidOperationException("PgBolt dashboard: QueryTimeout must be in [1 ms, 1 day].");
        if (RefreshInterval < TimeSpan.FromSeconds(1) || RefreshInterval > MaxInterval)
            throw new InvalidOperationException("PgBolt dashboard: RefreshInterval must be in [1 s, 1 day].");
        ValidateInterval(ScanBudget, nameof(ScanBudget));
        ValidateInterval(ServerStaleAfter, nameof(ServerStaleAfter));
        ValidateInterval(StatsStaleAfter, nameof(StatsStaleAfter));
        if (ActionsPolicy is not null && string.IsNullOrWhiteSpace(ActionsPolicy))
            throw new InvalidOperationException("PgBolt dashboard: ActionsPolicy must be null or a non-blank policy name.");
    }

    private static void ValidateInterval(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value > MaxInterval)
            throw new InvalidOperationException($"PgBolt dashboard: {name} must be in (0, 1 day].");
    }
}
