using System.Text.Json;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Sends an action notification in the HX-Trigger header (§7).</summary>
internal static class DashboardNotify
{
    /// <summary>Encodes {"pgbolt:notify": message}. The default encoder escapes non-ASCII as \uXXXX because Kestrel rejects non-ASCII header values.</summary>
    public static string Header(string message) =>
        JsonSerializer.Serialize(new Dictionary<string, string> { ["pgbolt:notify"] = message });
}
