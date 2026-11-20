using System.Text.Json;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>Уведомление действия в заголовке HX-Trigger (§7).</summary>
internal static class DashboardNotify
{
    /// <summary>JSON {"pgbolt:notify": message}; кодировщик по умолчанию пишет не-ASCII как \uXXXX — Kestrel отвергает не-ASCII в заголовках.</summary>
    public static string Header(string message) =>
        JsonSerializer.Serialize(new Dictionary<string, string> { ["pgbolt:notify"] = message });
}
