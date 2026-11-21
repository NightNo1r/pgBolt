namespace PgBolt.Dashboard.Endpoints;

internal static class DashboardHtmx
{
    /// <summary>
    /// JSON for &lt;meta name="htmx-config"&gt;. Disables eval for CSP, history caching to keep arguments out of
    /// localStorage, inline indicator styles and default attribute inheritance. Only &lt;body&gt; enables inheritance via
    /// hx-inherit. Swaps 4xx and 5xx responses into the target and raises an error event.
    /// </summary>
    public const string Config =
        """{"allowEval":false,"historyCacheSize":0,"includeIndicatorStyles":false,"disableInheritance":true,"responseHandling":[{"code":"204","swap":false},{"code":"[23]..","swap":true},{"code":"[45]..","swap":true,"error":true},{"code":"...","swap":false}]}""";
}
