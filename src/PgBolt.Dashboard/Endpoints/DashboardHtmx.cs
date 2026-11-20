namespace PgBolt.Dashboard.Endpoints;

internal static class DashboardHtmx
{
    /// <summary>
    /// JSON для &lt;meta name="htmx-config"&gt;: без eval (CSP), без кеша истории (args не попадают в localStorage), без
    /// встроенных стилей индикатора, без наследования атрибутов (его включает только &lt;body&gt; через hx-inherit),
    /// свап ответов 4xx / 5xx в цель запроса с событием ошибки.
    /// </summary>
    public const string Config =
        """{"allowEval":false,"historyCacheSize":0,"includeIndicatorStyles":false,"disableInheritance":true,"responseHandling":[{"code":"204","swap":false},{"code":"[23]..","swap":true},{"code":"[45]..","swap":true,"error":true},{"code":"...","swap":false}]}""";
}
