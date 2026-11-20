using System.Net;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Unicode;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard.Components;
using PgBolt.Dashboard.Components.Parts;

namespace PgBolt.Dashboard.Endpoints;

internal static class DashboardView
{
    /// <summary>htmx-запрос, кроме восстановления истории: ответ — фрагмент, а не страница целиком.</summary>
    public static bool WantsFragment(HttpRequest request) =>
        request.Headers["HX-Request"] == "true" && request.Headers["HX-History-Restore-Request"] != "true";

    /// <summary>Цель свапа — #main (навигация): только такой фрагмент несёт &lt;title&gt;.</summary>
    public static bool TargetsMain(HttpRequest request) => request.Headers["HX-Target"] == "main";

    /// <summary>Фрагмент (<paramref name="partial"/> — без &lt;title&gt;) для htmx или страница в оболочке <see cref="Shell"/>.</summary>
    public static async Task<IResult> PageAsync<TPage>(HttpContext http, string title, IReadOnlyDictionary<string, object?> parameters,
        int status = 200, bool partial = false) where TPage : IComponent
    {
        string html;
        if (WantsFragment(http.Request))
        {
            var body = await RenderAsync(http, typeof(TPage), parameters);
            html = partial ? body : $"<title>{WebUtility.HtmlEncode(title)} · PgBolt</title>{body}";
        }
        else
        {
            html = await RenderAsync(http, typeof(Shell), new Dictionary<string, object?>
            {
                [nameof(Shell.Title)] = title,
                [nameof(Shell.Context)] = DashboardContext.Of(http),
                [nameof(Shell.Body)] = typeof(TPage),
                [nameof(Shell.BodyParameters)] = parameters,
            });
        }
        http.Response.Headers.Vary = "HX-Request, HX-History-Restore-Request";
        http.Response.Headers.CacheControl = "no-store";
        return Results.Content(html, "text/html; charset=utf-8", Encoding.UTF8, status);
    }

    /// <summary>Единственный рендер ошибки: фрагмент с &lt;title&gt; только при цели #main.</summary>
    public static Task<IResult> ErrorAsync(HttpContext http, int status, string message) =>
        PageAsync<ErrorView>(http, "Ошибка", new Dictionary<string, object?>
        {
            [nameof(ErrorView.Status)] = status,
            [nameof(ErrorView.Message)] = message,
        }, status, partial: !TargetsMain(http.Request));

    public static async Task<string> RenderAsync(HttpContext http, Type component, IReadOnlyDictionary<string, object?> parameters)
    {
        await using var renderer = new HtmlRenderer(new RenderServices(http.RequestServices),
            http.RequestServices.GetRequiredService<ILoggerFactory>());
        // Вызывающие передают Dictionary — без копии; FromDictionary словарь не хранит.
        var view = ParameterView.FromDictionary(parameters as IDictionary<string, object?> ?? new Dictionary<string, object?>(parameters));
        return await renderer.Dispatcher.InvokeAsync(async () => (await renderer.RenderComponentAsync(component, view)).ToHtmlString());
    }

    /// <summary>
    /// HtmlRenderer берёт HtmlEncoder из сервисов, иначе HtmlEncoder.Default кодирует всё вне Basic Latin (кириллица → &amp;#x…;).
    /// Кодировщик с UnicodeRanges.All подменяется только для рендера dashboard: регистрация хоста не меняется.
    /// NavigationManager хоста (AddRazorPages / AddRazorComponents) скрыт: рендер вне Razor Components его не инициализирует, а
    /// с ним HtmlRenderer дописывает &lt;form&gt; без action адрес из NavigationManager.Uri и падает.
    /// </summary>
    private sealed class RenderServices(IServiceProvider inner) : IServiceProvider
    {
        private static readonly HtmlEncoder Encoder = HtmlEncoder.Create(UnicodeRanges.All);

        public object? GetService(Type serviceType) =>
            serviceType == typeof(HtmlEncoder) ? Encoder
            : serviceType == typeof(NavigationManager) ? null
            : inner.GetService(serviceType);
    }
}
