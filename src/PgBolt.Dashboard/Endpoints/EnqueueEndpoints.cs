using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard.Components.Pages;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Forms;
using PgBolt.Dashboard.Views;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>
/// «Поставить» задачу по имени (§8): форма из схемы args или JSON, проверка подмножеством схемы, IJobClient.EnqueueAsync.
/// Ответы POST — фрагменты без &lt;title&gt; (отклонение 23); тип, исчезнувший из job_types, — 200 с сообщением (отклонение 15).
/// </summary>
internal static class EnqueueEndpoints
{
    public static void Map(RouteGroupBuilder g)
    {
        DashboardRoutes.Get(g, "/types/enqueue", async (string? name, string? mode, HttpContext http, DashboardStore store) =>
        {
            if (mode is not (null or "form" or "json"))
                throw new DashboardBadRequestException($"invalid mode: {mode}");
            var type = await FindAsync(store, name, http.RequestAborted) ?? throw NotFound(name);
            var fields = SchemaForm.TryBuild(type.ArgsSchemaJson);
            return await RenderAsync(http, "page", type, fields, mode == "json" || fields is null ? "{}" : null);
        });
        DashboardRoutes.Post(g, "/actions/types/json", async (string? name, IFormCollection form, HttpContext http, DashboardStore store) =>
        {
            var type = await FindAsync(store, name, http.RequestAborted) ?? throw NotFound(name);
            var fields = SchemaForm.TryBuild(type.ArgsSchemaJson);
            var json = form.TryGetValue("json", out var current) ? current.ToString()
                : fields is null ? "{}"
                : FormArgs.Read(fields, form, bestEffort: true).Args.ToJsonString(JobCard.Indented);
            return await RenderAsync(http, "body", type, fields, json);
        });
        DashboardRoutes.Post(g, "/actions/types/enqueue", async (string? name, IFormCollection form, HttpContext http, DashboardStore store,
            IJobClient client, ILoggerFactory loggers) =>
        {
            var ct = http.RequestAborted;
            var logger = loggers.CreateLogger(ActionEndpoints.LogCategory);
            var type = await FindAsync(store, name, ct);
            if (type is null)
            {
                var unknown = Unknown(name!);
                ActionEndpoints.Log(logger, http, "enqueue", $"type {name}", unknown);
                return await DashboardView.PageAsync<EnqueueResultView>(http, "", new Dictionary<string, object?>
                {
                    [nameof(EnqueueResultView.Message)] = unknown,
                }, partial: true);
            }
            var fields = SchemaForm.TryBuild(type.ArgsSchemaJson);
            string? json = fields is null || form.ContainsKey("json") ? form["json"].ToString() : null;
            var (args, errors) = ReadArgs(fields, form, json);
            var options = ReadOptions(form, errors);
            if (errors.Count > 0)
                return await RenderAsync(http, "result", type, fields, json, form, errors, status: 400);

            string message;
            Guid? jobId = null;
            try
            {
                var result = await client.EnqueueAsync(type.Name, args, options, ct);
                (message, jobId) = result switch
                {
                    { Created: true, Id: { } id } => ("задача поставлена", id.Value),
                    { Id: { } id } => ("уже есть активная задача", id.Value),
                    _ => ("задача с этим ключом только что завершилась", (Guid?)null),
                };
            }
            catch (UnknownJobException e)
            {
                message = Unknown(e.JobName);
            }
            catch (ArgumentException e)
            {
                // Опции проверены выше; остаётся отказ core по unique_key (зарезервированный префикс pgbolt:).
                errors.Add(new(options.UniqueKey is null ? "options" : "uniqueKey", e.Message));
                return await RenderAsync(http, "result", type, fields, json, form, errors, status: 400);
            }
            ActionEndpoints.Log(logger, http, "enqueue", $"type {type.Name}", jobId is { } job ? $"{message} {job}" : message);
            return await RenderAsync(http, "result", type, fields, json, form, errors, message, jobId);
        });
    }

    private static async Task<JobTypeRow?> FindAsync(DashboardStore store, string? name, CancellationToken ct) =>
        string.IsNullOrEmpty(name) ? throw new DashboardBadRequestException("name is required") : await store.GetJobTypeAsync(name, ct);

    private static DashboardNotFoundException NotFound(string? name) => new($"job type {name} not found");

    private static string Unknown(string name) => $"тип задачи {name} не опубликован";

    /// <summary>args из JSON (json не null) или из полей формы; ошибки — по имени поля формы (json, args.{Path}).</summary>
    private static (JsonElement Args, List<FieldError> Errors) ReadArgs(IReadOnlyList<FormField>? fields, IFormCollection form, string? json)
    {
        if (json is null)
        {
            var (obj, readErrors) = FormArgs.Read(fields!, form, bestEffort: false);
            var args = JsonSerializer.SerializeToElement(obj);
            // Ошибки схемы — и у полей, разобранных без ошибок: поле с ошибкой разбора в args не пишется, его повтор не нужен.
            var argErrors = readErrors.Concat(ArgsValidator.Validate(fields!, args).Where(e => readErrors.All(r => r.Path != e.Path)));
            return (args, argErrors.Select(e => e with { Path = "args." + e.Path }).ToList());
        }
        JsonElement parsed;
        try
        {
            parsed = JsonSerializer.Deserialize<JsonElement>(json);
        }
        catch (JsonException)
        {
            return (default, [new("json", "некорректный JSON")]);
        }
        var errors = fields is null ? [] : ArgsValidator.Validate(fields, parsed);
        return (parsed, errors.Select(e => new FieldError("json", e.Path.Length == 0 ? e.Message : $"{e.Path}: {e.Message}")).ToList());
    }

    /// <summary>Опции постановки; пустое поле — умолчание типа. Ошибки — по имени поля опции.</summary>
    private static EnqueueOptions ReadOptions(IFormCollection form, List<FieldError> errors)
    {
        string? Text(string key) => form[key].ToString() is { Length: > 0 } s ? s : null;

        T? Number<T>(string key, T min, string message) where T : struct, INumber<T>
        {
            if (Text(key) is not { } s)
                return null;
            if (T.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value >= min)
                return value;
            errors.Add(new(key, message));
            return null;
        }

        var priority = Number("priority", short.MinValue, "ожидается целое число от -32768 до 32767");
        var delay = Number("delay", 0, "ожидается целое число секунд ≥ 0");
        var maxAttempts = Number("maxAttempts", 1, "ожидается целое число ≥ 1");
        DateTimeOffset? runAt = null;
        if (Text("runAt") is { } r)
        {
            if (DateTimeOffset.TryParse(r, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var at))
                runAt = at;
            else
                errors.Add(new("runAt", "ожидается дата-время ISO 8601"));
        }
        if (delay is not null && runAt is not null)
            errors.Add(new("runAt", "задержка и время запуска взаимоисключимы"));
        return new EnqueueOptions
        {
            Queue = Text("queue"),
            Priority = priority,
            Delay = delay is { } seconds ? TimeSpan.FromSeconds(seconds) : null,
            RunAt = runAt,
            UniqueKey = Text("uniqueKey"),
            MaxAttempts = maxAttempts,
        };
    }

    private static Task<IResult> RenderAsync(HttpContext http, string part, JobTypeRow type, IReadOnlyList<FormField>? fields, string? json,
        IFormCollection? values = null, IReadOnlyList<FieldError>? errors = null, string? message = null, Guid? jobId = null, int status = 200) =>
        DashboardView.PageAsync<EnqueuePage>(http, $"Поставить {type.Name}", new Dictionary<string, object?>
        {
            [nameof(EnqueuePage.Type)] = type,
            [nameof(EnqueuePage.Fields)] = fields,
            [nameof(EnqueuePage.Json)] = json,
            [nameof(EnqueuePage.Values)] = values,
            [nameof(EnqueuePage.Errors)] = errors ?? [],
            [nameof(EnqueuePage.Message)] = message,
            [nameof(EnqueuePage.JobId)] = jobId,
            [nameof(EnqueuePage.Part)] = part,
            [nameof(EnqueuePage.Context)] = DashboardContext.Of(http),
        }, status, partial: part != "page");
}
