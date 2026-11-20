using System.Globalization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Dashboard.Components.Pages;
using PgBolt.Dashboard.Components.Parts;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres.Client;

namespace PgBolt.Dashboard.Endpoints;

/// <summary>
/// Действия над задачами, batch, расписаниями и настройками очередей через API core; доступ и CSRF проверяет фильтр группы.
/// Ответ — фрагмент строки / карточки после действия (без &lt;title&gt;) и уведомление в HX-Trigger; неприменимое действие —
/// тоже 200 с уведомлением.
/// </summary>
internal static class ActionEndpoints
{
    private const int MaxSelected = 100;
    internal const string LogCategory = "PgBolt.Dashboard.Actions";

    /// <summary>Итог действия; Job — карточка, если действие её уже прочитало (неприменимый trigger).</summary>
    private readonly record struct JobOutcome(bool Found, bool Done, string Message, JobInfo? Job = null);

    public static void Map(RouteGroupBuilder g)
    {
        DashboardRoutes.Post(g, "/actions/jobs/{id:guid}/{action}", async (Guid id, string action, string? view, string? kind,
            HttpContext http, IJobClient client, ILoggerFactory loggers) =>
        {
            CheckJobAction(action);
            var card = IsCard(view);
            // Вкладка, где нажата кнопка: строка после действия сохраняет её колонки; без kind — вкладка нового состояния.
            var tab = DashboardRoutes.OptionalEnum<JobState>(kind, "kind");
            var outcome = await RunJobAsync(client, action, id, http, loggers.CreateLogger(LogCategory));
            if (!outcome.Found)
                throw new DashboardNotFoundException($"job {id} not found");
            var job = outcome.Job ?? await JobEndpoints.GetJobAsync(client, id, http.RequestAborted);
            Notify(http, outcome.Message);
            var context = DashboardContext.Of(http);
            return card
                ? await DashboardView.PageAsync<JobCard>(http, "", new Dictionary<string, object?>
                {
                    [nameof(JobCard.Job)] = job,
                    [nameof(JobCard.Context)] = context,
                }, partial: true)
                : await DashboardView.PageAsync<JobRow>(http, "", new Dictionary<string, object?>
                {
                    [nameof(JobRow.Row)] = RowOf(job),
                    [nameof(JobRow.Kind)] = tab ?? job.State,
                    [nameof(JobRow.State)] = job.State,
                    [nameof(JobRow.Context)] = context,
                }, partial: true);
        });
        DashboardRoutes.Post(g, "/actions/jobs/{action}", async (string action, IFormCollection form, HttpContext http, IJobClient client,
            ILoggerFactory loggers) =>
        {
            CheckJobAction(action);
            var ids = form["ids"].Select(v => Guid.TryParse(v, out var id) ? id : throw new DashboardBadRequestException($"invalid job id: {v}"))
                .Distinct().ToList();
            if (ids.Count > MaxSelected)
                throw new DashboardBadRequestException($"at most {MaxSelected} jobs per request");
            var logger = loggers.CreateLogger(LogCategory);
            var done = 0;
            var notApplicable = new List<(Guid Id, string Message)>();
            foreach (var id in ids)
            {
                var outcome = await RunJobAsync(client, action, id, http, logger);
                if (outcome.Done)
                    done++;
                else
                    notApplicable.Add((id, outcome.Message));
            }
            return await DashboardView.PageAsync<ActionSummary>(http, "", new Dictionary<string, object?>
            {
                [nameof(ActionSummary.Done)] = done,
                [nameof(ActionSummary.NotApplicable)] = notApplicable,
            }, partial: true);
        });
        DashboardRoutes.Post(g, "/actions/batches/{id:guid}/cancel", async (Guid id, HttpContext http, IJobClient client,
            ILoggerFactory loggers) =>
        {
            var ct = http.RequestAborted;
            var logger = loggers.CreateLogger(LogCategory);
            DashboardNotFoundException NotFound()
            {
                Log(logger, http, "cancel", $"batch {id}", "batch не найден");
                return new($"batch {id} not found");
            }
            var before = await client.GetBatchAsync(new BatchId(id), ct) ?? throw NotFound();
            // Отклонение 24: повторная отмена тоже CancellationRequested — признак берётся до вызова.
            var message = await client.CancelBatchAsync(new BatchId(id), ct) switch
            {
                BatchCancelResult.NotFound => throw NotFound(),
                BatchCancelResult.AlreadyFinished => "batch уже завершён",
                _ => before.CancellationRequested ? "отмена уже запрошена" : "отмена запрошена",
            };
            Log(logger, http, "cancel", $"batch {id}", message);
            var batch = await BatchEndpoints.GetBatchAsync(client, id, ct);
            Notify(http, message);
            return await DashboardView.PageAsync<BatchPage>(http, "", new Dictionary<string, object?>
            {
                [nameof(BatchPage.Batch)] = batch,
                [nameof(BatchPage.Context)] = DashboardContext.Of(http),
            }, partial: true);
        });
        DashboardRoutes.Post(g, "/actions/recurring/{action}", async (string action, string? id, string? view, HttpContext http,
            IRecurringJobManager manager, DashboardStore store, ILoggerFactory loggers) =>
        {
            if (action is not ("pause" or "resume" or "trigger" or "delete"))
                throw new DashboardNotFoundException($"unknown action: {action}");
            if (string.IsNullOrEmpty(id))
                throw new DashboardBadRequestException("id is required");
            var card = IsCard(view);
            var ct = http.RequestAborted;
            var logger = loggers.CreateLogger(LogCategory);
            DashboardNotFoundException NotFound()
            {
                Log(logger, http, action, $"recurring {id}", "расписание не найдено");
                return new($"recurring job {id} not found");
            }
            var recurring = await manager.GetAsync(id, ct) ?? throw NotFound();
            var removed = false;
            string message;
            switch (action)
            {
                case "pause":
                    message = await manager.PauseAsync(id, ct) ? "пауза" : throw NotFound();
                    break;
                case "resume":
                    message = await manager.ResumeAsync(id, ct) ? "возобновлено" : throw NotFound();
                    break;
                case "trigger":
                    message = await TriggerRecurringAsync(manager, id, ct) ?? throw NotFound();
                    break;
                default:
                    if (recurring.Owner is { } owner)
                        message = $"расписание объявлено в коде (owner {owner}), удаление недоступно";
                    else if (await manager.RemoveAsync(id, ct))
                        (removed, message) = (true, "расписание удалено");
                    else
                        throw NotFound();
                    break;
            }
            Log(logger, http, action, $"recurring {id}", message);
            if (removed)
            {
                // Удалённого расписания GetAsync уже не вернёт: заглушка на месте строки / карточки.
                Notify(http, message);
                return await DashboardView.PageAsync<RecurringRemoved>(http, "", new Dictionary<string, object?>
                {
                    [nameof(RecurringRemoved.Card)] = card,
                }, partial: true);
            }
            // Действие уже записано в лог: исчезновение строки после него — просто 404.
            var after = await manager.GetAsync(id, ct) ?? throw new DashboardNotFoundException($"recurring job {id} not found");
            Notify(http, message);
            var context = DashboardContext.Of(http);
            return card
                ? await DashboardView.PageAsync<RecurringPage>(http, "", new Dictionary<string, object?>
                {
                    [nameof(RecurringPage.Recurring)] = after,
                    [nameof(RecurringPage.Runs)] = await store.ListRecurringRunsAsync(id, null, ct),
                    [nameof(RecurringPage.Context)] = context,
                }, partial: true)
                : await DashboardView.PageAsync<RecurringListRow>(http, "", new Dictionary<string, object?>
                {
                    [nameof(RecurringListRow.Row)] = new RecurringRow(after.Id, after.JobName, after.Cron, after.TimeZone, after.Owner,
                        after.Paused, after.NextFireAt?.UtcDateTime, after.LastFireAt?.UtcDateTime, after.LastJobId?.Value, after.LastError),
                    [nameof(RecurringListRow.Context)] = context,
                }, partial: true);
        });
        DashboardRoutes.Post(g, "/actions/queues/{setting}", async (string setting, string? queue, IFormCollection form,
            HttpContext http, IQueueSettingsManager settings, DashboardStore store, ILoggerFactory loggers) =>
        {
            if (setting is not ("limit" or "weight"))
                throw new DashboardNotFoundException($"unknown setting: {setting}");
            if (string.IsNullOrWhiteSpace(queue))
                throw new DashboardBadRequestException("queue is required");
            var op = form["op"].ToString();
            var value = op == "set" ? Value(form, setting == "limit" ? int.MaxValue : QueueSettingsManager.MaxWeight) : (int?)null;
            var ct = http.RequestAborted;
            var by = $"dashboard: {User(http)}";
            var (write, message) = (setting, op) switch
            {
                ("limit", "set") => (settings.SetConcurrencyOverrideAsync(queue, value, by, ct), $"лимит {value}"),
                ("limit", "unlimited") => (settings.SetConcurrencyOverrideAsync(queue, null, by, ct), "без лимита"),
                ("limit", "pause") => (settings.SetConcurrencyOverrideAsync(queue, 0, by, ct), "пауза"),
                ("limit", "reset") => (settings.ResetConcurrencyOverrideAsync(queue, ct), "лимит из кода"),
                ("weight", "set") => (settings.SetWeightOverrideAsync(queue, value, by, ct), $"вес {value}"),
                ("weight", "none") => (settings.SetWeightOverrideAsync(queue, null, by, ct), "без веса"),
                ("weight", "reset") => (settings.ResetWeightOverrideAsync(queue, ct), "вес из кода"),
                _ => throw new DashboardNotFoundException($"unknown op: {op}"),
            };
            await write;
            Log(loggers.CreateLogger(LogCategory), http, $"{setting} {op}", $"queue {queue}", message);
            Notify(http, message);
            // Запись менеджера уже сбросила его кэш: настройки строки читаются из БД. Очередь, выпавшая из списка, — пустой
            // фрагмент: htmx удаляет строку.
            var lines = await OverviewEndpoints.QueueLinesAsync(store, settings, queue, ct);
            return lines.Count == 0
                ? Results.Content("", "text/html; charset=utf-8")
                : await DashboardView.PageAsync<QueueRows>(http, "", new Dictionary<string, object?>
                {
                    [nameof(QueueRows.Queues)] = lines,
                    [nameof(QueueRows.Context)] = DashboardContext.Of(http),
                }, partial: true);
        });
        DashboardRoutes.Post(g, "/actions/queues/cache/reset", (HttpContext http, IQueueSettingsManager settings, ILoggerFactory loggers) =>
        {
            settings.InvalidateCache();
            http.RequestServices.GetService<IQueueSettingsRefresher>()?.RefreshNow();
            const string Message = "кэш настроек сброшен";
            Log(loggers.CreateLogger(LogCategory), http, "reset", "queue settings cache", Message);
            Notify(http, Message);
            return Results.Content("", "text/html; charset=utf-8");
        });
    }

    /// <summary>Целое из поля value формы в 1…max; иначе 400.</summary>
    private static int Value(IFormCollection form, int max) =>
        int.TryParse(form["value"], NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value >= 1 && value <= max
            ? value
            : throw new DashboardBadRequestException($"value must be an integer between 1 and {max}");

    /// <summary>Одно действие над задачей с записью в лог; Found = false — задачи нет.</summary>
    private static async Task<JobOutcome> RunJobAsync(IJobClient client, string action, Guid id, HttpContext http, ILogger logger)
    {
        var ct = http.RequestAborted;
        var job = new JobId(id);
        var notFound = new JobOutcome(false, false, "задача не найдена");
        JobOutcome outcome = action switch
        {
            "cancel" => await client.CancelAsync(job, $"dashboard: {User(http)}", ct) switch
            {
                CancelResult.NotFound => notFound,
                CancelResult.AlreadyFinished => new(true, false, "задача уже завершена"),
                CancelResult.Cancelled => new(true, true, "отменена"),
                _ => new(true, true, "отмена запрошена"),
            },
            "trigger" => await client.TriggerAsync(job, ct) ? new(true, true, "запущена")
                : await client.GetAsync(job, ct) is { } info ? new(true, false, "неприменимо: задача не Scheduled", info)
                : notFound,
            _ => await client.RequeueAsync(job, ct) switch
            {
                RequeueResult.NotFound => notFound,
                RequeueResult.NotTerminal => new(true, false, "неприменимо: задача не завершена"),
                RequeueResult.Conflict => new(true, false, "конфликт unique_key: есть активная задача с тем же ключом"),
                _ => new(true, true, "поставлена заново"),
            },
        };
        Log(logger, http, action, $"job {id}", outcome.Message);
        return outcome;
    }

    /// <summary>Сообщение о ручном запуске; null — расписания нет; тип, исчезнувший из job_types, — сообщение (отклонение 15).</summary>
    private static async Task<string?> TriggerRecurringAsync(IRecurringJobManager manager, string id, CancellationToken ct)
    {
        try
        {
            return await manager.TriggerAsync(id, ct) switch
            {
                null => null,
                { Created: true, Id: var job } => $"поставлена задача {job?.Value}",
                { Id: { } job } => $"не поставлена: есть активная задача {job.Value}",
                _ => "не поставлена",
            };
        }
        catch (UnknownJobException e)
        {
            return $"тип задачи {e.JobName} не опубликован";
        }
    }

    private static void CheckJobAction(string action)
    {
        if (action is not ("cancel" or "trigger" or "requeue"))
            throw new DashboardNotFoundException($"unknown action: {action}");
    }

    /// <summary>view: row (по умолчанию) или card.</summary>
    private static bool IsCard(string? view) => view switch
    {
        null or "row" => false,
        "card" => true,
        _ => throw new DashboardBadRequestException($"invalid view: {view}"),
    };

    /// <summary>Строка списка из карточки; событие и сообщение — последнего события (GetAsync отдаёт новые первыми).</summary>
    private static JobListRow RowOf(JobInfo job)
    {
        var last = job.Events.FirstOrDefault();
        return new JobListRow(job.Id.Value, job.Name, job.Queue, job.Priority, job.Attempt, job.MaxAttempts, job.CreatedAt.UtcDateTime,
            job.RunAt?.UtcDateTime, job.StartedAt?.UtcDateTime, job.ServerId, job.FinishedAt?.UtcDateTime, job.CancellationRequested,
            last?.Kind, last?.Message);
    }

    private static string User(HttpContext http) => http.User.Identity?.Name ?? "anonymous";

    private static void Notify(HttpContext http, string message) => http.Response.Headers["HX-Trigger"] = DashboardNotify.Header(message);

    internal static void Log(ILogger logger, HttpContext http, string action, string target, string result) =>
        DashboardActionLog.DashboardActionPerformed(logger, User(http), action, target, result);
}
