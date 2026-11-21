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
/// Executes job, batch, schedule and queue actions through the core API. The route group filter checks access and
/// CSRF. Returns an updated row or card without &lt;title&gt; and a notification in HX-Trigger. An inapplicable action
/// also returns 200 with a notification.
/// </summary>
internal static class ActionEndpoints
{
    private const int MaxSelected = 100;
    internal const string LogCategory = "PgBolt.Dashboard.Actions";

    /// <summary>Action result. Job holds the card if the action already read it, as when a job cannot be triggered.</summary>
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
                Log(logger, http, "cancel", $"batch {id}", "batch not found");
                return new($"batch {id} not found");
            }
            var before = await client.GetBatchAsync(new BatchId(id), ct) ?? throw NotFound();
            // Отклонение 24: повторная отмена тоже CancellationRequested — признак берётся до вызова.
            var message = await client.CancelBatchAsync(new BatchId(id), ct) switch
            {
                BatchCancelResult.NotFound => throw NotFound(),
                BatchCancelResult.AlreadyFinished => "batch already finished",
                _ => before.CancellationRequested ? "cancellation already requested" : "cancellation requested",
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
                Log(logger, http, action, $"recurring {id}", "schedule not found");
                return new($"recurring job {id} not found");
            }
            var recurring = await manager.GetAsync(id, ct) ?? throw NotFound();
            var removed = false;
            string message;
            switch (action)
            {
                case "pause":
                    message = await manager.PauseAsync(id, ct) ? "paused" : throw NotFound();
                    break;
                case "resume":
                    message = await manager.ResumeAsync(id, ct) ? "resumed" : throw NotFound();
                    break;
                case "trigger":
                    message = await TriggerRecurringAsync(manager, id, ct) ?? throw NotFound();
                    break;
                default:
                    if (recurring.Owner is { } owner)
                        message = $"schedule defined in code (owner {owner}), cannot delete";
                    else if (await manager.RemoveAsync(id, ct))
                        (removed, message) = (true, "schedule deleted");
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
                ("limit", "set") => (settings.SetConcurrencyOverrideAsync(queue, value, by, ct), $"limit {value}"),
                ("limit", "unlimited") => (settings.SetConcurrencyOverrideAsync(queue, null, by, ct), "unlimited"),
                ("limit", "pause") => (settings.SetConcurrencyOverrideAsync(queue, 0, by, ct), "paused"),
                ("limit", "reset") => (settings.ResetConcurrencyOverrideAsync(queue, ct), "configured limit"),
                ("weight", "set") => (settings.SetWeightOverrideAsync(queue, value, by, ct), $"weight {value}"),
                ("weight", "none") => (settings.SetWeightOverrideAsync(queue, null, by, ct), "unweighted"),
                ("weight", "reset") => (settings.ResetWeightOverrideAsync(queue, ct), "configured weight"),
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
            const string Message = "settings cache reset";
            Log(loggers.CreateLogger(LogCategory), http, "reset", "queue settings cache", Message);
            Notify(http, Message);
            return Results.Content("", "text/html; charset=utf-8");
        });
    }

    /// <summary>Reads an integer in the range 1 through max from the value form field. Invalid input returns 400.</summary>
    private static int Value(IFormCollection form, int max) =>
        int.TryParse(form["value"], NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value >= 1 && value <= max
            ? value
            : throw new DashboardBadRequestException($"value must be an integer between 1 and {max}");

    /// <summary>Runs and logs one job action. Found is false if the job does not exist.</summary>
    private static async Task<JobOutcome> RunJobAsync(IJobClient client, string action, Guid id, HttpContext http, ILogger logger)
    {
        var ct = http.RequestAborted;
        var job = new JobId(id);
        var notFound = new JobOutcome(false, false, "job not found");
        JobOutcome outcome = action switch
        {
            "cancel" => await client.CancelAsync(job, $"dashboard: {User(http)}", ct) switch
            {
                CancelResult.NotFound => notFound,
                CancelResult.AlreadyFinished => new(true, false, "job already finished"),
                CancelResult.Cancelled => new(true, true, "cancelled"),
                _ => new(true, true, "cancellation requested"),
            },
            "trigger" => await client.TriggerAsync(job, ct) ? new(true, true, "triggered")
                : await client.GetAsync(job, ct) is { } info ? new(true, false, "not applicable: job is not Scheduled", info)
                : notFound,
            _ => await client.RequeueAsync(job, ct) switch
            {
                RequeueResult.NotFound => notFound,
                RequeueResult.NotTerminal => new(true, false, "not applicable: job has not finished"),
                RequeueResult.Conflict => new(true, false, "unique_key conflict: an active job already uses this key"),
                _ => new(true, true, "requeued"),
            },
        };
        Log(logger, http, action, $"job {id}", outcome.Message);
        return outcome;
    }

    /// <summary>Returns the manual trigger message, or null if the schedule is missing. Reports a type removed from job_types as a message (deviation 15).</summary>
    private static async Task<string?> TriggerRecurringAsync(IRecurringJobManager manager, string id, CancellationToken ct)
    {
        try
        {
            return await manager.TriggerAsync(id, ct) switch
            {
                null => null,
                { Created: true, Id: var job } => $"enqueued job {job?.Value}",
                { Id: { } job } => $"not enqueued: active job {job.Value}",
                _ => "not enqueued",
            };
        }
        catch (UnknownJobException e)
        {
            return $"job type {e.JobName} is not published";
        }
    }

    private static void CheckJobAction(string action)
    {
        if (action is not ("cancel" or "trigger" or "requeue"))
            throw new DashboardNotFoundException($"unknown action: {action}");
    }

    /// <summary>Accepts row (default) or card as the view.</summary>
    private static bool IsCard(string? view) => view switch
    {
        null or "row" => false,
        "card" => true,
        _ => throw new DashboardBadRequestException($"invalid view: {view}"),
    };

    /// <summary>Builds a list row from the card using its latest event and message. GetAsync returns newest events first.</summary>
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
