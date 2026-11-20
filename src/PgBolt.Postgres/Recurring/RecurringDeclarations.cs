using Microsoft.Extensions.Hosting;

namespace PgBolt.Postgres.Recurring;

/// <summary>Объявление AddRecurring: аргументы и имя типа разрешаются при первой синхронизации (метаданные [Job] могут появиться позже).</summary>
internal sealed record RecurringDeclaration(string Id, string Cron, RecurringJobOptions? Options, Func<(string Type, string ArgsJson)> Resolve);

/// <summary>Code-first расписания процесса (часть 4 §4.3, §6.1).</summary>
internal sealed class RecurringDeclarations(PgBoltOptions options, IHostEnvironment? environment)
{
    /// <summary>Процесс синхронизирует набор владельца: есть AddRecurring или ClearRecurring(). RecurringOwner участие не включает.</summary>
    public bool HasSync => options.Recurring.Count > 0 || options.RecurringCleared;

    /// <summary>RecurringOwner или непустой (не из одних пробелов) ApplicationName — как проверка RecurringOwner в PgBoltOptions.</summary>
    public string Owner => field ??= options.RecurringOwner
        ?? (environment?.ApplicationName is { } name && !string.IsNullOrWhiteSpace(name)
            ? name
            : throw new InvalidOperationException(
                "PgBolt: recurring jobs are declared but RecurringOwner is not set and IHostEnvironment.ApplicationName is unavailable or blank; set PgBoltOptions.RecurringOwner."));

    public IReadOnlyList<RecurringDefinition> Definitions => field ??= options.Recurring
        .Select(d =>
        {
            var (type, json) = d.Resolve();
            return RecurringDefinition.Create(d.Id, type, json, d.Cron, d.Options);
        })
        .ToList();
}
