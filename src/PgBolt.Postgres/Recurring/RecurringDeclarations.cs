using Microsoft.Extensions.Hosting;

namespace PgBolt.Postgres.Recurring;

/// <summary>An AddRecurring declaration resolves arguments and type name on first synchronization because [Job] metadata may arrive
/// later.</summary>
internal sealed record RecurringDeclaration(string Id, string Cron, RecurringJobOptions? Options, Func<(string Type, string ArgsJson)> Resolve);

/// <summary>Code-defined schedules declared by this process (Part 4 §4.3, §6.1).</summary>
internal sealed class RecurringDeclarations(PgBoltOptions options, IHostEnvironment? environment)
{
    /// <summary>AddRecurring or ClearRecurring() enables synchronization of the owner's set. Setting RecurringOwner alone does not.</summary>
    public bool HasSync => options.Recurring.Count > 0 || options.RecurringCleared;

    /// <summary>Uses RecurringOwner or a nonblank ApplicationName, matching PgBoltOptions validation for RecurringOwner.</summary>
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
