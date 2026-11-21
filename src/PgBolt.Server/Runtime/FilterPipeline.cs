using Microsoft.Extensions.DependencyInjection;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Runs filters in list order (§3.6), with global filters first and the first filter outermost. The job scope resolves
/// instances from their scoped registrations (Task 16), avoiding reflection-based activation here.
/// </summary>
internal sealed class FilterPipeline(IReadOnlyList<Type> filterTypes)
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate terminal) =>
        filterTypes.Count == 0 ? terminal() : InvokeAt(0, context, terminal);

    private ValueTask InvokeAt(int index, JobExecutingContext context, JobExecutionDelegate terminal)
    {
        if (index == filterTypes.Count)
            return terminal();
        var filter = (IJobFilter)context.Services.GetRequiredService(filterTypes[index]);
        return filter.InvokeAsync(context, () => InvokeAt(index + 1, context, terminal));
    }
}
