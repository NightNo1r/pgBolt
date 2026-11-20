using Microsoft.Extensions.DependencyInjection;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Конвейер фильтров (§3.6): порядок списка (глобальные уже впереди), первый — внешний. Экземпляры резолвятся из
/// scope задачи; типы зарегистрированы как scoped при регистрации (Task 16), поэтому здесь нет reflection-активации.
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
