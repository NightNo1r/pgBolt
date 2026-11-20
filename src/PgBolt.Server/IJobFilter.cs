namespace PgBolt.Server;

/// <summary>Продолжение конвейера: следующий фильтр или вызов исполнителя (§3.6).</summary>
public delegate ValueTask JobExecutionDelegate();

/// <summary>
/// Фильтр выполнения (§3.6). Резолвится из DI scope задачи (регистрируется как scoped). Может выполнить код до и
/// после <c>next</c>, перехватить или заменить исключение, не вызывать <c>next</c> (задача считается успешной).
/// Исключение фильтра — исключение задачи (§5.2).
/// </summary>
public interface IJobFilter
{
    ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next);
}
