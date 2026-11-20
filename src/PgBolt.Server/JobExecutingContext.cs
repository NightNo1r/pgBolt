namespace PgBolt.Server;

/// <summary>Контекст выполнения задачи для фильтров (§3.6).</summary>
public sealed class JobExecutingContext
{
    internal JobExecutingContext(JobContext job, IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, CancellationToken cancellationToken)
    {
        Job = job;
        Services = services;
        ArgsUtf8 = argsUtf8;
        CancellationToken = cancellationToken;
    }

    public JobContext Job { get; }

    /// <summary>Провайдер DI scope задачи.</summary>
    public IServiceProvider Services { get; }

    /// <summary>Аргументы задачи в UTF-8 (тот же буфер получает invoker).</summary>
    public ReadOnlyMemory<byte> ArgsUtf8 { get; }

    /// <summary>Токен выполнения: shutdown, отмена, таймаут, потеря lease.</summary>
    public CancellationToken CancellationToken { get; }
}
