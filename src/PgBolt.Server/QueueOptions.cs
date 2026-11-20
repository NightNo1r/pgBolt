using PgBolt.Postgres.Client;

namespace PgBolt.Server;

/// <summary>Очередь виртуального сервера (§3.3). Создаётся только через <see cref="PgBoltServerOptions.Queue"/>.</summary>
public sealed class QueueOptions
{
    internal QueueOptions(string name) => Name = name;

    public string Name { get; }

    /// <summary>Слоты, доступные только этой очереди.</summary>
    public int Reserved { get; set; }

    /// <summary>Потолок одновременных задач очереди на этом сервере (reserved + общие); null — нет.</summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>
    /// Значение по умолчанию кластерного лимита очереди: сколько задач очереди одновременно держат lease во всём кластере; null — нет.
    /// Публикуется в хранилище при старте процесса, переопределение через IQueueSettingsManager имеет приоритет.
    /// Если лимит появился после старта процесса (переопределение или значение другого процесса), этот процесс подхватит его
    /// не позже чем через PgBoltProcessingOptions.QueueSettingsRefreshInterval; до тех пор его dequeue лимит не соблюдает.
    /// </summary>
    public int? ClusterMaxConcurrency { get; set; }

    /// <summary>
    /// Значение по умолчанию веса очереди (1…1000) — настройка очереди на весь кластер: каждый сервер делит по весам свой общий
    /// пул между очередями с весом; null — без веса. Переопределение через IQueueSettingsManager имеет приоритет.
    /// </summary>
    public int? Weight { get; set; }

    /// <summary>Нижняя граница адаптивного polling.</summary>
    public TimeSpan MinPollInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Верхняя граница адаптивного polling.</summary>
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(10);

    internal void Validate(string serverName)
    {
        var field = $"Queue('{Name}')";
        if (Reserved < 0)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(Reserved)}", "must be >= 0");
        if (MaxConcurrency is int max && max < Math.Max(1, Reserved))
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxConcurrency)}", "must be null or >= max(1, Reserved)");
        if (ClusterMaxConcurrency is < 1)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(ClusterMaxConcurrency)}", "must be null or >= 1");
        if (Weight is < 1 or > QueueSettingsManager.MaxWeight)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(Weight)}", $"must be null or in [1, {QueueSettingsManager.MaxWeight}]");
        if (MinPollInterval <= TimeSpan.Zero)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MinPollInterval)}", "must be > 0");
        if (MaxPollInterval < MinPollInterval)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxPollInterval)}", "must be >= MinPollInterval");
        if (MaxPollInterval > PgBoltServerOptions.MaxInterval)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxPollInterval)}", $"must be <= {PgBoltServerOptions.MaxInterval}");
    }
}
