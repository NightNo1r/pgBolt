using PgBolt.Postgres;

namespace PgBolt.Server;

/// <summary>Общие для процесса настройки обслуживания (§3.1, §6.3).</summary>
public sealed class PgBoltProcessingOptions
{
    /// <summary>
    /// Запускать в этом процессе promoter, sweeper и обслуживание хранилища (часть 2 §6.2): партиции, уборку servers, retention.
    /// Если ни один процесс, работающий со схемой, не выполняет обслуживание (у всех false и нет ручного вызова
    /// IPgBoltMaintenance), партиции кончатся через PartitionDaysAhead суток и enqueue начнёт падать; retention и уборка servers
    /// тоже не выполняются.
    /// Workflows (часть 5): продолжения и batches разрешает resolver, который работает только в процессах с AddPgBoltServer и
    /// RunMaintenance = true; если таких нет, сигналы копятся и продолжения не стартуют (§8).
    /// </summary>
    public bool RunMaintenance { get; set; } = true;

    /// <summary>Пауза promoter'а (±20 %).</summary>
    public TimeSpan PromoterInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Пауза sweeper'а (±20 %).</summary>
    public TimeSpan SweeperInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Пауза цикла обслуживания хранилища (±20 %): как часто под пытается захватить задачи maintenance_tasks.</summary>
    public TimeSpan MaintenancePollInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Интервал задачи partitions в кластере (EnsurePartitionsAsync). Интервал записывается при захвате, поэтому при разных
    /// настройках на подах действует интервал пода, захватившего задачу последним.
    /// </summary>
    public TimeSpan PartitionsInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Интервал задачи servers в кластере (уборка строк серверов без heartbeat).</summary>
    public TimeSpan ServersCleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Интервал задачи retention в кластере; после LockTimeout/HoldTimeout повтор — не раньше следующего интервала.</summary>
    public TimeSpan RetentionInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Строка servers без heartbeat дольше этого удаляется. Строку живого сервера (пауза дольше порога) восстанавливает его
    /// следующий heartbeat; lease от строки servers не зависят.
    /// </summary>
    public TimeSpan DeadServerTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>Запускать в этом процессе тикер recurring (часть 4 §5.1). Безопасно на всех подах: строки захватываются SKIP LOCKED.</summary>
    public bool RunScheduler { get; set; } = true;

    /// <summary>Пауза тикера (±20 %): задержка срабатывания ≈ интервал.</summary>
    public TimeSpan SchedulerInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Порог MisfirePolicy.Skip: срабатывание старше отбрасывается (часть 4 §5.3). null — max(1 мин, 5 × SchedulerInterval);
    /// явное значение — не меньше 5 × SchedulerInterval. Действует значение пода, захватившего строку.
    /// </summary>
    public TimeSpan? MisfireThreshold { get; set; }

    /// <summary>Интервал периодической синхронизации code-first расписаний (часть 4 §6.1).</summary>
    public TimeSpan RecurringSyncInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Пауза resolver'а workflow (±20 %, часть 5 §6.4); финализация в процессе с resolver'ом будит его сразу (in-process сигнал).
    /// Также задержка повтора для сигналов, чьих детей держат чужие транзакции (retry_after).
    /// </summary>
    public TimeSpan ResolverInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Сброс счётчиков исходов процесса (часть 6 §4.1); работает в любом процессе с AddPgBoltServer, независимо от RunMaintenance.
    /// </summary>
    public TimeSpan StatsFlushInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Интервал задачи stats в кластере (часть 6 §4.2: снимок queue_stats и retention stats_minutes). Как у остальных задач,
    /// действует значение пода, захватившего задачу последним; захват — на 0,8 × интервал.
    /// </summary>
    public TimeSpan StatsSnapshotInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Пауза (±10 %) между перечитываниями настроек очередей из хранилища: какие очереди лимитированы и какие у них веса.
    /// Определяет, как быстро процесс подхватывает изменения, сделанные другим процессом; запись через менеджер в этом же
    /// процессе обновляет настройки сразу.
    /// </summary>
    public TimeSpan QueueSettingsRefreshInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Хранение минутных счётчиков stats_minutes.</summary>
    public TimeSpan StatsRetention { get; set; } = TimeSpan.FromDays(7);

    internal TimeSpan EffectiveMisfireThreshold =>
        MisfireThreshold ?? TimeSpan.FromTicks(Math.Max(TimeSpan.FromMinutes(1).Ticks, (5 * SchedulerInterval).Ticks));

    internal void Validate()
    {
        Check(PromoterInterval, nameof(PromoterInterval));
        Check(SweeperInterval, nameof(SweeperInterval));
        Check(MaintenancePollInterval, nameof(MaintenancePollInterval));
        Check(PartitionsInterval, nameof(PartitionsInterval));
        Check(ServersCleanupInterval, nameof(ServersCleanupInterval));
        Check(RetentionInterval, nameof(RetentionInterval));
        Check(DeadServerTimeout, nameof(DeadServerTimeout));
        Check(SchedulerInterval, nameof(SchedulerInterval));
        Check(RecurringSyncInterval, nameof(RecurringSyncInterval));
        Check(ResolverInterval, nameof(ResolverInterval));
        Check(StatsFlushInterval, nameof(StatsFlushInterval));
        Check(StatsSnapshotInterval, nameof(StatsSnapshotInterval));
        Check(QueueSettingsRefreshInterval, nameof(QueueSettingsRefreshInterval));
        if (StatsRetention <= TimeSpan.Zero || StatsRetention > PgBoltOptions.MaxRetention)
            throw new InvalidOperationException("PgBolt processing: StatsRetention must be > 0 and <= 3650 days.");
        if (MisfireThreshold is { } threshold && threshold < 5 * SchedulerInterval)
            throw new InvalidOperationException(
                $"PgBolt processing: MisfireThreshold must be >= 5 × SchedulerInterval ({5 * SchedulerInterval}).");
    }

    private static void Check(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value > PgBoltServerOptions.MaxInterval)
            throw new InvalidOperationException($"PgBolt processing: {name} must be > 0 and <= {PgBoltServerOptions.MaxInterval}.");
    }
}
