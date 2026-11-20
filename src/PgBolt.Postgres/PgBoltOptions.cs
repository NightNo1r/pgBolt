using System.Text.Json;
using Npgsql;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres;

public sealed class PgBoltOptions
{
    private readonly List<Action<IPgBoltJobsBuilder>> _jobRegistrations = [];
    private readonly List<RecurringDeclaration> _recurring = [];

    public NpgsqlDataSource? DataSource { get; set; }
    public string? ConnectionString { get; set; }
    public string Schema { get; set; } = "pgbolt";
    public bool AutoMigrate { get; set; } = true;
    public bool UsePreparedStatements { get; set; }
    /// <summary>fillfactor партиций jobs (контракт HOT, §4.2).</summary>
    public int JobsFillFactor { get; set; } = 80;
    public int PartitionDaysAhead { get; set; } = 7;
    public TimeSpan PartitionLockTimeout { get; set; } = TimeSpan.FromSeconds(2);

    internal static readonly TimeSpan MaxRetention = TimeSpan.FromDays(3650);

    /// <summary>
    /// Срок хранения завершённых задач (Succeeded, Failed, Cancelled) — от суток их создания (UTC), на уровне суточных партиций
    /// (часть 2 §5.1–§5.3); округляется вверх до суток. Незавершённые задачи не удаляются никогда. Задача, созданная в сутки D
    /// и завершившаяся в D + k, хранится после завершения Rd − k + 1 суток (при k &gt; Rd — до ближайшего запуска retention).
    /// Первый запуск retention после обновления удаляет все партиции старше срока. Партиция, закреплённая живыми задачами,
    /// после компактирования занимает объём живых строк, а при их числе больше <see cref="RetentionCompactionMaxLiveJobs"/> —
    /// диск целиком до их ухода. null — хранить вечно (задаётся только кодом: отсутствие ключа в конфигурации — значение
    /// по умолчанию); число партиций тогда растёт на одну в сутки. Значение должно совпадать на всех подах и приложениях
    /// схемы: действует самый короткий срок среди процессов, выполняющих обслуживание.
    /// Таблицы с именами партиций (jobs_YYYYMMDD, jobs_YYYYMMDD_rN, job_events_YYYYMMDD) в схеме создаёт только PgBolt:
    /// отсоединённая таблица с таким именем удаляется при любом сроке, включая null. Для архивирования партицию нужно
    /// переименовать или перенести в другую схему (часть 2 §5.2).
    /// </summary>
    public TimeSpan? JobsRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Срок хранения job_events — от суток события (UTC), на уровне суточных партиций; округляется вверх до суток. null — хранить
    /// вечно (только кодом). Первый запуск retention после обновления удаляет все партиции старше срока. Должен совпадать на всех
    /// подах схемы. Архивирование партиций — см. <see cref="JobsRetention"/>: переименовать или перенести в другую схему.
    /// </summary>
    public TimeSpan? EventsRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>lock_timeout попыток retention взять блокировку родителя или партиции (часть 2 §5.3): на столько горячий путь
    /// может встать в очередь за одной попыткой.</summary>
    public TimeSpan RetentionLockTimeout { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Наибольшее число живых задач, при котором закреплённая партиция jobs компактируется (часть 2 §5.3); 0 — компактирование
    /// выключено. Компактирование, как TRUNCATE, не MVCC-безопасно для снимков REPEATABLE READ/SERIALIZABLE, взятых до него:
    /// если отчёты или выгрузки читают jobs в REPEATABLE READ, выставьте 0. При публикации jobs в логической репликации
    /// компактирование пропускается. Должно совпадать на всех подах схемы.
    /// </summary>
    public int RetentionCompactionMaxLiveJobs { get; set; } = 1000;
    public TimeSpan JobTypesCacheTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// TTL кэша чтения <see cref="IQueueSettingsManager"/> в этом процессе; null — без кэша (каждое чтение идёт в БД). Кэш
    /// сбрасывают запись менеджера этого процесса и <see cref="IQueueSettingsManager.InvalidateCache"/>; изменения из других
    /// процессов видны по истечении TTL. Dequeue и серверы этот кэш не используют. Если задан — больше 0 и не больше суток.
    /// </summary>
    public TimeSpan? QueueSettingsCacheDuration { get; set; }

    internal IReadOnlyList<Action<IPgBoltJobsBuilder>> JobRegistrations => _jobRegistrations;

    /// <summary>Регистрирует исполнителей сборки: o.AddJobs(My.Jobs.PgBoltJobRegistry.Register).</summary>
    public PgBoltOptions AddJobs(Action<IPgBoltJobsBuilder> register)
    {
        ArgumentNullException.ThrowIfNull(register);
        _jobRegistrations.Add(register);
        return this;
    }

    /// <summary>
    /// Владелец code-first расписаний (часть 4 §4.3): все процессы одного владельца объявляют одинаковый набор. По умолчанию —
    /// IHostEnvironment.ApplicationName. Только имя: синхронизацию включают AddRecurring или ClearRecurring().
    /// </summary>
    /// <remarks>
    /// <para>
    /// Синхронизация (§6) приводит строки владельца к объявленному набору: на старте и, если в процессе есть сервер
    /// (AddPgBoltServer), периодически — раз в PgBoltProcessingOptions.RecurringSyncInterval. В процессе без сервера
    /// периодической синхронизации нет, только на старте; защиты от отката набора старым подом тогда тоже нет.
    /// </para>
    /// <para>
    /// Rolling deploy: пока живы старые поды, старый и новый наборы чередуются, затем сходятся к новому. Удалённая и снова
    /// вставленная строка теряет курсор и paused: рядом со сменой версии срабатывание может выпасть или повториться, а пауза
    /// расписания, удаляемого новой версией, — сброситься при возврате его старым подом.
    /// </para>
    /// <para>
    /// Выведенный из эксплуатации владелец: его строки срабатывают дальше (или раз в минуту пишут ошибку строки), а
    /// IRecurringJobManager их не удаляет. Снять их — последним деплоем сервиса с ClearRecurring() или SQL
    /// <c>DELETE FROM {schema}.recurring_jobs WHERE owner = '&lt;owner&gt;';</c> (если процесс владельца всё же жив,
    /// следующая синхронизация вставит строки заново).
    /// </para>
    /// </remarks>
    public string? RecurringOwner { get; set; }

    internal IReadOnlyList<RecurringDeclaration> Recurring => _recurring;
    internal bool RecurringCleared { get; private set; }

    /// <summary>Code-first расписание; синхронизируется на старте и периодически (часть 4 §6). Проверки — сразу, кроме аргументов.</summary>
    /// <remarks>
    /// Метаданные TArgs генератор регистрирует по месту этого вызова; аргументы сериализуются при первой синхронизации.
    /// Удаление последнего AddRecurring без ClearRecurring() строки владельца не удаляет: процесс без объявлений не
    /// синхронизирует. Жизненный цикл набора (rolling deploy, процесс без сервера, вывод владельца) — <see cref="RecurringOwner"/>.
    /// </remarks>
    public PgBoltOptions AddRecurring<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null)
    {
        RecurringDefinition.Validate(id, cron, options);
        _recurring.Add(new RecurringDeclaration(id, cron, options, () =>
        {
            var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
            return (descriptor.Name, JsonSerializer.Serialize(args, typeInfo));
        }));
        return this;
    }

    /// <summary>Динамическое code-first расписание по имени задачи; тип по job_types не проверяется (часть 4 §4.3).</summary>
    /// <remarks>Удаление последнего AddRecurring и жизненный цикл набора — как у <see cref="AddRecurring{TArgs}"/>.</remarks>
    public PgBoltOptions AddRecurring(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobName);
        RecurringDefinition.Validate(id, cron, options);
        var json = args.GetRawText();
        _recurring.Add(new RecurringDeclaration(id, cron, options, () => (jobName, json)));
        return this;
    }

    /// <summary>Явно пустой набор: строки владельца удаляются. Несовместим с AddRecurring (часть 4 §4.3).</summary>
    /// <remarks>
    /// Так убирают последнее AddRecurring и выводят владельца из эксплуатации (<see cref="RecurringOwner"/>). Когда выкатка
    /// с ClearRecurring() завершена и строк владельца нет, вызов можно убрать следующей версией.
    /// </remarks>
    public PgBoltOptions ClearRecurring()
    {
        RecurringCleared = true;
        return this;
    }

    internal void Validate()
    {
        if ((DataSource is null) == string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException("PgBolt: set exactly one of DataSource or ConnectionString.");
        SchemaName.Validate(Schema);
        if (JobsFillFactor is < 10 or > 100)
            throw new InvalidOperationException("PgBolt: JobsFillFactor must be in [10, 100].");
        if (PartitionDaysAhead < 1)
            throw new InvalidOperationException("PgBolt: PartitionDaysAhead must be >= 1.");
        if (PartitionLockTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException("PgBolt: PartitionLockTimeout must be positive.");
        ValidateRetention(JobsRetention, nameof(JobsRetention));
        ValidateRetention(EventsRetention, nameof(EventsRetention));
        if (RetentionLockTimeout <= TimeSpan.Zero || RetentionLockTimeout > TimeSpan.FromMinutes(1))
            throw new InvalidOperationException("PgBolt: RetentionLockTimeout must be in (0, 1 min].");
        if (RetentionCompactionMaxLiveJobs is < 0 or > 10_000)
            throw new InvalidOperationException("PgBolt: RetentionCompactionMaxLiveJobs must be in [0, 10000].");
        if (JobTypesCacheTtl <= TimeSpan.Zero)
            throw new InvalidOperationException("PgBolt: JobTypesCacheTtl must be positive.");
        if (QueueSettingsCacheDuration is { } cache && (cache <= TimeSpan.Zero || cache > TimeSpan.FromDays(1)))
            throw new InvalidOperationException("PgBolt: QueueSettingsCacheDuration must be null or in (0, 1 day].");
        if (RecurringCleared && _recurring.Count > 0)
            throw new InvalidOperationException("PgBolt: ClearRecurring() cannot be combined with AddRecurring.");
        if (_recurring.GroupBy(r => r.Id, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1) is { } duplicate)
            throw new InvalidOperationException($"PgBolt: recurring job '{duplicate.Key}' is declared more than once.");
        if (RecurringOwner is { } owner && (string.IsNullOrWhiteSpace(owner) || owner.Length > RecurringDefinition.MaxIdLength))
            throw new InvalidOperationException(
                $"PgBolt: RecurringOwner must be non-blank and at most {RecurringDefinition.MaxIdLength} characters.");
    }

    private static void ValidateRetention(TimeSpan? retention, string name)
    {
        if (retention is { } r && (r <= TimeSpan.Zero || r > MaxRetention))
            throw new InvalidOperationException($"PgBolt: {name} must be null or in (0, 3650 days].");
    }
}
