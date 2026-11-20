# Changelog

## Unreleased

### Breaking changes

- Observability (часть 7a): в начальную миграцию `V001` добавлена колонка `queue_stats.oldest_ready_seconds`; схемы прошлых сборок
  не обновляются — старт падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- Публичный `PgBolt.Postgres.Storage.ActiveQueueSetting`: поле `bool Limited` заменено на `int? Limit` (эффективный лимит очереди,
  пауза — 0); прежнее значение `Limited` — это `Limit is not null`.
- Кластерный лимит и веса очередей (часть 3b): в начальную миграцию `V001` добавлена таблица `queue_settings`, индекс
  `queue_owner (lease_owner)` заменён на `queue_leased (queue)` с тем же предикатом; схемы сборок части 6 не обновляются — старт
  падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- Dashboard (часть 6): в начальную миграцию `V001` добавлены `stats_minutes`, `queue_stats` и индексы `queue_awaiting_list`,
  `job_events_failed`, `job_events_cancelled`, `jobs_parent`, `jobs_recurring`, `batches_parent`; схемы сборок части 5 не
  обновляются — старт падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- `PgBoltStore.AckAsync` возвращает `IReadOnlyList<Guid>` (id подтверждённых задач) вместо числа.
- `PgBoltProcessingOptions.RunMaintenance = true` запускает и задачу `stats` (снимок `queue_stats` раз в `StatsSnapshotInterval`,
  retention `stats_minutes`).
- Workflows (часть 5): в начальную миграцию `V001` добавлены `batches`, `completions` и колонки `queue` / `jobs`; схемы сборок
  части 4 не обновляются — старт падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- `JobContext.BatchId`: `Guid?` → `BatchId?`; новый необязательный параметр конструктора `parentBatchId`. `JobContext.ParentId`
  заполняется только для родителя-задачи продолжения.
- `IJobClient`: новые члены `Workflow`, `CancelBatchAsync`, `GetBatchAsync` — пользовательские реализации и моки должны их реализовать.
- `jobs.state = 1` больше не используется: `JobState.Awaiting` выводится из строки `queue` с `await_condition`.
- `PgBoltProcessingOptions.RunMaintenance = true` запускает и resolver workflow: продолжения и batches разрешаются только в процессах
  с `AddPgBoltServer` и `RunMaintenance = true`.
- `PgBolt.Postgres.Client.IWakeupSignal`: метод `Task<bool> WaitAsync(IReadOnlyCollection<string> queues, TimeSpan timeout, CancellationToken ct)`
  удалён, вместо него — `IDisposable Subscribe(string queue, Action callback)`. Пользовательские реализации (например,
  Redis pub/sub) должны реализовать `Subscribe`: callback вызывается синхронно из `Signal`, должен быть дешёвым и не
  блокировать; `Dispose` результата отписывает.
- `PgBolt.Postgres.Storage.PgBoltStore.HeartbeatAsync(Guid serverId, TimeSpan lease, CancellationToken ct)` заменён на
  `HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)`:
  продлеваются только lease из списка; строки сервера вне списка больше не продлеваются и уходят sweeper'у.
- `PgBolt.Postgres.Hosting.IPgBoltMaintenance`: `EnsurePartitionsAsync(int daysAhead = 7, …)` → `EnsurePartitionsAsync(int? daysAhead = null, …)`
  (`null` — `PgBoltOptions.PartitionDaysAhead`); новые члены `ApplyRetentionAsync` и `RemoveDeadServersAsync`. Пользовательские
  реализации и моки интерфейса должны реализовать их; вызовы с явным `int` совместимы по исходному коду, не бинарно.
- `PgBoltStore.HeartbeatAsync` возвращает `HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases)` вместо
  `IReadOnlyList<HeartbeatLease>`.
- **Retention включён по умолчанию** (`JobsRetention = EventsRetention = 7 суток`): первый запуск обслуживания после обновления
  удаляет все партиции `jobs`/`job_events` старше срока и компактирует закреплённые. Чтобы сохранить историю, задайте сроки
  (или `null`, только кодом) до обновления.
- Миграции объединены в одну начальную `V001` (включая `maintenance_tasks` и объекты части 4); `PgBoltMigrations.LatestVersion = 1`.
  Схемы, созданные предыдущими сборками, не обновляются: старт падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- `EnqueueOptions.UniqueKey` и результаты `uniqueKeySelector` с префиксом `pgbolt:` отклоняются (`ArgumentException`): префикс
  зарезервирован для внутренних ключей.
- `PgBoltProcessingOptions.RunMaintenance = false` больше не выключает обработку целиком: тикер recurring управляется
  `RunScheduler` (по умолчанию `true`).

### Added

- Observability (часть 7a), подробности и примеры PromQL — в [`docs/observability.md`](docs/observability.md):
  - трассировка: producer-span при постановке (`enqueue {job.type}`, `workflow submit`, `recurring trigger`) и consumer-span
    выполнения задачи — ребёнок producer-span'а через `jobs.trace_parent`; источник `ActivitySource` `PgBolt`;
  - метрики `Meter` `PgBolt`: метрики пода (`pgbolt.job.enqueued`, `pgbolt.recurring.fired`, `pgbolt.job.duration`,
    `pgbolt.job.wait_time`, `pgbolt.job.outcome.dropped`, `pgbolt.worker.active`, `pgbolt.worker.capacity`, `pgbolt.storage.errors`,
    `pgbolt.maintenance.runs`, `pgbolt.maintenance.duration`, `pgbolt.retention.deferred`) и кластерные `pgbolt.cluster.queue.jobs`,
    `pgbolt.cluster.queue.oldest_ready_age`, `pgbolt.cluster.queue.limit`;
  - логи: события жизненного цикла задачи (`EventId` 3001–3008, категория `PgBolt.Job`), scope задачи (`JobId`, `JobType`, `Queue`,
    `Attempt`), события телеметрии 8001 / 8002, `SlotReleaseFailed` (1112), `QueueStatsReadFailed` (5012); все вызовы логов переведены на
    `[LoggerMessage]` со стабильными `EventId` и `EventName` (тексты и категории прежние);
  - `PgBoltDiagnostics` (`PgBolt.Abstractions`): имена источника, `Meter`, instruments и границы бакетов гистограмм;
  - пакет `PgBolt.OpenTelemetry`: `AddPgBoltInstrumentation()` для `TracerProviderBuilder` и `MeterProviderBuilder` (подписка и
    границы бакетов); зависит от OpenTelemetry 1.15.3 (поднята с 1.12.0 из-за уязвимости GHSA-g94r-2vxg-569j в `OpenTelemetry.Api` 1.12.0);
  - `LeasedJob.WaitSeconds` (последний параметр, по умолчанию 0): время от `run_at` до lease; колонка `queue_stats.oldest_ready_seconds`;
  - бенчмарк `JobRunner` (`benchmarks/PgBolt.Server.Benchmarks`): накладные расходы телеметрии на задачу; без слушателей добавляется
    только scope логов (≈ 32 байта), порога по времени нет, цифры и ограничения замера — в README бенчмарка.
- Кластерные метрики bloat `pgbolt.cluster.table.dead_tuples` (`n_dead_tup`) и `pgbolt.cluster.table.size` (`pg_total_relation_size`)
  с тегом `pgbolt.table` = `queue` / `completions` (константы `PgBoltDiagnostics.Instruments.ClusterTableDeadTuples` и
  `ClusterTableSize`); читает их цикл `stats` каждого пода с серверами, ошибка чтения — `pgbolt.storage.errors` с операцией
  `table_stats_read`. Пороги алертов — в [`docs/observability.md`](docs/observability.md).
- Нагрузочный стенд `benchmarks/PgBolt.LoadTest`: сценарии `sustained`, `ramp`, `partitions`, `vacuum` напрямую и через PgBouncer,
  профиль нагрузки B (10 000 задач/с) и SLO с отчётами в `results/`; результаты, решения и рекомендации по конфигурации — в
  [`docs/performance.md`](docs/performance.md). Reloptions `queue` в `V001` по результатам не менялись.
- Кластерный лимит конкурентности очереди: `QueueOptions.ClusterMaxConcurrency` (`null` или ≥ 1) — сколько задач очереди
  одновременно держат lease во всём кластере. Dequeue лимитированной очереди сериализуется `pg_advisory_xact_lock` и считает
  взятые строки; батч в неявной транзакции, один round trip, работает через PgBouncer в transaction mode. Dequeue очередей
  без лимита не меняется. Пока лимит у очереди исчерпан, сервер не опрашивает её по сигналам enqueue и освобождению слота —
  только после коммита исхода своей задачи этой очереди и плановым опросом.
- Веса очередей в общем пуле сервера: `QueueOptions.Weight` (`null` или 1…1000) — настройка очереди на весь кластер, каждый
  сервер делит по ней свой общий пул (stride scheduling); без весов у очередей сервера — прежний строгий порядок объявления.
  Смена весов применяется на границе плана выборки.
- `IQueueSettingsManager` (регистрирует `AddPgBolt`): `ListAsync`, `GetAsync` (`QueueSettings` — значения из кода,
  переопределения, кто и когда их задал, `EffectiveMaxConcurrency`, `Paused`, эффективный вес), `SetConcurrencyOverrideAsync`
  (`null` — без лимита, 0 — пауза, ≥ 1 — лимит), `ResetConcurrencyOverrideAsync`, `SetWeightOverrideAsync` (`null` — без веса,
  1…1000), `ResetWeightOverrideAsync`, `InvalidateCache`. Переопределение приоритетнее значения из кода; лимит и вес
  переопределяются независимо.
- `PgBoltOptions.QueueSettingsCacheDuration` — TTL кэша `IQueueSettingsManager` (`null` по умолчанию — без кэша; иначе > 0 и
  ≤ 1 сут). `PgBoltProcessingOptions.QueueSettingsRefreshInterval` — интервал обновления настроек очередей в процессе с
  сервером (10 с ± 10 %, > 0 и ≤ 1 сут).
- При старте процесс с сервером публикует значения `ClusterMaxConcurrency` / `Weight` своих очередей в `queue_settings`
  (`published_by` — `{MachineName}/{ProcessId}`); разные значения одной очереди на серверах процесса — ошибка старта, расхождение
  с недавно стартовавшим другим процессом — Warning (побеждает последний записавший).
- `PgBoltStore`: `DequeueLimitedAsync` → `LimitedDequeueResult(Jobs, LimitReached)`, `SyncQueueDefaultsAsync`
  (`QueueDefault` → `QueueDefaultConflict`), `GetActiveQueueSettingsAsync` → `ActiveQueueSetting`.
- Диагностики генератора: BOLT010 — разные константные `ClusterMaxConcurrency` или `Weight` одной очереди в вызовах
  `PgBoltServerOptions.Queue` проекта; BOLT011 — недопустимое константное значение опции очереди или
  `QueueSettingsCacheDuration`.
- Dashboard, экран очередей: колонки «Лимит» (`взято / лимит`, «Пауза») и «Вес» с происхождением значения («из кода» или
  «переопределён: кто, когда»); действия «Задать лимит», «Без лимита», «Пауза», «Сбросить к значению из кода», «Задать вес»,
  «Без веса», «Сбросить кэш настроек»; очереди с переопределением видны и без задач и серверов.
- Допущение: лимит, впервые появившийся у очереди после старта процесса (переопределение или значение другого процесса),
  этот процесс соблюдает не позже чем через `QueueSettingsRefreshInterval` — со следующей выборки после обновления снимка;
  до тех пор его dequeue лимит не учитывает. Изменение числа у уже лимитированной очереди, в том числе пауза, действует сразу.
- Пакет `PgBolt.Dashboard`: `services.AddPgBoltDashboard(...)` + `app.MapPgBoltDashboard("/pgbolt")` (`RouteGroupBuilder`) —
  обзор (счётчики, throughput за час и сутки), очереди, задачи по состояниям (keyset, обход порциями с бюджетом `ScanBudget`),
  карточки задачи и batch, batches, recurring с историей запусков, серверы с задачами обслуживания, типы задач; действия
  cancel / trigger / requeue (в т. ч. над выбранными, до 100), отмена batch, pause / resume / trigger / delete расписаний;
  запуск задачи по имени с формой из `job_types.args_schema`. Без `AddRazorComponents`, `UseAntiforgery` и статических файлов.
- Безопасность dashboard: без авторизации на группе — только локальные запросы без заголовков прокси (`AllowAnonymousRemote`
  снимает ограничение); в service mesh (sidecar на loopback без заголовков) авторизация на группе обязательна; `ActionsPolicy`,
  `ReadOnly`; CSRF — `HX-Request` + `Sec-Fetch-Site` / `Origin` (за TLS-прокси хост применяет `ForwardedHeaders`).
- Статистика: счётчики подтверждённых исходов серверов (`stats_minutes`, сброс раз в `StatsFlushInterval` = 15 с, финальный —
  при остановке) и снимок живых состояний (`queue_stats`, `StatsSnapshotInterval` = 10 с); `StatsRetention` = 7 суток.
- `QueryTimeout` ограничивает запросы самого dashboard; карточки и перечитывание после действий идут через `IJobClient` /
  `IRecurringJobManager` (поиск по PK) без него.
- Workflows (часть 5): fluent API `IJobClient.Workflow()` — `Enqueue`, `StartBatch`, `After(JobId | BatchId)`, `ContinueWith`,
  `ContinueWithBatch` (`ContinuationCondition.OnSuccess` / `OnAnyFinished`), `EnqueueMany` в теле batch, `SubmitAsync` одной
  транзакцией (`WorkflowOptions.Transaction` — точка сохранения); вложенные batches и batch-продолжения; `CancelBatchAsync`,
  `GetBatchAsync` (`BatchInfo`, `BatchState`, `BatchCancelResult`); `ParentNotFoundException`, `UniqueKeyConflictException`;
  `JobInfo.ParentId`, `ParentBatchId`, `BatchId`, `Continuation`, `JobState.Awaiting` для ждущих задач; `JobContext.ParentBatchId`.
- `PgBolt.Postgres.Client.ICompletionSignal` / `InProcessCompletionSignal` (пробуждение resolver'а), `PgBoltProcessingOptions.ResolverInterval`
  (1 с); resolver (учёт сигналов, обход детей, отмена членов) в цикле обслуживания `PgBolt.Server`; Warning при лаге сигналов.
- Retention: родители с ждущими продолжениями и Failed/Cancelled-члены существующих batches не удаляются; строки `batches` старше
  `JobsRetention` удаляются сверху вниз (`RetentionResult.DeletedBatches`).
- Генератор регистрирует метаданные для типизированных шагов workflow (`Enqueue<TArgs>`, `ContinueWith<TArgs>`, `EnqueueMany<TArgs>`);
  BOLT001 / BOLT006 для них; новая диагностика BOLT009 (Warning) — workflow не отправлен (`SubmitAsync`) в методе.
- Пакет `PgBolt.Server`: worker runtime — `AddPgBoltServer`, `AddPgBoltJobFilter`, `ConfigurePgBoltProcessing`,
  `RetryPolicy`, `IJobFilter`, `JobExecutorNotFoundException`.
- `PgBolt.Abstractions`: `JobAttribute.TimeoutSeconds`, `JobDescriptor.TimeoutSeconds`, `JobAbortedException`,
  `JobArgumentsException`; диагностики генератора BOLT007 (отрицательный `TimeoutSeconds`, ошибка) и BOLT008
  (`TimeoutSeconds` больше предела таймеров .NET 4294967 с, предупреждение: сервер урезает таймаут до предела).
- `PgBoltStore`: `ReleaseAsync`, `RegisterServerAsync`, `RemoveServerAsync`, `GetJobTypeNamesByQueuesAsync`.
- Часть 2 (Storage maintenance): таблица `maintenance_tasks` (в начальной миграции `V001`); цикл обслуживания в `PgBolt.Server` — периодический
  `EnsurePartitionsAsync`, retention с компактированием партиций, закреплённых живыми задачами, уборка `servers`; кластерная
  координация задач через `maintenance_tasks`.
- `PgBoltOptions`: `JobsRetention`, `EventsRetention`, `RetentionLockTimeout`, `RetentionCompactionMaxLiveJobs`.
- `PgBoltProcessingOptions`: `MaintenancePollInterval`, `PartitionsInterval`, `ServersCleanupInterval`, `RetentionInterval`,
  `DeadServerTimeout`.
- `RetentionResult`, `PinnedPartition`, `DeferredPartition`, `DeferReason`; `PgBoltStore.TryClaimMaintenanceAsync`,
  `CompleteMaintenanceAsync`, `RemoveDeadServersAsync`.
- Recurring-задачи (часть 4): `IRecurringJobManager` (`AddOrUpdateAsync`, `RemoveAsync`, `PauseAsync`, `ResumeAsync`,
  `TriggerAsync`, `GetAsync`), code-first `PgBoltOptions.AddRecurring` / `ClearRecurring` / `RecurringOwner`, cron (Cronos, с
  секундами) в таймзоне, `MisfirePolicy` (`FireOnce`, `Skip`, `CatchUp` с `CatchUpWindow`), `OverlapPolicy` (`Skip`, `Allow`);
  `JobContext.ScheduledAt`, `JobContext.RecurringJobId`, `JobInfo.ScheduledAt`, `JobInfo.RecurringJobId`;
  `PgBoltProcessingOptions.RunScheduler`, `SchedulerInterval`, `MisfireThreshold`, `RecurringSyncInterval`.
- Генератор регистрирует метаданные `[Job]`-типа и для мест вызова `AddRecurring<TArgs>` / `IRecurringJobManager.AddOrUpdateAsync<TArgs>`
  (продюсер без исполнителя); анализатор проверяет их покрытие JSON-контекстом (BOLT001 / BOLT006), как у `EnqueueAsync`.

### Changed

- Observability (часть 7a):
  - `jobs.trace_parent` пишет id producer-span'а (без него — id `Activity.Current`, как раньше); `IRecurringJobManager.TriggerAsync`
    пишет `trace_parent` по тому же правилу (раньше — `NULL`);
  - цикл `stats` работает на любом поде с серверами: под читает снимок `queue_stats` для кластерных метрик, а `ProcessingMaintenance`
    создаётся при наличии серверов независимо от флагов; снимок по-прежнему делает только под с `RunMaintenance = true`;
  - исключение `JobRunner.RunAsync` или отправки исхода освобождает слот и lease: `PgBoltServer.RunExecutionAsync` больше не бросает,
    запись снимается из таблицы до первого лога, исход учитывается в `pgbolt.job.outcome.dropped` с причиной `abandoned`.
- Observability, исправления:
  - `JobRunner`: длительность и причина исхода снимаются до классификации (время `RetryPolicy` не входит в длительность попытки, отмена
    во время него не меняет причину), а без слушателя источника `PgBolt` `trace_parent` не разбирается;
  - `ProducerSpan` восстанавливает `Activity.Current` до записи сбоя телеметрии, имя span'а строится только при наличии слушателя;
  - `pgbolt.storage.errors` не учитывает сбои логгера после ответа БД и исключения остановки;
  - `pgbolt.job.outcome.dropped` с причиной `abandoned` пишет `LeaseKeeper.Unregister` — один раз на выполнение.
- Invoker исполнителей оборачивает ошибки десериализации аргументов (`JsonException`, `NotSupportedException`,
  `null` вместо объекта) в `JobArgumentsException`.
- `job_types.published_by` — идентификатор процесса-публикатора `{MachineName}/{ProcessId}`.
- Регистрация сервера — upsert; строку `servers`, удалённую уборкой, восстанавливает следующий heartbeat.
- Версия схемы новее кода — Warning вместо ошибки старта; `EnsurePartitionsAsync` не мигрирует более новую схему.
- `UsePreparedStatements` работает через PgBouncer в режиме transaction с `max_prepared_statements > 0`: к `ConnectionString`
  всегда добавляется `No Reset On Close=true` (раньше — только без prepared statements, и сброс сессии `DISCARD ALL` при возврате
  соединения в пул приводил к ошибке `prepared statement "…" does not exist`).
- Известные ограничения: компактирование не MVCC-безопасно для снимков REPEATABLE READ/SERIALIZABLE, взятых до него
  (выход — `RetentionCompactionMaxLiveJobs = 0`); при публикации `jobs` в логической репликации компактирование пропускается;
  закреплённые сутки остаются присоединёнными до ухода живых задач.
