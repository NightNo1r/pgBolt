# PgBolt — часть 7a: observability (трассировка, метрики, логи)

Статус: реализовано, план `docs/superpowers/plans/2026-10-16-pgbolt-observability.md` · Дата: 2026-10-16 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»), `2026-09-28-pgbolt-worker-design.md` (далее «worker»), `2026-09-29-pgbolt-storage-maintenance-design.md` (далее «часть 2»), `2026-09-30-pgbolt-scheduler-design.md` (далее «часть 4»), `2026-10-03-pgbolt-dashboard-design.md` (далее «часть 6»), `2026-10-10-pgbolt-queue-limits-design.md` (далее «часть 3b»)

## 1. Цель и рамки

Дать оператору production-кластера PgBolt (несколько подов, на каждом — виртуальные серверы со своими пулами воркеров) стандартные сигналы для стека OpenTelemetry → Prometheus / Tempo / Loki: дашборды и алерты по метрикам, разбор инцидентов по трейсам, поиск логов конкретной задачи. Часть 7 roadmap делится на 7a (эта спека) и 7b (нагрузочный стенд, SLO, прогоны через PgBouncer, autovacuum/bloat, `COPY` против `unnest`, бенчмарк числа партиций, Grafana-дашборды).

**Входит:**
- трассировка: producer-span при enqueue, consumer-span выполнения задачи — дочерний к enqueue через `jobs.trace_parent` (§3);
- метрики пода (аддитивные) и кластерные метрики (одинаковые на всех подах) с явными правилами агрегации (§4);
- логи: жизненный цикл задачи, scope задачи, перевод всех вызовов на `[LoggerMessage]` со стабильными `EventId` (§5);
- изоляция сбоев телеметрии и устойчивость завершения выполнения (§8);
- пакет `PgBolt.OpenTelemetry` с `AddPgBoltInstrumentation()` (§6.4);
- документация `docs/observability.md` с PromQL-примерами (§9).

**Не входит:**
- spans для polling диспетчера, ack, heartbeat, задач обслуживания (корни раз в секунды — шум; SQL-spans даёт Npgsql, если пользователь подключит его источник);
- baggage и `tracestate` (колонка `trace_parent` хранит только `traceparent`);
- метрики workflow resolver'а (лаг уже пишется в лог, часть 5);
- exemplars, готовые Grafana-дашборды (7b);
- изменение `OutcomeStats` / `stats_minutes` dashboard'а (часть 6) — отдельный канал, не трогается;
- обёртка существующих вызовов логов в защиту от исключений провайдеров (поведение до 7a), в том числе предупреждения `TimeoutOf` в `JobRunner` и логов `RetryDecider`; защищены только точки телеметрии 7a и последний рубеж (§8 п. 1–2).

### Критерии успеха
- HTTP-запрос → `EnqueueAsync` → выполнение на другом поде видны в одном трейсе: consumer-span — ребёнок producer-span'а, вызовы из кода задачи (HttpClient, Npgsql) — дети consumer-span'а (E2E-тест).
- Для любой метрики пода `sum` по подам даёт кластерное значение. `pgbolt.cluster.queue.jobs` и `pgbolt.cluster.queue.oldest_ready_age` на каждом поде с серверами публикуют последний снимок `queue_stats` с отставанием не больше одного прохода цикла `stats` пода (≤ 1,2 × `StatsSnapshotInterval` от коммита снимка), при свежем снимке — с нулями для известных очередей без строк (в том числе в простое, когда `queue_stats` пуст), при устаревшем — пустой набор; `pgbolt.cluster.queue.limit` — с отставанием не больше одного обновления `QueueSettingsCache` (≤ 1,1 × `QueueSettingsRefreshInterval`, jitter ±10 %); `max` по подам не даёт дублей и пропусков при смене пода, делающего снимок (E2E-тест с двумя хостами на одной БД).
- `histogram_quantile` по `sum by (le)` гистограмм всех подов корректен: бакеты одинаковы на всех подах.
- Без слушателей (`ActivityListener`, `MeterListener`) выполнение задачи не аллоцирует больше, чем до части 7a, кроме scope логов (§5.2); подтверждается бенчмарком `JobRunner`.
- Исключение каждой неуспешной попытки задачи (исходы `Retry`, `Fail`, `ConfirmCancel`) попадает в лог с `JobId` и `EventId`; при созданном consumer-span'е (есть слушатель источника `PgBolt`, сэмплер не отказал) — и с его `TraceId`.
- Телеметрия никогда не меняет исход задачи и не останавливает runtime: исключение слушателя `ActivityListener` / `MeterListener` или логгер-провайдера в точке телеметрии 7a перехватывается (§8), а исключение `JobRunner.RunAsync` не оставляет занятый слот и бесконечно продлеваемый lease и не выходит из `PgBoltServer.RunExecutionAsync`, который не бросает никогда, даже если бросает и логгер-провайдер на всех событиях (§8).

## 2. Принятые решения

| Решение | Значение | Почему |
|---|---|---|
| Примитивы | `System.Diagnostics.ActivitySource` и `System.Diagnostics.Metrics.Meter` из BCL в `PgBolt.Postgres` / `PgBolt.Server` | Без новых зависимостей ядра, AOT-совместимо, почти бесплатно без слушателя; так сделаны Npgsql, ASP.NET Core |
| Пакет OpenTelemetry | Отдельный `PgBolt.OpenTelemetry` поверх BCL-инструментирования | Удобство одной строкой, ядро не тянет OTel SDK |
| Где consumer-span | В `JobRunner.RunAsync`, не фильтром | Фильтр создаётся в scope на каждую задачу и не видит исход (retry / fail / release), отказ «нет исполнителя», shutdown. Пересматривает worker §1 («встраивается фильтром, §3.6») |
| Связь выполнения с enqueue | Родитель (не `ActivityLink`), удалённый (`isRemote: true`) | Один трейс от HTTP до выполнения и вложенных вызовов; как MassTransit / Hangfire.OTel. Минус — трейсы с ретраями и отложенными задачами длинные по времени |
| Что пишется в `trace_parent` | Id producer-span'а; без него — id `Activity.Current`, как до 7a; в том числе `IRecurringJobManager.TriggerAsync` (до 7a — `NULL`) | Выполнение — ребёнок enqueue, а не HTTP-span'а; поведение без OTel не меняется |
| Имя consumer-span'а | Имя типа задачи | Ограниченная кардинальность, поиск в Tempo по типу |
| Агрегация | Метрики пода — аддитивны (`sum`); кластерные величины — префикс `pgbolt.cluster.*`, одинаковое значение на всех подах (`max`) | Один публикатор кластерных величин при смене владельца снимка неизбежно даёт дубли или пропуски |
| Источник кластерных величин | Снимок `queue_stats` (часть 6 §4.2): каждый под с серверами, независимо от `RunMaintenance`, раз в проход цикла `stats` читает таблицу вместе с моментом последнего успешного снимка; владелец задачи — после своего прохода, тем же чтением (§4.4) | Без новых сканов `queue`; цена — один `SELECT` маленьких таблиц на под раз в 10 с; один путь публикации — одинаковые значения на всех подах |
| Свежесть и устаревание кластерных величин | По строке `stats` в `maintenance_tasks` (последний успешный проход), как dashboard (часть 6 §4.2, §6.4), уточнённой `measured_at` строк снимка; возраст — на стороне PG + время с момента получения, не момент чтения. Свежий снимок публикуется с нулями для известных очередей, устаревший — пустым набором | В простое `queue_stats` пуст (часть 6 §4.2): свежесть по самим строкам не отличает «живых задач нет» от «снимок не обновляется»; чтение застывшей таблицы не должно продлевать жизнь ряда |
| `Meter` | Один на DI-контейнер: singleton `PgBoltInstrumentation` берёт его из `IMeterFactory`, без фабрики создаёт собственный и освобождает вместе с контейнером; статического `Meter` нет | Изоляция хостов одного процесса и тестов (`MetricCollector` по `meterScope`); callbacks остановленного хоста не публикуются |
| Бакеты гистограмм | Фиксированные public-константы `PgBoltDiagnostics` (§3.1): runtime передаёт их в `InstrumentAdvice`, `PgBolt.OpenTelemetry` — во View | Одинаковые на всех подах → сливаемые перцентили; один источник значений для runtime и пакета |
| Сбой телеметрии | Каждая точка телеметрии 7a — в `try/catch`; сбой пропускает сигнал и пишет событие 8001 не чаще раза в минуту | Слушатели и логгер-провайдеры — пользовательский код, BCL вызывает их синхронно и исключений не перехватывает |
| Логи | `[LoggerMessage]`, стабильные `EventId` по диапазонам, тексты существующих сообщений не меняются, категории не меняются | Алертинг по `EventId`; нет форматирования без включённого уровня; фильтры пользователей не ломаются |
| Миграции | Изменения вносятся в `V001__initial.sql` | Как в частях 3b–6: пользователей нет, схемы прошлых сборок отвергаются при старте — `PgBoltInitializer` проверяет и новую колонку (§7.2) |

## 3. Трассировка

### 3.1 Имена (`PgBolt.Abstractions`)

```csharp
public static class PgBoltDiagnostics
{
    public const string ActivitySourceName = "PgBolt";
    public const string MeterName = "PgBolt";

    /// <summary>Имена instruments §4.2–§4.3.</summary>
    public static class Instruments
    {
        public const string JobEnqueued = "pgbolt.job.enqueued";
        public const string RecurringFired = "pgbolt.recurring.fired";
        public const string JobDuration = "pgbolt.job.duration";
        public const string JobWaitTime = "pgbolt.job.wait_time";
        public const string JobOutcomeDropped = "pgbolt.job.outcome.dropped";
        public const string WorkerActive = "pgbolt.worker.active";
        public const string WorkerCapacity = "pgbolt.worker.capacity";
        public const string StorageErrors = "pgbolt.storage.errors";
        public const string MaintenanceRuns = "pgbolt.maintenance.runs";
        public const string MaintenanceDuration = "pgbolt.maintenance.duration";
        public const string RetentionDeferred = "pgbolt.retention.deferred";
        public const string ClusterQueueJobs = "pgbolt.cluster.queue.jobs";
        public const string ClusterQueueOldestReadyAge = "pgbolt.cluster.queue.oldest_ready_age";
        public const string ClusterQueueLimit = "pgbolt.cluster.queue.limit";
    }

    /// <summary>Границы бакетов гистограмм §4.5, секунды.</summary>
    public static class HistogramBuckets
    {
        public static IReadOnlyList<double> Job { get; } = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600];
        public static IReadOnlyList<double> Maintenance { get; } = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300];
    }
}
```

`PgBoltMetrics`, `PgBoltClientMetrics` и `PgBolt.OpenTelemetry` берут имена и границы только отсюда; литералов имён instruments и границ в других сборках нет. `IReadOnlyList<double>` из collection expression неизменяем.

Единственный `ActivitySource` — `internal static` в `PgBolt.Postgres` (`PgBoltTelemetry.Source`), версия — информационная версия сборки `PgBolt.Postgres`. `PgBolt.Server` использует его же через существующий `InternalsVisibleTo`.

### 3.2 Producer-span

Создаётся на один вызов клиентского API (не на задачу), `ActivityKind.Producer`, родитель — `Activity.Current`:

| Вызов | Имя span'а |
|---|---|
| `IJobClient.EnqueueAsync` / `EnqueueManyAsync` (все перегрузки, в т. ч. динамическая по имени и транзакционные) | `enqueue {job.type}` |
| `Workflow().…SubmitAsync()` | `workflow submit` |
| `IRecurringJobManager.TriggerAsync` | `recurring trigger` |

Атрибуты: `messaging.system = "pgbolt"`, `messaging.operation.type = "send"`, `messaging.destination.name` (очередь; для workflow с несколькими очередями — не ставится), `pgbolt.job.type` (для enqueue), `messaging.batch.message_count` (если задач > 1), `pgbolt.recurring.id` (для trigger). Ошибка вызова → статус `Error`, `error.type` (полное имя типа исключения), событие исключения. Старт и завершение span'а — под защитой §8. Перед стартом сохраняется прежний `Activity.Current`; сбой старта (бросил `Sample` или `ActivityStarted`; во втором случае `Activity.Start` уже сделал недостартовавший span текущим) → `Activity.Current` восстанавливается в сохранённый, span считается `null`; сбой завершения → `Activity.Current` также восстанавливается в сохранённый.

`trace_parent` строк — `activity.Id` producer-span'а, только при `activity.IdFormat == ActivityIdFormat.W3C` (родитель в формате Hierarchical даёт Hierarchical-ребёнка — такой id не пишется). Иначе (span не создан: нет слушателя, сэмплер отказал, сбой старта; или не W3C) пишется `PgBoltJobClient.CurrentTraceParent()` — `Activity.Current.Id` при W3C-формате, иначе `NULL` — вычисленный после восстановления `Activity.Current`, поэтому id недостартовавшего span'а в `trace_parent` не попадает. `RecurringJobManager.TriggerAsync` до 7a писал `TraceParent: null` и переходит на то же правило. Транзакционный enqueue, откаченный вызывающим, оставляет span без задачи — допустимо.

Recurring-задачи, поставленные тиком планировщика (`TickRecurringAsync`), получают `trace_parent = NULL`: тик — фоновая работа без контекста, consumer-span такой задачи — корень нового трейса (§3.3). Продолжения workflow получают `trace_parent` span'а submit (одинаковый у всех узлов). `IJobClient.RequeueAsync` `trace_parent` не меняет: повторное выполнение — ребёнок исходного producer-span'а (как ретрай), это фиксируется в документации.

### 3.3 Consumer-span выполнения

В `JobRunner.RunAsync` после проверки shutdown, `ActivityKind.Consumer`:
- перед стартом `Activity.Current = null`: цикл диспетчера (`Task.Run`) и выполнения (`Task<Task>.Start`) наследуют `ExecutionContext` из `StartAsync` хоста, и без сброса задача без `trace_parent` стала бы ребёнком чужой долгоживущей `Activity`; изменение локально для асинхронного потока `RunAsync`;
- родитель — `ActivityContext.TryParse(job.TraceParent, null, isRemote: true, out ctx)`; `null` или не парсится → `parentContext: default` → корень, без ошибки и без лога;
- имя — `job.Type`;
- атрибуты при старте: `messaging.system = "pgbolt"`, `messaging.operation.type = "process"`, `messaging.destination.name` (очередь), `messaging.message.id` (id задачи), `pgbolt.job.type`, `pgbolt.job.attempt`, `pgbolt.job.max_attempts`, `pgbolt.server.name`, при наличии — `pgbolt.batch.id`, `pgbolt.job.parent_job_id` (`WorkflowCodes.ParentJob(job.Continuation, job.ParentId)`, коды 1, 2), `pgbolt.job.parent_batch_id` (`WorkflowCodes.ParentBatch(...)`, коды 3, 4; `LeasedJob.ParentId` продолжения batch'а — id batch'а), `pgbolt.recurring.id`;
- атрибут после классификации: `pgbolt.job.outcome` — `succeeded | retry | failed | cancelled | released` (из `JobOutcome`: `Ack`, `Retry`, `Fail`, `ConfirmCancel`, `Release`);
- `Retry` и `Fail` → статус `Error`, событие исключения (`Activity.AddException`), `error.type`: `timeout` / `heartbeat_lost` / `lease_lost`, если `Execution.FirstReason` — `Timeout` / `HeartbeatLost` / `LeaseLost`, иначе полное имя типа исключения;
- отказ «нет исполнителя»: `JobRunner` создаёт `JobExecutorNotFoundException(job.Type)` и передаёт в `OutcomeClassifier.NoExecutor(execution, error, retry)` (сейчас исключение создаётся внутри классификатора) — span, событие исключения и лог §5.1 получают тот же объект, `error.type = PgBolt.Server.JobExecutorNotFoundException`.

Порядок в `RunAsync`:
1. Shutdown до старта → `Release(Ran: false)`, событие `JobReleased` (§5.1); span'а и scope нет.
2. Отметка начала попытки `_clock.Now` (`MonoClock`, независимо от span'а — основа `job.duration`), сброс `Activity.Current`, старт span'а с атрибутами, `BeginScope` (§5.2), `job.wait_time` (§4.2), событие `JobStarted`.
3. Поиск исполнителя (или `NoExecutor`), выполнение, классификация исхода.
4. Атрибут исхода, статус, `job.duration`, событие §5.1 исхода — пока span текущий и scope открыт.
5. `Dispose` scope, `Stop` span'а — в `finally` вокруг п. 3–4: выполняются и при исключении, вышедшем из п. 3 вне защиты (существующее предупреждение `TimeoutOf`, лог `RetryDecider`), — span не остаётся открытым, scope не утекает; исключение затем выходит из `RunAsync` (§8). Исход возвращается в `PgBoltServer.RunExecutionAsync`, который отдаёт его в `CompletionSink` (ack пакетный и к попытке не привязан).

Каждый шаг телеметрии в п. 1, 2, 4, 5 — под защитой §8. Внутри задачи `Activity.Current` — этот span. Каждая попытка — отдельный consumer-span под тем же родителем.

## 4. Метрики

### 4.1 Общее

- `PgBoltInstrumentation` (internal, `PgBolt.Postgres`, singleton DI, регистрирует `AddPgBolt`) владеет `Meter`: `IMeterFactory.Create(new MeterOptions(PgBoltDiagnostics.MeterName) { Version = <информационная версия сборки PgBolt.Postgres> })`, если фабрика зарегистрирована (`Host.CreateApplicationBuilder` и `Host.CreateDefaultBuilder` регистрируют её сами, иначе — `services.AddMetrics()`); без фабрики — `new Meter(те же MeterOptions)`, освобождаемый в `Dispose` singleton'а вместе с контейнером. Он же сообщает о сбоях телеметрии (§8).
- `PgBoltClientMetrics` (`PgBolt.Postgres`) и `PgBoltMetrics` (`PgBolt.Server`) — singleton'ы того же контейнера; instruments создаются один раз на `Meter` из `PgBoltInstrumentation` с именами и бакетами из `PgBoltDiagnostics` (§3.1). Observable-instruments `PgBoltMetrics` создаёт один раз; источники виртуальных серверов (`SlotAllocator`, ёмкость, имена очередей сервера) регистрируются в `PgBoltServer.Start` и снимаются в `finally` вокруг всего тела `PgBoltServer.FlushAndStopAsync` — в том числе при раннем выходе по отменённому `ct` и при исключении остановки sink'а; callback перечисляет зарегистрированные. Кластерные gauges (§4.3) публикуются, только пока зарегистрирован хотя бы один источник сервера.
- Теги — через `TagList` (до 8 тегов без аллокаций). Синхронные instruments пишутся без проверки слушателя (BCL проверяет сам); дорогие вычисления тегов — под `instrument.Enabled`. Каждая запись — метод `PgBoltMetrics` / `PgBoltClientMetrics` с защитой §8 внутри.
- Имена атрибутов: `pgbolt.queue`, `pgbolt.job.type`, `pgbolt.job.outcome`, `pgbolt.server.name`, `pgbolt.job.state`, `pgbolt.enqueue.result`, `pgbolt.drop.reason`, `pgbolt.storage.operation`, `pgbolt.maintenance.task`, `pgbolt.maintenance.result`, `pgbolt.retention.reason`.
- Кардинальность ограничена: типы задач, очереди, имена виртуальных серверов, задачи обслуживания — конечные множества. Id задач, batch'ей, серверов в метки не попадают. Различение подов — атрибут ресурса `service.instance.id`: OTel .NET SDK ставит его только через `ResourceBuilder.AddService(...)` (`autoGenerateServiceInstanceId: true` по умолчанию) или `OTEL_RESOURCE_ATTRIBUTES`, в `ResourceBuilder.CreateDefault()` его нет; требование фиксируется в документации.

### 4.2 Метрики пода (аддитивные, агрегация — `sum`)

| Метрика | Тип, единица | Теги | Где пишется |
|---|---|---|---|
| `pgbolt.job.enqueued` | Counter, `{job}` | queue, job.type, enqueue.result = `created` / `duplicate` | клиент: enqueue, workflow submit, recurring `TriggerAsync` (правила ниже) |
| `pgbolt.recurring.fired` | Counter, `{job}` | — | тик планировщика (`RecurringTickResult.Fired`) |
| `pgbolt.job.duration` | Histogram, `s` | queue, job.type, job.outcome | `JobRunner`, `_clock.Elapsed` от отметки начала попытки (§3.3 п. 2, пишется и без слушателя источника) до классификации |
| `pgbolt.job.wait_time` | Histogram, `s` | queue, job.type | `JobRunner` при старте попытки (§3.3 п. 2), по `LeasedJob.WaitSeconds` (§7.1) |
| `pgbolt.job.outcome.dropped` | Counter, `{job}` | queue, drop.reason = `lease_lost` / `abandoned` / `stopped` / `send_timeout` | переход записи выполнения в `Removed` без применения исхода (таблица ниже) |
| `pgbolt.worker.active` | ObservableUpDownCounter, `{worker}` | server.name, queue | `SlotAllocator.InUse(queue)` |
| `pgbolt.worker.capacity` | ObservableUpDownCounter, `{worker}` | server.name | `Workers` + сумма `Reserved` сервера |
| `pgbolt.storage.errors` | Counter, `{error}` | storage.operation (таблица ниже) | места, где runtime ловит и логирует исключение обращения к БД |
| `pgbolt.maintenance.runs` | Counter, `{run}` | maintenance.task, maintenance.result = `ok` / `error` / `deferred` | `ProcessingMaintenance.RunTaskAsync` (только захваченные запуски) |
| `pgbolt.maintenance.duration` | Histogram, `s` | maintenance.task | то же |
| `pgbolt.retention.deferred` | Counter, `{partition}` | retention.reason (значения `DeferReason`) | `RetentionReport.Write` (метрика, обещанная core §4.4) |

`maintenance.result = deferred` — `TaskOutcome` с ошибкой без исключения (например, `lock_timeout` партиций).

**`job.enqueued`.** Значение — число строк; на вызов — один `Add` на тройку `(queue, job.type, enqueue.result)`: у enqueue очередь одна, узлы workflow могут быть в разных очередях. Workflow submit учитывает все узлы как `created` (конфликт ключа бросает `UniqueKeyConflictException`, вызов не учитывается). Транзакционный enqueue учитывается при возврате из вызова, даже если вызывающий потом откатит транзакцию. Задачи тика планировщика сюда не входят — их считает `recurring.fired`.

**`job.wait_time`.** Задачи, взятые и отданные release без запуска (сигнал остановки или сбой heartbeat во время dequeue, dequeue после своего таймаута, shutdown до старта в `RunAsync`), не учитываются: их ожидание войдёт в `wait_time` следующего lease.

**`job.outcome.dropped`.** Счётчик увеличивает код, выполнивший CAS-переход записи в `Removed` без применения исхода, поэтому каждая запись учитывается не более раза:

| Путь | Где | `drop.reason` |
|---|---|---|
| lease потерян во время выполнения (`Running → Removed`) | `LeaseKeeper` (п. 5 heartbeat) | `lease_lost` |
| lease потерян с исходом в очереди отправки (`OutcomePending → Removed`) | `LeaseKeeper` (п. 5 heartbeat) | `lease_lost` |
| store ответил `LeaseLost` на retry / fail / confirm-cancel / release с первой попытки | `CompletionSink.SendSingleAsync` | `lease_lost` |
| ack подтвердил не все lease пакета с первой попытки | `CompletionSink.SendAckBatchAsync`, на `count − acked` | `lease_lost` |
| выполнение не остановилось за `ShutdownTimeout` | `PgBoltServer.AbandonRunning` | `abandoned` |
| `JobRunner.RunAsync` или `CompletionSink.Complete` бросил (§8) | `PgBoltServer.RunExecutionAsync`, при `LeaseKeeper.Unregister` = `true` | `abandoned` |
| сбой запуска выполнения после `Register` (`Take`, `startExecution`) | `Dispatcher.StartOne`, при `LeaseKeeper.Unregister` = `true` | `abandoned` |
| исход сдан после остановки sink'а | `CompletionSink.Complete` / `EnqueueRegistered` при `_stopped` (только при успешном переходе) | `stopped` |
| ack-буфер отменённой остановки; отправка прервана отменённой остановкой | `CompletionSink.DiscardAckBufferLocked`, `CompletionSink.Abandon` при отменённом `_abort` | `stopped` |
| бюджет `LeaseDuration` на отправку исчерпан | `CompletionSink.Abandon` | `send_timeout` |

Не учитываются: `CompletionSink.Complete` с состоянием не `Running` и `BeginAttempt` над `Removed` (переход уже учтён `LeaseKeeper` или `AbandonRunning`); ответ `LeaseLost` / неполный ack после неудачной попытки (исход, вероятно, уже применён — сейчас это лог Information).

**`storage.errors`.** Одно увеличение на каждое перехваченное исключение, независимо от того, пропустил ли его лог ограничитель (`_statsErrors` и др.):

| `storage.operation` | Место |
|---|---|
| `dequeue` | `Dispatcher`: ошибка `FetchAsync` |
| `ack` | `CompletionSink.SendAckBatchAsync`: неудачная попытка |
| `outcome` | `CompletionSink.SendSingleAsync` (retry, fail, confirm-cancel, release): неудачная попытка |
| `heartbeat` | `LeaseKeeper`: ошибка или таймаут heartbeat, ошибка `RegisterAgainAsync` |
| `settings` | `QueueSettingsCache`: ошибка обновления |
| `stats_flush` | `OutcomeStatsFlusher`: ошибка сброса |
| `stats_read` | `ProcessingMaintenance`: ошибка `ReadQueueStatsAsync` (§4.4) |
| `promoter`, `sweeper`, `resolver`, `scheduler`, `recurring-sync`, `housekeeping`, `stats` | `ProcessingMaintenance.LoopAsync`: исключение итерации цикла (имя цикла) |

Исключения задач обслуживания внутри `RunTaskAsync` учитываются в `maintenance.runs` с `result = error`, не здесь.

### 4.3 Кластерные метрики (одинаковы на всех подах, агрегация — `max`)

| Метрика | Тип, единица | Теги | Источник |
|---|---|---|---|
| `pgbolt.cluster.queue.jobs` | ObservableGauge, `{job}` | queue, job.state = `scheduled` / `enqueued` / `processing` / `awaiting` | `ClusterQueueStats` (§4.4): при свежем снимке — все четыре состояния для каждой известной очереди (очереди зарегистрированных серверов пода ∪ строки снимка), без строки — нули; при устаревшем — пустой набор |
| `pgbolt.cluster.queue.oldest_ready_age` | ObservableGauge, `s` | queue | `ClusterQueueStats`: при свежем снимке — только очереди с готовыми задачами (`OldestReadySeconds` не `NULL`); при устаревшем — пустой набор |
| `pgbolt.cluster.queue.limit` | ObservableGauge, `{worker}` | queue | `QueueSettingsCache.Current.Limits`: эффективный кластерный лимит; очереди без лимита не публикуются, пауза — 0 |

Префикс `pgbolt.cluster.` — сигнал «не суммировать по подам». Гистограммы и счётчики кластерных метрик не вводятся. Публикуют только поды с зарегистрированными серверами (§4.1); известные очереди разных подов различаются, но значение каждого ряда у всех публикующих его подов одно и то же, поэтому `max` корректен. Расхождение между подами — не больше одного прохода цикла `stats` (для `limit` — одного обновления `QueueSettingsCache`), критерий §1.

`limit` не устаревает: при недоступной БД публикуется последнее прочитанное значение — то же, по которому работает dequeue (`QueueSettingsCache` оставляет прошлый снимок при ошибке обновления); после остановки серверов пода (§4.1) не публикуется. Это фиксируется в документации.

### 4.4 `ClusterQueueStats`

Singleton DI в `PgBolt.Server` (регистрирует `AddPgBoltServer`). Хранит последний прочитанный снимок: строки `IReadOnlyList<QueueStatsRow>` и момент последнего успешного снимка `snapshotAt` по `MonoClock` = момент получения − `AgeSeconds` (`null` при `AgeSeconds = null`). Источник — `ReadQueueStatsAsync` (§7.2): `QueueStatsSnapshot(IReadOnlyList<QueueStatsRow> Rows, double? AgeSeconds)`, `QueueStatsRow(Queue, Scheduled, Enqueued, Processing, Awaiting, OldestReadySeconds)`.

Свежесть — как у dashboard (часть 6 §4.2, §6.4): по строке `stats` в `maintenance_tasks`, а не по наличию строк `queue_stats` (в простое таблица пуста). `AgeSeconds` — возраст последнего успешного снимка на часах PG, не меньше 0:
- момент снимка — `greatest(last_finished_at строки stats при last_error IS NULL, max(queue_stats.measured_at))`: проход без ошибки — его `last_finished_at` (пустой снимок такой же свежий, как непустой); проход, упавший после коммита снимка (ошибка `TrimStatsAsync`), — `measured_at` закоммиченных строк, поэтому ошибка trim снимок не теряет; проход, упавший на самом снимке (откат), — `measured_at` предыдущего снимка;
- `AgeSeconds = null` — строки `stats` нет, или последний проход завершился ошибкой, а `queue_stats` пуст: момент последнего успешного снимка неизвестен, снимок считается устаревшим до следующего успешного прохода.

Кто публикует: каждый под с серверами, независимо от `RunMaintenance`:
- `PgBoltProcessingService` создаёт `ProcessingMaintenance` при `_servers.Count > 0` (сейчас — только при `RunMaintenance`, `RunScheduler` или синхронизации recurring), цикл `stats` запускается всегда (сейчас — только при `RunMaintenance = true`), интервал и jitter прежние;
- проход: при `RunMaintenance = true` сначала задача `stats` части 6 без изменений (захват, `SnapshotQueueStatsAsync`, `TrimStatsAsync`, `CompleteMaintenanceAsync`); затем при любом `RunMaintenance` — `ReadQueueStatsAsync` и замена снимка `ClusterQueueStats` прочитанным. Владелец публикует тем же чтением после `CompleteMaintenanceAsync` — путь публикации один на всех подах. Исключение задачи `stats` вне `RunTaskAsync` (захват, `CompleteMaintenanceAsync`), кроме отмены остановкой, чтение не пропускает: чтение выполняется, затем исключение уходит в цикл и логируется, как в части 6. Под с `RunMaintenance = false` задачу `stats` не захватывает;
- снимок устарел, если `snapshotAt` равен `null` или `clock.Elapsed(snapshotAt) > 4 × StatsSnapshotInterval`: callbacks `jobs` и `oldest_ready_age` возвращают пустой набор — ряд в Prometheus пропадает, а не застывает, и `absent()` означает «снимок устарел или его нет», а не «очередь пуста». Возраст считается от момента снимка на стороне PG, поэтому чтение таблицы, которую владелец перестал обновлять, не продлевает ряд. Порог 4 ×: проход владельца — раз в 0,8–1,2 интервала, чтение пода — раз в 0,8–1,2 интервала, поэтому в норме возраст публикуемого снимка — до ~2,4 × интервал; одна неудачная попытка снимка даёт до ~3,6 × и ряд не скрывает, две подряд — скрывают. Порог считается по `StatsSnapshotInterval` читающего пода, а частоту снимков задаёт владелец, поэтому `StatsSnapshotInterval` должен быть одинаковым на всех подах кластера (требование документации §9; при меньшем интервале на читателе ряды периодически пропадают);
- свежий снимок: `jobs` — четыре состояния для каждой известной очереди (§4.3: очереди зарегистрированных серверов пода ∪ строки снимка), очередь без строки — нули; `oldest_ready_age` — только строки с `OldestReadySeconds` не `NULL`;
- ошибка чтения — лог существующим ограничителем `_statsErrors` (раз в минуту), `storage.errors` с `stats_read`, снимок не заменяется и продолжает стареть по `snapshotAt`; исключение не выходит из прохода. Ошибка захвата и снимка — как в части 6.

Под без серверов (только клиент) кластерных метрик не публикует.

### 4.5 Бакеты гистограмм

`InstrumentAdvice<double>.HistogramBucketBoundaries` из `PgBoltDiagnostics.HistogramBuckets` (§3.1):
- `job.duration`, `job.wait_time` — `Job`: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600;
- `maintenance.duration` — `Maintenance`: 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300.

`PgBolt.OpenTelemetry` повторяет их View'ами (§6.4) из тех же констант — для SDK, не учитывающих advice.

Кардинальность: число рядов `job.duration` — типы × очереди × 5 исходов на под. OTel .NET SDK по умолчанию держит 2000 рядов на метрику и сверх них пишет overflow-ряд; `PgBolt.OpenTelemetry` лимит не меняет, документация (§9) описывает расчёт и `MetricStreamConfiguration.CardinalityLimit` во View пользователя для больших инсталляций.

### 4.6 Ошибки в callbacks

Observable-callbacks читают только память (`SlotAllocator`, `QueueSettingsCache.Current`, `ClusterQueueStats`) без I/O и без блокировок дольше копирования. Исключение callback'а ловится, `PgBoltInstrumentation` пишет событие 8002 `MetricCallbackFailed` не чаще раза в минуту (§8), callback возвращает пустой набор.

## 5. Логи

### 5.1 Жизненный цикл задачи (категория `PgBolt.Job`)

Событие выбирается только по исходу классификатора (`JobOutcome` и его `Kind`); причина отмены — поле `Reason` (`Execution.FirstReason`: `None`, `Timeout`, `HeartbeatLost`, `LeaseLost`, `Shutdown`). `OutcomeClassifier` превращает `Timeout`, `HeartbeatLost`, `LeaseLost` и `Shutdown` с не-`OperationCanceledException` в `Retry` / `Fail`, поэтому отдельного события «таймаут / потеря lease» нет.

| `Name` | Исход | Уровень | Исключение | Поля |
|---|---|---|---|---|
| `JobStarted` | старт попытки | Debug | — | JobId, JobType, Queue, Attempt |
| `JobSucceeded` | `Ack` | Debug | — | JobId, DurationMs |
| `JobFailedWillRetry` | `Retry`, `Kind = Retry` | Warning | ошибка попытки | JobId, Attempt, MaxAttempts, NextAttemptIn, Reason |
| `JobFailed` | `Fail`, `Kind = Failed` | Error | ошибка попытки | JobId, Attempt, MaxAttempts, Reason |
| `JobNoExecutorWillRetry` | `Retry`, `Kind = NoExecutor` | Warning | `JobExecutorNotFoundException` | JobId, JobType, NextAttemptIn |
| `JobNoExecutorFailed` | `Fail`, `Kind = NoExecutor` | Error | `JobExecutorNotFoundException` | JobId, JobType |
| `JobCancelled` | `ConfirmCancel` | Information | ошибка попытки | JobId |
| `JobReleased` | `Release` | Debug | — | JobId, Ran |

События пишутся внутри span'а и scope (§3.3 п. 2–4), кроме `JobReleased` с `Ran = false` (п. 1). Для `Reason = LeaseLost` исход не применится (`outcome.dropped`), событие фиксирует результат попытки. Существующие логи `LeaseKeeper` (запрос отмены — Information, неподтверждённое продление и потеря lease — Warning) и `CompletionSink` (исход отброшен) остаются с прежними текстами: они описывают lease и доставку исхода, а не результат попытки, и не дублируют события таблицы.

### 5.2 Scope задачи

`JobRunner` оборачивает выполнение в `_jobLogger.BeginScope(new JobLogScope(job, queue))`. `JobLogScope` — `sealed class : IReadOnlyList<KeyValuePair<string, object?>>` с полями `JobId`, `JobType`, `Queue`, `Attempt` и `ToString()` для текстовых провайдеров. Аллокации scope на задачу — объект `JobLogScope` и, при провайдерах с `IExternalScopeProvider`, узел, который `LoggerFactoryScopeProvider.Push` создаёт на каждый `BeginScope`; это и есть исключение «кроме scope логов» критерия §1. Scope виден логам кода задачи через любой `ILogger` (провайдеры с `IExternalScopeProvider`). `TraceId` / `SpanId` добавляет провайдер из `Activity.Current` (OTel logs exporter, `ActivityTrackingOptions`) — только при созданном consumer-span'е; без слушателя источника `PgBolt` логи задачи `TraceId` не получают (критерий §1).

### 5.3 `[LoggerMessage]` и `EventId`

Все существующие вызовы `logger.LogX(...)` в `src/` переводятся на `[LoggerMessage]` в `static partial class Log` рядом с компонентом; тексты сообщений и категории не меняются. Диапазоны `EventId`:

| Диапазон | Компонент |
|---|---|
| 1000–1999 | жизненный цикл сервера и процесса (`PgBoltProcessingService`, `PgBoltServer`, настройки очередей) |
| 2000–2999 | диспетчер |
| 3000–3999 | жизненный цикл задачи (§5.1), retry policy |
| 4000–4999 | `CompletionSink`, `LeaseKeeper`, `OutcomeStatsFlusher` |
| 5000–5999 | обслуживание, retention, recurring tick, resolver |
| 6000–6999 | клиент, `PgBoltInitializer`, recurring sync |
| 7000–7999 | dashboard |
| 8000–8999 | телеметрия: 8001 `TelemetryFailed`, 8002 `MetricCallbackFailed` (§8) |

Каждое событие получает явные уникальные `Id` (не 0) и `Name` (`PascalCase`, например `JobFailedWillRetry`); тест проверяет уникальность по всем сборкам и попадание `Id` в диапазон компонента своего `Log`-класса. Полная таблица событий генерируется в плане и публикуется в `docs/observability.md` как контракт: `Id` и `Name` не переиспользуются и не меняются.

## 6. Компоненты

### 6.1 `PgBolt.Abstractions`
- `PgBoltDiagnostics` (§3.1) с вложенными `Instruments` и `HistogramBuckets` — единственный новый публичный тип ядра; источник имён и бакетов для runtime и `PgBolt.OpenTelemetry`.

### 6.2 `PgBolt.Postgres`
- `PgBoltTelemetry` (internal): `ActivitySource`, константы имён атрибутов, хелперы старта / завершения producer-span'а и вычисления `trace_parent` (с защитой §8).
- `PgBoltInstrumentation` (internal, singleton DI): `Meter` (§4.1), `ReportFailure(Exception)` и `ReportCallbackFailure(Exception)` — события 8001 / 8002 категории `PgBolt.Telemetry` (§8).
- `PgBoltClientMetrics` (internal, singleton DI): `pgbolt.job.enqueued`.
- `PgBoltJobClient`, `WorkflowBuilder`, `RecurringJobManager`: producer-span, счётчик; `RecurringJobManager.TriggerAsync` пишет `trace_parent` по §3.2.
- Хранилище: `wait_seconds` в dequeue (`LeasedJob.WaitSeconds`), колонка `oldest_ready_seconds` в снимке `queue_stats`, `QueueStatsRow`, `QueueStatsSnapshot`, `ReadQueueStatsAsync`, лимит в активных настройках (§7).
- `PgBoltInitializer.EnsureNotPreReleaseSchemaAsync`: проверка колонки `queue_stats.oldest_ready_seconds` (§7.2).

### 6.3 `PgBolt.Server`
- `PgBoltMetrics` (internal, singleton DI, регистрирует `AddPgBoltServer`): все instruments §4.2–§4.3; observable-источники (`SlotAllocator` и ёмкость серверов — регистрация / снятие, `QueueSettingsCache`, `ClusterQueueStats`).
- `ClusterQueueStats` (internal, singleton DI, §4.4).
- `JobRunner`: сброс `Activity.Current`, consumer-span, `job.wait_time`, `job.duration`, логи §5.1, scope §5.2 — в порядке §3.3 и с защитой §8; создаёт `JobExecutorNotFoundException`.
- `OutcomeClassifier.NoExecutor(Execution, JobExecutorNotFoundException, RetryDecider)` — исключение передаётся снаружи.
- `PgBoltServer`: регистрация источников метрик в `Start`, снятие в `finally` `FlushAndStopAsync` (§4.1); `AbandonRunning` — `outcome.dropped`; `RunExecutionAsync` не бросает (§8).
- `Dispatcher`: `storage.errors`; `outcome.dropped` при откате `StartOne` (§4.2).
- `CompletionSink`, `LeaseKeeper`: `job.outcome.dropped`, `storage.errors` (§4.2).
- `ProcessingMaintenance`: цикл `stats` на любом поде с серверами, чтение `ReadQueueStatsAsync` и публикация в `ClusterQueueStats` каждый проход, `maintenance.*`, `storage.errors` (§4.2, §4.4). `PgBoltProcessingService`: условие создания `ProcessingMaintenance` (§4.4).
- `QueueSettingsCache`, `OutcomeStatsFlusher`: `storage.errors`; `QueueSettingsSnapshot(FrozenDictionary<string, int> Limits, FrozenDictionary<string, int> Weights)`, `DequeueFetchStrategy` — по `Limits` (§7.3). `RetentionReport.Write`: `retention.deferred`.
- `IServerStore`, `PgBoltServerStore`: новый `Task<QueueStatsSnapshot> ReadQueueStatsAsync(CancellationToken)`; `SnapshotQueueStatsAsync` без изменения сигнатуры. Тесты: `FakeServerStore` (новый метод, `ActiveQueueSetting`), построение `QueueSettingsSnapshot` в `DispatcherTests` и `FetchStrategyTests`, `ActiveQueueSetting` в `QueueSettingsCacheTests`, `tests/PgBolt.Server.Tests/Integration/QueueLimitTests.cs` и `tests/PgBolt.Postgres.Tests/QueueSettingsSyncTests.cs`.
- `Log`-классы `[LoggerMessage]`.

### 6.4 `PgBolt.OpenTelemetry` (новый пакет)

Зависимости — только `PgBolt.Abstractions` и пакет `OpenTelemetry` (SDK, нужен для `AddView`); на `PgBolt.Postgres` и `PgBolt.Server` пакет не ссылается и их internals не использует. Имена источника, `Meter`, гистограмм и границы бакетов — `PgBoltDiagnostics` (§3.1): `AddSource(ActivitySourceName)`, `AddMeter(MeterName)`, View на `Instruments.JobDuration` и `Instruments.JobWaitTime` с `ExplicitBucketHistogramConfiguration { Boundaries = [.. HistogramBuckets.Job] }`, на `Instruments.MaintenanceDuration` — с `HistogramBuckets.Maintenance`.

```csharp
public static class PgBoltOpenTelemetryExtensions
{
    public static TracerProviderBuilder AddPgBoltInstrumentation(this TracerProviderBuilder builder);   // AddSource
    public static MeterProviderBuilder AddPgBoltInstrumentation(this MeterProviderBuilder builder);     // AddMeter + View'ы бакетов §4.5
}
```

## 7. Изменения хранилища

### 7.1 Dequeue
`RETURNING` CTE `l` в `DequeueTemplate` и `DequeueLimitedTemplate` (часть 3b §6.2) дополнительно возвращает общий фрагмент `greatest(0, extract(epoch FROM clock_timestamp() - q.run_at))::double precision AS wait_seconds`; `SelectLeasedJobs` выбирает `l.wait_seconds` последним столбцом, `ReadLeasedJob` читает его. `RETURNING` отдаёт новую строку, `run_at` lease не меняет. `clock_timestamp()`, а не `now()`: в dequeue с лимитом `now()` — начало неявной транзакции батча, до ожидания advisory lock, а `run_at`, поставленный trigger'ом или resolver'ом во время ожидания, позже него; `greatest(0, …)` страхует остальные случаи `run_at` в будущем. Время считается на стороне PG — расхождение часов подов не влияет.

`LeasedJob` (публичный позиционный record с опциональными хвостовыми параметрами) получает последний параметр `double WaitSeconds = 0`; существующие вызовы конструктора не меняются.

Семантика: время с `run_at` до lease. Для немедленных задач `run_at` — момент enqueue, для отложенных и повторов — плановое время (включает лаг promoter'а), для продолжений и trigger — момент перевода в готовые (resolver и trigger ставят `run_at = now()`). Requeue ставит `run_at = now()`. Release и sweeper `run_at` не меняют, поэтому `wait_time` повторного lease включает время прошлой попытки; это фиксируется в документации.

### 7.2 Снимок `queue_stats`
- `queue_stats` получает колонку `oldest_ready_seconds double precision` (`NULL` — нет готовых задач) последней, после `measured_at`: тесты dashboard вставляют в `queue_stats` шесть значений позиционно (`OverviewPagesTests`, `JobPagesTests`, `QueueSettingsActionsTests`, `PlanFixture`, `OverviewStoreTests`) и остаются без изменений; вносится в `V001__initial.sql`. Все запросы к `queue_stats` перечисляют колонки явно.
- `SnapshotQueueStatsTemplate` (сигнатура `SnapshotQueueStatsAsync` и итоговый `DELETE` прежние, добавлена колонка):

```sql
WITH s AS (
    SELECT queue,
           count(*) FILTER (WHERE lease_owner IS NULL AND NOT ready AND await_condition IS NULL) AS scheduled,
           count(*) FILTER (WHERE lease_owner IS NULL AND ready)                                 AS enqueued,
           count(*) FILTER (WHERE lease_owner IS NOT NULL)                                       AS processing,
           count(*) FILTER (WHERE await_condition IS NOT NULL)                                   AS awaiting,
           extract(epoch FROM now() - min(run_at) FILTER (WHERE lease_owner IS NULL AND ready))::double precision
                                                                                                 AS oldest_ready_seconds
    FROM {schema}.queue GROUP BY queue
), up AS (
    INSERT INTO {schema}.queue_stats (queue, scheduled, enqueued, processing, awaiting, oldest_ready_seconds, measured_at)
    SELECT s.*, now() FROM s ORDER BY s.queue
    ON CONFLICT (queue) DO UPDATE SET scheduled = EXCLUDED.scheduled, enqueued = EXCLUDED.enqueued,
        processing = EXCLUDED.processing, awaiting = EXCLUDED.awaiting,
        oldest_ready_seconds = EXCLUDED.oldest_ready_seconds, measured_at = EXCLUDED.measured_at
)
DELETE FROM {schema}.queue_stats q WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.queue = q.queue)
```

- `ReadQueueStatsAsync` — один statement (одно MVCC-представление строк и строки `stats`):

```sql
WITH f AS (
    SELECT greatest(CASE WHEN t.last_error IS NULL THEN t.last_finished_at END,
                    (SELECT max(measured_at) FROM {schema}.queue_stats)) AS at
    FROM (SELECT 1) one LEFT JOIN {schema}.maintenance_tasks t ON t.name = 'stats'
)
SELECT q.queue, q.scheduled, q.enqueued, q.processing, q.awaiting, q.oldest_ready_seconds,
       CASE WHEN f.at IS NOT NULL THEN greatest(0, extract(epoch FROM now() - f.at))::double precision END AS age_seconds
FROM f LEFT JOIN {schema}.queue_stats q ON true
```

  `greatest` в PostgreSQL пропускает `NULL`, поэтому `at` — последний известный успешный снимок (§4.4), `NULL` — неизвестен. `greatest(0, …)` применяется только к известному моменту (`greatest(0, NULL)` дал бы 0) и страхует `last_finished_at` транзакции, закоммиченной после начала этой. Пустой `queue_stats` → одна строка с `queue = NULL`, у которой читается только `age_seconds`. Результат — `QueueStatsSnapshot(Rows, AgeSeconds)`; `QueueStatsRow` и `QueueStatsSnapshot` — internal record'ы `PgBolt.Postgres.Storage`, как `OutcomeStatsRow`; `PgBoltStore.ReadQueueStatsAsync` — internal, как `SnapshotQueueStatsAsync`.
- `extract` возвращает `numeric` (PostgreSQL 14+), поэтому все секунды §7 приводятся к `double precision`.
- `PgBoltInitializer.EnsureNotPreReleaseSchemaAsync` дополнительно требует колонку: `NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass(@schema || '.queue_stats') AND attname = 'oldest_ready_seconds' AND NOT attisdropped)` — то же исключение о схеме прошлой сборки.
- Dashboard новую колонку не читает; его выборки `queue_stats` перечисляют колонки явно (`DashboardStore.Overview`).

### 7.3 Активные настройки очередей
`GetActiveQueueSettingsAsync` возвращает значение эффективного лимита (`lim`) вместо `lim IS NOT NULL`; в `ActiveQueueSetting` поле `bool Limited` заменяется на `int? Limit`; `QueueSettingsSnapshot` получает `FrozenDictionary<string, int> Limits` вместо `FrozenSet<string> Limited`; `DequeueFetchStrategy` проверяет `Limits.ContainsKey(queue)` вместо `Limited.Contains(queue)`.

## 8. Ошибки

- **Защита точек телеметрии.** `ActivityListener` (`Sample`, `ActivityStarted`, `ActivityStopped`), измерительные callbacks `MeterListener` и логгер-провайдеры вызываются BCL синхронно, их исключения не перехватываются (`Logger` собирает исключения провайдеров в `AggregateException`). Поэтому каждая точка телеметрии 7a — в отдельном `try/catch`:
  - `JobRunner`: старт span'а с атрибутами; `BeginScope`; каждое событие §5.1; `job.wait_time`, `job.duration`; атрибуты исхода и статус; `Dispose` scope; `Stop` span'а. Сбой старта или остановки span'а → `Activity.Current = null` и продолжение без span'а; сбой `BeginScope` → без scope; прочее → сигнал пропускается. Выполнение исполнителя и классификация исхода от телеметрии не зависят;
  - клиент: старт и завершение producer-span'а (сбой → восстановление прежнего `Activity.Current`, при старте — span как `null`, §3.2), исход вызова не меняется;
  - каждая запись метрики — внутри метода `PgBoltMetrics` / `PgBoltClientMetrics`;
  - observable-callbacks — §4.6.

  Сбой → `PgBoltInstrumentation.ReportFailure(e)`: событие 8001 `TelemetryFailed` (категория `PgBolt.Telemetry`, Warning с исключением), не чаще раза в минуту на контейнер по `TimeProvider` контейнера; вызов логгера в нём тоже в `try/catch`, его сбой глотается.
- **Последний рубеж — `PgBoltServer.RunExecutionAsync`, не бросает никогда.** Сейчас исключение `RunAsync` попадает в catch с `LogCritical`, слот не освобождается, `CompletionSink.Complete` не вызывается, запись остаётся `Running`, и `LeaseKeeper` продлевает lease до рестарта; исключение самого `LogCritical` (бросающий провайдер) или `SlotAllocator.Release` (`InvalidOperationException` при ошибке учёта) уходит в `tracked.Task`, `WaitExecutionsAsync` его пробрасывает (ловит только `TimeoutException` и отмену), и `PgBoltProcessingService.StopAsync` падает до `FlushAndStopAsync` — LeaseKeeper и sink не останавливаются. После 7a порядок:
  1. `RunAsync`. Исключение → сначала `LeaseKeeper.Unregister(execution)` (не бросает; `Running → Removed`, удаление из таблицы — heartbeat не продлевает lease, он истечёт, sweeper засчитает попытку, как у брошенного выполнения) и при `true` — `outcome.dropped` с `abandoned` (метод `PgBoltMetrics`, защита §8); только потом существующий `LogCritical` (текст прежний) в собственном `try/catch`, сбой лога глотается. Ни один лог не стоит перед снятием записи.
  2. В `finally` п. 1 — `_slots.Release` и `_dispatcher.OnSlotFreed` в одном `try/catch`, ровно один раз при успехе и при исключении; исключение → новое событие `SlotReleaseFailed` (Error, диапазон 1000–1999: «PgBolt server {Server}: slot of queue {Queue} was not returned after job {JobId} finished.») в собственном `try/catch`.
  3. Только при исходе из п. 1 — `_sink.Complete(execution, outcome)` в `try/catch`; исключение обрабатывается как в п. 1 (`Unregister` → `outcome.dropped` при `true` → `LogCritical` под `try/catch`).
  4. Внешний `finally` — `tracked.Registration.DisposeAsync()` и `_running.TryRemove` (не бросают: callback регистрации — один `Interlocked`).

  Порядок «слот, затем `Complete`» на успешном пути прежний. `tracked.Task` всегда завершается успешно, поэтому `WaitExecutionsAsync` и остановка процесса от исключения выполнения не зависят.
- Битый `trace_parent` → корневой span (§3.3).
- Callbacks — §4.6. Устаревший снимок — §4.4.
- Ошибка `ReadQueueStatsAsync` не влияет на задачу `stats` и runtime (§4.4).

## 9. Документация

- `docs/observability.md`: подключение (`AddPgBoltInstrumentation` или `AddSource` / `AddMeter` по `PgBoltDiagnostics`), таблицы spans, атрибутов, метрик, `EventId`; правила агрегации (`sum` / `max`) и допуск расхождения кластерных метрик (§4.3); свежесть кластерного снимка (§4.4: нули при свежем снимке, пустой набор при устаревшем, порог 4 × `StatsSnapshotInterval`, одинаковый `StatsSnapshotInterval` на всех подах) и то, что `limit` не устаревает (§4.3); требование `service.instance.id` (§4.1); кардинальность и `CardinalityLimit` (§4.5); семантика `job.enqueued` (откаченный транзакционный enqueue учитывается, тик — в `recurring.fired`) и `wait_time` (после release / sweeper включает прошлую попытку); requeue в исходном трейсе (§3.2); PromQL:
  - пропускная способность: `sum by (pgbolt_queue, pgbolt_job_outcome) (rate(pgbolt_job_duration_seconds_count[5m]))`;
  - p99 выполнения и ожидания: `histogram_quantile(0.99, sum by (le, pgbolt_queue) (rate(pgbolt_job_wait_time_seconds_bucket[5m])))`;
  - насыщение пула: `sum by (pgbolt_queue) (pgbolt_worker_active) / on() group_left sum(pgbolt_worker_capacity)`;
  - насыщение кластерного лимита: `sum by (pgbolt_queue) (pgbolt_worker_active) / max by (pgbolt_queue) (pgbolt_cluster_queue_limit)`;
  - застрявшая очередь: `max by (pgbolt_queue) (pgbolt_cluster_queue_oldest_ready_age_seconds) > 300`;
  - потеря lease: `sum(rate(pgbolt_job_outcome_dropped_total{pgbolt_drop_reason="lease_lost"}[5m])) > 0`;
  - снимок `queue_stats` устарел (ни один под с `RunMaintenance = true` не делает снимок или снимок стабильно падает): `absent(pgbolt_cluster_queue_jobs)` при живых подах с серверами — в простое ряд есть с нулями, поэтому алерт пустую очередь не путает с устаревшим снимком.
- Roadmap: часть 7 → 7a / 7b, статус 7a; worker §1 — пометка о пересмотре «встраивается фильтром».
- CHANGELOG: Added — трассировка, метрики, `EventId`, `PgBoltDiagnostics` (имена источника, `Meter`, instruments, бакеты), `PgBolt.OpenTelemetry`, `LeasedJob.WaitSeconds`, `queue_stats.oldest_ready_seconds`; Changed — `trace_parent` пишет id producer-span'а, `IRecurringJobManager.TriggerAsync` пишет `trace_parent` (было `NULL`), цикл `stats` работает на любом поде с серверами, исключение выполнения освобождает слот и lease; Changed (ломающее) — публичный `PgBolt.Postgres.Storage.ActiveQueueSetting`: `bool Limited` заменён на `int? Limit` (эффективный лимит, пауза — 0; `Limited` = `Limit is not null`).

## 10. Изменения других частей

- Core: `LeasedJob.WaitSeconds` (последний параметр, default 0); `queue_stats.oldest_ready_seconds` и проверка колонки в `PgBoltInitializer`; `trace_parent` — id producer-span'а (§3.2).
- Worker: OTel-инструментирование встроено в `JobRunner`, а не фильтром (worker §1, §3.6); `OutcomeClassifier.NoExecutor` принимает исключение; `PgBoltServer.RunExecutionAsync` не бросает: снимает запись до любого лога, освобождает слот и при исключении `RunAsync` (§8); `Dispatcher.StartOne` при откате учитывает `outcome.dropped`.
- Часть 3b: `ActiveQueueSetting.Limit` вместо `Limited`, `QueueSettingsSnapshot.Limits`; `wait_seconds` в dequeue с лимитом.
- Часть 4: `IRecurringJobManager.TriggerAsync` пишет `trace_parent` (§3.2, было `NULL`). Пункт §9 части 4 «trace для срабатывания — новый корневой span» реализуется так: `trace_parent` тика остаётся `NULL`, consumer-span поставленной тиком задачи — корень нового трейса (§3.3); отдельного span'а тика нет.
- Часть 6: `queue_stats` с новой колонкой (`SnapshotQueueStatsAsync` пишет её, сигнатура прежняя); цикл `stats` (§4.2 части 6) запускается на любом поде с серверами — захват и снимок только при `RunMaintenance = true`, затем на каждом поде чтение `ReadQueueStatsAsync` со свежестью по строке `stats` в `maintenance_tasks`, по тому же правилу, что dashboard §6.4 (§4.4). Dashboard не меняется.

## 11. Тестирование

**Unit (`PgBolt.Server.Tests`, `PgBolt.Postgres.Tests` без БД):**
- consumer-span через `ActivityListener`: родитель из `trace_parent` (удалённый); пустой и битый `trace_parent` → корень, в том числе при ambient `Activity.Current` вызывающего потока (не её ребёнок); атрибуты, статус и `error.type` для каждого `JobOutcome` и причин `Timeout` / `HeartbeatLost` / `LeaseLost`; `pgbolt.job.parent_job_id` / `pgbolt.job.parent_batch_id` для продолжений задачи и batch'а, ни одного — для члена batch-продолжения (код 5); `NoExecutor` с событием `JobExecutorNotFoundException`; исключение, вышедшее из п. 3 §3.3, — span остановлен, scope закрыт; задача, не запущенная из-за shutdown, — без span'а; `Activity.Current` внутри исполнителя — consumer-span; события §5.1 исхода несут `TraceId` / `SpanId` consumer-span'а (записаны до `Stop`);
- защита §8: `ActivityListener`, бросающий в `ActivityStarted` и в `ActivityStopped`, `MeterListener`, бросающий в измерительном callback'е, логгер-провайдер, бросающий на событиях §5.1 и на `BeginScope`, → тот же исход, слот освобождён, одно событие 8001 за минуту (`FakeTimeProvider`), `Activity.Current` после старта со сбоем — `null`; `job.duration` пишется и без `ActivityListener`;
- `RunExecutionAsync`: логгер-провайдер, бросающий на всех событиях всех категорий, включая `LogCritical` (исключение `RunAsync` — из существующего предупреждения об урезанном таймауте в `TimeoutOf`) → задача `tracked` завершена без исключения, слот освобождён, диспетчер разбужен, запись `Removed` и снята из таблицы (heartbeat её не продлевает), `outcome.dropped` с `abandoned` ровно один раз; провайдер начинает бросать после `PgBoltServer.Start` (незащищённый лог «started» иначе уронит старт), остановка: `WaitExecutionsAsync` завершается без исключения, `LeaseKeeper` и sink остановлены (`StopAsync` после этого может бросить из незащищённого существующего лога «stopped» — поведение до 7a, вне scope); `Dispatcher.StartOne` с бросающим `startExecution` → `outcome.dropped` с `abandoned`;
- метрики через `MetricCollector<T>` (`Microsoft.Extensions.Diagnostics.Testing`): `job.duration` с outcome; `job.wait_time` пишется при старте и не пишется для release без запуска; `job.enqueued` — workflow с узлами в двух очередях даёт `Add` на каждую тройку `(queue, job.type, result)`; `job.outcome.dropped` — каждая строка таблицы §4.2 и отсутствие двойного учёта (`Complete` после потери lease, `BeginAttempt` над `Removed`); `storage.errors` — каждая строка таблицы §4.2; `worker.active` / `capacity` на настоящем `SlotAllocator` (класс `sealed`), снятие источников сервера после остановки, в том числе при отменённом `ct` `FlushAndStopAsync`, после чего `cluster.*` (и `limit`) не публикуются; `cluster.*` из `ClusterQueueStats` и `QueueSettingsCache`; свежий пустой снимок (`Rows` пуст, `AgeSeconds` = 0) → `jobs` с нулями четырёх состояний для очередей серверов пода, `oldest_ready_age` пуст; очередь со строкой в снимке, но без сервера на поде, — тоже публикуется; `oldest_ready_age` только для строк с `OldestReadySeconds`; устаревание (`FakeTimeProvider`): `AgeSeconds = null` → пустой набор; `AgeSeconds` + прошедшее время > 4 × интервал → пустой набор, ≤ 4 × — снимок публикуется; ошибка чтения не продлевает снимок; исключение в callback'е → пустой набор и одно событие 8002;
- `Meter`: два контейнера в одном процессе — `MetricCollector` с `meterScope` = `IMeterFactory` одного видит только его измерения; без `IMeterFactory` собственный `Meter` освобождается с контейнером, callbacks после `Dispose` не вызываются;
- цикл `stats` (`FakeServerStore`): под с серверами и `RunMaintenance = false` не захватывает задачу и публикует результат `ReadQueueStatsAsync` каждый проход; владелец вызывает `ReadQueueStatsAsync` после `CompleteMaintenanceAsync` того же прохода; исключение захвата чтение не пропускает и затем логируется циклом; ошибка чтения → `stats_read`, снимок прежний; `ProcessingMaintenance` создаётся при серверах и всех флагах `false`;
- логи через `ListLogger`: каждая строка таблицы §5.1 (исход × `Kind`, `Reason`) — `Name`, уровень, исключение; scope с полями задачи виден логу из исполнителя;
- `EventId`: уникальность `Id` и `Name` по всем сборкам `src/`, явный `Id` (не 0) у каждого `LoggerMessageAttribute`, `Id` в диапазоне компонента своего `Log`-класса (рефлексия);
- существующие тесты, противоречащие новому поведению и меняемые планом: `ProcessingServiceTests.Maintenance_runs_only_when_enabled` и `Flusher_runs_without_maintenance` (`Assert.Null(service.Maintenance)` при серверах и всех флагах `false` → `ProcessingMaintenance` создан, цикл `stats` только читает), `StatsTaskTests.Stats_loop_runs_only_with_RunMaintenance` (→ чтение без захвата) и тесты `StatsTaskTests` с точной последовательностью вызовов `FakeServerStore` (+ `ReadQueueStats`), `FetchStrategyTests` (построение `QueueSettingsSnapshot` с `Limits`).

**Postgres (Testcontainers):**
- producer-span и запись его id в `trace_parent` для enqueue (в т. ч. транзакционного), workflow submit, recurring trigger; без слушателя — id `Activity.Current`, без него — `NULL`;
- `wait_seconds` dequeue и dequeue с лимитом (немедленная, отложенная, после trigger); `wait_seconds ≥ 0`, когда trigger задачи коммитится, пока dequeue с лимитом ждёт advisory lock;
- снимок `queue_stats` пишет `oldest_ready_seconds` (`NULL` без готовых задач), upsert его обновляет, ушедшие очереди удаляются; `ReadQueueStatsAsync`: строки снимка; строки `stats` нет → `AgeSeconds = null`; успешный проход над пустой `queue` → пустые `Rows` и `AgeSeconds` от `last_finished_at`; `last_error` после закоммиченного снимка (ошибка trim) → `AgeSeconds` от `measured_at`; `last_error` над пустым `queue_stats` → `AgeSeconds = null`;
- `PgBoltInitializer`: `queue_stats` без `oldest_ready_seconds` → исключение о схеме прошлой сборки;
- `GetActiveQueueSettingsAsync` возвращает лимит, паузу (0).

**`PgBolt.OpenTelemetry.Tests` (новый проект, без БД, `OpenTelemetry.Exporter.InMemory`):** `TracerProviderBuilder.AddPgBoltInstrumentation` — span источника `PgBolt` доходит до экспортёра; `MeterProviderBuilder.AddPgBoltInstrumentation` — измерения `Meter` `PgBolt` доходят, границы бакетов `job.duration`, `job.wait_time`, `maintenance.duration` равны §4.5 при instruments без advice (View применён). Проект ссылается только на `PgBolt.OpenTelemetry` (и через него на `PgBolt.Abstractions`); измерения создаёт тестовый `Meter` с именами из `PgBoltDiagnostics`.

**E2E:**
- корневая `Activity` → enqueue → выполнение на сервере: один `TraceId`, цепочка parent root → producer → consumer → span из кода задачи;
- два хоста (два «пода») на одной БД, `MetricCollector` с `meterScope` = `IMeterFactory` каждого хоста: после смены владельца снимка и прохода цикла `stats` каждого хоста `pgbolt.cluster.*` совпадают на обоих; в простое (пустая `queue`) оба публикуют `pgbolt.cluster.queue.jobs` с нулями; `worker.capacity` аддитивна.

**Бенчмарк (`benchmarks/`):** `JobRunnerBenchmarks` (сейчас нет) создаётся первой задачей плана до изменений 7a, и его baseline записывается в `results/`; после 7a — без слушателей, со слушателями `ActivitySource` и `Meter`: аллокации и время против baseline. Место — новый проект `benchmarks/PgBolt.Server.Benchmarks` (Exe, BenchmarkDotNet, `IsPackable=false`, `IsTestProject=false`, в `PgBolt.slnx` в папке `/benchmarks/`, отчёты в `benchmarks/PgBolt.Server.Benchmarks/results/`) с `ProjectReference` на `src/PgBolt.Server`; `PgBolt.Server.csproj` и `PgBolt.Postgres.csproj` получают `InternalsVisibleTo Include="PgBolt.Server.Benchmarks"` (`JobRunner`, `Execution`, `FilterPipeline`, `RetryDecider`, `MonoClock`; `JobRegistry.TryAdd`). `PgBolt.Benchmarks` не меняется: его принцип — копии ядра в `Copies/` без `InternalsVisibleTo`, а копии `Execution`, `ExecutionTable`, `JobOutcome` с глобальным `using PgBolt.Server.Runtime` при ссылке на `PgBolt.Server` дали бы неоднозначные имена; копировать `JobRunner` с зависимостями (`JobRegistry`, `FilterPipeline`, `RetryDecider`, `OutcomeClassifier`) нельзя — бенчмарк должен мерить продуктовый код.

## 12. Риски

- Длинные трейсы (ретраи через час, отложенные задачи, requeue спустя дни) хуже отображаются в Tempo и рвутся хвостовым сэмплированием — следствие выбранной связи «родитель»; пересмотр — опцией `Link` позже, без смены формата `trace_parent`.
- Пользователь, суммирующий `pgbolt.cluster.*`, получит N-кратное значение — снижается префиксом и документацией.
- Кластерные метрики подов расходятся до одного прохода цикла `stats`; `max` в этом окне берёт большее из двух соседних снимков — допуск задокументирован.
- Окно устаревания `4 × StatsSnapshotInterval` (от последнего успешного снимка по строке `stats` в `maintenance_tasks`) при долгой недоступности БД, двух и более неудачных снимках подряд или отсутствии пода с `RunMaintenance = true` скрывает `pgbolt.cluster.queue.jobs` и `oldest_ready_age` целиком — так и задумано: `absent()` означает «снимок устарел», в простое ряд остаётся с нулями. Последний проход с ошибкой над пустым `queue_stats` скрывает их до следующего успешного прохода (момент последнего успешного снимка неизвестен). `limit` не устаревает (§4.3).
- Защита §8 скрывает ошибку пользовательского слушателя от выполнения задачи; видна по событию 8001 (раз в минуту).
- Перевод ~90 вызовов логов — механическая, но широкая правка; риск — случайно изменённые тексты; тесты существующих логов (`ListLogger`) ловят изменения шаблонов.
