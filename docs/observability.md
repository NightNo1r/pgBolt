# Observability: трассировка, метрики, логи

PgBolt отдаёт стандартные сигналы для стека OpenTelemetry → Prometheus / Tempo / Loki:

- **трассировка**: producer-span при постановке задачи и consumer-span её выполнения в одном трейсе;
- **метрики**: метрики пода (аддитивные) и кластерные (`pgbolt.cluster.*`), готовые к `histogram_quantile`;
- **логи**: события жизненного цикла задачи, scope задачи и стабильные `EventId` для алертов.

Ядро использует только BCL (`System.Diagnostics.ActivitySource` и `System.Diagnostics.Metrics.Meter`) и не зависит от OpenTelemetry SDK;
без подписчиков телеметрия почти ничего не стоит (см. [Стоимость](#стоимость)).

- [Подключение](#подключение)
- [Трассировка](#трассировка)
- [Метрики](#метрики)
- [Кластерные метрики и агрегация](#кластерные-метрики-и-агрегация)
- [Кардинальность](#кардинальность)
- [Логи](#логи)
- [Примеры PromQL](#примеры-promql)
- [Стоимость](#стоимость)

## Подключение

### Пакет `PgBolt.OpenTelemetry`

Пакет зависит от `PgBolt.Abstractions` и OpenTelemetry SDK 1.15.3 (пакет `OpenTelemetry`). Версия поднята с 1.12.0 из-за уязвимости
GHSA-g94r-2vxg-569j в `OpenTelemetry.Api` 1.12.0. На `PgBolt.Postgres` и `PgBolt.Server` пакет не ссылается.

```csharp
builder.Services.AddOpenTelemetry()                               // пакет OpenTelemetry.Extensions.Hosting
    .ConfigureResource(r => r.AddService("orders-worker"))        // даёт service.instance.id, см. ниже
    .WithTracing(t => t.AddPgBoltInstrumentation().AddOtlpExporter())
    .WithMetrics(m => m.AddPgBoltInstrumentation().AddOtlpExporter());
```

- `TracerProviderBuilder.AddPgBoltInstrumentation()` подписывает провайдер на источник `PgBolt`;
- `MeterProviderBuilder.AddPgBoltInstrumentation()` подписывает на `Meter` `PgBolt` и задаёт View'ами границы бакетов гистограмм
  (для SDK, не учитывающих advice instrument'а).

### Без пакета

Имена источника и `Meter` — константы `PgBoltDiagnostics` из `PgBolt.Abstractions`:

```csharp
tracing.AddSource(PgBoltDiagnostics.ActivitySourceName);   // "PgBolt"
metrics.AddMeter(PgBoltDiagnostics.MeterName);             // "PgBolt"
```

Имена instruments (`PgBoltDiagnostics.Instruments`) — тоже публичные константы. Границы бакетов (`PgBoltDiagnostics.HistogramBuckets.Job`,
`.Maintenance`) — не константы, а публичные статические свойства только для чтения (`IReadOnlyList<double>`).

### Meter и `IMeterFactory`

`Meter` создаётся один на DI-контейнер. Если в контейнере есть `IMeterFactory`, `Meter` берётся у него (`Host.CreateApplicationBuilder` и
`Host.CreateDefaultBuilder` регистрируют фабрику сами, иначе вызовите `services.AddMetrics()`); без фабрики PgBolt создаёт собственный
`Meter` и освобождает его вместе с контейнером. Статического `Meter` нет, поэтому несколько хостов в одном процессе не смешивают измерения.

### Идентификатор пода: `service.instance.id`

Метрики пода аддитивны, а кластерные одинаковы на всех подах — отличить поды друг от друга можно только атрибутом ресурса
`service.instance.id`. OTel .NET SDK ставит его через `ResourceBuilder.AddService(...)` (по умолчанию `autoGenerateServiceInstanceId: true`)
или через переменную окружения `OTEL_RESOURCE_ATTRIBUTES`; в `ResourceBuilder.CreateDefault()` его нет. Без него Prometheus-совместимые
бэкенды склеят измерения разных подов в один ряд.

## Трассировка

### Spans

| Span | Kind | Когда создаётся |
|---|---|---|
| `enqueue {job.type}` | Producer | вызов `IJobClient.EnqueueAsync` / `EnqueueManyAsync` (все перегрузки, в том числе динамическая по имени и транзакционные) |
| `workflow submit` | Producer | `Workflow()….SubmitAsync()` |
| `recurring trigger` | Producer | `IRecurringJobManager.TriggerAsync` |
| `{job.type}` | Consumer | одна попытка выполнения задачи в `JobRunner` |

Producer-span создаётся на один вызов клиентского API, а не на задачу. Ошибка вызова даёт статус `Error`, `error.type` (полное имя типа
исключения) и событие исключения. Транзакционный enqueue, откаченный вызывающим кодом, оставляет span без задачи.

Каждая попытка выполнения — отдельный consumer-span под тем же родителем. Задача, не запущенная из-за остановки сервера
(`Release` до старта), span'а не получает.

### Связь выполнения с постановкой

Колонка `jobs.trace_parent` хранит `traceparent` (только W3C; `tracestate` и baggage не передаются):

- если у вызова есть producer-span, пишется его id; consumer-span выполнения становится его **ребёнком** (удалённым), а не `ActivityLink`:
  весь путь HTTP → enqueue → выполнение → вызовы из кода задачи (HttpClient, Npgsql) лежит в одном трейсе. Цена — трейсы с ретраями
  через час и отложенными задачами длинные по времени, их хуже показывает Tempo и рвёт хвостовое сэмплирование;
- без producer-span'а (нет слушателя, сэмплер отказал, родитель не W3C) пишется id `Activity.Current`, если он W3C, иначе `NULL`;
- `IRecurringJobManager.TriggerAsync` пишет `trace_parent` по тому же правилу (раньше писал `NULL`);
- задачи, поставленные тиком планировщика, получают `trace_parent = NULL`: тик — фоновая работа без контекста, consumer-span такой задачи
  — корень нового трейса;
- продолжения workflow получают `trace_parent` span'а `workflow submit` (одинаковый у всех узлов);
- `IJobClient.RequeueAsync` `trace_parent` **не меняет**: повторное выполнение, как и ретрай, — ребёнок исходного producer-span'а в
  исходном трейсе.

Пустой или не разбирающийся `trace_parent` даёт корневой consumer-span без ошибки и без записи в лог. Перед стартом consumer-span'а runtime
сбрасывает `Activity.Current`: задача без `trace_parent` не становится ребёнком чужой долгоживущей `Activity`, под которой запущен хост.
Внутри задачи `Activity.Current` — её consumer-span.

### Атрибуты span'ов

| Атрибут | Producer | Consumer | Значение |
|---|---|---|---|
| `messaging.system` | да | да | `pgbolt` |
| `messaging.operation.type` | да | да | `send` / `process` |
| `messaging.destination.name` | да | да | очередь (для workflow с несколькими очередями у producer-span'а не ставится) |
| `messaging.message.id` | — | да | id задачи |
| `messaging.batch.message_count` | при задач > 1 | — | число задач вызова |
| `pgbolt.job.type` | enqueue | да | тип задачи |
| `pgbolt.job.attempt` | — | да | номер попытки |
| `pgbolt.job.max_attempts` | — | да | максимум попыток |
| `pgbolt.server.name` | — | да | имя виртуального сервера |
| `pgbolt.batch.id` | — | при наличии | id batch'а, в который входит задача |
| `pgbolt.job.parent_job_id` | — | для продолжения задачи | id родительской задачи |
| `pgbolt.job.parent_batch_id` | — | для продолжения batch'а | id родительского batch'а |
| `pgbolt.recurring.id` | trigger | при наличии | id recurring-задачи |
| `pgbolt.job.outcome` | — | после выполнения | `succeeded` / `retry` / `failed` / `cancelled` / `released` |
| `error.type` | при ошибке | при `retry` / `failed` | см. ниже |

Для исходов `retry` и `failed` consumer-span получает статус `Error` и событие исключения. `error.type` — `timeout` / `heartbeat_lost` /
`lease_lost`, если попытка прервана по соответствующей причине, иначе полное имя типа исключения; для задачи без зарегистрированного
исполнителя — `PgBolt.Server.JobExecutorNotFoundException`.

## Метрики

Метрики пода аддитивны: **`sum` по подам** даёт значение кластера. Кластерные (`pgbolt.cluster.*`) публикуют одно и то же значение с
каждого пода: **`max`** по подам.

Имя в Prometheus — после преобразования OTel → Prometheus (точки в подчёркивания, единица `s` — суффикс `_seconds`, счётчики — `_total`);
точный вид зависит от экспортёра и настроек перевода имён.

### Метрики пода (агрегация `sum`)

| Метрика | Тип, единица | Теги | Что считает |
|---|---|---|---|
| `pgbolt.job.enqueued` | Counter, `{job}` | `pgbolt.queue`, `pgbolt.job.type`, `pgbolt.enqueue.result` = `created` / `duplicate` | задачи, поставленные клиентом: enqueue, workflow submit, recurring `TriggerAsync` |
| `pgbolt.recurring.fired` | Counter, `{job}` | — | задачи, поставленные тиком планировщика |
| `pgbolt.job.duration` | Histogram, `s` | `pgbolt.queue`, `pgbolt.job.type`, `pgbolt.job.outcome` | длительность попытки выполнения |
| `pgbolt.job.wait_time` | Histogram, `s` | `pgbolt.queue`, `pgbolt.job.type` | время от `run_at` до lease |
| `pgbolt.job.outcome.dropped` | Counter, `{job}` | `pgbolt.queue`, `pgbolt.drop.reason` = `lease_lost` / `abandoned` / `stopped` / `send_timeout` | исходы, которые не были применены |
| `pgbolt.worker.active` | ObservableUpDownCounter, `{worker}` | `pgbolt.server.name`, `pgbolt.queue` | занятые слоты очереди |
| `pgbolt.worker.capacity` | ObservableUpDownCounter, `{worker}` | `pgbolt.server.name` | ёмкость виртуального сервера: `Workers` и сумма `Reserved` |
| `pgbolt.storage.errors` | Counter, `{error}` | `pgbolt.storage.operation` | перехваченные ошибки обращения к БД |
| `pgbolt.maintenance.runs` | Counter, `{run}` | `pgbolt.maintenance.task`, `pgbolt.maintenance.result` = `ok` / `error` / `deferred` | запуски задач обслуживания |
| `pgbolt.maintenance.duration` | Histogram, `s` | `pgbolt.maintenance.task` | длительность запуска задачи обслуживания |
| `pgbolt.retention.deferred` | Counter, `{partition}` | `pgbolt.retention.reason` | партиции, которые retention не смог обработать за этот проход |

Prometheus-имена: `pgbolt_job_enqueued_total`, `pgbolt_recurring_fired_total`, `pgbolt_job_duration_seconds_{bucket,sum,count}`,
`pgbolt_job_wait_time_seconds_*`, `pgbolt_job_outcome_dropped_total`, `pgbolt_worker_active`, `pgbolt_worker_capacity`,
`pgbolt_storage_errors_total`, `pgbolt_maintenance_runs_total`, `pgbolt_maintenance_duration_seconds_*`, `pgbolt_retention_deferred_total`.
Теги становятся метками с подчёркиваниями: `pgbolt_queue`, `pgbolt_job_outcome` и так далее.

**`pgbolt.job.enqueued`.** Значение — число строк. На один вызов приходится по одной записи на тройку (очередь, тип, результат): узлы
workflow могут лежать в разных очередях. Workflow submit считает все узлы как `created` (конфликт ключа бросает исключение, вызов не
учитывается). Транзакционный enqueue учитывается при возврате из вызова, **даже если вызывающий затем откатит транзакцию**. Задачи тика
планировщика сюда не входят — их считает `pgbolt.recurring.fired`.

**`pgbolt.job.wait_time`.** Считается от `run_at` до lease: для немедленных задач `run_at` — момент enqueue, для отложенных и повторов —
плановое время (включая лаг promoter'а), для продолжений workflow и trigger — момент перевода в готовые, `Requeue` ставит `run_at = now()`.
Release и sweeper `run_at` не меняют, поэтому **`wait_time` повторного lease после release или sweeper'а включает время прошлой попытки**.
Задачи, взятые, но отданные без запуска (остановка сервера, потеря heartbeat во время dequeue, dequeue после своего таймаута), в метрику
не попадают: их ожидание войдёт в следующий lease. Время считается на стороне PostgreSQL — расхождение часов подов не влияет.

**`pgbolt.job.duration`.** Пишется и без слушателя источника трассировки; `pgbolt.job.outcome` — `succeeded` / `retry` / `failed` /
`cancelled` / `released`.

**`pgbolt.job.outcome.dropped`.** Исход, который runtime не смог применить:

| `pgbolt.drop.reason` | Причина |
|---|---|
| `lease_lost` | lease потерян во время выполнения или с исходом в очереди отправки; хранилище ответило, что lease потерян; ack подтвердил не все lease пакета |
| `abandoned` | выполнение не остановилось за `ShutdownTimeout`; исключение `JobRunner.RunAsync` или `CompletionSink.Complete`; сбой запуска выполнения после регистрации |
| `stopped` | исход сдан после остановки отправки или отправка прервана отменённой остановкой |
| `send_timeout` | бюджет `LeaseDuration` на отправку исхода исчерпан |

Каждая запись выполнения учитывается не более одного раза. Не учитывается ответ «lease потерян» после неудачной попытки отправки: исход,
вероятно, уже применён.

**`pgbolt.storage.errors`.** По одному увеличению на каждое перехваченное исключение:

| `pgbolt.storage.operation` | Место |
|---|---|
| `dequeue` | диспетчер: ошибка выборки задач |
| `ack` | пакетный ack исходов |
| `outcome` | отправка retry / fail / confirm-cancel / release |
| `heartbeat` | ошибка или таймаут heartbeat, ошибка повторной регистрации сервера |
| `settings` | обновление настроек очередей |
| `stats_flush` | сброс статистики исходов |
| `stats_read` | чтение снимка `queue_stats` |
| `table_stats_read` | чтение статистики таблиц `queue` и `completions` |
| `promoter`, `sweeper`, `resolver`, `scheduler`, `recurring-sync`, `housekeeping`, `stats` | исключение итерации цикла обслуживания (имя цикла) |

Ошибки внутри задач обслуживания считаются в `pgbolt.maintenance.runs` с `result = error`, не здесь. `result = deferred` — задача
вернула ошибку без исключения (например, `lock_timeout` при создании партиций). `pgbolt.maintenance.runs` и
`pgbolt.maintenance.duration` считают только запуски, которые под реально захватил.

**Бакеты гистограмм** фиксированы и одинаковы на всех подах — поэтому `histogram_quantile` по `sum by (le)` корректен:

- `pgbolt.job.duration`, `pgbolt.job.wait_time`, секунды: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600;
- `pgbolt.maintenance.duration`, секунды: 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300.

## Кластерные метрики и агрегация

| Метрика | Тип, единица | Теги | Что публикует |
|---|---|---|---|
| `pgbolt.cluster.queue.jobs` | ObservableGauge, `{job}` | `pgbolt.queue`, `pgbolt.job.state` = `scheduled` / `enqueued` / `processing` / `awaiting` | число задач очереди по состояниям |
| `pgbolt.cluster.queue.oldest_ready_age` | ObservableGauge, `s` | `pgbolt.queue` | возраст самой старой готовой, но не взятой задачи |
| `pgbolt.cluster.queue.limit` | ObservableGauge, `{worker}` | `pgbolt.queue` | эффективный кластерный лимит конкурентности очереди (пауза — 0); очереди без лимита не публикуются |
| `pgbolt.cluster.table.dead_tuples` | ObservableGauge, `{tuple}` | `pgbolt.table` = `queue` / `completions` | число мёртвых строк таблицы (`pg_stat_user_tables.n_dead_tup`) |
| `pgbolt.cluster.table.size` | ObservableGauge, `By` | `pgbolt.table` | полный размер таблицы с индексами и TOAST (`pg_total_relation_size`) |

Prometheus-имена: `pgbolt_cluster_queue_jobs`, `pgbolt_cluster_queue_oldest_ready_age_seconds`, `pgbolt_cluster_queue_limit`,
`pgbolt_cluster_table_dead_tuples` (единица `{tuple}` суффикса не даёт), `pgbolt_cluster_table_size_bytes`; тег таблицы — метка `pgbolt_table`.

Префикс `pgbolt.cluster.` означает «не суммировать по подам»: значение каждого ряда одно и то же у всех подов, которые его публикуют,
поэтому агрегируйте **`max`** (суммирование даёт N-кратное значение). Публикуют только поды с зарегистрированными серверами
(`AddPgBoltServer`); под, который только ставит задачи, и под после остановки своих серверов кластерных метрик не публикует.

### Откуда берутся значения и допуск расхождения

`jobs` и `oldest_ready_age` берутся из снимка `queue_stats`. Задача `stats` делает снимок на поде с `RunMaintenance = true`; цикл `stats`
**каждого** пода с серверами раз в `StatsSnapshotInterval` читает таблицу `queue_stats` вместе с моментом последнего успешного снимка.
Поэтому:

- значения разных подов расходятся не больше чем на один проход цикла `stats` (до 1,2 × `StatsSnapshotInterval` от коммита снимка);
  `max` в этом окне берёт большее из двух соседних снимков;
- смена пода, делающего снимок, не даёт дублей и пропусков: публикует каждый под независимо;
- `limit` берётся из кэша настроек очередей и отстаёт не больше чем на одно обновление `QueueSettingsRefreshInterval` (до 1,1 ×
  интервала, jitter ±10 %).

### Свежесть снимка

Свежесть определяет момент последнего успешного снимка на стороне PostgreSQL (строка задачи `stats` в `maintenance_tasks`, уточнённая
`measured_at` строк снимка) плюс время с момента получения:

- **снимок свежий** (не старше 4 × `StatsSnapshotInterval`): `pgbolt.cluster.queue.jobs` публикуется для каждой известной очереди со всеми
  четырьмя состояниями, и очереди без строк получают **нули** — в простое (когда `queue_stats` пуст) ряд есть и равен нулю;
  `oldest_ready_age` публикуется только для очередей с готовыми задачами;
- **снимок устарел** (старше 4 × `StatsSnapshotInterval`, момент снимка неизвестен или ни один под с `RunMaintenance = true` его не
  обновляет): `jobs` и `oldest_ready_age` возвращают **пустой набор** — ряд пропадает, а не застывает. `absent()` поэтому означает
  «снимок устарел или его нет», а не «очередь пуста».

Порог 4 × выбран так, что одна неудачная попытка снимка ряд не скрывает (возраст публикуемого снимка в норме до ~2,4 × интервал, при одной
неудаче до ~3,6 ×), а две подряд — скрывают. Известные очереди — это очереди серверов пода и очереди, у которых есть строка в снимке.

**`StatsSnapshotInterval` должен быть одинаковым на всех подах кластера.** Частоту снимков задаёт владелец задачи `stats`, а порог
устаревания каждый под считает по собственному значению; при меньшем интервале на читающем поде ряды будут периодически пропадать.

### Статистика таблиц

`dead_tuples` и `size` показывают bloat `queue` и `completions`: растущее число мёртвых строк при стабильной нагрузке означает, что
autovacuum не успевает или его удерживает старая транзакция (xmin). Значения читает тот же цикл `stats` каждого пода с серверами, в том
числе при `RunMaintenance = false`. Если успешного чтения не было дольше 4 × `StatsSnapshotInterval` (БД недоступна), оба ряда
пропадают, а не застывают; ошибка чтения считается в `pgbolt.storage.errors` с `pgbolt.storage.operation = table_stats_read`.
`n_dead_tup` — оценка статистики PostgreSQL, обновляется с задержкой; агрегируйте `max`, как и остальные кластерные метрики:

```promql
max by (pgbolt_table) (pgbolt_cluster_table_dead_tuples)
max by (pgbolt_table) (pgbolt_cluster_table_size_bytes)
```

Алерты на `queue` (пороги — по сценарию `vacuum` нагрузочного стенда при 10 000 задач/с,
[docs/performance.md](performance.md#vacuum); при меньшем потоке пропорционально ниже). Мёртвые строки не убираются — autovacuum
не успевает или его удерживает старая транзакция, с `for: 10m`:

```promql
max(pgbolt_cluster_table_dead_tuples{pgbolt_table="queue"}) > 1000000
```

При работающем autovacuum `n_dead_tup` `queue` растёт пилой и падает после каждого прохода: пик 1,28–2,41 млн при
`autovacuum_naptime` 1 мин, выше 1 млн подряд — не дольше 72 с, поэтому `for: 10m` пилу не ловит. При удержании xmin счёт после
первого прохода autovacuum растёт без спадов (с `autovacuum_naptime` 1 мин этот проход приходит через ~30 с удержания и сбрасывает
счёт до 0,29–0,37 млн) и пересекает 1 млн через 2,6–3 мин удержания. Прогоны удерживали xmin 5 мин, и этот алерт в них не
сработал: ожидаемое срабатывание — около 13 мин удержания при 10 000/с. Раньше удержание ловит алерт размера ниже.

Таблица растёт — больше 500 МБ за час:

```promql
delta(max(pgbolt_cluster_table_size_bytes{pgbolt_table="queue"})[1h:]) > 524288000
```

Под стационарной нагрузкой (20 мин) размер `queue` ходит пилой: с `autovacuum_naptime` 1 мин в `bouncer` — 135–259 и 142–262 МБ,
размах до 124 МБ, в остальных прогонах — до 42 МБ; это предел `delta` любого окна внутри замера. Минимумы пилы в `bouncer`
поднимаются на 36–43 МБ за 10 мин (135 → 158 → 178 и 142 → 162 → 178 МБ); если так пойдёт и дальше, за час — ~220–260 МБ тренда
плюс размах пилы, ~340–380 МБ, ниже порога 500 МБ. Окно длиннее 20 мин не измерялось: если алерт срабатывает без долгих
транзакций, проверьте autovacuum `queue` или поднимите порог. За 5 мин удержания xmin размер вырос на 324–511 МБ: 500 МБ сверх
минимума стационарной фазы набраны к концу удержания или примерно через 20–25 с после него, сверх её максимума — не позже чем
через 3 мин после конца удержания. После удержания размер растёт и в фазе восстановления (пик 755–893 МБ); в 3 из 6
прогонов к концу её autovacuum вернул его к 385–498 МБ, в остальных он остался на пике. Алерт держится, пока прирост за последний
час выше порога.

### `limit` не устаревает

`pgbolt.cluster.queue.limit` не пропадает при недоступной БД: публикуется последнее прочитанное значение — то же, по которому работает
dequeue (`QueueSettingsCache` при ошибке обновления сохраняет прошлый снимок). Метрика перестаёт публиковаться только после остановки
серверов пода.

## Кардинальность

Теги метрик ограничены конечными множествами: типы задач, очереди, имена виртуальных серверов, задачи обслуживания. Id задач, batch'ей
и серверов в теги не попадают.

Самая многочисленная метрика — `pgbolt.job.duration`: число рядов на под — число пар (очередь, тип задачи), по которым выполнялись попытки, умноженное на 5
исходов. OTel .NET SDK по умолчанию держит до 2000 рядов на метрику и сверх них пишет overflow-ряд. Для
инсталляций с сотнями типов задач посчитайте число рядов по формуле выше и при необходимости поднимите `CardinalityLimit`.

`MeterProviderBuilder.AddPgBoltInstrumentation()` уже добавляет View'ы бакетов на `pgbolt.job.duration`, `pgbolt.job.wait_time` и
`pgbolt.maintenance.duration`; второй View на тот же instrument создаст **вторую, дублирующую метрику**. Чтобы поднять лимит, подпишитесь
без `AddPgBoltInstrumentation()` — границы бакетов instrument'ы передают сами:

```csharp
metrics
    .AddMeter(PgBoltDiagnostics.MeterName)
    .AddView(PgBoltDiagnostics.Instruments.JobDuration, new MetricStreamConfiguration { CardinalityLimit = 5000 })
    .AddView(PgBoltDiagnostics.Instruments.JobWaitTime, new MetricStreamConfiguration { CardinalityLimit = 5000 });
```

## Логи

### События жизненного цикла задачи

Категория `PgBolt.Job`. Событие выбирается по исходу попытки; причина прерывания — поле `Reason` (`None`, `Timeout`, `HeartbeatLost`,
`LeaseLost`, `Shutdown`): отдельного события «таймаут» или «потеря lease» нет, они приходят как `JobFailedWillRetry` / `JobFailed`.

События перечислены в таблице раздела «3000–3099: Жизненный цикл задачи» ниже; в ней у каждого события указан исход.

Исключение каждой неуспешной попытки (`Retry`, `Fail`, подтверждённая отмена) попадает в лог вместе с `JobId` и `EventId`. Для
`Reason = LeaseLost` исход не применится (см. `pgbolt.job.outcome.dropped`), событие фиксирует результат попытки. Логи `LeaseKeeper` и
`CompletionSink` описывают lease и доставку исхода и не дублируют эти события.

### Scope задачи и корреляция с трейсом

Выполнение задачи обёрнуто в scope логгера с полями `JobId`, `JobType`, `Queue`, `Attempt`; он виден логам кода задачи через любой
`ILogger` (провайдеры с `IExternalScopeProvider`, текстовые провайдеры получают `ToString()`). `TraceId` и `SpanId` добавляет провайдер
логов из `Activity.Current` (OTel logs exporter или `ActivityTrackingOptions`) — **только если consumer-span создан**: нужен слушатель
источника `PgBolt`, и сэмплер не должен отказать. Без слушателя логи задачи `TraceId` не получают.

### Стабильные EventId

Все вызовы логов PgBolt используют `[LoggerMessage]` с явными `EventId` и `EventName`. `Id` и `Name` — контракт для алертов и фильтров:
они не переиспользуются и не меняются (набор зафиксирован тестом `EventIdContractTests`). Тексты сообщений и категории логгеров не
менялись. Диапазоны:

| Диапазон | Компонент |
|---|---|
| 1000–1999 | жизненный цикл сервера и процесса, настройки очередей |
| 2000–2999 | диспетчер |
| 3000–3999 | жизненный цикл задачи, retry policy |
| 4000–4999 | `CompletionSink`, `LeaseKeeper`, `OutcomeStatsFlusher` |
| 5000–5999 | обслуживание, retention, recurring, resolver |
| 6000–6999 | `PgBoltInitializer`, синхронизация recurring |
| 7000–7999 | dashboard |
| 8000–8999 | телеметрия |

### Сбои телеметрии

Исключение слушателя `ActivityListener` / `MeterListener` или логгер-провайдера в точке телеметрии PgBolt перехватывается: сигнал
пропускается, исход задачи не меняется, слот освобождается, остановка процесса проходит. О сбое сообщает событие 8001 `TelemetryFailed`
(сбой observable-callback'а — 8002 `MetricCallbackFailed`), не чаще раза в минуту на контейнер, категория `PgBolt.Telemetry`. Если в
логах появилось 8001, ищите ошибку в своих слушателях или провайдерах.

### Таблица событий

Ниже все события; `Level` — уровень по умолчанию. Для 5107 уровень выбирается во время выполнения: Warning, если партиции просрочены не
меньше чем на сутки, иначе Debug.

### 1000–1099: Процесс и хост (`PgBoltProcessingService`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 1000 | `JobTypeSchemaMismatch` | Warning | — | `PgBolt: job types {JobTypes} were published by another process with a different argument schema.` |
| 1001 | `ProcessingStartFailed` | Critical | да | `PgBolt: starting processing failed; stopping what was started.` |
| 1002 | `HostStopTimeoutServers` | Warning | — | `PgBolt: host stop timeout reached; pending outcomes are dropped and servers rows are left, leases will expire.` |
| 1003 | `HostStopTimeoutMaintenance` | Warning | — | `PgBolt: host stop timeout reached while stopping maintenance loops.` |
| 1004 | `QueueDefaultsConflict` | Warning | — | `PgBolt: queue '{Queue}' defaults were recently published by {PublishedBy} with other values; overwritten: {Changes}.` |
| 1005 | `HostShutdownTimeoutTooShort` | Warning | — | `PgBolt: HostOptions.ShutdownTimeout {HostTimeout} is shorter than {Required} (max server ShutdownTimeout + AckFlushInterval + 2 s); executions may be abandoned and their attempts counted.` |
| 1006 | `PartitionDaysAheadTooShort` | Warning | — | `PgBolt: PartitionDaysAhead ({DaysAhead} days) is shorter than 2 × PartitionsInterval ({Interval}); a single failed iteration may leave no partition for the next day.` |
| 1007 | `DeadServerTimeoutTooShort` | Warning | — | `PgBolt: DeadServerTimeout ({Timeout}) is shorter than 3 × HeartbeatInterval ({Heartbeat}); rows of live servers will be removed on short heartbeat hiccups and restored by the next heartbeat.` |

### 1100–1199: Виртуальный сервер (`PgBoltServer`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 1100 | `ServerStarted` | Information | — | `PgBolt server {Server} ({ServerId}) started: queues {Queues}, capacity {Capacity}.` |
| 1101 | `ExecutionsWaitCancelled` | Warning | — | `PgBolt server {Server}: waiting for executions was cancelled by the host.` |
| 1102 | `ServerStopCancelled` | Warning | — | `PgBolt server {Server}: stop was cancelled by the host; the servers row is left for cleanup.` |
| 1103 | `ServerRowRemoveFailed` | Error | да | `PgBolt server {Server}: failed to remove the servers row.` |
| 1104 | `ServerStopped` | Information | — | `PgBolt server {Server} ({ServerId}) stopped.` |
| 1105 | `DispatcherFailed` | Critical | да | `PgBolt server {Server}: dispatcher failed; stopping continues.` |
| 1106 | `CancellationCallbackFailed` | Warning | да | `PgBolt server {Server}: a cancellation callback of a job threw.` |
| 1107 | `JobTypesWithoutExecutor` | Warning | — | `PgBolt server {Server}: job types {JobTypes} default to its queues but have no executor in this process; such jobs go to NoExecutor.` |
| 1108 | `MissingExecutorsCheckFailed` | Warning | да | `PgBolt server {Server}: failed to check job types without executors.` |
| 1109 | `JobFinishFailed` | Critical | да | `PgBolt server {Server}: unexpected failure while finishing job {JobId}.` |
| 1110 | `JobAbandoned` | Warning | — | `PgBolt server {Server}: job {JobId} did not stop within {ShutdownTimeout} and is abandoned; its lease will expire and the sweeper will count the attempt.` |
| 1111 | `JobIgnoresCancellation` | Warning | — | `PgBolt server {Server}: job {JobId} has ignored cancellation ({Reason}) for {Elapsed}; its slot stays occupied.` |
| 1112 | `SlotReleaseFailed` | Error | да | `PgBolt server {Server}: slot of queue {Queue} was not returned after job {JobId} finished.` |

### 1200–1299: Настройки очередей (`QueueSettingsCache`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 1200 | `QueueSettingsRefreshFailed` | Warning | да | `PgBolt: refreshing queue settings failed; using the previous snapshot.` |

### 2000–2999: Диспетчер (`Dispatcher`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 2000 | `DequeueFailed` | Error | да | `PgBolt: dequeue from queue {Queue} failed; fetching paused for {Pause}.` |
| 2001 | `DequeueTimedOutJobsReleased` | Warning | — | `PgBolt: dequeue from queue {Queue} returned {Count} jobs after {Elapsed}, past its timeout {Timeout}; they are released without running.` |
| 2002 | `DispatcherIterationFailed` | Critical | да | `PgBolt: dispatcher iteration failed unexpectedly; fetching paused for {Pause}.` |
| 2003 | `WakeupSubscriptionDisposeFailed` | Warning | да | `PgBolt: disposing a wakeup subscription failed.` |
| 2004 | `WakeupSubscriptionFailed` | Error | да | `PgBolt: wakeup subscription for queue {Queue} failed; the queue is polled without wakeup signals.` |
| 2005 | `StartRollbackSlotReleaseFailed` | Error | да | `PgBolt: slot of queue {Queue} was not returned after the failed start of job {JobId}.` |
| 2006 | `JobStartRolledBack` | Warning | — | `PgBolt: job {JobId} of queue {Queue} was not started; its registration is rolled back, the lease will expire.` |

### 3000–3099: Жизненный цикл задачи (`JobRunner`, категория `PgBolt.Job`)

| Id | Name | Исход | Level | Исключение | Шаблон |
|---|---|---|---|---|---|
| 3001 | `JobStarted` | старт попытки | Debug | — | `PgBolt: job {JobId} ({JobType}) attempt {Attempt} started on queue {Queue}.` |
| 3002 | `JobSucceeded` | `Ack` | Debug | — | `PgBolt: job {JobId} succeeded in {DurationMs} ms.` |
| 3003 | `JobFailedWillRetry` | `Retry` | Warning | да | `PgBolt: job {JobId} attempt {Attempt} of {MaxAttempts} failed ({Reason}); retrying in {NextAttemptIn}.` |
| 3004 | `JobFailed` | `Fail` | Error | да | `PgBolt: job {JobId} failed at attempt {Attempt} of {MaxAttempts} ({Reason}); no attempts left.` |
| 3005 | `JobNoExecutorWillRetry` | нет исполнителя, `Retry` | Warning | да | `PgBolt: no executor is registered for job {JobId} of type {JobType}; retrying in {NextAttemptIn}.` |
| 3006 | `JobNoExecutorFailed` | нет исполнителя, `Fail` | Error | да | `PgBolt: no executor is registered for job {JobId} of type {JobType}; the job is failed.` |
| 3007 | `JobCancelled` | `ConfirmCancel` | Information | да | `PgBolt: job {JobId} was cancelled.` |
| 3008 | `JobReleased` | `Release` | Debug | — | `PgBolt: job {JobId} was released without spending an attempt (ran: {Ran}).` |

### 3100–3999: Retry policy и таймауты (`RetryDecider`, `JobRunner`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 3100 | `RetryPolicyFailed` | Error | да | `PgBolt: RetryPolicy threw for attempt {Attempt}; the job is failed without retry.` |
| 3101 | `JobTimeoutCapped` | Warning | — | `PgBolt: job '{JobName}' has TimeoutSeconds = {TimeoutSeconds}, above the .NET timer limit; its timeout is capped at {MaxJobTimeout}.` |

### 4000–4099: Отправка исходов (`CompletionSink`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 4000 | `OutcomeDiscardedSinkStopped` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink is stopped.` |
| 4001 | `OutcomeDiscardedNotRunning` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: execution is {State} (abandoned or lease lost).` |
| 4002 | `SinkStopCancelled` | Warning | — | `PgBolt: completion sink stop was cancelled; {Count} outcome send(s) aborted.` |
| 4003 | `AckDiscardedStopCancelled` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink stop was cancelled; its lease will expire.` |
| 4004 | `AckPartialAfterRetry` | Information | — | `PgBolt: ack affected {Acked} of {Count} jobs after a failed attempt; the rest was probably already applied.` |
| 4005 | `AckPartialLeaseLost` | Warning | — | `PgBolt: ack affected {Acked} of {Count} jobs; lease of the rest was lost.` |
| 4006 | `OutcomeLeaseNotFoundAfterRetry` | Information | — | `PgBolt: outcome {Outcome} of job {JobId}: lease not found after a failed attempt; the outcome was probably already applied.` |
| 4007 | `OutcomeDiscardedLeaseLost` | Warning | — | `PgBolt: outcome {Outcome} of job {JobId} discarded: lease was lost.` |
| 4008 | `OutcomeNotSentExecutionState` | Debug | — | `PgBolt: outcome of job {JobId} is not sent: execution is {State}.` |
| 4009 | `LeaseFreedNotificationFailed` | Error | да | `PgBolt: lease-freed notification for queue {Queue} failed.` |
| 4010 | `OutcomeSendFailedRetrying` | Warning | да | `PgBolt: sending outcome of {Count} job(s) failed; retrying in {Delay}.` |
| 4011 | `OutcomeNotSentStopCancelled` | Warning | да | `PgBolt: outcome of job {JobId} was not sent before stop was cancelled; its lease will expire.` |
| 4012 | `OutcomeSendBudgetExceeded` | Error | да | `PgBolt: outcome of job {JobId} was not sent within {Budget}; the execution is dropped, its lease will expire.` |

### 4100–4199: Lease и heartbeat (`LeaseKeeper`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 4100 | `LeaseRenewalNotConfirmed` | Warning | — | `PgBolt: lease renewal of job {JobId} (lease {LeaseId}) is not confirmed; cancelling the execution.` |
| 4101 | `HeartbeatTimedOut` | Warning | да | `PgBolt: heartbeat of server {ServerId} timed out after {Timeout}; {Count} leases not renewed.` |
| 4102 | `HeartbeatFailed` | Warning | да | `PgBolt: heartbeat of server {ServerId} failed; {Count} leases not renewed.` |
| 4103 | `JobCancellationRequested` | Information | — | `PgBolt: cancellation of job {JobId} requested; cancelling the execution.` |
| 4104 | `LeaseLostExecutionCancelled` | Warning | — | `PgBolt: lease {LeaseId} of job {JobId} is lost; the execution is cancelled and its outcome will be dropped.` |
| 4105 | `LeaseLostOutcomeDropped` | Warning | — | `PgBolt: lease {LeaseId} of job {JobId} is lost; pending outcome {Outcome} is dropped.` |
| 4106 | `HeartbeatRecovered` | Information | — | `PgBolt: heartbeat of server {ServerId} recovered.` |
| 4107 | `HeartbeatRecoveryCallbackFailed` | Error | да | `PgBolt: heartbeat recovery callback of server {ServerId} failed.` |
| 4108 | `ServerReRegistered` | Warning | — | `PgBolt: servers row of server {ServerId} was missing (removed by cleanup); registered again.` |
| 4109 | `ServerReRegistrationFailed` | Warning | да | `PgBolt: re-registering server {ServerId} failed; retrying on the next heartbeat.` |
| 4110 | `LeaseDeadlineCheckFailed` | Error | да | `PgBolt: lease deadline check of server {ServerId} failed.` |

### 4200–4299: Статистика исходов (`OutcomeStatsFlusher`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 4200 | `OutcomeStatsFlushFailed` | Warning | да | `PgBolt: flushing outcome statistics failed; values are kept for the next flush.` |

### 5000–5099: Обслуживание (`ProcessingMaintenance`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 5000 | `BatchCompletionNotCounted` | Debug | — | `PgBolt: completion of a member of batch {BatchId} is not counted: the batch row was removed by retention.` |
| 5001 | `ResolverLagging` | Warning | — | `PgBolt: the oldest workflow completion signal is {Age} old (ResolverInterval {Interval}); continuations are delayed. Check that a process with RunMaintenance = true is running and that no long user transaction holds workflow rows (After).` |
| 5002 | `RecurringFireFailed` | Error | — | `PgBolt: recurring job {Id} was not fired: {Error}; retrying in {Delay}.` |
| 5003 | `RecurringMisfireSkipped` | Warning | — | `PgBolt: recurring job {Id} skipped fires from {From:O} to {To:O} by its misfire policy.` |
| 5004 | `RecurringOverlapSkipped` | Debug | — | `PgBolt: recurring job {Id} fire skipped: previous run is still active.` |
| 5005 | `RecurringWaitingForPrevious` | Debug | — | `PgBolt: recurring job {Id} waits for its previous run (catch-up).` |
| 5006 | `RecurringExhausted` | Warning | — | `PgBolt: recurring job {Id} has no future occurrences and will not fire again.` |
| 5007 | `MaintenanceTaskFailed` | Error | да | `PgBolt: maintenance task {Task} failed.` |
| 5008 | `FuturePartitionsLockTimeout` | Warning | — | `PgBolt: future partitions were not created (lock_timeout); retrying in {Delay}.` |
| 5009 | `TodayPartitionsMissing` | Error | — | `PgBolt: today's partitions are missing and could not be created (lock_timeout); enqueue fails until they exist. Retrying in {Delay}.` |
| 5010 | `DeadServersRemoved` | Information | — | `PgBolt: removed {Count} servers rows without heartbeat for {Timeout}.` |
| 5011 | `MaintenanceLoopIterationFailed` | Error | да | `PgBolt: {Loop} iteration failed; retrying after the next interval.` |
| 5012 | `QueueStatsReadFailed` | Warning | да | `PgBolt: reading queue statistics failed; the previous snapshot is kept.` |
| 5013 | `TableStatsReadFailed` | Warning | да | `PgBolt: reading table statistics failed; the previous values are kept.` |

### 5100–5199: Retention (`RetentionReport`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 5100 | `RetentionPartitionsDropped` | Information | — | `PgBolt retention: dropped {JobsCount} jobs partitions [{JobsDays}], {EventsCount} job_events partitions [{EventsDays}] and {Remnants} compaction remnants.` |
| 5101 | `RetentionBatchesDeleted` | Information | — | `PgBolt retention: deleted {Count} finished batches rows older than JobsRetention.` |
| 5102 | `RetentionPinnedPartitions` | Information | — | `PgBolt retention: {Count} jobs partitions are pinned by live jobs: {Compacted} compacted now, {Small} left as is (few terminal rows), {TooMany} not compactable (more than {Max} live jobs), {Replicated} skipped (logical replication); {Live} live jobs in total, oldest day {Oldest}.` |
| 5103 | `RetentionPartitionTooManyLiveJobs` | Warning | — | `PgBolt retention: jobs partition {Day} holds {Live} live jobs, more than RetentionCompactionMaxLiveJobs ({Max}); it is not compacted and keeps its whole disk space until they finish (overdue by {Overdue} days). Run, cancel or delete these jobs, or raise the limit.` |
| 5104 | `RetentionCompactionSkippedReplication` | Warning | — | `PgBolt retention: compaction of {Count} pinned jobs partitions is skipped because jobs is in a logical replication publication; they keep their disk space until their live jobs finish (oldest day {Oldest}).` |
| 5105 | `RetentionTooManyAttachedPartitions` | Warning | — | `PgBolt retention: {Attached} jobs partitions are attached, normally {Normal}; days pinned by live jobs stay attached and slow down dequeue planning.` |
| 5106 | `RetentionHoldTimeout` | Warning | — | `PgBolt retention: holding the lock on {Table} hit statement_timeout or idle_in_transaction_session_timeout ({Count} partitions deferred); the hot path waited up to the hold limit. Likely a slow disk or a process pause (GC).` |
| 5107 | `RetentionDeferredByLockTimeout` | Warning / Debug | — | `PgBolt retention: {Count} {Table} partitions were deferred by lock_timeout (overdue by up to {Overdue} days); long queries or a holder of the partition itself (autovacuum, direct query) block their removal.` |
| 5108 | `RetentionRemnantDropFailed` | Error | — | `PgBolt retention: dropping the detached {Table} table of {Day} failed: {Error}; it will be retried as a remnant.` |

### 6000–6099: Инициализация схемы (`PgBoltInitializer`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 6000 | `SchemaVersionNewer` | Warning | — | `PgBolt: schema '{Schema}' has version {Version}, newer than this build knows ({Latest}); continuing because migrations are additive.` |
| 6001 | `StartupFuturePartitionsLockTimeout` | Warning | — | `PgBolt: future partitions were not created (lock_timeout); retrying in background.` |
| 6002 | `BackgroundPartitionCreationFailed` | Error | да | `PgBolt: background partition creation failed.` |

### 6100–6199: Синхронизация recurring

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 6100 | `RecurringJobInserted` | Information | — | `PgBolt: recurring job {Id} of owner {Owner} inserted.` |
| 6101 | `RecurringJobUpdated` | Information | — | `PgBolt: recurring job {Id} of owner {Owner} updated ({Fields}).` |
| 6102 | `RecurringJobDeleted` | Information | — | `PgBolt: recurring job {Id} of owner {Owner} deleted (no longer declared).` |
| 6103 | `RecurringJobOwnerCollision` | Error | — | `PgBolt: recurring job {Id} declared by owner {Owner} belongs to another owner or was created through the API; left untouched.` |
| 6104 | `RecurringSyncDeadlockRetry` | Warning | да | `PgBolt: recurring sync of owner {Owner} hit a deadlock (40P01); retrying the transaction (attempt {Attempt} of {MaxAttempts}).` |

### 7000–7099: Dashboard: проверка при старте

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 7000 | `ActionsPolicyNotRegistered` | Error | — | `PgBolt dashboard: ActionsPolicy '{Policy}' is not registered: every dashboard request fails until AddAuthorization defines it.` |
| 7001 | `AnonymousRemoteAccessAllowed` | Warning | — | `PgBolt dashboard allows anonymous remote access (AllowAnonymousRemote = true).` |
| 7002 | `LocalRequestsOnly` | Warning | — | `PgBolt dashboard is reachable only from local requests: add RequireAuthorization(...) to the dashboard group or set AllowAnonymousRemote.` |

### 7100–7199: Dashboard: действия и ошибки

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 7100 | `DashboardActionPerformed` | Information | — | `PgBolt dashboard: {User} {Action} {Target}: {Result}.` |
| 7101 | `DashboardRequestFailed` | Error | да | `PgBolt dashboard: {Method} {Path} failed.` |

### 8000–8999: Телеметрия (категория `PgBolt.Telemetry`)

| Id | Name | Level | Исключение | Шаблон |
|---|---|---|---|---|
| 8001 | `TelemetryFailed` | Warning | да | `PgBolt: telemetry failed; the signal is skipped.` |
| 8002 | `MetricCallbackFailed` | Warning | да | `PgBolt: metric callback failed; the measurement is skipped.` |

## Примеры PromQL

Имена меток — после преобразования OTel → Prometheus (`pgbolt.queue` → `pgbolt_queue`).

Пропускная способность по очередям и исходам:

```promql
sum by (pgbolt_queue, pgbolt_job_outcome) (rate(pgbolt_job_duration_seconds_count[5m]))
```

p99 ожидания (для выполнения — `pgbolt_job_duration_seconds_bucket`):

```promql
histogram_quantile(0.99, sum by (le, pgbolt_queue) (rate(pgbolt_job_wait_time_seconds_bucket[5m])))
```

Насыщение пула воркеров:

```promql
sum by (pgbolt_queue) (pgbolt_worker_active) / on() group_left sum(pgbolt_worker_capacity)
```

Насыщение кластерного лимита очереди (метрики пода — `sum`, кластерная — `max`):

```promql
sum by (pgbolt_queue) (pgbolt_worker_active) / max by (pgbolt_queue) (pgbolt_cluster_queue_limit)
```

Застрявшая очередь — готовая задача ждёт дольше 5 минут:

```promql
max by (pgbolt_queue) (pgbolt_cluster_queue_oldest_ready_age_seconds) > 300
```

Потеря lease:

```promql
sum(rate(pgbolt_job_outcome_dropped_total{pgbolt_drop_reason="lease_lost"}[5m])) > 0
```

Снимок `queue_stats` устарел — ни один под с `RunMaintenance = true` не делает снимок или он стабильно падает. Алерт имеет смысл при
живых подах с серверами; в простое ряд остаётся с нулями, поэтому пустая очередь с устаревшим снимком не путается:

```promql
absent(pgbolt_cluster_queue_jobs)
```

## Стоимость

Без слушателей (`ActivityListener`, `MeterListener`) телеметрия не добавляет аллокаций на задачу, кроме scope логов — одного объекта
`JobLogScope` (32 байта). Бенчмарк `benchmarks/PgBolt.Server.Benchmarks` измеряет один `JobRunner.RunAsync` успешной no-op задачи
(без БД и сети); результаты и сравнение с состоянием до внедрения телеметрии — в
[README бенчмарка](../benchmarks/PgBolt.Server.Benchmarks/README.md).
