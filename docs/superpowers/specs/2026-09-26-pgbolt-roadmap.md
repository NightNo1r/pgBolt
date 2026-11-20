# PgBolt — карта проекта

Высокопроизводительный job runner (аналог Hangfire) на PostgreSQL + .NET 10, доступ к БД через Npgsql (без EF Core).

## Сквозные решения (зафиксированы)

| Решение | Значение |
|---|---|
| Платформа | Только .NET 10, AOT-совместимость на горячем пути |
| Доступ к БД | Npgsql, `NpgsqlDataSource`, без EF Core |
| Подключение | Напрямую к PG **или** через PgBouncer в **transaction mode**. Statement mode не поддерживается |
| Следствия PgBouncer | Нет `LISTEN`, нет session-level locks/`SET`/temp tables; prepared statements выключены по умолчанию (опция). Разрешены многошаговые транзакции и `pg_advisory_xact_lock` |
| Целевая нагрузка | 5–20k jobs/sec, десятки миллионов задач в сутки на одном узле PG |
| Семантика доставки | At-least-once |
| Модель задач | Типизированные `IJob<TArgs>` + source generator; динамический enqueue по имени + JSON (вариант A). Расширение до лямбд через interceptors (вариант B) — отдельной частью позже |
| Результаты задач | Не хранятся |
| Уникальность | Опциональный `unique_key`: не более одной активной задачи с ключом |
| Топология | Несколько сервисов на одной схеме, маршрутизация через очереди, registry публикуется в БД. Имя схемы настраивается |
| Раскладка данных | Горячая `queue` + партиционированные `jobs` / `job_events` |

## Части

Каждая часть проходит собственный цикл «спека → план → реализация».

### 1. Core: модель, схема, горячий путь, Client API
Спека: `2026-09-26-pgbolt-core-design.md`. Основа для всех остальных частей.

### 2. Storage maintenance
Зависит от 1, 3 (расписание — в `PgBolt.Server`). Спека: `2026-09-29-pgbolt-storage-maintenance-design.md`, план: `docs/superpowers/plans/2026-09-29-pgbolt-storage-maintenance.md`. **Статус: реализовано.**
- Периодический вызов `EnsurePartitionsAsync` из core (§4.4); проверка горизонта при старте уже в core (`PgBoltInitializer`).
- Retention от суток создания задачи: drop партиций `jobs`/`job_events` (протокол core §4.4); компактирование партиций `jobs` с живыми задачами (не больше `RetentionCompactionMaxLiveJobs`): живые строки под блокировкой родителя переносятся в новую партицию тех же суток, старая удаляется; незавершённые задачи не удаляются никогда.
- Уборка мёртвых записей `servers`.
- Кластерная координация периодических задач через таблицу `maintenance_tasks`.
- Изменения core/worker: `LOCK TABLE ONLY` + `DETACH` (до 10 партиций под одной блокировкой; компактирование — по одной партиции), удержание блокировки ограничено `statement_timeout`/`idle_in_transaction_session_timeout`, `DROP` отдельной транзакцией, компактирование вместо построчного удаления (уточнение core §4.4; не MVCC-безопасно для снимков REPEATABLE READ, пропускается при публикации `jobs` в логической репликации); самовосстановление строки `servers` через heartbeat; версия схемы новее кода — предупреждение, а не ошибка.
- Перенесено: opt-in индексы по `args` — в часть 6; настройка autovacuum `queue`, мониторинг bloat, `COPY` против `unnest`, влияние числа присоединённых партиций на dequeue — в часть 7.

### 3. Worker runtime (Server)
Зависит от 1. Спека: `2026-09-28-pgbolt-worker-design.md`, план: `docs/superpowers/plans/2026-09-28-pgbolt-worker.md`. **Статус: реализовано.**
- `AddPgBoltServer(name, …)`: N виртуальных серверов в одном процессе, у каждого свой `server_id`, heartbeat, `lease_owner`, свой набор очередей.
- Слоты воркеров: `Workers` (общий пул) + на очередь `Reserved` и `MaxConcurrency`; общий пул — строгий порядок объявления очередей.
- Один диспетчер на сервер, fetch только под свободные слоты, адаптивный polling (до 10 с) + in-process wakeup.
- Пакетный ack, heartbeat по строкам — только известных runtime lease (решено: liveness по `servers` не делаем, пересмотр — по бенчмарку части 7), отмена выполнения за `HeartbeatInterval` до истечения его lease при неподтверждённом продлении (проверка по каждому выполнению), fencing по `lease_id`, sweeper, promoter.
- Один координирующий hosted service на процесс: все виртуальные серверы останавливаются одновременно.
- Изменения core: heartbeat по списку lease (§5.5), `IWakeupSignal.Subscribe` вместо `WaitAsync` (§5.10), `PgBoltInitializer.Ready`, `JobDescriptor.TimeoutSeconds`.
- Retry `15 с × 2^(n−1)` до 1 ч ± 20 %, опциональный таймаут (по умолчанию нет), фильтры выполнения, graceful shutdown без траты попытки (только для задач, остановившихся по shutdown-токену, и незапускавшихся); остановка из-за потери heartbeat тратит попытку.
- `LISTEN`/`NOTIFY` исключены из проекта: постоянное прямое соединение на под и глобальный lock на коммите.

### 3b. Worker: кластерный лимит и веса очередей
Зависит от 3. Точки расширения заложены в 3 (`IFetchStrategy`, стратегия `SlotAllocator`, `QueueOptions`). Спека: `2026-10-10-pgbolt-queue-limits-design.md`, план: `docs/superpowers/plans/2026-10-12-pgbolt-queue-limits.md`. **Статус: реализовано.**
- Кластерный лимит конкурентности на очередь: `pg_advisory_xact_lock(hash(queue))` + подсчёт leased-строк в dequeue (батч в неявной транзакции, работает через PgBouncer), лимит и вес — в таблице `queue_settings`: значения по умолчанию из кода (`QueueOptions.ClusterMaxConcurrency` / `Weight`) и переопределения из БД (`IQueueSettingsManager`, пауза — лимит 0).
- Веса очередей для общего пула сервера (stride scheduling); смена весов на лету на границе плана.
- Dashboard: колонки и действия лимита и веса на экране очередей; анализатор BOLT010 / BOLT011.

### 4. Scheduler: recurring
Зависит от 1, 3. Спека: `2026-09-30-pgbolt-scheduler-design.md`, план: `docs/superpowers/plans/2026-09-30-pgbolt-scheduler.md`. **Статус: реализовано.**
- Таблица `recurring_jobs`; runtime API `IRecurringJobManager` + code-first объявления в `AddPgBolt` с периодической синхронизацией набора владельца.
- Cron (Cronos, с секундами), таймзоны, misfire-политики `FireOnce` / `Skip` / `CatchUp` (окно по времени), время срабатывания в `JobContext.ScheduledAt` (колонки `jobs.recurring_id`, `jobs.scheduled_at`).
- Без лидера: тик захватывает строки `FOR UPDATE SKIP LOCKED`, ставит задачи и сдвигает курсор одной транзакцией.
- Наложение `Skip` (через зарезервированный `unique_key` `pgbolt:recurring:{id}`) / `Allow`; «запустить сейчас», пауза.
- Миграции V001 и V002 объединяются в одну начальную V001 с объектами части 4: совместимость со схемами предыдущих сборок не поддерживается (пользователей ещё нет).

### 5. Workflows: continuations и batches
Зависит от 1, 3. Спека: `2026-10-02-pgbolt-workflows-design.md`, план: `docs/superpowers/plans/2026-10-02-pgbolt-workflows.md`. **Статус: реализовано.**
- Один родитель на узел (задача или batch), условия OnSuccess / OnAnyFinished; при Failed-родителе OnSuccess-продолжение ждёт requeue, при Cancelled — каскадная отмена.
- `Awaiting` — производное состояние строки `queue` (`await_condition`): уникальность и retention по PK `queue` без отдельных механизмов.
- Доставка завершения: флаг `notify` на строке `queue` + сигналы `completions` из statement'ов финализации; resolver на всех подах (`SKIP LOCKED`, in-process wakeup) запускает и отменяет продолжения, агрегирует счётчики batch за проход (без горячей строки).
- Batches: атомарное создание, вложенные batches, batch-продолжения, отмена batch; добавление в существующий batch не поддерживается.
- Fluent API `client.Workflow().Enqueue(…).ContinueWith(…).ContinueWithBatch(…).SubmitAsync()`; анализатор BOLT007.
- Retention: Failed-родители с ждущими детьми и Failed- и Cancelled-члены batches, пока существует строка batch, считаются живыми (компактирование); строки `batches` удаляются сверху вниз.

### 6. Dashboard
Зависит от 1, 3 (частично 4, 5). Спека: `2026-10-03-pgbolt-dashboard-design.md`, план: `docs/superpowers/plans/2026-10-05-pgbolt-dashboard.md`. **Статус: реализовано.**
- Библиотека `PgBolt.Dashboard`: Minimal API + Razor-компоненты через `HtmlRenderer` + htmx (C#-native, без Blazor-runtime и npm), нулевая настройка хоста; авторизация через политики, read-only, CSRF без antiforgery хоста.
- Очереди, задачи по состояниям, карточка и история, recurring (с историей запусков), batches, серверы (с состоянием задач обслуживания), типы задач; действия cancel / trigger / requeue (в т. ч. над выбранными), отмена batch, управление расписаниями; запуск задачи по имени с формой из `job_types.args_schema`.
- Статистика: агрегированные счётчики подтверждённых исходов от серверов (`stats_minutes`, сброс раз в 15 с) и снимок живых состояний (`queue_stats`, кластерная задача `stats`); без записей на горячем пути.
- Выборки по состоянию без индексов `jobs` по изменяемым колонкам: живые — по `queue`, Failed / Cancelled — по `job_events` (существующие `kind`, протоколы записи не меняются), Succeeded — обход PK порциями с бюджетом.
- Отложено: готовый хост (образ / `dotnet tool`); opt-in индексы и поиск по `args` (перенесено из части 2) — отдельной частью, в рамках контракта HOT (core §4.2); массовые действия по фильтру; ручной запуск задач обслуживания.

### 7. Observability и производительность (сквозная)
Начинается вместе с 1. Делится на 7a (observability) и 7b (производительность: 7b-1 и 7b-2).

#### 7a. Observability
Зависит от 1–6. Спека: `2026-10-16-pgbolt-observability-design.md`, план: `docs/superpowers/plans/2026-10-16-pgbolt-observability.md`. **Статус: реализовано.**
- Трассировка: producer-span при enqueue (`enqueue {job.type}`, `workflow submit`, `recurring trigger`), consumer-span выполнения в `JobRunner` — ребёнок producer-span'а через `jobs.trace_parent`.
- Метрики пода (аддитивные, агрегация `sum`) и кластерные `pgbolt.cluster.*` (одинаковы на всех подах, агрегация `max`): снимок `queue_stats` читает цикл `stats` на любом поде с серверами; `Meter` на DI-контейнер, фиксированные бакеты гистограмм.
- Логи: события жизненного цикла задачи и scope задачи, все вызовы логов на `[LoggerMessage]` со стабильными `EventId`; телеметрия не меняет исход задачи, `PgBoltServer.RunExecutionAsync` не бросает.
- Пакет `PgBolt.OpenTelemetry` с `AddPgBoltInstrumentation()`; документация `docs/observability.md`; бенчмарк `JobRunner` (без слушателей — без новых аллокаций, кроме scope логов).

#### 7b-1. Нагрузочный стенд и SLO
Зависит от 7a. Спека: `2026-10-17-pgbolt-load-slo-design.md`, план: `docs/superpowers/plans/2026-10-17-pgbolt-load-slo.md`. **Статус: реализовано.** Результаты и решения — `docs/performance.md`.
- Follow-up 7a: флаки тестов, точность телеметрии (`job.duration`, причина исхода, `ProducerSpan`), ложные `storage.errors`, дубли кода.
- Стенд `benchmarks/PgBolt.LoadTest`: сценарии `sustained`, `ramp`, `partitions`, `vacuum` в режимах `direct` и `bouncer` (PgBouncer, transaction mode), профиль нагрузки B (10 000 jobs/s) и SLO, отчёты в `results/`.
- Кластерные метрики bloat `pgbolt.cluster.table.dead_tuples` и `pgbolt.cluster.table.size`; пороги алертов — в `docs/observability.md`.
- Итог: профиль B (10 000/с) — пропускная способность, корректность и соединения выполнены в обоих режимах, SLO задержки старта нарушен по p50 (≈ 95 мс при пороге 50 мс); максимальный устойчивый rate `ramp` — 12 000/с `direct`, 10 000/с `bouncer`; число партиций `jobs` по умолчанию (P = 15) держит пропускную способность, при P = 60 SLO 1 нарушен в трёх режимах из четырёх короткими провалами, при P = 200 — ~4,2 тыс./с; reloptions `queue` в `V001` не меняются; при удержании xmin выполнение падает до ~1–2 тыс./с.
- Найдено и исправлено: `No Reset On Close=true` ставится всегда — иначе `UsePreparedStatements` через PgBouncer падал на `prepared statement "…" does not exist`.
- BenchmarkDotNet для сценариев с БД не используется; микрозамеры — существующие `PgBolt.Benchmarks` и `PgBolt.Server.Benchmarks`. Пункт закрыт.

#### 7b-2. Производительность: крупные находки стенда
Зависит от 7b-1. Обоснование, классификация и номера находок — [docs/performance.md](../../performance.md), раздел «Решения»; ниже — пункты с цифрами из отчётов.
- Задержка доставки немедленных задач, поставленных из другого процесса: p50 SLO 2 ≈ 95 мс при пороге 50 мс (`sustained`, 10 000/с, `direct` и `bouncer`); p99 по окнам при 2 000/с (322–375 мс) — на уровне 10 000/с (278–487 мс), p50 за весь `ramp` 96,2–96,4 мс; в четверти срезов воркеры простаивают при ~1000 готовых задач. Пересмотреть рост интервала адаптивного polling по пустым выборкам после освобождения слота и межпроцессный wakeup.
  Исправление опроса активной очереди реализовано. В десятиминутном замере p50 составил 37,4 мс напрямую и 37,8 мс через PgBouncer. **Находка открыта:** напрямую SLO 1–6 выполнены, через PgBouncer нарушены SLO 1, 2 и 4. [Спецификация](2026-11-20-pgbolt-active-queue-polling-design.md) · [План](../plans/2026-11-20-pgbolt-active-queue-polling.md) · [Результаты приёмки](../../performance.md#проверка-опроса-активной-очереди-20-ноября-2026)
- Отсечение партиций `jobs` в dequeue: условие на ключ партиционирования (например, граница `id` по допустимому диапазону дат), чтобы планирование и блокировки не росли с числом партиций. Сейчас: mean plan dequeue 0,5 → 1,1 → 4,6 → 11,4 мс и 100 → 2025 блокировок при P = 15 → 60 → 200 → 400; пропускная способность 10 → 4,2 → 1,9 тыс./с при P = 60 → 200 → 400; порог P по SLO 1 — 60. Prepared statements не помогают. Кандидат V003 на PG 17.11 открыл только 1–2 партиции для пачки соответствующих суток при P=15 и 400. Приёмка по потоку и задержкам остаётся открытой.
- Autovacuum `queue` под нагрузкой B: SLO 7–9 сценария `vacuum` не выполняет ни один вариант — текущие reloptions, `autovacuum_vacuum_cost_delay = 0`, `autovacuum_naptime = 10s`. SLO 8 нарушают все окна фазы 1: по p50 SLO 2 (находка 1) и по хвосту — p99 SLO 2 выше 500 мс в 41–72 окнах из 120 в пяти прогонах из шести, в `bouncer` 147 и 182 среза с backlog выше rate у текущих и `cost_delay = 0`; SLO 9 — выполнение при удержанном xmin падает до ~1 тыс./с (среднее 1,8–1,9 тыс./с), backlog не разобран за 5 мин фазы 3. Перепроверить `autovacuum_naptime = 10s` после исправления находки 1 (p50), оценивая и хвост p99 и backlog фазы 1: в `bouncer` он устраняет рост backlog фазы 1 (1336 → 7012 против 1221 → 1471; срезов с backlog выше rate 147 → 0), размер `queue` фазы 1 38,7–46,1 МБ против 141,9–247,6 МБ, p99 SLO 2 фазы 1 611,8 мс против 18 743,3 мс; в `direct` p99 хуже (753,2 против 412,2 мс).
- Bulk enqueue через binary `COPY` против `unnest`.
- Готовые Grafana-дашборды и exemplars (вынесены из 7a).

### 8. Interceptors (вариант B модели задач)
Зависит от 1. Отложено.
- `client.Enqueue<TJob>(j => j.ExecuteAsync(new(...)))` → на этапе компиляции превращается в `EnqueueAsync<TArgs>`.

## Порядок

```
1 Core ──► 2 Storage maintenance (расписание — в 3 Worker)
   │
   └──► 3 Worker ──► 4 Scheduler
             │  ├──► 5 Workflows
             │  └──► 3b Кластерный лимит, веса
             └──► 6 Dashboard
7a Observability — после 1–6; 7b-1 Стенд и SLO — после 7a; 7b-2 — после 7b-1
8 Interceptors — после стабилизации 1
```
