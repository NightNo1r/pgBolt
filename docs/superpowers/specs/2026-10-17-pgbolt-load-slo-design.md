# PgBolt — часть 7b-1: нагрузочный стенд, SLO и follow-up 7a

Статус: реализовано, план `docs/superpowers/plans/2026-10-17-pgbolt-load-slo.md`; результаты и решения — `docs/performance.md` · Дата: 2026-10-17 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»), `2026-09-28-pgbolt-worker-design.md` (далее «worker»), `2026-09-29-pgbolt-storage-maintenance-design.md` (далее «часть 2»), `2026-10-16-pgbolt-observability-design.md` (далее «7a»); отчёт 7a: `.superpowers/sdd/2026-10-16-pgbolt-observability/report.md`, отложенные находки: `parked.md` и `final-review.md` там же.

## 1. Цель и рамки

Проверить обещание roadmap «5–20k jobs/sec на одном узле PG» воспроизводимым стендом, зафиксировать профиль нагрузки B и SLO для него, найти и, где фикс локальный, устранить узкие места: autovacuum `queue`, число партиций `jobs` и индексов, работа через PgBouncer. Перед стендом закрыть все отложенные Minor части 7a: флаки и ложные `storage.errors` исказили бы прогоны.

**Входит:**
- блок A — follow-up 7a: все пункты `parked.md` и `final-review.md` F1–F8, кроме перечисленных в §3.7 (§3);
- блок B — проект `benchmarks/PgBolt.LoadTest` со сценариями `sustained`, `ramp`, `partitions`, `vacuum` в режимах прямого подключения и через PgBouncer (§4–§7);
- профиль нагрузки B и SLO (§5);
- кластерные метрики bloat `pgbolt.cluster.table.dead_tuples` и `pgbolt.cluster.table.size` (§8);
- исправления по результатам прогонов по правилу §9, в том числе параметры autovacuum `queue` в `V001__initial.sql` (`completions` не меняется, §7.4);
- документация `docs/performance.md`, обновления `docs/observability.md`, roadmap, `CHANGELOG.md` (§10).

**Не входит (7b-2 или позже):** `COPY` против `unnest` для bulk enqueue; готовые Grafana-дашборды и exemplars; хосты PgBolt в дочерних процессах стенда; прогон стенда в CI; workflows, `unique_key` и recurring в профиле нагрузки; крупные исправления, найденные стендом (§9).

### Критерии успеха
- Каждый сценарий запускается одной командой (§4.1) на машине с Docker и .NET 10 и пишет отчёт с вердиктом по каждому SLO.
- Отчёты полного прогона всех сценариев во всех их режимах сохраняются локально в `benchmarks/PgBolt.LoadTest/results/`.
- SLO нагрузки B (§5.2, §7.3) выполняются в режимах `direct` и `bouncer`, либо каждое нарушение объяснено в `docs/performance.md` и заведено в roadmap как пункт 7b-2 с цифрами из отчёта. Невыполненный SLO не блокирует завершение части.
- Конфигурация по умолчанию (P = 15 партиций, текущие индексы) выполняет SLO 1 и 2 во всех четырёх режимах сценария `partitions` (§6), либо нарушение оформлено так же.
- Все пункты блока A закрыты; полный прогон `dotnet test PgBolt.slnx --filter "Category!=Load"` зелёный, в том числе классы, флакавшие в 7a.

## 2. Принятые решения

| Решение | Значение | Почему |
|---|---|---|
| Форма стенда | Консольный проект `benchmarks/PgBolt.LoadTest`, `IsTestProject=false` | Десятиминутные прогоны, параметры CLI, отчёты и коды возврата естественны для консоли; xUnit для этого неудобен; не замедляет `dotnet test` |
| Хосты PgBolt | Внутри процесса стенда, отдельный `IHost` на хост | Проще; основной кандидат в узкое место — PG в Docker. Дочерние процессы — если отчёт покажет упор в CPU клиента |
| Окружение | Свои контейнеры PG 17 и PgBouncer 1.24 через Testcontainers, либо внешняя строка подключения | `PostgresFixture` тестов поднимает `max_locks_per_transaction=256` — это скрыло бы проблему партиций |
| Продюсер | Отдельный `IHost` с `AddPgBolt` (без сервера) в том же процессе | Как отдельный сервис-продюсер: in-process wakeup до хостов не доходит, диспетчеры работают на polling — реалистичный худший случай |
| Подключение хостов и продюсера | `ConnectionString` (с `Maximum Pool Size` и `Application Name`), не свой `NpgsqlDataSource` | Работает `PgBoltDataSourceFactory`: он всегда ставит `No Reset On Close=true` (иначе `DISCARD ALL` при возврате в пул удаляет на сервере statement'ы, которые PgBouncer считает живыми), а без `UsePreparedStatements` — ещё `Max Auto Prepare=0`; оба нужны для PgBouncer |
| Задержка старта | Точные перцентили по HdrHistogram в стенде | Бакеты `pgbolt.job.wait_time` (7a §4.5) слишком грубые для p99 и p99.9 |
| BenchmarkDotNet | Не используется для сценариев с БД | BDN — микрозамеры; здесь нужны минуты устойчивой нагрузки. Пункт roadmap «BenchmarkDotNet» закрыт существующими `PgBolt.Benchmarks` и `PgBolt.Server.Benchmarks` |
| Целевой поток профиля B | 10 000 jobs/s | 20k — цель для выделенного железа, не для Docker на ноутбуке; потолок измеряет `ramp` |
| Невыполненный SLO | Не блокирует часть: объяснение в docs и пункт 7b-2 | Стенд — инструмент измерения; крупный фикс заслуживает своей спеки |
| Мониторинг bloat | Кластерные gauge'и в продукте | Оператору нужен сигнал до деградации; запрос к каталогу PG раз в 10 с дешёвый |
| Миграции | Изменения вносятся в `V001__initial.sql` | Как в частях 3b–7a: пользователей нет, схемы прошлых сборок отвергаются при старте |
| Пакеты | `HdrHistogram` и `Testcontainers` в стенде — через Central Package Management (`Directory.Packages.props`) | Единые версии; продуктовые проекты новых зависимостей не получают |
| Одна спека | Блок A, стенд, метрики и прогоны — в одной части; цикл «прогон → решение → правка → повторный прогон» оформлен явными шагами (§9) | Блок A мал и должен закрыться до прогонов; разбиение на две спеки не уменьшает объём плана |

## 3. Блок A: follow-up 7a

Ссылки на строки — по состоянию ветки `feature/load-slo` (1d5e22b; код совпадает с `main` 810843f). Блок выполняется до стенда. Изменения поведения (§3.2–§3.3) — по TDD: сначала тест, воспроизводящий некорректное поведение.

### 3.1 Флаки тестов
1. `DispatcherTests.Jobs_of_dequeue_returning_after_its_timeout_are_released_not_run` падал в 2 из 4 прогонов: `Dispatcher` ставит Release (`Dispatcher.cs:156-157`) до `Log.DequeueTimedOutJobsReleased` (`:158-159`), а тест проверяет лог сразу после двух Release. Продукт не меняется: порядок «сначала состояние, потом лог» — правило кода (как в `PgBoltServer.AbandonFailed` и `Dispatcher.StartOne`); бросающий логгер до `RegisterForRelease` оставил бы взятые задачи ни запущенными, ни отпущенными до истечения lease. Исправляется тест: проверка лога ждёт его через `Eventually.True(...)`.
2. `QueueStatsSnapshotTests` (Task 4 F2, финал F6): порядок строк задаёт продуктовый `ReadQueueStatsSql` (`FROM f LEFT JOIN queue_stats q ON true`, без `ORDER BY`), своего запроса у теста нет. Продукт не меняется: строки сортируются в тесте по `queue` перед проверкой.
3. `QueueStatsSnapshotTests` (Task 4 F1, `:60`, `:90`): `double.Epsilon` не делает границу строгой — сравнение переписывается как строгое неравенство без эпсилона.
4. `ProcessingServiceTests.Maintenance_runs_only_when_enabled` (Task 12 F4, `:332`): проверка без синхронизации — ожидание наблюдаемого события (`Eventually.True`) вместо проверки сразу после старта.

### 3.2 Точность телеметрии
1. `job.duration` (Task 9 F2, `JobRunner.cs:97`) снимается после `OutcomeClassifier.Classify` (`:93`) и включает время `RetryPolicy`. Исправление: длительность фиксируется до классификации. Тест: фейковая политика повтора с задержкой не увеличивает записанную длительность.
2. `FirstReason` (Task 9 F3, `JobRunner.cs:178,215,221`) перечитывается после классификации; гонка с `LeaseKeeper` даёт расхождение span/лога с исходом. Исправление: причина читается один раз до классификации, тот же снимок получают классификатор, span и лог. Тест: причина, выставленная после классификации, не попадает ни в span, ни в лог.
3. `ProducerSpan` (Task 6 F1, финал F1, `ProducerSpan.cs:55-57,91-93`) сообщает о сбое до восстановления `Activity.Current`, событие 8001 получает SpanId неэкспортируемого span'а. Исправление: сначала восстановить `Activity.Current`, затем `ReportFailure` — как в `JobRunner`. Тест: `Activity.Current` в момент записи 8001 — внешний span.
4. Позиционные `null` в вызовах `ProducerSpan.Start` (Task 7 F1: `RecurringJobManager.cs:49`, `WorkflowBuilder.cs:166`, `PgBoltJobClient.cs:16`) — именованные аргументы.
5. Без слушателя источника `PgBolt`: `JobRunner.StartSpan` выходит до `ActivityContext.TryParse` (`JobRunner.cs:128`, проверка `HasListeners()`), `PgBoltJobClient` не форматирует строку `enqueue {type}` (`:16`). Бенчмарк `JobRunner` прогоняется до и после этого изменения, обе цифры — в README бенчмарка (§3.6).

### 3.3 Ложные `storage.errors`
1. `CompletionSink.cs:231-234,270-273` (Task 11 F3): `catch` вокруг отправки ловит и исключение логгера после успешного ответа БД — ложный `storage.errors` и повторная отправка уже применённого ack/исхода. Исправление: `try` охватывает только обращение к БД; логирование успешного ответа — вне его. Тест: бросающий логгер после успешной отправки не даёт `storage.errors` и повторной отправки.
2. `LeaseKeeper.cs:193-197` (Task 11 F4) и `ProcessingMaintenance.LoopAsync` (`:355-357`, Task 12 F2): при отменённом токене остановки не-OCE исключение (например, `NpgsqlException` от разорванного соединения) учитывается как `storage.errors`. Исправление: при `ct.IsCancellationRequested` любое исключение — остановка: выход без метрики и без лога ошибки. Тесты на оба места.
3. `CompletionSink.cs:413-414` (Task 11 F2): двойное чтение `_abort` — причина `Drop` и выбор лога могут разойтись; значение читается один раз.

### 3.4 Дубли и упрощения кода (поведение не меняется)
1. Правило «снять запись и учесть abandoned» повторено в `Dispatcher` и `PgBoltServer` (×2, `AbandonRunning` — `PgBoltServer.cs:113,291`, финал F2): метрика `outcome.dropped{abandoned}` переносится в `LeaseKeeper.Unregister` (`LeaseKeeper.cs:88`, параметр причины), `AbandonRunning` удаляется. Вызовы `Unregister` в тестах получают новый параметр.
2. Защищённый жизненный цикл `Activity` написан дважды (`JobRunner` ↔ `ProducerSpan`) — общий internal helper в `PgBolt.Postgres.Diagnostics`, доступный `PgBolt.Server` (как `PgBoltInstrumentation`).
3. Защищённая запись в instrument — дважды (`PgBoltClientMetrics` ↔ `PgBoltMetrics`) — общий helper (финал F4, Task 8 F2). Правило `count <= 0` → без измерения действует только для счётчиков (`Add`); запись в гистограмму (`Record`) пишет и 0 — это законное значение `wait_time` и `duration`. Проверка `Fired > 0` у вызывающего (`ProcessingMaintenance.cs:175`, Task 12 F3) после этого удаляется.
4. `PgBoltInstrumentation` передаётся рядом с `PgBoltMetrics`, который его уже держит, — лишний параметр убирается.
5. `PgBoltProcessingService` — двойной `GetRequiredService<PgBoltMetrics>` (Task 12 F8) → один.
6. `StatsOnceAsync` — двойной вызов чтения снимка (`ProcessingMaintenance.cs:240-245`, Task 12 F1) → `try/finally`.
7. Приватный `ProcessingMaintenance.ReadQueueStatsAsync` (`:250`) публикует снимок и глотает ошибки — переименовывается в `PublishQueueStatsAsync` (финал F3).
8. `PgBoltServer.ExecutionStarted` (`PgBoltServer.cs:69,206-207`, Task 10 F1) — тестовый хук, вызываемый после `_running[...]`: заменяется ожиданием наблюдаемого состояния в тестах; если без хука тест недетерминирован — хук остаётся internal и вызывается до `_running[...]`. Решение фиксирует план после чтения тестов.

### 3.5 Тесты и бенчмарк
- Незакрытые `PgBoltInstrumentation` и `Meter` в фикстурах: `PgBoltInstrumentationTests` (Task 2 F1), `JobRunnerTests.cs:131` (Task 9 F4, финал F7), тесты Housekeeping / ProcessingMaintenance / SchedulerLoop / RetentionReport (Task 12 F5) — `using` / `Dispose`.
- `ListLogger`: лишние поля scope у дочерних логгеров (Task 2 F2).
- `FetchStrategyTests`: имена тестов и параметр `limited` ссылаются на старое `Limited` (Task 5 F1) — переименование.
- `JobRunnerTelemetryTests`: «тот же объект» исключения NoExecutor — `Assert.Same` (Task 9 F1).
- `RunExecutionTests.Complete_exception_on_stopped_sink`: проверка, что `Complete` действительно бросил (Task 10 F2); дубль `TestTelemetry.Sum` (Task 11 F5).
- `StatsTaskTests.cs:162`: повтор `RunScheduler=false` (уже задан в `:30`, Task 12 F6).
- Настройка `ActivityListener` скопирована в тестах `ProducerTracingTests`, `ProducerSpanTests`, `JobRunnerTelemetryTests`, `ObservabilityEndToEndTests` — общий helper в `PgBolt.Testing` только для тестовых проектов. `JobRunnerBenchmarks` сохраняет свою копию: бенчмарк не ссылается на `PgBolt.Testing`, а тот тянет xunit и Testcontainers.
- `PgBolt.Server.Tests/Fakes/Eventually` переносится в `PgBolt.Testing`, становится `public` (как остальные типы `PgBolt.Testing`) и получает параметр таймаута (по умолчанию 5 с, как сейчас). Для `Assert.Fail` `PgBolt.Testing` получает пакет `xunit.v3.assert` (сейчас там только `xunit.v3.extensibility.core`): через Central Package Management — в `Directory.Packages.props` добавляется `PackageVersion` `xunit.v3.assert` с версией `xunit.v3` (3.1.0), сейчас его там нет. `EndToEndTests` и `ObservabilityEndToEndTests` используют его с увеличенным таймаутом вместо своих циклов ожидания (`EventuallyAsync`).
- `ObservabilityEndToEndTests` (Task 15 F1–F4): комментарий про шестую задачу, `&` в условии ожидания, магические ёмкости → именованные константы, параметры `StartHostAsync`.
- `JobRunnerBenchmarks.GlobalSetup` (Task 1 F1) проверяет исход Ack.

### 3.6 Документация и комментарии
- Ссылки на спеку/план в комментариях и XML-doc (Task 9 F5, Task 12 F7, финал F5) — все ссылки вида `§N`, «часть X», «Task N» заменяются сутью: во всех файлах `src/`, которые меняет блок A, и полностью в `JobRunner.cs` (`:235`), `ProcessingMaintenance.cs` (`:109,147,158,167,185,194,210,294`), `RetentionReport.cs` (`:74`).
- `docs/observability.md:48-49`: `HistogramBuckets` названы константами (финал F8) — исправить формулировку; дубли таблиц событий — одна таблица.
- `benchmarks/PgBolt.Server.Benchmarks/README.md`: непроверяемые цифры заменяются результатами прогонов из `results/` — до и после раннего выхода §3.2 п. 5, без порога по времени (Task 16: +40 нс без слушателей); пункт бенчмарка в `CHANGELOG.md` уточняется (Task 16 F1–F4).

### 3.7 Без изменений
- Task 8 F1 — информационная находка (изменение вне Files было полезным).
- Task 11 F1 — закрыт ранее: в `CompletionSink` один `HashSet` (`:212`, `ackedIds.ToHashSet()`), им пользуются и `CountAcked`, и подсчёт потерянных lease.
- Task 14 F1 — версия OpenTelemetry 1.15.3 уже отражена в `CHANGELOG.md`.
- Task 16: +32 B на задачу (объект `JobLogScope`) — принимается: в пределах критерия аллокаций 7a. По времени (+40 нс) порога нет, цифры фиксирует README (§3.6).
- Task 16 «View пользователя поверх `AddPgBoltInstrumentation()` даёт дубль метрики» — ограничение уже описано в `docs/observability.md`.
- Коллекции `Serial` (`PgBolt.Postgres.Tests`, глобальное состояние БД) и `TelemetryCollection` (`PgBolt.Server.Tests`, статические слушатели) не объединяются: они в разных сборках, а определение коллекции xUnit действует только внутри своей сборки; назначение у них разное.

## 4. Стенд

### 4.1 Проект и запуск

`benchmarks/PgBolt.LoadTest` — консольное приложение .NET 10, `IsTestProject=false`, входит в `PgBolt.slnx`, ссылки на `PgBolt.Server` и `PgBolt.Postgres`, генератор задач — как в существующих тестовых хостах. Сборка — частью `dotnet build PgBolt.slnx` (TreatWarningsAsErrors).

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- <sustained|ramp|partitions|vacuum> [флаги]
```

| Флаг | Сценарии | По умолчанию | Смысл |
|---|---|---|---|
| `--mode direct\|bouncer\|both\|direct-prepared\|bouncer-prepared\|all` | все | без `--connection` — `both` (`direct` + `bouncer`), у `partitions` — `all` (4 режима); с `--connection` без `--bouncer-connection` — `direct`, у `partitions` — `direct` и `direct-prepared`; с `--connection` и `--bouncer-connection` — `both`, у `partitions` — `direct`, `bouncer`, `direct-prepared` (§4.2) | Режимы подключения. `*-prepared` и `all` допустимы только в `partitions` |
| `--connection "<cs>"` | все | свои контейнеры (§4.2) | Внешняя БД |
| `--bouncer-connection "<cs>"` | все | свой PgBouncer | Внешний PgBouncer; допустим только вместе с `--connection`, обязателен для режимов `bouncer*` при `--connection` |
| `--out <dir>` | все | `benchmarks/PgBolt.LoadTest/results` | Каталог отчётов |
| `--rate <n>` | `sustained`, `partitions`, `vacuum` | 10 000 | Целевой поток enqueue/с |
| `--warmup <t>` | `sustained`, `partitions`, `vacuum` | 1 мин; `partitions` — 30 с; `vacuum` — 2 мин | Прогрев до окна замера (в `vacuum` — до фазы 1) |
| `--duration <t>` | `sustained`, `partitions` | 10 мин; `partitions` — 2 мин на комбинацию | Окно замера |
| `--ramp-start <n>` | `ramp` | 2 000 | Поток первой ступени |
| `--ramp-step <n>` | `ramp` | 2 000 | Прирост потока на ступень |
| `--ramp-step-duration <t>` | `ramp` | 60 с | Длительность ступени |
| `--ramp-max <n>` | `ramp` | 40 000 | Поток последней ступени |
| `--partitions <P,...>` | `partitions` | 15,60,200,400 | Число партиций `jobs` (§6.2) |
| `--indexes current,extra` | `partitions` | `current,extra` | Наборы индексов (§6.2) |
| `--phase1 <t>` | `vacuum` | 20 мин | Стационарная фаза (§7.2) |
| `--hold-xmin <t>` | `vacuum` | 5 мин | Фаза удержания xmin |
| `--phase3 <t>` | `vacuum` | 10 мин | Фаза восстановления |
| `--queue-reloptions "k=v,..."` | `vacuum` | — | `ALTER TABLE queue SET (...)` до прогона (§7.4) |
| `--pg-setting "k=v,..."` | `vacuum` | — | Параметры сервера своего контейнера PG: каждая пара передаётся как `-c k=v` при старте контейнера (§4.2, §7.4); вместе с `--connection` — ошибка разбора |

Флаг вне своих сценариев — ошибка разбора; так же `--bouncer-connection` без `--connection` и `--pg-setting` с `--connection`. Длительности окна замера, фаз и ступени кратны 10 с (окна §4.4); ступень `ramp` — не короче 30 с.

Коды возврата:
- 0 — все оцениваемые SLO выполнены (для `ramp` — прогон без сбоев стенда, в том числе остановка роста с пометкой «стенд», §5.3; для `partitions` — §6.3);
- 1 — нарушен хотя бы один SLO, в том числе SLO 1 с пометкой «enqueue» (§4.4); для `partitions` — только SLO у `P = 15, current` хотя бы в одном режиме (§6.3);
- 2 — сбой самого стенда (§4.4): исключение стенда, недоступность БД или контейнеров (для среза — 30 подряд неудачных попыток, §4.5), неверные аргументы, `max_prepared_statements = 0` у PgBouncer в режиме `bouncer-prepared` (§4.2), упор в CPU процесса стенда (в `ramp` упор кода 2 не даёт, в `partitions` — только у `P = 15, current`, §4.4).

Вердикт «нет данных» (источник недоступен, отрезок для статистики короче нужного) нарушением не считается и на код возврата не влияет.

### 4.2 Окружение

Без `--connection` стенд поднимает в общей Docker-сети на время одного вызова:
- `postgres:17-alpine`: значения PostgreSQL по умолчанию, кроме `shared_buffers=1GB`, `max_wal_size=4GB` (по умолчанию 1 GB checkpoint на 10k/с шёл бы чаще раза в минуту со всплесками full-page writes), `shared_preload_libraries=pg_stat_statements` (после старта стенд один раз на контейнер выполняет `CREATE EXTENSION IF NOT EXISTS pg_stat_statements` в схеме `public`, а не в схеме прогона: `DROP SCHEMA ... CASCADE` удалил бы расширение), `track_io_timing=on`; `pg_stat_statements.track_planning=on` только в `partitions` (там нужен `mean_plan_time`, §6.3), в остальных сценариях — off (по умолчанию): при on statement'ы одной структуры из многих соединений (dequeue и ack десятков воркеров) конкурируют за запись своей строки статистики, и это исказило бы SLO `sustained` и `vacuum`; `fsync` и `synchronous_commit` включены; `max_connections` (100) и `max_locks_per_transaction` (64) — по умолчанию: оба задают ёмкость общей таблицы блокировок (§6.1), увеличенный `max_connections` сдвинул бы порог P в `partitions`; пары `--pg-setting` (§4.1, только `vacuum`, §7.4) добавляются к команде старта как `-c k=v` и перекрывают перечисленные значения;
- `edoburu/pgbouncer:v1.24.1-p1` с network alias `pgbouncer` в общей сети (канал к админ-консоли, §4.5): `AUTH_TYPE=scram-sha-256` (PgBouncer должен проходить SCRAM к PG 17: образ `postgres` использует `scram-sha-256` и в `password_encryption`, и в `pg_hba` для host; при `AUTH_TYPE` по умолчанию, `md5`, образ пишет в `userlist.txt` md5-хэш, с которым PgBouncer к такому серверу не подключится, а при `scram-sha-256` — пароль открытым текстом), `pool_mode=transaction`, `default_pool_size=40`, `max_client_conn=1000`, `max_prepared_statements=200` во всех режимах (безымянные statement'ы Npgsql при `Max Auto Prepare=0` PgBouncer не отслеживает, режим `bouncer` от этого не меняется; одно значение позволяет прогонять `bouncer` и `bouncer-prepared` в одном вызове на одном контейнере), `admin_users` и `stats_users` — пользователь стенда (для `SHOW POOLS` и `SHOW CONFIG`).

Соединения PG в худшем случае — переход из режима через PgBouncer в прямой: idle серверные соединения PgBouncer (до `default_pool_size` = 40, держатся до `server_idle_timeout`) + пулы хостов и продюсера (2 × 20 + 8 = 48; пулы закрываются при остановке их `IHost` — источник данных принадлежит PgBolt и освобождается с контейнером DI) + соединения стенда (срез, измерение блокировок или удержание xmin, подготовка и удаление схемы — не больше 3) = 91 ≤ 97 (`max_connections` 100 − `superuser_reserved_connections` 3). Autovacuum и фоновые процессы в `max_connections` не входят. Пулы и `default_pool_size` поэтому не уменьшаются, порог SLO 6 (48) прежний.

С `--connection` стенд работает с внешней БД: создаёт уникальную схему и удаляет её после прогона; `pg_stat_statements` используется, если расширение доступно, иначе соответствующие колонки отчёта помечаются «нет данных». Режимы `bouncer*` с внешней БД требуют `--bouncer-connection` (без него режимы по умолчанию — только прямые, §4.1); `--bouncer-connection` без `--connection` не допускается: свой контейнер PG и внешний PgBouncer смотрели бы в разные БД. Канала к админ-консоли внешнего PgBouncer у стенда нет (§4.5), поэтому данные `SHOW POOLS` и SLO 6 для `bouncer` — «нет данных». Режим `bouncer-prepared` требует `max_prepared_statements > 0`, которого у внешнего PgBouncer может не быть (иначе — ошибки «prepared statement does not exist»), поэтому с `--bouncer-connection` он выполняется только при явном `--mode bouncer-prepared` или `--mode all`. Перед режимом `bouncer-prepared` стенд читает `max_prepared_statements` через `SHOW CONFIG` тем же каналом, что `SHOW POOLS` (§4.5): значение 0 — сбой стенда (код 2); канал недоступен (внешний PgBouncer) — предупреждение в отчёте, прогон выполняется.

В своих контейнерах каждый прогон (режим, комбинация `partitions`, кандидат `vacuum`) идёт на новой схеме, прежняя удаляется (`DROP SCHEMA ... CASCADE`).

В отчёт записываются: модель CPU и число ядер хоста, CPU и память Docker (`docker info`), версии PG и PgBouncer, значения `shared_buffers`, `max_connections`, `max_wal_size`, `max_locks_per_transaction`, `autovacuum_naptime`, `autovacuum_max_workers`, `autovacuum_vacuum_cost_delay`, `pg_stat_statements.track_planning`, пары `--pg-setting`, reloptions `queue` и `completions`, версия .NET, коммит репозитория.

### 4.3 Состав процесса

- **Хосты PgBolt:** 2 `IHost`, каждый — `AddPgBolt` + `AddPgBoltServer` с одним виртуальным сервером, `Workers = 32`, очереди `default` и `critical`, `ConnectionString` с `Maximum Pool Size=20` и `Application Name=pgbolt-load-host-<n>` (не свой `NpgsqlDataSource`: тогда `PgBoltDataSourceFactory` выставляет значения для PgBouncer), обслуживание включено (`RunMaintenance = true` на обоих), остальные опции — по умолчанию; кластерных лимитов очередей нет. Режимы `*-prepared` включают `UsePreparedStatements` у обоих хостов и у продюсера (enqueue тоже идёт prepared statement'ом).
- **Продюсер:** отдельный `IHost` с `AddPgBolt` (без сервера), `ConnectionString` с `Maximum Pool Size=8` и `Application Name=pgbolt-load-producer`.
  - Генератор по общему token bucket на заданный rate выдаёт задачи: очередь (§5.1), задержку в целых секундах (0 — немедленная, 1..30 — отложенная), `FailFirstAttempt`, плотный порядковый номер `Seq` (с 0).
  - Задачи раскладываются в пачки по ключу (очередь, немедленная или отложенная): у `EnqueueManyAsync` один `EnqueueOptions` на вызов, то есть одна `Queue` и одна `Delay` (в SQL — `@queue` и `@delay` statement'а). Пачка отправляется при 100 задачах или по возрасту 10 мс от первой задачи. Задержка отложенной пачки выбирается равномерно случайно из целых секунд 1..30 в момент отправки, одна на пачку: распределение задержек по задачам остаётся равномерным, а пачки не дробятся.
  - Готовые пачки идут отправителям через ограниченный канал: не больше 2 × 4 = 8 пачек на ключ (по две на отправителя). Если места для пачки ключа нет, генератор ждёт его и копит недовыданные токены числом, без материализации задач, с потолком rate × 1 с; когда канал освобождается, недовыданное досылается. Задачи не накапливаются в памяти процесса, где меряются задержки; кратковременный сбой enqueue (например, всплеск fsync WAL на сотни миллисекунд) не теряет нагрузку, а устойчивое отставание переполняет потолок и видно как недогруз по правилу §4.4. Отчёт пишет по окнам максимальное время ожидания генератора и число токенов, отброшенных сверх потолка.
  - 4 задачи-отправителя берут готовые пачки и вызывают `EnqueueManyAsync(args, new EnqueueOptions { Queue = ..., Delay = ... })` (у немедленных `Delay` не задаётся). `LoadArgs` пачки собираются непосредственно перед вызовом: `EnqueuedAtMicros` — одна метка на пачку, взятая перед вызовом; `RunAtMicros = EnqueuedAtMicros + Delay`. Время ожидания в пачке поэтому не попадает в задержку старта; время самого вызова попадает.
  - Оценка при 10 000/с: 4 ключа, пачки уходят по возрасту 10 мс — немедленные ~72 задачи `default` и ~18 `critical`, отложенные ~8 и ~2; всего ~400 вызовов/с. Отчёт пишет число вызовов/с и средний размер пачки.
- **Подготовка схемы** (все сценарии): старт `IHost` продюсера — `PgBoltInitializer` мигрирует новую схему и создаёт партиции по умолчанию (сегодня + 7 суток); для набора индексов `extra` (§6.2) — `CREATE INDEX` на родителе `jobs` (индекс партиционированного родителя создаётся и на существующих партициях, новые получают его при `ATTACH PARTITION` в `PartitionManager`); `IPgBoltMaintenance.EnsurePartitionsAsync(daysAhead: P − 1)` — сутки от сегодня до сегодня + P − 1, то есть P партиций `jobs`; затем старт хостов. `sustained`, `ramp` и `vacuum` используют P = 15 (`daysAhead: 14`) и `current` — установившееся состояние по умолчанию (§6.2). Обслуживание хостов число партиций не меняет: задача `partitions` досоздаёт только сегодня + 7, retention удаляет только прошлые сутки.
- **Задача** `LoadJob : IJob<LoadArgs>`: `LoadArgs(long Seq, long EnqueuedAtMicros, long RunAtMicros, int WorkMillis, bool FailFirstAttempt, string Padding)`; для немедленных `RunAtMicros = EnqueuedAtMicros`; `Padding` дополняет сериализованные args до ~200 байт. Обработчик: (1) `Interlocked.Increment` счётчика выполнений `Seq` — первым шагом, до броска исключения; (2) на первой попытке (`JobContext.Attempt == 1`) записывает задержку старта в гистограмму: немедленные — `now − EnqueuedAtMicros`, отложенные — `now − RunAtMicros` (повторы не пишутся: их задержка определяется политикой повтора); (3) если `FailFirstAttempt` и попытка первая — бросает исключение; (4) `await Task.Delay(WorkMillis)`.
- **Счётчик выполнений:** зубчатый массив кусков `int[2^20]`, индексируемый `Seq` (кусок `Seq >> 20`, смещение `Seq & (2^20 − 1)`). Внешний массив ссылок создаётся до старта по верхней оценке числа задач прогона (rate и длительности сценария); кусок для очередного диапазона `Seq` выделяет и публикует генератор до выдачи первого `Seq` из него. Поэтому `Interlocked.Increment` в обработчике всегда идёт по уже опубликованному куску, а рост ничего не копирует (рост копированием терял бы инкременты по старому массиву). Без словаря по id: 6–22 млн записей в куче дали бы паузы GC в процессе, где меряются задержки.
- **Часы:** `Stopwatch`-основанные микросекунды от общего начала прогона; хосты и продюсер в одном процессе, поэтому общие часы корректны. `run_at` в БД считается от `now()` сервера и для замера не используется.

### 4.4 Недогруз и ограничение стенда

Окно 10 с — «недогруз», если продюсер поставил в нём (задачи пачек, вызов `EnqueueManyAsync` которых успешно завершился в этом окне) меньше 98 % от rate × 10 с. Во всех сценариях недогруз означает, что не справляется enqueue: все отправители заняты вызовами, вызовы падают или их латентность растёт; канал готовых пачек полон, и токены сверх потолка rate × 1 с отбрасываются (§4.3). Окно недогруза, в котором оценивается SLO 1, — нарушение SLO 1 с пометкой «enqueue» независимо от числа выполненных задач; в `ramp` оно останавливает рост (§5.3). Отчёт для таких окон пишет число вызовов, ошибок и p99 латентности `EnqueueManyAsync`.

Код 2 — только сбой самого стенда: исключение стенда, недоступность БД или контейнеров (для среза — 30 подряд неудачных попыток, §4.5; отдельные неудачные срезы — пропуски отсчёта), неверные аргументы, `max_prepared_statements = 0` у PgBouncer в режиме `bouncer-prepared` (§4.2), либо упор в CPU процесса стенда — загрузка (`Process.TotalProcessorTime` по окнам 10 с) выше 80 % доступных ядер (`Environment.ProcessorCount`) больше чем в 10 % окон замера (в `vacuum` — окон фаз 1–3). Исключения:
- `ramp`: доля упора считается по оцениваемым окнам каждой ступени; ступень, где упор есть больше чем в 10 % её оцениваемых окон, останавливает рост с пометкой «стенд». Это результат, а не сбой: код 0, максимальный устойчивый rate помечается «ограничено стендом» (§5.3);
- `partitions`: доля упора считается по окнам замера каждой комбинации; комбинация с упором помечается «ограничено стендом», код 2 — только если упор у `P = 15, current` хотя бы в одном режиме (§6.3).

При упоре в CPU отчёт пишется со значениями, вердикты SLO помечаются «ограничено стендом».

### 4.5 Измерения

- **Гистограммы задержки** (HdrHistogram, 1 мкс … 60 с, 3 значащие цифры): отдельно немедленные и отложенные задачи; общая за окно замера и по окнам 10 с. Запись из многих потоков — через `Recorder` (интервальная гистограмма каждого окна 10 с добавляется в общую). Значение больше 60 с насыщается до 60 с и учитывается в счётчике переполнений (в отчёте): исключение в обработчике отправило бы задачу в повтор и исказило SLO 5.
- **Счётчики PgBolt** через `MeterListener` на `Meter` `PgBolt` хостов и продюсера: `pgbolt.job.outcome.dropped`, `pgbolt.storage.errors` (по `pgbolt.storage.operation`), `pgbolt.job.enqueued`.
- **Загрузка CPU процесса стенда** по окнам 10 с (§4.4).
- **Срез раз в секунду** (отдельное прямое соединение, не из пулов хостов):
  - выполнено задач всего;
  - готовые не взятые задачи `queue` (SLO 4) — ограниченным счётом по частичному индексу `queue_fetch` (`WHERE lease_owner IS NULL AND ready`): `SELECT count(*) FROM (SELECT 1 FROM queue WHERE ready AND lease_owner IS NULL LIMIT rate + 1) s`. Значение rate + 1 означает «больше порога SLO 4»: первая часть SLO 4 в этом срезе уже нарушена, средние второй части считаются по этим значениям. Полный счёт по таблице каждую секунду читал бы в фазе 2 `vacuum` миллионы мёртвых версий и сам искажал бы измерения фаз 2–3 (SLO 9);
  - отложенные и задачи в lease — раз в 10 с двумя счётами по частичным индексам: `WHERE NOT ready` (`queue_delayed`) и `WHERE lease_owner IS NOT NULL` (`queue_leased`), без чтения всей кучи, раздутой в фазах 2–3 `vacuum` (в отчёт и `.csv`, без порога);
  - соединения: `pg_stat_activity` по `application_name` хостов и продюсера; в режимах через PgBouncer — `SHOW POOLS` админ-консоли (`sv_active`, `sv_idle`, `cl_waiting`, `maxwait`, `maxwait_us`). Админ-консоль PgBouncer принимает только simple query protocol, а Npgsql шлёт команды через extended protocol и при открытии соединения загружает типы, поэтому канал — `psql -At -h pgbouncer -p <порт PgBouncer> -U <пользователь стенда> -d pgbouncer -c "SHOW POOLS"` (пароль — через `PGPASSWORD`) раз в секунду через `ExecAsync` Testcontainers в контейнере `postgres:17-alpine` (psql есть в образе, сеть общая, alias — §4.2). Разбор вывода (строки через `|` без заголовка, порядок колонок PgBouncer 1.24 — образ закреплён; так же `SHOW CONFIG` для `max_prepared_statements`, §4.2) — чистая функция, тест §11.2. При внешнем `--bouncer-connection` канала нет — «нет данных»;
  - ожидание таблицы блокировок: число backend'ов PgBolt с `wait_event_type = 'LWLock' AND wait_event = 'LockManager'` в `pg_stat_activity` (частое чтение `pg_locks` не используется: оно само берёт LWLock'и всех разделов таблицы блокировок и усиливает измеряемую конкуренцию). Backend'ы PgBolt — client backend'ы базы прогона (`backend_type = 'client backend' AND datname = current_database()`), кроме pid соединений стенда: в режимах через PgBouncer `application_name` серверного соединения меняется от транзакции к транзакции и для отбора не годится. С внешней БД в счёт попадают и чужие backend'ы этой базы;
  - `pg_stat_user_tables` для `queue` и `completions`: `n_live_tup`, `n_dead_tup`, `autovacuum_count`, `last_autovacuum`;
  - `pg_total_relation_size` `queue` и `pg_relation_size` каждого её индекса;
  - `pg_stat_wal.wal_bytes`;
  - `pg_stat_checkpointer`: `num_timed`, `num_requested`, `write_time`, `sync_time`, `buffers_written`;
  - `pg_stat_progress_vacuum` по всем отношениям схемы прогона (с `relname`; идёт ли vacuum, фаза): autovacuum партиции `jobs` после миллионов ack-update занимает одного из workers и конкурирует за I/O с проходом по `queue`; число активных autovacuum workers — backend'ы `pg_stat_activity` с `backend_type = 'autovacuum worker'`.

  Нагрузка самого среза отмечается в отчёте: среднее и максимальное время запросов среза (отдельно ежесекундных и 10-секундного счёта по `queue`, который при раздутой таблице читает её целиком).

  Срез запускается по тикам раз в секунду, тики не накапливаются: срез дольше 1 с пропускает тики, пришедшиеся на время его выполнения, следующий срез — на первом тике после завершения. Неудачный запрос среза (ошибка или таймаут запроса, сбой `docker exec psql`, «out of shared memory» в `partitions`) и пропущенный тик — пропуск отсчёта: значение не записывается, статистики и пороги SLO считаются по имеющимся срезам; при ошибке соединение среза переоткрывается на следующей попытке. Число пропусков (отдельно неудачных запросов и пропущенных тиков) — в отчёте. Код 2 — только при 30 подряд неудачных попытках среза (потеря соединения стенда); попытка неудачна, если не выполнился ни один её запрос.
- **Блокировки statement'ов** (`partitions`, один раз на комбинацию до нагрузки): отдельное прямое соединение (не через PgBouncer); для каждого из dequeue, ack, enqueue — `BEGIN; EXPLAIN <SQL statement'а>` с параметрами горячего пути (id задач текущих суток, очередь `default`, `n = 32`). Планировщик блокирует отношения, оставшиеся после отсечения партиций, и их индексы; блокировки держатся до конца транзакции. Затем `SELECT count(*) FILTER (WHERE NOT fastpath), count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'relation' AND relation <> 'pg_locks'::regclass` и `ROLLBACK` (блокировка самого представления `pg_locks` при занятых 16 fast-path слотах попала бы в счёт «вне fast-path»). `EXPLAIN` без `ANALYZE` statement не выполняет, поэтому блокировки исполнителя (маршрутизация вставки enqueue в одну партицию) не учитываются.
  - SQL берётся из `PgBoltStore` схемы прогона: `DequeueSql` уже internal, `AckSql` и `EnqueueSql` становятся internal (меняется только видимость); в `PgBolt.Postgres.csproj` добавляется `InternalsVisibleTo("PgBolt.LoadTest")`.
  - Для `direct` и `bouncer` число измеряется: это custom plan (Npgsql без prepare шлёт безымянный statement).
  - Для `*-prepared` стенд сначала определяет тип плана каждого statement'а на том же измерительном соединении: `PREPARE` (параметры statement'а переписаны в позиционные `$n`), 6× `EXPLAIN EXECUTE` с параметрами горячего пути при `plan_cache_mode = auto`, затем `generic_plans` и `custom_plans` из `pg_prepared_statements` и `DEALLOCATE`. Тип плана пишется в отчёт; он определён до нагрузки, и под нагрузкой (другая статистика таблиц) хосты могут выбрать иной план — это ограничение оценки. Custom plan (`generic_plans = 0`) — значение равно измеренному для `direct`. Generic plan — значение вычисляется: отношения range table (все P партиций `jobs` и прочие таблицы statement'а — родитель `jobs`, `queue`, у ack `completions`; в PG 17 `AcquireExecutorLocks` блокирует их до отсечения) плюс индексы index scan'ов в партициях, переживших начальное отсечение (их открывает `ExecInitNode`): у ack и enqueue — PK оставшихся 1–2 партиций; у dequeue начального отсечения нет (`j.id = l.job_id` — параметр исполнения), поэтому ещё P индексов PK. В отчёте значение помечено «нижняя оценка»: индексы, которые открывает изменение строк, в него не входят. Напрямую блокировки generic plan не измеряются: построение generic plan блокирует при планировании индексы всех партиций (завышение), а `EXPLAIN EXECUTE` уже построенного плана не открывает индексы, которые откроет выполнение (занижение).
- **`pg_stat_statements`**: сброс перед окном замера (в `ramp` — перед первой ступенью, в `vacuum` — перед фазой 1); в конце — `calls`, `mean_plan_time`, `mean_exec_time`, `total_*` по классам; время планирования — только при `pg_stat_statements.track_planning = on` (в своих контейнерах — только в `partitions`, §4.2; во внешней БД — как настроено), иначе колонки плана — «нет данных». Обязательные классы — dequeue, ack, outcome (повтор упавших задач), heartbeat, enqueue, promoter: в профиле B каждый выполняется в любом окне от 20 с (heartbeat — раз в `HeartbeatInterval × (1 ± 0,1)` = 15 с ± 1,5 с на каждый хост, независимо от числа задач в lease; promoter — раз в `PromoterInterval` = 1 с). Необязательный класс — dequeue с лимитом: `DequeueLimitedSql` вызывается только для очередей с кластерным лимитом, а профиль B лимитов не задаёт; класс выводится в отчёт, если найден. Класс определяется по фрагментам текста запроса (константы стенда); тест (§11.2) проверяет, что после короткого прогона каждый обязательный класс находит хотя бы один statement и ни один statement не попадает в два класса.

### 4.6 Отчёт

`<out>/<yyyyMMdd-HHmm>-<scenario>-<mode>.md` и рядом `.csv` с посекундными срезами. Отчёт: окружение (§4.2), параметры прогона, таблица SLO (порог, значение, PASS/FAIL/«нет данных», пометки «enqueue» и «ограничено стендом»), перцентили задержки (p50, p90, p99, p99.9, max) и число переполнений гистограммы, пропускная способность по окнам 10 с, вызовы `EnqueueManyAsync`/с и средний размер пачки, загрузка CPU стенда, сводка `pg_stat_statements`, сводка bloat и autovacuum, checkpoint'ы, ожидания `LockManager`, повторные выполнения, недогруз, ошибки `EnqueueManyAsync` и число `Seq` неуспешных вызовов (§5.2 SLO 5), время запросов среза и число пропусков отсчёта (§4.5), предупреждения окружения (§4.2). Сценарий `partitions` дополнительно пишет сводную таблицу по всем комбинациям (§6.3) и вычисленную ёмкость таблицы блокировок (§6.1).

## 5. Профиль B и SLO (`sustained`, `ramp`)

### 5.1 Профиль нагрузки B
- 10 000 enqueue/с; прогрев 1 мин, окно замера 10 мин.
- Очереди: `default` — 80 %, `critical` — 20 %; оба хоста обслуживают обе очереди.
- 90 % задач немедленные, 10 % — отложенные на равномерно случайные 1–30 с (целые секунды, §4.3).
- 1 % задач падает на первой попытке и повторяется по политике по умолчанию (15 с ± 20 %).
- Работа задачи — 1 мс; args ~200 байт.

### 5.2 SLO нагрузки B

Оцениваются на окне замера отдельно для каждого режима подключения.

| # | SLO | Порог |
|---|---|---|
| 1 | Пропускная способность | каждое окно 10 с: выполнено ≥ 95 % заданного rate; окно недогруза (§4.4) — нарушение с пометкой «enqueue» |
| 2 | Задержка старта немедленных задач (enqueue → start) | p50 ≤ 50 мс, p99 ≤ 500 мс, p99.9 ≤ 2 с |
| 3 | Опоздание отложенных задач (run_at → start) | p99 ≤ 1,5 с (`PromoterInterval` 1 с + 0,5 с) |
| 4 | Backlog готовых не взятых задач | в каждом срезе ≤ rate × 1 с; среднее по срезам последней минуты окна замера ≤ среднее по срезам первой минуты + rate × 1 с |
| 5 | Корректность | к концу прогона (после остановки продюсера и дренажа ≤ 2 мин) все строки `jobs` схемы прогона в состоянии Succeeded, `queue` пуста; `outcome.dropped` = 0; `storage.errors` = 0; счётчик выполнений (§4.3) у `Seq` успешных вызовов `EnqueueManyAsync` = 2 у задач с `FailFirstAttempt` и = 1 у остальных; `Seq` вызовов, завершившихся исключением, из этой сверки исключаются (строки могли быть закоммичены — таймаут после commit — или нет; допустимо 0..2), их число — в отчёте; `jobs.attempt` = 2 у задач с `FailFirstAttempt` и = 1 у остальных по всем строкам `jobs` схемы (сверка одним запросом `count(*) FILTER (...)` по полю `FailFirstAttempt` в `args`, без сравнения с числом `Seq`) |
| 6 | Соединения | `bouncer`: максимальный `maxwait` из `SHOW POOLS` ≤ 100 мс в каждом срезе, `cl_waiting` (максимум и среднее) — в отчёте без порога; `direct`: соединений хостов и продюсера ≤ сумма `Maximum Pool Size` (48) — санитарная проверка |

Дренаж (п. 5) входит в прогон, но не в окно замера. Повторы упавших задач приходят через ~15 с после первой попытки и учитываются в SLO 1 вместе с первыми попытками.

### 5.3 `ramp`
- Старт — `--ramp-start` (2 000/с), каждые `--ramp-step-duration` (60 с) поток растёт на `--ramp-step` (2 000/с) до `--ramp-max` (40 000/с); прогрева и окна замера `--duration` нет; профиль задач — как в §5.1.
- Ступень оценивается по окнам 10 с, начинающимся не раньше трети ступени (при 60 с — последние 40 с; первая треть — переходный процесс): SLO 1 (с недогрузом, §4.4) в каждом из этих окон и p99 SLO 2 по ним вместе.
- Остановка на первой ступени с нарушением; результат — «максимальный устойчивый rate» (последняя ступень без нарушения) и что нарушено первым: SLO 1, SLO 2, «enqueue» (недогруз) или «стенд» (упор в CPU больше чем в 10 % оцениваемых окон ступени, §4.4). При остановке «стенд» максимальный устойчивый rate помечается «ограничено стендом».
- Порогов у `ramp` нет: код возврата 0, если стенд отработал без сбоев, в том числе при остановке «стенд» на любой ступени (это результат, а не сбой); иначе 2.

## 6. Сценарий `partitions`

### 6.1 Вопрос
С какого числа присоединённых партиций `jobs` (P) и индексов на партицию деградируют dequeue, ack и enqueue и как это зависит от prepared statements.

Без prepare Npgsql шлёт безымянный statement, и PG строит custom plan с параметрами как константами. Ack (`j.id = ANY(@ids)`) и enqueue (CTE `existing`: `WHERE id = ANY(@ids)`; вставка маршрутизируется в партицию при выполнении) отсекают партиции на этапе планирования, а отсечённые партиции с PG 12 не блокируются. Dequeue (`JOIN jobs j ON j.id = l.job_id`) условия на ключ партиционирования не имеет: каждый вызов планируется по всем партициям и блокирует их и их индексы.

С prepared statements (`UsePreparedStatements`) после пяти выполнений при `plan_cache_mode = auto` PG выбирает между custom и generic plan по стоимости (`choose_custom_plan`), переход на generic не гарантирован. Generic plan при каждом выполнении блокирует все отношения range table (`AcquireExecutorLocks`), в том числе все P партиций `jobs`, а затем индексы index scan'ов в партициях, переживших начальное отсечение. Вывод по statement'ам:
- **dequeue:** custom plan при планировании блокирует все P партиций и все их индексы (`get_relation_info`) — ≈ 5P при `current` (партиция + 4 индекса), ≈ 7P при `extra`; generic plan — P партиций и P индексов PK (начального отсечения нет) — ≈ 2P. Для dequeue `*-prepared` по блокировкам скорее лучше;
- **ack и enqueue:** custom plan оставляет 1–2 партиции, generic plan блокирует все P — `*-prepared` хуже. При больших P custom plan дешевле generic с Append по P партициям, и PG может остаться на custom — тогда блокировки как в `direct`.

Какой план выбран, стенд определяет для каждого statement'а и комбинации (§4.5).

В PG 17 у backend'а 16 fast-path слотов блокировок, остальные идут в общую таблицу блокировок ёмкостью `max_locks_per_transaction × (MaxBackends + max_prepared_transactions)`, где MaxBackends = `max_connections` + `autovacuum_max_workers` + 1 + `max_worker_processes` + `max_wal_senders`; конкуренция за неё видна как ожидание LWLock `LockManager`. В своих контейнерах (§4.2) это 64 × (100 + 3 + 1 + 8 + 10 + 0) ≈ 7,8 тыс.; отчёт `partitions` пишет ёмкость, вычисленную по фактическим параметрам сервера.

### 6.2 Матрица
- **P:** 15 (установившееся состояние по умолчанию: `JobsRetention` 7 суток + сегодня + `PartitionDaysAhead` 7), 60, 200, 400. Партиции создаёт продуктовый код: `EnsurePartitionsAsync(daysAhead: P − 1)` (§4.3). Будущие партиции планируются и блокируются так же, как прошлые сутки, закреплённые живыми задачами.
- **Индексы:** `current` — PK, `jobs_batch`, `jobs_parent`, `jobs_recurring`; `extra` — плюс два индекса, которые стенд создаёт на родителе `jobs` после инициализации схемы и до `EnsurePartitionsAsync(P − 1)` (§4.3): btree `(type, id)` и GIN `(args jsonb_path_ops)`. Это имитация opt-in индексов, отложенных в части 6; результаты — входные данные для того решения. Индексы `extra` не нарушают контракт HOT (core §4.2): колонки неизменяемы.
- **Режимы:** `direct`, `bouncer`, `direct-prepared`, `bouncer-prepared`.
- **Каждая комбинация:** новая схема, прогрев `--warmup` (30 с) и `--duration` (2 мин) нагрузки B (§5.1) на `--rate` (10 000/с); дренаж не нужен (SLO 5 не оценивается), схема удаляется.

32 комбинации, ~1,8 ч (≈ 3,5 мин на комбинацию с подготовкой и удалением схемы); `--partitions`, `--indexes`, `--mode` сужают матрицу.

### 6.3 Что меряем и как решаем
Для каждой комбинации: SLO 1 и 2 (PASS/FAIL, p99), `mean_plan_time` и `mean_exec_time` dequeue / ack / enqueue из `pg_stat_statements`, блокировки на statement вне fast-path и всего (§4.5: измерение для custom plan, нижняя оценка для generic plan в `*-prepared`), тип плана каждого statement'а в `*-prepared`, максимум и среднее число backend'ов в ожидании `LockManager`, `storage.errors` (в том числе ошибки «out of shared memory» при исчерпании таблицы блокировок), пометка «ограничено стендом» при упоре в CPU (§4.4).

Правило: конфигурация `P = 15, current` обязана выполнять SLO 1 и 2 во всех режимах. Код возврата — 1, если она нарушает SLO хотя бы в одном прогнанном режиме, иначе 0 (нарушения при других P и `extra` на код не влияют; если `P = 15, current` нет в матрице — 0 при отсутствии сбоев стенда). Упор в CPU считается по окнам замера каждой комбинации: комбинация с упором помечается «ограничено стендом», код 2 — только при упоре у `P = 15, current` хотя бы в одном режиме (§4.4). Для остальных комбинаций отчёт показывает порог P — наименьшее P матрицы, с которого SLO нарушается, — по режимам и вариантам индексов. Выводы (§9) делаются только по `current`: порог P ≤ 60 хотя бы в одном режиме — пункт 7b-2 (изменение SQL — например, отсечение партиций по границе `id` в dequeue); порог > 60 или его отсутствие — локальная правка в виде рекомендаций в `docs/performance.md` (`max_locks_per_transaction`, срок хранения и число партиций), значения опций по умолчанию не меняются. Порог `extra` только записывается в `docs/performance.md` как входные данные решения об opt-in индексах (часть 6) и правок не вызывает.

## 7. Сценарий `vacuum`

### 7.1 Проблема
`queue` — таблица с очень высоким churn: на задачу insert, не-HOT update при lease (меняются колонки частичных индексов `queue_fetch` / `queue_leased` / `queue_expiry`), heartbeat и delete при ack. На 10k/с — десятки тысяч мёртвых строк в секунду по таблице с 9 индексами. Сейчас (`V001__initial.sql:19-20`): `autovacuum_vacuum_scale_factor = 0`, `autovacuum_vacuum_threshold = 5000`, `autovacuum_vacuum_cost_limit = 2000`; у `completions` (`:175`) — `scale_factor = 0`, `threshold = 1000`. Ритм между проходами задаёт глобальный `autovacuum_naptime` (по умолчанию 1 мин).

### 7.2 Сценарий
- Прогрев `--warmup` (2 мин) нагрузки B, затем фаза 1 (стационарная): `--phase1` (20 мин) нагрузки B.
- Фаза 2 (удержание xmin): `--hold-xmin` (5 мин) нагрузки B при открытой на прямом соединении транзакции `REPEATABLE READ` с выполненным `SELECT` (держит xmin) — имитация долгого отчёта, аналитики или отстающей реплики.
- Фаза 3 (восстановление): транзакция закрывается, ещё `--phase3` (10 мин) нагрузки B. После фазы 3 продюсер и хосты останавливаются, дренажа нет, схема удаляется.
- `--queue-reloptions "<k=v,...>"` до прогона выполняет `ALTER TABLE queue SET (...)` — сравнение параметров без изменения кода; `--pg-setting "<k=v,...>"` задаёт глобальные параметры своего контейнера PG (§4.2) — для глобального кандидата (§7.4).

### 7.3 SLO

| # | SLO | Порог |
|---|---|---|
| 7 | Размер `queue` в фазе 1 | `pg_total_relation_size(queue)`: среднее по срезам последней минуты фазы 1 ≤ 2 × среднее по срезам за 60–120 с от начала фазы 1; фаза 1 короче 3 мин — «нет данных» |
| 8 | Стабильность в фазе 1 | SLO 1–4 выполняются во всех окнах 10 с фазы 1: перцентили SLO 2 и 3 считаются по каждому окну 10 с; вторая часть SLO 4 сравнивает первую и последнюю минуты фазы 1 |
| 9 | Восстановление | момент восстановления T — начало первого окна 10 с фазы 3, начиная с которого во всех окнах до конца фазы 3 выполняются SLO 1, SLO 2 (p99 по окну 10 с) и первая часть SLO 4; T ≤ начало фазы 3 + 5 мин, иначе FAIL. Вторая часть SLO 4 — на отрезке от T до конца фазы 3: среднее по срезам последней минуты ≤ среднее по срезам первой минуты после T + rate × 1 с (отрезок короче 2 мин — «нет данных») |

Кроме SLO 7–9, в `vacuum` оценивается SLO 6 — по срезам фаз 1–3. SLO 5 не оценивается: дренажа нет (§7.2); `outcome.dropped`, `storage.errors` и повторные выполнения пишутся в отчёт без порога.

Размер в фазах 2–3 и пик `n_dead_tup` записываются в отчёт без порога: после удержания xmin файл таблицы и индексы не уменьшаются без `VACUUM FULL`/`REINDEX`, это поведение PostgreSQL, а не PgBolt.

### 7.4 Кандидаты
Подбираются reloptions только `queue`. `completions` не меняется: строки в неё пишутся только для задач с подписчиками (`... FROM j WHERE notify` в `PgBoltStore.Ack.cs`, так же в Fail/Release/Sweeper), workflows в профиль B не входят (§1), churn около нуля — замерять нечего. Её gauge'и (§8) и строки отчёта остаются наблюдением.

Сравниваются прогоны в обоих режимах: текущие reloptions и базовый кандидат `autovacuum_vacuum_cost_delay = 0` для `queue` (остальные параметры как есть). Другого табличного кандидата нет: при `cost_delay = 0` учёт стоимости в PG 17 выключен (`VacuumUpdateCosts`: `VacuumCostActive = vacuum_cost_delay > 0`), `cost_limit` ни на что не влияет, и более быстрого табличного рычага нет. Кандидат меняет только скорость прохода: пороги запуска (`autovacuum_vacuum_threshold`, `autovacuum_vacuum_insert_threshold`) не подбираются — при десятках тысяч мёртвых строк в секунду любой разумный порог превышается меньше чем за секунду, а момент запуска задаёт `autovacuum_naptime`.

Победитель выбирается по режиму `bouncer` как худшему. Вариант, выполняющий SLO 7–9, всегда выигрывает у невыполняющего. Пиковый размер `queue` в фазе 1 и p99 SLO 2 фазы 1 сравниваются только между выполняющими: вариант лучше другого, если его размер меньше, а p99 не хуже; разница меньше 10 % — равенство (§12); в остальных случаях (в том числе размер меньше, а p99 хуже) — тоже равенство. При равенстве предпочтение — текущим параметрам, если они выполняют SLO 7–9 и в `direct`; иначе — варианту, выполняющему SLO 7–9 в обоих режимах. Если SLO 7–9 в `bouncer` не выполняет ни один табличный вариант, остаются текущие параметры, а находка уходит в 7b-2 (§9), если её не закрывает глобальный кандидат (ниже). Кандидат-победитель принимается, только если в `direct` он тоже выполняет SLO 7–9 и, если текущие параметры в `direct` их выполняют, не хуже текущих по тем же двум показателям; иначе расхождение режимов фиксируется в отчёте и остаются текущие параметры. Итог этого выбора — табличный победитель (принятый кандидат или текущие параметры); он вносится в `V001__initial.sql`.

Глобальный кандидат. Только если базовый кандидат не выполняет SLO 7–9 хотя бы в одном режиме, после выбора табличного победителя выполняется ещё один прогон в обоих режимах: `--pg-setting autovacuum_naptime=10s` (свои контейнеры, §4.2) и `--queue-reloptions` табличного победителя — чаще запускаемые проходы при тех же параметрах таблицы. Он сравнивается с табличным победителем по тем же правилам (худший режим `bouncer`, выполняющий SLO 7–9 выигрывает, размер и p99 с равенством в пределах 10 %, условие для `direct`); при равенстве предпочтение — табличному победителю. Глобальный параметр на уровне таблицы не задаётся, поэтому в `V001__initial.sql` глобальный кандидат не попадает ни при каком исходе; цифры его прогона пишутся в `docs/performance.md`, а выигравший становится там рекомендацией `autovacuum_naptime = 10s` (локальная правка, §9). Если табличный победитель не выполняет SLO 7–9, находка закрывается этой рекомендацией, когда глобальный кандидат выиграл и выполняет SLO 7–9 в обоих режимах; иначе она уходит в 7b-2. Рекомендация `autovacuum_max_workers` опирается только на наблюдение отчёта (число активных autovacuum workers, §4.5), отдельного прогона нет. `autovacuum_vacuum_cost_limit` кластера не рекомендуется: для `queue` его перекрывает табличный `autovacuum_vacuum_cost_limit = 2000`, а при `cost_delay = 0` он не действует.

## 8. Кластерные метрики bloat

### 8.1 Метрики

| Метрика | Тип, единица | Теги | Источник |
|---|---|---|---|
| `pgbolt.cluster.table.dead_tuples` | ObservableGauge, `{tuple}` | `pgbolt.table` = `queue` / `completions` | `pg_stat_user_tables.n_dead_tup` |
| `pgbolt.cluster.table.size` | ObservableGauge, `By` | `pgbolt.table` | `pg_total_relation_size` (таблица, индексы, TOAST) |

Префикс `pgbolt.cluster.` и агрегация `max` — как у кластерных метрик 7a §4.3. Значение — чтение каталога PG в момент прохода пода, поэтому значения подов расходятся не больше, чем на один проход цикла `stats`; `max` корректен для gauge'ей, сравниваемых с порогом алерта.

### 8.2 Чтение
- `PgBoltStore.ReadTableStatsAsync` (internal, рядом с `ReadQueueStatsAsync` в `PgBoltStore.Stats.cs`), один statement:

```sql
SELECT c.relname, coalesce(s.n_dead_tup, 0)::bigint, pg_total_relation_size(c.oid)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE n.nspname = @schema AND c.relname IN ('queue', 'completions')
```

- Результат — internal record'ы `TableStatsRow(Table, DeadTuples, SizeBytes)` в `PgBolt.Postgres.Storage`.
- Проводка — как у `ReadQueueStatsAsync`: `IServerStore.ReadTableStatsAsync`, реализация в `PgBoltServerStore` (делегирует `PgBoltStore`), в тестах — `FakeServerStore` (обработчик по образцу `OnReadQueueStats`).
- Вызывается в том же проходе цикла `stats` (`ProcessingMaintenance.StatsOnceAsync`, 7a §4.4) на каждом поде с серверами, после `PublishQueueStatsAsync`, независимо от `RunMaintenance`.
- Хранение — singleton `ClusterTableStats` в `PgBolt.Server` (регистрирует `AddPgBoltServer`): последние строки и момент успешного чтения по `MonoClock`. Устаревание — правилом 7a: при отсутствии успешного чтения дольше `4 × StatsSnapshotInterval` callbacks возвращают пустой набор.
- Константы: имена instruments — в публичном `PgBoltDiagnostics.Instruments` (`ClusterTableDeadTuples`, `ClusterTableSize`), рядом с `ClusterQueueJobs`; атрибут `pgbolt.table` — в internal `PgBoltTelemetry.Attr` (там же, где `pgbolt.queue`); значение операции — `StorageOperations.TableStatsRead` = `table_stats_read`. Instruments создаются в `PgBoltMetrics` рядом с `pgbolt.cluster.queue.*`.
- Ошибка чтения: `storage.errors` с `pgbolt.storage.operation` = `table_stats_read`, лог через существующий ограничитель ошибок цикла `stats` (раз в минуту), прежние строки не заменяются и стареют; исключение не выходит из прохода. Отмена остановкой — по правилу §3.3 п. 2.
- Callbacks читают только память, исключение callback'а — событие 8002, как в 7a §4.6.

### 8.3 Документация
`docs/observability.md`: таблица метрик, константы `PgBoltDiagnostics.Instruments`, значение `table_stats_read` в таблице `pgbolt.storage.operation`, Prometheus-имена `pgbolt_cluster_table_dead_tuples` (единица `{tuple}` суффикса не даёт, как у `pgbolt_cluster_queue_jobs`) и `pgbolt_cluster_table_size_bytes` (единица `By` → суффикс `_bytes`), метка `pgbolt_table`. PromQL-примеры и рекомендации алертов: `max(pgbolt_cluster_table_dead_tuples{pgbolt_table="queue"})` выше порога дольше 10 мин (autovacuum не успевает или удерживается xmin); рост `max(pgbolt_cluster_table_size_bytes{pgbolt_table="queue"})` за час. Пороги — по результатам `vacuum` (§7) из `docs/performance.md`.

## 9. Исправления по результатам

- **Запланировано заранее:** reloptions `queue` по §7.4; при условии §7.4 — прогон глобального кандидата `autovacuum_naptime=10s` (только рекомендация в docs).
- **Локальная правка** (делается в этой части): параметры таблиц и индексов в `V001__initial.sql`, текст одного SQL-statement без изменения протокола, значение по умолчанию опции, рекомендация конфигурации в docs.
- **Крупная правка** (пункт 7b-2 в roadmap с цифрами из отчёта и ссылкой на него): меняет протокол горячего пути (core §5, worker), затрагивает несколько statement'ов или требует новой опции поведения.
- Отнесение конкретной находки к локальной или крупной фиксирует отчёт прогона; при сомнении — крупная.

Цикл по результатам — отдельные шаги, которые план оформляет отдельными задачами:
1. **Прогон.** Полный прогон сценария в его режимах; отчёты — в `results/`.
2. **Решение по правилам.** Для каждой находки — вывод по детерминированным правилам: §6.3 (порог P), §7.4 (победитель reloptions), классификация «локальная/крупная» выше. Для каждой находки фиксируется показатель, по которому она заведена: SLO или колонка отчёта (например, p99 SLO 2, `mean_exec_time` statement'а). Решение и цифры записываются в `docs/performance.md`; крупная находка — пункт 7b-2 в roadmap, на этом цикл для неё заканчивается.
3. **Правка.** Только для локальной находки: одна правка на задачу; изменение поведения — по TDD, как в блоке A.
4. **Повторный прогон затронутого сценария** в затронутых режимах. Его отчёт заменяет прежний в сводке `docs/performance.md`; прежний остаётся в `results/` для сравнения. Сравнение с прежним прогоном: для находок §6.3 — по порогу P, для §7.4 — по правилу выбора победителя, для прочих локальных правок (текст SQL, значение опции, параметры таблиц и индексов) — по показателю, по которому находка заведена (шаг 2); разница меньше 10 % — равенство (§12). Если повторный прогон не лучше прежнего, правка откатывается, а находка уходит в 7b-2.

## 10. Документация

- **`docs/performance.md`** (новый): профиль B и SLO; запуск стенда и сценариев; окружение замера; сводка результатов (`sustained`, `ramp`, таблица `partitions`, `vacuum`) с условиями измерений; решения цикла §9; рекомендации: срок хранения и число партиций, `max_locks_per_transaction`, глобальные параметры autovacuum (`autovacuum_naptime`, `autovacuum_max_workers`, §7.4), конфигурация PgBouncer (`pool_mode`, `default_pool_size`, `max_prepared_statements` и `UsePreparedStatements`), размер пулов Npgsql.
- **`benchmarks/PgBolt.LoadTest/README.md`**: методика, команды и флаги (§4.1), описание отчёта.
- **`docs/observability.md`**: §8.3.
- **Roadmap**: 7b разбивается на 7b-1 (эта часть, статус) и 7b-2 (`COPY` против `unnest`, Grafana-дашборды и exemplars, крупные находки стенда); пункт про BenchmarkDotNet — закрыт (§2).
- **`CHANGELOG.md`**: исправления блока A, новые метрики, изменение reloptions `queue`, стенд.

## 11. Тестирование

### 11.1 Блок A
По пунктам §3.2–§3.3 — тесты, описанные там. §3.1 меняет только тесты. По §3.4–§3.6 существующие тесты остаются зелёными; утверждения и вызовы меняются только там, где это названо в §3.4–§3.5: новый параметр `LeaseKeeper.Unregister` (§3.4 п. 1), замена хука `ExecutionStarted` ожиданием состояния (§3.4 п. 8), `Assert.Same` и проверка, что `Complete` бросил (§3.5), переименования и переносы helper'ов (`ActivityListener`, `Eventually`). Повторный бенчмарк `JobRunner` без слушателей: аллокации не выше результата 7a (+32 B `JobLogScope` принимаются, §3.7); время — цифры до и после раннего выхода в README без порога (§3.6).

### 11.2 Стенд
Новый проект `tests/PgBolt.LoadTest.Tests` (xUnit v3, как остальные):
- без БД: вычисление SLO 1–9 по синтетическим срезам и гистограммам (граничные значения порогов, статистики минут SLO 4 и SLO 7, момент восстановления SLO 9, «нет данных» на коротких отрезках, дренаж; набор SLO `vacuum` — SLO 6 по фазам 1–3, SLO 5 не оценивается); окна пропускной способности и недогруз как нарушение SLO 1 «enqueue»; SLO 5 с неуспешными вызовами enqueue (их `Seq` исключены из сверки счётчика, число — в отчёте); код возврата (упор в CPU → 2; в `ramp` — остановка «стенд» и код 0; в `partitions` — пометка комбинации «ограничено стендом», код 2 только при упоре у `P = 15, current`, код 1 — по `P = 15, current`); выбор победителя §7.4 (выполняющий SLO 7–9 выигрывает у невыполняющего, равенство в пределах 10 %; при равенстве в `bouncer` текущие параметры, нарушающие SLO 7–9 в `direct`, уступают варианту, выполняющему их в обоих режимах; глобальный кандидат против табличного победителя, в `V001__initial.sql` не попадает); token bucket (с фейковыми часами за 10 с выдано rate × 10 с с допуском ≤ 0,5 % — заметно меньше запаса 2 % порога недогруза, §4.4); сборка пачек продюсера (однородность ключа, одна случайная задержка 1..30 с на отложенную пачку, отправка по 100 задачам и по возрасту 10 мс с фейковыми часами, одна метка `EnqueuedAtMicros` на пачку, `RunAtMicros = EnqueuedAtMicros + Delay`); ограниченный канал пачек (при полном канале генератор ждёт и копит токены числом до потолка rate × 1 с, досылает их после освобождения канала, сверх потолка отбрасывает и считает); счётчик выполнений по `Seq` (куски выделяются до выдачи `Seq`, границы кусков); насыщение гистограммы и счётчик переполнений; срез (§4.5): неудачный запрос и пропущенный тик — пропуск отсчёта и счётчик пропусков, тики не накапливаются при срезе дольше 1 с, код 2 — на 30-й подряд неудачной попытке, не раньше; разбор вывода `psql -At` для `SHOW POOLS` и `SHOW CONFIG` (§4.5) на зафиксированных строках PgBouncer 1.24; разбор аргументов CLI (значения по умолчанию по сценариям: `--connection` без `--bouncer-connection` — `direct`, у `partitions` — `direct` и `direct-prepared`; `partitions` с `--bouncer-connection` без `bouncer-prepared`; `--pg-setting` — пары `-c` команды контейнера; недопустимые сочетания с кодом 2: флаг вне своих сценариев, `*-prepared` и `all` вне `partitions`, `bouncer*` с `--connection` без `--bouncer-connection`, `--bouncer-connection` без `--connection`, `--pg-setting` с `--connection`, длительности не кратны 10 с); классификация statement'ов по фрагментам на фиксированных текстах; рендер отчёта (наличие таблицы SLO и окружения);
- `[Trait("Category", "Load")]`, с Docker, каждый сценарий в коротком режиме внутри процесса:
  - `sustained --mode direct --rate 500 --warmup 5s --duration 20s`;
  - `sustained --mode bouncer --rate 500 --warmup 5s --duration 20s` — путь через свой PgBouncer (`AUTH_TYPE=scram-sha-256`, канал `psql` к админ-консоли, §4.2, §4.5);
  - `ramp --mode direct --ramp-start 500 --ramp-step 500 --ramp-step-duration 30s --ramp-max 1000`;
  - `partitions --mode direct --partitions 15,20 --indexes current --rate 500 --warmup 5s --duration 20s`;
  - `vacuum --mode direct --rate 500 --warmup 5s --phase1 20s --hold-xmin 10s --phase3 20s`.

  Проверяется только, что отчёт создан и код возврата ∈ {0, 1} (не 2); пороги SLO в smoke не утверждаются. После короткого `sustained --mode bouncer` срезы `SHOW POOLS` разобраны и не пусты (есть строки пула базы прогона). После короткого `sustained` каждый обязательный класс `pg_stat_statements` (§4.5) находит хотя бы один statement.

### 11.3 Метрики bloat
- `PgBolt.Postgres.Tests`: `ReadTableStatsAsync` возвращает обе таблицы схемы и только их (другая схема в той же БД не видна); размер > 0; `n_dead_tup` > 0 после вставки и удаления строк `queue`: вставка, удаление и `SELECT pg_stat_force_next_flush()` выполняются на одном открытом соединении вне явной транзакции (функция действует на текущий backend, сброс статистики происходит, когда он переходит в idle после этого statement'а), затем чтение `ReadTableStatsAsync`.
- `PgBolt.Server.Tests`: публикация `ClusterTableStats` через `MetricCollector` после прохода цикла `stats` (`FakeServerStore`); устаревание (фейковые часы); ошибка чтения — `storage.errors{table_stats_read}`, прежние значения до устаревания; под без серверов не публикует; при `RunMaintenance = false` публикует.

### 11.4 Полный прогон
`dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный; `Category=Load` (существующий `LoadSmokeTests` и smoke стенда) — зелёный на машине разработчика перед финальным ревью.

## 12. Риски

- **Окружение.** Docker for Mac (виртуализация, fsync виртуального диска) может стать узким местом раньше PgBolt; отчёт фиксирует окружение, `ramp` показывает потолок, `--connection` позволяет повторить на выделенном PG. Сравнения внутри одного прогона (режимы, P, reloptions) от этого не теряют смысла.
- **Шум.** Однократные прогоны на ноутбуке шумные; пороги SLO заданы с запасом, а сравнения кандидатов (§7.4) и повторных прогонов (§9) при разнице меньше 10 % считаются равенством. Checkpoint'ы видны в отчёте (`pg_stat_checkpointer`), чтобы их всплески не приписывались PgBolt.
- **Длительность.** Полный набор — около 5,5 ч без глобального кандидата §7.4: `sustained` 2 режима × ~14 мин (прогрев 1 + замер 10 + дренаж ≤ 2 + подготовка) ≈ 0,5 ч; `ramp` 2 × до ~22 мин (до 20 ступеней по 1 мин + подготовка) ≈ 0,7 ч; `partitions` 32 × ~3,5 мин ≈ 1,8 ч; `vacuum` 2 режима × 2 варианта (текущие параметры и базовый кандидат) × ~38 мин (2 + 20 + 5 + 10 + подготовка) ≈ 2,5 ч. Глобальный кандидат `vacuum` (`autovacuum_naptime=10s`, только если базовый не выполняет SLO 7–9 хотя бы в одном режиме) — ещё 2 режима × ~38 мин ≈ 75 мин (свои контейнеры поднимаются заново с `-c`, подготовка входит в ~38 мин), итого до ~6,8 ч; повторные прогоны §9 — сверх этого. Прогоны идут в фоне отдельной задачей, задачи разработки проверяются коротким режимом.
- **Диск.** Прогон `vacuum` даёт ~22 млн строк `jobs` (10–15 ГБ данных), плюс WAL до `max_wal_size` (4 ГБ). Схема удаляется после каждого прогона; в образе диска Docker нужно не меньше 30 ГБ свободного места, отчёт пишет размер БД в конце прогона.
- **Мелкие пачки.** ~400 вызовов `EnqueueManyAsync`/с, у отложенных пачки по 2–8 задач (§4.3), могут занять отправителей продюсера и дать недогруз «enqueue», который отражает стенд, а не PgBolt (задачи при этом не копятся в памяти: канал пачек ограничен, §4.3). Отчёт показывает вызовы/с и средний размер пачки; если недогруз объясняется ими, правится стенд (возраст пачки, число отправителей), не продукт.
- **Разрастание.** Находки стенда могут потянуть изменения горячего пути; правило §9 ограничивает часть локальными правками.
- **Стенд в одном процессе.** Продюсер, два хоста и сбор срезов делят CPU и пул потоков; отчёт пишет загрузку CPU процесса стенда по окнам 10 с, упор выше 80 % доступных ядер больше чем в 10 % окон замера даёт код 2 (в `ramp` — остановку с пометкой «стенд», в `partitions` — пометку комбинации, §4.4).
