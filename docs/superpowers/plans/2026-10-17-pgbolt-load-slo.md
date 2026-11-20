# PgBolt 7b-1 — нагрузочный стенд, SLO и follow-up 7a: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть отложенные Minor части 7a, добавить кластерные метрики bloat, построить консольный нагрузочный стенд `benchmarks/PgBolt.LoadTest` со сценариями `sustained` / `ramp` / `partitions` / `vacuum` и SLO нагрузки B, прогнать его и внести локальные исправления по результатам.

**Architecture:** Блок A — точечные правки `PgBolt.Server` / `PgBolt.Postgres` и тестов без изменения протоколов. Метрики bloat — новый statement `ReadTableStatsAsync` и singleton `ClusterTableStats` в существующем цикле `stats`. Стенд — консольное приложение: чистая логика (CLI, token bucket, пачки, счётчики, SLO, отчёт) отделена от инфраструктуры (контейнеры, хосты PgBolt в одном процессе, срезы PG) и покрыта тестами без БД; инфраструктура проверяется коротким smoke `Category=Load`. Прогоны и решения — отдельными задачами в конце.

**Tech Stack:** .NET 10, Npgsql 10, xunit.v3 3.1.0, Testcontainers 4.15.0 (PostgreSQL 17, PgBouncer 1.24.1), HdrHistogram 2.5.0, `Microsoft.Extensions.Hosting` 10.0.0, `Microsoft.Extensions.Diagnostics.Testing` 10.0.0.

**Spec:** `docs/superpowers/specs/2026-10-17-pgbolt-load-slo-design.md` (далее «спека», ссылки `§N`). Исполнитель читает спеку и план вместе; значения, пороги и правила спеки в плане не повторяются, если на них есть ссылка.

## Global Constraints

- Ветка `feature/load-slo` (от `main` 810843f). Коммиты — автор `NightNo1r <57496218+NightNo1r@users.noreply.github.com>`, **без** `Co-Authored-By`: `git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "…"`. Сообщения — на русском с conventional-префиксом (`fix(server):`, `feat(loadtest):`, `test:`, `docs:` …), как в истории.
- Комментарии и XML-doc нового и изменённого кода — на русском, по сути, **без ссылок** на спеку и план (`§N`, «часть X», «Task N», «7a», «7b»); плотность как у соседнего кода.
- `TreatWarningsAsErrors=true`, `Nullable=enable`; у `src/` (кроме Dashboard) `IsAotCompatible=true`: без reflection на горячем пути.
- Версии пакетов — только в `Directory.Packages.props`. Новые: `HdrHistogram` 2.5.0 (только стенд), `xunit.v3.assert` 3.1.0 (только `PgBolt.Testing`). Продуктовые проекты новых пакетов не получают.
- Изменения схемы — только в `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql`.
- Тексты существующих лог-сообщений, `EventId`, категории логгеров и имена метрик 7a не меняются.
- Новые имена instruments — константы `PgBoltDiagnostics.Instruments`; атрибуты — `PgBoltTelemetry.Attr`; значения операции — `StorageOperations` (§8.2).
- Тесты с БД требуют Docker. Класс: `dotnet test <csproj> --filter "FullyQualifiedName~<Class>"`. Перед каждым коммитом задач блока A и метрик — полный прогон `dotnet test PgBolt.slnx --filter "Category!=Load"`. Smoke стенда: `dotnet test tests/PgBolt.LoadTest.Tests --filter "Category=Load"`.
- Тесты стенда без БД не помечаются `Category`; тесты стенда с Docker — `[Trait("Category", "Load")]` и `[Collection("Load")]` (Task 11).
- Команды длиннее ~2 мин (прогоны стенда, Task 20–22) запускаются в фоне (`run_in_background`), результат ждётся по завершении процесса; Docker-контейнеры останавливать только свои (по id/имени).

## Review Focus

1. **Недоступная БД или Docker при запуске стенда** — ожидается быстрый выход с кодом 2 и понятным сообщением, а не зависание или стек. Тест: Task 15 `Unreachable_connection_exits_with_code_2` (`--connection "Host=127.0.0.1;Port=1;Timeout=3"`).
2. **Прерывание прогона (Ctrl+C)** — внешняя схема удаляется, контейнеры освобождаются, частичный отчёт пишется с пометкой «прерван», код 2. Тест: Task 15 `Cancelled_run_drops_schema_and_writes_partial_report`.
3. **Несуществующий каталог `--out`** — создаётся. Тест: Task 15 `Report_writer_creates_output_directory`.
4. **Внешняя БД без `pg_stat_statements`** — колонки «нет данных», прогон не падает. Тест: Task 14 `Statements_reader_without_extension_returns_no_data` (`PostgresFixture` расширение не загружает).
5. **Малый rate или короткое окно** (rate 1, окно меньше 10 с, нет ни одной отложенной задачи) — SLO «нет данных», без деления на ноль и исключений. Тест: Task 13 `Empty_inputs_give_no_data`.

---

## Карта файлов

| Файл | Ответственность | Task |
|---|---|---|
| `tests/PgBolt.Testing/Eventually.cs`, `ActivityCapture.cs`, `PgBolt.Testing.csproj` | общие ожидание и захват span'ов | 1, 7 |
| `src/PgBolt.Server/Runtime/JobRunner.cs`, `OutcomeClassifier.cs` | duration, снимок причины, ранний выход | 2 |
| `src/PgBolt.Postgres/Diagnostics/ProducerSpan.cs`, `PgBoltJobClient.cs`, `WorkflowBuilder.cs`, `RecurringJobManager.cs` | порядок восстановления, именованные аргументы | 3 |
| `src/PgBolt.Server/Runtime/CompletionSink.cs`, `LeaseKeeper.cs`, `ProcessingMaintenance.cs` | ложные `storage.errors` | 4 |
| `LeaseKeeper.cs`, `PgBoltServer.cs`, `Dispatcher.cs` | abandoned в `Unregister`, хук `ExecutionStarted` | 5 |
| `src/PgBolt.Postgres/Diagnostics/GuardedActivity.cs`, `GuardedInstruments.cs`; `PgBoltMetrics.cs`, `PgBoltClientMetrics.cs`, `ProcessingMaintenance.cs`, `PgBoltProcessingService.cs` | общие helper'ы и упрощения | 6 |
| тесты `PgBolt.*.Tests` | гигиена тестов | 7 |
| комментарии `src/`, `docs/observability.md`, README бенчмарка, `CHANGELOG.md` | документация блока A | 8 |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Stats.cs`, `TableStatsRecords.cs`; `IServerStore.cs`, `PgBoltServerStore.cs`, `FakeServerStore.cs` | чтение статистики таблиц | 9 |
| `src/PgBolt.Server/Runtime/ClusterTableStats.cs`, `PgBoltMetrics.cs`, `ProcessingMaintenance.cs`, `PgBoltDiagnostics.cs`, `PgBoltTelemetry.cs`, `StorageOperations` | gauge'и bloat | 10 |
| `benchmarks/PgBolt.LoadTest/Cli/*`, `Env/PostgresCommand.cs` | аргументы, команда контейнера PG | 11 |
| `benchmarks/PgBolt.LoadTest/Load/*` | профиль B, token bucket, пачки, конвейер продюсера, счётчик | 12 |
| `benchmarks/PgBolt.LoadTest/Measure/LatencyRecorder.cs`, `Windows.cs`; `Slo/*` | гистограммы, окна, SLO, коды возврата | 13 |
| `benchmarks/PgBolt.LoadTest/Env/*`, `Measure/Sampler*.cs`, `Measure/Statement*.cs`; видимость `AckSql`/`EnqueueSql`, `InternalsVisibleTo` в `PgBolt.Postgres.csproj` | контейнеры, PgBouncer-консоль, срезы, `pg_stat_statements` | 14 |
| `benchmarks/PgBolt.LoadTest/Run/*`, `Report/*`, `Scenarios/Sustained.cs`, `Program.cs` | хосты, продюсер, задача, отчёт, `sustained` | 15 |
| `Scenarios/Ramp.cs` | `ramp` | 16 |
| `Scenarios/Partitions.cs`, `Measure/LockProbe.cs` | `partitions` | 17 |
| `Scenarios/Vacuum.cs` | `vacuum` | 18 |
| `benchmarks/PgBolt.LoadTest/README.md` | README стенда | 19 |
| `benchmarks/PgBolt.LoadTest/results/*`, `docs/performance.md` | прогоны и решения | 20–22 |
| `docs/performance.md`, `docs/observability.md`, roadmap, `CHANGELOG.md` | итоговая документация | 23 |

---

### Task 1: Флаки тестов и общий `Eventually`

**Files:**
- Create: `tests/PgBolt.Testing/Eventually.cs`
- Delete: `tests/PgBolt.Server.Tests/Fakes/Eventually.cs`
- Modify: `Directory.Packages.props` (`xunit.v3.assert` 3.1.0), `tests/PgBolt.Testing/PgBolt.Testing.csproj` (`PackageReference xunit.v3.assert`), `tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs`, `tests/PgBolt.Postgres.Tests/QueueStatsSnapshotTests.cs:60,72,90`, `tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs:332`, все использования `Eventually` в `PgBolt.Server.Tests` (namespace)

**Interfaces:**
- Produces: `public static class Eventually` в namespace `PgBolt.Testing`: `Task True(Func<bool> condition, string because, TimeSpan? timeout = null)` и асинхронная перегрузка, если она есть в текущем `Fakes/Eventually.cs` (те же сигнатуры + `timeout`); по умолчанию 5 с, интервал опроса — как сейчас.

- [ ] **Step 1:** Перенести `Eventually` в `PgBolt.Testing` (`public`, параметр `timeout`), удалить старый файл, поправить `using` в `PgBolt.Server.Tests`. Run: `dotnet build PgBolt.slnx` — без ошибок.
- [ ] **Step 2:** `DispatcherTests.Jobs_of_dequeue_returning_after_its_timeout_are_released_not_run`: проверку лога `DequeueTimedOutJobsReleased` после двух Release обернуть в `await Eventually.True(() => logs.Entries.Any(...), "...")`; продукт (`Dispatcher.cs`) не менять (§3.1 п. 1).
- [ ] **Step 3:** `QueueStatsSnapshotTests`: строки результата `ReadQueueStatsAsync` сортировать в тесте по `Queue` перед проверкой (`:72`); границы `:60`, `:90` — строгое неравенство без `double.Epsilon` (§3.1 п. 2–3).
- [ ] **Step 4:** `ProcessingServiceTests.Maintenance_runs_only_when_enabled` (`:332`): заменить проверку сразу после старта ожиданием наблюдаемого события через `Eventually.True`; отрицательная проверка («не запустилось») — после положительного ожидания события соседнего цикла, а не после фиксированной паузы.
- [ ] **Step 5:** Run 5 раз подряд: `for i in 1 2 3 4 5; do dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~DispatcherTests|FullyQualifiedName~ProcessingServiceTests" || break; done` и `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~QueueStatsSnapshotTests"`. Expected: все PASS.
- [ ] **Step 6:** Полный прогон (Global Constraints) — PASS. Commit `test: устранить флаки DispatcherTests и QueueStatsSnapshotTests, общий Eventually в PgBolt.Testing`.

---

### Task 2: `JobRunner` — длительность, снимок причины, ранний выход без слушателя

**Files:**
- Modify: `src/PgBolt.Server/Runtime/JobRunner.cs:60-135,169-225`, `src/PgBolt.Server/Runtime/OutcomeClassifier.cs:11-20`, вызовы `OutcomeClassifier.Classify` в тестах
- Test: `tests/PgBolt.Server.Tests/Unit/JobRunnerTelemetryTests.cs`, `tests/PgBolt.Server.Tests/Unit/OutcomeClassifierTests.cs` (если есть — иначе в `JobRunnerTests.cs`)
- Bench: `benchmarks/PgBolt.Server.Benchmarks/results/`

**Interfaces:**
- Produces: `OutcomeClassifier.Classify(Execution execution, CancelReason reason, Exception? error, RetryDecider retry, TimeSpan? timeout)` — причина передаётся снимком; `TagOutcome` и `LogOutcome` получают тот же `CancelReason reason` параметром и не читают `execution.FirstReason`.

- [ ] **Step 1: baseline бенчмарка до изменения.** Run: `dotnet run -c Release --project benchmarks/PgBolt.Server.Benchmarks -- --filter '*JobRunner*'`; скопировать `BenchmarkDotNet.Artifacts/results/*JobRunnerBenchmarks-report-github.md` в `benchmarks/PgBolt.Server.Benchmarks/results/JobRunnerBenchmarks-before-early-exit-report-github.md`.
- [ ] **Step 2: тесты** (`JobRunnerTelemetryTests`, коллекция `TelemetryCollection`):
  - `Duration_excludes_retry_policy_time` — `RetryPolicy.Custom((attempt, error) => { time.Advance(TimeSpan.FromMilliseconds(300)); return TimeSpan.FromSeconds(1); })` с `FakeTimeProvider` часов runner'а, задача бросает → записанная `pgbolt.job.duration` < 0,3 с.
  - `Reason_set_after_classification_reaches_neither_span_nor_log` — задача бросает обычное исключение без отмены (`FirstReason = None` на момент снимка; `Execution.Cancel` пишет причину только из `None`, поэтому при уже отменённой задаче гонку не воспроизвести); `RetryPolicy.Custom`, вызываемая при классификации, выполняет `execution.Cancel(CancelReason.HeartbeatLost)` (execution захвачен тестом); `error.type` span'а — полное имя типа исключения (не `heartbeat_lost`), `Reason` в логе `JobFailedWillRetry` — `None`, то есть совпадает с причиной, по которой классифицирован исход.
  - `No_listener_skips_trace_parent_parsing_but_resets_current` — без `ActivityListener` на `PgBolt`, внутри внешней `Activity` (слушатель на другой источник): задача видит `Activity.Current == null`; некорректный `TraceParent` не даёт ни лога, ни исключения.
- [ ] **Step 3:** Run — первые два FAIL.
- [ ] **Step 4:** Реализация: `var reason = execution.FirstReason;` и `var duration = _clock.Elapsed(startedAt);` — сразу после `ExecuteAsync` (в ветке NoExecutor — до `OutcomeClassifier.NoExecutor`), до классификации; `reason` передаётся в `Classify`, `TagOutcome`, `LogOutcome`. В `StartSpan`: `Activity.Current = null;` затем `if (!Source.HasListeners()) return null;` до `ActivityContext.TryParse` — сброс `Activity.Current` сохраняется, он нужен и без слушателя `PgBolt`.
- [ ] **Step 5:** Run `JobRunnerTelemetryTests`, `JobRunnerTests`, `OutcomeClassifier*` — PASS.
- [ ] **Step 6:** Бенчмарк после: та же команда; отчёт — `results/JobRunnerBenchmarks-after-early-exit-report-github.md`. Expected: `Allocated` не выше baseline (§3.7, §11.1).
- [ ] **Step 7:** Полный прогон — PASS. Commit `fix(server): длительность и причина исхода снимаются до классификации, без слушателя span не разбирается`.

---

### Task 3: `ProducerSpan` — порядок восстановления, именованные аргументы, без форматирования без слушателя

**Files:**
- Modify: `src/PgBolt.Postgres/Diagnostics/ProducerSpan.cs:55-57,91-93`, `src/PgBolt.Postgres/Client/PgBoltJobClient.cs:16`, `src/PgBolt.Postgres/Client/Workflows/WorkflowBuilder.cs:166`, `src/PgBolt.Postgres/Client/RecurringJobManager.cs:49`
- Test: `tests/PgBolt.Postgres.Tests/Diagnostics/ProducerSpanTests.cs`, `ProducerTracingTests.cs` (коллекция `Serial`)

**Interfaces:**
- Consumes: `PgBoltInstrumentation.ReportFailure`.
- Produces: поведение — в момент записи 8001 `Activity.Current` равен внешнему (до enqueue) span'у; без слушателя `PgBolt` имя `enqueue {type}` не строится. Сигнатура: `ProducerSpan.Start(PgBoltInstrumentation instrumentation, string operation, string? queue, string? jobType, int messageCount, string? recurringId)` — вместо готового `name` принимает `operation` (`"enqueue"`, `"workflow submit"`, `"recurring trigger"`) и строит имя span'а сам (`jobType is null ? operation : $"{operation} {jobType}"`) после проверки `Source.HasListeners()`; `PgBoltJobClient.StartSpan` передаёт `"enqueue"` и `type`. Без слушателя `PgBolt` span не создаётся, `TraceParent = CurrentTraceParent()` (id внешней `Activity`).

- [ ] **Step 1: тесты:**
  - `Failure_is_reported_after_current_is_restored` — два случая: бросающий `ActivityListener.ActivityStopped` на `PgBolt` (путь `Dispose`) и бросающий `ActivityStarted` (путь `catch` в `Start`); внешний span `outer`; `ListLogger.OnEntry` захватывает `Activity.Current?.SpanId` при записи 8001 → в обоих случаях равен `outer.SpanId`.
  - `Without_listener_no_span_and_trace_parent_is_outer_activity_id` — поведенческий: без слушателя `PgBolt`, внутри `Activity` другого источника (со своим слушателем) `ProducerSpan.Start` span не создаёт (`PgBolt`-span'ов нет), а `TraceParent` равен id внешней `Activity`; со слушателем `PgBolt` создаётся span `enqueue {type}`.
- [ ] **Step 2:** Run — первый FAIL (второй тест фиксирует поведение без слушателя и остаётся регрессионным).
- [ ] **Step 3:** Реализация: в обоих местах сначала восстановить `Activity.Current`, затем `ReportFailure`; ранний выход `if (!Source.HasListeners())` до построения имени (сигнатура — в Interfaces); вызовы `ProducerSpan.Start` в трёх файлах — именованные аргументы вместо позиционных `null`.
- [ ] **Step 4:** Run `ProducerSpanTests`, `ProducerTracingTests` — PASS; полный прогон — PASS.
- [ ] **Step 5:** Commit `fix(postgres): ProducerSpan восстанавливает Activity.Current до отчёта о сбое`.

---

### Task 4: Ложные `storage.errors`

**Files:**
- Modify: `src/PgBolt.Server/Runtime/CompletionSink.cs:224-280,413-414`, `src/PgBolt.Server/Runtime/LeaseKeeper.cs:180-198`, `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs:340-362`
- Test: `tests/PgBolt.Server.Tests/Unit/CompletionSinkTests.cs`, `LeaseKeeperTests.cs`, `ProcessingMaintenanceTests.cs`

**Interfaces:**
- Produces: правило — `storage.errors` учитывает только исключение самого обращения к хранилищу; при отменённом токене остановки любое исключение — остановка без метрики и лога ошибки.

- [ ] **Step 1: тесты:**
  - `CompletionSinkTests.Throwing_logger_after_successful_ack_is_not_a_storage_error` — лог после ответа БД пишется только при частичном ack (`AckPartialLeaseLost`) и при `false` из `SendOutcomeAsync` (`OutcomeDiscardedLeaseLost`): `FakeServerStore` на ack возвращает подмножество отправленных id, `ListLogger.OnEntry` бросает на `AckPartialLeaseLost` → `storage.errors{ack}` = 0, `FakeServerStore` получил ack ровно один раз. То же `..._after_discarded_outcome_...` для `outcome`: фейк возвращает `false`, `OnEntry` бросает на `OutcomeDiscardedLeaseLost`. В обоих тестах дополнительно: записи сняты из `ExecutionTable` (вызван `onLeaseFreed` / таблица пуста) и `outcome.dropped{lease_lost}` не больше числа реально потерянных id (при сбое логгера `Finish` всё равно выполнился); после обоих тестов `StopAsync` sink'а завершается без исключения.
  - `LeaseKeeperTests.Non_cancellation_exception_during_stop_is_not_a_storage_error` — heartbeat фейка ждёт отмены токена и бросает `NpgsqlException` (или `IOException`) → после остановки `storage.errors{heartbeat}` = 0, лога `HeartbeatFailed` нет.
  - `ProcessingMaintenanceTests.Non_cancellation_exception_during_stop_is_not_counted` — задача цикла бросает не-OCE после отмены `_stop` → `storage.errors` = 0, лога `MaintenanceLoopIterationFailed` нет.
  - `CompletionSinkTests.Drop_reason_and_log_agree_when_abort_changes` — `_abort` читается один раз: причина `outcome.dropped` и выбранный лог соответствуют одному значению. Оба чтения `_abort` стоят в `Abandon` после отправки, между ними только `Drop` (`_table.Remove` и `OutcomeDropped`), поэтому тест останавливает sink (отменяет `_abort`) синхронно из `MeterListener` на измерении `pgbolt.job.outcome.dropped`: при двух чтениях второе увидело бы другое значение.
- [ ] **Step 2:** Run — FAIL (кроме, возможно, последнего — тогда он остаётся регрессионным).
- [ ] **Step 3:** Реализация: в `CompletionSink` `try` охватывает только вызов хранилища, лог успешного ответа — после `try` и последним, в своём `try { … } catch { }` (сбой логгера глотается, как в `PgBoltServer.AbandonFailed`; иначе исключение вышло бы из задачи `SendAckBatchAsync`/`SendSingleAsync`, запущенной через `StartSend`, осталось бы ненаблюдённым и могло бы быть проброшено `Task.WhenAll` из `StopAsync`): порядок «ответ БД → учёт (`CountAcked`, `OutcomeDropped{lease_lost}` для потерянных) → `Finish` всех записей → лог», так как без `Finish` записи остались бы в `ExecutionTable` в `OutcomeInFlight`; `catch (Exception) when (ct.IsCancellationRequested) { return; }` перед общим `catch` в `LeaseKeeper` и `ProcessingMaintenance.LoopAsync` (порядок `catch` сохраняет обработку таймаута heartbeat); `_abort` — в локальную переменную один раз.
- [ ] **Step 4:** Run трёх классов — PASS; полный прогон — PASS.
- [ ] **Step 5:** Commit `fix(server): storage.errors без сбоев логгера и исключений остановки`.

---

### Task 5: abandoned в `LeaseKeeper.Unregister`, хук `ExecutionStarted`

**Files:**
- Modify: `src/PgBolt.Server/Runtime/LeaseKeeper.cs:88`, `src/PgBolt.Server/Runtime/PgBoltServer.cs:69,113,206-207,256-260,291-300`, `src/PgBolt.Server/Runtime/Dispatcher.cs:256-257`, тесты с `Unregister` и `ExecutionStarted`

**Interfaces:**
- Produces: `public bool LeaseKeeper.Unregister(Execution execution, string? dropReason = null)` — при `true` и `dropReason` не `null` пишет `outcome.dropped{queue, dropReason}`; `PgBoltServer.AbandonRunning` удалён (цикл со снятием и логом `JobAbandoned` — в месте вызова `:113` через `Unregister(execution, DropReasons.Abandoned)`).

- [ ] **Step 1: тест** `LeaseKeeperTests.Unregister_with_reason_counts_dropped_once` — `Unregister(e, DropReasons.Abandoned)` дважды → `outcome.dropped{abandoned}` = 1; без причины — 0.
- [ ] **Step 2:** Run — FAIL (нет перегрузки).
- [ ] **Step 3:** Реализация; `Dispatcher:256` и `AbandonFailed` — `Unregister(execution, DropReasons.Abandoned)` без отдельной метрики.
- [ ] **Step 4:** `ExecutionStarted`: найти тесты, использующие хук (`grep -rn ExecutionStarted tests`). Заменить ожиданием наблюдаемого состояния (`Eventually.True(() => server.Running.Count == n)` или аналог, доступный тестам). Если тест без хука недетерминирован (нужно получить `Task` выполнения), хук остаётся `internal` и вызывается **до** записи `_running[...]`; решение записать в сообщение коммита.
- [ ] **Step 5:** Run `LeaseKeeperTests`, `DispatcherTests`, `RunExecutionTests`, `ShutdownTests` — PASS; полный прогон — PASS.
- [ ] **Step 6:** Commit `refactor(server): учёт abandoned в LeaseKeeper.Unregister`.

---

### Task 6: Общие helper'ы телеметрии и упрощения обслуживания

**Files:**
- Create: `src/PgBolt.Postgres/Diagnostics/GuardedActivity.cs`, `src/PgBolt.Postgres/Diagnostics/GuardedInstruments.cs`
- Modify: `src/PgBolt.Server/Runtime/JobRunner.cs` (`StopSpan`), `src/PgBolt.Postgres/Diagnostics/ProducerSpan.cs`, `src/PgBolt.Server/Runtime/PgBoltMetrics.cs:134-145`, `src/PgBolt.Postgres/Diagnostics/PgBoltClientMetrics.cs:21-30`, `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs:175,240-266`, `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`, `src/PgBolt.Server/Runtime/PgBoltServer.cs`, конструкторы/вызовы с лишним `PgBoltInstrumentation`, включая места создания `JobRunner` и `PgBoltServer`: `benchmarks/PgBolt.Server.Benchmarks/JobRunnerBenchmarks.cs:53-55` и тесты (`grep -rn "new JobRunner(\|new PgBoltServer(" tests benchmarks`: `JobRunnerTests`, `JobRunnerTelemetryTests`, тесты `PgBoltServer`)
- Test: `tests/PgBolt.Postgres.Tests/Diagnostics/GuardedInstrumentsTests.cs`

**Interfaces:**
- Produces:
  - `internal static class GuardedActivity` — `static void Stop(Activity span, Activity? restore, PgBoltInstrumentation instrumentation)`: остановка span'а с перехватом; порядок «остановить, восстановить `Activity.Current = restore`, затем `ReportFailure` при сбое» (как после Task 3). `ProducerSpan.Dispose` передаёт сохранённый `_saved` (восстановление всегда, как в его `finally`), `JobRunner.StopSpan` передаёт `null`. Один владелец логики.
  - `internal static class GuardedInstruments` — `static void Add(Counter<long> counter, long value, in TagList tags, PgBoltInstrumentation instrumentation)` (при `value <= 0` — без измерения) и `static void Record(Histogram<double> histogram, double value, in TagList tags, PgBoltInstrumentation instrumentation)` (0 записывается).
  - `ProcessingMaintenance.PublishQueueStatsAsync` (переименование приватного `ReadQueueStatsAsync`).
  - `PgBoltInstrumentation PgBoltMetrics.Instrumentation { get; }` — `JobRunner` и `PgBoltServer` берут экземпляр оттуда вместо отдельного параметра конструктора; места создания (бенчмарк, тесты) обновляются.

- [ ] **Step 1: тесты** `GuardedInstrumentsTests`: `Add_skips_non_positive_counts` (0 и −1 → нет измерений в `MetricCollector`), `Record_keeps_zero` (0 → одно измерение 0), `Throwing_listener_is_reported_not_thrown` (бросающий `MeterListener` → 8001, вызов не бросает; слушатель включает только instruments собственного `Meter` теста — `instrument.Meter == meter`, — чтобы параллельные тесты сборки не получили его исключения).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация helper'ов, перевод `PgBoltMetrics` и `PgBoltClientMetrics` на `GuardedInstruments`, `JobRunner`/`ProducerSpan` — на `GuardedActivity`; удалить проверку `Fired > 0` (`ProcessingMaintenance.cs:175`); убрать параметр `PgBoltInstrumentation` там, где рядом передаётся `PgBoltMetrics` (`grep -rn "PgBoltInstrumentation instrumentation" src/PgBolt.Server`); один `GetRequiredService<PgBoltMetrics>` в `PgBoltProcessingService`; `StatsOnceAsync` — одно чтение снимка в `finally` под условием `if (!ct.IsCancellationRequested) { await PublishQueueStatsAsync(ct); }` (Task 10 расширит этот блок чтением статистики таблиц) (при отмене остановкой снимок не читается, как сейчас: безусловный `finally` прочитал бы его на отменённом токене и подменил исходное OCE из чтения); переименование `PublishQueueStatsAsync`.
- [ ] **Step 4:** Run `GuardedInstrumentsTests`, `JobRunner*`, `ProducerSpan*`, `ProcessingMaintenanceTests`, `StatsTaskTests`, `PgBoltMetricsTests` — PASS; полный прогон — PASS.
- [ ] **Step 5:** Commit `refactor: общие защищённые Activity и instruments, упрощения цикла stats`.

---

### Task 7: Гигиена тестов и бенчмарка

**Files:**
- Create: `tests/PgBolt.Testing/ActivityCapture.cs`
- Modify: `tests/PgBolt.Postgres.Tests/Diagnostics/PgBoltInstrumentationTests.cs`, `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs:131`, `HousekeepingTests.cs`, `ProcessingMaintenanceTests.cs`, `SchedulerLoopTests.cs`, `RetentionReportTests.cs`, `tests/PgBolt.Testing/ListLogger.cs`, `FetchStrategyTests.cs`, `JobRunnerTelemetryTests.cs`, `RunExecutionTests.cs`, `StatsTaskTests.cs:162`, `ProducerTracingTests.cs`, `ProducerSpanTests.cs`, `tests/E2E/PgBolt.E2E.Tests/ObservabilityEndToEndTests.cs`, `EndToEndTests.cs`, `tests/E2E/PgBolt.E2E.Tests/*.csproj` (ссылка на `PgBolt.Testing`, если её нет)

**Interfaces:**
- Consumes: `Eventually` (Task 1).
- Produces: `public sealed class ActivityCapture : IDisposable` в `PgBolt.Testing` — `ActivityCapture(string sourceName, Func<Activity, bool>? filter = null)`; `IReadOnlyList<Activity> Stopped`; включает `ActivitySamplingResult.AllDataAndRecorded` только для своего источника.

- [ ] **Step 1:** Пункты §3.5 по списку: `using`/`Dispose` у `PgBoltInstrumentation` и `Meter` в перечисленных фикстурах; лишние поля scope у дочерних логгеров `ListLogger`; переименование `Limited`/`limited` в `FetchStrategyTests` по текущему имени свойства; `Assert.Same` для исключения NoExecutor; проверка, что `Complete` бросил (`Complete` вызывается внутри `RunExecutionAsync`, не тестом): обработчик `_logs.OnEntry` выставляет флаг `threw` перед `throw`, после `await execution` — `Assert.True(threw)`; удаление дубля `TestTelemetry.Sum`; удаление повтора `RunScheduler=false` в `StatsTaskTests:162`.
- [ ] **Step 2:** `ActivityCapture` и перевод четырёх файлов (`ProducerTracingTests`, `ProducerSpanTests`, `JobRunnerTelemetryTests`, `ObservabilityEndToEndTests`) на него; `JobRunnerBenchmarks` не трогается.
- [ ] **Step 3:** E2E: `EndToEndTests` и `ObservabilityEndToEndTests` — `Eventually` из `PgBolt.Testing` с увеличенным таймаутом вместо своих циклов; F1–F4 Task 15 (§3.5): комментарий про шестую задачу, `&` → `&&`, магические ёмкости → именованные константы, параметры `StartHostAsync` — именованные.
- [ ] **Step 4:** `JobRunnerBenchmarks.GlobalSetup`: проверка, что исход прогревочного запуска — Ack (`InvalidOperationException` иначе), уже есть (`JobRunnerBenchmarks.cs:66-68`) — только убедиться, что она на месте; в рамках этой задачи файл не менять (правка создания `JobRunner` — в Task 6).
- [ ] **Step 5:** `dotnet build PgBolt.slnx` и полный прогон — PASS (утверждения существующих тестов не ослаблены). Commit `test: гигиена фикстур телеметрии, общий ActivityCapture`.

---

### Task 8: Комментарии и документация блока A

**Files:**
- Modify: все файлы `src/`, изменённые в Task 2–7, полностью `src/PgBolt.Server/Runtime/JobRunner.cs`, `ProcessingMaintenance.cs`, `RetentionReport.cs`; `docs/observability.md:48-49` и таблицы событий; `benchmarks/PgBolt.Server.Benchmarks/README.md`; `CHANGELOG.md`

- [ ] **Step 1:** `grep -niE "§[0-9]|часть [0-9]|task [0-9]|review focus|7a|7b" <файлы>` — каждую ссылку заменить сутью (что и почему). Expected после правки: grep по этим файлам пуст.
- [ ] **Step 2:** `docs/observability.md`: `HistogramBuckets` — «публичные статические массивы», не «константы»; дубли таблиц событий — одна таблица.
- [ ] **Step 3:** README бенчмарка: цифры — только из `results/` (before/after early exit из Task 2), без порога по времени; +32 B `JobLogScope` принимаются. `CHANGELOG.md`: раздел «Не выпущено» — исправления Task 1–7 одной строкой на группу, уточнённый пункт бенчмарка.
- [ ] **Step 4:** `dotnet build PgBolt.slnx` — без ошибок. Commit `docs: комментарии без ссылок на спеку, observability и README бенчмарка`.

---

### Task 9: `ReadTableStatsAsync`

**Files:**
- Create: `src/PgBolt.Postgres/Storage/TableStatsRecords.cs`, `tests/PgBolt.Postgres.Tests/TableStatsTests.cs`
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.Stats.cs`, `src/PgBolt.Server/Runtime/IServerStore.cs`, `PgBoltServerStore.cs`, `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`

**Interfaces:**
- Produces: `internal sealed record TableStatsRow(string Table, long DeadTuples, long SizeBytes)`; `internal Task<IReadOnlyList<TableStatsRow>> PgBoltStore.ReadTableStatsAsync(CancellationToken ct)` — SQL дословно §8.2; `IServerStore.ReadTableStatsAsync(CancellationToken)`; `FakeServerStore.OnReadTableStats` (`Func<CancellationToken, Task<IReadOnlyList<TableStatsRow>>>`, по умолчанию пустой список) с `StoreCall("ReadTableStats")`.

- [ ] **Step 1: тесты** `TableStatsTests` (Direct/Bouncer, как `QueueStatsSnapshotTests`):
  - `Returns_queue_and_completions_of_own_schema_only` — две схемы в одной БД; строки — ровно `queue`, `completions` своей схемы; `SizeBytes > 0`.
  - `Dead_tuples_appear_after_delete` (только Direct) — на одном открытом соединении теста вне транзакции: вставка 100 строк `queue` SQL-ом, `DELETE` и `SELECT pg_stat_force_next_flush()`; затем `ReadTableStatsAsync` → `queue.DeadTuples > 0` (§11.3).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация и проводка.
- [ ] **Step 4:** Run `TableStatsTests` — PASS; полный прогон — PASS.
- [ ] **Step 5:** Commit `feat(postgres): чтение статистики таблиц queue и completions`.

---

### Task 10: `ClusterTableStats` и gauge'и bloat

**Files:**
- Create: `src/PgBolt.Server/Runtime/ClusterTableStats.cs`, `tests/PgBolt.Server.Tests/Unit/ClusterTableStatsTests.cs`
- Modify: `src/PgBolt.Abstractions/PgBoltDiagnostics.cs` (`Instruments.ClusterTableDeadTuples = "pgbolt.cluster.table.dead_tuples"`, `ClusterTableSize = "pgbolt.cluster.table.size"`), `src/PgBolt.Postgres/Diagnostics/PgBoltTelemetry.cs` (`Attr.Table = "pgbolt.table"`), `StorageOperations` (`TableStatsRead = "table_stats_read"`), `src/PgBolt.Server/Runtime/PgBoltMetrics.cs`, `ProcessingMaintenance.cs` (`StatsOnceAsync`), `src/PgBolt.Server/PgBoltServerServiceCollectionExtensions.cs` (регистрация `ClusterTableStats`, новый аргумент `PgBoltMetrics`), `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs:111` (новый аргумент конструктора `ProcessingMaintenance`), `benchmarks/PgBolt.Server.Benchmarks/JobRunnerBenchmarks.cs:54` (новый аргумент `PgBoltMetrics`), `tests/PgBolt.Server.Tests/Fakes/TestTelemetry.cs` и тесты, создающие `ProcessingMaintenance`, `ClusterQueueStats` или `PgBoltMetrics` (`grep -rln "new ProcessingMaintenance\|new ClusterQueueStats\|new PgBoltMetrics" tests`: `StatsTaskTests`, `ResolverLoopTests`, `ClusterQueueStatsTests`, `HousekeepingTests`, `ProcessingMaintenanceTests`, `PgBoltMetricsTests`, `SchedulerLoopTests`, `WorkflowResolverLoopTests`, `RecurringBudgetTests`), `tests/PgBolt.Abstractions.Tests/PgBoltDiagnosticsTests.cs`, `tests/E2E/PgBolt.E2E.Tests/EventIdContractTests.cs` (строка `(5013, "TableStatsReadFailed", LogLevel.Warning)` в `Expected`), `docs/observability.md` (метрики и строка 5013 в таблице событий 5000–5099)

**Interfaces:**
- Consumes: `IServerStore.ReadTableStatsAsync` (Task 9), `ProcessingMaintenance.PublishQueueStatsAsync` (Task 6).
- Produces: `internal sealed class ClusterTableStats(MonoClock clock, PgBoltProcessingOptions options)` (опции регистрируются напрямую, без `IOptions`, как у `ClusterQueueStats`) — `void Update(IReadOnlyList<TableStatsRow> rows)`; `bool TryGetFresh(out IReadOnlyList<TableStatsRow> rows)` по образцу `ClusterQueueStats.TryGetFresh` (`false`, если последнего успешного `Update` не было дольше `4 × StatsSnapshotInterval`); конструкторы `PgBoltMetrics` и `ProcessingMaintenance` получают `ClusterTableStats` — все места создания обновляются (список в Modify); `PgBoltMetrics` создаёт `ObservableGauge<long>` `{tuple}` и `By` с тегом `pgbolt.table` из `ClusterTableStats.TryGetFresh` (через `GuardedInstruments`/обработку callback-исключения 8002, как у `cluster.queue.*`).

- [ ] **Step 1: тесты** `ClusterTableStatsTests` (`MetricCollector`, `FakeServerStore`, `FakeTimeProvider`):
  - `Stats_pass_publishes_table_gauges` — проход цикла `stats` → оба gauge'а с `pgbolt.table=queue|completions` и значениями фейка.
  - `Stale_rows_are_hidden` — после `Advance(4 × StatsSnapshotInterval + 1 мс)` без успешного чтения → пустой набор.
  - `Read_failure_counts_storage_error_and_keeps_rows` — фейк бросает → `storage.errors{table_stats_read}` = 1, прежние значения публикуются до устаревания.
  - `Published_when_maintenance_disabled` — `RunMaintenance = false` → публикуются.
  - `Pod_without_servers_does_not_publish` — без `AddPgBoltServer` gauge'ей нет.
  - `PgBoltDiagnosticsTests` — новые имена дословно.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация: вызов `ReadTableStatsAsync` в `StatsOnceAsync` после `PublishQueueStatsAsync` — внутри того же блока `if (!ct.IsCancellationRequested)` в `finally` (Task 6); ошибка — `StorageError(StorageOperations.TableStatsRead)` и лог нового события `Log.TableStatsReadFailed` (EventId 5013, Warning, «PgBolt: reading table statistics failed; the previous values are kept.») через существующий ограничитель ошибок цикла `stats` (`_statsErrors`); отмена — по правилу Task 4.
- [ ] **Step 3а:** `StatsTaskTests`: точные проверки последовательности вызовов `_store.Calls` (`:118-129`, `:137`, `:154`) получают `ReadTableStats` после `ReadQueueStats`; другие утверждения не меняются.
- [ ] **Step 4:** `docs/observability.md` — §8.3 спеки и строка события 5013 `TableStatsReadFailed` в таблицу 5000–5099 (без порогов алертов: их добавит Task 23).
- [ ] **Step 5:** Run `ClusterTableStatsTests`, `ClusterQueueStatsTests`, `StatsTaskTests`, `PgBoltDiagnosticsTests`, `EventIdContractTests` — PASS; полный прогон — PASS.
- [ ] **Step 6:** Commit `feat(server): кластерные метрики bloat queue и completions`.

---

### Task 11: Проект стенда и разбор аргументов

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/PgBolt.LoadTest.csproj`, `Program.cs`, `Cli/LoadTestOptions.cs`, `Cli/OptionsParser.cs`, `Env/PostgresCommand.cs`; `tests/PgBolt.LoadTest.Tests/PgBolt.LoadTest.Tests.csproj`, `LoadCollection.cs`, `Cli/OptionsParserTests.cs`, `Cli/PostgresCommandTests.cs`, `ProgramTests.cs`
- Modify: `PgBolt.slnx` (`/benchmarks/` и `/tests/`), `Directory.Packages.props` (`HdrHistogram` 2.5.0)

**Interfaces:**
- Produces:
  - csproj стенда — как `PgBolt.Server.Benchmarks.csproj` (`Exe`, `IsPackable=false`, `IsTestProject=false`), `PackageReference`: `HdrHistogram`, `Testcontainers`, `Testcontainers.PostgreSql`, `Microsoft.Extensions.Hosting`, `Npgsql`; `ProjectReference`: `PgBolt.Server`, `PgBolt.Postgres`; `InternalsVisibleTo PgBolt.LoadTest.Tests`. Все типы стенда — `internal`, namespace `PgBolt.LoadTest.*`.
  - Тестовый csproj — как `PgBolt.Server.Tests.csproj` + `ProjectReference` на стенд и `PgBolt.Testing`, `Microsoft.Extensions.TimeProvider.Testing`.
  - `enum Scenario { Sustained, Ramp, Partitions, Vacuum }`; `enum LoadMode { Direct, Bouncer, DirectPrepared, BouncerPrepared }` (имя `LoadMode`, а не `ConnectionMode`: публичный `PgBolt.Testing.ConnectionMode`, на который ссылается тестовый проект, дал бы CS0104). `PostgresFixture` в `PgBolt.LoadTest.Tests` подключается одной коллекцией, а не отдельным `IClassFixture` на класс и не через `[assembly: AssemblyFixture]`: `[CollectionDefinition("Load", DisableParallelization = true)] public sealed class LoadCollection : ICollectionFixture<PostgresFixture>` (файл `tests/PgBolt.LoadTest.Tests/LoadCollection.cs`), и все классы `Category=Load` помечаются `[Collection("Load")]`. Причина: по умолчанию xUnit v3 выполняет разные классы параллельно, а стенд держит глобальное состояние процесса (`RunClock.Reset()` в начале прогона, `MeterCounters` слушает все `Meter` `PgBolt` процесса, CPU считается по `Process.TotalProcessorTime`): два одновременных `Program.RunAsync` портят друг другу часы, счётчики и загрузку CPU. Коллекция с `DisableParallelization` выполняется отдельно от параллельной фазы, то есть и от `StandFailureTests` (без Docker, вне коллекции). Фикстура коллекции создаётся только при запуске её тестов, поэтому прогон «без Docker» (`Category!=Load`) контейнеры не поднимает, а одна фикстура заменяет по набору контейнеров на класс.
  - `sealed record LoadTestOptions` — свойства по таблице §4.1 (`Scenario`, `IReadOnlyList<LoadMode> Modes`, `string? Connection`, `string? BouncerConnection`, `string Out`, `int Rate`, `TimeSpan Warmup`, `TimeSpan Duration`, `int RampStart`, `int RampStep`, `TimeSpan RampStepDuration`, `int RampMax`, `IReadOnlyList<int> Partitions`, `IReadOnlyList<IndexSet> Indexes` (`enum IndexSet { Current, Extra }`), `TimeSpan Phase1`, `TimeSpan HoldXmin`, `TimeSpan Phase3`, `IReadOnlyDictionary<string,string> QueueReloptions`, `IReadOnlyDictionary<string,string> PgSettings`) со значениями по умолчанию по сценарию (§4.1).
  - `static class OptionsParser` — `ParseResult Parse(IReadOnlyList<string> args)`; `sealed record ParseResult(LoadTestOptions? Options, string? Error)`.
  - `Program.Main` — разбор; при `Error` — сообщение в stderr и код 2; иначе пока код 0 (сценарии подключают Task 15–18). Вызов `Program`-уровня для тестов — `static Task<int> Program.RunAsync(string[] args, CancellationToken ct)` (`Main` лишь вызывает его).
  - `static class PostgresCommand` — `IReadOnlyList<string> Build(LoadTestOptions options, bool trackPlanning)`: чистая функция сборки команды контейнера PG (`IReadOnlyList<string>`): `-c shared_buffers=1GB`, `-c max_wal_size=4GB`, `-c shared_preload_libraries=pg_stat_statements`, `-c track_io_timing=on`, `-c pg_stat_statements.track_planning=on|off` и пары `--pg-setting` как `-c k=v`; пара с ключом, совпадающим с параметром по умолчанию, заменяет его: значение по умолчанию с тем же ключом в команду не попадает. Task 14 использует её в `LoadEnvironment.StartAsync`.

- [ ] **Step 1: тесты** `OptionsParserTests` (по одному `[Fact]`/`[Theory]` на пункт §11.2 «разбор аргументов CLI»):
  - значения по умолчанию каждого сценария (таблица §4.1), включая режимы: без `--connection` — `both`/`all`; `--connection` без `--bouncer-connection` — `[Direct]`, у `partitions` — `[Direct, DirectPrepared]`; оба — `both`, у `partitions` — `[Direct, Bouncer, DirectPrepared]`;
  - `--mode all` и `*-prepared` вне `partitions` → ошибка; флаг вне своих сценариев (например, `--hold-xmin` у `sustained`) → ошибка; `--bouncer-connection` без `--connection` → ошибка; `--pg-setting` с `--connection` → ошибка; `bouncer` с `--connection` без `--bouncer-connection` → ошибка;
  - длительности: `5s`, `2m`, `1h` разбираются; не кратные 10 с (`15s` у `--duration`) → ошибка; `--ramp-step-duration 20s` → ошибка (меньше 30 с); короткие режимы §11.2 (`--warmup 5s`) разрешены — прогрев кратности не требует;
  - `--queue-reloptions "autovacuum_vacuum_cost_delay=0"` и `--pg-setting "autovacuum_naptime=10s"` → словари;
  - `PostgresCommandTests.Pg_setting_becomes_dash_c_pair_and_overrides_default` — `--pg-setting "autovacuum_naptime=10s,max_wal_size=2GB"` → в команде есть пара `-c`, `autovacuum_naptime=10s`, а `max_wal_size` встречается один раз со значением `2GB`; `track_planning` on только при `trackPlanning = true`;
  - `ProgramTests.Parse_error_exits_with_code_2` — `Program.RunAsync(["sustained", "--hold-xmin", "1m"], ct)` → код 2, сообщение в stderr.
- [ ] **Step 2:** Run `dotnet test tests/PgBolt.LoadTest.Tests --filter "FullyQualifiedName~OptionsParserTests"` — FAIL.
- [ ] **Step 3:** Реализация (ручной разбор, без пакета CLI).
- [ ] **Step 4:** Run — PASS; `dotnet build PgBolt.slnx` — без ошибок; полный прогон — PASS.
- [ ] **Step 5:** Commit `feat(loadtest): проект нагрузочного стенда и разбор аргументов`.

---

### Task 12: Профиль B, token bucket, пачки, конвейер продюсера, счётчик выполнений

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/Load/ProfileB.cs`, `TokenBucket.cs`, `BatchAssembler.cs`, `ProducerPipeline.cs`, `ExecutionCounter.cs`, `RunClock.cs`; тесты `tests/PgBolt.LoadTest.Tests/Load/*Tests.cs`

**Interfaces:**
- Produces:
  - `static class RunClock` — `long NowMicros` (общий `Stopwatch` процесса от старта прогона; `Reset()` в начале прогона).
  - `readonly record struct JobDraft(long Seq, string Queue, bool Delayed, bool FailFirstAttempt)`; `sealed class ProfileB(Random rng)` — `JobDraft Next(long seq)`: очередь `default`/`critical` 80/20, отложенная 10 %, `FailFirstAttempt` 1 % (§5.1).
  - `sealed class TokenBucket(double ratePerSecond, double ceiling, TimeProvider time)` — `int Available()` (начисляет с прошлого вызова, излишек сверх `ceiling` прибавляет к `Dropped`), `void Consume(int count)`, `long Dropped`, `void SetRate(double ratePerSecond)` — токены, накопленные до смены, начисляются по старому потоку (до момента вызова), затем `ratePerSecond` и `ceiling` (= новый rate × 1 с) заменяются. Потолок для продюсера — `rate × 1 с` (§4.3). Потокобезопасен: `SetRate` вызывает поток сценария (`ramp`), `Available()`/`Consume()` — цикл генератора, поэтому всё состояние (токены, время последнего начисления, rate, ceiling, `Dropped`) защищено одним `lock`; `Dropped` читается под ним же.
  - `readonly record struct BatchKey(string Queue, bool Delayed)`; `sealed record PendingBatch(BatchKey Key, IReadOnlyList<JobDraft> Jobs, long FirstAddedMicros)`; `sealed class BatchAssembler(int maxSize = 100, long maxAgeMicros = 10_000)` — `PendingBatch? Add(JobDraft job, long nowMicros)` (возвращает полную пачку), `IReadOnlyList<PendingBatch> TakeAged(long nowMicros)`, `IReadOnlyList<PendingBatch> TakeAll()`.
  - `static int ProfileB.PickDelaySeconds(Random rng)` — равномерно 1..30, одна на отложенную пачку (§4.3); вызывается 4 отправителями параллельно, поэтому `Random` должен быть потокобезопасным (`Random.Shared`), а не экземпляром генератора `ProfileB`.
  - `sealed record SendRequest(BatchKey Key, IReadOnlyList<JobDraft> Jobs, int DelaySeconds, long EnqueuedAtMicros)` с `long RunAtMicros` (= `EnqueuedAtMicros + DelaySeconds × 1 000 000`; у немедленных `DelaySeconds = 0`, то есть `RunAtMicros = EnqueuedAtMicros`). `LoadArgs` из запроса собирает Task 15.
  - `sealed class ProducerPipeline` — конвейер без БД и без `LoadArgs`: `ProducerPipeline(ProfileB profile, TokenBucket bucket, BatchAssembler assembler, Func<SendRequest, CancellationToken, Task> send, Func<long> nowMicros, Func<int> pickDelaySeconds, TimeProvider time, Action<long> onSeqIssued, int senders = 4, int channelCapacityPerKey = 8)`; `Task RunAsync(CancellationToken ct)` — цикл генератора: токены `bucket.Available()` → задачи `profile.Next(seq)` → пачки `assembler`; готовая пачка (полная или `TakeAged`) кладётся в ограниченный канал своего ключа (8 пачек на ключ); при полном канале ключа генератор ждёт и не вызывает `Consume`, поэтому токены копятся в `bucket` числом до потолка и после освобождения канала выдаются; сверх потолка бакет отбрасывает и считает (`Dropped`); перед `profile.Next(seq)` генератор вызывает `onSeqIssued(seq)` (на каждом `Seq`; `Producer` передаёт `ctx.Counter.EnsureChunk`, который сам выделяет кусок только на первом `Seq` нового куска), поэтому кусок `ExecutionCounter` опубликован до выдачи первого `Seq` из него; `void SetRate(int rate)` — делегирует `bucket.SetRate(rate)` (поток и потолок rate × 1 с меняются вместе); `Task StopAsync()` — `TakeAll`, дослать остаток, завершить каналы, дождаться отправителей; `PipelineStats Stats` (накопительные `Generated`, `Dropped`, `TimeSpan MaxGeneratorWait`) и `PipelineStats TakeWindow()` — дельты `Generated` и `Dropped` и максимум `MaxGeneratorWait` с прошлого вызова, значения за окно сбрасываются. Генератор обновляет счётчики окна, пока `TakeWindow` вызывает `WindowCollector`, поэтому `TakeWindow` читает и сбрасывает их атомарно: `Interlocked.Exchange` для `Generated`, `MaxGeneratorWait` генератор обновляет CAS-циклом (максимум), `Dropped` окна — дельта `bucket.Dropped` (читается под `lock` бакета) к значению прошлого вызова. Отправитель (4 задачи) берёт пачку из канала, для отложенной вызывает `pickDelaySeconds()` один раз, берёт одну `EnqueuedAtMicros = nowMicros()` непосредственно перед вызовом `send` и вызывает `send(new SendRequest(...))`.
  - `sealed class ExecutionCounter(long capacity)` — куски `int[1 << 20]`; `void EnsureChunk(long seq)` (вызывает генератор через `onSeqIssued` до выдачи `Seq` из нового куска; идемпотентен для `Seq` уже существующего куска), `int Increment(long seq)`, `int Get(long seq)`; `capacity` — верхняя оценка числа задач прогона (внешний массив ссылок фиксированной длины).

- [ ] **Step 1: тесты:**
  - `ProfileBTests.Distribution_matches_profile` — `Random(42)`, 1 000 000 задач: доли `critical` 20 % ± 0,3 %, отложенных 10 % ± 0,3 %, `FailFirstAttempt` 1 % ± 0,1 %; `PickDelaySeconds` за 300 000 вызовов покрывает все 1..30, каждое значение 1/30 ± 0,5 %.
  - `TokenBucketTests.Issues_rate_times_ten_seconds` — `FakeTimeProvider`, rate 10 000, шаг 1 мс × 10 000 с `Consume(Available())` → выдано 100 000 ± 0,5 % (§11.2).
  - `TokenBucketTests.Accumulates_up_to_ceiling_and_counts_dropped` — без `Consume` 3 с при ceiling = rate → `Available() == rate`, `Dropped == 2 × rate` (± 1).
  - `TokenBucketTests.Rate_change_applies_from_change_moment_and_moves_ceiling` — rate 1 000 → за 0,5 с накоплено 500; `SetRate(4 000)` → `Available() == 500` (накопленное по старому потоку), ещё через 0,5 с — 2 500, через 2 с без `Consume` — `Available() == 4 000` (потолок = новый rate × 1 с), излишек в `Dropped`.
  - `BatchAssemblerTests`: `Full_batch_at_100`, `Aged_batch_at_10ms` (`TakeAged` в 9 999 мкс — пусто, в 10 000 — пачка), `Keys_are_homogeneous` (смешанный поток → каждая пачка одного ключа), `TakeAll_flushes_partial`.
  - `ProducerPipelineTests` (фейковые `TimeProvider` и `nowMicros`, `send` — подменяемый делегат, блокируемый через `TaskCompletionSource`):
    - `Full_channel_accumulates_tokens_up_to_ceiling_and_resends` — `send` заблокирован, каналы ключа заполнены (8 пачек) → генератор ждёт, `bucket.Available()` растёт, но не выше `rate × 1 с`; после разблокировки `send` недовыданные токены досылаются (число отправленных задач = выданное токенами, без потерь в пределах потолка);
    - `Tokens_over_ceiling_are_dropped_and_counted` — при заблокированном `send` дольше 1 с: `Stats.Dropped` растёт на излишек сверх потолка, в память не материализуется больше задач, чем вмещают каналы и пачки в сборке;
    - `Delayed_batch_has_one_delay` — отложенная пачка → один вызов `pickDelaySeconds` на пачку, `DelaySeconds` в 1..30 и одинаков для всех её задач; у немедленной `DelaySeconds = 0`, `pickDelaySeconds` не вызывается;
    - `Batch_shares_one_EnqueuedAtMicros_and_RunAt_is_EnqueuedAt_plus_delay` — `EnqueuedAtMicros` — одна метка на `SendRequest`, взятая в момент вызова `send` (после ожидания в канале), `RunAtMicros = EnqueuedAtMicros + DelaySeconds × 1 000 000`; у немедленной `RunAtMicros = EnqueuedAtMicros`;
    - `Batches_are_sent_at_100_jobs_or_10ms_age` — размер и возраст пачек в `SendRequest` через фейковые часы;
    - `Chunk_is_ensured_before_seq_is_issued` — `onSeqIssued` = `counter.EnsureChunk`, `send` для каждой задачи пачки выполняет `counter.Increment(seq)`; прогон через границу куска 2^20 без исключения (в том числе `NullReferenceException`);
    - `SetRate_changes_flow_and_ceiling` — после `pipeline.SetRate(r2)` выдача идёт по новому потоку, а `Stats.Dropped` при заблокированном `send` считается по потолку `r2 × 1 с`;
    - `Concurrent_set_rate_and_generation_keep_state_consistent` — поток сценария в цикле вызывает `pipeline.SetRate` (чередуя два значения), генератор работает, `TakeWindow` вызывается из третьего потока: исключений нет, сумма дельт `Generated` из `TakeWindow` плюс остаток `Stats` равна накопительному `Generated`, ни одна выдача не превышает потолок текущего rate × 1 с;
    - `TakeWindow_returns_deltas_and_resets` — два вызова `TakeWindow` подряд: `Generated`/`Dropped` — дельты за окно, `MaxGeneratorWait` — максимум за окно и после вызова сбрасывается; накопительный `Stats` не уменьшается.
  - `ExecutionCounterTests`: `Increment_on_chunk_boundaries` (`seq` = 0, 2^20 − 1, 2^20) после `EnsureChunk`; `Concurrent_increments_are_not_lost` (8 потоков × 100 000 по 1 000 `seq` → каждый `Get == 800`).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация.
- [ ] **Step 4:** Run `dotnet test tests/PgBolt.LoadTest.Tests --filter "FullyQualifiedName~PgBolt.LoadTest.Tests.Load."` — PASS.
- [ ] **Step 5:** Commit `feat(loadtest): профиль B, token bucket, сборка пачек, конвейер продюсера и счётчик выполнений`.

---

### Task 13: Гистограммы, окна, SLO и коды возврата

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/Measure/LatencyRecorder.cs`, `Measure/Windows.cs`, `Slo/SloModel.cs`, `Slo/SloEvaluator.cs`, `Slo/RampEvaluator.cs`, `Slo/PartitionsVerdict.cs`, `Slo/VacuumWinner.cs`, `Slo/ExitCode.cs`, `Slo/ExecutionReconciler.cs`; тесты `tests/PgBolt.LoadTest.Tests/Slo/*Tests.cs`, `Measure/LatencyRecorderTests.cs`

**Interfaces:**
- Consumes: `RunClock`, `ExecutionCounter` (Task 12).
- Produces:
  - `sealed class LatencyRecorder` — HdrHistogram `Recorder` (1 мкс … 60 с, 3 значащие цифры); `void Record(long micros)` (насыщение до 60 с, `long Overflows`); `LatencySummary CloseWindow()` — интервальная гистограмма окна добавляется в общую и в текущий сегмент (если он начат); `LatencySummary Total()` — общая за закрытые окна с последнего `ResetTotal()` (или с начала); `void ResetTotal()` — обнуляет общую гистограмму (вызывается в начале окна замера, чтобы прогрев в `Total()` не входил); `void BeginSegment()` — начинает пустой сегмент; `LatencySummary SegmentSummary()` — сводка объединённой гистограммы окон, закрытых с `BeginSegment()` (p99 по окнам вместе, а не из готовых перцентилей окон). `sealed record LatencySummary(long Count, double P50Ms, double P90Ms, double P99Ms, double P999Ms, double MaxMs)`.
  - `sealed record WindowStats(int Index, TimeSpan Start, long Completed, long Enqueued, long EnqueueCalls, long EnqueueErrors, double EnqueueP99Ms, double CpuFraction, TimeSpan MaxGeneratorWait, long DroppedTokens, LatencySummary Immediate, LatencySummary Delayed)`.
  - `sealed record Sample(TimeSpan At, long? ReadyBacklog, long? QueueTotalBytes, long? MaxWaitMicros, int? ClWaiting, int? Connections, ...)` — поля §4.5, `null` = пропуск отсчёта.
  - `enum Verdict { Pass, Fail, NoData }`; `sealed record SloVerdict(int Number, string Name, string Threshold, string Value, Verdict Result, string? Mark)` (`Mark` = `enqueue` | `ограничено стендом` | `null`).
  - `sealed record RunData(int Rate, IReadOnlyList<WindowStats> Windows, IReadOnlyList<Sample> Samples, LatencySummary Immediate, LatencySummary Delayed, CorrectnessData? Correctness, LoadMode Mode, PhaseMarks? Phases, TimeSpan MeasureStart, TimeSpan MeasureEnd)` — `Windows` и `Samples` содержат все окна и срезы прогона (в `.csv` пишутся все), а `SloEvaluator` оценивает только окна с `MeasureStart <= WindowStats.Start` и `WindowStats.Start + 10 с <= MeasureEnd` и срезы с `MeasureStart <= Sample.At < MeasureEnd` (время — от `RunClock`; «первая минута» SLO 4 отсчитывается от `MeasureStart`, «последняя» — до `MeasureEnd`; у `vacuum` `MeasureStart` = `Phases.Phase1Start`, `MeasureEnd` = `Phases.End`). Окна прогрева и дренажа не оцениваются. Сетка окон 10 с начинается заново в `MeasureStart`: окно прогрева, открытое в этот момент, закрывается неполным и не оценивается, поэтому некратный 10 с прогрев (`--warmup 5s`) не сдвигает окна замера; `PhaseMarks(TimeSpan Phase1Start, TimeSpan Phase2Start, TimeSpan Phase3Start, TimeSpan End)`.
  - `sealed record CorrectnessData(long MissingExecutions, long DuplicatedExecutions, long ExcludedFailedCalls, long JobsNotSucceeded, long QueueRowsAfterDrain, bool DrainedInTime, long AttemptMismatches, long OutcomeDropped, long StorageErrors)` — поля SLO 5: результат сверки счётчика выполнений (`ExecutionReconciler`), число строк `jobs` не в Succeeded, размер `queue` после дренажа, флаг «дренаж уложился в 2 мин», число строк `jobs` с неверным `attempt` (сверка одним запросом, §5.2), `outcome.dropped`, `storage.errors`.
  - `static class ExecutionReconciler` — `(long Missing, long Duplicated, long ExcludedFailedCalls) Reconcile(ExecutionCounter counter, long issued, IReadOnlySet<long> failFirst, IReadOnlySet<long> failedCallSeqs)`: по каждому `Seq` из `0..issued − 1`, кроме `failedCallSeqs`, ожидаемое значение счётчика 2 для `failFirst` и 1 для остальных; меньше ожидаемого — `Missing`, больше — `Duplicated`; `ExcludedFailedCalls` = число исключённых `Seq`. `issued` — `PipelineStats.Generated` после остановки конвейера, `failFirst` и `failedCallSeqs` — из `RunContext` (Task 15).
  - `static class SloEvaluator` — `IReadOnlyList<SloVerdict> Sustained(RunData data)` (SLO 1–6), `IReadOnlyList<SloVerdict> Vacuum(RunData data)` (SLO 6–9 по §7.3), `bool IsUnderloaded(WindowStats w, int rate)` (§4.4), `bool IsCpuBound(IReadOnlyList<WindowStats> windows)` (§4.4; для `ramp` и `partitions`, где вызывающий сам отбирает оцениваемые окна ступени или комбинации) и `bool IsCpuBound(RunData data)` — то же по окнам из [MeasureStart, MeasureEnd) (окна прогрева и дренажа не учитываются; у `vacuum` это окна фаз 1–3). `Sustained` и `Vacuum` сами вычисляют `IsCpuBound(data)` и при упоре ставят `Mark = "ограничено стендом"` у всех своих вердиктов (у вердикта SLO 1 с пометкой `enqueue` — `"enqueue, ограничено стендом"`); вызывающий сценарий передаёт тот же `IsCpuBound(data)` в `ExitCode.For` как `cpuBound`.
  - `static class RampEvaluator` — `RampResult Evaluate(IReadOnlyList<RampStep> steps)`; `RampStep(int Rate, IReadOnlyList<WindowStats> Evaluated, LatencySummary Immediate)` (`Immediate` — `SegmentSummary()` немедленных за оцениваемые окна ступени; SLO 2 оценивается по нему, а не по перцентилям отдельных окон); `RampResult(int? MaxSustainedRate, string? FirstViolation, bool LimitedByStand)`.
  - `static class PartitionsVerdict` — `int ExitCode(IReadOnlyList<CombinationResult> results)`; `int? Threshold(IReadOnlyList<CombinationResult> results, LoadMode mode, IndexSet indexes)`; `CombinationResult(int P, IndexSet Indexes, LoadMode Mode, bool Slo1, bool Slo2, bool CpuBound, ...)`.
  - `static class VacuumWinner` — `VacuumChoice Choose(VacuumCandidate current, VacuumCandidate baseline)` и `VacuumChoice ChooseGlobal(VacuumCandidate tableWinner, VacuumCandidate global)`; `VacuumCandidate(string Name, ModeOutcome Bouncer, ModeOutcome Direct)`, `ModeOutcome(bool Slo7To9, double PeakSizeBytes, double P99Ms)`; правила §7.4 дословно (равенство < 10 %).
  - `static class ExitCode` — `int For(IReadOnlyList<SloVerdict> verdicts, bool standFailure, bool cpuBound)` (§4.1).

- [ ] **Step 1: тесты** — по пунктам §11.2 «без БД», каждый пункт отдельным тестом:
  - `LatencyRecorderTests`: `Saturates_above_60s_and_counts_overflow`, `Window_and_total_percentiles` (известное распределение 1..1000 мс → p50 ≈ 500, p99 ≈ 990 в пределах точности HdrHistogram), `Concurrent_record_is_safe` (8 потоков), `Total_excludes_windows_before_reset` (окно до `ResetTotal()` не входит в `Total()`, окна после — входят), `Segment_summary_merges_windows` (сегмент объединяет окна после `BeginSegment()`, окна до него не входят).
  - `SloEvaluatorTests`: границы каждого порога SLO 1–4 (ровно на пороге — Pass, на 1 % хуже — Fail); `Underloaded_window_fails_slo1_with_enqueue_mark` (окно 97,9 % rate × 10 с → Fail, `Mark = "enqueue"`; 98 % — не недогруз); `Slo4_second_part_uses_minute_means`; `Slo5_reports_excluded_failed_calls` (`ExcludedFailedCalls` > 0 при нулевых `Missing`/`Duplicated` → Pass, число в `Value`); `Slo5_nonempty_queue_after_drain_fails` (`QueueRowsAfterDrain` > 0 или `DrainedInTime = false` → Fail); `Warmup_windows_are_not_evaluated` (недогруженное окно и срез с backlog выше порога до `MeasureStart` не дают Fail по SLO 1 и 4; первая минута SLO 4 считается от `MeasureStart`); `Drain_windows_are_not_evaluated` (окна после `MeasureEnd` с `Enqueued = 0` не дают недогруза и Fail SLO 1, срезы дренажа не входят в последнюю минуту SLO 4, упор CPU в окнах дренажа не ставит «ограничено стендом»); `Slo5_requires_two_runs_for_fail_first`; `Slo6_bouncer_maxwait` и `Slo6_direct_connections`; `Slo7_compares_last_minute_to_60_120s` и `Slo7_short_phase_no_data`; `Slo8_per_window_percentiles`; `Slo9_recovery_moment` (T ≤ phase3 + 5 мин → Pass, позже → Fail, отрезок после T < 2 мин → вторая часть SLO 4 «нет данных»); `Vacuum_evaluates_slo6_not_slo5`; `Cpu_bound_measure_windows_mark_verdicts` (упор только в окнах прогрева — не упор, `Mark` без «ограничено стендом»; упор больше чем в 10 % окон замера — «ограничено стендом» у всех вердиктов `Sustained` и `Vacuum`, включая «нет данных»); `Empty_inputs_give_no_data` (Review Focus 5).
  - `ExecutionReconcilerTests` на синтетическом `ExecutionCounter`: `Complete_counter_has_no_discrepancies`; `Missing_execution_is_reported` (`Seq` с 0 при ожидаемом 1); `Duplicate_execution_is_reported` (2 при ожидаемом 1); `Fail_first_expects_two` (`FailFirstAttempt` со счётчиком 1 — `Missing`, со счётчиком 2 — норма); `Failed_call_seq_with_0_1_2_is_excluded` (неуспешные `Seq` со счётчиком 0/1/2 не дают ни `Missing`, ни `Duplicated`, их число в `ExcludedFailedCalls`).
  - `CpuBoundTests`: 10 % окон выше 80 % — не упор, 11 % — упор; `ExitCode.For`: упор → 2, Fail → 1, только Pass/NoData → 0.
  - `RampEvaluatorTests`: остановка на первом нарушении SLO 1, SLO 2 (p99 по окнам вместе), `Slo2_uses_merged_p99_not_per_window` (в одном окне p99 600 мс, объединённый p99 в `RampStep.Immediate` ≤ 500 мс → нарушения нет), «enqueue», «стенд» (упор > 10 % окон ступени → `LimitedByStand`, код 0).
  - `PartitionsVerdictTests`: код 1 только по `P = 15, Current`; код 2 только при упоре у `P = 15, Current`; порог — наименьшее P с нарушением; `P = 15` отсутствует → 0.
  - `VacuumWinnerTests`: выполняющий SLO 7–9 выигрывает у невыполняющего; разница < 10 % — равенство; при равенстве текущие выигрывают, только если выполняют SLO 7–9 и в `direct`, иначе — вариант, выполняющий в обоих режимах; расхождение режимов → текущие; `Bouncer_slo_not_met_by_any_table_variant_keeps_current` (SLO 7–9 в `bouncer` не выполняет ни один табличный вариант → текущие параметры); глобальный против табличного: при равенстве — табличный.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация.
- [ ] **Step 4:** Run `dotnet test tests/PgBolt.LoadTest.Tests --filter "FullyQualifiedName~Slo|FullyQualifiedName~Measure"` — PASS.
- [ ] **Step 5:** Commit `feat(loadtest): гистограммы задержки, вычисление SLO и кодов возврата`.

---

### Task 14: Окружение, консоль PgBouncer, срезы, `pg_stat_statements`

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/Env/LoadEnvironment.cs`, `Env/PgBouncerConsole.cs`, `Env/PgBouncerOutputParser.cs`, `Env/EnvironmentInfo.cs`, `Measure/SamplerSchedule.cs`, `Measure/Sampler.cs`, `Measure/StatementClasses.cs`, `Measure/StatementsReader.cs`; тесты `tests/PgBolt.LoadTest.Tests/Env/*Tests.cs`, `Measure/SamplerScheduleTests.cs`, `Measure/StatementClassesTests.cs`, `Measure/StatementsReaderTests.cs`
- Modify: файлы `src/PgBolt.Postgres/Storage/PgBoltStore.*.cs` со свойствами `AckSql`, `HeartbeatSql`, `RetrySql`, `FinalFailSql`, `PromoteSql`, `EnqueueSql` (→ `internal`; меняется только видимость), `src/PgBolt.Postgres/PgBolt.Postgres.csproj` (`<InternalsVisibleTo Include="PgBolt.LoadTest" />` и `<InternalsVisibleTo Include="PgBolt.LoadTest.Tests" />`)

**Interfaces:**
- Consumes: `LoadTestOptions` (Task 11), `Sample` (Task 13).
- Produces:
  - `sealed class LoadEnvironment : IAsyncDisposable` — `static Task<LoadEnvironment> StartAsync(LoadTestOptions options, bool trackPlanning, CancellationToken ct)`: свои контейнеры (§4.2: образы, `-c` параметры, `--pg-setting`, network alias `pgbouncer`, `AUTH_TYPE=scram-sha-256`, `max_prepared_statements=200`, `admin_users`/`stats_users`, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements` в `public`) или внешние строки; `string DirectConnectionString`, `string? BouncerConnectionString`, `PgBouncerConsole? Console`, `Task<string> CreateSchemaNameAsync()`, `Task DropSchemaAsync(string schema)`.
  - `static class PgBouncerOutputParser` — `IReadOnlyList<PoolRow> ParsePools(string psqlOutput)`, `int? ParseMaxPreparedStatements(string psqlOutput)`; `sealed record PoolRow(string Database, string User, int ClActive, int ClWaiting, int SvActive, int SvIdle, long MaxWaitMicros)` (колонки PgBouncer 1.24, вывод `psql -At`, разделитель `|`); `MaxWaitMicros = maxwait × 1 000 000 + maxwait_us` (`maxwait` — секунды, `maxwait_us` — микросекундная часть).
  - `sealed class PgBouncerConsole` — `Task<IReadOnlyList<PoolRow>> ShowPoolsAsync(CancellationToken)`, `Task<int?> MaxPreparedStatementsAsync(CancellationToken)` через `ExecAsync` в контейнере PG (§4.5).
  - `sealed class SamplerSchedule(TimeSpan period, int maxConsecutiveFailures = 30)` — чистая логика тиков: `TickDecision OnTick(TimeSpan now)` (пропуск тика, если предыдущий срез ещё идёт), `void OnCompleted(TimeSpan now, bool anyQueryOk)`, `long SkippedTicks`, `long FailedAttempts`, `bool Exhausted` (30 подряд неудачных).
  - `sealed class Sampler` — раз в секунду собирает `Sample` по §4.5 (ограниченный счёт готовых, раз в 10 с — `queue_delayed`/`queue_leased`, `pg_stat_*`, `LockManager`, PgBouncer), пишет время запросов; `IReadOnlyList<Sample> Samples`; `Exhausted` → стенд завершает прогон кодом 2.
  - `static class StatementClasses` — `string? Classify(string query)`; обязательные классы `dequeue`, `ack`, `outcome`, `heartbeat`, `enqueue`, `promoter`, необязательный `dequeue_limited`; фрагменты — константы, подобранные по текстам SQL `PgBoltStore`.
  - `sealed class StatementsReader` — `Task ResetAsync()`, `Task<StatementsSummary> ReadAsync()` (`calls`, `mean_plan_time`, `mean_exec_time`, `total_*` по классам; без расширения — вся сводка `NoData`; без `track_planning` — `NoData` только у `mean_plan_time`/`total_plan_time`, а `calls` и `*_exec_time` заполнены).
  - `static class EnvironmentInfo` — `Task<IReadOnlyList<(string Key, string Value)>> CollectAsync(...)` — поля §4.2 «В отчёт записываются».

- [ ] **Step 1а: образцы вывода PgBouncer.** До реализации снять вручную один раз: поднять `docker run --rm -d --name pgb-sample …` (`edoburu/pgbouncer:v1.24.1-p1` с `AUTH_TYPE=scram-sha-256` рядом с `postgres:17-alpine`, как в §4.2) и выполнить `psql -At -h <pgbouncer> -p <порт> -U <пользователь> -d pgbouncer -c "SHOW POOLS"` и `-c "SHOW CONFIG"` (`psql` — в контейнере `postgres:17-alpine`); остановить только свои контейнеры по имени. Вывод сохранить для тестов парсера.
- [ ] **Step 1: тесты без БД:**
  - `PgBouncerOutputParserTests.Parses_show_pools_of_1_24` и `Parses_show_config_max_prepared_statements` — на строках, снятых с `edoburu/pgbouncer:v1.24.1-p1` (образцы снимаются вручную до реализации — Step 1а — и фиксируются в тесте); `Max_wait_combines_seconds_and_micros` — строка с `maxwait = 1`, `maxwait_us = 250000` → `MaxWaitMicros == 1 250 000` (на образцах простаивающего PgBouncer обе колонки 0).
  - `SamplerScheduleTests`: `Slow_sample_skips_ticks_without_accumulating`, `Failed_query_is_a_skip`, `Exhausted_on_30th_consecutive_failure_not_before`, `Success_resets_failure_streak`.
  - `StatementClassesTests.Each_statement_text_has_one_class` — тексты `DequeueSql`, `DequeueLimitedSql`, `AckSql`, `RetrySql` и `FinalFailSql` (класс outcome), `HeartbeatSql`, `PromoteSql`, `EnqueueSql` — из internal-свойств `PgBoltStore` (экземпляр со схемой `pgbolt`, без подключения) → ровно один класс у каждого; посторонний запрос → `null`.
- [ ] **Step 2: тесты с Docker** (`Category=Load`, `[Collection("Load")]`, Task 11): `LoadEnvironmentTests.Own_containers_accept_direct_and_bouncer_connections` (SELECT 1 через оба, `ShowPoolsAsync` не пуст, `MaxPreparedStatementsAsync == 200`, расширение `pg_stat_statements` есть в `public`); `StatementsReaderTests.Statements_reader_without_extension_returns_no_data` (на `PostgresFixture`, Review Focus 4).
- [ ] **Step 3:** Run — FAIL.
- [ ] **Step 4:** Реализация.
- [ ] **Step 5:** Run `dotnet test tests/PgBolt.LoadTest.Tests` (без фильтра — включая `Category=Load`) — PASS.
- [ ] **Step 6:** Полный прогон `Category!=Load` (видимость в `PgBolt.Postgres`) — PASS. Commit `feat(loadtest): окружение, консоль PgBouncer, срезы и pg_stat_statements`.

---

### Task 15: Хосты, продюсер, отчёт и сценарий `sustained`

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/Run/LoadJob.cs`, `Run/LoadArgs.cs` (+ `JsonSerializerContext`), `Run/HostCluster.cs`, `Run/Producer.cs`, `Run/SchemaPreparer.cs`, `Run/RunContext.cs`, `Run/CorrectnessCheck.cs`, `Run/MeterCounters.cs`, `Run/WindowCollector.cs`, `Report/ReportWriter.cs`, `Scenarios/IScenario.cs`, `Scenarios/Sustained.cs`; тесты `tests/PgBolt.LoadTest.Tests/Report/ReportWriterTests.cs`, `Run/WindowCollectorTests.cs`, `Scenarios/SmokeTests.cs`, `Scenarios/StandFailureTests.cs`, `Scenarios/StandCancelTests.cs`
- Modify: `benchmarks/PgBolt.LoadTest/Program.cs`

**Interfaces:**
- Consumes: Task 11–14.
- Produces:
  - `sealed record LoadArgs(long Seq, long EnqueuedAtMicros, long RunAtMicros, int WorkMillis, bool FailFirstAttempt, string Padding)`; `JobDescriptor` `load.job` (очередь по умолчанию `default`, `MaxAttempts` по умолчанию); `sealed class LoadJob : IJob<LoadArgs>` — шаги обработчика §4.3 в указанном порядке.
  - `sealed class RunContext` — `ExecutionCounter Counter`, `long Executed` (общий счётчик выполнений через `Interlocked`), `LatencyRecorder Immediate`, `LatencyRecorder Delayed`, `ConcurrentBag<long> FailedCallSeqs` (`Seq` неуспешных вызовов `EnqueueManyAsync`) и `ConcurrentBag<long> FailFirstSeqs` (`Seq` задач с `FailFirstAttempt`, ~1 %; заполняет `Producer` при сборке `LoadArgs`); регистрируется singleton в DI хостов (обработчик получает его из DI). `RunContext(long capacity)`: `capacity` (размер внешнего массива `ExecutionCounter`) — оценка с запасом 10 %: сумма `rate × длительность` интервалов генерации + `max rate × 1 с` (потолок бакета); на сценарий — `sustained`: `rate × (прогрев + окно замера)`; `ramp`: Σ `rateᵢ × длительность ступени` по всем возможным ступеням до `--ramp-max`; `vacuum`: `rate × (прогрев + фазы 1–3)`; `partitions`: то же, что `sustained`, на каждую комбинацию (новый `RunContext` на комбинацию и режим).
  - `sealed class HostCluster : IAsyncDisposable` — `static Task<HostCluster> StartAsync(string connectionString, string schema, LoadMode mode, RunContext ctx, CancellationToken ct)`: 2 хоста `Microsoft.Extensions.Hosting` по §4.3 (`Workers = 32`, очереди `default` и `critical`, `Maximum Pool Size=20`, `Application Name=pgbolt-load-host-<n>`, `UsePreparedStatements` в `*-prepared`).
  - `sealed class Producer : IAsyncDisposable` — `static Task<Producer> StartAsync(string connectionString, string schema, LoadMode mode, RunContext ctx, int rate, CancellationToken ct)` (`rate` — начальный поток `TokenBucket`; генерация начинается здесь, после `SchemaPreparer` и старта `HostCluster`; хост с `AddPgBolt`, `Maximum Pool Size=8`, `Application Name=pgbolt-load-producer`); `void SetRate(int rate)` — вызов `ProducerPipeline.SetRate(rate)` (поток и потолок rate × 1 с меняются вместе); `Task StopAsync()`; `ProducerWindow TakeWindow()` (`sealed record ProducerWindow(long Enqueued, long EnqueueCalls, long EnqueueErrors, double EnqueueP99Ms, TimeSpan MaxGeneratorWait, long DroppedTokens)`) — статистика продюсера за окно со сбросом для `WindowStats`: `Enqueued` — задачи пачек, вызов `EnqueueManyAsync` которых успешно завершился в окне (источник `Enqueued` в `WindowStats` и недогруза; считается в `Producer`), `EnqueueCalls`, `EnqueueErrors`, p99 вызова, а ожидание генератора и отброшенные токены — из `ProducerPipeline.TakeWindow()`. Тонкая обвязка над `ProducerPipeline` (Task 12): конвейер создаётся с `onSeqIssued = ctx.Counter.EnsureChunk` и `pickDelaySeconds = () => ProfileB.PickDelaySeconds(Random.Shared)` (4 отправителя вызывают параллельно, `Random` генератора `ProfileB` не передаётся); `long Issued` — `Stats.Generated` конвейера; делегат `send` собирает из `SendRequest` `LoadArgs` (`EnqueuedAtMicros`, `RunAtMicros` берутся из запроса, `Padding` до ~200 байт) и вызывает `EnqueueManyAsync` (у немедленных `Delay` не задаётся), записывает латентность вызова, ошибки и `Seq` неуспешных вызовов в `RunContext.FailedCallSeqs`, `Seq` задач с `FailFirstAttempt` — в `RunContext.FailFirstSeqs`; логика генератора, пачек, канала и отправителей — в конвейере и покрыта его тестами.
  - `static class SchemaPreparer` — `Task PrepareAsync(string connectionString, string schema, int partitions, IndexSet indexes, CancellationToken ct)` (§4.3 «Подготовка схемы»: поднимает собственный короткоживущий `IHost` с `AddPgBolt` (прямая строка подключения, без задач и без генератора), его старт мигрирует схему; затем выполняются `extra` и `EnsurePartitionsAsync`, и хост освобождается до старта `HostCluster`: генерация нагрузки начинается только в `Producer.StartAsync` после старта хостов; `extra` — `CREATE INDEX` на родителе `jobs`; `IPgBoltMaintenance.EnsurePartitionsAsync` вызывается растущим `daysAhead` шагами по 50 до `partitions − 1` (`PartitionManager.EnsureAsync` создаёт все партиции `jobs` и `job_events` одной транзакцией; при P = 400 одним вызовом это около 6 тыс. блокировок при ёмкости ~7,8 тыс. — риск «out of shared memory» на подготовке, а не под нагрузкой); партиции по-прежнему создаёт продуктовый код).
  - `sealed class MeterCounters : IDisposable` — `MeterListener` на `Meter` `PgBolt` хостов и продюсера: `OutcomeDropped`, `StorageErrors` по операции, `Enqueued` (`pgbolt.job.enqueued`) — пишется в отчёт справочно и в SLO и недогрузе не участвует.
  - `sealed class WindowCollector` — закрывает окна 10 с и собирает `WindowStats`; источники — делегаты конструктора (чистая логика без БД и процесса): `Func<long> executedTotal` (`Completed` — дельта общего `Interlocked`-счётчика `RunContext.Executed`, который обработчик увеличивает вместе с `Counter.Increment`), `Func<LatencySummary> closeImmediate` и `Func<LatencySummary> closeDelayed` (`LatencyRecorder.CloseWindow()` обоих рекордеров), `Func<ProducerWindow> takeProducerWindow` (`Producer.TakeWindow`), `Func<TimeSpan> processorTime` (`Process.TotalProcessorTime`), `int processorCount`; доля CPU = Δ`processorTime` / (10 с × `processorCount`); метод `WindowStats CloseWindow(int index, TimeSpan start)`.
  - `sealed record ReportModel` — модель отчёта: параметры прогона (`LoadTestOptions`, режим, время старта), `IReadOnlyList<(string Key, string Value)>` окружения (`EnvironmentInfo`), `IReadOnlyList<SloVerdict>`, `RunData`, сводка `StatementsReader` (`StatementsSummary`), сводка `Sampler` (пропущенные тики, неудачные попытки, максимумы), `IReadOnlyList<string>` предупреждений, `bool Aborted` (пометка «прерван»).
  - `static class CorrectnessCheck` — `Task<CorrectnessData> RunAsync(string connectionString, string schema, RunContext ctx, long issued, MeterCounters counters, CancellationToken ct)` после дренажа ≤ 2 мин (SLO 5, §5.2): сверка счётчика — `ExecutionReconciler.Reconcile(ctx.Counter, issued, <FailFirstSeqs>, <FailedCallSeqs>)` (`issued` = `Producer.Issued` после `StopAsync`), строки `jobs` не в Succeeded, размер `queue`, сверка `jobs.attempt`, `outcome.dropped` и `storage.errors` из `MeterCounters`; флаг `DrainedInTime` выставляет сценарий по итогам дренажа.
  - `static class ReportWriter` — `Task<string> WriteAsync(string outDir, ReportModel model)` → путь `.md`; рядом `.csv`; каталог создаётся; секции §4.6; имя `<yyyyMMdd-HHmm>-<scenario>-<mode>.md` (для `partitions` — с суффиксом комбинации `-p<P>-<indexes>`, см. Task 17); при совпадении имени с существующим файлом добавляет суффикс `-2`, `-3`… (файлы не перезаписываются).
  - `interface IScenario { Task<int> RunAsync(LoadTestOptions options, CancellationToken ct); }`; `Program` выбирает сценарий, перехватывает исключения стенда → код 2 с сообщением, `Console.CancelKeyPress` → отмена.
- [ ] **Step 1: тесты без БД:** `ReportWriterTests.Report_writer_creates_output_directory` (Review Focus 3), `Report_contains_slo_table_and_environment` (заголовки секций §4.6, строки SLO с PASS/FAIL/«нет данных») и `Same_name_does_not_overwrite` (два отчёта с одним именем в одну минуту → два файла); `WindowCollectorTests.Cpu_fraction_and_completed_delta_per_window` — фейковые делегаты: два окна подряд → `Completed` — дельта `executedTotal` за окно, `CpuFraction` = Δ`processorTime` / (10 с × `processorCount`), сводки рекордеров и поля `ProducerWindow` попадают в `WindowStats` без искажений.
- [ ] **Step 2: тесты с Docker** (`Category=Load`, `SmokeTests`, вызов `Program.RunAsync` в процессе; класс помечен `[Collection("Load")]` — прогоны стенда в одном процессе не идут параллельно, Task 11):
  - `Sustained_direct_short` — `sustained --mode direct --rate 500 --warmup 5s --duration 20s --out <tmp>` → код ∈ {0, 1}, отчёт и `.csv` есть; в отчёте каждый обязательный класс `pg_stat_statements` имеет `calls > 0`.
  - `Sustained_bouncer_short` — `--mode bouncer` → код ∈ {0, 1}; в `.csv` есть срезы с непустыми колонками `SHOW POOLS`.
  - `StandFailureTests.Unreachable_connection_exits_with_code_2` (без Docker, без `Category`) — `--connection "Host=127.0.0.1;Port=1;Timeout=3" --mode direct` → код 2 за < 30 с (Review Focus 1).
  - `StandCancelTests.Cancelled_run_drops_schema_and_writes_partial_report` (отдельный класс с `[Trait("Category", "Load")]` и `[Collection("Load")]`; `StandFailureTests` без fixture и без `Category`, чтобы прогон «без Docker» не поднимал контейнеры) — внешняя БД `PostgresFixture` (`--connection`, режим `direct`), отмена через 10 с после старта → код 2, отчёт с пометкой «прерван», схемы прогона в БД нет (Review Focus 2).
- [ ] **Step 3:** Run — FAIL.
- [ ] **Step 4:** Реализация `sustained`: на каждый режим — новая схема, новый `RunContext` (capacity по формуле Interfaces), `SchemaPreparer` (P = 15, `current`), хосты, продюсер, прогрев, сброс `pg_stat_statements`, `ResetTotal()` обоих рекордеров (в начале окна замера прогрев из `Total()` исключается), окно замера с окнами 10 с (в `RunData` — `MeasureStart` и `MeasureEnd` = начало и конец окна замера; окна и срезы прогрева и дренажа остаются в `.csv`, но не оцениваются), снимок `Total()` обоих рекордеров в `MeasureEnd` — до остановки продюсера и дренажа, остановка продюсера, дренаж ≤ 2 мин, `CorrectnessCheck`, остановка хостов, `SloEvaluator.Sustained` (упор в CPU по окнам замера и пометка «ограничено стендом» вычисляются внутри), `ExitCode.For(verdicts, standFailure, cpuBound: SloEvaluator.IsCpuBound(data))`, отчёт, удаление схемы. Код вызова — максимум по режимам (2 > 1 > 0).
- [ ] **Step 5:** Run `dotnet test tests/PgBolt.LoadTest.Tests` — PASS; `dotnet build PgBolt.slnx` — без ошибок.
- [ ] **Step 6:** Commit `feat(loadtest): хосты, продюсер, отчёт и сценарий sustained`.

---

### Task 16: Сценарий `ramp`

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/Scenarios/Ramp.cs`
- Modify: `tests/PgBolt.LoadTest.Tests/Scenarios/SmokeTests.cs`

**Interfaces:**
- Consumes: `Producer.StartAsync(…, rate, …)` и `Producer.SetRate`, `RunContext(capacity)` (Task 15), `RampEvaluator` (Task 13).

- [ ] **Step 1: тест** `Ramp_direct_short` (`Category=Load`) — `ramp --mode direct --ramp-start 500 --ramp-step 500 --ramp-step-duration 30s --ramp-max 1000 --out <tmp>` → код 0; в отчёте строка «максимальный устойчивый rate».
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация §5.3: `Producer.StartAsync` с `rate = --ramp-start`, `RunContext` с capacity по формуле Task 15; без прогрева, сброс `pg_stat_statements` перед первой ступенью, ступени через `SetRate`, оценка по окнам с начала второй трети ступени (в начале оцениваемой части ступени — `BeginSegment()` рекордера немедленных; `RampStep(rate, окна, SegmentSummary())` для SLO 2 по p99 объединения), остановка по `RampEvaluator`; дренажа и SLO 5 нет.
- [ ] **Step 4:** Run — PASS. Commit `feat(loadtest): сценарий ramp`.

---

### Task 17: Сценарий `partitions` и проба блокировок

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/Scenarios/Partitions.cs`, `Measure/LockProbe.cs`; тест `tests/PgBolt.LoadTest.Tests/Measure/LockProbeTests.cs`
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs` (`BindEnqueue` → `internal`; меняется только видимость), `PgBoltStore.Ack.cs` (параметры ack из лямбды `:48` выносятся в `internal static void BindAck(NpgsqlParameterCollection ps, …)`, SQL и поведение не меняются), при необходимости `PgBoltStore.Dequeue.cs` (то же для параметров dequeue `:48` — `BindDequeue`)

**Interfaces:**
- Consumes: `SchemaPreparer`, `HostCluster`, `Producer`, `PartitionsVerdict`, `StatementsReader`.
- Produces: `sealed class LockProbe(string directConnectionString, string schema)` — `Task<LockCount> MeasureAsync(string sql, Action<NpgsqlParameterCollection> bind, CancellationToken)` (§4.5: `BEGIN; EXPLAIN …; count pg_locks …; ROLLBACK`; параметры statement'ов горячего пути проба берёт у продукта: `bind` — `PgBoltStore.BindEnqueue` для enqueue (11 параметров), `BindAck`/`BindDequeue` для ack и dequeue, а не собирает свои); `Task<PlanKind> DetectPlanAsync(string sql, Action<NpgsqlParameterCollection> bind, ...)` (`PREPARE` + 6× `EXPLAIN EXECUTE` + `pg_prepared_statements`; `@name` переписываются в `$n`, порядок `$n` берётся из коллекции параметров после биндинга); `static long GenericLowerBound(string statement, int partitions, int otherRelations, int survivingPartitions)` (§4.5; `survivingPartitions` — число партиций `jobs`, переживших начальное отсечение: у проб горячего пути ack и enqueue — 1, у dequeue не используется); `sealed record LockCount(long NonFastPath, long Total)`; `enum PlanKind { Custom, Generic }`.

- [ ] **Step 1: тесты** (`Category=Load`, `[Collection("Load")]` — `PostgresFixture` коллекции, Task 11; direct-соединение со своей схемой):
  - `LockProbeTests.Dequeue_locks_grow_with_partitions` — `EnsurePartitionsAsync(daysAhead: 14)` и `(daysAhead: 59)` → `Total` dequeue при 60 партициях > при 15, рост ≥ 4 × 45; `Ack_locks_do_not_grow_with_partitions` — `Total` ack одинаков (± 2).
  - `LockProbeTests.Detects_plan_kind` — `DetectPlanAsync` для ack возвращает `Custom` или `Generic` без исключения и `DEALLOCATE` выполнен (`pg_prepared_statements` пуст после вызова).
  - `LockBoundTests.GenericLowerBound_matches_formula` (отдельный класс без `Category` и вне коллекции `Load`: БД не нужна, тест идёт в обычном прогоне `Category!=Load`) — dequeue при P = 60: 60 + 60 + прочие; ack и enqueue при `survivingPartitions = 1`: P + 1 + прочие (нижняя оценка: при id одних суток переживает одна партиция, то есть один PK).
  - `SmokeTests.Partitions_direct_short` — `partitions --mode direct --partitions 15,20 --indexes current --rate 500 --warmup 5s --duration 20s` → код ∈ {0, 1}; сводная таблица и ёмкость таблицы блокировок в отчёте.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация §6.2–§6.3: матрица P × индексы × режимы, на комбинацию — новая схема, `SchemaPreparer(P, indexes)`, проба блокировок до нагрузки (для `*-prepared` — тип плана), прогрев, `ResetTotal()` рекордеров, замер (`RunData.MeasureStart` и `MeasureEnd` — его начало и конец, `Total()` снимается в `MeasureEnd`, Task 15), SLO 1–2, итог по `PartitionsVerdict`; отчёты — по одному на комбинацию с именем `<yyyyMMdd-HHmm>-partitions-<mode>-p<P>-<indexes>.md` плюс сводный `<yyyyMMdd-HHmm>-partitions-summary.md` (§6.3; `ReportWriter` не перезаписывает существующие файлы, Task 15); `track_planning=on` в своих контейнерах; ёмкость таблицы блокировок — из `current_setting` по формуле §6.1; `bouncer-prepared` — проверка `max_prepared_statements` (§4.2).
- [ ] **Step 4:** Run `dotnet test tests/PgBolt.LoadTest.Tests` — PASS.
- [ ] **Step 5:** Commit `feat(loadtest): сценарий partitions и проба блокировок statement'ов`.

---

### Task 18: Сценарий `vacuum`

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/Scenarios/Vacuum.cs`
- Modify: `tests/PgBolt.LoadTest.Tests/Scenarios/SmokeTests.cs`

**Interfaces:**
- Consumes: `SloEvaluator.Vacuum`, `PhaseMarks` (Task 13), `LoadEnvironment` с `--pg-setting` (Task 14).

- [ ] **Step 1: тест** `Vacuum_direct_short` (`Category=Load`) — `vacuum --mode direct --rate 500 --warmup 5s --phase1 20s --hold-xmin 10s --phase3 20s --queue-reloptions "autovacuum_vacuum_cost_delay=0"` → код ∈ {0, 1}; в отчёте строки SLO 6–9 (7 — «нет данных» при фазе < 3 мин), reloptions `queue` в окружении содержат `autovacuum_vacuum_cost_delay=0`, есть строка входов выбора `ModeOutcome` (выполнение SLO 7–9, пик `pg_total_relation_size(queue)` фазы 1, p99 немедленных фазы 1).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация §7.2: `ALTER TABLE queue SET (...)` до старта хостов; прогрев, фаза 1, фаза 2 — отдельное прямое соединение `BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1 FROM <schema>.queue LIMIT 1;` удерживается до конца фазы, фаза 3; без дренажа; `pg_stat_statements` сбрасывается перед фазой 1; в начале фазы 1 — `ResetTotal()` рекордеров (прогрев из `Total()` исключается). `RunData.MeasureStart` = `Phases.Phase1Start`, `MeasureEnd` = `Phases.End`; `SloEvaluator.Vacuum` сам вычисляет упор в CPU и пометку «ограничено стендом» по окнам фаз 1–3 (Task 13), а `ExitCode.For` получает `SloEvaluator.IsCpuBound(data)`. `Total()` немедленных снимается в конце фазы 1 — после закрытия последнего окна фазы 1 и до начала фазы 2 (дальше `Total()` продолжает копить окна фаз 2–3). Отчёт пишет строку входов `ModeOutcome` режима: выполнение SLO 7–9, пик `pg_total_relation_size(queue)` в фазе 1, p99 SLO 2 фазы 1 (по этому снимку объединённой гистограммы немедленных фазы 1); эти значения подаются в `VacuumWinner` (Task 21).
- [ ] **Step 4:** Run — PASS. Commit `feat(loadtest): сценарий vacuum`.

---

### Task 19: README стенда

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/README.md`

- [ ] **Step 1:** README по §10: назначение, требования (Docker, ≥ 30 ГБ свободно в образе диска Docker), команды каждого сценария (по умолчанию и короткий режим), таблица флагов (§4.1), коды возврата, описание отчёта (§4.6), длительность полного набора (§12), smoke: `dotnet test tests/PgBolt.LoadTest.Tests --filter "Category=Load"`.
- [ ] **Step 2:** Проверить каждую команду README коротким режимом (`--rate 500 --warmup 5s --duration 20s` и т. п.) — код ∈ {0, 1}.
- [ ] **Step 3:** Commit `docs(loadtest): README нагрузочного стенда`.

---

### Task 20: Прогоны `sustained`, `ramp`, `partitions` и решения

**Files:**
- Create: `benchmarks/PgBolt.LoadTest/results/*.md`, `*.csv`, `docs/performance.md` (черновик: разделы «Профиль B и SLO», «Окружение», «Результаты», «Решения»)

- [ ] **Step 1:** В фоне (Global Constraints), последовательно: `dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- sustained`, затем `-- ramp`, затем `-- partitions`. Expected: коды 0 или 1 (2 — сбой стенда: разобрать причину по отчёту, исправить стенд отдельным коммитом `fix(loadtest): …`, повторить сценарий).
- [ ] **Step 2:** Сохранить выгрузки локально и сверить выводы с измеренными окнами.
- [ ] **Step 3:** Решения по правилам §9 шаг 2 и §6.3: для каждого нарушенного SLO и порога P — находка, показатель, классификация «локальная/крупная» (при сомнении — крупная), запись в `docs/performance.md` с цифрами и условиями прогона. Крупные — список для Task 23 (пункты 7b-2).
- [ ] **Step 4:** Commit `docs(perf): результаты и решения sustained, ramp, partitions`.

---

### Task 21: Прогоны `vacuum`, выбор reloptions `queue`

**Files:**
- Modify: `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql:19-20` (только если победил кандидат), `docs/performance.md`, `benchmarks/PgBolt.LoadTest/results/*`

- [ ] **Step 1:** В фоне: `-- vacuum` (текущие reloptions) и `-- vacuum --queue-reloptions "autovacuum_vacuum_cost_delay=0"`.
- [ ] **Step 2:** Выбор табличного победителя: значения `ModeOutcome` обоих режимов берутся из строки входов в отчётах (Task 18) и применяются по правилам §7.4 вручную — `VacuumWinner.Choose` в CLI стенда не вызывается, его поведение закреплено `VacuumWinnerTests`; допустим одноразовый вызов `Choose` в локальном тесте/скрипте на этих значениях. Входные значения и результат записать в `docs/performance.md`. То же для `ChooseGlobal` в Step 3.
- [ ] **Step 3:** Если базовый кандидат не выполняет SLO 7–9 хотя бы в одном режиме: `-- vacuum --pg-setting "autovacuum_naptime=10s" --queue-reloptions "<reloptions табличного победителя>"`; `VacuumWinner.ChooseGlobal`; результат — только рекомендация в `docs/performance.md` (§7.4).
- [ ] **Step 4:** Если табличный победитель — кандидат: `V001__initial.sql` — `autovacuum_vacuum_cost_delay = 0` в `WITH (...)` таблицы `queue` (остальные параметры как есть); тест `MigrationTests` (или существующий тест reloptions, `grep -rn "autovacuum_vacuum_threshold" tests`) дополнить ожиданием `autovacuum_vacuum_cost_delay=0`. Полный прогон — PASS.
- [ ] **Step 5:** Повторный прогон `-- vacuum` (без `--queue-reloptions`) в обоих режимах на новой V001 (§9 шаг 4): сравнение с прежним прогоном затронутого сценария — прогоном на текущих reloptions (Step 1 без `--queue-reloptions`, который отчёт повторного прогона заменяет в сводке) — по правилу §7.4: `VacuumWinner.Choose(current: прогон Step 1 на текущих reloptions, baseline: повторный прогон)`. Победитель не повторный прогон (равенство или проигрыш) — откат правки V001, находка → 7b-2. Отчёт кандидата (Step 1) остаётся в `results/` для справки; конфигурации повторного прогона и кандидата совпадают, поэтому между собой не сравниваются.
- [ ] **Step 6:** Commit(ы): при правке — `perf(postgres): autovacuum_vacuum_cost_delay = 0 для queue`, `docs(perf): решение по autovacuum queue`.

---

### Task 22: Локальные правки по находкам

**Files:**
- Modify: определяются находками Task 20 (только классы «локальная правка» §9)

- [ ] **Step 1:** Если локальных находок, кроме reloptions (Task 21), нет — записать в `docs/performance.md` «локальных правок нет», commit `docs(perf): локальных правок нет` и закрыть задачу.
- [ ] **Step 2:** Иначе для каждой находки отдельно: падающий тест на изменяемое поведение (если меняется поведение кода), правка, полный прогон — PASS, коммит `perf(<область>): <суть>`; повторный прогон затронутого сценария в затронутых режимах в фоне; сравнение по показателю находки (§9 шаг 4, равенство < 10 %); не лучше — `git revert` правки и находка → 7b-2; отчёт и вывод — в `docs/performance.md`, commit `docs(perf): …`.

---

### Task 23: Итоговая документация

**Files:**
- Modify: `docs/performance.md`, `docs/observability.md`, `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md`, `CHANGELOG.md`

- [ ] **Step 1:** `docs/performance.md` по §10: профиль B и SLO; запуск стенда (ссылка на README); окружение замера; сводка `sustained`, `ramp`, таблица `partitions` (порог `current` по режимам; порог `extra` как вход решения об opt-in индексах), `vacuum`; решения цикла §9; рекомендации (срок хранения и число партиций, `max_locks_per_transaction`, `autovacuum_naptime`, `autovacuum_max_workers`, PgBouncer: `pool_mode`, `default_pool_size`, `max_prepared_statements` и `UsePreparedStatements`, пулы Npgsql). Все цифры — из отчётов `results/` со ссылками.
- [ ] **Step 2:** `docs/observability.md`: пороги алертов bloat из результатов `vacuum` (§8.3).
- [ ] **Step 3:** Roadmap: 7b → 7b-1 (спека, план, «Статус: реализовано», краткий итог) и 7b-2 (`COPY` против `unnest`, Grafana-дашборды и exemplars, крупные находки Task 20–22 с цифрами); пункт BenchmarkDotNet — закрыт.
- [ ] **Step 4:** `CHANGELOG.md` «Не выпущено»: метрики `pgbolt.cluster.table.*`, reloptions `queue` (если менялись), стенд `PgBolt.LoadTest`, исправления блока A (если Task 8 их ещё не внёс — без дублей).
- [ ] **Step 5:** `dotnet build PgBolt.slnx`, полный прогон `Category!=Load` и `dotnet test PgBolt.slnx --filter "Category=Load"` (существующий `LoadSmokeTests` и smoke стенда) — PASS. Commit `docs: производительность, roadmap 7b-1/7b-2, CHANGELOG`.
