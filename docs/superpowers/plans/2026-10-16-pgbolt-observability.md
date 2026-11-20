# PgBolt 7a — Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Встроить в PgBolt трассировку enqueue → execute, метрики пода и кластера с корректной агрегацией по подам и логи `[LoggerMessage]` со стабильными `EventId`, плюс пакет `PgBolt.OpenTelemetry`.

**Architecture:** BCL-примитивы (`ActivitySource`, `Meter` из `IMeterFactory`) внутри `PgBolt.Postgres` / `PgBolt.Server`; один `Meter` на DI-контейнер (`PgBoltInstrumentation`), instruments — в singleton'ах `PgBoltClientMetrics` и `PgBoltMetrics`; consumer-span, scope и логи задачи — в `JobRunner`; кластерные gauges — из снимка `queue_stats`, который читает каждый под с серверами. Каждая точка телеметрии изолирована `try/catch`, `PgBoltServer.RunExecutionAsync` не бросает.

**Tech Stack:** .NET 10, C# latest, Npgsql 10, xunit.v3, Testcontainers (PostgreSQL 17 + PgBouncer), `Microsoft.Extensions.Diagnostics.Testing` (`MetricCollector`), OpenTelemetry .NET SDK 1.12.0, BenchmarkDotNet 0.15.8.

**Spec:** `docs/superpowers/specs/2026-10-16-pgbolt-observability-design.md` (далее «спека», ссылки `§N`). Исполнитель читает спеку и план вместе; значения и правила спеки в плане не повторяются, если на них есть ссылка.

## Global Constraints

- Ветка `feature/observability`. Коммиты — автор `NightNo1r <57496218+NightNo1r@users.noreply.github.com>`, **без** `Co-Authored-By`: `git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "…"`. Сообщения коммитов — на русском, conventional-префикс (`feat(server):`, `test:`, `docs:` …), как в истории.
- `TreatWarningsAsErrors=true`, `Nullable=enable`, `IsAotCompatible=true` у `src/` (кроме Dashboard): без reflection-API на горячем пути, без `dynamic`.
- Версии пакетов — только в `Directory.Packages.props`; новые: `Microsoft.Extensions.Diagnostics` 10.0.0, `Microsoft.Extensions.Diagnostics.Testing` 10.0.0, `OpenTelemetry` 1.12.0, `OpenTelemetry.Exporter.InMemory` 1.12.0.
- Имена источника, `Meter`, instruments и границы бакетов — только константы `PgBoltDiagnostics` (§3.1); литералов этих имён вне `PgBolt.Abstractions` нет.
- Имена атрибутов и значения тегов — дословно §3.2, §3.3, §4.1, §4.2 (`pgbolt.queue`, `pgbolt.job.outcome` = `succeeded|retry|failed|cancelled|released`, `pgbolt.drop.reason` = `lease_lost|abandoned|stopped|send_timeout` и т. д.).
- Тексты существующих лог-сообщений и категории логгеров не меняются (существующие тесты сравнивают подстроки `Message`).
- Изменения схемы — только в `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql`.
- События логов `[LoggerMessage]` везде (Task 2, 9, 10, 12, 13) создаются по форме Приложения A, включая `EventName = nameof(Метод)` в атрибуте.
- Комментарии в коде — на русском, со ссылками на спеку в стиле проекта (`// §4.4`, `/// часть 7a §8`), плотность как у соседнего кода.
- Тесты с БД требуют Docker. Команда прогона класса: `dotnet test <csproj> --filter "FullyQualifiedName~<ClassName>"`. Полный прогон перед финалом: `dotnet test PgBolt.slnx`.
- `PgBoltTelemetry.Source` статический, а xunit запускает классы параллельно: слушатель одного класса видит span'ы соседних, бросающий слушатель ломает чужие тесты (8001 в их `ListLogger`). Поэтому тесты с `ActivityListener` на источник `PgBolt` (захватывающим и бросающим) и с бросающим `MeterListener` — в непараллельной коллекции (`DisableParallelization = true`): в `PgBolt.Postgres.Tests` — существующая `Serial` (`[Collection("Serial")]`), в `PgBolt.Server.Tests` — `TelemetryCollection` (Task 8, `Fakes/TestTelemetry.cs`), в `PgBolt.E2E.Tests` — `TelemetryE2ECollection` (Task 15). Захваченные span'ы фильтруются по своему `TraceId` / `messaging.message.id`; `MeterListener` включает только instruments своего экземпляра `Meter` (`instrument.Meter == instrumentation.Meter`).

## Review Focus

1. **Слушатель или логгер-провайдер бросает исключение** — задача должна завершиться тем же исходом, слот освободиться, lease не продлеваться вечно, остановка процесса — пройти. Тесты: Task 9 (`Throwing_listeners_do_not_change_outcome`), Task 10 (`RunExecution_never_throws_with_throwing_logger`).
2. **Хост стартовал внутри чужой `Activity`** (тестовая фикстура, обёртка) — задачи без `trace_parent` не должны становиться её детьми. Тест: Task 9 (`Ambient_activity_is_not_parent_without_trace_parent`).
3. **Простой кластера (пустая `queue`)** — `pgbolt.cluster.queue.jobs` публикуется нулями, а не пропадает; пропадает только при устаревшем снимке. Тесты: Task 8 (`Fresh_empty_snapshot_publishes_zeros`), Task 15 (E2E idle).
4. **Два хоста в одном процессе / повторный старт хоста в тестах** — измерения не смешиваются, callbacks остановленного хоста не публикуются. Тесты: Task 2 (`Two_containers_have_isolated_meters`), Task 8 (`Stopped_server_sources_are_not_published`).
5. **`trace_parent` в не-W3C формате или мусор в колонке** — корневой span без ошибки и лога; producer от Hierarchical-родителя не пишет свой id. Тесты: Task 9 (`Malformed_trace_parent_starts_root`), Task 7 (`Hierarchical_parent_is_not_written`).

---

## Карта файлов

| Файл | Ответственность | Task |
|---|---|---|
| `benchmarks/PgBolt.Server.Benchmarks/*` | бенчмарк `JobRunner` | 1, 16 |
| `src/PgBolt.Abstractions/PgBoltDiagnostics.cs` | публичные имена и бакеты | 2 |
| `src/PgBolt.Postgres/Diagnostics/PgBoltInstrumentation.cs` | `Meter` контейнера, `ReportFailure` / `ReportCallbackFailure` (8001 / 8002) | 2 |
| `PgBoltInstrumentation.Log` (вложенный) | `[LoggerMessage]` 8001, 8002 | 2 |
| `tests/PgBolt.Testing/ListLogger.cs` | + `EventId`, scopes в `LogEntry` | 2 |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Dequeue*.cs`, `StoreRecords.cs` | `wait_seconds` | 3 |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Stats.cs`, `QueueStatsRecords.cs`, `V001__initial.sql`, `PgBoltInitializer.cs` | колонка, чтение снимка | 4 |
| `src/PgBolt.Postgres/Storage/QueueSettingsRecords.cs`, `PgBoltStore.QueueSettings.cs`, `src/PgBolt.Server/Runtime/QueueSettingsCache.cs`, `IFetchStrategy.cs` | лимит в активных настройках | 5 |
| `src/PgBolt.Postgres/Diagnostics/PgBoltTelemetry.cs`, `ProducerSpan.cs`, `PgBoltClientMetrics.cs`; клиент, workflow, recurring | producer-span, `job.enqueued` | 6, 7 |
| `src/PgBolt.Server/Runtime/PgBoltMetrics.cs`, `ClusterQueueStats.cs` | instruments сервера, кластерные gauges | 8 |
| `src/PgBolt.Server/Runtime/JobRunner.cs` (+ вложенный `Log`), `JobLogScope.cs`, `OutcomeClassifier.cs`; проводка параметров: ctor `PgBoltServer.cs`, `PgBoltProcessingService.cs:90`, `JobRunnerBenchmarks.cs` | consumer-span, scope, логи задачи | 9 |
| `src/PgBolt.Server/Runtime/PgBoltServer.cs`, `Dispatcher.cs` | последний рубеж, источники метрик | 10 |
| `src/PgBolt.Server/Runtime/CompletionSink.cs`, `LeaseKeeper.cs`, `QueueSettingsCache.cs`, `OutcomeStatsFlusher.cs` | `outcome.dropped`, `storage.errors` | 11 |
| `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs`, `RetentionReport.cs` | цикл `stats`, `maintenance.*` | 12 |
| `*Log.cs` рядом с компонентами | перевод логов на `[LoggerMessage]` (Приложение A) | 13 |
| `src/PgBolt.OpenTelemetry/*`, `tests/PgBolt.OpenTelemetry.Tests/*` | пакет | 14 |
| `tests/E2E/PgBolt.E2E.Tests/ObservabilityEndToEndTests.cs` | E2E | 15 |
| `docs/observability.md`, roadmap, worker-спека, `CHANGELOG.md` | документация | 16 |

---

### Task 1: Бенчмарк `JobRunner` — baseline до 7a

**Files:**
- Create: `benchmarks/PgBolt.Server.Benchmarks/PgBolt.Server.Benchmarks.csproj`, `Program.cs`, `JobRunnerBenchmarks.cs`, `README.md`, `results/JobRunnerBenchmarks-baseline-report-github.md`
- Modify: `PgBolt.slnx` (папка `/benchmarks/`), `src/PgBolt.Server/PgBolt.Server.csproj` и `src/PgBolt.Postgres/PgBolt.Postgres.csproj` (`<InternalsVisibleTo Include="PgBolt.Server.Benchmarks" />`)

**Interfaces:**
- Produces: класс `JobRunnerBenchmarks` с `[GlobalSetup]` и бенчмарком `RunAsync_success` (один `JobRunner.RunAsync` успешной no-op задачи); в Task 16 добавляются варианты со слушателями.

- [ ] **Step 1:** csproj: `Exe`, `IsPackable=false`, `IsTestProject=false`, `PackageReference BenchmarkDotNet`, `ProjectReference ..\..\src\PgBolt.Server\PgBolt.Server.csproj`, `Microsoft.Extensions.DependencyInjection`. `Program.cs`: `BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args)`.
- [ ] **Step 2:** `JobRunnerBenchmarks`: `[MemoryDiagnoser]`; setup собирает `ServiceCollection` с no-op `IJob<BenchArgs>` (свой `JsonSerializerContext`), `JobRegistry.TryAdd` / `AddExecutor` как в `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs` (helper `Runner`), `RetryDecider` с `NullLogger`, `NullLoggerFactory`, `MonoClock(TimeProvider.System)`; бенчмарк создаёт `Execution(job, "q", 0, ExecutionState.Running)` и `await runner.RunAsync(execution, CancellationToken.None)`.
- [ ] **Step 3:** Run: `dotnet run -c Release --project benchmarks/PgBolt.Server.Benchmarks -- --filter '*JobRunner*'`. Expected: отчёт с колонкой `Allocated`. Скопировать `BenchmarkDotNet.Artifacts/results/*JobRunnerBenchmarks-report-github.md` в `results/JobRunnerBenchmarks-baseline-report-github.md`.
- [ ] **Step 4:** README: назначение (критерий §1 №4), команда запуска, правило: `results/*-baseline-*` — до 7a, `results/*-7a-*` — после.
- [ ] **Step 5:** `dotnet build PgBolt.slnx` — без ошибок; commit `chore(bench): бенчмарк JobRunner и baseline до части 7a`.

---

### Task 2: `PgBoltDiagnostics`, `PgBoltInstrumentation`, `ListLogger` с `EventId`

**Files:**
- Create: `src/PgBolt.Abstractions/PgBoltDiagnostics.cs`, `src/PgBolt.Postgres/Diagnostics/PgBoltInstrumentation.cs`, `tests/PgBolt.Postgres.Tests/Diagnostics/PgBoltInstrumentationTests.cs`, `tests/PgBolt.Postgres.Tests/ListLoggerTests.cs`, `tests/PgBolt.Abstractions.Tests/PgBoltDiagnosticsTests.cs`
- Modify: `Directory.Packages.props` (4 пакета Global Constraints), `src/PgBolt.Postgres/PgBolt.Postgres.csproj` (явный `PackageReference Microsoft.Extensions.Logging.Abstractions` — генератор `[LoggerMessage]`), `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`, `tests/PgBolt.Testing/ListLogger.cs`, `tests/PgBolt.Postgres.Tests/*.csproj` и `tests/PgBolt.Server.Tests/*.csproj` (+ `Microsoft.Extensions.Diagnostics.Testing`, `Microsoft.Extensions.Diagnostics`)

**Interfaces:**
- Produces:
  - `public static class PgBoltDiagnostics` — дословно §3.1.
  - `internal sealed class PgBoltInstrumentation : IDisposable` — `PgBoltInstrumentation(IMeterFactory? meterFactory, TimeProvider time, ILoggerFactory? loggers)`; `Meter Meter { get; }`; `void ReportFailure(Exception error)`; `void ReportCallbackFailure(Exception error)`; `static string Version` (информационная версия сборки `PgBolt.Postgres`). Регистрация в `AddPgBolt`: `TryAddSingleton(sp => new PgBoltInstrumentation(sp.GetService<IMeterFactory>(), sp.GetRequiredService<TimeProvider>(), sp.GetService<ILoggerFactory>()))`.
  - вложенный `PgBoltInstrumentation.Log` (`internal sealed partial class PgBoltInstrumentation`, логгер категории `PgBolt.Telemetry`): события 8001, 8002 — Приложение A.0.
  - `LogEntry(string Category, LogLevel Level, string Message, Exception? Exception, EventId EventId = default, IReadOnlyList<object> Scopes = null!)` — `Scopes` — снимок стека scope'ов на момент записи (никогда не `null` у записей `ListLogger`: пустой массив); `ListLogger.BeginScope` кладёт `state` в `AsyncLocal`-стек корневого логгера и возвращает `IDisposable`, снимающий его.
  - `ListLogger.OnBeginScope` (`Action<object>?`, задаётся у корневого, как `OnEntry`) — вызывается синхронно в `BeginScope` любого логгера из `CreateLogger` до записи в стек и до увеличения `OpenScopes`; бросающий хук моделирует логгер-провайдер, который бросает на `BeginScope` (Task 9).
  - `ListLogger.OpenScopes` (`int`, общий счётчик корня для всех логгеров из `CreateLogger`) — число открытых и не снятых scope'ов: `BeginScope` увеличивает его (`Interlocked`), `Dispose` возвращённого `IDisposable` (идемпотентный) уменьшает. `AsyncLocal`-стек изменение внутри `RunAsync` вызывающему не возвращает, поэтому «scope закрыт» проверяется счётчиком.

- [ ] **Step 1: тесты.**
  - `PgBoltDiagnosticsTests.Names_and_buckets_match_spec` — `ActivitySourceName == "PgBolt"`, `MeterName == "PgBolt"`, все 14 `Instruments.*` дословно §3.1, `HistogramBuckets.Job` и `.Maintenance` поэлементно §4.5, обе последовательности строго возрастают.
  - `PgBoltInstrumentationTests.Meter_comes_from_factory_when_registered` — `ServiceCollection` + `AddMetrics()` + `AddPgBolt(o => o.ConnectionString = "Host=unused")`; `Meter.Name == "PgBolt"`, `Meter.Version == PgBoltInstrumentation.Version`, `Meter.Scope` — `IMeterFactory` контейнера.
  - `Two_containers_have_isolated_meters` — два провайдера с `AddMetrics()`; `MetricCollector<long>(meterScope: factory1, "PgBolt", "x")` видит счётчик, созданный на `Meter` первого контейнера, и не видит второго.
  - `Own_meter_is_disposed_with_container` — без `AddMetrics`; после `provider.Dispose()` `MeterListener` (включает только instruments этого `Meter`, Global Constraints) получает `MeasurementsCompleted` для instrument'а этого `Meter`.
  - `ReportFailure_logs_8001_once_per_minute` — `FakeTimeProvider`, `ListLogger`; три вызова → одна запись `EventId.Id == 8001`, категория `PgBolt.Telemetry`, Warning, исключение первое; `Advance(1 min)` → вторая запись. То же для `ReportCallbackFailure` / 8002 (отдельный ограничитель).
  - `ReportFailure_swallows_logger_exception` — логгер-провайдер бросает на `Log` → вызов не бросает.
  - `ListLoggerTests.Captures_event_id_and_scopes` (в `PgBolt.Postgres.Tests`) — `BeginScope("a")` → запись видит `Scopes == ["a"]`, после `Dispose` — пусто; `OpenScopes` — 1 до `Dispose` и 0 после (повторный `Dispose` не уводит в минус); `EventId` сохраняется; `OnBeginScope` вызывается с `state`, а при броске хука `BeginScope` бросает и `OpenScopes` остаётся 0.
- [ ] **Step 2:** Run обоих классов — FAIL (типов нет).
- [ ] **Step 3:** Реализация. Ограничитель — `long` последнего события по `time.GetTimestamp()` + `Interlocked.CompareExchange`. `Dispose` освобождает `Meter`, только если создал его сам.
- [ ] **Step 4:** Run — PASS; `dotnet test PgBolt.slnx --filter "FullyQualifiedName~ListLogger|FullyQualifiedName~Instrumentation|FullyQualifiedName~Diagnostics"` — PASS.
- [ ] **Step 5:** Commit `feat(diagnostics): имена PgBoltDiagnostics и Meter контейнера (часть 7a §3.1, §4.1, §8)`.

---

### Task 3: `wait_seconds` в dequeue

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/StoreRecords.cs:81-94` (`LeasedJob`), `PgBoltStore.Dequeue.cs` (`LeaseSet`, `SelectLeasedJobs`, `ReadLeasedJob`), `PgBoltStore.DequeueLimited.cs`
- Test: `tests/PgBolt.Postgres.Tests/DequeueWaitTests.cs`

**Interfaces:**
- Produces: `LeasedJob(..., short? Continuation = null, double WaitSeconds = 0)`; столбец `wait_seconds` — последний в `SelectLeasedJobs`.

- [ ] **Step 1: тесты** (абстрактный класс `(PostgresFixture pg, ConnectionMode mode)` + `_Direct` / `_Bouncer`, `TestDb.CreateAsync`):
  - `Immediate_job_wait_is_time_since_enqueue` — enqueue, `pg_sleep(0.2)`, dequeue → `WaitSeconds` в `[0.2, 5)`.
  - `Delayed_job_wait_counts_from_run_at` — `UPDATE queue SET run_at = now() - interval '30 seconds', ready = true` → `WaitSeconds ≥ 30`.
  - `Limited_dequeue_returns_wait` — очередь с лимитом (`queue_settings`), то же утверждение через `DequeueLimitedAsync`.
  - `Wait_counts_from_trigger_commit_during_lock_wait` — соединение A держит `pg_advisory_xact_lock` очереди (как dequeue с лимитом, часть 3b §6.2) в открытой транзакции; параллельно запускается `DequeueLimitedAsync`; во время его ожидания задача ставится в готовые `UPDATE queue SET run_at = now(), ready = true` и коммитится (trigger); затем A выполняет `SELECT pg_sleep(0.2)` и коммитит → `WaitSeconds ≥ 0.2` (с `now()` — началом транзакции батча, до ожидания lock — получилось бы 0, и тест отличает `clock_timestamp()` от `now()`).
- [ ] **Step 2:** Run `DequeueWaitTests` — FAIL (нет свойства).
- [ ] **Step 3:** `RETURNING` CTE `l` обоих шаблонов дополняется общим фрагментом (константа рядом с `LeaseSet`) `greatest(0, extract(epoch FROM clock_timestamp() - q.run_at))::double precision AS wait_seconds`; `SelectLeasedJobs` — `l.wait_seconds` последним; `ReadLeasedJob` читает `r.GetDouble(o + 13)` (dequeue с лимитом вызывает его только при `!r.IsDBNull(1)` — строка без задачи не читается).
- [ ] **Step 4:** Run `DequeueWaitTests` и существующие `DequeueTests`, `DequeueLimited*`, `QueueLimitTests` — PASS.
- [ ] **Step 5:** Commit `feat(postgres): время ожидания задачи в dequeue (часть 7a §7.1)`.

---

### Task 4: Снимок `queue_stats`: колонка, `ReadQueueStatsAsync`, проверка схемы

**Files:**
- Create: `src/PgBolt.Postgres/Storage/QueueStatsRecords.cs`, `tests/PgBolt.Postgres.Tests/QueueStatsSnapshotTests.cs`
- Modify: `V001__initial.sql:190-197`, `PgBoltStore.Stats.cs`, `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs:143-154`, `src/PgBolt.Server/Runtime/IServerStore.cs`, `PgBoltServerStore.cs`, `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`, `tests/PgBolt.Postgres.Tests/HostingTests.cs`, `tests/PgBolt.Postgres.Tests/MigrationTests.cs:179-181` (ожидаемая строка колонок `queue_stats` дополняется `,oldest_ready_seconds:double precision:false`)

**Interfaces:**
- Produces: `internal sealed record QueueStatsRow(string Queue, long Scheduled, long Enqueued, long Processing, long Awaiting, double? OldestReadySeconds)`; `internal sealed record QueueStatsSnapshot(IReadOnlyList<QueueStatsRow> Rows, double? AgeSeconds)`; `internal Task<QueueStatsSnapshot> PgBoltStore.ReadQueueStatsAsync(CancellationToken ct)`; `IServerStore.ReadQueueStatsAsync(CancellationToken)`; `FakeServerStore.OnReadQueueStats` (`Func<CancellationToken, Task<QueueStatsSnapshot>>`, как остальные хуки фейка; по умолчанию `_ => Task.FromResult(new QueueStatsSnapshot([], 0))`) с записью `StoreCall("ReadQueueStats")`.

- [ ] **Step 1: тесты** `QueueStatsSnapshotTests` (Direct/Bouncer):
  - `Snapshot_writes_oldest_ready_seconds` — готовая задача с `run_at = now() - 10 s` и отложенная → строка `oldest_ready_seconds ≥ 10`; очередь только с отложенными → `NULL`; повторный снимок после ухода готовой → upsert обновил колонку в `NULL`; ушедшая очередь удалена.
  - `Read_without_stats_row_has_no_age` — `AgeSeconds == null`, `Rows` пуст.
  - `Read_after_successful_pass_over_empty_queue` — строка `maintenance_tasks('stats')` с `last_finished_at = now() - 3 s`, `last_error = NULL`, `queue_stats` пуст → `Rows` пуст, `AgeSeconds` в `[3, 10)`.
  - `Read_after_trim_error_uses_measured_at` — снимок закоммичен (`measured_at ≈ now()`), строка `stats` с `last_error = 'x'`, `last_finished_at` старше → `AgeSeconds < 2`, строки прочитаны.
  - `Read_after_error_over_empty_table_has_no_age` — `last_error = 'x'`, `queue_stats` пуст → `AgeSeconds == null`.
  - `HostingTests.Initializer_rejects_queue_stats_without_new_column` (в `HostingTests` рядом с `Schema_without_pre_release_tables_fails_start`, helper `Build(...)`; не в `QueueStatsSnapshotTests`) — мигрированная схема, `ALTER TABLE queue_stats DROP COLUMN oldest_ready_seconds` → `PgBoltInitializer` бросает `InvalidOperationException` с текстом о pre-release схеме.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** SQL снимка и чтения — дословно §7.2; колонка `oldest_ready_seconds double precision` последней в `CREATE TABLE`; проверка инициализатора — второй `EXISTS` из §7.2 в том же запросе (`OR`); `ReadQueueStatsAsync` пропускает строку с `queue IS NULL`.
- [ ] **Step 4:** Run `QueueStatsSnapshotTests`, `MigrationTests`, `StatsStoreTests`, `OverviewStoreTests`, `OverviewPagesTests`, `HostingTests` — PASS (позиционные `INSERT` dashboard-тестов работают: новая колонка последняя, `NULL`).
- [ ] **Step 5:** Commit `feat(postgres): возраст старейшей готовой задачи и чтение снимка queue_stats (часть 7a §7.2)`.

---

### Task 5: Эффективный лимит в активных настройках

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/QueueSettingsRecords.cs:10`, `PgBoltStore.QueueSettings.cs:43-51,125-128`, `src/PgBolt.Server/Runtime/QueueSettingsCache.cs:12-14,41-47`, `src/PgBolt.Server/Runtime/IFetchStrategy.cs:19`
- Test: `tests/PgBolt.Postgres.Tests/QueueSettingsSyncTests.cs`, `tests/PgBolt.Server.Tests/Unit/QueueSettingsCacheTests.cs`, `DispatcherTests.cs`, `FetchStrategyTests.cs`, `tests/PgBolt.Server.Tests/Integration/QueueLimitTests.cs`, `FakeServerStore.cs`

**Interfaces:**
- Produces: `public readonly record struct ActiveQueueSetting(string Queue, int? Limit, int? Weight)`; `internal sealed record QueueSettingsSnapshot(FrozenDictionary<string, int> Limits, FrozenDictionary<string, int> Weights)` (`Empty` — оба пустые).

- [ ] **Step 1: тесты.** `QueueSettingsSyncTests.Active_settings_return_effective_limit` — default 5 → `Limit == 5`; override 2 → 2; пауза (override 0) → 0; только вес → `Limit == null`. `QueueSettingsCacheTests.Snapshot_keeps_limit_values` — фейк отдаёт `("a", 3, null), ("b", null, 2)` → `Limits == {a: 3}`, `Weights == {b: 2}`.
- [ ] **Step 2:** Run — FAIL (компиляция).
- [ ] **Step 3:** SQL: `SELECT queue, lim, weight FROM (…)`; чтение `r.GetNullableValue<int>(1)`; кэш строит `Limits` из строк с `Limit is not null`; `DequeueFetchStrategy` — `settings().Limits.ContainsKey(queue)`. Обновить построение снимков и записей в перечисленных тестах.
- [ ] **Step 4:** Run `QueueSettings*`, `FetchStrategyTests`, `DispatcherTests`, `QueueLimitTests` — PASS.
- [ ] **Step 5:** Commit `feat(postgres): эффективный лимит очереди в активных настройках (часть 7a §7.3)`.

---

### Task 6: `PgBoltTelemetry`, `ProducerSpan`, `PgBoltClientMetrics`

**Files:**
- Create: `src/PgBolt.Postgres/Diagnostics/PgBoltTelemetry.cs`, `ProducerSpan.cs`, `PgBoltClientMetrics.cs`; `tests/PgBolt.Postgres.Tests/Diagnostics/ProducerSpanTests.cs`, `PgBoltClientMetricsTests.cs`
- Modify: `PgBoltServiceCollectionExtensions.cs` (регистрация `PgBoltClientMetrics`)

**Interfaces:**
- Consumes: `PgBoltInstrumentation` (Task 2).
- Produces:
  - `internal static class PgBoltTelemetry` — `ActivitySource Source` (имя `PgBoltDiagnostics.ActivitySourceName`, версия `PgBoltInstrumentation.Version`); `const string System = "pgbolt"`; вложенный `static class Attr` с константами всех имён атрибутов §3.2, §3.3, §4.1; `static string? CurrentTraceParent()` (перенос из `PgBoltJobClient`, поведение то же).
  - `internal readonly struct ProducerSpan : IDisposable` — `static ProducerSpan Start(PgBoltInstrumentation instrumentation, string name, string? queue, string? jobType, int messageCount, string? recurringId)`; `string? TraceParent { get; }` (правило §3.2: id span'а при W3C, иначе `CurrentTraceParent()` после восстановления); `void Fail(Exception error)` (статус Error, `error.type` = полное имя типа, `AddException`); `Dispose()` — `Stop` + восстановление сохранённого `Activity.Current`; `void SetQueue(string? queue)` — ставит `messaging.destination.name`, если `queue` не `null` и span создан (очередь становится известна уже после `Start`, Task 7); не вызывается там, где очередь неоднозначна (workflow с несколькими очередями). Все обращения к `Activity` (включая `SetQueue`) — в `try/catch` → `instrumentation.ReportFailure`.
  - `internal sealed class PgBoltClientMetrics(PgBoltInstrumentation instrumentation)` — `void Enqueued(string queue, string jobType, string result, int count)` (`result` ∈ `EnqueueResults.Created` = `"created"`, `Duplicate` = `"duplicate"`; `count == 0` — без записи; защита §8).

- [ ] **Step 1: тесты** (`ActivityListener` на источник `PgBolt` с `Sample = AllDataAndRecorded`; классы в коллекции `Serial`, span'ы фильтруются по своему `TraceId`, Global Constraints):
  - `Producer_span_is_child_of_current_and_written_as_trace_parent` — внешняя W3C-`Activity`; `Start(..., "enqueue t", "q", "t", 1, null)` → `Kind == Producer`, родитель — внешняя, теги `messaging.system=pgbolt`, `messaging.operation.type=send`, `messaging.destination.name=q`, `pgbolt.job.type=t`, без `messaging.batch.message_count`; `TraceParent == span.Id`; после `Dispose` `Activity.Current` — внешняя.
  - `Batch_count_is_tagged_when_more_than_one` — `messageCount = 3` → тег `3`.
  - `Without_listener_trace_parent_is_current` — слушателя нет → `TraceParent == внешняя.Id`; без внешней → `null`.
  - `Hierarchical_parent_is_not_written` — внешняя `Activity` с `SetIdFormat(ActivityIdFormat.Hierarchical)` до `Start` → producer наследует Hierarchical → `TraceParent == null`.
  - `Throwing_ActivityStarted_restores_current` — слушатель бросает в `ActivityStarted` → `Start` не бросает, `Activity.Current` — внешняя, `TraceParent == внешняя.Id`, одно событие 8001.
  - `Throwing_Sample_restores_current` — слушатель бросает в `Sample` → `Start` не бросает, span `null` (`TraceParent == внешняя.Id`), `Activity.Current` — внешняя, одно событие 8001.
  - `Throwing_ActivityStopped_restores_current` — слушатель бросает в `ActivityStopped` → `Dispose` не бросает, `Activity.Current` — внешняя (сохранённая при `Start`), одно событие 8001.
  - `SetQueue_sets_destination_name` — `Start(..., queue: null, ...)`, затем `SetQueue("q")` → тег `messaging.destination.name=q`; `SetQueue(null)` тега не ставит; при не созданном span'е (нет слушателя) вызов ничего не делает.
  - `Fail_sets_error_status` — `Fail(new InvalidOperationException())` → `Status == Error`, `error.type == "System.InvalidOperationException"`, событие `exception`.
  - `PgBoltClientMetricsTests.Enqueued_adds_with_tags` — `MetricCollector<long>` на `Meter` инструментирования: `Enqueued("q", "t", "created", 2)` → значение 2 с тегами `pgbolt.queue=q`, `pgbolt.job.type=t`, `pgbolt.enqueue.result=created`; `count = 0` — без измерения.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация по Interfaces; `PgBoltJobClient.CurrentTraceParent` удалить, вызовы перевести на `PgBoltTelemetry.CurrentTraceParent()`.
- [ ] **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(postgres): producer-span и счётчик постановки (часть 7a §3.2, §4.2)`.

---

### Task 7: Producer-span и `job.enqueued` в клиенте, workflow, trigger

**Files:**
- Modify: `src/PgBolt.Postgres/Client/PgBoltJobClient.cs`, `Client/Workflows/WorkflowBuilder.cs`, `Client/RecurringJobManager.cs`, места создания `WorkflowBuilder`, `PgBoltServiceCollectionExtensions.cs:53,55` (новые параметры конструкторов); в `tests/PgBolt.Postgres.Tests` прямые создания обновляются новыми параметрами: `WorkflowTestSupport.cs:47`, `RetentionWorkflowTests.cs:20`, `ClientManagementTests.cs:16`, `ClientEnqueueTests.cs:19` (`new PgBoltJobClient(...)`), `RecurringTestSupport.cs:23-24` (`Manager(...)` → `new RecurringJobManager(...)`); `ClientTestSupport.cs` (общий помощник `ClientTelemetry`)
- Test: `tests/PgBolt.Postgres.Tests/Diagnostics/ProducerTracingTests.cs`

**Interfaces:**
- Consumes: `ProducerSpan`, `PgBoltClientMetrics`, `PgBoltInstrumentation` (Task 6, 2).
- Produces: конструкторы `PgBoltJobClient(PgBoltStore, JobTypesCache, IWakeupSignal, PgBoltInstrumentation, PgBoltClientMetrics)`, `RecurringJobManager(…, PgBoltInstrumentation, PgBoltClientMetrics)`, `WorkflowBuilder(…, NpgsqlTransaction?, PgBoltInstrumentation, PgBoltClientMetrics)`.
- Produces (тесты `PgBolt.Postgres.Tests`, `ClientTestSupport.cs`): `internal static class ClientTelemetry` — `static PgBoltInstrumentation Instrumentation()` = `new PgBoltInstrumentation(null, TimeProvider.System, null)`, `static PgBoltClientMetrics Metrics(PgBoltInstrumentation? instrumentation = null)` = `new PgBoltClientMetrics(instrumentation ?? Instrumentation())`; `TestTelemetry` из `PgBolt.Server.Tests` (Task 8) здесь недоступен. Все пять прямых созданий клиента и менеджера в `PgBolt.Postgres.Tests` передают `ClientTelemetry.Instrumentation()` и `ClientTelemetry.Metrics(...)` (один экземпляр инструментирования на пару).

- [ ] **Step 1: тесты** (Direct/Bouncer, слушатель как в Task 6 — классы в коллекции `Serial`, span'ы фильтруются по `TraceId` внешней `Activity`; `MetricCollector` на `Meter` контейнера, клиент из `ServiceCollection` + `AddPgBolt(TestDb)`):
  - `Enqueue_writes_producer_span_id` — внешняя `Activity`; `EnqueueAsync` → `jobs.trace_parent == producer.Id`, имя span'а `enqueue {type}`, родитель — внешняя; `job.enqueued` = 1 `created`.
  - `Duplicate_unique_key_counts_duplicate` — повтор с тем же `UniqueKey` → `duplicate` = 1.
  - `EnqueueMany_counts_once_per_triple` — 3 задачи → одно измерение 3, тег `messaging.batch.message_count = 3`.
  - `Transactional_enqueue_writes_trace_parent` — с `EnqueueOptions.Transaction`, commit → `trace_parent == producer.Id`.
  - `Without_listener_writes_current_activity_id` — без слушателя: `trace_parent == внешняя.Id`; без внешней — `NULL`.
  - `Workflow_submit_has_one_span_for_all_nodes` — 2 узла в очередях `a`, `b` → один span `workflow submit` с `messaging.batch.message_count = 2`, без `messaging.destination.name`; у обеих строк `trace_parent == span.Id`; `job.enqueued` — по измерению на `(a, ta, created)` и `(b, tb, created)`.
  - `Recurring_trigger_writes_trace_parent` — `TriggerAsync` → span `recurring trigger`, тег `pgbolt.recurring.id`, `trace_parent == span.Id` (до 7a — `NULL`); `job.enqueued` по результату.
  - `Workflow_single_queue_sets_destination` — 2 узла в одной очереди `a` → span `workflow submit` с `messaging.destination.name = a`.
  - `Dynamic_enqueue_sets_queue_after_descriptor` — `EnqueueAsync("имя типа", json)` → span `enqueue {имя}` с `messaging.destination.name` = эффективная очередь типа; с `EnqueueOptions.Queue` — очередь из опций.
  - `EnqueueMany_empty_list_has_no_span` — `EnqueueManyAsync` с пустым списком → `[]`, ни одного span'а `enqueue …` и ни одного измерения `job.enqueued`.
  - `Failed_enqueue_marks_span_error` — `EnqueueAsync` по неизвестному имени → исключение наружу, span `enqueue {имя}` со статусом Error и без `messaging.destination.name` (span стартует до поиска типа).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Каждый публичный путь оборачивается: `using var span = ProducerSpan.Start(...)`; `TraceParent: span.TraceParent`; `catch (Exception e) { span.Fail(e); throw; }`; после успеха — `metrics.Enqueued` по результату (enqueue — группы `Created`/`!Created`; workflow — по `plan.Jobs` группировкой `(Queue, Type)` с `Ids.Length`, всё `created`; trigger — по одному результату). Span стартует в публичном методе до первого обращения к `JobTypesCache` и хранилищу (иначе ошибка поиска типа не попадёт в span), а очередь ставится позже через `SetQueue` — единый порядок для всех путей:
  - `EnqueueAsync` / `EnqueueManyAsync` (типизированные и динамическая `EnqueueAsync(string jobName, ...)`): в `EnqueueManyAsync` проверки аргументов и выход при `args.Count == 0` остаются до `Start` — для пустого списка span'а нет; `messageCount` — `args.Count`; `Start(name: "enqueue {type}", queue: null, jobType: type, ...)`, у динамической — до `GetDescriptorAsync`, `type = jobName`; `ProducerSpan` передаётся в `EnqueueCoreAsync` по значению (структура держит ссылку на `Activity`), там после `EnqueueOptionsResolver.Resolve` вызывается `span.SetQueue(<эффективная очередь>)`;
  - workflow — первой строкой async-метода `SubmitCoreAsync` (не синхронной обёртки `SubmitAsync`): `using var span` и `try/catch` вокруг всего тела; `Start("workflow submit", queue: null, ..., messageCount: _jobs.Count, ...)` (`_jobs.Count` — все задачи workflow, включая членов batch'ей: они попадают в `_jobs` через `AddJob` ещё до `SubmitAsync`) до `GetDescriptorAsync` динамических типов и `WorkflowPlanner.Build` (план принимает `span.TraceParent`); после `Build` — если все `plan.Jobs` в одной очереди, `span.SetQueue(queue)`, при нескольких очередях тег не ставится;
  - `RecurringJobManager.TriggerAsync`: `Start("recurring trigger", queue: null, ..., recurringId: id)` до `GetRecurringForTriggerAsync`; после получения строки — `span.SetQueue(row.Queue)` (`null` — без тега).
- [ ] **Step 4:** Run `ProducerTracingTests` и существующие клиентские / workflow / recurring тесты Postgres — PASS.
- [ ] **Step 5:** Commit `feat(client): трассировка постановки задач (часть 7a §3.2)`.

---

### Task 8: `PgBoltMetrics` и `ClusterQueueStats`

**Files:**
- Create: `src/PgBolt.Server/Runtime/PgBoltMetrics.cs`, `ClusterQueueStats.cs`, `tests/PgBolt.Server.Tests/Unit/PgBoltMetricsTests.cs`, `ClusterQueueStatsTests.cs`, `tests/PgBolt.Server.Tests/Fakes/TestTelemetry.cs`
- Modify: `src/PgBolt.Server/PgBoltServerServiceCollectionExtensions.cs:42-51` (singleton'ы `ClusterQueueStats`, `PgBoltMetrics`)

**Interfaces:**
- Consumes: `PgBoltInstrumentation`, `PgBoltDiagnostics`, `QueueStatsSnapshot` (Task 2, 4), `QueueSettingsSnapshot.Limits` (Task 5), `SlotAllocator.InUse`.
- Produces:
  - `internal sealed class ClusterQueueStats(MonoClock clock, PgBoltProcessingOptions options)` — `void Publish(QueueStatsSnapshot snapshot)` (момент получения `clock.Now`, `snapshotAt = Now − AgeSeconds`); `bool TryGetFresh(out IReadOnlyList<QueueStatsRow> rows)` (`false` при `AgeSeconds == null` или `Elapsed(snapshotAt) > 4 × StatsSnapshotInterval`); потокобезопасно через неизменяемый объект + `Volatile`.
  - `internal sealed class PgBoltMetrics(PgBoltInstrumentation instrumentation, ClusterQueueStats cluster)` — `JobDuration(string queue, string jobType, string outcome, TimeSpan duration)`, `JobWaitTime(string queue, string jobType, double seconds)`, `OutcomeDropped(string queue, string reason, int count = 1)`, `StorageError(string operation)`, `MaintenanceRun(string task, string result, TimeSpan duration)`, `RecurringFired(int count)`, `RetentionDeferred(string reason, int count)`, `IDisposable RegisterServer(string serverName, SlotAllocator slots, int capacity, IReadOnlyList<string> queues, Func<QueueSettingsSnapshot> settings)`; `static string OutcomeName(JobOutcome outcome)`; константы `DropReasons.{LeaseLost, Abandoned, Stopped, SendTimeout}`, `MaintenanceResults.{Ok, Error, Deferred}`, `StorageOperations.{Dequeue, Ack, Outcome, Heartbeat, Settings, StatsFlush, StatsRead}` (значения `dequeue`, `ack`, `outcome`, `heartbeat`, `settings`, `stats_flush`, `stats_read` — §4.2; имена циклов `ProcessingMaintenance.LoopAsync` передаются в `StorageError` как есть). Задачи 10–12 передают в `StorageError` только эти константы, литералов `storage.operation` вне `PgBoltMetrics` нет.
  - `TestTelemetry` (tests): `static PgBoltInstrumentation Instrumentation(TimeProvider? time = null, ILoggerFactory? logs = null)` (собственный `Meter`), `static PgBoltMetrics Metrics(PgBoltInstrumentation? i = null, ClusterQueueStats? cluster = null)`; в том же файле `[CollectionDefinition(TelemetryCollection.Name, DisableParallelization = true)] public sealed class TelemetryCollection { public const string Name = "telemetry"; }` — непараллельная коллекция для тестов с `ActivityListener` / бросающим `MeterListener` (Global Constraints).

- [ ] **Step 1: тесты** (`MetricCollector<T>(instrumentation.Meter, PgBoltDiagnostics.Instruments.X)`, observable — `RecordObservableInstruments()`; класс с `MeterListener` — `[Collection(TelemetryCollection.Name)]`, `MeterListener` включает только instruments своего `Meter`):
  - `Histograms_use_spec_buckets` — `MeterListener` видит у `job.duration`, `job.wait_time`, `maintenance.duration` `Advice.HistogramBucketBoundaries` = `PgBoltDiagnostics.HistogramBuckets.*`, единицы `s`.
  - `Synchronous_instruments_have_spec_tags` — по одному вызову каждого метода → имена тегов и значения §4.2; `OutcomeName` для `Ack`/`Retry`/`Fail`/`ConfirmCancel`/`Release` → `succeeded|retry|failed|cancelled|released`; значения `DropReasons.*`, `MaintenanceResults.*` и `StorageOperations.*` дословно §4.2.
  - `Worker_gauges_follow_slot_allocator` — настоящий `SlotAllocator(2, [q1 Reserved=1, q2])`, `Take("q1")` → `worker.active{server, q1} = 1`, `{q2} = 0`, `worker.capacity{server} = 3`.
  - `Stopped_server_sources_are_not_published` — `Dispose` регистрации → нет `worker.*` и `cluster.*` (в том числе `limit`).
  - `Limit_gauge_publishes_effective_limits` — `Limits = {a: 3, p: 0}` → `cluster.queue.limit{a}=3`, `{p}=0`, других очередей нет.
  - `Fresh_empty_snapshot_publishes_zeros` — сервер с очередями `[a, b]`, `Publish(new([], 0))` → `cluster.queue.jobs` для `a` и `b`, четыре `pgbolt.job.state` = 0; `oldest_ready_age` пуст.
  - `Snapshot_queue_without_server_is_published` — строка `c` → её ряды есть.
  - `Oldest_ready_age_only_for_rows_with_value`.
  - `ClusterQueueStatsTests.Staleness` — `FakeTimeProvider`, интервал 10 с: `AgeSeconds = null` → `false`; `AgeSeconds = 5`, `Advance(34 s)` → `true`; `Advance(2 s)` → `false`; новый `Publish` с `AgeSeconds = 0` → `true`.
  - `Shared_queue_of_two_servers_is_published_once` — две регистрации (`s1`, `s2`) с общей очередью `q` и общим `settings` (`Limits = {q: 2}`), свежий снимок → для `q` по одному ряду на каждое из четырёх состояний `cluster.queue.jobs` и один ряд `cluster.queue.limit{q}`; `worker.active` / `capacity` — по ряду на каждый сервер.
  - `Callback_exception_reports_8002` — `RegisterServer(..., settings: () => throw new InvalidOperationException(), ...)` → `RecordObservableInstruments()` не бросает, измерений `cluster.queue.limit` нет, одна запись 8002 (список `queues` не бросающий: `RegisterServer` его копирует, поэтому бросающий список не годится).
  - `Throwing_meter_listener_does_not_throw` — `MeterListener` бросает в `SetMeasurementEventCallback` → `JobDuration(...)` не бросает, одно 8001.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Observable-instruments создаются в конструкторе один раз; callbacks копируют список регистраций под `Lock` и читают только память (§4.6). Кластерные gauges возвращают пусто, если регистраций нет. `RegisterServer` копирует `queues` в массив. Известные очереди — объединение (ordinal, без дублей) очередей всех регистраций и строк снимка, поэтому ряд `jobs` на очередь публикуется один раз; `limit` публикуется один раз на очередь из `settings()` первой регистрации (все серверы пода делят один `QueueSettingsCache`, §4.3).
- [ ] **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(server): instruments сервера и кластерные gauges (часть 7a §4)`.

---

### Task 9: `JobRunner` — consumer-span, scope, логи задачи, защита

**Files:**
- Create: `src/PgBolt.Server/Runtime/JobLogScope.cs`, `tests/PgBolt.Server.Tests/Unit/JobRunnerTelemetryTests.cs`
- Modify: `src/PgBolt.Server/Runtime/JobRunner.cs`, `OutcomeClassifier.cs:34-38`, `PgBoltServer.cs` (ctor :29-48 и вызов `new JobRunner` :46 — только проводка новых параметров), `PgBoltProcessingService.cs:90` (передача `PgBoltMetrics` и `PgBoltInstrumentation` из DI), `benchmarks/PgBolt.Server.Benchmarks/JobRunnerBenchmarks.cs` (setup создаёт `PgBoltInstrumentation(null, TimeProvider.System, null)` и `PgBoltMetrics(instrumentation, new ClusterQueueStats(clock, new PgBoltProcessingOptions()))`, `serverName: "bench"`), `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs` (helper `Runner`), `tests/PgBolt.Server.Tests/Unit/OutcomeClassifierTests.cs` (строки 144, 149: вызовы `NoExecutor` получают `new JobExecutorNotFoundException(type)`)

**Interfaces:**
- Consumes: `PgBoltTelemetry` (Task 6), `PgBoltMetrics`, `TestTelemetry` (Task 8; регистрация `PgBoltMetrics` / `ClusterQueueStats` в `AddPgBoltServer` — там же).
- Produces:
  - `PgBoltServer(ServerDefinition, IServerStore, JobRegistry, IServiceScopeFactory, IReadOnlyList<Type>, IWakeupSignal, MonoClock, ILoggerFactory, OutcomeStats, QueueSettingsCache, PgBoltMetrics metrics, PgBoltInstrumentation instrumentation)` — коммит Task 9 собирается целиком: ctor принимает параметры уже здесь, `_runner = new JobRunner(..., metrics, instrumentation, Name)`; поля `metrics` используются в Task 10.
  - `JobRunner(JobRegistry registry, IServiceScopeFactory scopes, FilterPipeline filters, RetryDecider retry, PgBoltServerOptions options, MonoClock clock, ILoggerFactory loggers, PgBoltMetrics metrics, PgBoltInstrumentation instrumentation, string serverName)`.
  - `OutcomeClassifier.NoExecutor(Execution execution, JobExecutorNotFoundException error, RetryDecider retry)`.
  - вложенный `JobRunner.Log` (`internal sealed partial class JobRunner`): события 3001–3008 — Приложение A.0, пишутся в `_jobLogger` (категория `PgBolt.Job`).
  - `JobLogScope(LeasedJob job, string queue) : IReadOnlyList<KeyValuePair<string, object?>>` — ключи `JobId`, `JobType`, `Queue`, `Attempt`; `ToString()` = `JobId:{id} JobType:{type} Queue:{queue} Attempt:{n}`.

- [ ] **Step 1: тесты** (`ActivityListener` на `PgBolt`, `ListLogger` с `EventId`/`Scopes`/`OpenScopes` из Task 2, `MetricCollector`; класс в `[Collection(TelemetryCollection.Name)]`, захваченные span'ы фильтруются по `messaging.message.id` своей задачи, Global Constraints):
  - `Consumer_span_is_remote_child_of_trace_parent` — `Leased.Job(...) with { TraceParent = "00-<32 hex>-<16 hex>-01", BatchId = b, RecurringId = "r" }` → `Kind == Consumer`, `DisplayName == job.Type`, `ParentId == trace_parent`, `HasRemoteParent`, все стартовые теги §3.3 (`messaging.system`, `messaging.operation.type=process`, `messaging.destination.name`, `messaging.message.id`, `pgbolt.job.type`, `pgbolt.job.attempt`, `pgbolt.job.max_attempts`, `pgbolt.server.name`, `pgbolt.batch.id`, `pgbolt.recurring.id`).
  - `Malformed_trace_parent_starts_root` — `"garbage"` и `null` → `Parent == null`, `ParentSpanId == default`, ни одной записи лога уровня ≥ Warning.
  - `Ambient_activity_is_not_parent_without_trace_parent` — `RunAsync` вызывается внутри `using var ambient = new Activity("host").Start()` → span — корень.
  - `Current_activity_inside_executor_is_consumer_span` — `RunnerProbe.Behavior` запоминает `Activity.Current`.
  - `Outcome_tags_and_status` (Theory) — успех → `succeeded`, статус Unset; исключение с ретраем → `retry`, Error, `error.type` = полное имя, событие `exception`; последняя попытка → `failed`; таймаут (`TimedArgs`) → `error.type == "timeout"`; `Execution.Cancel(CancelReason.LeaseLost)` + `OperationCanceledException` — `lease_lost`; `Execution.Cancel(CancelReason.HeartbeatLost)` + `OperationCanceledException` — `heartbeat_lost`; запрос отмены → `cancelled`; shutdown во время выполнения → `released`.
  - `Workflow_parent_attributes` (Theory по кодам 1–5, как `Workflow_links_follow_continuation_code`) — `parent_job_id` для 1, 2; `parent_batch_id` для 3, 4; ни одного для 5.
  - `No_executor_has_span_and_exception_event` — неизвестный тип → `error.type == "PgBolt.Server.JobExecutorNotFoundException"`, событие исключения, тот же объект в логе `JobNoExecutorWillRetry` (3005).
  - `Shutdown_before_start_has_no_span` — отменённый `shutdown` → нет span'а, запись `JobReleased` (3008, Debug).
  - `Lifecycle_events_match_table` (Theory) — каждая строка таблицы §5.1: `EventId`, уровень, исключение (то же, что у исхода), поля; записи исхода несут `Scopes` с `JobLogScope` и записаны при `Activity.Current == consumer-span` (провайдер фиксирует `Activity.Current?.SpanId` в `OnEntry`).
  - `Scope_is_visible_to_executor_logs` — исполнитель пишет через `JobContext.Logger` → `Scopes` содержит `JobLogScope` с `JobId`.
  - `Metrics_duration_and_wait` — `job.duration` с `pgbolt.job.outcome`, значение = `FakeTimeProvider`-время выполнения; `job.wait_time` = `job.WaitSeconds`; без `ActivityListener` `job.duration` тоже пишется; при shutdown до старта `wait_time` нет.
  - `Throwing_listeners_do_not_change_outcome` — `ActivityListener` бросает в `ActivityStarted` (затем — отдельный прогон — в `ActivityStopped`), `MeterListener` бросает, логгер-провайдер бросает на событиях 3001–3008 (`OnEntry`) и на `BeginScope` (`ListLogger.OnBeginScope`, Task 2) → тот же `JobOutcome`, что без телеметрии; ровно одно событие 8001 (`FakeTimeProvider`, без сдвига); после старта со сбоем `Activity.Current == null` внутри исполнителя.
  - `Exception_outside_protection_stops_span_and_scope` — тип с `TimeoutSeconds` выше `MaxJobTimeout` и логгер, бросающий на предупреждении `TimeoutOf` → `RunAsync` бросает, span остановлен (`ActivityStopped` вызван), scope закрыт (`ListLogger.OpenScopes == 0` после броска; `OnEntry` перед броском запоминает `OpenScopes` на момент предупреждения — он должен быть 1, иначе проверка ничего не доказывает).
- [ ] **Step 2:** Run `JobRunnerTelemetryTests` — FAIL.
- [ ] **Step 3:** Реализация в порядке §3.3 п. 1–5; каждая точка телеметрии — отдельный `try/catch` → `instrumentation.ReportFailure(e)` (§8). Стартовые атрибуты consumer-span'а (`SetTag`) выставляются только на ненулевом `Activity` — при отсутствии слушателя `StartActivity` вернёт `null`, и ни коллекция тегов, ни значения (строки, `Guid.ToString()`) не аллоцируются (критерий §1 №4); значения тегов не вычисляются заранее. Сброс `Activity.Current = null` перед стартом span'а; `ActivityContext.TryParse(job.TraceParent, null, isRemote: true, out var parent)`; `NoExecutor` получает исключение, созданное в `JobRunner`. Обновить helper `Runner` в `JobRunnerTests` (новые параметры из `TestTelemetry`, `serverName: "test"`). Проводка вне `JobRunner`, чтобы коммит собирался: ctor `PgBoltServer` получает `PgBoltMetrics metrics, PgBoltInstrumentation instrumentation` последними параметрами и передаёт их в `new JobRunner(..., metrics, instrumentation, Name)`; `PgBoltProcessingService.cs:90` берёт оба из DI (`GetRequiredService`; `PgBoltInstrumentation` регистрирует `AddPgBolt` — Task 2, `PgBoltMetrics` — `AddPgBoltServer`, Task 8) и передаёт в `PgBoltServer`; `JobRunnerBenchmarks` создаёт `PgBoltInstrumentation` / `PgBoltMetrics` напрямую и передаёт в `JobRunner`.
- [ ] **Step 4:** Run `JobRunnerTelemetryTests`, `JobRunnerTests`, `OutcomeClassifierTests`, `ProcessingServiceTests` — PASS; `dotnet build PgBolt.slnx` (собирает и бенчмарк) — без ошибок.
- [ ] **Step 5:** Commit `feat(server): consumer-span, scope и логи выполнения задачи (часть 7a §3.3, §5.1, §5.2, §8)`.

---

### Task 10: `PgBoltServer` — последний рубеж, источники метрик, проводка

**Files:**
- Modify: `src/PgBolt.Server/Runtime/PgBoltServer.cs` (ctor — `Dispatcher` получает `metrics`; `Start` :66-75, `FlushAndStopAsync` :112-131, `RunExecutionAsync` :190-208, `AbandonRunning` :211-222; параметры ctor `PgBoltServer` уже добавлены в Task 9), `Dispatcher.cs` (ctor, `StartOne` :245-274, catch fetch :129)
- Create: вложенный `PgBoltServer.Log` с событием 1112 `SlotReleaseFailed` (Приложение A.0), `tests/PgBolt.Server.Tests/Unit/RunExecutionTests.cs`
- Test: `DispatcherTests.cs`, `ProcessingServiceTests.cs`

**Interfaces:**
- Consumes: `PgBoltMetrics.RegisterServer`, `OutcomeDropped`, `StorageError` (Task 8); `JobRunner` ctor (Task 9).
- Produces: `Dispatcher(..., PgBoltMetrics metrics)` последним параметром. Ctor `PgBoltServer` — Task 9.

- [ ] **Step 1: тесты** (`ProcessingServiceTests`-стиль: `FakeServerStore`, `FakeTimeProvider`, `PgBoltProcessingService(provider, waitReady)`):
  - `RunExecution_never_throws_with_throwing_logger` — логгер-провайдер начинает бросать на всех событиях всех категорий после `StartAsync` (тест снимает бросающий `OnEntry` — `_logs.OnEntry = null` — после утверждений и до очистки: `DisposeAsync` повторно вызывает `StopAsync(CancellationToken.None)`, и бросок из лога «stopped» уронил бы очистку, а не проверяемое поведение); задача с `TimeoutSeconds` > `MaxJobTimeout` (исключение `RunAsync` из `TimeoutOf`) → задача `tracked` завершена без исключения; `SlotAllocator.InUse(q) == 0`; диспетчер разбужен (следующий `Dequeue` в `FakeServerStore.Calls`); запись снята из `ExecutionTable` и heartbeat её не шлёт; `outcome.dropped{abandoned} == 1`; остановка: `WaitExecutionsAsync` всех серверов без исключения, `LeaseKeeper` и sink остановлены (`StopAsync` может бросить из лога «stopped» — `Record.ExceptionAsync` допускается).
  - `RunExecution_completes_when_slot_release_throws` — `PgBoltServer` получает `internal SlotAllocator Slots => _slots;`; исполнитель задачи сам вызывает `processing.Servers[0].Slots.Release(q, fromReserved: false)` → после выполнения `_slots.Release` бросает `InvalidOperationException` → событие 1112 `SlotReleaseFailed` записано, `Complete` всё равно вызван (ack в `FakeServerStore.Calls`), задача `tracked` без исключения.
  - `Complete_exception_on_stopped_sink_does_not_throw_or_count_abandoned` — настройка: исполнитель игнорирует токен и ждёт `TaskCompletionSource`; `StopAsync` вызывается с уже отменённым `ct` при работающей задаче: `WaitExecutionsAsync` прерывается отменой (запись остаётся `Running`, `AbandonRunning` не вызывается), `FlushAndStopAsync` останавливает sink; после возврата `StopAsync` тест завершает `TaskCompletionSource`, и `_sink.Complete` попадает на остановленный sink. Логгер-провайдер бросает только на «discarded: completion sink is stopped» (исключение `_sink.Complete`, §8 п. 3), а не на «waiting for executions was cancelled» и «stop was cancelled» (иначе `StopAsync` упадёт до остановки sink'а) → `RunExecutionAsync` не бросает (`tracked` завершена), слот освобождён, диспетчер разбужен, `outcome.dropped{abandoned}` не учтён (`Unregister` вернул `false`: запись уже `Removed`); `outcome.dropped{stopped} == 1` дополнительно проверяется в Task 11.
  - `Abandoned_execution_counts_dropped` — исполнитель игнорирует токен, `ShutdownTimeout` истёк → `outcome.dropped{abandoned} == 1`.
  - `Dispatcher_start_failure_counts_abandoned` — `startExecution` бросает → `outcome.dropped{abandoned} == 1`; fetch бросает → `storage.errors{dequeue} == 1`.
  - `Server_sources_unregistered_on_stop_even_when_cancelled` — `FlushAndStopAsync` с отменённым `ct` → `worker.*` и `cluster.*` не публикуются.
  - `Server_sources_unregistered_when_sink_stop_throws` — `FlushAndStopAsync`, где `_sink.StopAsync` / `_keeper.StopAsync` бросает (например, отправка исхода в полёте, остановка с отменённым `ct`, логгер-провайдер бросает на «completion sink stop was cancelled») → исключение выходит наружу (`Record.ExceptionAsync`), `worker.*` и `cluster.*` после этого не публикуются; бросающий `OnEntry` снимается (`_logs.OnEntry = null`) после утверждений и до очистки, как в `RunExecution_never_throws_with_throwing_logger`.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** `RunExecutionAsync` — порядок §8 п. 1–4 дословно. `Start` — `_metricsRegistration = metrics.RegisterServer(Name, _slots, Options.Capacity, <имена очередей сервера>, () => settings.Current)`; `FlushAndStopAsync` — тело в `try`, `finally { _metricsRegistration?.Dispose(); }`. `AbandonRunning` — `OutcomeDropped(execution.Queue, Abandoned)` при успешном переходе. Передача `PgBoltMetrics` / `PgBoltInstrumentation` в `PgBoltServer` и регистрация `ClusterQueueStats` / `PgBoltMetrics` в `AddPgBoltServer` сделаны в Task 9 и Task 8.
- [ ] **Step 4:** Run `RunExecutionTests`, `DispatcherTests`, `ProcessingServiceTests`, интеграционные `BasicScenarioTests` — PASS.
- [ ] **Step 5:** Commit `fix(server): завершение выполнения не бросает и освобождает слот и lease (часть 7a §8)`.

---

### Task 11: `outcome.dropped` и `storage.errors` в sink, keeper, кэше настроек, flusher'е

**Files:**
- Modify: `src/PgBolt.Server/Runtime/CompletionSink.cs` (ctor :35, точки CAS :69, :93, :178, :351, :404; `SendAckBatchAsync` :196, `SendSingleAsync` :239), `LeaseKeeper.cs` (ctor :35, :223, :230, heartbeat-ошибки :189, :195, `RegisterAgainAsync` :276-279 — `catch` без отмены остановкой; успешный исход :270 не считается), `QueueSettingsCache.cs` (ctor :22, :88), `OutcomeStatsFlusher.cs`, места создания (`PgBoltServer`, `PgBoltServerServiceCollectionExtensions.cs:47`, `PgBoltProcessingService` для flusher'а)
- Test: `CompletionSinkTests.cs` (в т. ч. создание :206), `LeaseKeeperTests.cs`, `DispatcherTests.cs` (создание `LeaseKeeper` / `CompletionSink` :72-74), `QueueSettingsCacheTests.cs`, `OutcomeStatsFlusherTests.cs`, `RunExecutionTests.cs` (Task 10; добавляется утверждение `stopped`)

**Interfaces:**
- Consumes: `PgBoltMetrics` (Task 8).
- Produces: `PgBoltMetrics metrics` последним обязательным параметром конструкторов `CompletionSink`, `LeaseKeeper`, `QueueSettingsCache`, `OutcomeStatsFlusher`.

- [ ] **Step 1: тесты** — по одному на каждую строку таблицы `job.outcome.dropped` §4.2 с источником в этих классах (`lease_lost` ×4, `stopped` ×2, `send_timeout`) и на каждую строку `storage.errors` (`ack`, `outcome`, `heartbeat` ×2 — ошибка и таймаут, `RegisterAgainAsync`, `settings`, `stats_flush`); плюс `No_double_count_after_lease_lost` — `LeaseKeeper` перевёл `OutcomePending → Removed`, затем `BeginAttempt` и `Complete` над `Removed` → итог `lease_lost == 1`; `Retry_attempt_lease_lost_is_not_counted` — `LeaseLost` после неудачной попытки → без `outcome.dropped`; `Ack_partial_counts_lease_lost_per_queue` — пакет ack из выполнений двух очередей, подтверждена часть → `lease_lost` учтён отдельным измерением на каждую очередь по числу неподтверждённых; в `RunExecutionTests.Complete_exception_on_stopped_sink_does_not_throw_or_count_abandoned` (Task 10) добавить `outcome.dropped{stopped} == 1`; `Stop_is_not_counted_as_storage_error` — `QueueSettingsCache.StopAsync` при ожидающем цикле → `storage.errors{settings}` нет (аналогично для отменённой остановкой попытки `OutcomeStatsFlusher` — `stats_flush` нет).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Счётчик увеличивает код, выполнивший успешный CAS в `Removed` (таблица §4.2); `storage.errors` — в каждом `catch` таблицы до ограничителя лога, но не для исключения, пойманного при отменённой остановке (`token` / `ct` / `_abort` остановки — то же условие, что у существующего лога, например `!token.IsCancellationRequested` в `QueueSettingsCache.LoopAsync`, `!ct.IsCancellationRequested` в `OutcomeStatsFlusher.FlushOnceAsync`; попытка `SendAckBatchAsync` / `SendSingleAsync`, прерванная `_abort`): иначе счётчик растёт на 1 при каждой остановке пода. Попытка, оборванная бюджетом `LeaseDuration` (`budget`), считается. Ошибка в `LeaseKeeper` и `Dispatcher` — по тому же правилу. `SendAckBatchAsync`: неподтверждённые записи `inFlight` (без `ackedIds`) группируются по `Queue` (как `CountAcked`), `OutcomeDropped(queue, LeaseLost, count)` — на каждую очередь, так как пакет ack содержит выполнения разных очередей. `Complete` / `EnqueueRegistered` на остановленном sink'е: `stopped` учитывается сразу после успешного CAS, до записи лога (бросающий логгер не должен терять счёт).
- [ ] **Step 4:** Run перечисленных классов (в т. ч. `DispatcherTests`, `RunExecutionTests`) — PASS.
- [ ] **Step 5:** Commit `feat(server): метрики потерянных исходов и ошибок хранилища (часть 7a §4.2)`.

---

### Task 12: Цикл `stats` на любом поде с серверами, метрики обслуживания

**Files:**
- Modify: `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs` (ctor :15-17, `Start` :52-70, `StatsOnceAsync` :231-233, `RunTaskAsync` :246-271, `LoopAsync` :319, `TickOnceAsync`), `RetentionReport.cs`, `PgBoltProcessingService.cs:106-110`
- Test: `StatsTaskTests.cs`, `ProcessingServiceTests.cs` (`Maintenance_runs_only_when_enabled` :339, `Flusher_runs_without_maintenance` :478), `ResolverLoopTests.cs:24`, `HousekeepingTests.cs:27`, `ProcessingMaintenanceTests.cs:24`, `Integration/WorkflowResolverLoopTests.cs:17`, `Integration/RecurringBudgetTests.cs:27`, `RetentionReportTests.cs`, `SchedulerLoopTests.cs:26` (создание `ProcessingMaintenance` и тест `Default_owner_without_declarations_starts_no_sync_loop` :134-146)

**Interfaces:**
- Consumes: `ClusterQueueStats.Publish`, `PgBoltMetrics` (Task 8), `IServerStore.ReadQueueStatsAsync` (Task 4).
- Produces: `ProcessingMaintenance(IServerStore, IWakeupSignal, PgBoltProcessingOptions, IPgBoltMaintenance, PgBoltOptions, MonoClock, ILogger, PgBoltMetrics metrics, ClusterQueueStats cluster, RecurringDeclarations? recurring = null, ICompletionSignal? completions = null)`; `RetentionReport.Write(ILogger, RetentionResult, PgBoltOptions, PgBoltMetrics metrics)`.
  - вложенный `ProcessingMaintenance.Log` (`internal sealed partial class ProcessingMaintenance`) с новым событием 5012 `QueueStatsReadFailed` (Warning, с исключением, ограничитель `_statsErrors`) — Приложение A.0; остальные события класса переводит Task 13.

- [ ] **Step 1: тесты.**
  - `StatsTaskTests.Stats_loop_reads_without_maintenance` (замена `Stats_loop_runs_only_with_RunMaintenance`) — `RunMaintenance = false` → нет `Claim("stats")`, на каждом проходе `ReadQueueStats`, результат опубликован в `ClusterQueueStats`.
  - `Owner_reads_after_complete` — `RunMaintenance = true` → порядок вызовов `Claim`, `Snapshot`, `Trim`, `Complete`, `ReadQueueStats` (обновить тесты с точной последовательностью).
  - `Claim_exception_does_not_skip_read` — `OnClaim` бросает → `ReadQueueStats` вызван, затем запись «iteration failed» в логе цикла.
  - `Complete_exception_does_not_skip_read` — `RunMaintenance = true`, `OnComplete` бросает → `ReadQueueStats` вызван (после `Complete`), затем запись «iteration failed» в логе цикла.
  - `Read_error_keeps_snapshot` — `OnReadQueueStats` бросает → `storage.errors{stats_read} == 1`, прежний снимок в `ClusterQueueStats`, проход не бросает; запись лога `EventId.Id == 5012` (Warning, с исключением); второй сбой в пределах минуты (`FakeTimeProvider`) — `stats_read == 2`, новой записи 5012 нет (ограничитель `_statsErrors`).
  - `Maintenance_task_metrics` — успешный `retention` → `maintenance.runs{retention, ok}`, `maintenance.duration{retention}`; исключение → `error`; `TaskOutcome` с ошибкой без исключения (`partitions` lock_timeout) → `deferred`.
  - `Read_cancelled_by_stop_is_not_an_error` — хук `OnReadQueueStats` (получает `ct`) бросает `OperationCanceledException` при отменённом `ct` → `storage.errors{stats_read}` нет, записи 5012 нет, исключение выходит из прохода как отмена цикла.
  - `Loop_iteration_error_counts_storage_error` — исключение итерации `promoter` → `storage.errors{promoter}`.
  - `Scheduler_tick_counts_fired` — `OnTick` с `Fired = 3` → `recurring.fired == 3`.
  - `RetentionReportTests.Deferred_counts_by_reason` — два `DeferReason.LiveJobs`, один `LockTimeout` → `retention.deferred{LiveJobs}=2`, `{LockTimeout}=1` (значения тега — `DeferReason.ToString()`).
  - `ProcessingServiceTests.Maintenance_is_created_for_servers_with_all_flags_off` (замена двух старых утверждений `Assert.Null(service.Maintenance)`) — `ProcessingMaintenance` создан, цикл `stats` только читает.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Условие создания `_servers.Count > 0`; в `Start` цикл `stats` вне `if (RunMaintenance)`; проход — §4.4 «Кто публикует» дословно (захват только при `RunMaintenance`, затем чтение; исключение захвата — чтение, затем `throw`). `RunTaskAsync` замеряет `clock.Now`/`Elapsed`. Исключение `ReadQueueStatsAsync` при `ct.IsCancellationRequested` (отмена остановкой) не считается в `storage.errors`, не логируется и уходит в цикл как отмена — как в `RunTaskAsync` / `LoopAsync`. Ошибка `ReadQueueStatsAsync` без отмены пишется событием 5012 `QueueStatsReadFailed` через `_statsErrors.TryEnter()` (вызов `Log.QueueStatsReadFailed`, а не `logger.LogX`, §5.3). Обновить все 7 мест создания `ProcessingMaintenance` в тестах (`StatsTaskTests.cs:36`, `ResolverLoopTests.cs:24`, `HousekeepingTests.cs:27`, `SchedulerLoopTests.cs:26`, `ProcessingMaintenanceTests.cs:24`, `Integration/RecurringBudgetTests.cs:27`, `Integration/WorkflowResolverLoopTests.cs:17`): `TestTelemetry.Metrics()`, `new ClusterQueueStats(clock, options)`. Цикл `stats` теперь работает и при `RunMaintenance = false`, поэтому `IdleWaits` считает паузы обоих циклов: в `SchedulerLoopTests.Default_owner_without_declarations_starts_no_sync_loop` первое ожидание — `IdleWaits >= 2` (планировщик и `stats` на паузе) до `_time.Advance`, второе — только `Tick >= 2` (условие привязано к планировщику, без `IdleWaits`). Остальные тесты с `RunMaintenance = false` (`Without_maintenance_only_scheduler_runs`, `Declarations_start_sync_loop_even_without_scheduler`, `Resolver_runs_only_with_maintenance`, тесты `ProcessingServiceTests`) `IdleWaits` не считают — проверено.
- [ ] **Step 4:** Run всех перечисленных тестовых классов, а также интеграционных классов с `RunMaintenance = false` на ограниченном пуле или прокси (цикл `stats` добавляет соединение раз в `StatsSnapshotInterval`): `LeaseFailureTests` (:333-344 `MaxPoolSize = 2`, :258-262), `ShutdownTests` (:136-145, прокси с паузой), `MaintenanceTests` (:66), `RecurringTests` (:57, :80) — PASS.
- [ ] **Step 5:** Commit `feat(server): кластерный снимок на каждом поде и метрики обслуживания (часть 7a §4.2, §4.4)`.

---

### Task 13: Перевод логов на `[LoggerMessage]`

**Files:**
- Create / Modify: по Приложению A.1 — вложенные `Log`-классы (соглашение Приложения A) для `PgBoltProcessingService`, `PgBoltServer`, `QueueSettingsCache`, `Dispatcher`, `RetryDecider`, `JobRunner` (существующее предупреждение `TimeoutOf`), `CompletionSink`, `LeaseKeeper`, `OutcomeStatsFlusher`, `ProcessingMaintenance`, `RetentionReport`, `PgBoltInitializer`, `RecurringSyncLog`, `PgBoltStore.RecurringSync`, `DashboardStartupCheck`, `ActionEndpoints` / `EnqueueEndpoints`, `DashboardErrorFilter`
- Create: `tests/E2E/PgBolt.E2E.Tests/EventIdContractTests.cs` (единственный тестовый проект, уже ссылающийся на `PgBolt.Postgres`, `PgBolt.Server` и `PgBolt.Dashboard`; БД тесту не нужна)

**Interfaces:**
- Consumes: события Task 2 (8001, 8002), Task 9 (3001–3008), Task 10 (`SlotReleaseFailed`), Task 12 (5012 `QueueStatsReadFailed`) — уже в нужных классах.
- Produces: таблица Приложения A реализована дословно (`Id`, `Name`, уровень, шаблон).

- [ ] **Step 1: тест** `EventIdContractTests.EventIds_are_unique_explicit_and_in_range` — сборки `PgBolt.Postgres`, `PgBolt.Server`, `PgBolt.Dashboard` (через `typeof` их типов); перебрать все методы, включая непубличные статические вложенных классов, методы с `LoggerMessageAttribute` → каждый `EventId > 0`, `LoggerMessageAttribute.EventName` не пуст и равен имени метода, пары `(Id)` и `(EventName)` уникальны по всем сборкам, `Id` в диапазоне компонента (таблица диапазонов §5.3 по полному имени объявляющего класса); множество `(Id, Name, Level)` равно Приложению A (зашито в тест как массив; 5107 с уровнем во время выполнения — без `Level` в атрибуте, поэтому ожидается `LogLevel.None`).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Перевести каждый вызов из Приложения A; шаблоны — дословно; вызовы с уровнем во время выполнения (`RetentionReport` :71) — `[LoggerMessage(EventId = …, EventName = nameof(X), Message = …)] static partial void X(ILogger logger, LogLevel level, …)`. Динамические шаблоны — по примечаниям Приложения A.
- [ ] **Step 4:** `dotnet test PgBolt.slnx` — PASS (существующие проверки подстрок `Message` не меняются).
- [ ] **Step 5:** Commit `refactor(logging): [LoggerMessage] со стабильными EventId (часть 7a §5.3)`.

---

### Task 14: Пакет `PgBolt.OpenTelemetry`

**Files:**
- Create: `src/PgBolt.OpenTelemetry/PgBolt.OpenTelemetry.csproj`, `PgBoltOpenTelemetryExtensions.cs`; `tests/PgBolt.OpenTelemetry.Tests/PgBolt.OpenTelemetry.Tests.csproj`, `InstrumentationTests.cs`
- Modify: `PgBolt.slnx`

**Interfaces:**
- Consumes: `PgBoltDiagnostics` (Task 2).
- Produces: §6.4 дословно; csproj: `IsAotCompatible=true`, `PackageId=PgBolt.OpenTelemetry`, `PackageReference OpenTelemetry`, `ProjectReference ..\PgBolt.Abstractions` (без Postgres / Server).

- [ ] **Step 1: тесты** (`OpenTelemetry.Exporter.InMemory`, `Sdk.CreateTracerProviderBuilder()` / `Sdk.CreateMeterProviderBuilder()`):
  - `Tracer_extension_subscribes_PgBolt_source` — `new ActivitySource(PgBoltDiagnostics.ActivitySourceName).StartActivity("x")` → в экспортёре.
  - `Meter_extension_applies_spec_buckets` — тестовый `Meter(PgBoltDiagnostics.MeterName)` создаёт гистограммы `JobDuration`, `JobWaitTime`, `MaintenanceDuration` **без** advice, `Record(1)`, `ForceFlush` → у каждой метрики `HistogramBuckets` с границами §4.5.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Реализация §6.4.
- [ ] **Step 4:** Run — PASS; `dotnet pack src/PgBolt.OpenTelemetry -c Release` — пакет собирается.
- [ ] **Step 5:** Commit `feat(otel): пакет PgBolt.OpenTelemetry (часть 7a §6.4)`.

---

### Task 15: E2E — сквозной трейс и агрегация по подам

**Files:**
- Create: `tests/E2E/PgBolt.E2E.Tests/ObservabilityEndToEndTests.cs`
- Modify: `tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj` (+ `Microsoft.Extensions.Diagnostics.Testing`), `tests/E2E/PgBolt.E2E.Tests/LocalJob.cs` (новая локальная задача `[Job("e2e.trace", Queue = "e2e")] TraceArgs(string Run)` + `TraceJob`, `[JsonSerializable(typeof(TraceArgs))]` в `E2EJson`; `PingJob` общий для других E2E и не меняется)

**Interfaces:**
- Consumes: всё выше; хосты — как `StartServerAsync` в `EndToEndTests.cs` (`Host.CreateEmptyApplicationBuilder` + `AddMetrics()` + `AddPgBolt` + `AddPgBoltServer`).

- [ ] **Step 1: тесты** (`[Collection("e2e-limited")]` не нужен; свой schema через `TestDb`; класс — в `[Collection(TelemetryE2ECollection.Name)]`, где `[CollectionDefinition(Name, DisableParallelization = true)] public sealed class TelemetryE2ECollection { public const string Name = "e2e-telemetry"; }` объявлен в том же файле; span'ы фильтруются по `TraceId` корневой `Activity`):
  - `Trace_flows_from_enqueue_to_job_code` — `ActivityListener` на `PgBolt` и `"E2E"`; корневая `Activity` → `EnqueueAsync<TraceArgs>` на хосте-клиенте → `TraceJob` на хосте-сервере стартует `Activity` из `ActivitySource("E2E")` (`StartActivity("e2e.work")`) → все четыре span'а с одним `TraceId`, цепочка по фактическим именам `root → enqueue e2e.trace (producer) → e2e.trace (consumer) → e2e.work`.
  - `Cluster_metrics_agree_across_two_hosts` — два хоста с серверами, `RunMaintenance = true` у первого, `false` у второго, `StatsSnapshotInterval = 200 ms`; очереди `q` задан `ClusterMaxConcurrency` (например, 3), чтобы `cluster.queue.limit` не был пуст; `MetricCollector` с `meterScope` = `IMeterFactory` каждого хоста; поставить 5 отложенных задач в `q` (`EnqueueOptions.Delay = 1 h` — promoter первого хоста их не переведёт) и одну готовую задачу в очередь `unserved` (через `EnqueueOptions.Queue`), которую не обслуживает ни один хост (строка снимка без сервера публикуется, §4.3), — иначе `oldest_ready_age` пуст на всех хостах и сравнение его не проверяет; дождаться (`Eventually`) прохода цикла `stats` каждого хоста → `cluster.queue.jobs{q, scheduled} = 5` и `cluster.queue.limit{q}` = заданному лимиту на обоих; остановить первый хост и запустить третий с `RunMaintenance = true` (новый владелец снимка) → после прохода цикла `stats` второго и третьего хостов их значения `jobs`, `oldest_ready_age{unserved}` (присутствует на обоих хостах; сравнивается с допуском в несколько `StatsSnapshotInterval`, так как возраст растёт) и `limit` совпадают; `worker.capacity` хостов складывается в сумму `Capacity`.
  - `Idle_cluster_publishes_zeros` — пустая `queue`, та же пара хостов (первый `RunMaintenance = true`, второй `false`, `StatsSnapshotInterval = 200 ms`); дождаться (`Eventually`) прохода цикла `stats` каждого хоста (иначе снимок устареет и нулей не будет) → оба хоста публикуют четыре нулевых состояния для своих очередей.
- [ ] **Step 2:** Run `ObservabilityEndToEndTests` — PASS (Tasks 1–14 уже сделаны, проводка есть; если падает — дефект предыдущих задач, исправлять там).
- [ ] **Step 3:** Commit `test(e2e): сквозной трейс и кластерные метрики двух хостов (часть 7a)`.

---

### Task 16: Бенчмарк после 7a, документация

**Files:**
- Modify: `benchmarks/PgBolt.Server.Benchmarks/JobRunnerBenchmarks.cs`, `README.md`; Create: `results/JobRunnerBenchmarks-7a-report-github.md`
- Create: `docs/observability.md`
- Modify: `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md` (часть 7 → 7a / 7b, статус 7a «реализовано»), `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md` (§1, §3.6 — пометка о пересмотре со ссылкой на 7a §2), `docs/superpowers/specs/2026-10-16-pgbolt-observability-design.md` (строка «Статус: реализовано, план …»), `CHANGELOG.md`

- [ ] **Step 1:** Бенчмарк: `[Params(None, Activity, Meter, Both)]` слушателей в `GlobalSetup`; запуск, как в Task 1; сравнить `Allocated` варианта `None` с baseline: прирост допустим только на scope логов (объект `JobLogScope`; узел scope при `NullLoggerFactory` не создаётся). Превышение — дефект Task 9, исправлять там.
- [ ] **Step 2:** `docs/observability.md` — содержание §9 полностью: подключение, таблицы spans / атрибутов / метрик, таблица `EventId` (Приложение A, включая новые 8001, 8002, 3001–3008, `SlotReleaseFailed`, 5012 `QueueStatsReadFailed`), агрегация `sum` / `max` и допуск, свежесть и порог 4 ×, одинаковый `StatsSnapshotInterval`, `limit` не устаревает, `service.instance.id`, кардинальность и `CardinalityLimit`, семантика `job.enqueued` и `wait_time`, requeue в исходном трейсе, все PromQL §9.
- [ ] **Step 3:** CHANGELOG — пункты §9 в разделы `## Unreleased` по образцу существующих: Added и Changed — как в §9; в `### Breaking changes` — два пункта: «в `V001` добавлена `queue_stats.oldest_ready_seconds`; схемы прошлых сборок не обновляются — старт падает с просьбой пересоздать схему» и ломающая правка `ActiveQueueSetting` (`bool Limited` → `int? Limit`).
- [ ] **Step 4:** `dotnet test PgBolt.slnx` — PASS.
- [ ] **Step 5:** Commit `docs: observability, roadmap 7a, CHANGELOG; бенчмарк JobRunner после 7a`.

---

## Приложение A. Контракт `EventId`

Соглашение (действует для всех задач): события компонента — во вложенном `private static partial class Log` внутри компонента (класс компонента помечается `partial`); исключения — `RecurringSyncLog` (сам `internal static partial class`) и `Endpoints/DashboardActionLog.cs` (у `ActionEndpoints` уже есть метод `Log`). Форма: `[LoggerMessage(EventId = N, EventName = nameof(Name), Level = LogLevel.X, Message = "…")] static partial void Name(ILogger logger, [Exception e,] …)`; `EventName` задаётся явно (генератор подставляет имя метода только в сгенерированный `EventId`, а не в атрибут, и `EventIdContractTests` читает `LoggerMessageAttribute.EventName`); для уровня во время выполнения — без `Level`, с параметром `LogLevel level`.

### A.0 Новые события 7a — окончательные шаблоны

Строки «per §5.1 / per §8» в таблицах ниже заменяются этими значениями.

| Id | Name | Класс | Level | Exc | Template |
|---|---|---|---|---|---|
| 1112 | SlotReleaseFailed | `PgBoltServer.Log` | Error | да | `PgBolt server {Server}: slot of queue {Queue} was not returned after job {JobId} finished.` |
| 3001 | JobStarted | `JobRunner.Log` (категория `PgBolt.Job`) | Debug | нет | `PgBolt: job {JobId} ({JobType}) attempt {Attempt} started on queue {Queue}.` |
| 3002 | JobSucceeded | `JobRunner.Log` | Debug | нет | `PgBolt: job {JobId} succeeded in {DurationMs} ms.` |
| 3003 | JobFailedWillRetry | `JobRunner.Log` | Warning | да | `PgBolt: job {JobId} attempt {Attempt} of {MaxAttempts} failed ({Reason}); retrying in {NextAttemptIn}.` |
| 3004 | JobFailed | `JobRunner.Log` | Error | да | `PgBolt: job {JobId} failed at attempt {Attempt} of {MaxAttempts} ({Reason}); no attempts left.` |
| 3005 | JobNoExecutorWillRetry | `JobRunner.Log` | Warning | да | `PgBolt: no executor is registered for job {JobId} of type {JobType}; retrying in {NextAttemptIn}.` |
| 3006 | JobNoExecutorFailed | `JobRunner.Log` | Error | да | `PgBolt: no executor is registered for job {JobId} of type {JobType}; the job is failed.` |
| 3007 | JobCancelled | `JobRunner.Log` | Information | да | `PgBolt: job {JobId} was cancelled.` |
| 3008 | JobReleased | `JobRunner.Log` | Debug | нет | `PgBolt: job {JobId} was released without spending an attempt (ran: {Ran}).` |
| 5012 | QueueStatsReadFailed | `ProcessingMaintenance.Log` | Warning | да | `PgBolt: reading queue statistics failed; the previous snapshot is kept.` |
| 8001 | TelemetryFailed | `PgBoltInstrumentation.Log` (категория `PgBolt.Telemetry`) | Warning | да | `PgBolt: telemetry failed; the signal is skipped.` |
| 8002 | MetricCallbackFailed | `PgBoltInstrumentation.Log` | Warning | да | `PgBolt: metric callback failed; the measurement is skipped.` |

`Reason` — `Execution.FirstReason` (`None`, `Timeout`, `HeartbeatLost`, `LeaseLost`, `Shutdown`); `NextAttemptIn` — `TimeSpan` задержки `Retry`; `DurationMs` — `long`. `JobCancelled` — исключение попытки, если оно есть (`null` допустим).

### A.1 Инвентаризация существующих вызовов


Source: spec `docs/superpowers/specs/2026-10-16-pgbolt-observability-design.md` §5.3. Scan of `src/**/*.cs` (obj/bin excluded) on branch `feature/observability` @ `ba53a8b`.

### Conventions

- **Numbering.** Each component sub-range starts at its base (1000, 1100, 2000, 3100, 4000, ...) and is numbered in source order. Exceptions: job lifecycle keeps 3001–3008 for the new §5.1 events, and telemetry keeps 8001/8002.
- **Log class.** The default is a nested `private static partial class Log` inside the component, with the component class marked `partial`. Every component in scope is our own non-generic class, so all of them can be made partial. Adding `partial` to the public `PgBoltInitializer` does not break anything. There are two exceptions:
  - `RecurringSyncLog` is already a log-helper class, so it becomes `internal static partial class RecurringSyncLog` and hosts its generated methods directly, with no nested class.
  - `ActionEndpoints` already has a method named `Log`, so a nested type `Log` would fail with CS0102. Its event goes to a new file `Endpoints/DashboardActionLog.cs` as `internal static partial class DashboardActionLog`.
- **Generated method shape.** The generated method is `[LoggerMessage(EventId = N, EventName = nameof(Name), Level = LogLevel.X, Message = "...")] static partial void Name(ILogger logger, [Exception e,] args...);`. `EventName` is always explicit: the generator puts the method name only into the generated `EventId`, not into the attribute. For a runtime level, `Level` is left out of the attribute and `LogLevel level` is added as a method parameter.
- **Templates stay verbatim.** Format specifiers like `{From:O}` and templates with more than 6 placeholders (5102) are supported by the generator. Parameter names must match the placeholders, ignoring case.

---

### 1000–1999 Server and process lifecycle

#### PgBoltProcessingService: 1000–1099
Log class: nested `private static partial class Log` in `internal sealed partial class PgBoltProcessingService` (src/PgBolt.Server/Runtime/PgBoltProcessingService.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 1000 | JobTypeSchemaMismatch | Warning | no | PgBolt.Server/Runtime/PgBoltProcessingService.cs:73 | `PgBolt: job types {JobTypes} were published by another process with a different argument schema.` |
| 1001 | ProcessingStartFailed | Critical | yes | PgBolt.Server/Runtime/PgBoltProcessingService.cs:123 | `PgBolt: starting processing failed; stopping what was started.` |
| 1002 | HostStopTimeoutServers | Warning | no | PgBolt.Server/Runtime/PgBoltProcessingService.cs:137 | `PgBolt: host stop timeout reached; pending outcomes are dropped and servers rows are left, leases will expire.` |
| 1003 | HostStopTimeoutMaintenance | Warning | no | PgBolt.Server/Runtime/PgBoltProcessingService.cs:151 | `PgBolt: host stop timeout reached while stopping maintenance loops.` |
| 1004 | QueueDefaultsConflict | Warning | no | PgBolt.Server/Runtime/PgBoltProcessingService.cs:169 | `PgBolt: queue '{Queue}' defaults were recently published by {PublishedBy} with other values; overwritten: {Changes}.` |
| 1005 | HostShutdownTimeoutTooShort | Warning | no | PgBolt.Server/Runtime/PgBoltProcessingService.cs:182 | `PgBolt: HostOptions.ShutdownTimeout {HostTimeout} is shorter than {Required} (max server ShutdownTimeout + AckFlushInterval + 2 s); executions may be abandoned and their attempts counted.` |
| 1006 | PartitionDaysAheadTooShort | Warning | no | PgBolt.Server/Runtime/PgBoltProcessingService.cs:193 | `PgBolt: PartitionDaysAhead ({DaysAhead} days) is shorter than 2 × PartitionsInterval ({Interval}); a single failed iteration may leave no partition for the next day.` |
| 1007 | DeadServerTimeoutTooShort | Warning | no | PgBolt.Server/Runtime/PgBoltProcessingService.cs:198 | `PgBolt: DeadServerTimeout ({Timeout}) is shorter than 3 × HeartbeatInterval ({Heartbeat}); rows of live servers will be removed on short heartbeat hiccups and restored by the next heartbeat.` |

#### PgBoltServer: 1100–1199
Log class: nested `private static partial class Log` in `internal sealed partial class PgBoltServer` (src/PgBolt.Server/Runtime/PgBoltServer.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 1100 | ServerStarted | Information | no | PgBolt.Server/Runtime/PgBoltServer.cs:73 | `PgBolt server {Server} ({ServerId}) started: queues {Queues}, capacity {Capacity}.` |
| 1101 | ExecutionsWaitCancelled | Warning | no | PgBolt.Server/Runtime/PgBoltServer.cs:104 | `PgBolt server {Server}: waiting for executions was cancelled by the host.` |
| 1102 | ServerStopCancelled | Warning | no | PgBolt.Server/Runtime/PgBoltServer.cs:119 | `PgBolt server {Server}: stop was cancelled by the host; the servers row is left for cleanup.` |
| 1103 | ServerRowRemoveFailed | Error | yes | PgBolt.Server/Runtime/PgBoltServer.cs:128 | `PgBolt server {Server}: failed to remove the servers row.` |
| 1104 | ServerStopped | Information | no | PgBolt.Server/Runtime/PgBoltServer.cs:130 | `PgBolt server {Server} ({ServerId}) stopped.` |
| 1105 | DispatcherFailed | Critical | yes | PgBolt.Server/Runtime/PgBoltServer.cs:142 | `PgBolt server {Server}: dispatcher failed; stopping continues.` |
| 1106 | CancellationCallbackFailed | Warning | yes | PgBolt.Server/Runtime/PgBoltServer.cs:154 | `PgBolt server {Server}: a cancellation callback of a job threw.` |
| 1107 | JobTypesWithoutExecutor | Warning | no | PgBolt.Server/Runtime/PgBoltServer.cs:166 | `PgBolt server {Server}: job types {JobTypes} default to its queues but have no executor in this process; such jobs go to NoExecutor.` |
| 1108 | MissingExecutorsCheckFailed | Warning | yes | PgBolt.Server/Runtime/PgBoltServer.cs:172 | `PgBolt server {Server}: failed to check job types without executors.` |
| 1109 | JobFinishFailed | Critical | yes | PgBolt.Server/Runtime/PgBoltServer.cs:201 | `PgBolt server {Server}: unexpected failure while finishing job {JobId}.` |
| 1110 | JobAbandoned | Warning | no | PgBolt.Server/Runtime/PgBoltServer.cs:218 | `PgBolt server {Server}: job {JobId} did not stop within {ShutdownTimeout} and is abandoned; its lease will expire and the sweeper will count the attempt.` |
| 1111 | JobIgnoresCancellation | Warning | no | PgBolt.Server/Runtime/PgBoltServer.cs:234 | `PgBolt server {Server}: job {JobId} has ignored cancellation ({Reason}) for {Elapsed}; its slot stays occupied.` |
| 1112 | SlotReleaseFailed **(NEW)** | Error | yes | PgBolt.Server/Runtime/PgBoltServer.cs `RunExecutionAsync` (new catch around `_slots.Release` + `_dispatcher.OnSlotFreed`, spec §8 item 2) | `PgBolt server {Server}: slot of queue {Queue} was not returned after job {JobId} finished.` |

#### QueueSettingsCache: 1200–1299
Log class: nested `private static partial class Log` in `internal sealed partial class QueueSettingsCache` (src/PgBolt.Server/Runtime/QueueSettingsCache.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 1200 | QueueSettingsRefreshFailed | Warning | yes | PgBolt.Server/Runtime/QueueSettingsCache.cs:88 | `PgBolt: refreshing queue settings failed; using the previous snapshot.` |

---

### 2000–2999 Dispatcher
Log class: nested `private static partial class Log` in `internal sealed partial class Dispatcher` (src/PgBolt.Server/Runtime/Dispatcher.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 2000 | DequeueFailed | Error | yes | PgBolt.Server/Runtime/Dispatcher.cs:133 | `PgBolt: dequeue from queue {Queue} failed; fetching paused for {Pause}.` |
| 2001 | DequeueTimedOutJobsReleased | Warning | no | PgBolt.Server/Runtime/Dispatcher.cs:157 | `PgBolt: dequeue from queue {Queue} returned {Count} jobs after {Elapsed}, past its timeout {Timeout}; they are released without running.` |
| 2002 | DispatcherIterationFailed | Critical | yes | PgBolt.Server/Runtime/Dispatcher.cs:195 | `PgBolt: dispatcher iteration failed unexpectedly; fetching paused for {Pause}.` |
| 2003 | WakeupSubscriptionDisposeFailed | Warning | yes | PgBolt.Server/Runtime/Dispatcher.cs:209 | `PgBolt: disposing a wakeup subscription failed.` |
| 2004 | WakeupSubscriptionFailed | Error | yes | PgBolt.Server/Runtime/Dispatcher.cs:231 | `PgBolt: wakeup subscription for queue {Queue} failed; the queue is polled without wakeup signals.` |
| 2005 | StartRollbackSlotReleaseFailed | Error | yes (`releaseError`) | PgBolt.Server/Runtime/Dispatcher.cs:266 | `PgBolt: slot of queue {Queue} was not returned after the failed start of job {JobId}.` |
| 2006 | JobStartRolledBack | Warning | no | PgBolt.Server/Runtime/Dispatcher.cs:270 | `PgBolt: job {JobId} of queue {Queue} was not started; its registration is rolled back, the lease will expire.` |

---

### 3000–3999 Job lifecycle, RetryDecider, JobRunner

Reserved NEW events (spec §5.1) are hosted in the nested `Log` of `internal sealed partial class JobRunner` (src/PgBolt.Server/Runtime/JobRunner.cs). Their templates and levels are defined by spec §5.1, not taken from existing code.

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 3001 | JobStarted **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |
| 3002 | JobSucceeded **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |
| 3003 | JobFailedWillRetry **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |
| 3004 | JobFailed **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |
| 3005 | JobNoExecutorWillRetry **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |
| 3006 | JobNoExecutorFailed **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |
| 3007 | JobCancelled **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |
| 3008 | JobReleased **(NEW)** | per §5.1 | per §5.1 | JobRunner.cs (new) | per §5.1 |

Existing events (3100+), in the component order RetryDecider, then JobRunner:

| Id | Name | Level | Exc | file:line | Template | Log class |
|---|---|---|---|---|---|---|
| 3100 | RetryPolicyFailed | Error | yes | PgBolt.Server/Runtime/RetryDecider.cs:30 | `PgBolt: RetryPolicy threw for attempt {Attempt}; the job is failed without retry.` | nested `Log` in `internal sealed partial class RetryDecider` |
| 3101 | JobTimeoutCapped | Warning | no | PgBolt.Server/Runtime/JobRunner.cs:78 | `PgBolt: job '{JobName}' has TimeoutSeconds = {TimeoutSeconds}, above the .NET timer limit; its timeout is capped at {MaxJobTimeout}.` | nested `Log` in `internal sealed partial class JobRunner` |

---

### 4000–4999 CompletionSink, LeaseKeeper, OutcomeStatsFlusher

#### CompletionSink: 4000–4099
Log class: nested `private static partial class Log` in `internal sealed partial class CompletionSink` (src/PgBolt.Server/Runtime/CompletionSink.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 4000 | OutcomeDiscardedSinkStopped | Warning | no | PgBolt.Server/Runtime/CompletionSink.cs:71 **and** :95 (shared event, same template and level; the `{Outcome}` argument is `string?` because :95 passes `execution.Outcome?.GetType().Name`) | `PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink is stopped.` |
| 4001 | OutcomeDiscardedNotRunning | Warning | no | PgBolt.Server/Runtime/CompletionSink.cs:77 | `PgBolt: outcome {Outcome} of job {JobId} discarded: execution is {State} (abandoned or lease lost).` |
| 4002 | SinkStopCancelled | Warning | no | PgBolt.Server/Runtime/CompletionSink.cs:128 | `PgBolt: completion sink stop was cancelled; {Count} outcome send(s) aborted.` |
| 4003 | AckDiscardedStopCancelled | Warning | no | PgBolt.Server/Runtime/CompletionSink.cs:181 | `PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink stop was cancelled; its lease will expire.` |
| 4004 | AckPartialAfterRetry | Information | no | PgBolt.Server/Runtime/CompletionSink.cs:219 | `PgBolt: ack affected {Acked} of {Count} jobs after a failed attempt; the rest was probably already applied.` |
| 4005 | AckPartialLeaseLost | Warning | no | PgBolt.Server/Runtime/CompletionSink.cs:222 | `PgBolt: ack affected {Acked} of {Count} jobs; lease of the rest was lost.` |
| 4006 | OutcomeLeaseNotFoundAfterRetry | Information | no | PgBolt.Server/Runtime/CompletionSink.cs:255 | `PgBolt: outcome {Outcome} of job {JobId}: lease not found after a failed attempt; the outcome was probably already applied.` |
| 4007 | OutcomeDiscardedLeaseLost | Warning | no | PgBolt.Server/Runtime/CompletionSink.cs:258 | `PgBolt: outcome {Outcome} of job {JobId} discarded: lease was lost.` |
| 4008 | OutcomeNotSentExecutionState | Debug | no | PgBolt.Server/Runtime/CompletionSink.cs:328 | `PgBolt: outcome of job {JobId} is not sent: execution is {State}.` |
| 4009 | LeaseFreedNotificationFailed | Error | yes | PgBolt.Server/Runtime/CompletionSink.cs:361 | `PgBolt: lease-freed notification for queue {Queue} failed.` |
| 4010 | OutcomeSendFailedRetrying | Warning | yes (`error`) | PgBolt.Server/Runtime/CompletionSink.cs:386 | `PgBolt: sending outcome of {Count} job(s) failed; retrying in {Delay}.` |
| 4011 | OutcomeNotSentStopCancelled | Warning | yes (`error`) | PgBolt.Server/Runtime/CompletionSink.cs:408 | `PgBolt: outcome of job {JobId} was not sent before stop was cancelled; its lease will expire.` |
| 4012 | OutcomeSendBudgetExceeded | Error | yes (`error`) | PgBolt.Server/Runtime/CompletionSink.cs:411 | `PgBolt: outcome of job {JobId} was not sent within {Budget}; the execution is dropped, its lease will expire.` |

#### LeaseKeeper: 4100–4199
Log class: nested `private static partial class Log` in `internal sealed partial class LeaseKeeper` (src/PgBolt.Server/Runtime/LeaseKeeper.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 4100 | LeaseRenewalNotConfirmed | Warning | no | PgBolt.Server/Runtime/LeaseKeeper.cs:137 | `PgBolt: lease renewal of job {JobId} (lease {LeaseId}) is not confirmed; cancelling the execution.` |
| 4101 | HeartbeatTimedOut | Warning | yes | PgBolt.Server/Runtime/LeaseKeeper.cs:189 | `PgBolt: heartbeat of server {ServerId} timed out after {Timeout}; {Count} leases not renewed.` |
| 4102 | HeartbeatFailed | Warning | yes | PgBolt.Server/Runtime/LeaseKeeper.cs:195 | `PgBolt: heartbeat of server {ServerId} failed; {Count} leases not renewed.` |
| 4103 | JobCancellationRequested | Information | no | PgBolt.Server/Runtime/LeaseKeeper.cs:215 | `PgBolt: cancellation of job {JobId} requested; cancelling the execution.` |
| 4104 | LeaseLostExecutionCancelled | Warning | no | PgBolt.Server/Runtime/LeaseKeeper.cs:227 | `PgBolt: lease {LeaseId} of job {JobId} is lost; the execution is cancelled and its outcome will be dropped.` |
| 4105 | LeaseLostOutcomeDropped | Warning | no | PgBolt.Server/Runtime/LeaseKeeper.cs:233 | `PgBolt: lease {LeaseId} of job {JobId} is lost; pending outcome {Outcome} is dropped.` |
| 4106 | HeartbeatRecovered | Information | no | PgBolt.Server/Runtime/LeaseKeeper.cs:243 | `PgBolt: heartbeat of server {ServerId} recovered.` |
| 4107 | HeartbeatRecoveryCallbackFailed | Error | yes | PgBolt.Server/Runtime/LeaseKeeper.cs:250 | `PgBolt: heartbeat recovery callback of server {ServerId} failed.` |
| 4108 | ServerReRegistered | Warning | no | PgBolt.Server/Runtime/LeaseKeeper.cs:270 | `PgBolt: servers row of server {ServerId} was missing (removed by cleanup); registered again.` |
| 4109 | ServerReRegistrationFailed | Warning | yes | PgBolt.Server/Runtime/LeaseKeeper.cs:278 | `PgBolt: re-registering server {ServerId} failed; retrying on the next heartbeat.` |
| 4110 | LeaseDeadlineCheckFailed | Error | yes | PgBolt.Server/Runtime/LeaseKeeper.cs:351 | `PgBolt: lease deadline check of server {ServerId} failed.` |

#### OutcomeStatsFlusher: 4200–4299
Log class: nested `private static partial class Log` in `internal sealed partial class OutcomeStatsFlusher` (src/PgBolt.Server/Runtime/OutcomeStatsFlusher.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 4200 | OutcomeStatsFlushFailed | Warning | yes | PgBolt.Server/Runtime/OutcomeStatsFlusher.cs:46 | `PgBolt: flushing outcome statistics failed; values are kept for the next flush.` |

---

### 5000–5999 Maintenance and retention

#### ProcessingMaintenance: 5000–5099
Log class: nested `private static partial class Log` in `internal sealed partial class ProcessingMaintenance` (src/PgBolt.Server/Runtime/ProcessingMaintenance.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 5000 | BatchCompletionNotCounted | Debug | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:123 | `PgBolt: completion of a member of batch {BatchId} is not counted: the batch row was removed by retention.` |
| 5001 | ResolverLagging | Warning | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:163 | `PgBolt: the oldest workflow completion signal is {Age} old (ResolverInterval {Interval}); continuations are delayed. Check that a process with RunMaintenance = true is running and that no long user transaction holds workflow rows (After).` (source: two literals joined with `+`, see Notes) |
| 5002 | RecurringFireFailed | Error | no (`{Error}` is a string) | PgBolt.Server/Runtime/ProcessingMaintenance.cs:198 | `PgBolt: recurring job {Id} was not fired: {Error}; retrying in {Delay}.` |
| 5003 | RecurringMisfireSkipped | Warning | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:201 | `PgBolt: recurring job {Id} skipped fires from {From:O} to {To:O} by its misfire policy.` |
| 5004 | RecurringOverlapSkipped | Debug | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:204 | `PgBolt: recurring job {Id} fire skipped: previous run is still active.` |
| 5005 | RecurringWaitingForPrevious | Debug | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:206 | `PgBolt: recurring job {Id} waits for its previous run (catch-up).` |
| 5006 | RecurringExhausted | Warning | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:208 | `PgBolt: recurring job {Id} has no future occurrences and will not fire again.` |
| 5007 | MaintenanceTaskFailed | Error | yes | PgBolt.Server/Runtime/ProcessingMaintenance.cs:266 | `PgBolt: maintenance task {Task} failed.` |
| 5008 | FuturePartitionsLockTimeout | Warning | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:279 | `PgBolt: future partitions were not created (lock_timeout); retrying in {Delay}.` |
| 5009 | TodayPartitionsMissing | Error | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:281 | `PgBolt: today's partitions are missing and could not be created (lock_timeout); enqueue fails until they exist. Retrying in {Delay}.` |
| 5010 | DeadServersRemoved | Information | no | PgBolt.Server/Runtime/ProcessingMaintenance.cs:291 | `PgBolt: removed {Count} servers rows without heartbeat for {Timeout}.` |
| 5011 | MaintenanceLoopIterationFailed | Error | yes | PgBolt.Server/Runtime/ProcessingMaintenance.cs:322 | `PgBolt: {Loop} iteration failed; retrying after the next interval.` |
| 5012 | QueueStatsReadFailed **(NEW)** | Warning | yes | PgBolt.Server/Runtime/ProcessingMaintenance.cs, цикл `stats` (new catch around `ReadQueueStatsAsync`, spec §4.4, gated by `_statsErrors`) | `PgBolt: reading queue statistics failed; the previous snapshot is kept.` |

#### RetentionReport: 5100–5199
Log class: nested `private static partial class Log` in `internal static partial class RetentionReport` (src/PgBolt.Server/Runtime/RetentionReport.cs). The `ILogger` comes from `ProcessingMaintenance` (`RetentionReport.Write(logger, ...)` at ProcessingMaintenance.cs:298) and from tests (`tests/PgBolt.Server.Tests/Unit/RetentionReportTests.cs:20,142`).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 5100 | RetentionPartitionsDropped | Information | no | PgBolt.Server/Runtime/RetentionReport.cs:21 | `PgBolt retention: dropped {JobsCount} jobs partitions [{JobsDays}], {EventsCount} job_events partitions [{EventsDays}] and {Remnants} compaction remnants.` |
| 5101 | RetentionBatchesDeleted | Information | no | PgBolt.Server/Runtime/RetentionReport.cs:27 | `PgBolt retention: deleted {Count} finished batches rows older than JobsRetention.` |
| 5102 | RetentionPinnedPartitions | Information | no | PgBolt.Server/Runtime/RetentionReport.cs:37 | `PgBolt retention: {Count} jobs partitions are pinned by live jobs: {Compacted} compacted now, {Small} left as is (few terminal rows), {TooMany} not compactable (more than {Max} live jobs), {Replicated} skipped (logical replication); {Live} live jobs in total, oldest day {Oldest}.` (8 placeholders: fine for the source generator) |
| 5103 | RetentionPartitionTooManyLiveJobs | Warning | no | PgBolt.Server/Runtime/RetentionReport.cs:44 | `PgBolt retention: jobs partition {Day} holds {Live} live jobs, more than RetentionCompactionMaxLiveJobs ({Max}); it is not compacted and keeps its whole disk space until they finish (overdue by {Overdue} days). Run, cancel or delete these jobs, or raise the limit.` |
| 5104 | RetentionCompactionSkippedReplication | Warning | no | PgBolt.Server/Runtime/RetentionReport.cs:50 | `PgBolt retention: compaction of {Count} pinned jobs partitions is skipped because jobs is in a logical replication publication; they keep their disk space until their live jobs finish (oldest day {Oldest}).` |
| 5105 | RetentionTooManyAttachedPartitions | Warning | no | PgBolt.Server/Runtime/RetentionReport.cs:58 | `PgBolt retention: {Attached} jobs partitions are attached, normally {Normal}; days pinned by live jobs stay attached and slow down dequeue planning.` |
| 5106 | RetentionHoldTimeout | Warning | no | PgBolt.Server/Runtime/RetentionReport.cs:64 | `PgBolt retention: holding the lock on {Table} hit statement_timeout or idle_in_transaction_session_timeout ({Count} partitions deferred); the hot path waited up to the hold limit. Likely a slow disk or a process pause (GC).` |
| 5107 | RetentionDeferredByLockTimeout | **runtime** (`overdue >= 1 ? Warning : Debug`) | no | PgBolt.Server/Runtime/RetentionReport.cs:71 | `PgBolt retention: {Count} {Table} partitions were deferred by lock_timeout (overdue by up to {Overdue} days); long queries or a holder of the partition itself (autovacuum, direct query) block their removal.` |
| 5108 | RetentionRemnantDropFailed | Error | no (`{Error}` is a string) | PgBolt.Server/Runtime/RetentionReport.cs:77 | `PgBolt retention: dropping the detached {Table} table of {Day} failed: {Error}; it will be retried as a remnant.` |

Generated signature for 5107:
`[LoggerMessage(EventId = 5107, EventName = nameof(RetentionDeferredByLockTimeout), Message = "PgBolt retention: {Count} {Table} partitions were deferred ...")] public static partial void RetentionDeferredByLockTimeout(ILogger logger, LogLevel level, int count, string table, int overdue);`
The call site passes `overdue >= 1 ? LogLevel.Warning : LogLevel.Debug`.

---

### 6000–6999 Initializer and recurring sync

#### PgBoltInitializer: 6000–6099
Log class: nested `private static partial class Log` in `public sealed partial class PgBoltInitializer` (src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs). Adding `partial` to the public type does not change its API.

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 6000 | SchemaVersionNewer | Warning | no | PgBolt.Postgres/Hosting/PgBoltInitializer.cs:98 | `PgBolt: schema '{Schema}' has version {Version}, newer than this build knows ({Latest}); continuing because migrations are additive.` |
| 6001 | StartupFuturePartitionsLockTimeout | Warning | no | PgBolt.Postgres/Hosting/PgBoltInitializer.cs:118 | `PgBolt: future partitions were not created (lock_timeout); retrying in background.` |
| 6002 | BackgroundPartitionCreationFailed | Error | yes | PgBolt.Postgres/Hosting/PgBoltInitializer.cs:173 | `PgBolt: background partition creation failed.` |

#### RecurringSyncLog and PgBoltStore.RecurringSync: 6100–6199
Log class: `internal static partial class RecurringSyncLog` (existing file src/PgBolt.Postgres/Recurring/RecurringSyncLog.cs, made partial) hosts the generated methods directly. It already is the recurring-sync log class, so it needs no nested `Log`. Event 6104 is also hosted here as an `internal` method and called from `PgBoltStore.RetrySyncDeadlockAsync`. This keeps the whole 6100 sub-range in one class. The alternative would be a nested `Log` in the already-partial `PgBoltStore`.

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 6100 | RecurringJobInserted | Information | no | PgBolt.Postgres/Recurring/RecurringSyncLog.cs:12 | `PgBolt: recurring job {Id} of owner {Owner} inserted.` |
| 6101 | RecurringJobUpdated | Information | no | PgBolt.Postgres/Recurring/RecurringSyncLog.cs:14 | `PgBolt: recurring job {Id} of owner {Owner} updated ({Fields}).` |
| 6102 | RecurringJobDeleted | Information | no | PgBolt.Postgres/Recurring/RecurringSyncLog.cs:16 | `PgBolt: recurring job {Id} of owner {Owner} deleted (no longer declared).` |
| 6103 | RecurringJobOwnerCollision | Error | no | PgBolt.Postgres/Recurring/RecurringSyncLog.cs:18 | `PgBolt: recurring job {Id} declared by owner {Owner} belongs to another owner or was created through the API; left untouched.` |
| 6104 | RecurringSyncDeadlockRetry | Warning | yes | PgBolt.Postgres/Storage/PgBoltStore.RecurringSync.cs:34 | `PgBolt: recurring sync of owner {Owner} hit a deadlock (40P01); retrying the transaction (attempt {Attempt} of {MaxAttempts}).` |

---

### 7000–7999 Dashboard

#### DashboardStartupCheck: 7000–7099
Log class: nested `private static partial class Log` in `internal static partial class DashboardStartupCheck` (src/PgBolt.Dashboard/Security/DashboardStartupCheck.cs).

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 7000 | ActionsPolicyNotRegistered | Error | no | PgBolt.Dashboard/Security/DashboardStartupCheck.cs:31 | `PgBolt dashboard: ActionsPolicy '{Policy}' is not registered: every dashboard request fails until AddAuthorization defines it.` |
| 7001 | AnonymousRemoteAccessAllowed | Warning | no | PgBolt.Dashboard/Security/DashboardStartupCheck.cs:37 | `PgBolt dashboard allows anonymous remote access (AllowAnonymousRemote = true).` |
| 7002 | LocalRequestsOnly | Warning | no | PgBolt.Dashboard/Security/DashboardStartupCheck.cs:39 | `PgBolt dashboard is reachable only from local requests: add RequireAuthorization(...) to the dashboard group or set AllowAnonymousRemote.` |

#### Endpoints: 7100–7199

| Id | Name | Level | Exc | file:line | Template | Log class |
|---|---|---|---|---|---|---|
| 7100 | DashboardActionPerformed | Information | no | PgBolt.Dashboard/Endpoints/ActionEndpoints.cs:304 (inside the helper `ActionEndpoints.Log`; 9 call sites below) | `PgBolt dashboard: {User} {Action} {Target}: {Result}.` | **new file** `src/PgBolt.Dashboard/Endpoints/DashboardActionLog.cs`: `internal static partial class DashboardActionLog`. A nested `Log` in `ActionEndpoints` would clash with the existing method `ActionEndpoints.Log` (CS0102). |
| 7101 | DashboardRequestFailed | Error | yes | PgBolt.Dashboard/Endpoints/DashboardErrorFilter.cs:23 | `PgBolt dashboard: {Method} {Path} failed.` | nested `private static partial class Log` in `internal sealed partial class DashboardErrorFilter` |

---

### 8000–8999 Telemetry (NEW, spec §8)
Hosted by `PgBoltInstrumentation` (category `PgBolt.Telemetry`), using a nested `Log` in `internal sealed partial class PgBoltInstrumentation`. Templates come from spec §8.

| Id | Name | Level | Exc | file:line | Template |
|---|---|---|---|---|---|
| 8001 | TelemetryFailed **(NEW)** | per §8 | yes | PgBoltInstrumentation.ReportFailure (new) | per §8 |
| 8002 | MetricCallbackFailed **(NEW)** | per §8 | yes | PgBoltInstrumentation.ReportCallbackFailure (new) | per §8 |

---

### Notes

#### Totals
- **90** direct `ILogger` calls in `src/`. This is 89 `LogX(...)` calls plus 1 `logger.Log(level, ...)` with a runtime level (RetentionReport.cs:71). `grep` over `src/**/*.cs` without obj/bin finds exactly 90.
- **89 existing events.** CompletionSink.cs:71 and :95 share 4000 `OutcomeDiscardedSinkStopped`, which has the same template and level.
- **12 new reserved events:** 1112 `SlotReleaseFailed`, 3001–3008, 5012 `QueueStatsReadFailed`, 8001, 8002.
- **101 EventIds** in the contract table.

| Component | Calls | Events | Ids |
|---|---|---|---|
| PgBoltProcessingService | 8 | 8 | 1000–1007 |
| PgBoltServer | 12 | 12 (+1 NEW) | 1100–1111, 1112 NEW |
| QueueSettingsCache | 1 | 1 | 1200 |
| Dispatcher | 7 | 7 | 2000–2006 |
| RetryDecider + JobRunner | 2 | 2 (+8 NEW) | 3100–3101, 3001–3008 NEW |
| CompletionSink | 14 | 13 | 4000–4012 |
| LeaseKeeper | 11 | 11 | 4100–4110 |
| OutcomeStatsFlusher | 1 | 1 | 4200 |
| ProcessingMaintenance | 12 | 12 (+1 NEW) | 5000–5011, 5012 NEW |
| RetentionReport | 9 | 9 | 5100–5108 |
| PgBoltInitializer | 3 | 3 | 6000–6002 |
| RecurringSyncLog + PgBoltStore.RecurringSync | 5 | 5 | 6100–6104 |
| DashboardStartupCheck | 3 | 3 | 7000–7002 |
| Dashboard endpoints | 2 | 2 | 7100–7101 |
| Telemetry | 0 | 2 NEW | 8001–8002 |
| **Total** | **90** | **89 + 12 NEW = 101** | |

#### Dynamic templates
- **No interpolated or runtime-built templates.** Every template is a string literal.
- **ProcessingMaintenance.cs:163–165 (5001)** joins two string literals with `+`. That is a compile-time constant expression, so it is valid in an attribute argument as is, for example `Message = "... are delayed. " + "Check that ..."`. It can also be merged into one literal with an identical rendered text: the first part ends with `". "`, so the join has exactly one space.
- **Interpolated strings passed as argument values, not templates.** `ActionEndpoints.Log` call sites pass `$"batch {id}"`, `$"recurring {id}"`, `$"job {id}"`, `$"queue {queue}"`, `$"{setting} {op}"`, `$"type {name}"`, `$"type {type.Name}"`, and `jobId is { } job ? $"{message} {job}" : message` as the `target`/`action`/`result` values. The template stays constant, so nothing is needed for `[LoggerMessage]`. The strings are still built before the `IsEnabled` check, as they are today.

#### Logging through helper methods
1. **`ActionEndpoints.Log`** (src/PgBolt.Dashboard/Endpoints/ActionEndpoints.cs:303–304)
   ```csharp
   internal static void Log(ILogger logger, HttpContext http, string action, string target, string result) =>
       logger.LogInformation("PgBolt dashboard: {User} {Action} {Target}: {Result}.", User(http), action, target, result);
   ```
   Event 7100. Keep the helper and its signature, so no call site changes. Its body becomes `DashboardActionLog.DashboardActionPerformed(logger, User(http), action, target, result);`. It has 9 call sites:
   - ActionEndpoints.cs:88 `Log(logger, http, "cancel", $"batch {id}", "batch не найден")`
   - ActionEndpoints.cs:99 `Log(logger, http, "cancel", $"batch {id}", message)`
   - ActionEndpoints.cs:120 `Log(logger, http, action, $"recurring {id}", "расписание не найдено")`
   - ActionEndpoints.cs:146 `Log(logger, http, action, $"recurring {id}", message)`
   - ActionEndpoints.cs:197 `Log(loggers.CreateLogger(LogCategory), http, $"{setting} {op}", $"queue {queue}", message)`
   - ActionEndpoints.cs:215 `Log(loggers.CreateLogger(LogCategory), http, "reset", "queue settings cache", Message)`
   - ActionEndpoints.cs:253 `Log(logger, http, action, $"job {id}", outcome.Message)`
   - EnqueueEndpoints.cs:49 `ActionEndpoints.Log(logger, http, "enqueue", $"type {name}", unknown)`
   - EnqueueEndpoints.cs:84 `ActionEndpoints.Log(logger, http, "enqueue", $"type {type.Name}", jobId is { } job ? $"{message} {job}" : message)`
2. **`RecurringSyncLog.Write(ILogger logger, string owner, RecurringSyncResult result)`** (src/PgBolt.Postgres/Recurring/RecurringSyncLog.cs:9). Events 6100–6103. The category comes from the caller. Call sites:
   - ProcessingMaintenance.cs:190 `RecurringSyncLog.Write(logger, declarations.Owner, result)` (processing logger)
   - PgBoltInitializer.cs:113 `RecurringSyncLog.Write(_logger, _recurring.Owner, ...)` (`ILogger<PgBoltInitializer>`)
3. **`RetentionReport.Write(ILogger logger, RetentionResult result, PgBoltOptions core)`** (src/PgBolt.Server/Runtime/RetentionReport.cs:14). Events 5100–5108. Call sites:
   - ProcessingMaintenance.cs:298
   - tests `RetentionReportTests.cs:20,142`
4. **`PgBoltStore.RetrySyncDeadlockAsync(..., ILogger logger, string owner, ...)`** (src/PgBolt.Postgres/Storage/PgBoltStore.RecurringSync.cs:23). Event 6104. The logger is passed in by `SyncRecurringAsync(owner, definitions, ILogger logger, ct)`, which is declared in `IServerStore`/`PgBoltServerStore`.

#### Other observations
- **Throttling stays outside the generated calls.** `LogThrottle` gates 5007, 5011 (`errorLog?.TryEnter()`) and 4200 (`_throttle`), and these guards stay at the call sites.
- **Rendered text is unchanged.** `[LoggerMessage]` produces the same `{OriginalFormat}` and the same rendered message as `LogX` with an identical template, including `(null)` for null arguments. Tests that assert on text (`tests/PgBolt.Testing/ListLogger.cs`) are therefore unaffected.
- **Similar texts kept as separate events:**
  - 5008 `PgBolt: future partitions were not created (lock_timeout); retrying in {Delay}.` (ProcessingMaintenance) and 6001 `...; retrying in background.` (PgBoltInitializer) have different templates and live in different components.
  - 2005 (Dispatcher, slot not returned after a failed start) and the new 1112 `SlotReleaseFailed` (PgBoltServer, after a finished job) have different templates.
- **Arguments that are objects or types:**
  - 4105 `{Outcome}` is passed `execution.Outcome` (a `JobOutcome` object, not `.GetType().Name` as in the 4000-series). Keep the parameter type as the object type so the record's `ToString()` rendering is unchanged.
  - 4003 passes `nameof(JobOutcome.Ack)`, a constant string.

