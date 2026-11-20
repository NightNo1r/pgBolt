# PgBolt Storage maintenance (часть 2) — план реализации

> **Для исполнителя-агента.** План самодостаточен и не требует скиллов: протокол исполнения через субагентов, шаблоны их заданий, выбор моделей и требования к окружению описаны ниже («Протокол исполнения»). Шаги задач размечены чекбоксами (`- [ ]`).

**Goal:** Реализовать обслуживание хранилища PgBolt: кластерно координируемые периодические задачи (`maintenance_tasks`), периодическое создание партиций, retention `jobs`/`job_events` с компактированием закреплённых партиций, уборку `servers` с самовосстановлением строки живого сервера — и изменения core/worker из §9 спеки.

**Architecture:** Операции живут в core (`PgBolt.Postgres`): `IPgBoltMaintenance` (`EnsurePartitionsAsync`, `ApplyRetentionAsync`, `RemoveDeadServersAsync`) поверх `PartitionManager`, нового `RetentionRunner` (каталог партиций `PartitionCatalog`, транзакция 1 под `LOCK TABLE ONLY` родителя с таймаутами удержания, транзакция 2 — `DROP` по `oid`) и `PgBoltStore` (захват/завершение задач `maintenance_tasks`, уборка `servers`). Расписание — третий цикл `housekeeping` в `ProcessingMaintenance` (`PgBolt.Server`), который захватывает задачи upsert'ом `maintenance_tasks` и пишет логи по `RetentionResult` (`RetentionReport`). Heartbeat сообщает, есть ли строка `servers`, и `LeaseKeeper` регистрирует сервер заново upsert'ом.

**Tech Stack:** .NET 10 (C# 14), Npgsql 10 (`NpgsqlBatch`, slim data source), Microsoft.Extensions.Hosting 10, xUnit v3, Testcontainers (PostgreSQL 17 + PgBouncer 1.24), Microsoft.Extensions.TimeProvider.Testing.

**Spec:** `docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md` (далее «спека», ссылки вида «§5.3»). Основа: `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` («core §4.4»), `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md` («worker §6.2»). Образец стиля кода и тестов — реализованный план части 3 `docs/superpowers/plans/2026-09-28-pgbolt-worker.md` и существующие тесты `tests/PgBolt.Postgres.Tests`, `tests/PgBolt.Server.Tests`.

## Global Constraints

- Только .NET 10 (`net10.0`). `PgBolt.Postgres`, `PgBolt.Server` собираются с `IsAotCompatible=true` и `TreatWarningsAsErrors=true` (из `Directory.Build.props`). Без reflection; SQL — строковые константы или интерполяция только валидированного имени схемы (`SchemaName.Validate`), дат `yyyyMMdd` (`CultureInfo.InvariantCulture`), чисел (`oid`, миллисекунды, fillfactor) и UUID-границ `UuidV7.LowerBound`.
- Подключение напрямую или через PgBouncer в transaction mode. Запрещено: `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` (кроме `SET LOCAL`), temp tables. Транзакции открываются только `BeginTransactionAsync`/закрываются `CommitAsync`/`RollbackAsync`; `BEGIN`/`COMMIT` в тексте команд запрещены (совместимость с Npgsql multiplexing, §5.3 п. 5).
- Параметры в `NpgsqlBatchCommand` — позиционные (`$1`, `$2`, …, без имени у `NpgsqlParameter<T>`); в `NpgsqlCommand` — как в существующем коде (`P.Add` с именами) или позиционные. `oid` на стороне клиента — `long`: читается `c.oid::bigint`, передаётся `bigint`/`bigint[]` с `::oid`/`::oid[]` в SQL (§5.2).
- Календарные сутки — только `(now() AT TIME ZONE 'UTC')::date`; моменты — только `timestamptz` против `now()` (§3). `now() AT TIME ZONE 'UTC'` не сравнивается с `timestamptz`.
- Контракт HOT (core §4.2): у `jobs` нет индексов по изменяемым колонкам; новые партиции `jobs` — `WITH (fillfactor = JobsFillFactor)`.
- Значения по умолчанию (§5.1, §5.3, §6.1): `JobsRetention = EventsRetention = 7 сут`; `RetentionLockTimeout = 100 мс`; `RetentionCompactionMaxLiveJobs = 1000`; `RetentionTuning.Default = (PartitionsPerTransaction: 10, HoldStatementTimeout: 2 с, HoldIdleTimeout: 1 с)`; `MinCompactionGain = 100`; `MaintenancePollInterval = 1 мин` (±20 %); `PartitionsInterval = 1 ч`; `ServersCleanupInterval = 5 мин`; `RetentionInterval = 1 ч`; `DeadServerTimeout = 30 мин`; ускоренный повтор `partitions`/`servers` — 1 мин; `PinnedWarningDays = 7`; запас числа партиций — 30.
- Валидация (§5.1, §6.1): `JobsRetention`/`EventsRetention` — `null` или `(0, 3650 сут]`; `RetentionLockTimeout` — `(0, 1 мин]`; `RetentionCompactionMaxLiveJobs` — `[0, 10 000]`; интервалы `PgBoltProcessingOptions` и `DeadServerTimeout` — `(0, PgBoltServerOptions.MaxInterval]` (1 сут).
- Имена задач `maintenance_tasks`: `"partitions"`, `"servers"`, `"retention"`; порядок в итерации — именно такой.
- Коммиты: Conventional Commits; автор и коммиттер — `NightNo1r <57496218+NightNo1r@users.noreply.github.com>` (`git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit ...`); строк `Co-Authored-By` **нет**.

## Требования к окружению

Проверить **до Task 1**. Если любой пункт не выполняется — остановиться и доложить.

| Требование | Проверка | Ожидаемо |
|---|---|---|
| .NET SDK 10 | `dotnet --version` | `10.0.*` |
| Docker | `docker info` | без ошибок |
| Образы | `docker pull postgres:17-alpine && docker pull edoburu/pgbouncer:v1.24.1-p1` | оба скачаны |
| Сборка зелёная | `dotnet test PgBolt.slnx --filter "Category!=Load"` | все тесты проходят |
| Ветка | `git branch --show-current` | `part-2-storage-maintenance` |

- Docker недоступен — **не** заменять базу моками и не пропускать интеграционные тесты; остановиться и доложить.
- Тестовые проекты — VSTest: `dotnet test tests/<Проект> --filter "FullyQualifiedName~<Класс>"`.

## Протокол исполнения

Исполняется основной сессией (**координатор**) через субагентов — инструментом запуска агента (в Claude Code — `Agent`, тип `general-purpose`, запуск в foreground: следующий шаг зависит от результата). Скиллы не нужны. Состояние хранится в самом плане (чекбоксы) и в git, поэтому прерванное исполнение продолжается с первой неотмеченной задачи.

### Роли
- **Координатор** — основная сессия. Ведёт задачи строго по порядку (Task 1 → Task 12), запускает исполнителя и ревьюера по шаблонам ниже, отмечает чекбоксы, сам код не пишет.
- **Исполнитель** — свежий субагент на задачу (и на каждый раунд исправлений), модель из таблицы.
- **Ревьюер** — свежий субагент на задачу, всегда Opus 5.5.

Если среда не поддерживает субагентов, координатор выполняет обе роли сам, последовательно, с тем же чек-листом.

### Модели

| Задачи | Исполнитель | Почему |
|---|---|---|
| 1, 2, 3, 9, 12 | Sonnet 5.5 (`claude-sonnet-5-5`, параметр `model: "sonnet"`) | Код дан целиком, логика локальная |
| 4, 5, 6, 7, 8, 10, 11 | Opus 5.5 (`claude-opus-5-5`, `model: "opus"`) | Блокировки PostgreSQL, DDL, таймауты, PgBouncer, конкурентные тесты |
| Ревью каждой задачи и итоговое ревью ветки | Opus 5.5 (`model: "opus"`) | Сверка со спекой, поиск гонок |

### Подготовка (один раз)
1. Проверить «Требования к окружению»; при невыполнении — остановиться и доложить.
2. `git switch part-2-storage-maintenance && git pull --ff-only`; рабочее дерево чистое (`git status --short` пуст).
3. Найти первую задачу с неотмеченными чекбоксами — с неё начинать.

### Цикл одной задачи
1. Запомнить `BASE=$(git rev-parse HEAD)`.
2. **Исполнитель** — задание по шаблону «Исполнитель» (ниже), модель из таблицы.
3. Отчёт `STATUS: BLOCKED` → остановиться и доложить пользователю (текст отчёта целиком). `DONE` → проверить сам: `git log --format='%an <%ae>%n%B' $BASE..HEAD` (автор NightNo1r, нет `Co-Authored-By`), `git status --short` пуст, указанный прогон тестов задачи повторить (`dotnet test …` из последнего шага «Запустить — проходят»). Расхождение с отчётом → считать `CHANGES` с этим фактом.
4. **Ревьюер** — задание по шаблону «Ревьюер», `git diff $BASE..HEAD`.
5. `CHANGES` → новый исполнитель по шаблону «Исправление» (замечания ревьюера целиком), затем снова ревьюер (на `git diff $BASE..HEAD`). Не более двух раундов исправлений; третий `CHANGES` — остановиться и доложить.
6. `APPROVE` → координатор отмечает все чекбоксы задачи (`- [ ]` → `- [x]`) и коммитит план: `chore(plan): mark task N done` (автор NightNo1r, без `Co-Authored-By`).
7. Конец фазы (после Task 4, 8, 11, 12) — `dotnet test PgBolt.slnx --filter "Category!=Load"`; падение — остановиться и доложить.

### Завершение
1. **Итоговое ревью ветки** — ревьюер (Opus) по шаблону «Ревьюер», но предмет — `git diff main..HEAD`, текст задачи заменяется ссылкой на весь план и спеку, фокус — сквозные проблемы (согласованность задач, гонки, соответствие спеке целиком). `CHANGES` → исправление и повторное ревью по тем же правилам (не более двух раундов).
2. `dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный.
3. `git push` ветки; PR не создавать без запроса пользователя. Доложить: SHA, итог тестов, отклонения от плана из отчётов исполнителей.

### Шаблон «Исполнитель»

```text
Ты реализуешь одну задачу плана в репозитории <абсолютный путь>, ветка part-2-storage-maintenance (текущий HEAD: <SHA>).
План: docs/superpowers/plans/2026-09-29-pgbolt-storage-maintenance.md. Спека: docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md.
Прочитай в плане разделы «Global Constraints», «Контракт внутренних типов», «Правила для исполнителя» и задачу «Task N» целиком;
разделы спеки, указанные в задаче. Выполни шаги задачи строго по порядку: падающий тест → убедиться, что падает → реализация →
тесты проходят → коммит командой из плана (автор NightNo1r, без Co-Authored-By). Чекбоксы плана не отмечай.
Не делай работу других задач; код плана — эталон, отклонения только ради компиляции/несовместимости API/явной ошибки плана.
Две безуспешные попытки исправить один падающий тест → остановись.
Ответ строго в формате:
STATUS: DONE | BLOCKED
SHA: <HEAD после коммита>
TESTS: <команда> → passed N / failed M
DEVIATIONS: <список отклонений от кода плана с причинами или «нет»>
NOTES: <для BLOCKED — вывод ошибки и гипотезы>
```

### Шаблон «Ревьюер»

```text
Ты ревьюер задачи Task N плана docs/superpowers/plans/2026-09-29-pgbolt-storage-maintenance.md в репозитории <путь>.
Прочитай задачу, «Global Constraints», «Контракт внутренних типов», «Чек-лист ревьюера» и разделы спеки из задачи.
Изменения: `git diff <BASE>..HEAD`. Отчёт исполнителя: <отчёт целиком>.
Проверь по чек-листу ревьюера; сам запусти тесты задачи (команды из шагов «Запустить»). Ничего не редактируй.
Ответ: APPROVE или CHANGES; для CHANGES — нумерованный список «файл:строка — что не так — чем подтверждается — как исправить».
Стиль не оценивай, если он не нарушает Global Constraints.
```

### Шаблон «Исправление»

```text
Ты исправляешь замечания ревью по задаче Task N плана docs/superpowers/plans/2026-09-29-pgbolt-storage-maintenance.md
в репозитории <путь>, ветка part-2-storage-maintenance (HEAD: <SHA>). Прочитай задачу, «Global Constraints», «Контракт внутренних
типов», «Правила для исполнителя». Замечания: <список ревьюера целиком>.
Исправь каждое новым коммитом (Conventional Commits, автор NightNo1r через git -c user.name=NightNo1r
-c user.email=57496218+NightNo1r@users.noreply.github.com, без Co-Authored-By); не ослабляй тесты. С замечанием не согласен —
не исправляй, а объясни в отчёте с доказательством. Ответ — в формате шаблона «Исполнитель» плюс FIXED/REJECTED по номерам замечаний.
```

### Чек-лист ревьюера
- Реализовано всё из шагов; тесты задачи есть и проходят (интеграционные — в Direct и Bouncer, где предусмотрено).
- SQL совпадает со спекой по смыслу и порядку statement'ов; `LOCK TABLE ONLY` на родителе; `SET LOCAL` таймаутов удержания — сразу после `LOCK`; `DROP` — только в транзакции 2 и только после проверки `oid`/`pg_inherits`.
- Имена и сигнатуры совпадают с «Контрактом внутренних типов».
- Сутки — только через `(now() AT TIME ZONE 'UTC')::date`; никаких `DateTime.UtcNow`/`DateOnly.FromDateTime(DateTime.UtcNow)` для «сегодня».
- В runtime `PgBolt.Server` нет `Task.Delay`/таймеров мимо `TimeProvider`; unit-тесты runtime не используют реальное время (кроме `Eventually`).
- Тесты не ослаблены: нет `Skip`, удалённых assert'ов, подгонки ожидаемых значений.
- Сборка без предупреждений (`TreatWarningsAsErrors`, AOT/trim-анализ).
- Коммиты — Conventional Commits, автор NightNo1r, без `Co-Authored-By`.

### Правила для исполнителя
- Не делать работу будущих задач. Не менять тесты ради прохождения, если задача этого не предписывает.
- Код плана — эталон; отклонения — только для устранения ошибки компиляции, несовместимости API Npgsql/PostgreSQL или явной ошибки плана; каждое — в отчёт.
- Две безуспешные попытки исправить один падающий тест → `STATUS: BLOCKED` с выводом и гипотезами.

## Структура файлов

**Core — `src/PgBolt.Postgres`:**

| Файл | Действие | Ответственность |
|---|---|---|
| `Schema/Migrations/V002__maintenance_tasks.sql` | создать | Таблица `maintenance_tasks` (§4) |
| `Hosting/PgBoltInitializer.cs` | изменить | Проверка версии: ошибка только при `version < Latest`, Warning при новее (§4, §9.3) |
| `Hosting/IPgBoltMaintenance.cs` | изменить | Интерфейс §5.1 и `PgBoltMaintenance` (миграция только при `null`/`<`) |
| `Hosting/PgBoltServiceCollectionExtensions.cs` | изменить | DI `RetentionRunner`, `PgBoltMaintenance` |
| `PgBoltOptions.cs` | изменить | `JobsRetention`, `EventsRetention`, `RetentionLockTimeout`, `RetentionCompactionMaxLiveJobs` |
| `Storage/PgBoltStore.Maintenance.cs` | создать | `TryClaimMaintenanceAsync`, `CompleteMaintenanceAsync`, `RemoveDeadServersAsync` (§5.6, §5.7) |
| `Storage/PgBoltStore.Servers.cs` | изменить | Регистрация — upsert (§9.2) |
| `Storage/PgBoltStore.Heartbeat.cs` | изменить | `HeartbeatResult` с `ServerPresent` (§9.2) |
| `Storage/StoreRecords.cs` | изменить | `HeartbeatResult` |
| `Schema/RetentionResult.cs` | создать | Публичные `RetentionResult`, `PinnedPartition`, `DeferredPartition`, `DeferReason` |
| `Schema/RetentionTuning.cs` | создать | Внутренние параметры и тестовые хуки |
| `Schema/PartitionCatalog.cs` | создать | Список таблиц схемы, разбор имён, горизонт retention (§5.2) |
| `Schema/RetentionRunner.cs` | создать | Оркестрация, транзакция 1 (`RunLockedAsync`), транзакция 2 (`DropAsync`), остатки, `job_events` (§5.3 п. 3, 5, 6, §5.4, §5.5) |
| `Schema/RetentionRunner.Jobs.cs` | создать | Предпроверка, публикации, отсоединение и компактирование `jobs` (§5.3) |

**Worker — `src/PgBolt.Server`:**

| Файл | Действие | Ответственность |
|---|---|---|
| `PgBoltProcessingOptions.cs` | изменить | Интервалы задач и `DeadServerTimeout` (§6.1) |
| `Runtime/IServerStore.cs`, `Runtime/PgBoltServerStore.cs` | изменить | `HeartbeatResult`, захват/завершение задач |
| `Runtime/LeaseKeeper.cs` | изменить | Повторная регистрация при `ServerPresent = false` (§9.2) |
| `Runtime/PgBoltServer.cs` | изменить | `ServerRegistration` создаётся один раз и передаётся `LeaseKeeper` |
| `Runtime/RetentionReport.cs` | создать | Логи по `RetentionResult` (§8) |
| `Runtime/ProcessingMaintenance.cs` | изменить | Цикл `housekeeping` (§6.2) |
| `Runtime/PgBoltProcessingService.cs` | изменить | Зависимости обслуживания, предупреждения §6.4 |

**Тесты:** `tests/PgBolt.Postgres.Tests/{MigrationTests, HostingTests, OptionsTests, MaintenanceTasksTests, ServersTests, HeartbeatTests, CancelTests, SweeperTests, PartitionCatalogTests, RetentionTestBase, RetentionEventsTests, RetentionJobsTests, RetentionCompactionTests, RetentionScenarioTests}.cs`; `tests/PgBolt.Server.Tests/{Fakes/FakeServerStore, Fakes/FakeMaintenance, Unit/LeaseKeeperTests, Unit/DispatcherTests, Unit/CompletionSinkTests, Unit/FakeServerStoreTests, Unit/ServerOptionsTests, Unit/RetentionReportTests, Unit/ProcessingMaintenanceTests, Unit/HousekeepingTests, Unit/ProcessingServiceTests, Integration/ServerTestHost, Integration/MaintenanceTests}.cs`; `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`.

**Документация:** спеки core/worker (приведение в соответствие, §9), спека части 2 (статус), roadmap (статус части 2), `CHANGELOG.md`.

## Контракт внутренних типов

Соседние задачи опираются на эти имена и сигнатуры; менять их нельзя.

```csharp
// PgBolt.Postgres.Hosting — публичный
public interface IPgBoltMaintenance
{
    Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default);
    Task<RetentionResult> ApplyRetentionAsync(CancellationToken ct = default);           // Task 6
    Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default); // Task 3
}
internal sealed class PgBoltMaintenance(PgBoltOptions options, MigrationRunner migrations, PartitionManager partitions,
    PgBoltStore store, RetentionRunner retention) : IPgBoltMaintenance   // store — Task 3, retention — Task 6
{
    internal static bool NeedsMigration(bool autoMigrate, int? version); // Task 1
}

// PgBolt.Postgres.Storage
public sealed record HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases);          // Task 4
public sealed partial class PgBoltStore
{
    public Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct);
    public Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct);  // UTC; null — не захвачено (спека §5.7: DateTimeOffset? → правка в Task 12 Step 4)
    public Task<bool> CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct);
    public Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct);
}

// PgBolt.Postgres.Schema — публичные (Task 5)
public sealed record RetentionResult(IReadOnlyList<DateOnly> DroppedJobsPartitions, IReadOnlyList<DateOnly> DroppedEventsPartitions,
    int DroppedCompactionRemnants, IReadOnlyList<PinnedPartition> Pinned, IReadOnlyList<DeferredPartition> Deferred, int AttachedJobsPartitions);
public sealed record PinnedPartition(DateOnly Day, int OverdueDays, int LiveJobs, bool Compacted);
public sealed record DeferredPartition(string Table, DateOnly Day, DeferReason Reason, int OverdueDays, int? LiveJobs, string? Error = null);
public enum DeferReason { LockTimeout, HoldTimeout, DropLockTimeout, DropFailed, LiveJobs, Replicated }

// PgBolt.Postgres.Schema — внутренние (Task 5)
internal enum RetentionStep { Listed, Prechecked, LocksChecked, WarmedUp, BatchA, BatchB, Committed }
internal sealed record RetentionHookContext(RetentionStep Step, string Parent, IReadOnlyList<DateOnly> Days);
internal sealed record RetentionTuning(int PartitionsPerTransaction, TimeSpan HoldStatementTimeout, TimeSpan HoldIdleTimeout)
{
    public static RetentionTuning Default { get; }
    public Func<RetentionHookContext, CancellationToken, Task>? Hook { get; init; }
    public string? BatchBTestPrefix { get; init; }
}
internal enum CatalogKind { JobsPartition, EventsPartition, DetachedJobs, DetachedEvents, CompactionRemnant }
internal sealed record CatalogTable(long Oid, string Name, CatalogKind Kind, DateOnly Day) { public bool IsJobs { get; } public string Table { get; } }
internal static partial class PartitionCatalog
{
    public static CatalogTable? Classify(long oid, string name, string? parent);
    public static int RetentionDays(TimeSpan retention);
    public static DateOnly? Horizon(DateOnly today, TimeSpan? retention);
    public static int Overdue(DateOnly horizon, DateOnly day);
    public static Task<(DateOnly Today, List<CatalogTable> Tables)> ListAsync(NpgsqlDataSource dataSource, string schema, CancellationToken ct);
}
internal sealed partial class RetentionRunner(NpgsqlDataSource dataSource, PgBoltOptions options, RetentionTuning tuning)
{
    internal const int MinCompactionGain = 100;
    public Task<RetentionResult> ApplyAsync(CancellationToken ct = default);
    internal string WarmUpSql(string partition);   // Task 7, тест EXPLAIN
}

// PgBolt.Server.Runtime (Task 4, 10)
internal interface IServerStore
{
    Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct);
    Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct);
    Task CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct);
    // остальные члены — без изменений
}
internal sealed class LeaseKeeper(ServerRegistration registration, IServerStore store, ExecutionTable table,
    PgBoltServerOptions options, MonoClock clock, ILogger logger, Action onRecovered);
internal sealed class ProcessingMaintenance(IServerStore store, IWakeupSignal wakeup, PgBoltProcessingOptions options,
    IPgBoltMaintenance maintenance, PgBoltOptions core, MonoClock clock, ILogger logger)
{
    internal static readonly TimeSpan HousekeepingRetryDelay; // 1 мин
    internal Task<int> HousekeepingOnceAsync(CancellationToken ct); // число выполненных (захваченных) задач
}
internal static class RetentionReport
{
    internal const int PinnedWarningDays = 7;
    internal const int PartitionCountSlack = 30;
    public static void Write(ILogger logger, RetentionResult result, PgBoltOptions core);
}
```

## Review Focus

Входы и условия, которые спека подразумевает, но легко пропустить; тест на каждый добавлен в задачу-владельца:

1. **Пустая или только что созданная схема** (партиции только на сегодня и вперёд, остатков нет) — `ApplyRetentionAsync` не падает и возвращает пустой результат с `AttachedJobsPartitions` = числу партиций. Тест — Task 6.
2. **`EnsurePartitionsAsync` параллельно с retention** (`ATTACH` будущих суток берёт `SHARE UPDATE EXCLUSIVE` на родителе и конфликтует с `LOCK … ACCESS EXCLUSIVE`) — ни взаимоблокировки, ни исключения: в худшем случае `Deferred(LockTimeout)`, обе операции завершаются. Тест — Task 8.
3. **Большие `args` (TOAST, > 2 КБ) у живых задач** переносятся компактированием байт в байт (`args::text` до и после совпадает). Тест — Task 7.
4. **Одноимённые таблицы в другой схеме** (`jobs_YYYYMMDD` другой схемы PgBolt в той же БД) retention не видит и не трогает. Тест — Task 5.
5. **Компактирование на верхней границе порога** (`RetentionCompactionMaxLiveJobs = 10 000` живых строк с `args` ~1 КБ) укладывается в `HoldStatementTimeout` и не даёт `HoldTimeout` (обоснование верхней границы валидации, §5.3 «Порог»). Тест — Task 8 (только Direct).

---

## Фаза 0. Изменения core

### Task 1: Миграция V002 и проверка версии схемы

Спека: §4, §9.3, §12 «Миграции».

**Files:**
- Create: `src/PgBolt.Postgres/Schema/Migrations/V002__maintenance_tasks.sql`
- Modify: `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` (метод `InitializeAsync`)
- Modify: `src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs` (класс `PgBoltMaintenance`)
- Test: `tests/PgBolt.Postgres.Tests/MigrationTests.cs`, `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Produces: таблица `{schema}.maintenance_tasks(name, next_run_at, last_run_at, last_finished_at, last_error)`; `PgBoltMigrations.LatestVersion == 2`; `internal static bool PgBoltMaintenance.NeedsMigration(bool autoMigrate, int? version)`.

- [x] **Step 1: Тесты миграции**

В `MigrationTests.Migrate_creates_all_tables_once` добавить `"maintenance_tasks"` в массив таблиц:

```csharp
foreach (var table in new[] { "queue", "jobs", "job_events", "job_types", "servers", "schema_version", "maintenance_tasks" })
```

Добавить тесты в `MigrationTests`:

```csharp
    [Fact]
    public async Task Maintenance_tasks_has_spec_columns_and_no_rows()
    {
        await Runner().MigrateAsync(Ct);
        Assert.Equal(2, PgBoltMigrations.LatestVersion);
        await using var cmd = _db.Admin.CreateCommand("""
            SELECT string_agg(attname || ':' || format_type(atttypid, atttypmod) || ':' || attnotnull, ',' ORDER BY attnum)
            FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND NOT attisdropped
            """);
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.maintenance_tasks");
        Assert.Equal(
            "name:text:true,next_run_at:timestamp with time zone:true,last_run_at:timestamp with time zone:false," +
            "last_finished_at:timestamp with time zone:false,last_error:text:false",
            (string)(await cmd.ExecuteScalarAsync(Ct))!);
        Assert.Equal(0L, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.maintenance_tasks"));
    }

    [Fact]
    public void Script_from_version_1_contains_only_v002()
    {
        var script = PgBoltMigrations.GetScript("my_schema", fromVersion: 1);
        Assert.Contains("-- V002 maintenance_tasks", script);
        Assert.Contains("CREATE TABLE my_schema.maintenance_tasks", script);
        Assert.DoesNotContain("-- V001", script);
        Assert.Contains("VALUES (2, now())", script);
    }

    [Fact]
    public async Task Upgrade_from_v001_with_data_applies_v002()
    {
        var v1 = PgBoltMigrations.All[0];
        await _db.ExecAsync(PgBoltMigrations.Render(PgBoltMigrations.BootstrapSql, _db.Schema));
        await _db.ExecAsync(PgBoltMigrations.Render(v1.Sql, _db.Schema));
        await _db.ExecAsync("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (1, now())");
        await _db.ExecAsync("INSERT INTO {schema}.servers VALUES (gen_random_uuid(), 's', 'h', ARRAY['q'], 1, now(), now())");

        Assert.Equal(1, await Runner().MigrateAsync(Ct));

        Assert.Equal(2, await Runner().GetCurrentVersionAsync(Ct));
        Assert.Equal(1L, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.servers"));
        Assert.True(await _db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{_db.Schema}.maintenance_tasks")));
    }
```

`PgBoltMigrations.Render` и `BootstrapSql` — `internal`, тестовый проект их видит (`InternalsVisibleTo`).

- [x] **Step 2: Тесты проверки версии**

В `HostingTests` добавить логгер-сборщик (в конец файла, вне классов тестов) и тесты:

```csharp
internal sealed class ListLogger<T> : ILogger<T>
{
    public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        Entries.Enqueue((logLevel, formatter(state, exception)));
}
```

Добавить `using System.Collections.Concurrent;` и `using Microsoft.Extensions.Logging;` в начало файла. В `HostingTests`:

```csharp
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Newer_schema_version_starts_with_warning(bool autoMigrate)
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (@v, now())",
            ("v", PgBolt.Postgres.Schema.PgBoltMigrations.LatestVersion + 1));
        var log = new ListLogger<PgBoltInitializer>();
        var services = new ServiceCollection();
        services.AddSingleton<ILogger<PgBoltInitializer>>(log);
        services.AddPgBolt(o => { o.ConnectionString = pg.ConnectionString(mode); o.Schema = _db.Schema; o.AutoMigrate = autoMigrate; });
        await using var sp = services.BuildServiceProvider();

        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

        Assert.Contains(log.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("newer"));
        Assert.True(await PartitionCountAsync() > 0);
    }

    [Fact]
    public async Task AutoMigrate_off_on_v001_schema_fails_start_with_version()   // §12 «Миграции»
    {
        await _db.ExecAsync(PgBolt.Postgres.Schema.PgBoltMigrations.Render(PgBolt.Postgres.Schema.PgBoltMigrations.BootstrapSql, _db.Schema));
        await _db.ExecAsync(PgBolt.Postgres.Schema.PgBoltMigrations.Render(PgBolt.Postgres.Schema.PgBoltMigrations.All[0].Sql, _db.Schema));
        await _db.ExecAsync("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (1, now())");
        await using var sp = Build(o => o.AutoMigrate = false);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));

        Assert.Contains("schema version", e.Message);
        Assert.Contains("is 1, expected 2", e.Message);
        Assert.False(await _db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{_db.Schema}.maintenance_tasks")));
    }

    [Fact]
    public async Task Ensure_partitions_migrates_missing_schema()
    {
        await using var sp = Build();

        await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(1, Ct);

        Assert.Equal(PgBolt.Postgres.Schema.PgBoltMigrations.LatestVersion,
            await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).GetCurrentVersionAsync(Ct));
        Assert.Equal(2L, await PartitionCountAsync());
    }

    [Fact]
    public async Task Ensure_partitions_works_on_newer_schema()
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (@v, now())",
            ("v", PgBolt.Postgres.Schema.PgBoltMigrations.LatestVersion + 1));
        await using var sp = Build();

        var result = await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(1, Ct);

        Assert.False(result.TimedOut);
        Assert.Equal(2L, await PartitionCountAsync());
    }

    [Theory]
    [InlineData(true, null, true)]
    [InlineData(true, 1, true)]
    [InlineData(true, 2, false)]
    [InlineData(true, 3, false)]
    [InlineData(false, null, false)]
    [InlineData(false, 1, false)]
    public void Needs_migration_only_for_missing_or_older_schema(bool autoMigrate, int? version, bool expected) =>
        Assert.Equal(expected, PgBoltMaintenance.NeedsMigration(autoMigrate, version));
```

`LatestVersion` после этой задачи — 2, поэтому строки `2`/`3` соответствуют «текущая» и «новее».

- [x] **Step 3: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests"`
Expected: FAIL (нет `maintenance_tasks`, `NeedsMigration`, предупреждения).

- [x] **Step 4: Миграция V002**

`src/PgBolt.Postgres/Schema/Migrations/V002__maintenance_tasks.sql` (весь файл):

```sql
CREATE TABLE {schema}.maintenance_tasks (
    name             text        PRIMARY KEY,
    next_run_at      timestamptz NOT NULL,
    last_run_at      timestamptz,
    last_finished_at timestamptz,
    last_error       text
);
```

Строк задач миграция не вставляет: их создаёт первый захват (§4, §5.7). `EmbeddedResource Include="Schema\Migrations\*.sql"` уже подхватывает файл.

- [x] **Step 5: Проверка версии в `PgBoltInitializer`**

В `InitializeAsync` заменить блок от `if (_options.AutoMigrate)` до закрывающей скобки `else` на:

```csharp
        if (_options.AutoMigrate)
            await _migrations.MigrateAsync(cancellationToken);

        // §4, §9.3: версия новее кода — не ошибка (схему поднял более новый релиз при rolling deploy, миграции аддитивны).
        var version = await _migrations.GetCurrentVersionAsync(cancellationToken);
        if (version is null || version < PgBoltMigrations.LatestVersion)
            throw new InvalidOperationException(
                $"PgBolt schema version in '{_options.Schema}' is {version?.ToString() ?? "absent"}, expected {PgBoltMigrations.LatestVersion}. " +
                "Apply PgBoltMigrations.GetScript(...) or enable AutoMigrate.");
        if (version > PgBoltMigrations.LatestVersion)
            _logger.LogWarning(
                "PgBolt: schema '{Schema}' has version {Version}, newer than this build knows ({Latest}); continuing because migrations are additive.",
                _options.Schema, version, PgBoltMigrations.LatestVersion);
```

При `AutoMigrate = true` после `MigrateAsync` версия не меньше `LatestVersion`, поэтому исключение там недостижимо; текст исключения прежний (на нём держится `AutoMigrate_off_on_unmigrated_schema_fails_start`).

- [x] **Step 6: Условие миграции в `PgBoltMaintenance`**

В `IPgBoltMaintenance.cs` заменить тело `PgBoltMaintenance`:

```csharp
internal sealed class PgBoltMaintenance(PgBoltOptions options, MigrationRunner migrations, PartitionManager partitions) : IPgBoltMaintenance
{
    public async Task<PartitionEnsureResult> EnsurePartitionsAsync(int daysAhead = 7, CancellationToken ct = default)
    {
        if (NeedsMigration(options.AutoMigrate, options.AutoMigrate ? await migrations.GetCurrentVersionAsync(ct) : null))
            await migrations.MigrateAsync(ct);
        return await partitions.EnsureAsync(daysAhead, ct);
    }

    /// <summary>§4: миграция — только если схемы нет или она старше кода; более новая схема не трогается.</summary>
    internal static bool NeedsMigration(bool autoMigrate, int? version) =>
        autoMigrate && (version is null || version < PgBoltMigrations.LatestVersion);
}
```

Сигнатура `EnsurePartitionsAsync` меняется в Task 2.

- [x] **Step 7: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests"`
Expected: PASS (Direct и Bouncer).

Затем полный прогон core: `dotnet test tests/PgBolt.Postgres.Tests`. Expected: PASS (V002 аддитивна, остальные тесты не меняются).

- [x] **Step 8: Commit**

```bash
git add src/PgBolt.Postgres/Schema/Migrations/V002__maintenance_tasks.sql src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs tests/PgBolt.Postgres.Tests/MigrationTests.cs tests/PgBolt.Postgres.Tests/HostingTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(core): add maintenance_tasks migration and tolerate newer schema versions"
```

---

### Task 2: Настройки retention и `EnsurePartitionsAsync(int?)`

Спека: §5.1 (настройки, валидация, XML-doc), §9.3.

**Files:**
- Modify: `src/PgBolt.Postgres/PgBoltOptions.cs`
- Modify: `src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs`
- Test: `tests/PgBolt.Postgres.Tests/OptionsTests.cs`, `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Produces: `PgBoltOptions.JobsRetention`, `EventsRetention` (`TimeSpan?`), `RetentionLockTimeout` (`TimeSpan`), `RetentionCompactionMaxLiveJobs` (`int`), `internal static readonly TimeSpan PgBoltOptions.MaxRetention` (3650 сут); `IPgBoltMaintenance.EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default)`.

- [x] **Step 1: Тесты опций**

В `OptionsTests.Defaults_match_spec` добавить:

```csharp
        Assert.Equal(TimeSpan.FromDays(7), o.JobsRetention);
        Assert.Equal(TimeSpan.FromDays(7), o.EventsRetention);
        Assert.Equal(TimeSpan.FromMilliseconds(100), o.RetentionLockTimeout);
        Assert.Equal(1000, o.RetentionCompactionMaxLiveJobs);
```

Добавить тесты:

```csharp
    public static TheoryData<string, TimeSpan> InvalidRetention => new()
    {
        { "JobsRetention", TimeSpan.Zero },
        { "JobsRetention", TimeSpan.FromSeconds(-1) },
        { "JobsRetention", TimeSpan.FromDays(3650) + TimeSpan.FromTicks(1) },
        { "EventsRetention", TimeSpan.Zero },
        { "EventsRetention", TimeSpan.FromDays(3651) },
    };

    [Theory]
    [MemberData(nameof(InvalidRetention))]
    public void Validate_rejects_retention_out_of_range(string field, TimeSpan value)
    {
        var o = new PgBoltOptions { ConnectionString = "Host=x" };
        if (field == "JobsRetention") o.JobsRetention = value; else o.EventsRetention = value;
        var e = Assert.Throws<InvalidOperationException>(o.Validate);
        Assert.Contains(field, e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_accepts_null_retention_and_bounds()
    {
        new PgBoltOptions { ConnectionString = "Host=x", JobsRetention = null, EventsRetention = null }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", JobsRetention = TimeSpan.FromDays(3650), EventsRetention = TimeSpan.FromTicks(1) }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", RetentionLockTimeout = TimeSpan.FromMinutes(1), RetentionCompactionMaxLiveJobs = 0 }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", RetentionCompactionMaxLiveJobs = 10_000 }.Validate();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(60_001)]
    public void Validate_rejects_retention_lock_timeout_out_of_range(int ms) =>
        Assert.Contains("RetentionLockTimeout", Assert.Throws<InvalidOperationException>(() =>
            new PgBoltOptions { ConnectionString = "Host=x", RetentionLockTimeout = TimeSpan.FromMilliseconds(ms) }.Validate()).Message);

    [Theory]
    [InlineData(-1)]
    [InlineData(10_001)]
    public void Validate_rejects_compaction_limit_out_of_range(int limit) =>
        Assert.Contains("RetentionCompactionMaxLiveJobs", Assert.Throws<InvalidOperationException>(() =>
            new PgBoltOptions { ConnectionString = "Host=x", RetentionCompactionMaxLiveJobs = limit }.Validate()).Message);
```

В `HostingTests` добавить:

```csharp
    [Fact]
    public async Task Ensure_partitions_without_argument_uses_partition_days_ahead()
    {
        await using var sp = Build(o => o.PartitionDaysAhead = 3);

        await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(ct: Ct);

        Assert.Equal(4L, await PartitionCountAsync());
    }
```

- [x] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~OptionsTests|FullyQualifiedName~HostingTests"`
Expected: FAIL (компиляция: нет свойств).

- [x] **Step 3: Свойства и валидация `PgBoltOptions`**

После `PartitionLockTimeout` добавить:

```csharp
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
```

В `Validate()` перед проверкой `JobTypesCacheTtl` добавить:

```csharp
        ValidateRetention(JobsRetention, nameof(JobsRetention));
        ValidateRetention(EventsRetention, nameof(EventsRetention));
        if (RetentionLockTimeout <= TimeSpan.Zero || RetentionLockTimeout > TimeSpan.FromMinutes(1))
            throw new InvalidOperationException("PgBolt: RetentionLockTimeout must be in (0, 1 min].");
        if (RetentionCompactionMaxLiveJobs is < 0 or > 10_000)
            throw new InvalidOperationException("PgBolt: RetentionCompactionMaxLiveJobs must be in [0, 10000].");
```

И метод:

```csharp
    private static void ValidateRetention(TimeSpan? retention, string name)
    {
        if (retention is { } r && (r <= TimeSpan.Zero || r > MaxRetention))
            throw new InvalidOperationException($"PgBolt: {name} must be null or in (0, 3650 days].");
    }
```

- [x] **Step 4: `EnsurePartitionsAsync(int?)`**

`IPgBoltMaintenance.cs` — интерфейс и метод:

```csharp
/// <summary>
/// Обслуживание схемы (core §4.4, часть 2). Все операции идемпотентны и безопасны при параллельном вызове. Без PgBolt.Server
/// вызывайте их из своего планировщика: EnsurePartitionsAsync — не реже раза в сутки (при PartitionDaysAhead ≥ 2),
/// ApplyRetentionAsync — раз в час и не чаще после LockTimeout/HoldTimeout.
/// </summary>
public interface IPgBoltMaintenance
{
    /// <param name="daysAhead">null — PgBoltOptions.PartitionDaysAhead.</param>
    Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default);
}
```

```csharp
    public async Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default)
    {
        if (NeedsMigration(options.AutoMigrate, options.AutoMigrate ? await migrations.GetCurrentVersionAsync(ct) : null))
            await migrations.MigrateAsync(ct);
        return await partitions.EnsureAsync(daysAhead ?? options.PartitionDaysAhead, ct);
    }
```

Вызовы с явным `int` (`HostingTests`, `LeaseFailureTests`) компилируются без изменений.

- [x] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~OptionsTests|FullyQualifiedName~HostingTests"`
Expected: PASS. Затем `dotnet build PgBolt.slnx` — без предупреждений.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/PgBoltOptions.cs src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs tests/PgBolt.Postgres.Tests/OptionsTests.cs tests/PgBolt.Postgres.Tests/HostingTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(core): add retention options and default EnsurePartitionsAsync days ahead"
```

---

### Task 3: Захват задач `maintenance_tasks` и уборка `servers`

Спека: §5.6, §5.7, §12 «`servers`, heartbeat и `maintenance_tasks`».

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Maintenance.cs`
- Modify: `src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs`, `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`
- Test: `tests/PgBolt.Postgres.Tests/MaintenanceTasksTests.cs` (создать)

**Interfaces:**
- Consumes: `maintenance_tasks` (Task 1).
- Produces: `PgBoltStore.TryClaimMaintenanceAsync`, `CompleteMaintenanceAsync`, `RemoveDeadServersAsync` (сигнатуры — «Контракт»); `IPgBoltMaintenance.RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default)`; конструктор `PgBoltMaintenance(options, migrations, partitions, store)`.

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/MaintenanceTasksTests.cs` (весь файл):

```csharp
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class MaintenanceTasksTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private Task MakeDueAsync(string name) =>
        Db.ExecAsync("UPDATE {schema}.maintenance_tasks SET next_run_at = now() - interval '1 second' WHERE name = @n", ("n", name));

    private Task<bool> RowAsync(string sql, string name) =>
        Db.ScalarAsync<bool>($"SELECT {sql} FROM {{schema}}.maintenance_tasks WHERE name = @n", ("n", name));

    [Fact]
    public async Task Claim_without_row_creates_and_claims_it()
    {
        var claimedAt = await Store.TryClaimMaintenanceAsync("retention", Hour, Ct);

        Assert.NotNull(claimedAt);
        Assert.Equal(DateTimeKind.Utc, claimedAt.Value.Kind);
        Assert.True(await RowAsync("next_run_at BETWEEN now() + interval '59 minutes' AND now() + interval '61 minutes' AND last_run_at IS NOT NULL", "retention"));
        Assert.Null(await Store.TryClaimMaintenanceAsync("retention", Hour, Ct));
    }

    [Fact]
    public async Task Due_row_is_claimed_again_with_new_token()
    {
        var first = await Store.TryClaimMaintenanceAsync("servers", Hour, Ct);
        await MakeDueAsync("servers");

        var second = await Store.TryClaimMaintenanceAsync("servers", Hour, Ct);

        Assert.NotNull(second);
        Assert.NotEqual(first, second);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Parallel_claims_have_exactly_one_winner(bool rowExists)
    {
        if (rowExists)
        {
            await Store.TryClaimMaintenanceAsync("partitions", Hour, Ct);
            await MakeDueAsync("partitions");
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Store.TryClaimMaintenanceAsync("partitions", Hour, Ct)));

        Assert.Single(results, r => r is not null);
    }

    [Fact]
    public async Task Complete_writes_result_only_for_current_claim()
    {
        var stale = (await Store.TryClaimMaintenanceAsync("retention", Hour, Ct))!.Value;
        await MakeDueAsync("retention");
        var current = (await Store.TryClaimMaintenanceAsync("retention", Hour, Ct))!.Value;

        Assert.False(await Store.CompleteMaintenanceAsync("retention", stale, "old", TimeSpan.FromMinutes(1), Ct));
        Assert.True(await RowAsync("last_finished_at IS NULL AND last_error IS NULL", "retention"));

        Assert.True(await Store.CompleteMaintenanceAsync("retention", current, null, null, Ct));
        Assert.True(await RowAsync("last_finished_at IS NOT NULL AND last_error IS NULL AND next_run_at > now() + interval '59 minutes'", "retention"));
    }

    [Fact]
    public async Task Complete_with_retry_delay_shortens_next_run()
    {
        var claimedAt = (await Store.TryClaimMaintenanceAsync("partitions", Hour, Ct))!.Value;

        Assert.True(await Store.CompleteMaintenanceAsync("partitions", claimedAt, "lock_timeout", TimeSpan.FromMinutes(1), Ct));

        Assert.True(await RowAsync("last_error = 'lock_timeout' AND next_run_at <= now() + interval '1 minute'", "partitions"));
    }

    [Fact]
    public async Task Remove_dead_servers_deletes_only_stale_rows()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        await Store.RegisterServerAsync(ServerB, "b", "h", ["q"], 1, Ct);
        await Db.ExecAsync("UPDATE {schema}.servers SET heartbeat_at = now() - interval '31 minutes' WHERE id = @id", ("id", ServerA));

        Assert.Equal(1, await Store.RemoveDeadServersAsync(TimeSpan.FromMinutes(30), Ct));

        Assert.Equal(new[] { ServerB }, await Db.ScalarAsync<Guid[]>("SELECT array_agg(id) FROM {schema}.servers"));
    }

    [Fact]
    public async Task Remove_dead_servers_rejects_non_positive_threshold() =>
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Store.RemoveDeadServersAsync(TimeSpan.Zero, Ct));
}

public sealed class MaintenanceTasksTests_Direct(PostgresFixture pg) : MaintenanceTasksTests(pg, ConnectionMode.Direct);
public sealed class MaintenanceTasksTests_Bouncer(PostgresFixture pg) : MaintenanceTasksTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MaintenanceTasksTests"`
Expected: FAIL (компиляция).

- [x] **Step 3: SQL**

`src/PgBolt.Postgres/Storage/PgBoltStore.Maintenance.cs` (весь файл):

```csharp
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string TryClaimMaintenanceTemplate = """
        INSERT INTO {schema}.maintenance_tasks AS t (name, next_run_at, last_run_at)
        VALUES (@name, now() + @interval, now())
        ON CONFLICT (name) DO UPDATE
            SET next_run_at = EXCLUDED.next_run_at, last_run_at = EXCLUDED.last_run_at
            WHERE t.next_run_at <= now()
        RETURNING t.last_run_at;
        """;

    private const string CompleteMaintenanceTemplate = """
        UPDATE {schema}.maintenance_tasks
        SET last_error = @error,
            last_finished_at = now(),
            next_run_at = CASE WHEN @retry_delay::interval IS NULL THEN next_run_at
                               ELSE LEAST(next_run_at, now() + @retry_delay::interval) END
        WHERE name = @name AND last_run_at = @claimed_at;
        """;

    private const string RemoveDeadServersTemplate = """
        DELETE FROM {schema}.servers WHERE heartbeat_at < now() - @older_than;
        """;

    private string TryClaimMaintenanceSql => field ??= Render(TryClaimMaintenanceTemplate);
    private string CompleteMaintenanceSql => field ??= Render(CompleteMaintenanceTemplate);
    private string RemoveDeadServersSql => field ??= Render(RemoveDeadServersTemplate);

    /// <summary>
    /// Захват периодической задачи (часть 2 §5.7): upsert строки, если её срок наступил. Возвращает last_run_at захвата (UTC) —
    /// токен владения для <see cref="CompleteMaintenanceAsync"/>; null — задачу держит другой под.
    /// </summary>
    public Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return QueryAsync<DateTime?>(TryClaimMaintenanceSql, ps =>
        {
            P.Add(ps, "name", name);
            P.Add(ps, "interval", interval);
        }, async (r, c) => await r.ReadAsync(c) ? r.GetDateTime(0) : null, ct);
    }

    /// <summary>
    /// Результат запуска (§5.7): только пока строку не захватил другой под (last_run_at = claimedAt). retryDelay не null —
    /// next_run_at сокращается до now() + retryDelay. Возвращает false, если строка перезахвачена.
    /// </summary>
    public async Task<bool> CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return await NonQueryAsync(CompleteMaintenanceSql, ps =>
        {
            P.Add(ps, "name", name);
            P.Add(ps, "claimed_at", claimedAt, NpgsqlDbType.TimestampTz);
            P.AddNullable(ps, "error", error, NpgsqlDbType.Text);
            ps.Add(new NpgsqlParameter("retry_delay", NpgsqlDbType.Interval) { Value = retryDelay is { } d ? d : DBNull.Value });
        }, ct) == 1;
    }

    /// <summary>Уборка строк упавших серверов (§5.6): только heartbeat_at &lt; now() − olderThan.</summary>
    public Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(olderThan, TimeSpan.Zero);
        return NonQueryAsync(RemoveDeadServersSql, ps => P.Add(ps, "older_than", olderThan), ct);
    }
}
```

`claimedAt` приходит из `TryClaim` с `DateTimeKind.Utc` и микросекундной точностью — равенство `last_run_at = @claimed_at` точное. Тип `DateTime?` (UTC) — отклонение от текста спеки §5.7 (`DateTimeOffset?`): Npgsql читает `timestamptz` как `DateTime` с `Kind = Utc`, токен возвращается в `CompleteMaintenanceAsync` без преобразований; спека приводится в Task 12 Step 4.

- [x] **Step 4: `IPgBoltMaintenance.RemoveDeadServersAsync` и DI**

В начало `IPgBoltMaintenance.cs` рядом с `using PgBolt.Postgres.Schema;` добавить `using PgBolt.Postgres.Storage;` (`PgBoltStore` в конструкторе `PgBoltMaintenance`; без него CS0246). `RetentionRunner`/`RetentionResult` (Task 6) — в `PgBolt.Postgres.Schema`, этот using уже есть; в `PgBoltServiceCollectionExtensions.cs` usings `PgBolt.Postgres`, `PgBolt.Postgres.Schema`, `PgBolt.Postgres.Storage` уже есть.

В интерфейс:

```csharp
    /// <summary>Удаляет строки servers с heartbeat_at старше olderThan (часть 2 §5.6); строку живого сервера восстановит его heartbeat.</summary>
    Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default);
```

`PgBoltMaintenance` — добавить параметр первичного конструктора `PgBoltStore store` (после `partitions`) и метод:

```csharp
    public Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default) =>
        store.RemoveDeadServersAsync(olderThan, ct);
```

В `PgBoltServiceCollectionExtensions` регистрация:

```csharp
        services.AddSingleton<IPgBoltMaintenance>(sp => new PgBoltMaintenance(options,
            sp.GetRequiredService<MigrationRunner>(), sp.GetRequiredService<PartitionManager>(), sp.GetRequiredService<PgBoltStore>()));
```

- [x] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MaintenanceTasksTests|FullyQualifiedName~HostingTests"`
Expected: PASS.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Storage/PgBoltStore.Maintenance.cs src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs tests/PgBolt.Postgres.Tests/MaintenanceTasksTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(core): claim maintenance tasks and remove dead servers"
```

---

### Task 4: Самовосстановление строки `servers`

Спека: §9.2, §12 «`servers`, heartbeat» и «Worker» (`LeaseKeeper`).

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.Servers.cs`, `src/PgBolt.Postgres/Storage/PgBoltStore.Heartbeat.cs`, `src/PgBolt.Postgres/Storage/StoreRecords.cs`
- Modify: `src/PgBolt.Server/Runtime/IServerStore.cs`, `src/PgBolt.Server/Runtime/PgBoltServerStore.cs`, `src/PgBolt.Server/Runtime/LeaseKeeper.cs`, `src/PgBolt.Server/Runtime/PgBoltServer.cs`
- Modify (тесты под новую сигнатуру): `tests/PgBolt.Postgres.Tests/{HeartbeatTests,ServersTests,CancelTests,SweeperTests}.cs`, `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`, `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`, `tests/PgBolt.Server.Tests/Unit/{FakeServerStoreTests,DispatcherTests,CompletionSinkTests,LeaseKeeperTests}.cs`

**Interfaces:**
- Produces: `HeartbeatResult`; `PgBoltStore.HeartbeatAsync(...) : Task<HeartbeatResult>`; `IServerStore.HeartbeatAsync(...) : Task<HeartbeatResult>`; конструктор `LeaseKeeper(ServerRegistration registration, …)`; `FakeServerStore.ServerPresent` (`bool`, по умолчанию `true`).

- [x] **Step 1: Тесты core**

В `ServersTests` добавить:

```csharp
    [Fact]
    public async Task Register_again_upserts_and_keeps_started_at()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h1", ["q"], 1, Ct);
        await Db.ExecAsync("UPDATE {schema}.servers SET started_at = now() - interval '1 hour', heartbeat_at = now() - interval '1 hour' WHERE id = @id", ("id", ServerA));

        await Store.RegisterServerAsync(ServerA, "a2", "h2", ["q", "r"], 5, Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT name = 'a2' AND host = 'h2' AND capacity = 5 AND queues = ARRAY['q', 'r']
                   AND started_at < now() - interval '59 minutes' AND heartbeat_at > now() - interval '1 minute'
            FROM {schema}.servers WHERE id = @id
            """, ("id", ServerA)));
    }

    [Fact]
    public async Task Heartbeat_reports_server_row_presence_and_still_extends_leases()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        Assert.True((await Store.HeartbeatAsync(ServerA, [], Lease, Ct)).ServerPresent);

        await EnqueueAsync();
        var job = await LeaseOneAsync();
        var withLease = await Store.HeartbeatAsync(ServerA, [new LeaseRef(job.Id, job.LeaseId)], Lease, Ct);
        Assert.True(withLease.ServerPresent);                                       // §12: и с непустым списком lease
        Assert.Equal(job.LeaseId, Assert.Single(withLease.Leases).LeaseId);
        await Store.RemoveServerAsync(ServerA, Ct);
        await Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() + interval '1 second' WHERE job_id = @id", ("id", job.Id));

        var result = await Store.HeartbeatAsync(ServerA, [new LeaseRef(job.Id, job.LeaseId)], Lease, Ct);

        Assert.False(result.ServerPresent);
        Assert.Equal(job.LeaseId, Assert.Single(result.Leases).LeaseId);
        Assert.True(await Db.ScalarAsync<bool>("SELECT lease_until > now() + interval '4 minutes' FROM {schema}.queue WHERE job_id = @id", ("id", job.Id)));
        Assert.False((await Store.HeartbeatAsync(ServerA, [], Lease, Ct)).ServerPresent);
    }
```

- [x] **Step 2: Тесты `LeaseKeeper`**

В `LeaseKeeperTests`: добавить поле `Registration` сразу после объявления `ServerId` (строка 15: статические поля инициализируются в порядке объявления, и выше `ServerId` поле получило бы `Guid.Empty`) и заменить создание:

```csharp
    private static readonly ServerRegistration Registration = new(ServerId, "s", "h", ["q"], 1);

    private LeaseKeeper Create(TimeSpan? lease = null, TimeSpan? interval = null, double jitter = 0.5) =>
        new(Registration, _store, _table, new PgBoltServerOptions { LeaseDuration = lease ?? L, HeartbeatInterval = interval ?? H },
            _clock, _logger, () => Interlocked.Increment(ref _recovered))
        {
            Jitter = () => jitter,
        };
```

Новые тесты:

```csharp
    // ---- строка servers (часть 2 §9.2) ------------------------------------------------------

    [Fact]
    public async Task Missing_server_row_is_registered_again()
    {
        var keeper = Create();
        _store.ServerPresent = false;

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Same(Registration, Assert.Single(_store.CallsOf("RegisterServer")).Detail);
        Assert.Contains(_logger.At(LogLevel.Warning), e => e.Message.Contains("registered again"));
    }

    [Fact]
    public async Task Present_server_row_is_not_registered()
    {
        await Create().HeartbeatOnceAsync(Ct);

        Assert.Empty(_store.CallsOf("RegisterServer"));
    }

    [Fact]
    public async Task Failed_registration_is_logged_and_retried_on_next_heartbeat()
    {
        var keeper = Create();
        _store.ServerPresent = false;
        _store.OnRegisterServer = (_, _) => Task.FromException(new TimeoutException("db down"));

        await keeper.HeartbeatOnceAsync(Ct);
        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(2, _store.CallsOf("RegisterServer").Count());
        Assert.Equal(2, _logger.At(LogLevel.Warning).Count(e => e.Message.Contains("re-registering")));
    }

    [Fact]
    public async Task Registration_ending_after_planned_time_delays_only_next_heartbeat()
    {
        var keeper = Create(jitter: 0);   // шаг расписания 0.9 H
        _store.ServerPresent = false;
        var registration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnRegisterServer = (_, _) => registration.Task;   // фейк не наблюдает токен: длительность задаёт тест
        keeper.Start();

        _time.Advance(H * 0.9);                                  // первый heartbeat: sentAt = S
        await Eventually.True(() => _store.CallsOf("RegisterServer").Count() == 1, "first heartbeat registers");
        _time.Advance(H);                                        // S + H: плановый S + 0.9 H уже прошёл
        _store.ServerPresent = true;
        registration.SetResult();

        await Eventually.True(() => HeartbeatCalls == 2, "next heartbeat starts right after the registration");
        _time.Advance(H * 0.9 - Tick);
        await Task.Yield();
        Assert.Equal(2, HeartbeatCalls);                         // расписание — от фактического sentAt = S + H
        _time.Advance(Tick);
        await Eventually.True(() => HeartbeatCalls == 3, "third heartbeat at S + H + 0.9 H");
        await keeper.StopAsync();
    }

    [Fact]
    public async Task Stop_during_registration_cancels_and_waits_for_it()
    {
        var keeper = Create(jitter: 0);
        _store.ServerPresent = false;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedCancel = false;
        _store.OnRegisterServer = async (_, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                observedCancel = true;
                throw;
            }
        };
        keeper.Start();
        _time.Advance(H * 0.9);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        await keeper.StopAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.True(observedCancel);
        Assert.Single(_store.CallsOf("RegisterServer"));
    }
```

- [x] **Step 3: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~ServersTests"` и `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~LeaseKeeperTests"`
Expected: FAIL (компиляция).

- [x] **Step 4: Core — `HeartbeatResult`, upsert, heartbeat**

`StoreRecords.cs` — после `HeartbeatLease`:

```csharp
/// <summary>Ответ heartbeat (часть 2 §9.2): обновлена ли строка servers и живые lease из списка.</summary>
public sealed record HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases);
```

`PgBoltStore.Servers.cs` — шаблон регистрации:

```csharp
    private const string RegisterServerTemplate = """
        INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
        VALUES (@id, @name, @host, @queues, @capacity, now(), now())
        ON CONFLICT (id) DO UPDATE
            SET name = EXCLUDED.name, host = EXCLUDED.host, queues = EXCLUDED.queues,
                capacity = EXCLUDED.capacity, heartbeat_at = now();
        """;
```

и XML-doc метода: `/// <summary>Регистрирует виртуальный сервер или восстанавливает его строку (часть 3 §7.2, часть 2 §9.2): upsert, started_at существующей строки не меняется. host — Environment.MachineName.</summary>`.

`PgBoltStore.Heartbeat.cs` — шаблон и метод целиком:

```csharp
    private const string HeartbeatTemplate = """
        WITH s AS (
            UPDATE {schema}.servers SET heartbeat_at = now() WHERE id = @server
            RETURNING 1
        ),
        l AS MATERIALIZED (
            SELECT job_id FROM {schema}.queue
            WHERE job_id = ANY(@ids) AND lease_id = ANY(@lease_ids) AND lease_owner = @server
            ORDER BY job_id                                -- порядок захвата core §5.3 не меняется
            FOR UPDATE
        ),
        u AS (
            UPDATE {schema}.queue q SET lease_until = now() + @lease
            FROM l WHERE q.job_id = l.job_id
            RETURNING q.job_id, q.lease_id, q.cancel_requested
        )
        SELECT EXISTS (SELECT 1 FROM s) AS server_present, u.job_id, u.lease_id, u.cancel_requested
        FROM (VALUES (1)) AS one(x) LEFT JOIN u ON true
        """;

    private string HeartbeatSql => field ??= Render(HeartbeatTemplate);

    /// <summary>
    /// Продлевает lease из списка (только строки этого сервера с тем же lease_id), обновляет servers.heartbeat_at
    /// и доставляет запросы отмены (часть 3 §8.1). Lease из списка, отсутствующий в ответе, потерян. ServerPresent = false —
    /// строки servers нет (удалена уборкой), сервер должен зарегистрироваться заново (часть 2 §9.2).
    /// </summary>
    public Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(leases);
        var ids = new Guid[leases.Count];
        var leaseIds = new Guid[leases.Count];
        var i = 0;
        foreach (var l in leases)
        {
            (ids[i], leaseIds[i]) = (l.JobId, l.LeaseId);
            i++;
        }

        return QueryAsync(HeartbeatSql, ps =>
        {
            P.Add(ps, "server", serverId);
            P.Add(ps, "ids", ids);
            P.Add(ps, "lease_ids", leaseIds);
            P.Add(ps, "lease", lease);
        }, static async (r, c) =>
        {
            var present = false;
            var alive = new List<HeartbeatLease>();
            while (await r.ReadAsync(c))
            {
                present = r.GetBoolean(0);
                if (!r.IsDBNull(1))
                    alive.Add(new HeartbeatLease(r.GetGuid(1), r.GetGuid(2), r.GetBoolean(3)));
            }
            return new HeartbeatResult(present, alive);
        }, ct);
    }
```

- [x] **Step 5: Тесты core под новую сигнатуру**

Заменить в существующих тестах результат heartbeat на `.Leases` (поведение тех же проверок не меняется):
- `tests/PgBolt.Postgres.Tests/HeartbeatTests.cs`: каждое `await Store.HeartbeatAsync(...)`, чей результат передаётся в `Assert.Empty`/`Assert.Single` или присваивается переменной, — обернуть: `(await Store.HeartbeatAsync(...)).Leases` (строки 40, 60, 63, 75, 87, 100, 109; строка 137 результат не использует — без изменений).
- `ServersTests.cs:46`: `Assert.Empty((await Store.HeartbeatAsync(ServerA, [], Lease, Ct)).Leases);`
- `CancelTests.cs:35`, `SweeperTests.cs:66`: `Assert.Single((await Store.HeartbeatAsync(...)).Leases)`.
- `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs:52-53`: `(await store.HeartbeatAsync(...)).Leases.Count` и `Assert.Empty((await store.HeartbeatAsync(Server, [], lease, Ct)).Leases);`.

- [x] **Step 6: Worker — store, фейк, `LeaseKeeper`, `PgBoltServer`**

`IServerStore`: `Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct);` — `PgBoltServerStore` пробрасывает без изменений тела (меняется только тип).

`FakeServerStore`: добавить свойство и заменить метод (делегат `OnHeartbeat` остаётся прежним — существующие тесты не меняются):

```csharp
    /// <summary>ServerPresent ответа heartbeat (строка servers есть).</summary>
    public bool ServerPresent { get; set; } = true;

    public async Task<HeartbeatResult> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Heartbeat", Detail: leases.ToArray()));
        return new HeartbeatResult(ServerPresent, await OnHeartbeat(leases, ct));
    }
```

`FakeServerStoreTests.cs:26` → `Assert.Single((await store.HeartbeatAsync(...)).Leases)`, `:61` → `Assert.Empty((await store.HeartbeatAsync(...)).Leases)`.

`LeaseKeeper`:
- поле `private readonly ServerRegistration _registration;`, первый параметр конструктора `ServerRegistration registration` вместо `Guid serverId`; в теле: `_registration = registration ?? throw new ArgumentNullException(nameof(registration)); _serverId = registration.Id;`.
- в `HeartbeatCoreAsync`: переменная `IReadOnlyList<HeartbeatLease> alive` → `HeartbeatResult result`; вызов `result = await _store.HeartbeatAsync(...)`; после `using`-блока — `var alive = result.Leases;` (дальнейший код не меняется); в самом конце метода, после блока `if (wasLost && !HeartbeatLost) { … }`:

```csharp
        if (!result.ServerPresent)
            await RegisterAgainAsync(ct);
```

- новый метод:

```csharp
    /// <summary>
    /// Часть 2 §9.2: строки servers нет (удалена уборкой после паузы дольше DeadServerTimeout) — upsert регистрации.
    /// Таймаут — HeartbeatInterval / 2, поэтому следующий успешный ответ не позже S + 1.6 H; не бросает.
    /// Выполняется внутри цикла под его токеном: StopAsync отменяет и дожидается её до удаления строк серверов.
    /// </summary>
    private async Task RegisterAgainAsync(CancellationToken ct)
    {
        using var timeout = new CancellationTokenSource(_interval / 2, _clock.Time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
        try
        {
            await _store.RegisterServerAsync(_registration, linked.Token);
            _logger.LogWarning("PgBolt: servers row of server {ServerId} was missing (removed by cleanup); registered again.", _serverId);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "PgBolt: re-registering server {ServerId} failed; retrying on the next heartbeat.", _serverId);
        }
    }
```

`PgBoltServer`:
- поле `private readonly ServerRegistration _registration;`; в конструкторе после `_logger = …`:

```csharp
        _registration = new ServerRegistration(Id, Name, Environment.MachineName, [.. Options.Queues.Select(q => q.Name)], Options.Capacity);
```

- `_keeper = new LeaseKeeper(_registration, store, _table, Options, clock, _logger, () => dispatcher?.Wake());`
- `RegisterAsync`:

```csharp
    public async Task RegisterAsync(CancellationToken ct)
    {
        await _store.RegisterServerAsync(_registration, ct);
        await WarnMissingExecutorsAsync(_registration.Queues, ct);
    }
```

Места создания `LeaseKeeper` в тестах: `DispatcherTests.cs:58` → `new LeaseKeeper(new ServerRegistration(ServerId, "s", "h", ["q"], 1), _store, …)`; `CompletionSinkTests.cs:192` → `new LeaseKeeper(new ServerRegistration(Guid.NewGuid(), "s", "h", ["q"], 1), _store, …)`.

- [x] **Step 7: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests` и `dotnet test tests/PgBolt.Server.Tests --filter "Category!=Load"` и `dotnet test tests/E2E/PgBolt.E2E.Tests`
Expected: PASS.

- [x] **Step 8: Commit**

```bash
git add src/PgBolt.Postgres/Storage src/PgBolt.Server/Runtime tests/PgBolt.Postgres.Tests tests/PgBolt.Server.Tests tests/E2E/PgBolt.E2E.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat: restore servers row from heartbeat after cleanup"
```

---
## Фаза 1. Retention в core

### Task 5: Типы результата, параметры и каталог партиций

Спека: §5.1 (API результата), §5.2 (кандидаты, список, имена), §5.3 (`RetentionTuning`), §3.

**Files:**
- Create: `src/PgBolt.Postgres/Schema/RetentionResult.cs`, `src/PgBolt.Postgres/Schema/RetentionTuning.cs`, `src/PgBolt.Postgres/Schema/PartitionCatalog.cs`
- Test: `tests/PgBolt.Postgres.Tests/PartitionCatalogTests.cs` (создать)

**Interfaces:**
- Produces: все типы раздела «Контракт» с пометкой «Task 5».

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/PartitionCatalogTests.cs` (весь файл):

```csharp
using System.Globalization;
using PgBolt.Postgres.Schema;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed class PartitionCatalogUnitTests
{
    private static readonly DateOnly Day = new(2026, 10, 2);

    [Theory]
    [InlineData("jobs_20261002", "jobs", "JobsPartition")]
    [InlineData("jobs_20261002", null, "DetachedJobs")]
    [InlineData("jobs_20261002_r16384", null, "CompactionRemnant")]
    [InlineData("job_events_20261002", "job_events", "EventsPartition")]
    [InlineData("job_events_20261002", null, "DetachedEvents")]
    public void Classifies_our_tables(string name, string? parent, string kind)   // CatalogKind — internal: в сигнатуре публичного теста нельзя
    {
        var t = PartitionCatalog.Classify(42, name, parent);
        Assert.Equal(new CatalogTable(42, name, Enum.Parse<CatalogKind>(kind), Day), t);
    }

    [Theory]
    [InlineData("jobs_2026101", null)]
    [InlineData("jobs_20261002_old", null)]
    [InlineData("jobs_20261399", null)]
    [InlineData("jobs_20261002", "other.jobs")]
    [InlineData("jobs_20261002", "job_events")]
    [InlineData("jobs_20261002_r1", "jobs")]
    [InlineData("jobs_20261002_r12345678901", null)]
    [InlineData("job_events_20261002", "jobs")]
    [InlineData("job_events_20261002_r1", null)]
    public void Ignores_foreign_tables(string name, string? parent) =>
        Assert.Null(PartitionCatalog.Classify(42, name, parent));

    [Fact]
    public void Table_and_kind_helpers()
    {
        Assert.Equal("jobs", new CatalogTable(1, "x", CatalogKind.CompactionRemnant, Day).Table);
        Assert.True(new CatalogTable(1, "x", CatalogKind.DetachedJobs, Day).IsJobs);
        Assert.Equal("job_events", new CatalogTable(1, "x", CatalogKind.DetachedEvents, Day).Table);
    }

    [Theory]
    [InlineData(7 * 24 * 60, 7)]
    [InlineData(36 * 60, 2)]
    [InlineData(1, 1)]
    [InlineData(24 * 60, 1)]
    [InlineData(24 * 60 + 1, 2)]
    public void Retention_rounds_up_to_days(int minutes, int days) =>
        Assert.Equal(days, PartitionCatalog.RetentionDays(TimeSpan.FromMinutes(minutes)));

    [Fact]
    public void Horizon_and_overdue_follow_spec_example()
    {
        var today = new DateOnly(2026, 10, 10);
        var horizon = PartitionCatalog.Horizon(today, TimeSpan.FromDays(7));
        Assert.Equal(new DateOnly(2026, 10, 2), horizon);           // §5.2: Rd = 7, today = 2026-10-10 → D ≤ 2026-10-02
        Assert.Equal(0, PartitionCatalog.Overdue(horizon!.Value, new DateOnly(2026, 10, 2)));
        Assert.Equal(3, PartitionCatalog.Overdue(horizon.Value, new DateOnly(2026, 9, 29)));
        Assert.Null(PartitionCatalog.Horizon(today, null));
        Assert.Equal(new DateOnly(2026, 10, 7), PartitionCatalog.Horizon(today, TimeSpan.FromHours(36)));
        Assert.Equal(new DateOnly(2016, 10, 11), PartitionCatalog.Horizon(today, TimeSpan.FromDays(3650)));
    }

    [Fact]
    public void Tuning_defaults_match_spec()
    {
        Assert.Equal(new RetentionTuning(10, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1)), RetentionTuning.Default);
        Assert.Null(RetentionTuning.Default.Hook);
        Assert.Null(RetentionTuning.Default.BatchBTestPrefix);
    }
}

public abstract class PartitionCatalogTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, partitionDaysAhead: 1);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Lists_partitions_detached_tables_and_remnants_of_this_schema_only()
    {
        var (today, _) = await PartitionCatalog.ListAsync(_db.DataSource, _db.Schema, Ct);
        var d = today.AddDays(1).ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        await _db.ExecAsync($"ALTER TABLE {{schema}}.jobs DETACH PARTITION {{schema}}.jobs_{d}");
        await _db.ExecAsync($"CREATE TABLE {{schema}}.jobs_{d}_r777 (LIKE {{schema}}.jobs)");
        await _db.ExecAsync("CREATE TABLE {schema}.jobs_2026101 (LIKE {schema}.jobs)");
        await using var other = await TestDb.CreateAsync(pg, mode, partitionDaysAhead: 1);   // Review Focus 4

        var (listedToday, tables) = await PartitionCatalog.ListAsync(_db.DataSource, _db.Schema, Ct);

        Assert.Equal(today, listedToday);
        var t = today.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        Assert.Equal(
            new[]
            {
                ($"job_events_{t}", CatalogKind.EventsPartition), ($"job_events_{d}", CatalogKind.EventsPartition),
                ($"jobs_{t}", CatalogKind.JobsPartition), ($"jobs_{d}", CatalogKind.DetachedJobs),
                ($"jobs_{d}_r777", CatalogKind.CompactionRemnant),
            },
            tables.Select(x => (x.Name, x.Kind)).OrderBy(x => x.Name, StringComparer.Ordinal));
        Assert.All(tables, x => Assert.True(x.Oid > 0));
    }
}

public sealed class PartitionCatalogTests_Direct(PostgresFixture pg) : PartitionCatalogTests(pg, ConnectionMode.Direct);
public sealed class PartitionCatalogTests_Bouncer(PostgresFixture pg) : PartitionCatalogTests(pg, ConnectionMode.Bouncer);
```

Если тест запускается в последнюю секунду суток UTC, `today` первого и второго списка может отличаться — тест не содержит ожидания между ними, поэтому риск принят (как в `PartitionTests`).

- [x] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~PartitionCatalog"`
Expected: FAIL (компиляция).

- [x] **Step 3: `RetentionResult.cs`** (весь файл)

```csharp
namespace PgBolt.Postgres.Schema;

/// <summary>Результат запуска retention (часть 2 §5.1). Core не пишет логов: предупреждения строит вызывающий по этому результату.</summary>
/// <param name="DroppedJobsPartitions">Сутки удалённых целиком партиций jobs, включая отсоединённые прошлыми запусками.</param>
/// <param name="DroppedEventsPartitions">Сутки удалённых партиций job_events, включая отсоединённые прошлыми запусками.</param>
/// <param name="DroppedCompactionRemnants">Удалённые остатки компактирования прошлых запусков (без остатков этого запуска).</param>
/// <param name="Pinned">Партиции jobs, закреплённые живыми задачами (не больше RetentionCompactionMaxLiveJobs).</param>
/// <param name="Deferred">Отложенные партиции и таблицы.</param>
/// <param name="AttachedJobsPartitions">Присоединённых партиций jobs после запуска (§5.3, «Число партиций»).</param>
public sealed record RetentionResult(
    IReadOnlyList<DateOnly> DroppedJobsPartitions,
    IReadOnlyList<DateOnly> DroppedEventsPartitions,
    int DroppedCompactionRemnants,
    IReadOnlyList<PinnedPartition> Pinned,
    IReadOnlyList<DeferredPartition> Deferred,
    int AttachedJobsPartitions);

/// <param name="OverdueDays">На сколько суток кандидат просрочен (0 — стал кандидатом в текущие сутки).</param>
/// <param name="LiveJobs">Число живых задач по предпроверке.</param>
/// <param name="Compacted">true — сжата в этом запуске; false — терминальных строк меньше max(LiveJobs, 100).</param>
public sealed record PinnedPartition(DateOnly Day, int OverdueDays, int LiveJobs, bool Compacted);

/// <param name="Table">"jobs" или "job_events".</param>
/// <param name="LiveJobs">Только для LiveJobs и Replicated: живых задач по предпроверке с LIMIT T + 1 (T + 1 — «больше T»).</param>
/// <param name="Error">Текст исключения — только для DropFailed.</param>
public sealed record DeferredPartition(string Table, DateOnly Day, DeferReason Reason, int OverdueDays, int? LiveJobs,
    string? Error = null);

/// <summary>Причины отложения (часть 2 §5.1).</summary>
public enum DeferReason
{
    /// <summary>Блокировка не получена за RetentionLockTimeout; партиция не изменилась.</summary>
    LockTimeout,
    /// <summary>Под блокировкой родителя сработал statement_timeout или idle_in_transaction_session_timeout; откат.</summary>
    HoldTimeout,
    /// <summary>Таблица отсоединена, DROP не получил блокировку; удалится следующим запуском как остаток.</summary>
    DropLockTimeout,
    /// <summary>DROP отсоединённой таблицы упал с другой ошибкой; удалится следующим запуском как остаток.</summary>
    DropFailed,
    /// <summary>Живых задач больше RetentionCompactionMaxLiveJobs (или компактирование выключено).</summary>
    LiveJobs,
    /// <summary>jobs входит в публикацию логической репликации; компактирование пропущено.</summary>
    Replicated,
}
```

- [x] **Step 4: `RetentionTuning.cs`** (весь файл)

```csharp
namespace PgBolt.Postgres.Schema;

/// <summary>Точки тестовых хуков retention (§12): после списка, предпроверки, pg_locks, прогрева, пакетов А/Б и COMMIT транзакции 1.</summary>
internal enum RetentionStep { Listed, Prechecked, LocksChecked, WarmedUp, BatchA, BatchB, Committed }

/// <param name="Parent">"jobs", "job_events" или "" (Listed).</param>
/// <param name="Days">Сутки партиций шага (группы, компактируемой партиции или всех предпроверенных).</param>
internal sealed record RetentionHookContext(RetentionStep Step, string Parent, IReadOnlyList<DateOnly> Days);

/// <summary>
/// Внутренние параметры retention (§5.3): K — партиций в группе отсоединения, потолки удержания блокировки родителя.
/// Тесты создают свой экземпляр — глобального изменяемого состояния нет.
/// </summary>
internal sealed record RetentionTuning(int PartitionsPerTransaction, TimeSpan HoldStatementTimeout, TimeSpan HoldIdleTimeout)
{
    public static RetentionTuning Default { get; } = new(10, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1));

    /// <summary>Только для тестов: вызывается в точках <see cref="RetentionStep"/>; исключение прерывает запуск.</summary>
    public Func<RetentionHookContext, CancellationToken, Task>? Hook { get; init; }

    /// <summary>Только для тестов: команда, добавляемая первой в пакет Б (например, pg_sleep для statement_timeout).</summary>
    public string? BatchBTestPrefix { get; init; }
}
```

- [x] **Step 5: `PartitionCatalog.cs`** (весь файл)

```csharp
using System.Globalization;
using System.Text.RegularExpressions;
using Npgsql;

namespace PgBolt.Postgres.Schema;

internal enum CatalogKind { JobsPartition, EventsPartition, DetachedJobs, DetachedEvents, CompactionRemnant }

/// <param name="Oid">oid таблицы — её идентичность на весь запуск (§5.2).</param>
internal sealed record CatalogTable(long Oid, string Name, CatalogKind Kind, DateOnly Day)
{
    public bool IsJobs => Kind is CatalogKind.JobsPartition or CatalogKind.DetachedJobs or CatalogKind.CompactionRemnant;

    public string Table => IsJobs ? "jobs" : "job_events";
}

/// <summary>Таблицы схемы, которые создаёт PgBolt, и сутки retention (часть 2 §5.2, §3).</summary>
internal static partial class PartitionCatalog
{
    [GeneratedRegex("^jobs_([0-9]{8})(?:_r([0-9]{1,10}))?$", RegexOptions.CultureInvariant)]
    private static partial Regex JobsName();

    [GeneratedRegex("^job_events_([0-9]{8})$", RegexOptions.CultureInvariant)]
    private static partial Regex EventsName();

    /// <summary>§5.2, таблица имён. null — не наша таблица (чужой родитель, присоединённый остаток, другое имя).</summary>
    public static CatalogTable? Classify(long oid, string name, string? parent)
    {
        var jobs = JobsName().Match(name);
        if (jobs.Success)
        {
            if (!TryDay(jobs.Groups[1].Value, out var day))
                return null;
            if (jobs.Groups[2].Success)
                return parent is null ? new CatalogTable(oid, name, CatalogKind.CompactionRemnant, day) : null;
            return parent switch
            {
                "jobs" => new CatalogTable(oid, name, CatalogKind.JobsPartition, day),
                null => new CatalogTable(oid, name, CatalogKind.DetachedJobs, day),
                _ => null,
            };
        }

        var events = EventsName().Match(name);
        if (!events.Success || !TryDay(events.Groups[1].Value, out var eventsDay))
            return null;
        return parent switch
        {
            "job_events" => new CatalogTable(oid, name, CatalogKind.EventsPartition, eventsDay),
            null => new CatalogTable(oid, name, CatalogKind.DetachedEvents, eventsDay),
            _ => null,
        };
    }

    /// <summary>Rd = ceil(retention / 1 сутки), минимум 1.</summary>
    public static int RetentionDays(TimeSpan retention) =>
        checked((int)((retention.Ticks + TimeSpan.TicksPerDay - 1) / TimeSpan.TicksPerDay));

    /// <summary>§5.2: последние сутки-кандидат today − 1 − Rd; null — retention выключен.</summary>
    public static DateOnly? Horizon(DateOnly today, TimeSpan? retention) =>
        retention is { } r ? today.AddDays(-1 - RetentionDays(r)) : null;

    public static int Overdue(DateOnly horizon, DateOnly day) => horizon.DayNumber - day.DayNumber;

    /// <summary>
    /// «Сегодня» (сутки UTC сервера БД, §3) и все таблицы схемы с именами партиций PgBolt — присоединённые и отсоединённые,
    /// одним запросом (§5.2).
    /// </summary>
    public static async Task<(DateOnly Today, List<CatalogTable> Tables)> ListAsync(NpgsqlDataSource dataSource, string schema,
        CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        DateOnly today;
        await using (var cmd = new NpgsqlCommand("SELECT (now() AT TIME ZONE 'UTC')::date", conn))
            today = (DateOnly)(await cmd.ExecuteScalarAsync(ct))!;

        var tables = new List<CatalogTable>();
        await using (var cmd = new NpgsqlCommand($$"""
            SELECT c.oid::bigint, c.relname::text,
                   CASE i.inhparent WHEN '{{schema}}.jobs'::regclass       THEN 'jobs'
                                    WHEN '{{schema}}.job_events'::regclass THEN 'job_events'
                                    ELSE i.inhparent::regclass::text END
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
            WHERE n.nspname = $1 AND c.relkind = 'r'
              AND c.relname ~ '^(jobs_[0-9]{8}(_r[0-9]+)?|job_events_[0-9]{8})$'
            """, conn))
        {
            cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schema });
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                if (Classify(reader.GetInt64(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)) is { } table)
                    tables.Add(table);
        }
        return (today, tables);
    }

    private static bool TryDay(string text, out DateOnly day) =>
        DateOnly.TryParseExact(text, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out day);
}
```

`schema` интерполируется в текст только после `SchemaName.Validate` у вызывающего (`RetentionRunner`); в тестах — имя `TestDb`.

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~PartitionCatalog"`
Expected: PASS.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Schema/RetentionResult.cs src/PgBolt.Postgres/Schema/RetentionTuning.cs src/PgBolt.Postgres/Schema/PartitionCatalog.cs tests/PgBolt.Postgres.Tests/PartitionCatalogTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(core): add retention result types and partition catalog"
```

---

### Task 6: `RetentionRunner` — транзакции 1 и 2, остатки, `job_events`

Спека: §5.3 п. 3, 5, 6 и «Обработка ошибок», §5.4, §5.5, §5.1 (`ApplyRetentionAsync`, `AttachedJobsPartitions`), §12.

**Files:**
- Create: `src/PgBolt.Postgres/Schema/RetentionRunner.cs`
- Modify: `src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs`, `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`
- Test: `tests/PgBolt.Postgres.Tests/RetentionTestBase.cs`, `tests/PgBolt.Postgres.Tests/RetentionEventsTests.cs` (создать)

**Interfaces:**
- Consumes: `PartitionCatalog`, `RetentionTuning`, `RetentionResult` (Task 5); `PgBoltOptions` retention-настройки (Task 2).
- Produces: `RetentionRunner` с `ApplyAsync`; внутренние члены для Task 7: `Candidate`, `Accumulator`, `LockedOutcome`, `RunLockedAsync`, `DropAsync`, `ExcludeHeldAsync`, `DetachGroupAsync`, `HookAsync`, `Cmd`, `Arg`, `Qualified`, `LockTimeoutSql`, `SafeRollbackAsync`; `IPgBoltMaintenance.ApplyRetentionAsync`.

Этап `jobs` (§5.3 п. 1–2, компактирование) добавляет Task 7; в этой задаче `ApplyAsync` обрабатывает остатки, `job_events` и число присоединённых партиций.

- [x] **Step 1: База тестов retention**

`tests/PgBolt.Postgres.Tests/RetentionTestBase.cs` (весь файл):

```csharp
using System.Collections.Concurrent;
using System.Globalization;
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>
/// Прошлые партиции и строки задач для тестов retention (часть 2 §12). today берётся из БД (сутки UTC); партиции создаются
/// тем же DDL, что PartitionManager. TestDb уже содержит партиции на сегодня и два дня вперёд.
/// </summary>
public abstract class RetentionTestBase(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    protected DateOnly Today { get; private set; }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Today = await Db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");
    }

    protected DateOnly Ago(int days) => Today.AddDays(-days);

    protected static string Suffix(DateOnly day) => day.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
    protected static string Iso(DateOnly day) => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    protected static string J(DateOnly day) => $"jobs_{Suffix(day)}";
    protected static string E(DateOnly day) => $"job_events_{Suffix(day)}";

    protected static Guid IdOn(DateOnly day) =>
        Guid.CreateVersion7(new DateTimeOffset(day.ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero));

    protected async Task CreatePartitionsAsync(DateOnly day, bool jobs = true, bool events = true)
    {
        var lo = UuidV7.LowerBound(day);
        var hi = UuidV7.LowerBound(day.AddDays(1));
        if (jobs)
            await Db.ExecAsync($"""
                CREATE TABLE {{schema}}.{J(day)} (LIKE {{schema}}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 80);
                ALTER TABLE {{schema}}.jobs ATTACH PARTITION {{schema}}.{J(day)} FOR VALUES FROM ('{lo}') TO ('{hi}');
                """);
        if (events)
            await Db.ExecAsync($"""
                CREATE TABLE {{schema}}.{E(day)} (LIKE {{schema}}.job_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
                ALTER TABLE {{schema}}.job_events ATTACH PARTITION {{schema}}.{E(day)}
                    FOR VALUES FROM ('{Iso(day)} 00:00:00+00') TO ('{Iso(day.AddDays(1))} 00:00:00+00');
                """);
    }

    /// <summary>Терминальные строки суток (state 2 — Succeeded, 3 — Failed, 4 — Cancelled), без строк queue.</summary>
    protected async Task<Guid[]> InsertTerminalAsync(DateOnly day, int count, short state = 2, string args = "{}")
    {
        var ids = Enumerable.Range(0, count).Select(_ => IdOn(day)).ToArray();
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at)
            SELECT id, 'test.job', 'q', 0, @args::jsonb, @state, 1, 3, now(), now() FROM unnest(@ids) AS id
            """, ("ids", ids), ("args", args), ("state", state));
        return ids;
    }

    /// <summary>Живые (Pending) задачи суток — обычным enqueue: строки jobs и queue.</summary>
    protected async Task<Guid[]> EnqueueLiveAsync(DateOnly day, int count, string queue = "q", string args = """{"n":1}""")
    {
        var ids = Enumerable.Range(0, count).Select(_ => IdOn(day)).ToArray();
        await Store.EnqueueAsync(new EnqueueBatch("test.job", queue, 0, 3, null, null, TimeSpan.Zero, ids,
            [.. ids.Select(_ => args)], new DateTime?[count], new string?[count]), null, Ct);
        return ids;
    }

    private protected RetentionRunner Runner(Action<PgBoltOptions>? configure = null, RetentionTuning? tuning = null,
        NpgsqlDataSource? dataSource = null)
    {
        var options = new PgBoltOptions { ConnectionString = "Host=unused", Schema = Db.Schema, JobsRetention = null, EventsRetention = null };
        configure?.Invoke(options);
        return new RetentionRunner(dataSource ?? Db.DataSource, options, tuning ?? RetentionTuning.Default);
    }

    protected Task<bool> ExistsAsync(string table) =>
        Db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{Db.Schema}.{table}"));

    protected Task<bool> AttachedAsync(string table) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass(@t))", ("t", $"{Db.Schema}.{table}"));

    protected Task<long> OidAsync(string table) =>
        Db.ScalarAsync<long>("SELECT to_regclass(@t)::oid::bigint", ("t", $"{Db.Schema}.{table}"));

    protected Task<long> CountAsync(string table) => Db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.{table}");

    protected Task DetachAsync(string parent, string table) =>
        Db.ExecAsync($"ALTER TABLE {{schema}}.{parent} DETACH PARTITION {{schema}}.{table}");

    /// <summary>
    /// Ждёт, пока не меньше count сессий встанут в очередь блокировки таблицы (pg_locks, granted = false). Проверка oid
    /// в транзакции 2 не блокирует, поэтому ожидающая сессия — это DROP, уже прошедший проверку.
    /// </summary>
    protected async Task WaitForLockWaitersAsync(string table, int count)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (await Db.ScalarAsync<long>("SELECT count(*) FROM pg_locks WHERE relation = to_regclass(@t) AND NOT granted",
                   ("t", $"{Db.Schema}.{table}")) < count)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Expected {count} lock waiters on {table}.");
            await Task.Delay(20, Ct);
        }
    }

    /// <summary>Держит блокировку в открытой транзакции; Dispose закрывает соединение и откатывает её.</summary>
    protected async Task<NpgsqlConnection> HoldAsync(string table, string lockMode)
    {
        var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var cmd = new NpgsqlCommand(Db.Sql($"BEGIN; LOCK TABLE {{schema}}.{table} IN {lockMode} MODE"), conn);
        await cmd.ExecuteNonQueryAsync(Ct);
        return conn;
    }
}

/// <summary>Записывает вызовы хуков RetentionRunner и выполняет Then.</summary>
internal sealed class HookRecorder
{
    public ConcurrentQueue<RetentionHookContext> Calls { get; } = new();

    public Func<RetentionHookContext, CancellationToken, Task>? Then { get; set; }

    public async Task Hook(RetentionHookContext context, CancellationToken ct)
    {
        Calls.Enqueue(context);
        if (Then is { } then)
            await then(context, ct);
    }

    public int Count(RetentionStep step, string parent) => Calls.Count(c => c.Step == step && c.Parent == parent);

    public RetentionTuning Tuning(int k = 10, TimeSpan? holdStatement = null, TimeSpan? holdIdle = null, string? batchBPrefix = null) =>
        new(k, holdStatement ?? TimeSpan.FromSeconds(2), holdIdle ?? TimeSpan.FromSeconds(1)) { Hook = Hook, BatchBTestPrefix = batchBPrefix };
}
```

`Runner` по умолчанию выключает оба срока: каждый тест включает нужный явно.

- [x] **Step 2: Тесты `job_events`, остатков и транзакций**

`tests/PgBolt.Postgres.Tests/RetentionEventsTests.cs` (весь файл):

```csharp
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionEventsTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    [Fact]
    public async Task Fresh_schema_gives_empty_result()   // Review Focus 1
    {
        var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }).ApplyAsync(Ct);

        Assert.Empty(r.DroppedJobsPartitions);
        Assert.Empty(r.DroppedEventsPartitions);
        Assert.Empty(r.Pinned);
        Assert.Empty(r.Deferred);
        Assert.Equal(0, r.DroppedCompactionRemnants);
        Assert.Equal(3, r.AttachedJobsPartitions);   // сегодня + 2
    }

    [Fact]
    public async Task Events_candidates_follow_day_boundary()
    {
        foreach (var d in new[] { 5, 4, 3, 1 })
            await CreatePartitionsAsync(Ago(d), jobs: false);

        var r = await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct);   // horizon = today − 4

        Assert.Equal([Ago(5), Ago(4)], r.DroppedEventsPartitions);
        Assert.False(await ExistsAsync(E(Ago(5))));
        Assert.False(await ExistsAsync(E(Ago(4))));
        Assert.True(await AttachedAsync(E(Ago(3))));
        Assert.True(await AttachedAsync(E(Ago(1))));
        Assert.True(await AttachedAsync(E(Today)));
        Assert.True(await AttachedAsync(E(Today.AddDays(2))));
    }

    [Fact]
    public async Task Events_retention_rounds_up_and_null_keeps_everything()
    {
        await CreatePartitionsAsync(Ago(3), jobs: false);
        await CreatePartitionsAsync(Ago(2), jobs: false);

        Assert.Empty((await Runner().ApplyAsync(Ct)).DroppedEventsPartitions);
        var r = await Runner(o => o.EventsRetention = TimeSpan.FromHours(36)).ApplyAsync(Ct);   // Rd = 2 → horizon = today − 3

        Assert.Equal([Ago(3)], r.DroppedEventsPartitions);
        Assert.True(await AttachedAsync(E(Ago(2))));
    }

    [Fact]
    public async Task Remnants_are_dropped_with_any_retention()
    {
        var recent = Ago(1);
        await CreatePartitionsAsync(recent);
        await DetachAsync("jobs", J(recent));
        await DetachAsync("job_events", E(recent));
        await Db.ExecAsync($"CREATE TABLE {{schema}}.{J(Ago(2))}_r4242 (LIKE {{schema}}.jobs)");

        var r = await Runner().ApplyAsync(Ct);   // оба срока null

        Assert.Equal([recent], r.DroppedJobsPartitions);
        Assert.Equal([recent], r.DroppedEventsPartitions);
        Assert.Equal(1, r.DroppedCompactionRemnants);
        Assert.False(await ExistsAsync(J(recent)));
        Assert.False(await ExistsAsync(E(recent)));
        Assert.False(await ExistsAsync($"{J(Ago(2))}_r4242"));
    }

    [Fact]
    public async Task Locked_remnant_is_deferred_and_others_are_dropped()
    {
        await CreatePartitionsAsync(Ago(1));
        await DetachAsync("jobs", J(Ago(1)));
        await DetachAsync("job_events", E(Ago(1)));
        await using (var holder = await HoldAsync(J(Ago(1)), "ACCESS SHARE"))
        {
            var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

            var deferred = Assert.Single(r.Deferred);
            Assert.Equal(new DeferredPartition("jobs", Ago(1), DeferReason.DropLockTimeout, 0, null), deferred);
            Assert.Equal([Ago(1)], r.DroppedEventsPartitions);
            await EnqueueAsync();   // горячий путь не затронут: родитель не блокируется
        }

        Assert.Equal([Ago(1)], (await Runner().ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Concurrent_drops_of_one_detached_table_count_it_once()   // §5.5: перекрытие в транзакции 2
    {
        await CreatePartitionsAsync(Ago(1), events: false);
        await DetachAsync("jobs", J(Ago(1)));
        RetentionRunner Run() => Runner(o => o.RetentionLockTimeout = TimeSpan.FromSeconds(30));
        Task<RetentionResult> first, second;
        var holder = await HoldAsync(J(Ago(1)), "ACCESS SHARE");
        try
        {
            first = Run().ApplyAsync(Ct);
            second = Run().ApplyAsync(Ct);
            await WaitForLockWaitersAsync(J(Ago(1)), 2);   // оба запуска прошли проверку oid, оба DROP ждут блокировку
        }
        finally
        {
            await holder.DisposeAsync();
        }

        var results = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(10), Ct);

        // Первый DROP удаляет таблицу, второй после ожидания получает 42P01 → Skipped.
        Assert.Equal(new[] { Ago(1) }, results.SelectMany(r => r.DroppedJobsPartitions));
        Assert.All(results, r => Assert.Empty(r.Deferred));
        Assert.False(await ExistsAsync(J(Ago(1))));
    }

    [Fact]
    public async Task Cancellation_while_drop_waits_for_lock_throws_instead_of_drop_failed()   // §12
    {
        await CreatePartitionsAsync(Ago(1), events: false);
        await DetachAsync("jobs", J(Ago(1)));
        await using (var holder = await HoldAsync(J(Ago(1)), "ACCESS SHARE"))
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = Runner(o => o.RetentionLockTimeout = TimeSpan.FromSeconds(30)).ApplyAsync(cts.Token);
            await WaitForLockWaitersAsync(J(Ago(1)), 1);   // DROP транзакции 2 ждёт блокировку, до lock_timeout ещё 30 с
            await cts.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(10), Ct));
            Assert.True(await ExistsAsync(J(Ago(1))));
        }

        Assert.Equal([Ago(1)], (await Runner().ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Autovacuum_like_lock_on_todays_partition_does_not_block_removal()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await using var holder = await HoldAsync(E(Today), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal([Ago(10)], r.DroppedEventsPartitions);
        Assert.Empty(r.Deferred);
    }

    [Fact]
    public async Task Direct_holder_of_old_partition_is_deferred_before_lock()
    {
        await CreatePartitionsAsync(Ago(11), jobs: false);
        await CreatePartitionsAsync(Ago(10), jobs: false);
        var hooks = new HookRecorder();
        await using var holder = await HoldAsync(E(Ago(11)), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.EventsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("job_events", Ago(11), DeferReason.LockTimeout, 7, null), Assert.Single(r.Deferred));
        Assert.Equal([Ago(10)], r.DroppedEventsPartitions);
        var locksChecked = Assert.Single(hooks.Calls, c => c.Step == RetentionStep.LocksChecked);
        Assert.Equal([Ago(10)], locksChecked.Days);
    }

    [Fact]
    public async Task Lock_timeout_on_parent_defers_group_and_stops_events()
    {
        foreach (var d in new[] { 12, 11, 10 })
            await CreatePartitionsAsync(Ago(d), jobs: false);
        await using var holder = await HoldAsync("job_events", "ACCESS SHARE");

        var r = await Runner(o => o.EventsRetention = Days3, new HookRecorder().Tuning(k: 2)).ApplyAsync(Ct);

        Assert.Equal(new[] { Ago(12), Ago(11) }, r.Deferred.Select(d => d.Day));
        Assert.All(r.Deferred, d => Assert.Equal(DeferReason.LockTimeout, d.Reason));
        Assert.Empty(r.DroppedEventsPartitions);
        Assert.True(await AttachedAsync(E(Ago(10))));   // не тронута и не попала в результат
    }

    [Theory]
    [InlineData("BatchA")]
    [InlineData("BatchB")]
    public async Task Idle_pause_under_lock_is_cut_by_hold_timeout(string pauseAfter)   // RetentionStep — internal
    {
        var step = Enum.Parse<RetentionStep>(pauseAfter);
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await CreatePartitionsAsync(Ago(9), jobs: false);
        var hooks = new HookRecorder
        {
            Then = (c, ct) => c.Step == step ? Task.Delay(TimeSpan.FromSeconds(1.5), ct) : Task.CompletedTask,
        };

        var r = await Runner(o => o.EventsRetention = Days3, hooks.Tuning(k: 1, holdIdle: TimeSpan.FromMilliseconds(300))).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("job_events", Ago(10), DeferReason.HoldTimeout, 6, null), Assert.Single(r.Deferred));
        Assert.True(await AttachedAsync(E(Ago(10))));
        Assert.True(await AttachedAsync(E(Ago(9))));   // остановка после HoldTimeout
        await EnqueueAsync();                          // горячий путь работает

        Assert.Equal([Ago(10), Ago(9)], (await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct)).DroppedEventsPartitions);
    }

    [Fact]
    public async Task Statement_under_lock_is_cut_by_statement_timeout()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        var tuning = new HookRecorder().Tuning(holdStatement: TimeSpan.FromMilliseconds(100), batchBPrefix: "SELECT pg_sleep(2)");

        var r = await Runner(o => o.EventsRetention = Days3, tuning).ApplyAsync(Ct);

        Assert.Equal(DeferReason.HoldTimeout, Assert.Single(r.Deferred).Reason);
        Assert.True(await AttachedAsync(E(Ago(10))));
    }

    [Fact]
    public async Task Cancellation_between_batches_rolls_back_and_throws()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var hooks = new HookRecorder { Then = (c, _) => c.Step == RetentionStep.BatchA ? cts.CancelAsync() : Task.CompletedTask };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Runner(o => o.EventsRetention = Days3, hooks.Tuning()).ApplyAsync(cts.Token));

        Assert.True(await AttachedAsync(E(Ago(10))));
        await EnqueueAsync().WaitAsync(TimeSpan.FromSeconds(2), Ct);   // блокировка родителя снята
        Assert.Equal([Ago(10)], (await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct)).DroppedEventsPartitions);
    }

    [Fact]
    public async Task Groups_of_k_partitions_share_one_transaction()
    {
        for (var d = 30; d > 5; d--)
            await CreatePartitionsAsync(Ago(d), jobs: false);
        var hooks = new HookRecorder();

        var r = await Runner(o => o.EventsRetention = Days3, hooks.Tuning(k: 10)).ApplyAsync(Ct);

        Assert.Equal(25, r.DroppedEventsPartitions.Count);
        Assert.Equal(3, hooks.Count(RetentionStep.BatchA, "job_events"));
        Assert.Equal(3, hooks.Count(RetentionStep.Committed, "job_events"));
    }

    [Fact]
    public async Task Attached_jobs_partitions_are_counted_after_the_run()
    {
        await CreatePartitionsAsync(Ago(10));
        await CreatePartitionsAsync(Ago(1));

        Assert.Equal(5, (await Runner().ApplyAsync(Ct)).AttachedJobsPartitions);
    }
}

public sealed class RetentionEventsTests_Direct(PostgresFixture pg) : RetentionEventsTests(pg, ConnectionMode.Direct);
public sealed class RetentionEventsTests_Bouncer(PostgresFixture pg) : RetentionEventsTests(pg, ConnectionMode.Bouncer);

/// <summary>
/// Event trigger — глобальное состояние БД: он срабатывает на DROP любого параллельного теста. Поэтому тест вынесен
/// в непараллельную коллекцию "Serial" (определена в HotUpdateTests.cs: DisableParallelization, запуск после всех
/// параллельных коллекций).
/// </summary>
public abstract class RetentionDropFailureTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    [Fact]
    public async Task Failed_drop_is_deferred_as_drop_failed_and_retried_as_remnant()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await CreatePartitionsAsync(Ago(9), jobs: false);
        var trigger = $"evt_{Db.Schema}";
        await Db.ExecAsync($$"""
            CREATE FUNCTION {schema}.forbid_drop() RETURNS event_trigger LANGUAGE plpgsql AS $f$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects()
                           WHERE object_identity = '{schema}.{{E(Ago(10))}}') THEN
                    RAISE EXCEPTION 'drop forbidden by test';
                END IF;
            END $f$;
            CREATE EVENT TRIGGER {{trigger}} ON sql_drop EXECUTE FUNCTION {schema}.forbid_drop();
            """);
        try
        {
            var r = await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct);

            var failed = Assert.Single(r.Deferred);
            Assert.Equal(("job_events", Ago(10), DeferReason.DropFailed), (failed.Table, failed.Day, failed.Reason));
            Assert.Contains("drop forbidden by test", failed.Error);
            Assert.Equal([Ago(9)], r.DroppedEventsPartitions);
            Assert.False(await AttachedAsync(E(Ago(10))));
        }
        finally
        {
            await Db.ExecAsync($"DROP EVENT TRIGGER IF EXISTS {trigger}");
        }

        Assert.Equal([Ago(10)], (await Runner().ApplyAsync(Ct)).DroppedEventsPartitions);
    }
}

[Collection("Serial")]
public sealed class RetentionDropFailureTests_Direct(PostgresFixture pg) : RetentionDropFailureTests(pg, ConnectionMode.Direct);

[Collection("Serial")]
public sealed class RetentionDropFailureTests_Bouncer(PostgresFixture pg) : RetentionDropFailureTests(pg, ConnectionMode.Bouncer);

/// <summary>Только Direct: пул из одного соединения без сброса — проверяется, что SET LOCAL не остаётся на соединении.</summary>
public sealed class RetentionSessionStateTests(PostgresFixture pg) : RetentionTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Set_local_does_not_leak_to_the_pooled_connection()
    {
        await CreatePartitionsAsync(Ago(10), jobs: false);
        await CreatePartitionsAsync(Ago(9), jobs: false);
        var cs = new NpgsqlConnectionStringBuilder(Pg.DirectConnectionString) { MaxPoolSize = 1, NoResetOnClose = true }.ConnectionString;
        await using var single = NpgsqlDataSource.Create(cs);
        var hooks = new HookRecorder
        {
            // вторая группа откатывается без пакета Б: её партицию отсоединили до транзакции 1
            Then = (c, _) => c.Step == RetentionStep.LocksChecked && c.Days[0] == Ago(9)
                ? DetachAsync("job_events", E(Ago(9)))
                : Task.CompletedTask,
        };

        await Runner(o => o.EventsRetention = TimeSpan.FromDays(3), hooks.Tuning(k: 1), single).ApplyAsync(Ct);

        Assert.Equal(1, hooks.Count(RetentionStep.BatchB, "job_events"));   // пакет Б только у первой группы
        await using var cmd = single.CreateCommand(
            "SELECT current_setting('lock_timeout') || '|' || current_setting('statement_timeout') || '|' || current_setting('idle_in_transaction_session_timeout')");
        Assert.Equal("0|0|0", (string)(await cmd.ExecuteScalarAsync(Ct))!);
    }
}
```

Оба теста на ожидание DROP синхронизируются не задержками, а состоянием сервера: `WaitForLockWaitersAsync` ждёт нужного числа ожидающих блокировку сессий в `pg_locks`, а `RetentionLockTimeout = 30 с` гарантирует, что за время теста DROP не выйдет по `lock_timeout`. В `Concurrent_drops_…` блокировка снимается только после того, как оба запуска прошли проверку oid, поэтому второй DROP гарантированно встречает уже удалённую таблицу. Если `WaitForLockWaitersAsync` падает по таймауту, `finally` снимает блокировку, и запуски завершаются сами.

Коллекции `RetentionResult` сравниваются поэлементно (`Assert.Equal` по последовательности), а не равенством record: у `IReadOnlyList` оно ссылочное.

- [x] **Step 3: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RetentionEventsTests|FullyQualifiedName~RetentionDropFailureTests|FullyQualifiedName~RetentionSessionStateTests"`
Expected: FAIL (компиляция: нет `RetentionRunner`).

- [x] **Step 4: `RetentionRunner.cs`** (весь файл)

```csharp
using System.Data;
using System.Globalization;
using Npgsql;

namespace PgBolt.Postgres.Schema;

/// <summary>
/// Retention jobs/job_events (часть 2 §5.2–§5.5): остатки прошлых запусков, отсоединение группами под LOCK TABLE ONLY
/// родителя с таймаутами удержания (транзакция 1), компактирование закреплённых партиций jobs (RetentionRunner.Jobs.cs),
/// DROP отдельной транзакцией на таблицу только по тому же oid (транзакция 2).
/// </summary>
internal sealed partial class RetentionRunner
{
    internal const int MinCompactionGain = 100;
    private const string IdleInTransactionTimeout = "25P03";

    private readonly NpgsqlDataSource _dataSource;
    private readonly PgBoltOptions _options;
    private readonly RetentionTuning _tuning;
    private readonly string _schema;

    public RetentionRunner(NpgsqlDataSource dataSource, PgBoltOptions options, RetentionTuning tuning)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _tuning = tuning ?? throw new ArgumentNullException(nameof(tuning));
        _schema = SchemaName.Validate(options.Schema);
    }

    private int MaxLive => _options.RetentionCompactionMaxLiveJobs;

    private string LockTimeoutSql => $"SET LOCAL lock_timeout = '{Ms(_options.RetentionLockTimeout)}ms'";

    /// <summary>§5.5: остатки → jobs → job_events → число присоединённых партиций jobs.</summary>
    public async Task<RetentionResult> ApplyAsync(CancellationToken ct = default)
    {
        var (today, tables) = await PartitionCatalog.ListAsync(_dataSource, _schema, ct);
        await HookAsync(RetentionStep.Listed, "", [], ct);
        var jobsHorizon = PartitionCatalog.Horizon(today, _options.JobsRetention);
        var eventsHorizon = PartitionCatalog.Horizon(today, _options.EventsRetention);
        var acc = new Accumulator();

        // §5.2: остатки — при любом значении retention, без проверок задач; единственная проверка — в транзакции 2.
        foreach (var t in tables
                     .Where(t => t.Kind is CatalogKind.DetachedJobs or CatalogKind.DetachedEvents or CatalogKind.CompactionRemnant)
                     .OrderBy(t => t.Day))
        {
            var horizon = t.IsJobs ? jobsHorizon : eventsHorizon;
            var overdue = horizon is { } h ? Math.Max(0, PartitionCatalog.Overdue(h, t.Day)) : 0;
            acc.RecordDrop(t, overdue, await DropAsync(t.Name, t.Oid, ct), countRemnant: true);
        }

        if (eventsHorizon is { } eh)
            await ApplyEventsAsync(Candidates(tables, CatalogKind.EventsPartition, eh), acc, ct);

        return acc.ToResult(await CountAttachedJobsAsync(ct));
    }

    // ---- job_events (§5.4) ----------------------------------------------------------------------

    private async Task ApplyEventsAsync(List<Candidate> candidates, Accumulator acc, CancellationToken ct)
    {
        foreach (var chunk in candidates.Chunk(_tuning.PartitionsPerTransaction))
        {
            var group = await ExcludeHeldAsync("job_events", chunk, acc, ct);
            if (group.Count == 0)
                continue;
            var result = await DetachGroupAsync("job_events", group, ct);
            if (result.Outcome is LockedOutcome.LockTimeout or LockedOutcome.HoldTimeout)
            {
                acc.DeferGroup("job_events", group, result.Outcome);
                return;   // §5.4: остальные кандидаты job_events в этом запуске не обрабатываются
            }
            await DropDetachedAsync(result, acc, ct);
        }
    }

    // ---- общие шаги §5.3 п. 3, 5, 6 -------------------------------------------------------------

    private static List<Candidate> Candidates(IEnumerable<CatalogTable> tables, CatalogKind kind, DateOnly horizon) =>
        [.. tables.Where(t => t.Kind == kind && t.Day <= horizon)
                  .OrderBy(t => t.Day)
                  .Select(t => new Candidate(t, PartitionCatalog.Overdue(horizon, t.Day)))];

    /// <summary>§5.3 п. 3: партиции, которые держат в обход родителя, исключаются из группы → Deferred(LockTimeout).</summary>
    private async Task<List<Candidate>> ExcludeHeldAsync(string parent, IReadOnlyList<Candidate> chunk, Accumulator acc, CancellationToken ct)
    {
        var held = await HeldAsync(parent, [.. chunk.Select(c => c.Table.Oid)], ct);
        var group = new List<Candidate>(chunk.Count);
        foreach (var c in chunk)
        {
            if (held.Contains(c.Table.Oid))
                acc.Deferred.Add(new DeferredPartition(parent, c.Day, DeferReason.LockTimeout, c.Overdue, null));
            else
                group.Add(c);
        }
        if (group.Count > 0)
            await HookAsync(RetentionStep.LocksChecked, parent, [.. group.Select(c => c.Day)], ct);
        return group;
    }

    private async Task<HashSet<long>> HeldAsync(string parent, long[] oids, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand($"""
            WITH l AS MATERIALIZED (
                SELECT pid, relation FROM pg_locks
                WHERE locktype = 'relation' AND granted AND pid <> pg_backend_pid()
                  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
            )
            SELECT DISTINCT l.relation::bigint FROM l
            WHERE l.relation = ANY($1::bigint[]::oid[])
              AND NOT EXISTS (SELECT 1 FROM l p WHERE p.pid = l.pid AND p.relation = '{_schema}.{parent}'::regclass)
            """);
        cmd.Parameters.Add(Arg(oids));
        var held = new HashSet<long>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            held.Add(reader.GetInt64(0));
        return held;
    }

    /// <summary>
    /// §5.3 п. 5 / §5.4: транзакция 1 отсоединения группы. has_live (только jobs) — повтор проверки живых под блокировкой;
    /// такая партиция пропускается до следующего запуска и в результат не попадает. Неприсоединённая — в транзакцию 2.
    /// </summary>
    private async Task<GroupResult> DetachGroupAsync(string parent, IReadOnlyList<Candidate> group, CancellationToken ct)
    {
        var jobs = parent == "jobs";
        var check = jobs
            ? Cmd($"""
                SELECT p.oid::bigint,
                       to_regclass(p.rel) = p.oid
                         AND EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p.oid AND i.inhparent = '{_schema}.jobs'::regclass),
                       EXISTS (SELECT 1 FROM {_schema}.queue q WHERE q.job_id >= p.lo AND q.job_id < p.hi)
                FROM unnest($1::bigint[]::oid[], $2::text[], $3::uuid[], $4::uuid[]) AS p(oid, rel, lo, hi)
                """,
                Arg(group.Select(c => c.Table.Oid).ToArray()), Arg(group.Select(c => Qualified(c.Table.Name)).ToArray()),
                Arg(group.Select(c => c.Lo).ToArray()), Arg(group.Select(c => c.Hi).ToArray()))
            : Cmd($"""
                SELECT p.oid::bigint,
                       to_regclass(p.rel) = p.oid
                         AND EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p.oid AND i.inhparent = '{_schema}.job_events'::regclass),
                       false
                FROM unnest($1::bigint[]::oid[], $2::text[]) AS p(oid, rel)
                """,
                Arg(group.Select(c => c.Table.Oid).ToArray()), Arg(group.Select(c => Qualified(c.Table.Name)).ToArray()));

        var byOid = group.ToDictionary(c => c.Table.Oid);
        var detach = new List<Candidate>();
        var notAttached = new List<Candidate>();
        var days = group.Select(c => c.Day).ToArray();
        var outcome = await RunLockedAsync(parent, check, days, async (reader, t) =>
        {
            while (await reader.ReadAsync(t))
            {
                var candidate = byOid[reader.GetInt64(0)];
                var attached = !reader.IsDBNull(1) && reader.GetBoolean(1);
                if (!attached)
                    notAttached.Add(candidate);
                else if (!reader.GetBoolean(2))
                    detach.Add(candidate);
            }
        }, () => [.. detach.Select(c => new NpgsqlBatchCommand($"ALTER TABLE {_schema}.{parent} DETACH PARTITION {Qualified(c.Table.Name)}"))], ct);

        if (outcome == LockedOutcome.Committed)
            await HookAsync(RetentionStep.Committed, parent, days, ct);
        return outcome switch
        {
            LockedOutcome.Committed => new GroupResult(outcome, detach, notAttached),
            LockedOutcome.RolledBack => new GroupResult(outcome, [], notAttached),
            _ => new GroupResult(outcome, [], []),
        };
    }

    private async Task DropDetachedAsync(GroupResult result, Accumulator acc, CancellationToken ct)
    {
        foreach (var c in result.Detached.Concat(result.NotAttached))
            acc.RecordDrop(Detached(c.Table), c.Overdue, await DropAsync(c.Table.Name, c.Table.Oid, ct), countRemnant: true);
    }

    private static CatalogTable Detached(CatalogTable t) =>
        t with { Kind = t.IsJobs ? CatalogKind.DetachedJobs : CatalogKind.DetachedEvents };

    /// <summary>
    /// §5.3 п. 5: транзакция 1 — BeginTransactionAsync, пакет А (lock_timeout, LOCK TABLE ONLY родителя, таймауты удержания,
    /// проверка), пакет Б (DDL) или откат, CommitAsync. Пустой пакет Б — RollbackAsync сразу после пакета А.
    /// </summary>
    private async Task<LockedOutcome> RunLockedAsync(string parent, NpgsqlBatchCommand check, IReadOnlyList<DateOnly> days,
        Func<NpgsqlDataReader, CancellationToken, Task> readCheck, Func<List<NpgsqlBatchCommand>> batchB, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            await using (var a = new NpgsqlBatch(conn, tx))
            {
                a.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                a.BatchCommands.Add(new NpgsqlBatchCommand($"LOCK TABLE ONLY {_schema}.{parent} IN ACCESS EXCLUSIVE MODE"));
                a.BatchCommands.Add(new NpgsqlBatchCommand(
                    $"SET LOCAL statement_timeout = '{Ms(_tuning.HoldStatementTimeout + _options.RetentionLockTimeout)}ms'"));
                a.BatchCommands.Add(new NpgsqlBatchCommand($"SET LOCAL idle_in_transaction_session_timeout = '{Ms(_tuning.HoldIdleTimeout)}ms'"));
                a.BatchCommands.Add(check);
                await using var reader = await ExecuteToRowsAsync(a, ct);
                await readCheck(reader, ct);
            }
            await HookAsync(RetentionStep.BatchA, parent, days, ct);

            var commands = batchB();
            if (commands.Count == 0)
            {
                await tx.RollbackAsync(ct);
                return LockedOutcome.RolledBack;
            }
            await using (var b = new NpgsqlBatch(conn, tx))
            {
                if (_tuning.BatchBTestPrefix is { } prefix)
                    b.BatchCommands.Add(new NpgsqlBatchCommand(prefix));
                foreach (var command in commands)
                    b.BatchCommands.Add(command);
                await b.ExecuteNonQueryAsync(ct);
            }
            await HookAsync(RetentionStep.BatchB, parent, days, ct);
            await tx.CommitAsync(ct);
            return LockedOutcome.Committed;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw;
        }
        catch (NpgsqlException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw new OperationCanceledException(ct);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await SafeRollbackAsync(conn, tx);
            return LockedOutcome.LockTimeout;
        }
        catch (NpgsqlException e) when (IsHoldTimeout(e, conn))
        {
            await SafeRollbackAsync(conn, tx);
            return LockedOutcome.HoldTimeout;
        }
        catch
        {
            await SafeRollbackAsync(conn, tx);
            throw;
        }
    }

    /// <summary>
    /// §5.3 п. 6: транзакция 2 — DROP только если имя указывает на тот же oid и таблица не присоединена. Родитель не блокируется.
    /// </summary>
    /// <remarks>
    /// Перекрытие запусков (§5.5): проверка oid не блокирует, поэтому два пода могут пройти её для одной таблицы; DROP второго
    /// ждёт блокировку, после коммита первого PostgreSQL повторно разрешает имя, не находит таблицу и возвращает 42P01
    /// (DROP TABLE без IF EXISTS). Такой DROP — Skipped, а не Dropped: иначе сутки попадут в
    /// DroppedJobsPartitions/DroppedEventsPartitions обоих подов. Признак — только SQLSTATE ошибки: не зависит от NOTICE,
    /// lc_messages, client_min_messages и event trigger'ов.
    /// </remarks>
    private async Task<DropResult> DropAsync(string name, long oid, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            bool droppable;
            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                batch.BatchCommands.Add(Cmd("""
                    SELECT to_regclass($1) = $2::oid AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = $2::oid)
                    """, Arg(Qualified(name)), Arg(oid)));
                await using var reader = await ExecuteToRowsAsync(batch, ct);
                droppable = await reader.ReadAsync(ct) && !reader.IsDBNull(0) && reader.GetBoolean(0);
            }
            if (!droppable)
            {
                await tx.RollbackAsync(ct);
                return new DropResult(DropStatus.Skipped, null);
            }
            await using (var drop = new NpgsqlCommand($"DROP TABLE {Qualified(name)}", conn, tx))
                await drop.ExecuteNonQueryAsync(ct);
            await tx.CommitAsync(ct);
            return new DropResult(DropStatus.Dropped, null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw;
        }
        catch (NpgsqlException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw new OperationCanceledException(ct);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await SafeRollbackAsync(conn, tx);
            return new DropResult(DropStatus.LockTimeout, null);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            // Таблицу удалил другой под, пока этот DROP ждал блокировку (§5.5): пропуск, в результат не попадает.
            await SafeRollbackAsync(conn, tx);
            return new DropResult(DropStatus.Skipped, null);
        }
        catch (NpgsqlException e)
        {
            await SafeRollbackAsync(conn, tx);
            return new DropResult(DropStatus.Failed, e.Message);
        }
    }

    private async Task<int> CountAttachedJobsAsync(CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand($"SELECT count(*)::int FROM pg_inherits WHERE inhparent = '{_schema}.jobs'::regclass");
        return (int)(await cmd.ExecuteScalarAsync(ct))!;
    }

    // ---- помощники ------------------------------------------------------------------------------

    private Task HookAsync(RetentionStep step, string parent, IReadOnlyList<DateOnly> days, CancellationToken ct) =>
        _tuning.Hook is { } hook ? hook(new RetentionHookContext(step, parent, days), ct) : Task.CompletedTask;

    private string Qualified(string table) => $"{_schema}.{table}";

    private static int Ms(TimeSpan t) => (int)Math.Ceiling(t.TotalMilliseconds);

    private static NpgsqlBatchCommand Cmd(string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlBatchCommand(sql);
        foreach (var p in parameters)
            command.Parameters.Add(p);
        return command;
    }

    /// <summary>Позиционный параметр ($N по порядку добавления).</summary>
    private static NpgsqlParameter Arg<T>(T value) => new NpgsqlParameter<T> { TypedValue = value };

    /// <summary>Читатель на первом наборе строк: SET/LOCK пакета строк не возвращают.</summary>
    private static async Task<NpgsqlDataReader> ExecuteToRowsAsync(NpgsqlBatch batch, CancellationToken ct)
    {
        var reader = await batch.ExecuteReaderAsync(ct);
        while (reader.FieldCount == 0 && await reader.NextResultAsync(ct))
        {
        }
        return reader;
    }

    /// <summary>
    /// §5.3 п. 5: откат — только на открытом соединении и без токена; при разрыве транзакцию откатил сервер, Npgsql откатит
    /// незавершённую транзакцию при dispose. Ошибка самого отката не должна подменить исходную.
    /// </summary>
    private static async Task SafeRollbackAsync(NpgsqlConnection conn, NpgsqlTransaction tx)
    {
        if (conn.FullState != ConnectionState.Open)
            return;
        try
        {
            await tx.RollbackAsync(CancellationToken.None);
        }
        catch (Exception e) when (e is NpgsqlException or InvalidOperationException)
        {
            // Соединение разорвано во время отката: транзакцию откатит сервер.
        }
    }

    /// <summary>
    /// §5.3 «Обработка ошибок»: statement_timeout (57014), 25P03 от idle_in_transaction_session_timeout напрямую или любой
    /// разрыв соединения. Разрыв определяется по состоянию соединения, а не по типу исключения: через PgBouncer вместо 25P03
    /// может прийти PostgresException от самого PgBouncer (например, 08P01) или обрыв без SQLSTATE.
    /// </summary>
    private static bool IsHoldTimeout(NpgsqlException e, NpgsqlConnection conn) =>
        e is PostgresException { SqlState: PostgresErrorCodes.QueryCanceled or IdleInTransactionTimeout }
        || (conn.FullState & ConnectionState.Broken) != 0 || conn.FullState == ConnectionState.Closed;

    private enum LockedOutcome { Committed, RolledBack, LockTimeout, HoldTimeout }

    private enum DropStatus { Dropped, Skipped, LockTimeout, Failed }

    private readonly record struct DropResult(DropStatus Status, string? Error);

    private sealed record GroupResult(LockedOutcome Outcome, List<Candidate> Detached, List<Candidate> NotAttached);

    private sealed record Candidate(CatalogTable Table, int Overdue)
    {
        public DateOnly Day => Table.Day;
        public Guid Lo => UuidV7.LowerBound(Day);
        public Guid Hi => UuidV7.LowerBound(Day.AddDays(1));
    }

    private sealed class Accumulator
    {
        public List<DateOnly> DroppedJobs { get; } = [];
        public List<DateOnly> DroppedEvents { get; } = [];
        public int RemnantsDropped { get; private set; }
        public List<PinnedPartition> Pinned { get; } = [];
        public List<DeferredPartition> Deferred { get; } = [];

        public void DeferGroup(string table, IEnumerable<Candidate> group, LockedOutcome outcome)
        {
            var reason = outcome == LockedOutcome.LockTimeout ? DeferReason.LockTimeout : DeferReason.HoldTimeout;
            foreach (var c in group)
                Deferred.Add(new DeferredPartition(table, c.Day, reason, c.Overdue, null));
        }

        /// <param name="countRemnant">false — остаток компактирования этого запуска: в DroppedCompactionRemnants не считается (§5.1).</param>
        public void RecordDrop(CatalogTable table, int overdue, DropResult drop, bool countRemnant)
        {
            switch (drop.Status)
            {
                case DropStatus.Dropped when table.Kind == CatalogKind.CompactionRemnant:
                    if (countRemnant)
                        RemnantsDropped++;
                    break;
                case DropStatus.Dropped:
                    (table.IsJobs ? DroppedJobs : DroppedEvents).Add(table.Day);
                    break;
                case DropStatus.LockTimeout:
                    Deferred.Add(new DeferredPartition(table.Table, table.Day, DeferReason.DropLockTimeout, overdue, null));
                    break;
                case DropStatus.Failed:
                    Deferred.Add(new DeferredPartition(table.Table, table.Day, DeferReason.DropFailed, overdue, null, drop.Error));
                    break;
            }
        }

        public RetentionResult ToResult(int attachedJobs) =>
            new([.. DroppedJobs], [.. DroppedEvents], RemnantsDropped, [.. Pinned], [.. Deferred], attachedJobs);
    }
}
```

Замечания для исполнителя:
- `PostgresErrorCodes.LockNotAvailable` = `55P03`, `QueryCanceled` = `57014`, `UndefinedTable` = `42P01` (Npgsql). `25P03` — константа класса.
- Порядок catch в `DropAsync`/`RunLockedAsync`: `NpgsqlException` с фильтром `when (ct.IsCancellationRequested)` стоит до `PostgresException` (производный тип) — это компилируется, потому что у предыдущего catch есть фильтр (CS0160 возникает только без фильтра). Отмена токеном всегда даёт `OperationCanceledException`, а не `DropFailed`/`LockTimeout`/`Skipped`. В `DropAsync` оба catch `PostgresException` (`55P03`, `42P01`) — с фильтрами и стоят до общего `catch (NpgsqlException e)` → `DropFailed`; общий catch базового типа последним — CS0160 нет.
- `DROP TABLE` без `IF EXISTS`: исчезновение таблицы между проверкой oid и DROP (перекрытие, §5.5) узнаётся по `42P01`, а не по NOTICE — признак не зависит от `lc_messages`, `client_min_messages` и event trigger'ов.
- Если Npgsql 10 не принимает позиционные параметры в `NpgsqlBatchCommand` без имени — задать `ParameterName = ""`; сообщить в отчёте.
- `SafeRollbackAsync` глотает только `NpgsqlException`/`InvalidOperationException` отката — это не «пустой catch»: исходная ошибка пробрасывается вызывающим.
- `unnest` с несколькими массивами в `FROM` поддерживается с PostgreSQL 9.4.

- [x] **Step 5: `IPgBoltMaintenance.ApplyRetentionAsync` и DI**

В интерфейс:

```csharp
    /// <summary>
    /// Retention (часть 2 §5.2–§5.5): удаляет партиции jobs/job_events старше JobsRetention/EventsRetention без живых задач,
    /// компактирует закреплённые (не больше RetentionCompactionMaxLiveJobs живых), удаляет остатки прошлых запусков.
    /// LockTimeout/HoldTimeout — не ошибка, а Deferred; повторять не чаще раза в час.
    /// </summary>
    Task<RetentionResult> ApplyRetentionAsync(CancellationToken ct = default);
```

`PgBoltMaintenance` — добавить параметр первичного конструктора `RetentionRunner retention` (последним) и метод:

```csharp
    public Task<RetentionResult> ApplyRetentionAsync(CancellationToken ct = default) => retention.ApplyAsync(ct);
```

`PgBoltServiceCollectionExtensions`:

```csharp
        services.AddSingleton(sp => new RetentionRunner(sp.GetRequiredService<PgBoltDataSource>().Value, options, RetentionTuning.Default));
        services.AddSingleton<IPgBoltMaintenance>(sp => new PgBoltMaintenance(options,
            sp.GetRequiredService<MigrationRunner>(), sp.GetRequiredService<PartitionManager>(), sp.GetRequiredService<PgBoltStore>(),
            sp.GetRequiredService<RetentionRunner>()));
```

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~Retention"`
Expected: PASS (Direct и Bouncer; `RetentionSessionStateTests` — только Direct).

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Schema/RetentionRunner.cs src/PgBolt.Postgres/Hosting tests/PgBolt.Postgres.Tests/RetentionTestBase.cs tests/PgBolt.Postgres.Tests/RetentionEventsTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(core): retention of job_events partitions and remnants"
```

---
### Task 7: Retention `jobs` — предпроверка, отсоединение, компактирование

Спека: §5.3 (п. 1–5, «Публикации», «Корректность компактирования», «Обработка ошибок», «Порог», «Закреплённые партиции»), §11 (запрет FK, `SELECT *`), §12 «Retention».

**Files:**
- Create: `src/PgBolt.Postgres/Schema/RetentionRunner.Jobs.cs`
- Modify: `src/PgBolt.Postgres/Schema/RetentionRunner.cs` (метод `ApplyAsync`)
- Test: `tests/PgBolt.Postgres.Tests/RetentionJobsTests.cs`, `tests/PgBolt.Postgres.Tests/RetentionCompactionTests.cs` (создать)

**Interfaces:**
- Consumes: внутренние члены `RetentionRunner` из Task 6 (`Candidate`, `Accumulator`, `LockedOutcome`, `RunLockedAsync`, `DropAsync`, `ExcludeHeldAsync`, `DetachGroupAsync`, `DropDetachedAsync`, `Detached`, `HookAsync`, `Cmd`, `Arg`, `Qualified`, `LockTimeoutSql`, `SafeRollbackAsync`, `ExecuteToRowsAsync`, `MaxLive`, `MinCompactionGain`).
- Produces: `internal string RetentionRunner.WarmUpSql(string partition)`; этап `jobs` в `ApplyAsync`.

- [x] **Step 1: Тесты отсоединения и предпроверки**

`tests/PgBolt.Postgres.Tests/RetentionJobsTests.cs` (весь файл):

```csharp
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionJobsTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    private async Task TerminalPartitionAsync(DateOnly day, int rows = 3, bool events = false)
    {
        await CreatePartitionsAsync(day, events: events);
        await InsertTerminalAsync(day, rows);
    }

    [Fact]
    public async Task Jobs_candidates_follow_day_boundary()
    {
        foreach (var d in new[] { 5, 4, 3 })
            await TerminalPartitionAsync(Ago(d));

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal([Ago(5), Ago(4)], r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(Ago(3))));
        Assert.True(await AttachedAsync(J(Today)));
        Assert.Empty(r.Pinned);
        Assert.Empty(r.Deferred);
        Assert.Equal(4, r.AttachedJobsPartitions);
    }

    [Fact]
    public async Task Null_jobs_retention_keeps_partitions()
    {
        await TerminalPartitionAsync(Ago(30));

        Assert.Empty((await Runner(o => o.EventsRetention = Days3).ApplyAsync(Ct)).DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(Ago(30))));
    }

    [Fact]
    public async Task Partition_with_only_live_jobs_is_pinned_until_they_finish()
    {
        await CreatePartitionsAsync(Ago(10), events: false);
        await EnqueueLiveAsync(Ago(10), 2);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(Ago(10), 6, 2, false), Assert.Single(r.Pinned));
        var leased = await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);
        Assert.Equal(2, await Store.AckAsync(ServerA, [.. leased.Select(j => new LeaseRef(j.Id, j.LeaseId))], Ct));
        Assert.Equal([Ago(10)], (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Requeue_between_precheck_and_lock_keeps_partition_attached()
    {
        await CreatePartitionsAsync(Ago(10), events: false);
        var failed = await InsertTerminalAsync(Ago(10), 2, state: 3);
        await TerminalPartitionAsync(Ago(9));
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.LocksChecked)
                    Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(failed[0], ct)).Result);
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal([Ago(9)], r.DroppedJobsPartitions);
        Assert.DoesNotContain(r.Pinned, p => p.Day == Ago(10));
        Assert.DoesNotContain(r.Deferred, d => d.Day == Ago(10));
        Assert.True(await AttachedAsync(J(Ago(10))));
        Assert.True(await InQueueAsync(failed[0]));
        Assert.Equal((short)0, await JobStateAsync(failed[0]));
    }

    [Fact]
    public async Task Groups_of_k_partitions_share_one_transaction()
    {
        for (var d = 30; d > 5; d--)
            await TerminalPartitionAsync(Ago(d), rows: 1);
        var hooks = new HookRecorder();

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning(k: 10)).ApplyAsync(Ct);

        Assert.Equal(25, r.DroppedJobsPartitions.Count);
        Assert.Equal(3, hooks.Count(RetentionStep.BatchA, "jobs"));
    }

    [Fact]
    public async Task Interrupted_run_leaves_detached_table_that_next_run_drops()
    {
        await TerminalPartitionAsync(Ago(10));
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.Committed ? throw new InvalidOperationException("pod died") : Task.CompletedTask,
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct));

        Assert.True(await ExistsAsync(J(Ago(10))));
        Assert.False(await AttachedAsync(J(Ago(10))));
        Assert.Equal([Ago(10)], (await Runner().ApplyAsync(Ct)).DroppedJobsPartitions);   // оба срока null
        Assert.False(await ExistsAsync(J(Ago(10))));
    }

    [Fact]
    public async Task Lock_timeout_on_jobs_defers_first_group_and_stops_jobs_only()
    {
        foreach (var d in new[] { 12, 11, 10 })
            await TerminalPartitionAsync(Ago(d), events: true);
        await using (var holder = await HoldAsync("jobs", "ACCESS SHARE"))
        {
            var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }, new HookRecorder().Tuning(k: 2)).ApplyAsync(Ct);

            Assert.Equal(new[] { Ago(12), Ago(11) }, r.Deferred.Select(d => d.Day));
            Assert.All(r.Deferred, d => Assert.Equal(("jobs", DeferReason.LockTimeout), (d.Table, d.Reason)));
            Assert.Empty(r.DroppedJobsPartitions);
            Assert.Equal([Ago(12), Ago(11), Ago(10)], r.DroppedEventsPartitions);
            Assert.True(await AttachedAsync(J(Ago(10))));
            await EnqueueAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);
            await Store.DequeueAsync("q", 1, ServerA, Lease, Ct).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        }

        Assert.Equal([Ago(12), Ago(11), Ago(10)], (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Direct_holder_of_old_partition_is_deferred_before_lock()
    {
        await TerminalPartitionAsync(Ago(11));
        await TerminalPartitionAsync(Ago(10));
        await using var holder = await HoldAsync(J(Ago(11)), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("jobs", Ago(11), DeferReason.LockTimeout, 7, null), Assert.Single(r.Deferred));
        Assert.Equal([Ago(10)], r.DroppedJobsPartitions);
    }

    [Fact]
    public async Task Reader_through_parent_is_not_excluded_by_precheck_and_times_out_the_lock()
    {
        await TerminalPartitionAsync(Ago(10));
        await using var reader = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var cmd = new NpgsqlCommand(Db.Sql("BEGIN; SELECT count(*) FROM {schema}.jobs"), reader))
            await cmd.ExecuteNonQueryAsync(Ct);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("jobs", Ago(10), DeferReason.LockTimeout, 6, null), Assert.Single(r.Deferred));
    }

    [Fact]
    public async Task Holder_appearing_after_locks_check_times_out_in_detach()
    {
        await TerminalPartitionAsync(Ago(10));
        NpgsqlConnection? holder = null;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.LocksChecked)
                    holder = await HoldAsync(J(Ago(10)), "SHARE UPDATE EXCLUSIVE");
            },
        };
        try
        {
            var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

            Assert.Equal(new DeferredPartition("jobs", Ago(10), DeferReason.LockTimeout, 6, null), Assert.Single(r.Deferred));
            Assert.True(await AttachedAsync(J(Ago(10))));
        }
        finally
        {
            if (holder is not null)
                await holder.DisposeAsync();
        }
    }

    [Fact]
    public async Task Autovacuum_like_lock_on_todays_partition_does_not_block_removal()
    {
        await TerminalPartitionAsync(Ago(10));
        await using var holder = await HoldAsync(J(Today), "SHARE UPDATE EXCLUSIVE");

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal([Ago(10)], r.DroppedJobsPartitions);
        Assert.Empty(r.Deferred);
    }

    [Fact]
    public async Task Partition_dropped_after_listing_is_skipped()
    {
        await CreatePartitionsAsync(Ago(10), events: false);
        await EnqueueLiveAsync(Ago(10), 1);
        await InsertTerminalAsync(Ago(10), 150);
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.Listed)
                    return;
                await DetachAsync("jobs", J(Ago(10)));
                await Db.ExecAsync($"DROP TABLE {{schema}}.{J(Ago(10))}");
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);   // 42P01 в предпроверке

        Assert.Empty(r.Pinned);
        Assert.Empty(r.Deferred);
        Assert.Empty(r.DroppedJobsPartitions);
    }

    [Fact]
    public async Task Foreign_tables_with_partition_like_names_are_not_touched()
    {
        var foreignDay = Ago(20);
        var attachedRemnantDay = Ago(21);
        await Db.ExecAsync("CREATE TABLE {schema}.jobs_2026101 (LIKE {schema}.jobs)");
        await Db.ExecAsync($"CREATE TABLE {{schema}}.{J(Ago(30))}_old (LIKE {{schema}}.jobs)");
        await Db.ExecAsync("CREATE TABLE {schema}.jobs_20261399 (LIKE {schema}.jobs)");
        await Db.ExecAsync($"""
            CREATE TABLE {{schema}}.foreign_parent (LIKE {{schema}}.jobs) PARTITION BY RANGE (id);
            CREATE TABLE {{schema}}.{J(foreignDay)} (LIKE {{schema}}.jobs);
            ALTER TABLE {{schema}}.foreign_parent ATTACH PARTITION {{schema}}.{J(foreignDay)}
                FOR VALUES FROM ('{UuidV7.LowerBound(foreignDay)}') TO ('{UuidV7.LowerBound(foreignDay.AddDays(1))}');
            CREATE TABLE {{schema}}.{J(attachedRemnantDay)}_r1 (LIKE {{schema}}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
            ALTER TABLE {{schema}}.jobs ATTACH PARTITION {{schema}}.{J(attachedRemnantDay)}_r1
                FOR VALUES FROM ('{UuidV7.LowerBound(attachedRemnantDay)}') TO ('{UuidV7.LowerBound(attachedRemnantDay.AddDays(1))}');
            """);

        var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }).ApplyAsync(Ct);

        Assert.Empty(r.DroppedJobsPartitions);
        Assert.Empty(r.Deferred);
        foreach (var t in new[] { "jobs_2026101", $"{J(Ago(30))}_old", "jobs_20261399", J(foreignDay), $"{J(attachedRemnantDay)}_r1" })
            Assert.True(await ExistsAsync(t), t);
    }
}

public sealed class RetentionJobsTests_Direct(PostgresFixture pg) : RetentionJobsTests(pg, ConnectionMode.Direct);
public sealed class RetentionJobsTests_Bouncer(PostgresFixture pg) : RetentionJobsTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Тесты компактирования**

`tests/PgBolt.Postgres.Tests/RetentionCompactionTests.cs` (весь файл):

```csharp
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionCompactionTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    /// <summary>~6.4 КБ случайного hex: несжимаемо, поэтому значение уходит во внешний TOAST (повтор 'x' сжался бы inline).</summary>
    private static readonly string BigArgs =
        $$"""{"blob":"{{string.Concat(Enumerable.Range(0, 200).Select(_ => Guid.NewGuid().ToString("N")))}}"}""";

    private RetentionRunner Compactor(RetentionTuning? tuning = null, int maxLive = 1000, bool events = false) =>
        Runner(o =>
        {
            o.JobsRetention = TimeSpan.FromDays(3);
            o.RetentionCompactionMaxLiveJobs = maxLive;
            if (events)
                o.EventsRetention = TimeSpan.FromDays(3);
        }, tuning);

    /// <summary>
    /// Сутки D = today − ago (по умолчанию 10; OverdueDays = ago − 4 при сроке 3 сут): live живых и terminal терминальных строк.
    /// </summary>
    private async Task<(DateOnly Day, Guid[] Live)> PinnedPartitionAsync(int live = 2, int terminal = 150, bool events = false,
        short terminalState = 2, int ago = 10)
    {
        var day = Ago(ago);
        await CreatePartitionsAsync(day, events: events);
        var ids = await EnqueueLiveAsync(day, live);
        if (terminal > 0)
            await InsertTerminalAsync(day, terminal, terminalState);
        return (day, ids);
    }

    private async Task<Dictionary<Guid, string>> RowsAsync(string table, Guid[] ids)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql($"SELECT id, row_to_json(j)::text FROM {{schema}}.{table} j WHERE id = ANY(@ids)"));
        cmd.Parameters.AddWithValue("ids", ids);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var rows = new Dictionary<Guid, string>();
        while (await r.ReadAsync(Ct))
            rows[r.GetGuid(0)] = r.GetString(1);
        return rows;
    }

    private Task<string?> QueueSnapshotAsync() =>
        Db.ScalarAsync<string?>("SELECT string_agg(row_to_json(q)::text, ',' ORDER BY job_id) FROM {schema}.queue q");

    [Fact]
    public async Task Compaction_keeps_live_rows_and_drops_terminal_ones()
    {
        var (day, live) = await PinnedPartitionAsync(live: 3);
        var big = await EnqueueLiveAsync(day, 1, args: BigArgs);                      // Review Focus 3: TOAST
        await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);                        // часть живых — в lease
        var all = live.Concat(big).ToArray();
        var before = await RowsAsync(J(day), all);
        var oldOid = await OidAsync(J(day));
        var queueBefore = await QueueSnapshotAsync();

        var r = await Compactor().ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 4, true), Assert.Single(r.Pinned));
        Assert.Empty(r.Deferred);
        Assert.Equal(0, r.DroppedCompactionRemnants);
        Assert.True(await AttachedAsync(J(day)));
        Assert.NotEqual(oldOid, await OidAsync(J(day)));
        Assert.Equal(4L, await CountAsync(J(day)));
        Assert.Equal(before, await RowsAsync(J(day), all));
        Assert.True(await Db.ScalarAsync<bool>("SELECT length(args::text) > 5000 FROM {schema}.jobs WHERE id = @id", ("id", big[0])));
        Assert.Contains("fillfactor=80",
            await Db.ScalarAsync<string[]>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{Db.Schema}.{J(day)}")));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM pg_index WHERE indrelid = to_regclass(@t) AND indisprimary",
            ("t", $"{Db.Schema}.{J(day)}")));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM pg_constraint WHERE conrelid = to_regclass(@t) AND contype = 'c'",
            ("t", $"{Db.Schema}.{J(day)}")));
        Assert.False(await ExistsAsync($"{J(day)}_r{oldOid}"));
        Assert.Equal(queueBefore, await QueueSnapshotAsync());
    }

    [Fact]
    public async Task Compacted_partition_is_dropped_after_its_live_jobs_finish()
    {
        var (day, _) = await PinnedPartitionAsync();
        Assert.True(Assert.Single((await Compactor().ApplyAsync(Ct)).Pinned).Compacted);

        var leased = await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);
        Assert.Equal(2, await Store.AckAsync(ServerA, [.. leased.Select(j => new LeaseRef(j.Id, j.LeaseId))], Ct));

        Assert.Equal([day], (await Compactor().ApplyAsync(Ct)).DroppedJobsPartitions);
    }

    [Fact]
    public async Task Leased_jobs_finish_normally_after_compaction()
    {
        var (day, _) = await PinnedPartitionAsync(live: 6, events: true);
        var jobs = (await Store.DequeueAsync("q", 6, ServerA, Lease, Ct)).OrderBy(j => j.Id).ToArray();
        Assert.Equal(6, jobs.Length);
        await Store.CancelAsync(jobs[5].Id, "stop", Ct);   // cancel_requested у взятой задачи

        Assert.True(Assert.Single((await Compactor().ApplyAsync(Ct)).Pinned).Compacted);

        Assert.Equal(6, (await Store.HeartbeatAsync(ServerA, [.. jobs.Select(j => new LeaseRef(j.Id, j.LeaseId))], Lease, Ct)).Leases.Count);
        Assert.Equal(1, await Store.AckAsync(ServerA, [new LeaseRef(jobs[0].Id, jobs[0].LeaseId)], Ct));
        Assert.Equal(FailOutcome.Retried, await Store.RetryAsync(jobs[1].Id, jobs[1].LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "m", null, Ct));
        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(jobs[2].Id, jobs[2].LeaseId, ServerA, JobEventKind.Failed, "m", null, Ct));
        Assert.Equal(ReleaseOutcome.Released, await Store.ReleaseAsync(jobs[3].Id, jobs[3].LeaseId, ServerA, ran: true, null, Ct));
        Assert.Equal(ReleaseOutcome.Released, await Store.ReleaseAsync(jobs[4].Id, jobs[4].LeaseId, ServerA, ran: false, null, Ct));
        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(jobs[5].Id, jobs[5].LeaseId, ServerA, ran: true, null, Ct));

        Assert.Equal(new short?[] { 2, 0, 3, 0, 0, 4 }, await Task.WhenAll(jobs.Select(j => JobStateAsync(j.Id))));
        Assert.Equal(6L, await CountAsync(J(day)));   // итоги записаны в новую партицию тех же суток
    }

    [Fact]
    public async Task Live_threshold_decides_between_compaction_and_deferral()
    {
        await CreatePartitionsAsync(Ago(11), events: false);
        await EnqueueLiveAsync(Ago(11), 5);
        await InsertTerminalAsync(Ago(11), 150);
        await CreatePartitionsAsync(Ago(10), events: false);
        await EnqueueLiveAsync(Ago(10), 6);
        await InsertTerminalAsync(Ago(10), 150);
        var oid10 = await OidAsync(J(Ago(10)));

        var r = await Compactor(maxLive: 5).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(Ago(11), 7, 5, true), Assert.Single(r.Pinned));
        Assert.Equal(new DeferredPartition("jobs", Ago(10), DeferReason.LiveJobs, 6, 6), Assert.Single(r.Deferred));
        Assert.Equal(oid10, await OidAsync(J(Ago(10))));
    }

    [Fact]
    public async Task Zero_threshold_disables_compaction()
    {
        var (day, _) = await PinnedPartitionAsync(live: 1);

        var r = await Compactor(maxLive: 0).ApplyAsync(Ct);

        Assert.Empty(r.Pinned);
        Assert.Equal(new DeferredPartition("jobs", day, DeferReason.LiveJobs, 6, 1), Assert.Single(r.Deferred));
    }

    [Theory]
    [InlineData(50, 99, false)]
    [InlineData(50, 100, true)]
    [InlineData(300, 299, false)]
    [InlineData(300, 300, true)]
    [InlineData(3, 0, false)]
    public async Task Compaction_needs_at_least_max_of_live_and_100_terminal_rows(int live, int terminal, bool compacted)
    {
        var (day, _) = await PinnedPartitionAsync(live, terminal);
        var hooks = new HookRecorder();

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, live, compacted), Assert.Single(r.Pinned));
        Assert.Equal(compacted ? 1 : 0, hooks.Count(RetentionStep.BatchA, "jobs"));   // без сжатия LOCK не берётся
    }

    [Fact]
    public async Task Compacted_partition_is_left_alone_without_new_terminal_rows()
    {
        var (day, _) = await PinnedPartitionAsync();
        await Compactor().ApplyAsync(Ct);
        var oid = await OidAsync(J(day));

        var r = await Compactor().ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 2, false), Assert.Single(r.Pinned));
        Assert.Equal(oid, await OidAsync(J(day)));
    }

    [Fact]
    public async Task Each_compaction_has_its_own_locks_check_and_warm_up()
    {
        foreach (var d in new[] { 12, 11, 10 })
        {
            await CreatePartitionsAsync(Ago(d), events: false);
            await EnqueueLiveAsync(Ago(d), 2);
            await InsertTerminalAsync(Ago(d), 150);
        }
        var hooks = new HookRecorder();

        await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        var steps = hooks.Calls
            .Where(c => c.Parent == "jobs" && c.Step is RetentionStep.LocksChecked or RetentionStep.WarmedUp or RetentionStep.BatchA)
            .Select(c => (c.Step, c.Days[0]))
            .ToArray();
        Assert.Equal(
            new[] { 12, 11, 10 }.SelectMany(d => new[]
            {
                (RetentionStep.LocksChecked, Ago(d)), (RetentionStep.WarmedUp, Ago(d)), (RetentionStep.BatchA, Ago(d)),
            }),
            steps);
    }

    [Fact]
    public async Task Warm_up_reads_heap_not_index_only()
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var ids = await EnqueueLiveAsync(day, 20, args: BigArgs);
        await InsertTerminalAsync(day, 500);
        await Db.ExecAsync($"VACUUM ANALYZE {{schema}}.{J(day)}");   // visibility map: Index Only Scan стал бы возможен
        var sql = Compactor().WarmUpSql(J(day)).Replace("$1", $"ARRAY[{string.Join(",", ids.Select(i => $"'{i}'"))}]::uuid[]");
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var off = new NpgsqlCommand("SET enable_seqscan = off", conn))
            await off.ExecuteNonQueryAsync(Ct);
        await using var explain = new NpgsqlCommand("EXPLAIN " + sql, conn);
        var lines = new List<string>();
        await using (var r = await explain.ExecuteReaderAsync(Ct))
            while (await r.ReadAsync(Ct))
                lines.Add(r.GetString(0));
        var plan = string.Join('\n', lines);

        Assert.DoesNotContain("Index Only Scan", plan);
        Assert.True(plan.Contains("Index Scan") || plan.Contains("Bitmap Heap Scan"), plan);
    }

    [Fact]
    public async Task Partition_detached_by_someone_else_is_not_compacted()
    {
        var (day, _) = await PinnedPartitionAsync();
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.WarmedUp ? DetachAsync("jobs", J(day)) : Task.CompletedTask,
        };

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(0, hooks.Count(RetentionStep.BatchB, "jobs"));
        Assert.Equal([day], r.DroppedJobsPartitions);   // отсоединённая с тем же oid — в транзакцию 2
        Assert.Empty(r.Pinned);
    }

    [Fact]
    public async Task Live_jobs_above_threshold_found_under_lock_roll_back()
    {
        var (day, _) = await PinnedPartitionAsync(live: 5);
        var oid = await OidAsync(J(day));
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.WarmedUp)
                    await EnqueueLiveAsync(day, 2);
            },
        };

        var r = await Compactor(hooks.Tuning(), maxLive: 5).ApplyAsync(Ct);

        Assert.Equal(0, hooks.Count(RetentionStep.BatchB, "jobs"));
        Assert.Equal(new DeferredPartition("jobs", day, DeferReason.LiveJobs, 6, 6), Assert.Single(r.Deferred));
        Assert.Equal(oid, await OidAsync(J(day)));
    }

    [Fact]
    public async Task Job_requeued_before_lock_is_moved_with_live_rows()
    {
        var (day, _) = await PinnedPartitionAsync(terminal: 0);
        var failed = await InsertTerminalAsync(day, 150, state: 3);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.WarmedUp)
                    Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(failed[0], ct)).Result);
            },
        };

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 2, true), Assert.Single(r.Pinned));   // §5.1: LiveJobs — по предпроверке
        Assert.Equal(3L, await CountAsync(J(day)));                                    // перенесены живые под блокировкой
        Assert.True(await InQueueAsync(failed[0]));
        Assert.Equal((short)0, await JobStateAsync(failed[0]));
    }

    [Fact]
    public async Task Interrupted_compaction_leaves_remnant_that_next_run_drops()
    {
        var (day, _) = await PinnedPartitionAsync();
        var oid = await OidAsync(J(day));
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.Committed ? throw new InvalidOperationException("pod died") : Task.CompletedTask,
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => Compactor(hooks.Tuning()).ApplyAsync(Ct));

        Assert.True(await AttachedAsync(J(day)));
        Assert.True(await ExistsAsync($"{J(day)}_r{oid}"));
        var r = await Runner().ApplyAsync(Ct);   // оба срока null
        Assert.Equal(1, r.DroppedCompactionRemnants);
        Assert.False(await ExistsAsync($"{J(day)}_r{oid}"));
        Assert.Equal(2L, await CountAsync(J(day)));
    }

    [Fact]
    public async Task Locked_remnant_of_this_run_is_pinned_and_deferred()
    {
        var (day, _) = await PinnedPartitionAsync();
        var oid = await OidAsync(J(day));
        NpgsqlConnection? holder = null;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.Committed)
                    holder = await HoldAsync($"{J(day)}_r{oid}", "ACCESS SHARE");
            },
        };
        try
        {
            var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

            Assert.Equal(new PinnedPartition(day, 6, 2, true), Assert.Single(r.Pinned));
            Assert.Equal(new DeferredPartition("jobs", day, DeferReason.DropLockTimeout, 6, null), Assert.Single(r.Deferred));
            Assert.Equal(0, r.DroppedCompactionRemnants);
        }
        finally
        {
            if (holder is not null)
                await holder.DisposeAsync();
        }

        Assert.Equal(1, (await Runner().ApplyAsync(Ct)).DroppedCompactionRemnants);
    }

    [Fact]
    public async Task Stale_listing_never_drops_a_recompacted_partition()
    {
        var (day, _) = await PinnedPartitionAsync();
        var hooks = new HookRecorder
        {
            // другой под сжимает партицию между предпроверкой и транзакцией 1 этого запуска
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.Prechecked)
                    Assert.True(Assert.Single((await Compactor().ApplyAsync(ct)).Pinned).Compacted);
            },
        };

        var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

        Assert.Empty(r.Pinned);
        Assert.Empty(r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(day)));
        Assert.Equal(2L, await CountAsync(J(day)));
    }

    [Fact]
    public async Task Remnant_replaced_by_attached_table_with_same_name_is_not_dropped()
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await DetachAsync("jobs", J(day));   // остаток отсоединения
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.Listed)
                    return;
                await Db.ExecAsync($"ALTER TABLE {{schema}}.{J(day)} RENAME TO old_detached");
                await CreatePartitionsAsync(day, events: false);
            },
        };

        var r = await Runner(tuning: hooks.Tuning()).ApplyAsync(Ct);

        Assert.Empty(r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(day)));
    }

    [Fact]
    public async Task Holder_appearing_after_warm_up_rolls_back_compaction()
    {
        var (day, _) = await PinnedPartitionAsync();
        var oid = await OidAsync(J(day));
        NpgsqlConnection? holder = null;
        var hooks = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step == RetentionStep.WarmedUp)
                    holder = await HoldAsync(J(day), "SHARE UPDATE EXCLUSIVE");
            },
        };
        try
        {
            var r = await Compactor(hooks.Tuning()).ApplyAsync(Ct);

            Assert.Equal(new DeferredPartition("jobs", day, DeferReason.LockTimeout, 6, null), Assert.Single(r.Deferred));
            Assert.Equal(oid, await OidAsync(J(day)));
            Assert.Equal(152L, await CountAsync(J(day)));
        }
        finally
        {
            if (holder is not null)
                await holder.DisposeAsync();
        }
    }

    [Fact]
    public async Task Unexpected_ddl_error_rolls_back_and_throws()
    {
        var (day, _) = await PinnedPartitionAsync();
        var oid = await OidAsync(J(day));
        var hooks = new HookRecorder
        {
            Then = (c, _) => c.Step == RetentionStep.WarmedUp
                ? Db.ExecAsync($"CREATE TABLE {{schema}}.{J(day)}_r{oid} (id int)")
                : Task.CompletedTask,
        };

        var e = await Assert.ThrowsAsync<PostgresException>(() => Compactor(hooks.Tuning()).ApplyAsync(Ct));

        Assert.Equal(PostgresErrorCodes.DuplicateTable, e.SqlState);
        Assert.Equal(oid, await OidAsync(J(day)));
        Assert.True(await AttachedAsync(J(day)));
        Assert.Equal(152L, await CountAsync(J(day)));
    }

    [Theory]
    [InlineData("BatchA")]
    [InlineData("BatchB")]
    public async Task Hold_timeout_during_compaction_leaves_partition_intact(string pauseAfter)   // RetentionStep — internal
    {
        var step = Enum.Parse<RetentionStep>(pauseAfter);
        // §5.3 п. 2: отсоединения, затем компактирования от старых к новым — first обрабатывается раньше second.
        var (first, _) = await PinnedPartitionAsync(events: true);            // Ago(10), OverdueDays = 6
        var (second, _) = await PinnedPartitionAsync(events: true, ago: 9);   // Ago(9), OverdueDays = 5
        var firstOid = await OidAsync(J(first));
        var secondOid = await OidAsync(J(second));
        var hooks = new HookRecorder
        {
            // только этап jobs: пауза в job_events дала бы HoldTimeout и там
            Then = (c, ct) => c.Parent == "jobs" && c.Step == step ? Task.Delay(TimeSpan.FromSeconds(1.5), ct) : Task.CompletedTask,
        };

        var r = await Compactor(hooks.Tuning(holdIdle: TimeSpan.FromMilliseconds(300)), events: true).ApplyAsync(Ct);

        Assert.Equal(new DeferredPartition("jobs", first, DeferReason.HoldTimeout, 6, null), Assert.Single(r.Deferred));
        Assert.Empty(r.Pinned);                                   // second не обрабатывался: остановка этапа jobs (п. 2)
        Assert.Empty(r.DroppedJobsPartitions);
        Assert.Equal([first, second], r.DroppedEventsPartitions); // этап job_events выполняется после остановки jobs
        Assert.Equal(firstOid, await OidAsync(J(first)));
        Assert.Equal(secondOid, await OidAsync(J(second)));
        Assert.Equal(1, hooks.Count(step, "jobs"));               // транзакция 1 только у first

        Assert.Equal(2, (await Compactor().ApplyAsync(Ct)).Pinned.Count(p => p.Compacted));
    }

    [Fact]
    public async Task Compaction_follows_parent_column_changes()
    {
        var (day, live) = await PinnedPartitionAsync(terminal: 0);
        await Db.ExecAsync("ALTER TABLE {schema}.jobs ADD COLUMN extra text");
        await Db.ExecAsync("UPDATE {schema}.jobs SET extra = 'kept' WHERE id = ANY(@ids)", ("ids", live));
        await InsertTerminalAsync(day, 150);
        Assert.True(Assert.Single((await Compactor().ApplyAsync(Ct)).Pinned).Compacted);
        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE extra = 'kept'"));

        await Db.ExecAsync("ALTER TABLE {schema}.jobs DROP COLUMN extra");   // у сжатой партиции — удалённый слот
        var day2 = Ago(9);
        await CreatePartitionsAsync(day2, events: false);                      // создана после DROP COLUMN: без слота
        var live2 = await EnqueueLiveAsync(day2, 2);
        await InsertTerminalAsync(day2, 150);
        await InsertTerminalAsync(day, 150);
        var before = (await RowsAsync(J(day), live)).Concat(await RowsAsync(J(day2), live2)).ToDictionary();

        var r = await Compactor().ApplyAsync(Ct);

        Assert.Equal(2, r.Pinned.Count(p => p.Compacted));
        Assert.Equal(before, (await RowsAsync(J(day), live)).Concat(await RowsAsync(J(day2), live2)).ToDictionary());
        Assert.Equal(0L, await Db.ScalarAsync<long>(
            "SELECT count(*) FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND attgenerated <> ''",
            ("t", $"{Db.Schema}.jobs")));
        await using var cmd = Db.Admin.CreateCommand(Db.Sql("""
            SELECT count(DISTINCT cols) FROM (
                SELECT c.oid, (SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
                               WHERE attrelid = c.oid AND attnum > 0 AND NOT attisdropped) AS cols
                FROM pg_class c
                WHERE c.oid = to_regclass('{schema}.jobs')
                   OR c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = to_regclass('{schema}.jobs'))
            ) x
            """));
        Assert.Equal(1L, (long)(await cmd.ExecuteScalarAsync(Ct))!);   // контракт §5.3 «SELECT *»
    }
}

public sealed class RetentionCompactionTests_Direct(PostgresFixture pg) : RetentionCompactionTests(pg, ConnectionMode.Direct);
public sealed class RetentionCompactionTests_Bouncer(PostgresFixture pg) : RetentionCompactionTests(pg, ConnectionMode.Bouncer);

public abstract class RetentionPublicationTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    [Theory]
    [InlineData("FOR TABLE {schema}.jobs")]
    [InlineData("FOR ALL TABLES")]
    public async Task Publication_skips_compaction_but_not_detach(string target)
    {
        var pinned = Ago(11);
        var empty = Ago(10);
        await CreatePartitionsAsync(pinned);
        await EnqueueLiveAsync(pinned, 3);
        await InsertTerminalAsync(pinned, 150);
        await CreatePartitionsAsync(empty);
        await InsertTerminalAsync(empty, 5);
        var oid = await OidAsync(J(pinned));
        var publication = $"pub_{Db.Schema}";
        await Db.ExecAsync($"CREATE PUBLICATION {publication} {target}");
        try
        {
            var r = await Runner(o => { o.JobsRetention = TimeSpan.FromDays(3); o.EventsRetention = TimeSpan.FromDays(3); }).ApplyAsync(Ct);

            Assert.Equal(new DeferredPartition("jobs", pinned, DeferReason.Replicated, 7, 3), Assert.Single(r.Deferred));
            Assert.Empty(r.Pinned);
            Assert.Equal([empty], r.DroppedJobsPartitions);
            Assert.Equal([pinned, empty], r.DroppedEventsPartitions);
            Assert.Equal(oid, await OidAsync(J(pinned)));
        }
        finally
        {
            await Db.ExecAsync($"DROP PUBLICATION IF EXISTS {publication}");
        }

        Assert.True(Assert.Single((await Runner(o => o.JobsRetention = TimeSpan.FromDays(3)).ApplyAsync(Ct)).Pinned).Compacted);
    }
}

[Collection("Serial")]   // публикация FOR ALL TABLES — состояние всей БД; коллекция из HotUpdateTests.cs
public sealed class RetentionPublicationTests_Direct(PostgresFixture pg) : RetentionPublicationTests(pg, ConnectionMode.Direct);

[Collection("Serial")]
public sealed class RetentionPublicationTests_Bouncer(PostgresFixture pg) : RetentionPublicationTests(pg, ConnectionMode.Bouncer);
```

`CREATE PUBLICATION` при `wal_level = replica` выдаёт только предупреждение — это достаточно для каталога публикаций.

- [x] **Step 3: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RetentionJobsTests|FullyQualifiedName~RetentionCompactionTests|FullyQualifiedName~RetentionPublicationTests"`
Expected: FAIL (нет `WarmUpSql`; этап `jobs` не выполняется).

- [x] **Step 4: `RetentionRunner.Jobs.cs`** (весь файл)

```csharp
using System.Globalization;
using Npgsql;

namespace PgBolt.Postgres.Schema;

internal sealed partial class RetentionRunner
{
    /// <summary>
    /// §5.3: предпроверка всех присоединённых кандидатов, запрос публикаций, группы отсоединения, затем компактирования
    /// по одной. LockTimeout/HoldTimeout транзакции 1 останавливает этап jobs (п. 2).
    /// </summary>
    private async Task ApplyJobsAsync(List<Candidate> candidates, Accumulator acc, CancellationToken ct)
    {
        var detach = new List<Candidate>();
        var compact = new List<(Candidate Candidate, Guid[] Live)>();
        foreach (var c in candidates)
        {
            var p = await PrecheckAsync(c, ct);
            switch (p.Kind)
            {
                case PrecheckKind.Detach:
                    detach.Add(c);
                    break;
                case PrecheckKind.Compact:
                    compact.Add((c, p.Live));
                    break;
                case PrecheckKind.Pinned:
                    acc.Pinned.Add(new PinnedPartition(c.Day, c.Overdue, p.Live.Length, Compacted: false));
                    break;
                case PrecheckKind.TooManyLive:
                    acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LiveJobs, c.Overdue, MaxLive + 1));
                    break;
                case PrecheckKind.LockTimeout:
                    acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LockTimeout, c.Overdue, null));
                    break;
                case PrecheckKind.Gone:
                    break;
            }
        }
        await HookAsync(RetentionStep.Prechecked, "jobs", [.. candidates.Select(c => c.Day)], ct);

        if (compact.Count > 0 && await PublishedAsync(ct))
        {
            foreach (var (c, live) in compact)
                acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.Replicated, c.Overdue, live.Length));
            compact.Clear();
        }

        foreach (var chunk in detach.Chunk(_tuning.PartitionsPerTransaction))
        {
            var group = await ExcludeHeldAsync("jobs", chunk, acc, ct);
            if (group.Count == 0)
                continue;
            var result = await DetachGroupAsync("jobs", group, ct);
            if (result.Outcome is LockedOutcome.LockTimeout or LockedOutcome.HoldTimeout)
            {
                acc.DeferGroup("jobs", group, result.Outcome);
                return;   // компактирования в этом запуске тоже не выполняются
            }
            await DropDetachedAsync(result, acc, ct);
        }

        foreach (var (c, live) in compact)
            if (!await CompactAsync(c, live, acc, ct))
                return;
    }

    private enum PrecheckKind { Detach, Compact, Pinned, TooManyLive, LockTimeout, Gone }

    private sealed record Precheck(PrecheckKind Kind, Guid[] Live);

    /// <summary>
    /// §5.3 п. 1: короткая транзакция на партицию с lock_timeout. Живые — по PK queue; окупаемость сжатия — seq scan,
    /// остановленный на L + max(L, 100)-й строке. 55P03 → LockTimeout, 42P01 (удалена другим подом) → Gone.
    /// </summary>
    private async Task<Precheck> PrecheckAsync(Candidate c, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            var live = new List<Guid>();
            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                batch.BatchCommands.Add(Cmd($"SELECT job_id FROM {_schema}.queue WHERE job_id >= $1 AND job_id < $2 LIMIT $3",
                    Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1)));
                await using var reader = await ExecuteToRowsAsync(batch, ct);
                while (await reader.ReadAsync(ct))
                    live.Add(reader.GetGuid(0));
            }

            var kind = PrecheckKind.Detach;
            if (live.Count > MaxLive)
            {
                kind = PrecheckKind.TooManyLive;
            }
            else if (live.Count > 0)
            {
                var need = (long)live.Count + Math.Max(live.Count, MinCompactionGain);
                await using var count = new NpgsqlCommand($"SELECT count(*) FROM (SELECT 1 FROM {Qualified(c.Table.Name)} LIMIT $1) x", conn, tx);
                count.Parameters.Add(Arg(need));
                kind = (long)(await count.ExecuteScalarAsync(ct))! == need ? PrecheckKind.Compact : PrecheckKind.Pinned;
            }
            await tx.CommitAsync(ct);
            return new Precheck(kind, [.. live]);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw;
        }
        catch (NpgsqlException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw new OperationCanceledException(ct);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await SafeRollbackAsync(conn, tx);
            return new Precheck(PrecheckKind.LockTimeout, []);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            await SafeRollbackAsync(conn, tx);
            return new Precheck(PrecheckKind.Gone, []);
        }
    }

    /// <summary>§5.3 «Публикации»: любая публикация, затрагивающая jobs или её партиции (в т. ч. FOR ALL TABLES).</summary>
    private async Task<bool> PublishedAsync(CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT EXISTS (SELECT 1 FROM pg_publication WHERE puballtables)
                OR EXISTS (SELECT 1 FROM pg_publication_tables WHERE schemaname = $1 AND tablename ~ '^jobs(_[0-9]{8})?$')
            """);
        cmd.Parameters.Add(Arg(_schema));
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>§5.3 п. 4: текст строки читает все колонки и детоастит args — план по heap, не Index Only Scan.</summary>
    internal string WarmUpSql(string partition) =>
        $"SELECT sum(octet_length(j::text)) FROM {Qualified(partition)} j WHERE j.id = ANY($1)";

    private async Task<PrecheckKind?> WarmUpAsync(Candidate c, Guid[] ids, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                batch.BatchCommands.Add(Cmd(WarmUpSql(c.Table.Name), Arg(ids)));
                await batch.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
            return null;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw;
        }
        catch (NpgsqlException) when (ct.IsCancellationRequested)
        {
            await SafeRollbackAsync(conn, tx);
            throw new OperationCanceledException(ct);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await SafeRollbackAsync(conn, tx);
            return PrecheckKind.LockTimeout;
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            await SafeRollbackAsync(conn, tx);
            return PrecheckKind.Gone;
        }
    }

    /// <summary>
    /// §5.3 п. 3–6 для одной партиции: pg_locks → прогрев → транзакция 1 (повтор проверки присоединённости по oid и живых
    /// под блокировкой; DETACH, RENAME в остаток, CREATE LIKE, INSERT живых по @ids, ATTACH) → DROP остатка транзакцией 2.
    /// </summary>
    /// <returns>false — остановка этапа jobs (LockTimeout/HoldTimeout транзакции 1).</returns>
    private async Task<bool> CompactAsync(Candidate c, Guid[] live, Accumulator acc, CancellationToken ct)
    {
        if ((await ExcludeHeldAsync("jobs", [c], acc, ct)).Count == 0)
            return true;
        switch (await WarmUpAsync(c, live, ct))
        {
            case PrecheckKind.LockTimeout:
                acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LockTimeout, c.Overdue, null));
                return true;
            case PrecheckKind.Gone:
                return true;
        }
        var days = new[] { c.Day };
        await HookAsync(RetentionStep.WarmedUp, "jobs", days, ct);

        var name = c.Table.Name;
        var remnant = $"{name}_r{c.Table.Oid.ToString(CultureInfo.InvariantCulture)}";
        var attached = false;
        Guid[] ids = [];
        var check = Cmd($"""
            SELECT to_regclass($1) = $2::oid
                     AND EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = $2::oid AND inhparent = '{_schema}.jobs'::regclass),
                   ARRAY(SELECT job_id FROM {_schema}.queue WHERE job_id >= $3 AND job_id < $4 LIMIT $5)
            """, Arg(Qualified(name)), Arg(c.Table.Oid), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1));

        var outcome = await RunLockedAsync("jobs", check, days, async (reader, t) =>
        {
            if (!await reader.ReadAsync(t))
                return;
            attached = !reader.IsDBNull(0) && reader.GetBoolean(0);
            ids = reader.GetFieldValue<Guid[]>(1);
        }, () =>
        {
            if (!attached || ids.Length > MaxLive)
                return [];
            if (ids.Length == 0)
                return [new NpgsqlBatchCommand($"ALTER TABLE {_schema}.jobs DETACH PARTITION {Qualified(name)}")];
            return CompactionCommands(c, remnant, ids);
        }, ct);

        switch (outcome)
        {
            case LockedOutcome.LockTimeout or LockedOutcome.HoldTimeout:
                acc.DeferGroup("jobs", [c], outcome);
                return false;
            case LockedOutcome.RolledBack when !attached:
                // обработал другой под: удалится, только если отсоединена и имя указывает на тот же oid (п. 6)
                acc.RecordDrop(Detached(c.Table), c.Overdue, await DropAsync(name, c.Table.Oid, ct), countRemnant: true);
                return true;
            case LockedOutcome.RolledBack:
                acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LiveJobs, c.Overdue, MaxLive + 1));
                return true;
        }

        await HookAsync(RetentionStep.Committed, "jobs", days, ct);
        if (ids.Length == 0)
        {
            acc.RecordDrop(Detached(c.Table), c.Overdue, await DropAsync(name, c.Table.Oid, ct), countRemnant: true);
            return true;
        }
        acc.Pinned.Add(new PinnedPartition(c.Day, c.Overdue, live.Length, Compacted: true));   // §5.1: LiveJobs — по предпроверке
        acc.RecordDrop(c.Table with { Name = remnant, Kind = CatalogKind.CompactionRemnant }, c.Overdue,
            await DropAsync(remnant, c.Table.Oid, ct), countRemnant: false);
        return true;
    }

    /// <summary>
    /// §5.3 п. 5, пакет Б компактирования. SELECT *: партиции создаёт только PgBolt через LIKE родителя, порядок логических
    /// колонок у старой, новой таблицы и родителя совпадает (§11). CHECK по диапазону не создаётся — ATTACH проверит ≤ T строк.
    /// </summary>
    private List<NpgsqlBatchCommand> CompactionCommands(Candidate c, string remnant, Guid[] ids)
    {
        var partition = Qualified(c.Table.Name);
        return
        [
            new NpgsqlBatchCommand($"ALTER TABLE {_schema}.jobs DETACH PARTITION {partition}"),
            new NpgsqlBatchCommand($"ALTER TABLE {partition} RENAME TO {remnant}"),
            new NpgsqlBatchCommand(
                $"CREATE TABLE {partition} (LIKE {_schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = {_options.JobsFillFactor})"),
            Cmd($"INSERT INTO {partition} SELECT * FROM {Qualified(remnant)} WHERE id = ANY($1)", Arg(ids)),
            new NpgsqlBatchCommand($"ALTER TABLE {_schema}.jobs ATTACH PARTITION {partition} FOR VALUES FROM ('{c.Lo}') TO ('{c.Hi}')"),
        ];
    }
}
```

- [x] **Step 5: Этап `jobs` в `ApplyAsync`**

В `RetentionRunner.ApplyAsync` перед строкой `if (eventsHorizon is { } eh)` вставить:

```csharp
        if (jobsHorizon is { } jh)
            await ApplyJobsAsync(Candidates(tables, CatalogKind.JobsPartition, jh), acc, ct);
```

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~Retention"`
Expected: PASS (все классы retention, Direct и Bouncer).

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Schema/RetentionRunner.Jobs.cs src/PgBolt.Postgres/Schema/RetentionRunner.cs tests/PgBolt.Postgres.Tests/RetentionJobsTests.cs tests/PgBolt.Postgres.Tests/RetentionCompactionTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(core): retention of jobs partitions with compaction of pinned days"
```

---

### Task 8: Сценарии retention: перекрытие, UTC, нагрузка, MVCC, multiplexing, slim

Спека: §6.2 «Перекрытие», §3, §5.3 «Корректность компактирования», «MVCC», «Порог», §5.3 п. 5 (multiplexing), §12; Review Focus 2 и 5.

**Files:**
- Test: `tests/PgBolt.Postgres.Tests/RetentionScenarioTests.cs` (создать), `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`
- Modify (только если тесты найдут дефект): `src/PgBolt.Postgres/Schema/RetentionRunner*.cs`

**Interfaces:**
- Consumes: `RetentionRunner`, `RetentionTestBase`, `HookRecorder` (Task 6–7), `PgBoltStore` (Task 3).

- [x] **Step 1: Сценарные тесты**

`tests/PgBolt.Postgres.Tests/RetentionScenarioTests.cs` (весь файл):

```csharp
using System.Collections.Concurrent;
using System.Data;
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RetentionScenarioTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);

    private async Task PinnedAsync(DateOnly day, int live = 2, int terminal = 150, string queue = "q")
    {
        await CreatePartitionsAsync(day);
        await EnqueueLiveAsync(day, live, queue);
        await InsertTerminalAsync(day, terminal);
    }

    private Task<long> OrphansAsync() => Db.ScalarAsync<long>("""
        SELECT (SELECT count(*) FROM {schema}.queue q WHERE NOT EXISTS (SELECT 1 FROM {schema}.jobs j WHERE j.id = q.job_id))
             + (SELECT count(*) FROM {schema}.jobs j WHERE j.state = 0 AND NOT EXISTS (SELECT 1 FROM {schema}.queue q WHERE q.job_id = j.id))
        """);

    [Fact]
    public async Task Parallel_runs_process_every_partition_exactly_once()
    {
        for (var d = 20; d > 10; d--)
        {
            await CreatePartitionsAsync(Ago(d));
            await InsertTerminalAsync(Ago(d), 5);
        }
        for (var d = 10; d > 7; d--)
            await PinnedAsync(Ago(d));

        RetentionRunner Run() => Runner(o => { o.JobsRetention = Days3; o.RetentionLockTimeout = TimeSpan.FromSeconds(2); });

        // Перекрытие: предпроверка и прогрев одного запуска держат партицию в обход родителя, и предпроверка pg_locks
        // другого может отложить её (LockTimeout) — это штатно; третий, последовательный запуск доделывает отложенное.
        // Отсоединённую одним запуском таблицу другой может увидеть остатком и тоже дойти до DROP: его DROP после ожидания
        // блокировки получает 42P01 и даёт Skipped (Task 6), поэтому сутки не учитываются дважды.
        var parallel = await Task.WhenAll(Run().ApplyAsync(Ct), Run().ApplyAsync(Ct));
        var runs = parallel.Append(await Run().ApplyAsync(Ct)).ToArray();

        Assert.All(runs.SelectMany(r => r.Deferred), d => Assert.Equal(DeferReason.LockTimeout, d.Reason));
        var dropped = runs.SelectMany(r => r.DroppedJobsPartitions).ToList();
        Assert.Equal(10, dropped.Count);                 // каждая партиция удалена ровно один раз
        Assert.Equal(10, dropped.Distinct().Count());
        var compacted = runs.SelectMany(r => r.Pinned).Where(p => p.Compacted).Select(p => p.Day).ToList();
        Assert.Equal(3, compacted.Count);                // каждая закреплённая сжата ровно один раз
        Assert.Equal(3, compacted.Distinct().Count());
        for (var d = 10; d > 7; d--)
        {
            Assert.True(await AttachedAsync(J(Ago(d))));
            Assert.Equal(2L, await Db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.{J(Ago(d))} WHERE state = 0"));
        }
        Assert.Equal(6L, await CountAsync("queue"));
        Assert.Equal(0L, await OrphansAsync());
    }

    [Theory]
    [InlineData("Pacific/Kiritimati")]
    [InlineData("Etc/GMT+12")]
    public async Task Session_time_zone_does_not_shift_days(string zone)
    {
        await CreatePartitionsAsync(Ago(3));
        await CreatePartitionsAsync(Ago(2));
        var cs = new NpgsqlConnectionStringBuilder(Db.ConnectionString) { Timezone = zone }.ConnectionString;
        await using var zoned = NpgsqlDataSource.Create(cs);
        await using (var check = zoned.CreateCommand("SELECT current_setting('TimeZone')"))
            Assert.Equal(zone, (string)(await check.ExecuteScalarAsync(Ct))!);

        var r = await Runner(o => { o.JobsRetention = TimeSpan.FromDays(2); o.EventsRetention = TimeSpan.FromDays(2); },
            dataSource: zoned).ApplyAsync(Ct);   // Rd = 2 → horizon = today − 3 (UTC)

        Assert.Equal([Ago(3)], r.DroppedJobsPartitions);
        Assert.Equal([Ago(3)], r.DroppedEventsPartitions);
        Assert.True(await AttachedAsync(J(Ago(2))));
        Assert.True(await AttachedAsync(E(Ago(2))));
    }

    [Fact]
    public async Task Hot_path_keeps_working_during_compactions()
    {
        foreach (var d in new[] { 12, 11, 10 })
            await PinnedAsync(Ago(d), live: 20, queue: "old");
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var errors = new ConcurrentQueue<Exception>();

        async Task LoopAsync(Func<CancellationToken, Task> body)
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await body(stop.Token);
                }
                catch (Exception) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception e)
                {
                    errors.Enqueue(e);
                }
            }
        }

        var workers = new[]
        {
            LoopAsync(async _ => await EnqueueAsync("new")),
            LoopAsync(async ct =>
            {
                var leased = await Store.DequeueAsync("new", 5, ServerA, Lease, ct);
                if (leased.Count > 0)
                    await Store.AckAsync(ServerA, [.. leased.Select(j => new LeaseRef(j.Id, j.LeaseId))], ct);
            }),
            LoopAsync(async ct =>
            {
                foreach (var j in await Store.DequeueAsync("old", 2, ServerB, Lease, ct))
                {
                    await Store.HeartbeatAsync(ServerB, [new LeaseRef(j.Id, j.LeaseId)], Lease, ct);
                    await Store.ReleaseAsync(j.Id, j.LeaseId, ServerB, ran: false, null, ct);
                }
            }),
        };

        var compactions = 0;
        for (var i = 0; i < 6; i++)
        {
            compactions += (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).Pinned.Count(p => p.Compacted);
            await InsertTerminalAsync(Ago(10 + i % 3), 150);
        }
        await stop.CancelAsync();
        await Task.WhenAll(workers);

        Assert.Empty(errors);
        Assert.True(compactions >= 3, $"compactions: {compactions}");
        Assert.Equal(0L, await OrphansAsync());
        Assert.Equal(60L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE queue = 'old'"));
    }

    [Fact]
    public async Task Repeatable_read_snapshot_taken_before_compaction_misses_moved_rows()   // документирует §5.3 «MVCC»
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day);
        var live = await EnqueueLiveAsync(day, 2);
        await InsertTerminalAsync(day, 150);
        await using var rr = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await rr.BeginTransactionAsync(IsolationLevel.RepeatableRead, Ct);
        await using (var snapshot = new NpgsqlCommand(Db.Sql("SELECT count(*) FROM {schema}.queue"), rr, tx))
            await snapshot.ExecuteScalarAsync(Ct);

        Assert.True(Assert.Single((await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).Pinned).Compacted);

        await using (var read = new NpgsqlCommand(Db.Sql("SELECT count(*) FROM {schema}.jobs WHERE id = ANY(@ids)"), rr, tx))
        {
            read.Parameters.AddWithValue("ids", live);
            Assert.Equal(0L, (long)(await read.ExecuteScalarAsync(Ct))!);
        }
        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE id = ANY(@ids)", ("ids", live)));
    }

    [Fact]
    public async Task Works_with_multiplexing_data_source()
    {
        await PinnedAsync(Ago(11));
        await CreatePartitionsAsync(Ago(10));
        await InsertTerminalAsync(Ago(10), 3);
        var cs = new NpgsqlConnectionStringBuilder(Db.ConnectionString) { Multiplexing = true }.ConnectionString;
        await using var mux = NpgsqlDataSource.Create(cs);

        var r = await Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }, dataSource: mux).ApplyAsync(Ct);

        Assert.True(Assert.Single(r.Pinned).Compacted);
        Assert.Equal([Ago(10)], r.DroppedJobsPartitions);
        Assert.Equal([Ago(11), Ago(10)], r.DroppedEventsPartitions);
    }

    [Fact]
    public async Task Concurrent_partition_creation_neither_deadlocks_nor_fails()   // Review Focus 2
    {
        await PinnedAsync(Ago(12));
        for (var d = 11; d > 5; d--)
        {
            await CreatePartitionsAsync(Ago(d));
            await InsertTerminalAsync(Ago(d), 3);
        }
        var manager = new PartitionManager(Db.DataSource, Db.Schema, 80, TimeSpan.FromSeconds(2));
        var ensure = Task.Run(async () =>
        {
            for (var days = 3; days <= 14; days++)
                await manager.EnsureAsync(days, Ct);
        }, Ct);
        var retention = Runner(o => { o.JobsRetention = Days3; o.EventsRetention = Days3; }).ApplyAsync(Ct);

        await Task.WhenAll(ensure, retention).WaitAsync(TimeSpan.FromSeconds(60), Ct);

        Assert.All((await retention).Deferred, d => Assert.Equal(DeferReason.LockTimeout, d.Reason));
        Assert.True(await AttachedAsync(J(Today.AddDays(14))));
        Assert.Equal(0L, await OrphansAsync());
    }
}

public sealed class RetentionScenarioTests_Direct(PostgresFixture pg) : RetentionScenarioTests(pg, ConnectionMode.Direct);
public sealed class RetentionScenarioTests_Bouncer(PostgresFixture pg) : RetentionScenarioTests(pg, ConnectionMode.Bouncer);

/// <summary>Только Direct: верхняя граница порога компактирования укладывается в HoldStatementTimeout (§5.3 «Порог»).</summary>
public sealed class RetentionCapacityTests(PostgresFixture pg) : RetentionTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Compaction_of_ten_thousand_live_jobs_fits_hold_timeout()   // Review Focus 5
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await EnqueueLiveAsync(day, 10_000, args: $$"""{"blob":"{{new string('y', 1000)}}"}""");
        await InsertTerminalAsync(day, 10_000);

        var r = await Runner(o => { o.JobsRetention = TimeSpan.FromDays(3); o.RetentionCompactionMaxLiveJobs = 10_000; }).ApplyAsync(Ct);

        Assert.Equal(new PinnedPartition(day, 6, 10_000, true), Assert.Single(r.Pinned));
        Assert.Empty(r.Deferred);
    }
}
```

Если PgBouncer не передаёт стартовый параметр `TimeZone` (первая проверка `current_setting` в `Session_time_zone_does_not_shift_days`), не ослаблять тест: доложить (`STATUS: BLOCKED`) — это меняет гарантию §3 для режима Bouncer.

- [x] **Step 2: Slim data source**

В `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs` добавить `using PgBolt.Postgres;` и тест:

```csharp
    [Fact]
    public async Task Maintenance_operations_work_on_slim_data_source()
    {
        await new MigrationRunner(_slim, _db.Schema).MigrateAsync(Ct);
        Assert.False((await new PartitionManager(_slim, _db.Schema, 80, TimeSpan.FromSeconds(2)).EnsureAsync(1, Ct)).TimedOut);
        var store = new PgBoltStore(_slim, _db.Schema, usePreparedStatements: false);

        var claimedAt = await store.TryClaimMaintenanceAsync("retention", TimeSpan.FromHours(1), Ct);
        Assert.True(await store.CompleteMaintenanceAsync("retention", claimedAt!.Value, null, TimeSpan.FromMinutes(1), Ct));
        Assert.Equal(0, await store.RemoveDeadServersAsync(TimeSpan.FromMinutes(30), Ct));
        await store.RegisterServerAsync(Server, "s", "h", ["slim"], 1, Ct);
        Assert.True((await store.HeartbeatAsync(Server, [], TimeSpan.FromMinutes(1), Ct)).ServerPresent);

        var today = await _db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");
        var day = today.AddDays(-10);
        var suffix = day.ToString("yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
        await _db.ExecAsync($"""
            CREATE TABLE {{schema}}.jobs_{suffix} (LIKE {{schema}}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 80);
            ALTER TABLE {{schema}}.jobs ATTACH PARTITION {{schema}}.jobs_{suffix}
                FOR VALUES FROM ('{UuidV7.LowerBound(day)}') TO ('{UuidV7.LowerBound(day.AddDays(1))}');
            """);
        Guid IdOn() => Guid.CreateVersion7(new DateTimeOffset(day.ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero));
        Guid[] live = [IdOn(), IdOn()];
        await store.EnqueueAsync(new EnqueueBatch("slim.job", "slim", 0, 3, null, null, TimeSpan.Zero, live,
            ["{}", "{}"], [null, null], [null, null]), null, Ct);
        await _db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at)
            SELECT id, 'slim.job', 'slim', 0, '{}', 2, 1, 3, now(), now() FROM unnest(@ids) AS id
            """, ("ids", Enumerable.Range(0, 150).Select(_ => IdOn()).ToArray()));

        var options = new PgBoltOptions { ConnectionString = "Host=unused", Schema = _db.Schema, JobsRetention = TimeSpan.FromDays(3), EventsRetention = null };
        var r = await new RetentionRunner(_slim, options, RetentionTuning.Default).ApplyAsync(Ct);

        Assert.True(Assert.Single(r.Pinned).Compacted);
        Assert.Equal(2L, await _db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.jobs_{suffix}"));
    }
```

- [x] **Step 3: Запустить**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RetentionScenarioTests|FullyQualifiedName~RetentionCapacityTests"` и `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~SlimDataSourceTests"`
Expected: PASS. Если тест падает из-за дефекта `RetentionRunner` — исправить код (не тест), указать дефект и исправление в отчёте.

- [x] **Step 4: Прогон фазы**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add tests/PgBolt.Postgres.Tests/RetentionScenarioTests.cs tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs src/PgBolt.Postgres/Schema
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(core): retention scenarios under load, overlap, time zones and multiplexing"
```

---
## Фаза 2. Расписание в `PgBolt.Server`

### Task 9: Настройки обработки и `RetentionReport`

Спека: §6.1, §8, XML-doc `RunMaintenance` (§6.1).

**Files:**
- Modify: `src/PgBolt.Server/PgBoltProcessingOptions.cs`
- Create: `src/PgBolt.Server/Runtime/RetentionReport.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs`, `tests/PgBolt.Server.Tests/Unit/RetentionReportTests.cs` (создать)

**Interfaces:**
- Consumes: `RetentionResult`, `PartitionCatalog.RetentionDays` (Task 5), `PgBoltOptions` (Task 2).
- Produces: `PgBoltProcessingOptions.MaintenancePollInterval`, `PartitionsInterval`, `ServersCleanupInterval`, `RetentionInterval`, `DeadServerTimeout`; `RetentionReport.Write`.

- [x] **Step 1: Тесты настроек**

В `ServerOptionsTests` добавить:

```csharp
    [Fact]
    public void Maintenance_defaults_match_spec()
    {
        var p = new PgBoltProcessingOptions();
        Assert.Equal(TimeSpan.FromMinutes(1), p.MaintenancePollInterval);
        Assert.Equal(TimeSpan.FromHours(1), p.PartitionsInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), p.ServersCleanupInterval);
        Assert.Equal(TimeSpan.FromHours(1), p.RetentionInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), p.DeadServerTimeout);
    }

    [Theory]
    [InlineData("MaintenancePollInterval")]
    [InlineData("PartitionsInterval")]
    [InlineData("ServersCleanupInterval")]
    [InlineData("RetentionInterval")]
    [InlineData("DeadServerTimeout")]
    public void Maintenance_durations_must_be_positive_and_at_most_one_day(string field)
    {
        foreach (var bad in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1), TimeSpan.FromDays(1) + TimeSpan.FromTicks(1) })
        {
            var e = Assert.Throws<InvalidOperationException>(() => WithMaintenance(field, bad).Validate());
            Assert.Contains(field, e.Message, StringComparison.Ordinal);
        }
        WithMaintenance(field, TimeSpan.FromDays(1)).Validate();
    }

    private static PgBoltProcessingOptions WithMaintenance(string field, TimeSpan value)
    {
        var p = new PgBoltProcessingOptions();
        switch (field)
        {
            case "MaintenancePollInterval": p.MaintenancePollInterval = value; break;
            case "PartitionsInterval": p.PartitionsInterval = value; break;
            case "ServersCleanupInterval": p.ServersCleanupInterval = value; break;
            case "RetentionInterval": p.RetentionInterval = value; break;
            case "DeadServerTimeout": p.DeadServerTimeout = value; break;
        }
        return p;
    }
```

- [x] **Step 2: Тесты `RetentionReport`**

`tests/PgBolt.Server.Tests/Unit/RetentionReportTests.cs` (весь файл):

```csharp
using Microsoft.Extensions.Logging;
using PgBolt.Postgres;
using PgBolt.Postgres.Schema;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class RetentionReportTests
{
    private static readonly DateOnly D = new(2026, 10, 1);
    private readonly TestLogger _logger = new();
    private readonly PgBoltOptions _core = new();   // PartitionDaysAhead 7, JobsRetention 7 сут, порог 1000

    private static RetentionResult Result(IReadOnlyList<PinnedPartition>? pinned = null, IReadOnlyList<DeferredPartition>? deferred = null,
        int attached = 15, IReadOnlyList<DateOnly>? jobs = null, IReadOnlyList<DateOnly>? events = null, int remnants = 0) =>
        new(jobs ?? [], events ?? [], remnants, pinned ?? [], deferred ?? [], attached);

    private void Write(RetentionResult r) => RetentionReport.Write(_logger, r, _core);

    [Fact]
    public void Quiet_run_writes_nothing()
    {
        Write(Result());
        Assert.Empty(_logger.Entries);
    }

    [Fact]
    public void Dropped_partitions_are_one_information_line()
    {
        Write(Result(jobs: [D, D.AddDays(1)], events: [D], remnants: 2));

        var e = Assert.Single(_logger.Entries);
        Assert.Equal(LogLevel.Information, e.Level);
        Assert.Contains("dropped 2 jobs partitions [2026-10-01, 2026-10-02], 1 job_events partitions [2026-10-01] and 2 compaction remnants", e.Message);
    }

    [Fact]
    public void Pinned_partitions_are_summarised_in_one_information_line()
    {
        Write(Result(
            pinned: [new PinnedPartition(D, 1, 5, true), new PinnedPartition(D.AddDays(1), 0, 3, false)],
            deferred:
            [
                new DeferredPartition("jobs", D.AddDays(-1), DeferReason.LiveJobs, 2, 1001),
                new DeferredPartition("jobs", D.AddDays(2), DeferReason.Replicated, 0, 4),
            ]));

        var e = Assert.Single(_logger.At(LogLevel.Information));
        Assert.Contains("4 jobs partitions are pinned by live jobs: 1 compacted now, 1 left as is", e.Message);
        Assert.Contains("1 not compactable (more than 1000 live jobs), 1 skipped (logical replication); 1013 live jobs", e.Message);
        Assert.Contains("oldest day 2026-09-30", e.Message);
        Assert.Empty(_logger.At(LogLevel.Warning));   // просрочка меньше 7 суток
    }

    [Fact]
    public void Too_many_live_jobs_warn_per_partition_from_seven_overdue_days()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.LiveJobs, 7, 1001),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.LiveJobs, 6, 1001),
        ]));

        var w = Assert.Single(_logger.At(LogLevel.Warning));
        Assert.Contains("jobs partition 2026-10-01 holds >1000 live jobs", w.Message);
    }

    [Fact]
    public void Replicated_partitions_warn_once_from_seven_overdue_days()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.Replicated, 9, 3),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.Replicated, 8, 2),
        ]));

        var w = Assert.Single(_logger.At(LogLevel.Warning));
        Assert.Contains("compaction of 2 pinned jobs partitions is skipped because jobs is in a logical replication publication", w.Message);
    }

    [Theory]
    [InlineData(45, false)]
    [InlineData(46, true)]
    public void Attached_partitions_above_normal_plus_slack_warn(int attached, bool warns)
    {
        Write(Result(attached: attached));   // штатно 7 + 7 + 1 = 15, запас 30

        Assert.Equal(warns ? 1 : 0, _logger.At(LogLevel.Warning).Count(e => e.Message.Contains("jobs partitions are attached, normally 15")));
    }

    [Fact]
    public void No_partition_count_warning_without_jobs_retention()
    {
        _core.JobsRetention = null;
        Write(Result(attached: 1000));
        Assert.Empty(_logger.Entries);
    }

    [Fact]
    public void Hold_timeouts_warn_once_per_table()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.HoldTimeout, 0, null),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.HoldTimeout, 0, null),
            new DeferredPartition("job_events", D, DeferReason.HoldTimeout, 0, null),
        ]));

        Assert.Equal(2, _logger.At(LogLevel.Warning).Count(e => e.Message.Contains("hit statement_timeout or idle_in_transaction_session_timeout")));
    }

    [Fact]
    public void Lock_timeouts_are_one_line_per_table_warning_only_when_overdue()
    {
        Write(Result(deferred:
        [
            new DeferredPartition("jobs", D, DeferReason.LockTimeout, 0, null),
            new DeferredPartition("jobs", D.AddDays(1), DeferReason.DropLockTimeout, 1, null),
            new DeferredPartition("job_events", D, DeferReason.LockTimeout, 0, null),
        ]));

        Assert.Contains("2 jobs partitions were deferred by lock_timeout (overdue by up to 1 days)", Assert.Single(_logger.At(LogLevel.Warning)).Message);
        Assert.Contains("1 job_events partitions were deferred by lock_timeout", Assert.Single(_logger.At(LogLevel.Debug)).Message);
    }

    [Fact]
    public void Drop_failure_is_an_error_with_its_text()
    {
        Write(Result(deferred: [new DeferredPartition("job_events", D, DeferReason.DropFailed, 3, null, "permission denied")]));

        var e = Assert.Single(_logger.At(LogLevel.Error));
        Assert.Contains("dropping the detached job_events table of 2026-10-01 failed: permission denied", e.Message);
    }
}
```

- [x] **Step 3: Запустить — падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ServerOptionsTests|FullyQualifiedName~RetentionReportTests"`
Expected: FAIL (компиляция).

- [x] **Step 4: `PgBoltProcessingOptions`**

Заменить XML-doc `RunMaintenance` и добавить свойства после `SweeperInterval`:

```csharp
    /// <summary>
    /// Запускать в этом процессе promoter, sweeper и обслуживание хранилища (часть 2 §6.2): партиции, уборку servers, retention.
    /// Если ни один процесс, работающий со схемой, не выполняет обслуживание (у всех false и нет ручного вызова
    /// IPgBoltMaintenance), партиции кончатся через PartitionDaysAhead суток и enqueue начнёт падать; retention и уборка servers
    /// тоже не выполняются.
    /// </summary>
    public bool RunMaintenance { get; set; } = true;
```

```csharp
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
```

`Validate()` — заменить тело:

```csharp
    internal void Validate()
    {
        Check(PromoterInterval, nameof(PromoterInterval));
        Check(SweeperInterval, nameof(SweeperInterval));
        Check(MaintenancePollInterval, nameof(MaintenancePollInterval));
        Check(PartitionsInterval, nameof(PartitionsInterval));
        Check(ServersCleanupInterval, nameof(ServersCleanupInterval));
        Check(RetentionInterval, nameof(RetentionInterval));
        Check(DeadServerTimeout, nameof(DeadServerTimeout));
    }

    private static void Check(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value > PgBoltServerOptions.MaxInterval)
            throw new InvalidOperationException($"PgBolt processing: {name} must be > 0 and <= {PgBoltServerOptions.MaxInterval}.");
    }
```

Текст исключения для `PromoterInterval`/`SweeperInterval` не меняется (на нём держатся существующие тесты).

- [x] **Step 5: `RetentionReport.cs`** (весь файл)

```csharp
using System.Globalization;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres;
using PgBolt.Postgres.Schema;

namespace PgBolt.Server.Runtime;

/// <summary>Логи по результату retention (часть 2 §8): core логов не пишет, уровни выбираются здесь.</summary>
internal static class RetentionReport
{
    internal const int PinnedWarningDays = 7;
    internal const int PartitionCountSlack = 30;

    public static void Write(ILogger logger, RetentionResult result, PgBoltOptions core)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(result);
        ArgumentNullException.ThrowIfNull(core);

        if (result.DroppedJobsPartitions.Count + result.DroppedEventsPartitions.Count + result.DroppedCompactionRemnants > 0)
            logger.LogInformation(
                "PgBolt retention: dropped {JobsCount} jobs partitions [{JobsDays}], {EventsCount} job_events partitions [{EventsDays}] and {Remnants} compaction remnants.",
                result.DroppedJobsPartitions.Count, Days(result.DroppedJobsPartitions),
                result.DroppedEventsPartitions.Count, Days(result.DroppedEventsPartitions), result.DroppedCompactionRemnants);

        var max = core.RetentionCompactionMaxLiveJobs;
        var tooMany = result.Deferred.Where(d => d.Reason == DeferReason.LiveJobs).ToList();
        var replicated = result.Deferred.Where(d => d.Reason == DeferReason.Replicated).ToList();
        var pinned = result.Pinned.Count + tooMany.Count + replicated.Count;
        if (pinned > 0)
        {
            var live = result.Pinned.Sum(p => (long)p.LiveJobs) + tooMany.Concat(replicated).Sum(d => (long)(d.LiveJobs ?? 0));
            var oldest = result.Pinned.Select(p => p.Day).Concat(tooMany.Concat(replicated).Select(d => d.Day)).Min();
            logger.LogInformation(
                "PgBolt retention: {Count} jobs partitions are pinned by live jobs: {Compacted} compacted now, {Small} left as is (few terminal rows), {TooMany} not compactable (more than {Max} live jobs), {Replicated} skipped (logical replication); {Live} live jobs in total, oldest day {Oldest}.",
                pinned, result.Pinned.Count(p => p.Compacted), result.Pinned.Count(p => !p.Compacted), tooMany.Count, max,
                replicated.Count, live, Day(oldest));
        }

        foreach (var d in tooMany.Where(d => d.OverdueDays >= PinnedWarningDays))
            logger.LogWarning(
                "PgBolt retention: jobs partition {Day} holds {Live} live jobs, more than RetentionCompactionMaxLiveJobs ({Max}); it is not compacted and keeps its whole disk space until they finish (overdue by {Overdue} days). Run, cancel or delete these jobs, or raise the limit.",
                Day(d.Day), LiveText(d.LiveJobs, max), max, d.OverdueDays);

        var oldReplicated = replicated.Where(d => d.OverdueDays >= PinnedWarningDays).ToList();
        if (oldReplicated.Count > 0)
            logger.LogWarning(
                "PgBolt retention: compaction of {Count} pinned jobs partitions is skipped because jobs is in a logical replication publication; they keep their disk space until their live jobs finish (oldest day {Oldest}).",
                oldReplicated.Count, Day(oldReplicated.Min(d => d.Day)));

        if (core.JobsRetention is { } retention)
        {
            var normal = core.PartitionDaysAhead + PartitionCatalog.RetentionDays(retention) + 1;
            if (result.AttachedJobsPartitions > normal + PartitionCountSlack)
                logger.LogWarning(
                    "PgBolt retention: {Attached} jobs partitions are attached, normally {Normal}; days pinned by live jobs stay attached and slow down dequeue planning.",
                    result.AttachedJobsPartitions, normal);
        }

        foreach (var table in result.Deferred.Where(d => d.Reason == DeferReason.HoldTimeout).GroupBy(d => d.Table))
            logger.LogWarning(
                "PgBolt retention: holding the lock on {Table} hit statement_timeout or idle_in_transaction_session_timeout ({Count} partitions deferred); the hot path waited up to the hold limit. Likely a slow disk or a process pause (GC).",
                table.Key, table.Count());

        foreach (var table in result.Deferred.Where(d => d.Reason is DeferReason.LockTimeout or DeferReason.DropLockTimeout).GroupBy(d => d.Table))
        {
            var overdue = table.Max(d => d.OverdueDays);
            logger.Log(overdue >= 1 ? LogLevel.Warning : LogLevel.Debug,
                "PgBolt retention: {Count} {Table} partitions were deferred by lock_timeout (overdue by up to {Overdue} days); long queries or a holder of the partition itself (autovacuum, direct query) block their removal.",
                table.Count(), table.Key, overdue);
        }

        foreach (var d in result.Deferred.Where(d => d.Reason == DeferReason.DropFailed))
            logger.LogError("PgBolt retention: dropping the detached {Table} table of {Day} failed: {Error}; it will be retried as a remnant.",
                d.Table, Day(d.Day), d.Error);
    }

    private static string Day(DateOnly day) => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private static string Days(IEnumerable<DateOnly> days) => string.Join(", ", days.Select(Day));

    /// <summary>LiveJobs = T + 1 означает «больше T» (§5.1).</summary>
    private static string LiveText(int? live, int max) =>
        live > max ? $">{max}" : (live ?? 0).ToString(CultureInfo.InvariantCulture);
}
```

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ServerOptionsTests|FullyQualifiedName~RetentionReportTests"`
Expected: PASS.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Server/PgBoltProcessingOptions.cs src/PgBolt.Server/Runtime/RetentionReport.cs tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs tests/PgBolt.Server.Tests/Unit/RetentionReportTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): maintenance intervals and retention report"
```

---

### Task 10: Цикл `housekeeping` и предупреждения при старте

Спека: §6.2, §6.3, §6.4, §8, §12 «Worker».

**Files:**
- Modify: `src/PgBolt.Server/Runtime/IServerStore.cs`, `src/PgBolt.Server/Runtime/PgBoltServerStore.cs`, `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs`, `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`
- Create: `tests/PgBolt.Server.Tests/Fakes/FakeMaintenance.cs`, `tests/PgBolt.Server.Tests/Unit/HousekeepingTests.cs`
- Modify: `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`, `tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs`, `tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs`
- Modify (гонка с housekeeping): `tests/PgBolt.Server.Tests/Integration/LeaseFailureTests.cs`

**Interfaces:**
- Consumes: `PgBoltStore.TryClaimMaintenanceAsync`/`CompleteMaintenanceAsync` (Task 3), `IPgBoltMaintenance` (Task 2, 3, 6), `RetentionReport` и новые настройки (Task 9).
- Produces: `IServerStore.TryClaimMaintenanceAsync`, `CompleteMaintenanceAsync`; конструктор `ProcessingMaintenance(store, wakeup, options, maintenance, core, clock, logger)`; `HousekeepingOnceAsync`, `HousekeepingRetryDelay`; `FakeServerStore.OnClaim`/`OnComplete`; `FakeMaintenance`.

- [x] **Step 1: Фейки**

`FakeServerStore` — добавить делегаты и методы:

```csharp
    /// <summary>По умолчанию задачи обслуживания не захватываются (ни один тест, кроме housekeeping, их не ждёт).</summary>
    public Func<string, TimeSpan, CancellationToken, Task<DateTime?>> OnClaim { get; set; } =
        (_, _, _) => Task.FromResult<DateTime?>(null);

    public Func<string, DateTime, string?, TimeSpan?, CancellationToken, Task> OnComplete { get; set; } =
        (_, _, _, _, _) => Task.CompletedTask;

    public Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Claim", Detail: (name, interval)));
        return OnClaim(name, interval, ct);
    }

    public Task CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Complete", Detail: (name, claimedAt, error, retryDelay)));
        return OnComplete(name, claimedAt, error, retryDelay, ct);
    }
```

`tests/PgBolt.Server.Tests/Fakes/FakeMaintenance.cs` (весь файл):

```csharp
using System.Collections.Concurrent;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Schema;

namespace PgBolt.Server.Tests.Fakes;

/// <summary>Программируемый IPgBoltMaintenance; вызовы пишутся в Calls до вызова делегата.</summary>
internal sealed class FakeMaintenance : IPgBoltMaintenance
{
    public static RetentionResult Empty { get; } = new([], [], 0, [], [], 0);

    public ConcurrentQueue<(string Op, object? Detail)> Calls { get; } = new();

    public Func<CancellationToken, Task<PartitionEnsureResult>> OnEnsure { get; set; } =
        _ => Task.FromResult(new PartitionEnsureResult(0, TimedOut: false, TodayPresent: true));

    public Func<CancellationToken, Task<RetentionResult>> OnRetention { get; set; } = _ => Task.FromResult(Empty);

    public Func<TimeSpan, CancellationToken, Task<int>> OnRemoveDeadServers { get; set; } = (_, _) => Task.FromResult(0);

    public IEnumerable<string> Ops => Calls.Select(c => c.Op);

    public Task<PartitionEnsureResult> EnsurePartitionsAsync(int? daysAhead = null, CancellationToken ct = default)
    {
        Calls.Enqueue(("Ensure", daysAhead));
        return OnEnsure(ct);
    }

    public Task<RetentionResult> ApplyRetentionAsync(CancellationToken ct = default)
    {
        Calls.Enqueue(("Retention", null));
        return OnRetention(ct);
    }

    public Task<int> RemoveDeadServersAsync(TimeSpan olderThan, CancellationToken ct = default)
    {
        Calls.Enqueue(("RemoveDeadServers", olderThan));
        return OnRemoveDeadServers(olderThan, ct);
    }
}
```

- [x] **Step 2: Тесты `housekeeping`**

`tests/PgBolt.Server.Tests/Unit/HousekeepingTests.cs` (весь файл):

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Schema;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class HousekeepingTests
{
    private static readonly DateTime ClaimedAt = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Retry = TimeSpan.FromMinutes(1);

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly FakeMaintenance _maintenance = new();
    private readonly ListLoggerFactory _logs = new();
    private readonly PgBoltProcessingOptions _options = new();
    private readonly PgBoltOptions _core = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create() =>
        new(_store, new InProcessWakeupSignal(), _options, _maintenance, _core, new MonoClock(_time), _logs.CreateLogger("test"))
        {
            NextRandom = () => 0.5,
        };

    private void Claim(params string[] names) =>
        _store.OnClaim = (name, _, _) => Task.FromResult<DateTime?>(names.Contains(name) ? ClaimedAt : null);

    private (string Name, DateTime ClaimedAt, string? Error, TimeSpan? Retry)[] Completes =>
        [.. _store.CallsOf("Complete").Select(c => ((string, DateTime, string?, TimeSpan?))c.Detail!)];

    private static (string Name, DateTime ClaimedAt, string? Error, TimeSpan? Retry) Done(string name, string? error = null, TimeSpan? retry = null) =>
        (name, ClaimedAt, error, retry);

    [Fact]
    public async Task Claimed_tasks_run_in_order_and_complete_successfully()
    {
        Claim("partitions", "servers", "retention");

        Assert.Equal(3, await Create().HousekeepingOnceAsync(Ct));

        Assert.Equal(["Ensure", "RemoveDeadServers", "Retention"], _maintenance.Ops);
        Assert.Equal(
            new[] { ("partitions", TimeSpan.FromHours(1)), ("servers", TimeSpan.FromMinutes(5)), ("retention", TimeSpan.FromHours(1)) },
            _store.CallsOf("Claim").Select(c => ((string, TimeSpan))c.Detail!));
        Assert.Equal(new[] { Done("partitions"), Done("servers"), Done("retention") }, Completes);
        Assert.Equal(new (string, object?)[] { ("Ensure", null), ("RemoveDeadServers", TimeSpan.FromMinutes(30)), ("Retention", null) },
            _maintenance.Calls.ToArray());
    }

    [Fact]
    public async Task Unclaimed_tasks_do_not_run()
    {
        Claim("servers");

        Assert.Equal(1, await Create().HousekeepingOnceAsync(Ct));

        Assert.Equal(["RemoveDeadServers"], _maintenance.Ops);
        Assert.Single(Completes);
    }

    [Theory]
    [InlineData(true, LogLevel.Warning)]
    [InlineData(false, LogLevel.Error)]
    public async Task Partitions_lock_timeout_completes_with_accelerated_retry(bool todayPresent, LogLevel level)
    {
        Claim("partitions");
        _maintenance.OnEnsure = _ => Task.FromResult(new PartitionEnsureResult(0, TimedOut: true, TodayPresent: todayPresent));

        await Create().HousekeepingOnceAsync(Ct);

        Assert.Equal(new[] { Done("partitions", "lock_timeout", Retry) }, Completes);
        Assert.Single(_logs.At(level), e => e.Message.Contains("partitions"));
    }

    [Fact]
    public async Task Failed_operation_is_completed_with_error_and_next_task_runs()
    {
        Claim("partitions", "servers", "retention");
        _maintenance.OnEnsure = _ => Task.FromException<PartitionEnsureResult>(new TimeoutException("db down"));
        _maintenance.OnRetention = _ => Task.FromException<RetentionResult>(new InvalidOperationException("boom"));

        Assert.Equal(3, await Create().HousekeepingOnceAsync(Ct));

        Assert.Equal(new[] { Done("partitions", "db down", Retry), Done("servers"), Done("retention", "boom") },
            Completes);   // retention ускоренно не повторяется никогда
        Assert.Equal(2, _logs.At(LogLevel.Error).Count(e => e.Message.Contains("maintenance task")));
    }

    [Fact]
    public async Task Servers_failure_gets_accelerated_retry()
    {
        Claim("servers");
        _maintenance.OnRemoveDeadServers = (_, _) => Task.FromException<int>(new TimeoutException("db down"));

        await Create().HousekeepingOnceAsync(Ct);

        Assert.Equal(new[] { Done("servers", "db down", Retry) }, Completes);
    }

    [Fact]
    public async Task Retention_deferrals_are_not_errors_and_are_reported()
    {
        Claim("retention");
        var d = new DateOnly(2026, 9, 1);
        _maintenance.OnRetention = _ => Task.FromResult(FakeMaintenance.Empty with
        {
            Deferred =
            [
                new DeferredPartition("jobs", d, DeferReason.LockTimeout, 3, null),
                new DeferredPartition("jobs", d.AddDays(1), DeferReason.HoldTimeout, 2, null),
            ],
        });

        await Create().HousekeepingOnceAsync(Ct);

        Assert.Equal(new[] { Done("retention") }, Completes);
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("deferred by lock_timeout"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("hit statement_timeout"));
    }

    [Fact]
    public async Task Claim_failure_is_logged_by_the_loop_and_retried_after_poll_interval()
    {
        var claims = 0;
        _store.OnClaim = (_, _, _) => Interlocked.Increment(ref claims) == 1
            ? Task.FromException<DateTime?>(new TimeoutException("db down"))
            : Task.FromResult<DateTime?>(null);
        var maintenance = Create();
        maintenance.Start();

        await Eventually.True(() => maintenance.IdleWaits == 3, "promoter, sweeper and housekeeping wait for their intervals");
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("housekeeping"));
        Assert.Equal(1, Volatile.Read(ref claims));   // итерация прервана на первом захвате

        _time.Advance(_options.MaintenancePollInterval);
        await Eventually.True(() => Volatile.Read(ref claims) == 4, "next iteration tries all three tasks");
        await maintenance.StopAsync();
    }

    [Fact]
    public async Task Stop_cancels_running_task_without_complete()
    {
        Claim("retention");
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _maintenance.OnRetention = async ct =>
        {
            started.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return FakeMaintenance.Empty;
        };
        var maintenance = Create();
        maintenance.Start();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        await maintenance.StopAsync().WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Empty(Completes);
    }
}
```

- [x] **Step 3: Тесты предупреждений при старте**

В `ProcessingServiceTests` добавить свойство и тесты:

```csharp
    private PgBoltOptions CoreOptions =>
        (PgBoltOptions)_services.Single(d => d.ServiceType == typeof(PgBoltOptions)).ImplementationInstance!;

    [Fact]
    public async Task Warns_about_short_partition_horizon_and_dead_server_timeout()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));   // HeartbeatInterval 15 с
        _services.ConfigurePgBoltProcessing(p =>
        {
            p.PartitionsInterval = TimeSpan.FromDays(1);
            p.DeadServerTimeout = TimeSpan.FromSeconds(30);
        });
        CoreOptions.PartitionDaysAhead = 1;

        await StartAsync();

        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("PartitionDaysAhead"));
        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("DeadServerTimeout"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task No_maintenance_warnings_with_defaults_or_without_maintenance(bool runMaintenance)
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p =>
        {
            p.RunMaintenance = runMaintenance;
            if (!runMaintenance)
                p.DeadServerTimeout = TimeSpan.FromSeconds(1);
        });

        await StartAsync();

        Assert.DoesNotContain(_logs.At(LogLevel.Warning), e => e.Message.Contains("PartitionDaysAhead") || e.Message.Contains("DeadServerTimeout"));
    }
```

`using PgBolt.Postgres;` — добавить, если его нет.

- [x] **Step 4: Обновить `ProcessingMaintenanceTests`**

`Create()`:

```csharp
    private ProcessingMaintenance Create() =>
        new(_store, _wakeup, _options, new FakeMaintenance(), new PgBoltOptions(), new MonoClock(_time), _logs.CreateLogger("test"))
        {
            NextRandom = () => 0.5,
        };
```

В `Loop_error_is_logged_and_retried_after_interval` и `Stop_ends_both_loops` заменить `maintenance.IdleWaits == 2` на `maintenance.IdleWaits == 3` и текст причины на `"promoter, sweeper and housekeeping are waiting"`; добавить `using PgBolt.Postgres;`.

- [x] **Step 5: Запустить — падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~HousekeepingTests|FullyQualifiedName~ProcessingMaintenanceTests|FullyQualifiedName~ProcessingServiceTests"`
Expected: FAIL (компиляция).

- [x] **Step 6: `IServerStore` и `PgBoltServerStore`**

В `IServerStore`:

```csharp
    Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct);
    Task CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct);
```

В `PgBoltServerStore`:

```csharp
    public Task<DateTime?> TryClaimMaintenanceAsync(string name, TimeSpan interval, CancellationToken ct) =>
        _store.TryClaimMaintenanceAsync(name, interval, ct);

    /// <summary>false от store (строку перезахватил другой под, §5.7) — штатно: результат перекрывшегося запуска не пишется.</summary>
    public Task CompleteMaintenanceAsync(string name, DateTime claimedAt, string? error, TimeSpan? retryDelay, CancellationToken ct) =>
        _store.CompleteMaintenanceAsync(name, claimedAt, error, retryDelay, ct);
```

- [x] **Step 7: `ProcessingMaintenance`**

Заменить первичный конструктор и XML-doc класса, `Start`, добавить методы:

```csharp
/// <summary>
/// Promoter, sweeper и обслуживание хранилища процесса (часть 3 §6.3, часть 2 §6.2). Promoter и sweeper выполняют все поды
/// (SKIP LOCKED); задачи обслуживания — только под, захвативший их в maintenance_tasks.
/// </summary>
internal sealed class ProcessingMaintenance(IServerStore store, IWakeupSignal wakeup, PgBoltProcessingOptions options,
    IPgBoltMaintenance maintenance, PgBoltOptions core, MonoClock clock, ILogger logger)
{
    internal const int BatchSize = 1000;
    internal static readonly TimeSpan HousekeepingRetryDelay = TimeSpan.FromMinutes(1);
```

```csharp
    public void Start() =>
        _loops = Task.WhenAll(
            Task.Run(() => LoopAsync("promoter", PromoteOnceAsync, options.PromoterInterval)),
            Task.Run(() => LoopAsync("sweeper", SweepOnceAsync, options.SweeperInterval)),
            Task.Run(() => LoopAsync("housekeeping", HousekeepingOnceAsync, options.MaintenancePollInterval)));
```

```csharp
    /// <summary>
    /// §6.2: задачи partitions, servers, retention по порядку; выполняются только захваченные. Исключение захвата или Complete
    /// прерывает итерацию (его логирует цикл). Возвращает число выполненных задач.
    /// </summary>
    internal async Task<int> HousekeepingOnceAsync(CancellationToken ct)
    {
        var ran = 0;
        if (await RunTaskAsync("partitions", options.PartitionsInterval, EnsurePartitionsAsync, HousekeepingRetryDelay, ct))
            ran++;
        if (await RunTaskAsync("servers", options.ServersCleanupInterval, RemoveDeadServersAsync, HousekeepingRetryDelay, ct))
            ran++;
        if (await RunTaskAsync("retention", options.RetentionInterval, ApplyRetentionAsync, errorRetryDelay: null, ct))
            ran++;
        return ran;
    }

    /// <param name="errorRetryDelay">Ускоренный повтор после исключения; null — не раньше следующего интервала (retention).</param>
    private async Task<bool> RunTaskAsync(string name, TimeSpan interval,
        Func<CancellationToken, Task<(string? Error, TimeSpan? RetryDelay)>> run, TimeSpan? errorRetryDelay, CancellationToken ct)
    {
        if (await store.TryClaimMaintenanceAsync(name, interval, ct) is not { } claimedAt)
            return false;
        string? error;
        TimeSpan? retryDelay;
        try
        {
            (error, retryDelay) = await run(ct);
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            logger.LogError(e, "PgBolt: maintenance task {Task} failed.", name);
            (error, retryDelay) = (e.Message, errorRetryDelay);
        }
        await store.CompleteMaintenanceAsync(name, claimedAt, error, retryDelay, ct);
        return true;
    }

    private async Task<(string? Error, TimeSpan? RetryDelay)> EnsurePartitionsAsync(CancellationToken ct)
    {
        var result = await maintenance.EnsurePartitionsAsync(null, ct);
        if (!result.TimedOut)
            return (null, null);
        if (result.TodayPresent)
            logger.LogWarning("PgBolt: future partitions were not created (lock_timeout); retrying in {Delay}.", HousekeepingRetryDelay);
        else
            logger.LogError(
                "PgBolt: today's partitions are missing and could not be created (lock_timeout); enqueue fails until they exist. Retrying in {Delay}.",
                HousekeepingRetryDelay);
        return ("lock_timeout", HousekeepingRetryDelay);
    }

    private async Task<(string? Error, TimeSpan? RetryDelay)> RemoveDeadServersAsync(CancellationToken ct)
    {
        var removed = await maintenance.RemoveDeadServersAsync(options.DeadServerTimeout, ct);
        if (removed > 0)
            logger.LogInformation("PgBolt: removed {Count} servers rows without heartbeat for {Timeout}.", removed, options.DeadServerTimeout);
        return (null, null);
    }

    /// <summary>LockTimeout/HoldTimeout и прочие Deferred — не ошибка задачи (§2): только логи §8.</summary>
    private async Task<(string? Error, TimeSpan? RetryDelay)> ApplyRetentionAsync(CancellationToken ct)
    {
        RetentionReport.Write(logger, await maintenance.ApplyRetentionAsync(ct), core);
        return (null, null);
    }
```

Добавить `using PgBolt.Postgres;` и `using PgBolt.Postgres.Hosting;`. Тексты логов `PartitionsTimedOut` содержат слово `partitions` — на нём держится тест Step 2.

- [x] **Step 8: `PgBoltProcessingService`**

В `StartAsync`: перед `WarnIfHostShutdownTooShort();` получить настройки и вызвать проверку; в `try` использовать ту же переменную:

```csharp
        var processing = _services.GetRequiredService<PgBoltProcessingOptions>();
        var core = _services.GetRequiredService<PgBoltOptions>();
        WarnIfHostShutdownTooShort();                                  // §4.4 п. 5
        WarnIfMaintenanceMisconfigured(processing, core);              // часть 2 §6.4

        try                                                            // §4.4 п. 6
        {
            if (processing.RunMaintenance && _servers.Count > 0)
            {
                Maintenance = new ProcessingMaintenance(store, wakeup, processing, _services.GetRequiredService<IPgBoltMaintenance>(),
                    core, clock, _logger);
                Maintenance.Start();
            }
            foreach (var server in _servers)
                server.Start();
        }
```

Новый метод:

```csharp
    /// <summary>Часть 2 §6.4: конфигурация, при которой обслуживание не выполняет своих гарантий.</summary>
    private void WarnIfMaintenanceMisconfigured(PgBoltProcessingOptions processing, PgBoltOptions core)
    {
        if (!processing.RunMaintenance || _servers.Count == 0)
            return;
        if (TimeSpan.FromDays(core.PartitionDaysAhead) < 2 * processing.PartitionsInterval)
            _logger.LogWarning(
                "PgBolt: PartitionDaysAhead ({DaysAhead} days) is shorter than 2 × PartitionsInterval ({Interval}); a single failed iteration may leave no partition for the next day.",
                core.PartitionDaysAhead, processing.PartitionsInterval);
        var heartbeat = _servers.Max(s => s.Options.HeartbeatInterval);
        if (processing.DeadServerTimeout < 3 * heartbeat)
            _logger.LogWarning(
                "PgBolt: DeadServerTimeout ({Timeout}) is shorter than 3 × HeartbeatInterval ({Heartbeat}); rows of live servers will be removed on short heartbeat hiccups and restored by the next heartbeat.",
                processing.DeadServerTimeout, heartbeat);
    }
```

Добавить `using PgBolt.Postgres;`.

- [x] **Step 9: Гонка интеграционных тестов с первой итерацией housekeeping**

`ServerTestHost` запускает хосты с `RunMaintenance = true` по умолчанию, и цикл `housekeeping` выполняет первую итерацию сразу после старта (`LoopAsync` ждёт интервал только после итерации). В `LeaseFailureTests.Outcome_that_cannot_be_written_within_its_budget_expires_and_sweeper_spends_the_attempt` сразу после `StartHostAsync` удаляется сегодняшняя партиция `job_events` (`DropTodayEventsPartitionAsync`); задача `partitions` первой итерации (`EnsurePartitionsAsync`) может выполниться позже и пересоздать её — тогда исход повтора записывается, и ожидание «retry outcome is abandoned…» падает по таймауту. В этом тесте между `var aId = a.ServerId;` и `await DropTodayEventsPartitionAsync();` вставить:

```csharp
        await WaitUntilAsync(() => Db.ScalarAsync<bool>(
            "SELECT count(*) = 3 AND bool_and(last_finished_at IS NOT NULL) FROM {schema}.maintenance_tasks"),
            TimeSpan.FromSeconds(10), "first housekeeping iteration finished");
```

Почему этого достаточно:
- Следующая итерация — не раньше `MaintenancePollInterval × 0.8` = 48 с после первой; окно теста от удаления партиции до явного `EnsurePartitionsAsync(2)` — не больше 25 с ожиданий (10 + 15). Даже при второй итерации `partitions` не захватывается: `next_run_at` = момент захвата + `PartitionsInterval` (1 ч; `ServerTestHost` интервалы обслуживания не меняет); ускоренный повтор через 1 мин возможен только после `lock_timeout` первой итерации, которого на свежей схеме нет.
- `retention` (сроки по умолчанию 7 сут) на свежей схеме `TestDb` ничего не удаляет: партиций старше сегодняшней, отсоединённых таблиц и остатков нет; `DropTodayEventsPartitionAsync` делает `DETACH` и `DROP` одной транзакцией, остатка не оставляет.
- `servers` (`DeadServerTimeout` 30 мин) строки живых серверов теста не трогает.

Остальные интеграционные тесты `tests/PgBolt.Server.Tests/Integration/*.cs` и `tests/E2E` проверены на аналогичные гонки — правок не требуют:
- партиции удаляет и создаёт только этот тест; строки `servers` удаляет только `SlimDataSourceTests` (напрямую через store, без хоста), `heartbeat_at` в прошлое никто не сдвигает — уборка `servers` (30 мин) их не видит;
- тесты с блокировками (`LOCK TABLE queue IN EXCLUSIVE MODE`, `FOR UPDATE` строки `servers`) и с пулом из двух соединений (`PoolHogArgs`) уже запускают хост с `RunMaintenance = false`; `LOCK TABLE queue` не конфликтует с чтением `queue` предпроверки retention (ACCESS SHARE), кандидатов на свежей схеме всё равно нет;
- `LoadSmokeTests` проверяет число соединений `≤ MaxPoolSize` — housekeeping берёт соединения из того же пула, граница сохраняется;
- `ShutdownTests` (остановка быстрее `ShutdownTimeout`): итерация housekeeping отменяется токеном остановки, `StopAsync` цикла её дожидается без ожидания таймаутов;
- логи Warning/Error проверяет только `BasicScenarioTests` (по тексту `test.orphan`); предупреждения §6.4 при настройках `ServerTestHost` (H = 1 с, `PartitionDaysAhead` = 7) не выдаются.

- [x] **Step 10: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "Category!=Load"`
Expected: PASS.

Затем `dotnet test tests/E2E/PgBolt.E2E.Tests`. Expected: PASS.

- [x] **Step 11: Commit**

```bash
git add src/PgBolt.Server/Runtime tests/PgBolt.Server.Tests tests/PgBolt.Server.Tests/Integration/LeaseFailureTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): housekeeping loop for partitions, servers cleanup and retention"
```

---

### Task 11: Сквозные сценарии обслуживания

Спека: §12 «Worker» (интеграционные), §1 критерии («ровно один под», восстановление строки `servers`).

**Files:**
- Modify: `tests/PgBolt.Server.Tests/Integration/ServerTestHost.cs` (`TestHostSettings`, `ServerTestHost.StartAsync`)
- Create: `tests/PgBolt.Server.Tests/Integration/MaintenanceTests.cs`

**Interfaces:**
- Consumes: всё выше; `ServerIntegrationTestBase.StartHostAsync`, `WaitUntilAsync`.
- Produces: `TestHostSettings.Core` (`Action<PgBoltOptions>?`), `TestHostSettings.Processing` (`Action<PgBoltProcessingOptions>?`).

- [x] **Step 1: Настройки тестового хоста**

В `TestHostSettings` добавить:

```csharp
    /// <summary>Дополнительная настройка core (после Schema и AddJobs).</summary>
    public Action<PgBoltOptions>? Core { get; set; }

    /// <summary>Дополнительная настройка обработки (после RunMaintenance и интервалов promoter/sweeper).</summary>
    public Action<PgBoltProcessingOptions>? Processing { get; set; }
```

В `ServerTestHost.StartAsync`: в лямбде `services.AddPgBolt(o => { … })` последней строкой `settings.Core?.Invoke(o);`; в лямбде `services.ConfigurePgBoltProcessing(p => { … })` последней строкой `settings.Processing?.Invoke(p);`. Добавить `using PgBolt.Postgres;`, если его нет.

- [x] **Step 2: Тесты**

`tests/PgBolt.Server.Tests/Integration/MaintenanceTests.cs` (весь файл):

```csharp
using System.Globalization;
using Microsoft.Extensions.Logging;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class MaintenanceTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private static void FastMaintenance(TestHostSettings s)
    {
        s.Core = o =>
        {
            o.JobsRetention = TimeSpan.FromDays(3);
            o.EventsRetention = TimeSpan.FromDays(3);
            // Dequeue обоих хостов держит блокировки родителя и партиций; единичный LockTimeout при сроке по умолчанию
            // отложил бы удаление до следующего RetentionInterval (час) — тест упал бы по таймауту ожидания.
            o.RetentionLockTimeout = TimeSpan.FromSeconds(5);
        };
        s.Processing = p => p.MaintenancePollInterval = TimeSpan.FromMilliseconds(200);
    }

    private async Task<string> OldPartitionAsync()
    {
        var today = await Db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");
        var day = today.AddDays(-10);
        var suffix = day.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var lo = PgBolt.Postgres.UuidV7.LowerBound(day);
        var hi = PgBolt.Postgres.UuidV7.LowerBound(day.AddDays(1));
        await Db.ExecAsync($"""
            CREATE TABLE {{schema}}.jobs_{suffix} (LIKE {{schema}}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 80);
            ALTER TABLE {{schema}}.jobs ATTACH PARTITION {{schema}}.jobs_{suffix} FOR VALUES FROM ('{lo}') TO ('{hi}');
            INSERT INTO {{schema}}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at)
            VALUES ('{Guid.CreateVersion7(new DateTimeOffset(day.ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero))}', 't', 'q', 0, '{{}}', 2, 1, 3, now(), now());
            """);
        return $"jobs_{suffix}";
    }

    [Fact]
    public async Task Retention_runs_on_exactly_one_of_two_hosts()
    {
        var partition = await OldPartitionAsync();
        var a = await StartHostAsync(s => { s.Name = "a"; FastMaintenance(s); });
        var b = await StartHostAsync(s => { s.Name = "b"; FastMaintenance(s); });

        await WaitUntilAsync(async () => !await Db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{Db.Schema}.{partition}")),
            Timeout, "old partition is dropped");
        await WaitUntilAsync(() => Db.ScalarAsync<bool>(
            "SELECT count(*) = 3 AND bool_and(last_finished_at IS NOT NULL AND last_error IS NULL) FROM {schema}.maintenance_tasks"),
            Timeout, "all three tasks are claimed and completed");

        var reporters = new[] { a, b }.Count(h => h.Logs.Entries.Any(e => e.Message.Contains("PgBolt retention: dropped 1 jobs partitions")));
        Assert.Equal(1, reporters);
    }

    [Fact]
    public async Task Removed_servers_row_of_live_server_is_restored_and_gone_after_stop()
    {
        var host = await StartHostAsync(s => s.RunMaintenance = false);
        var id = host.ServerId;
        await Db.ExecAsync("DELETE FROM {schema}.servers WHERE id = @id", ("id", id));

        await WaitUntilAsync(() => Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.servers WHERE id = @id)", ("id", id)),
            Timeout, "next heartbeat registers the server again");
        Assert.Contains(host.Logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("registered again"));

        await host.StopAsync();

        Assert.False(await Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.servers WHERE id = @id)", ("id", id)));
    }

    [Fact]
    public async Task Dead_servers_rows_are_removed_by_the_servers_task()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
            VALUES (gen_random_uuid(), 'dead', 'h', ARRAY['q'], 1, now() - interval '2 hours', now() - interval '2 hours')
            """);
        await StartHostAsync(FastMaintenance);

        await WaitUntilAsync(async () => await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.servers WHERE name = 'dead'") == 0,
            Timeout, "dead server row is removed");
    }
}

public sealed class MaintenanceTests_Direct(PostgresFixture pg) : MaintenanceTests(pg, ConnectionMode.Direct);
public sealed class MaintenanceTests_Bouncer(PostgresFixture pg) : MaintenanceTests(pg, ConnectionMode.Bouncer);
```

`UuidV7` — `internal` в `PgBolt.Postgres`, `PgBolt.Server.Tests` его видит (`InternalsVisibleTo`). В литерале `'{{}}'` двойные скобки дают `'{}'` в интерполированной строке.

- [x] **Step 3: Запустить**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~MaintenanceTests"`
Expected: PASS (Direct и Bouncer).

- [x] **Step 4: Прогон фазы**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add tests/PgBolt.Server.Tests/Integration
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(server): end-to-end maintenance scenarios"
```

---

## Фаза 3. Документация

### Task 12: Приведение спек core/worker, карта проекта, changelog

Спека: §9 (изменения core и worker), §10 (обновление, breaking changes, риски), §11.

**Files:**
- Modify: `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md`, `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md`, `docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md` (отклонения плана — Step 4, статус — Step 5), `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md`, `CHANGELOG.md`

- [x] **Step 1: Core §4.4**

В `2026-09-26-pgbolt-core-design.md`, раздел «4.4 Партиции и retention»:
- в пункте про `EnsurePartitionsAsync` заменить «`EnsurePartitionsAsync(int daysAhead)` (публичный API `IPgBoltMaintenance`, по умолчанию `daysAhead = 7`, …)» на «`EnsurePartitionsAsync(int? daysAhead = null)` (публичный API `IPgBoltMaintenance`; `null` — `PgBoltOptions.PartitionDaysAhead`, …)»;
- фразу «Периодический вызов и retention — часть 2.» заменить на «Периодический вызов и retention — часть 2 (`2026-09-29-pgbolt-storage-maintenance-design.md`).»;
- абзац, начинающийся «Строку `jobs` нельзя перенести в другую партицию без смены id.», вместе с SQL-блоком протокола и абзацем «Блокировка держится миллисекунды; …» заменить на:

```markdown
- Строку `jobs` нельзя перенести в другую партицию без смены id. Поэтому партиция `jobs` удаляется целиком, только если в ней нет задач в состояниях Pending/Awaiting; иначе она **компактируется**: под блокировкой родителя живые строки переносятся в новую партицию тех же суток, старая таблица с терминальными строками удаляется (часть 2 §5.3; при числе живых больше `RetentionCompactionMaxLiveJobs` или публикации `jobs` в логической репликации партиция ждёт ухода живых задач). Проверка «нет Pending/Awaiting» выполняется по индексам (Pending ⇔ строка в `queue`: range scan по PK `queue` в диапазоне id партиции; Awaiting — индекс части 5) до блокировки и повторно под ней. Протокол (часть 2 §5.3–§5.4): `LOCK TABLE ONLY {schema}.jobs IN ACCESS EXCLUSIVE MODE` после `SET LOCAL lock_timeout` и сразу за ним `SET LOCAL statement_timeout`/`idle_in_transaction_session_timeout` (потолок удержания); группа до 10 партиций под одной блокировкой, `DETACH PARTITION` одним пакетом; `DROP TABLE` — отдельной транзакцией на таблицу и только если таблица с тем же `oid` не присоединена (`42P01` — таблицу уже удалил другой под, пропуск); прерванная обработка оставляет остатки, которые удаляет следующий запуск. Без `ONLY` блокировка рекурсировала бы на все партиции и ждала autovacuum сегодняшних; `DROP` в той же транзакции держал бы блокировку родителя на время удаления файлов. Партиции `job_events` удаляются так же, без проверки задач. Повторы ограничены: не больше одной неудачной попытки на таблицу за запуск, запуски — не чаще `RetentionInterval`; алерт — Warning (часть 2 §8), метрика — часть 7.
- Внешние ключи, ссылающиеся на `jobs` и `job_events`, запрещены (обязательно для частей 5–8): `DETACH` проверял бы ссылки сканированием под блокировкой родителя и падал бы с `23503`. Партиции создаёт только PgBolt (`LIKE` родителя), колонки меняются только `ALTER TABLE` родителя, генерируемых колонок в `jobs` нет (компактирование копирует строки `SELECT *`).
```

- последний пункт «Горизонт партиций: … Проверка горизонта при старте сервера — часть 2.» — заменить последнюю фразу на «Проверка горизонта при старте — `PgBoltInitializer`; периодическое создание — часть 2 (цикл `housekeeping`).».

- [x] **Step 2: Core §4.7 и §5.5**

§4.7 — в конец раздела добавить пункт:

```markdown
- Версия схемы новее кода (схему поднял более новый релиз при rolling deploy; миграции аддитивны в пределах мажорной версии) — не ошибка, а Warning при старте. Ошибка — только версия меньше `LatestVersion` при `AutoMigrate = false`. `EnsurePartitionsAsync` мигрирует только при `AutoMigrate` и отсутствующей или более старой схеме (часть 2 §4).
```

§5.5 — SQL-блок заменить SQL из спеки части 2 §9.2 (heartbeat с CTE `s … RETURNING 1`, `u`, итоговый `SELECT EXISTS (SELECT 1 FROM s) AS server_present, …`) и после пункта «Пустой список: …» добавить:

```markdown
- Результат — `HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases)`: всегда хотя бы одна строка (при пустом `u` — с `job_id IS NULL`). `ServerPresent = false` — строки `servers` нет (удалена уборкой части 2 после паузы дольше `DeadServerTimeout`), сервер регистрируется заново upsert'ом (часть 2 §9.2).
```

- [x] **Step 3: Worker §6.2, §7.2, §8.1**

В `2026-09-28-pgbolt-worker-design.md`:
- §7.2: SQL регистрации заменить upsert'ом из спеки части 2 §9.2; фразу «Уборка строк упавших серверов — часть 2; она обязана удалять только строки с устаревшим `heartbeat_at`.» заменить на «Уборка строк упавших серверов — часть 2 (§5.6): удаляются только строки с устаревшим `heartbeat_at`; строку живого сервера восстанавливает его heartbeat (§6.2).»;
- §8.1: SQL заменить новым SQL heartbeat (часть 2 §9.2), сигнатуру — на `Task<HeartbeatResult> HeartbeatAsync(...)`;
- §6.2 — в конец раздела добавить абзац:

```markdown
**Строка `servers` (часть 2 §9.2).** Ответ heartbeat сообщает `ServerPresent`. При `false` `LeaseKeeper` внутри того же вызова (после обработки lease) выполняет upsert регистрации с таймаутом `HeartbeatInterval / 2` и токеном цикла; ошибка — Warning, повтор при следующем heartbeat. Граница «следующий успешный ответ не позже `S + 1.6 HI`» сохраняется; `StopAsync` дожидается идущей регистрации до удаления строк серверов, поэтому строка после остановки не воскресает.
```

- [x] **Step 4: Спека части 2 — отклонения плана**

В `2026-09-29-pgbolt-storage-maintenance-design.md`:
- §5.3 п. 6, SQL-блок: строку `DROP TABLE IF EXISTS {schema}.<table>;` заменить на `DROP TABLE {schema}.<table>;`; в абзаце после блока фразу «`IF EXISTS` покрывает удаление другим подом во время ожидания блокировки (`NOTICE` вместо `42P01`).» заменить на «`42P01` после ожидания блокировки (таблицу удалил другой под) — пропуск, в результат не попадает. Признак — SQLSTATE ошибки, он не зависит от `NOTICE`, `lc_messages`, `client_min_messages` и event trigger'ов.»;
- §5.3 «Обработка ошибок», п. «Транзакция 1»: фразу «Признак разрыва по idle-timeout: `PostgresException` с `SqlState = 25P03` **или** `NpgsqlException`, не являющееся `PostgresException`, при `conn.FullState`, содержащем `Broken` или `Closed` (через PgBouncer — обрыв соединения без `25P03`);» заменить на «Признак разрыва по idle-timeout: `PostgresException` с `SqlState = 25P03` **или** любое `NpgsqlException` при `conn.FullState`, содержащем `Broken` или `Closed` (через PgBouncer вместо `25P03` приходит обрыв без SQLSTATE или ошибка самого PgBouncer, например `08P01`);»;
- §5.3 «Обработка ошибок», п. «Транзакция 2»: после «`droppable` не `true` → пропуск, в результат не попадает.» добавить «`42P01` на `DROP` (таблицу удалил другой под, пока `DROP` ждал блокировку) → откат, пропуск, в результат не попадает.»;
- §5.7, SQL-блок: комментарий `-- TryClaimMaintenanceAsync(name, interval) → DateTimeOffset? (claimed_at; null — не захвачено)` заменить на `-- TryClaimMaintenanceAsync(name, interval) → DateTime? (UTC; claimed_at; null — не захвачено)`;
- §6.2 п. «Перекрытие»: «`DROP IF EXISTS` и `42P01` в предпроверках обрабатывают уже удалённое» заменить на «`42P01` в предпроверках и в транзакции 2 обрабатывает уже удалённое»;
- §9.1: «`DROP TABLE IF EXISTS` — отдельной транзакцией на таблицу» заменить на «`DROP TABLE` — отдельной транзакцией на таблицу».

- [x] **Step 5: Статусы и changelog**

- `2026-09-29-pgbolt-storage-maintenance-design.md`, строка статуса: `Статус: одобрено ревью (5 итераций), реализовано (ред. 6; план docs/superpowers/plans/2026-09-29-pgbolt-storage-maintenance.md) · Дата: 2026-09-29 · …` (остальная часть строки без изменений).
- `2026-09-26-pgbolt-roadmap.md`, раздел «### 2. Storage maintenance»: вторую строку заменить на «Зависит от 1, 3 (расписание — в `PgBolt.Server`). Спека: `2026-09-29-pgbolt-storage-maintenance-design.md`, план: `docs/superpowers/plans/2026-09-29-pgbolt-storage-maintenance.md`. **Статус: реализовано.**».
- `CHANGELOG.md`, раздел `## Unreleased`: в `### Breaking changes` добавить пункты, в `### Added` и `### Changed` — пункты ниже (разделы уже есть):

```markdown
### Breaking changes

- `PgBolt.Postgres.Hosting.IPgBoltMaintenance`: `EnsurePartitionsAsync(int daysAhead = 7, …)` → `EnsurePartitionsAsync(int? daysAhead = null, …)`
  (`null` — `PgBoltOptions.PartitionDaysAhead`); новые члены `ApplyRetentionAsync` и `RemoveDeadServersAsync`. Пользовательские
  реализации и моки интерфейса должны реализовать их; вызовы с явным `int` совместимы по исходному коду, не бинарно.
- `PgBoltStore.HeartbeatAsync` возвращает `HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases)` вместо
  `IReadOnlyList<HeartbeatLease>`.
- **Retention включён по умолчанию** (`JobsRetention = EventsRetention = 7 суток`): первый запуск обслуживания после обновления
  удаляет все партиции `jobs`/`job_events` старше срока и компактирует закреплённые. Чтобы сохранить историю, задайте сроки
  (или `null`, только кодом) до обновления.

### Added

- Часть 2 (Storage maintenance): миграция V002 (`maintenance_tasks`); цикл обслуживания в `PgBolt.Server` — периодический
  `EnsurePartitionsAsync`, retention с компактированием партиций, закреплённых живыми задачами, уборка `servers`; кластерная
  координация задач через `maintenance_tasks`.
- `PgBoltOptions`: `JobsRetention`, `EventsRetention`, `RetentionLockTimeout`, `RetentionCompactionMaxLiveJobs`.
- `PgBoltProcessingOptions`: `MaintenancePollInterval`, `PartitionsInterval`, `ServersCleanupInterval`, `RetentionInterval`,
  `DeadServerTimeout`.
- `RetentionResult`, `PinnedPartition`, `DeferredPartition`, `DeferReason`; `PgBoltStore.TryClaimMaintenanceAsync`,
  `CompleteMaintenanceAsync`, `RemoveDeadServersAsync`.

### Changed

- Регистрация сервера — upsert; строку `servers`, удалённую уборкой, восстанавливает следующий heartbeat.
- Версия схемы новее кода — Warning вместо ошибки старта; `EnsurePartitionsAsync` не мигрирует более новую схему.
- Известные ограничения: компактирование не MVCC-безопасно для снимков REPEATABLE READ/SERIALIZABLE, взятых до него
  (выход — `RetentionCompactionMaxLiveJobs = 0`); при публикации `jobs` в логической репликации компактирование пропускается;
  закреплённые сутки остаются присоединёнными до ухода живых задач.
```

- [x] **Step 6: Проверить правки**

Run:

```bash
grep -n "LOCK TABLE ONLY" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "внешние ключи, ссылающиеся на \`jobs\`\|Внешние ключи, ссылающиеся на \`jobs\`" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "server_present" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md
grep -n "ON CONFLICT (id) DO UPDATE" docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md
grep -n "удаляются терминальные строки старше retention" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "Статус: реализовано" docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md
grep -n "^Статус" docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md
grep -n "ApplyRetentionAsync\|Retention включён по умолчанию" CHANGELOG.md
grep -n "IF EXISTS\|NOTICE. вместо" docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md
grep -n "DROP TABLE IF EXISTS" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "DROP TABLE {schema}.<table>;\|42P01. на .DROP" docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md
grep -n "DateTimeOffset?\|→ DateTime? (UTC" docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md
grep -n "не являющееся .PostgresException.\|08P01" docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md
git status --short
```

Expected:
- `LOCK TABLE ONLY`, запрет FK, `server_present` (core §5.5 и worker §8.1), upsert в worker §7.2 — есть;
- `удаляются терминальные строки старше retention` — нет;
- в карте две строки «Статус: реализовано» (части 2 и 3); статус спеки части 2 — «… реализовано (ред. 6; план …)»;
- в `CHANGELOG.md` — оба пункта;
- спека части 2: `IF EXISTS` и «`NOTICE` вместо» — нет; `DROP TABLE {schema}.<table>;` (§5.3 п. 6) и «`42P01` на `DROP`» (§5.3 «Транзакция 2») — есть; `DateTimeOffset?` — нет, `→ DateTime? (UTC` (§5.7) — есть; «не являющееся `PostgresException`» — нет, `08P01` (§5.3 «Транзакция 1») — есть;
- в core `DROP TABLE IF EXISTS` — нет;
- `git status --short` — только 5 файлов документации.

- [x] **Step 7: Итоговый прогон**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: PASS.

- [x] **Step 8: Commit**

```bash
git add docs/superpowers/specs CHANGELOG.md
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "docs: align core and worker specs with storage maintenance, mark part 2 implemented"
```

---

## Покрытие спеки

| Раздел спеки | Задачи |
|---|---|
| §1 критерий: партиции на сегодня всегда есть, одиночная неудача не ломает горизонт | 10 (ускоренный повтор `partitions`, Error без сегодняшней), 10 (предупреждение §6.4), 11 |
| §1 критерий: незавершённые задачи не удаляются никогда | 7 (предпроверка, повтор под блокировкой, requeue до `LOCK`, компактирование переносит живые), 8 (перекрытие, нагрузка) |
| §1 критерий: терминальные — не раньше `JobsRetention` от суток создания | 5 (горизонт, округление), 6–7 (граница кандидата) |
| §1 критерий: диск — сжатие закреплённых, пороги, Warning | 7 (пороги живых и терминальных), 9 (Warning), 8 (граница 10 000) |
| §1 критерий: пауза горячего пути ограничена | 6 (`ONLY`, таймауты удержания, `pg_locks`), 7 (группы, компактирование по одной), 8 (нагрузка, 10 000 строк) |
| §1 критерий: повтор не раньше `RetentionInterval`, без параллельных запусков | 3 (захват), 10 (retention без ускоренного повтора), 11 (ровно один под) |
| §1 критерий: восстановление строки `servers` | 4, 11 |
| §1 критерий: не зависит от часового пояса сессии | 5 (`today` из БД), 8 (Kiritimati, GMT+12) |
| §2 решения (расписание, координация, отсчёт, компактирование, блокировки, раздельные сроки, порог `servers`) | 3, 5–7, 9–10 |
| §3 время и сутки | 5, 8 |
| §4 V002 и проверка версии | 1 (в т. ч. `AutoMigrate = false` на схеме V001), 12 |
| §5.1 API, настройки, валидация, XML-doc | 2 (в т. ч. архивирование партиций §5.2), 3, 5, 6, 7 (`LiveJobs` сжатой — по предпроверке) |
| §5.2 кандидаты, список, имена, остатки, `oid` как `bigint` | 5, 6, 7 (чужие имена, `DROP` по `oid`) |
| §5.3 п. 1 предпроверка, порог терминальных | 7 |
| §5.3 «Публикации» | 7 (непараллельная коллекция `Serial`) |
| §5.3 п. 2 порядок и остановка | 6 (`job_events`), 7 (`jobs`; `HoldTimeout` компактирования после пакета А и Б — следующая компактируемая не тронута, `job_events` удалены) |
| §5.3 п. 3 предпроверка `pg_locks` | 6, 7 |
| §5.3 п. 4 прогрев | 7 (порядок шагов, план без Index Only Scan) |
| §5.3 п. 5 транзакция 1: пакеты, таймауты, откат, multiplexing | 6, 7, 8 (multiplexing) |
| §5.3 п. 6 транзакция 2 | 6 (в т. ч. `42P01` после ожидания блокировки при перекрытии → пропуск, не двойной учёт), 7 |
| §5.3 корректность компактирования, `SELECT *`, HOT | 7 (операции взятых задач, колонки, fillfactor), 8 (нагрузка) |
| §5.3 MVCC | 8 (документирующий тест), 2 (XML-doc), 12 (changelog) |
| §5.3 обработка ошибок | 6 (`55P03`, `57014`, `25P03`/разрыв по состоянию соединения, отмена между пакетами и в транзакции 2, `DropFailed` — непараллельная коллекция `Serial`, `42P01` на `DROP` → пропуск), 12 (спека: `DROP TABLE`, признак разрыва), 7 (`42P01`, `42P07`, отмена в предпроверке и прогреве) |
| §5.3 число партиций | 6 (`AttachedJobsPartitions`), 9 (Warning) |
| §5.4 retention `job_events` | 6 |
| §5.5 порядок, оба `null`, перекрытие запусков | 6 (параллельный `DROP` одной таблицы), 8 |
| §5.6 уборка `servers` | 3, 10, 11 |
| §5.7 захват и завершение | 3 (`DateTime?` UTC вместо `DateTimeOffset?`), 10, 12 (спека §5.7) |
| §6.1 настройки | 9 |
| §6.2 цикл `housekeeping`, ускоренный повтор, остановка, перекрытие | 10 (в т. ч. гонка `LeaseFailureTests` с первой итерацией), 8 (перекрытие) |
| §6.3 `IServerStore` | 4, 10 |
| §6.4 предупреждения при старте | 10 |
| §7 ручной режим | 2, 6 (XML-doc `IPgBoltMaintenance`) |
| §8 логи | 9, 10, 4 (`LeaseKeeper`), 1 (версия схемы) |
| §9.1 протокол удаления (core §4.4) | 6, 7, 12 |
| §9.2 самовосстановление `servers` | 4 (`ServerPresent` с пустым и непустым списком lease), 11, 12 |
| §9.3 прочее (версия схемы, `int?`) | 1, 2 |
| §10 обновление, breaking changes, риски | 1 (V001 → V002), 12 |
| §11 контракты для частей 5–7 | 7 (`state IN` не нужен — Awaiting не трогается; `SELECT *`), 12 (запрет FK в core) |
| §12 тестирование | 1–11 |
