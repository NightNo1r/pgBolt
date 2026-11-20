# PgBolt Scheduler (часть 4) — план реализации

> **Для исполнителя-агента.** План самодостаточен и не требует скиллов: протокол исполнения через субагентов, шаблоны их заданий, выбор моделей и требования к окружению описаны ниже («Протокол исполнения»). Шаги задач размечены чекбоксами (`- [ ]`).

**Goal:** Recurring-задачи PgBolt: таблица `recurring_jobs`, runtime API `IRecurringJobManager`, code-first объявления с синхронизацией набора владельца, тикер без лидера в `PgBolt.Server` с misfire-политиками `FireOnce`/`Skip`/`CatchUp` и политиками наложения `Skip`/`Allow`, время срабатывания в `JobContext`.

**Architecture:** Схема и SQL — в core (`PgBolt.Postgres`): объединённая миграция V001, `PgBoltStore.Recurring*.cs` (CRUD, тик, синхронизация), чистые `CronSchedule` / `RecurringDefinition` / `RecurringPlanner` (Cronos) в `PgBolt.Postgres/Recurring`. Тик — одна транзакция: захват строк `FOR UPDATE SKIP LOCKED` с умолчаниями из `job_types`, планирование в .NET, enqueue групп одним `NpgsqlBatch`, обновление курсоров. Расписание — циклы `scheduler` и `recurring-sync` в `ProcessingMaintenance` (`PgBolt.Server`); синхронизация на старте — в `PgBoltInitializer`.

**Tech Stack:** .NET 10 (C# 14), Npgsql 10 (`NpgsqlBatch`, slim data source), Cronos 0.13.0, Microsoft.Extensions.Hosting 10, xUnit v3, Testcontainers (PostgreSQL 17 + PgBouncer), Microsoft.Extensions.TimeProvider.Testing.

**Spec:** `docs/superpowers/specs/2026-09-30-pgbolt-scheduler-design.md` (далее «спека», ссылки вида «§5.3»). Основа: `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` («core §5.1»), `2026-09-28-pgbolt-worker-design.md` («worker §6.3»), `2026-09-29-pgbolt-storage-maintenance-design.md` («часть 2 §9.3»). Образец стиля кода и тестов — существующие `src/PgBolt.Postgres/Storage/*.cs`, `tests/PgBolt.Postgres.Tests`, `tests/PgBolt.Server.Tests`.

## Global Constraints

- Только .NET 10 (`net10.0`); `PgBolt.Postgres`, `PgBolt.Server` — `IsAotCompatible=true`, `TreatWarningsAsErrors=true` (`Directory.Build.props`). Без reflection. SQL — строковые константы с `{schema}`, подставляемым `Render`.
- Подключение напрямую или через PgBouncer в transaction mode: без `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` (кроме `SET LOCAL`), temp tables. Транзакции — только `BeginTransactionAsync` / `CommitAsync` / `RollbackAsync`; `BEGIN`/`COMMIT` в тексте команд запрещены. Advisory lock — только `pg_advisory_xact_lock`.
- Уровень изоляции — READ COMMITTED (по умолчанию). Все моменты — `timestamptz`; «сейчас» — `now()` БД, часы пода в расчётах курсора не участвуют (§5.4).
- Контракт HOT (core §4.2): индексов `jobs` по изменяемым колонкам нет; `jobs.recurring_id`, `jobs.scheduled_at` пишутся один раз при создании.
- Миграции объединены в одну `V001__initial.sql`; `V002__maintenance_tasks.sql` удаляется; `PgBoltMigrations.LatestVersion = 1`. Совместимость со схемами предыдущих сборок не поддерживается: нет таблицы `recurring_jobs` — старт падает (§3).
- Константы (§5): пакет тика `@n = 100` строк; лимит срабатываний пакета — 1000; `CatchUp + Allow` — до 100 срабатываний строки за тик; `OverlapRecheckDelay = 5 с`; ошибка строки — `wake_at = db_now + 1 мин`; ключ наложения `pgbolt:recurring:{id}`; префикс `pgbolt:` зарезервирован.
- Умолчания опций (§4.1, §8): `RecurringJobOptions`: `TimeZone = null` (UTC), `Misfire = FireOnce`, `CatchUpWindow = 1 сут` (`(0, 366 сут]`), `Overlap = Skip`. `PgBoltProcessingOptions`: `RunScheduler = true`, `SchedulerInterval = 1 с`, `MisfireThreshold = null` → `max(1 мин, 5 × SchedulerInterval)`, явный — `≥ 5 × SchedulerInterval`; `RecurringSyncInterval = 1 мин`. Интервалы — `(0, PgBoltServerOptions.MaxInterval]`.
- Хранимые коды: `misfire` — 0 FireOnce, 1 Skip, 2 CatchUp; `overlap` — 0 Skip, 1 Allow (значения enum'ов).
- Коммиты: Conventional Commits; автор и коммиттер — `NightNo1r <57496218+NightNo1r@users.noreply.github.com>` (`git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit ...`); строк `Co-Authored-By` **нет**.

## Требования к окружению

Проверить до Task 1; при невыполнении — остановиться и доложить.

| Требование | Проверка | Ожидаемо |
|---|---|---|
| .NET SDK 10 | `dotnet --version` | `10.0.*` |
| Docker | `docker info` | без ошибок |
| Сборка зелёная | `dotnet test PgBolt.slnx --filter "Category!=Load"` | все тесты проходят |
| Ветка | `git branch --show-current` | `feat/scheduler` (`git switch feat/scheduler && git pull --ff-only`) |

Docker недоступен — не заменять базу моками и не пропускать интеграционные тесты; остановиться и доложить. Тесты — VSTest: `dotnet test tests/<Проект> --filter "FullyQualifiedName~<Класс>"`. Интеграционные тестовые классы — абстрактные, с наследниками `_Direct` и `_Bouncer` (как `MaintenanceTasksTests`).

## Протокол исполнения

Исполняется основной сессией (**координатор**) через субагентов — инструментом запуска агента (в Claude Code — `Agent`, тип `general-purpose`, запуск в foreground: следующий шаг зависит от результата). Скиллы не нужны. Состояние хранится в самом плане (чекбоксы) и в git, поэтому прерванное исполнение продолжается с первой неотмеченной задачи.

### Роли
- **Координатор** — основная сессия. Ведёт задачи строго по порядку (Task 1 → Task 12), запускает исполнителя и ревьюера по шаблонам ниже, проверяет их отчёты, отмечает чекбоксы. Код задач сам не пишет.
- **Исполнитель** — свежий субагент на задачу (и на каждый раунд исправлений), модель из таблицы.
- **Ревьюер** — свежий субагент на задачу, всегда Opus 5.5.

Если среда не поддерживает субагентов, координатор выполняет обе роли сам, последовательно, с тем же чек-листом.

### Модели

| Задачи | Исполнитель | Почему |
|---|---|---|
| 1, 2, 3, 4, 12 | Sonnet 5.5 (`claude-sonnet-5-5`, параметр `model: "sonnet"`) | Код дан целиком, логика локальная |
| 5, 6, 7, 8, 9, 10, 11 | Opus 5.5 (`claude-opus-5-5`, `model: "opus"`) | Бинарный поиск по cron, блокировки и транзакции PostgreSQL, PgBouncer, конкурентные тесты, хостинг |
| Ревью каждой задачи и итоговое ревью ветки | Opus 5.5 (`model: "opus"`) | Сверка со спекой, поиск гонок и дублей срабатываний |

### Подготовка (один раз)
1. Проверить «Требования к окружению»; при невыполнении — остановиться и доложить.
2. Ветка исполнения — `feat/scheduler` (в ней же спека и этот план): `git switch feat/scheduler && git pull --ff-only`. Рабочее дерево чистое (`git status --short` пуст).
3. Найти первую задачу с неотмеченными чекбоксами — с неё начинать.

### Цикл одной задачи
1. Запомнить `BASE=$(git rev-parse HEAD)`.
2. **Исполнитель** — задание по шаблону «Исполнитель» (ниже), модель из таблицы.
3. Отчёт `STATUS: BLOCKED` → остановиться и доложить пользователю (текст отчёта целиком). `DONE` → проверить сам:
   - `git log --format='%an <%ae>%n%B' $BASE..HEAD` — автор NightNo1r, строк `Co-Authored-By` нет;
   - `git status --short` пуст;
   - повторить прогон из последнего шага «Запустить — проходят» задачи.
   Расхождение с отчётом → считать результат `CHANGES` с этим фактом.
4. **Ревьюер** — задание по шаблону «Ревьюер» на `git diff $BASE..HEAD`.
5. `CHANGES` → новый исполнитель по шаблону «Исправление» (замечания ревьюера целиком), затем снова ревьюер на `git diff $BASE..HEAD`. Не более двух раундов исправлений; третий `CHANGES` — остановиться и доложить.
6. `APPROVE` → координатор отмечает все чекбоксы задачи (`- [ ]` → `- [x]`) и коммитит план: `chore(plan): mark task N done` (автор NightNo1r, без `Co-Authored-By`).
7. Конец фазы (после Task 3, 7, 10, 12) — `dotnet test PgBolt.slnx --filter "Category!=Load"`; падение — остановиться и доложить.

### Завершение
1. **Итоговое ревью ветки** — ревьюер (Opus) по шаблону «Ревьюер», но предмет — `git diff main..HEAD` (код; изменения `docs/` — только на согласованность), текст задачи заменяется ссылкой на весь план и спеку; фокус — сквозные проблемы: согласованность задач, гонки тика / API / синхронизации, соответствие спеке целиком. `CHANGES` → исправление и повторное ревью по тем же правилам (не более двух раундов).
2. `dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный.
3. `git push`; PR не создавать без запроса пользователя. Доложить: SHA, итог тестов, отклонения от плана из отчётов исполнителей.

### Шаблон «Исполнитель»

```text
Ты реализуешь одну задачу плана в репозитории <абсолютный путь>, ветка feat/scheduler (текущий HEAD: <SHA>).
План: docs/superpowers/plans/2026-09-30-pgbolt-scheduler.md. Спека: docs/superpowers/specs/2026-09-30-pgbolt-scheduler-design.md.
Прочитай в плане разделы «Global Constraints», «Контракт внутренних типов», «Правила для исполнителя» и задачу «Task N» целиком;
разделы спеки, на которые ссылается задача. Выполни шаги задачи строго по порядку: падающий тест → убедиться, что падает →
реализация → тесты проходят → коммит командой из плана (автор NightNo1r, без Co-Authored-By). Чекбоксы плана не отмечай.
Не делай работу других задач; код плана — эталон, отклонения только ради компиляции, несовместимости API Npgsql/Cronos/
PostgreSQL или явной ошибки плана. Две безуспешные попытки исправить один падающий тест → остановись.
Ответ строго в формате:
STATUS: DONE | BLOCKED
SHA: <HEAD после коммита>
TESTS: <команда> → passed N / failed M
DEVIATIONS: <список отклонений от кода плана с причинами или «нет»>
NOTES: <для BLOCKED — вывод ошибки и гипотезы>
```

### Шаблон «Ревьюер»

```text
Ты ревьюер задачи Task N плана docs/superpowers/plans/2026-09-30-pgbolt-scheduler.md в репозитории <абсолютный путь>.
Прочитай задачу, «Global Constraints», «Контракт внутренних типов», «Чек-лист ревьюера» и разделы спеки
docs/superpowers/specs/2026-09-30-pgbolt-scheduler-design.md, на которые ссылается задача.
Изменения: `git diff <BASE>..HEAD`. Отчёт исполнителя: <отчёт целиком>.
Проверь по чек-листу ревьюера; сам запусти тесты задачи (команды из шагов «Запустить»). Ничего не редактируй.
Ответ: APPROVE или CHANGES; для CHANGES — нумерованный список «файл:строка — что не так — чем подтверждается — как исправить».
Стиль не оценивай, если он не нарушает Global Constraints.
```

### Шаблон «Исправление»

```text
Ты исправляешь замечания ревью по задаче Task N плана docs/superpowers/plans/2026-09-30-pgbolt-scheduler.md
в репозитории <абсолютный путь>, ветка feat/scheduler (HEAD: <SHA>). Прочитай задачу, «Global Constraints», «Контракт
внутренних типов», «Правила для исполнителя». Замечания: <список ревьюера целиком>.
Исправь каждое новым коммитом (Conventional Commits, автор через git -c user.name=NightNo1r
-c user.email=57496218+NightNo1r@users.noreply.github.com, без Co-Authored-By); не ослабляй тесты. С замечанием не согласен —
не исправляй, а объясни в отчёте с доказательством. Ответ — в формате шаблона «Исполнитель» плюс FIXED/REJECTED по номерам замечаний.
```

### Чек-лист ревьюера
- Реализовано всё из шагов; тесты задачи есть и проходят (интеграционные — в `_Direct` и `_Bouncer`).
- Имена и сигнатуры совпадают с «Контрактом внутренних типов».
- Тик: захват, enqueue и сдвиг курсора — в **одной** транзакции; `FOR UPDATE OF r SKIP LOCKED`; ошибка строки не откатывает пакет; строки сверх лимита срабатываний не обновляются; курсор при `CatchUp + Skip` и конфликте не двигается.
- Время курсора — только `now()` БД (`db_now`); нет `DateTime.UtcNow` в расчёте курсора, `next_fire_at`, `wake_at` (допустимо только в `RecurringDefinition` для проверки «есть будущие срабатывания»).
- `wake_at` и `next_fire_at` одновременно `NULL` или оба не `NULL`; `ResumeAsync` не оживляет `wake_at IS NULL`.
- Trigger не блокирует и не обновляет строку `recurring_jobs`; синхронизация вставляет в порядке id и не трогает чужие строки; `AddOrUpdate`/`Remove` отклоняют code-first строки.
- Клиентский enqueue (`RecurringIds = null`) идёт прежним `EnqueueTemplate`; колонки расписания — только во внутреннем `RecurringEnqueueTemplate` (тик, Trigger; §8). `40P01` синхронизации повторяется явно с Warning (§7), не через `RunAsync`/`DbRetry`. `TickBeforeCommit` в рабочем коде не задаётся.
- Нет `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` вне `SET LOCAL`, `BEGIN`/`COMMIT` в тексте SQL.
- Контракт HOT: новых индексов `jobs` нет; `recurring_id`/`scheduled_at` пишутся только при вставке.
- В runtime `PgBolt.Server` нет `Task.Delay`/таймеров мимо `TimeProvider`; unit-тесты runtime не зависят от реального времени (кроме `Eventually`).
- Тесты не ослаблены: нет `Skip`, удалённых assert'ов, подгонки ожидаемых значений (исключение — Task 4 Step 6, DST по документации Cronos, с объяснением в отчёте).
- Сборка без предупреждений (`TreatWarningsAsErrors`, AOT/trim-анализ).
- Коммиты — Conventional Commits, автор NightNo1r, без `Co-Authored-By`.

### Правила для исполнителя
- Не делать работу будущих задач. Не менять тесты ради прохождения, если задача этого не предписывает.
- Код плана — эталон; отклонения — только для устранения ошибки компиляции, несовместимости API Npgsql / Cronos / PostgreSQL или явной ошибки плана; каждое — в отчёт `DEVIATIONS`.
- Если имя или сигнатура из «Контракта внутренних типов» не компилируется — не переименовывать, а остановиться (`BLOCKED`): на них опираются соседние задачи.
- Две безуспешные попытки исправить один падающий тест → `STATUS: BLOCKED` с выводом и гипотезами.

## Контракт внутренних типов

Соседние задачи опираются на эти имена и сигнатуры; менять их нельзя. В скобках — задача, которая вводит член.

```csharp
// PgBolt.Abstractions — публичные (Task 2)
public enum MisfirePolicy { FireOnce = 0, Skip = 1, CatchUp = 2 }
public enum OverlapPolicy { Skip = 0, Allow = 1 }
public sealed record RecurringJobOptions { TimeZoneInfo? TimeZone; MisfirePolicy Misfire; TimeSpan CatchUpWindow; OverlapPolicy Overlap;
    string? Queue; short? Priority; int? MaxAttempts; IReadOnlyList<TimeSpan>? RetryDelays; }   // init-свойства
public sealed record RecurringJobInfo(string Id, string JobName, JsonElement Args, string Cron, string TimeZone, MisfirePolicy Misfire,
    TimeSpan CatchUpWindow, OverlapPolicy Overlap, string? Queue, short? Priority, int? MaxAttempts, IReadOnlyList<TimeSpan>? RetryDelays,
    string? Owner, bool Paused, DateTimeOffset? NextFireAt, DateTimeOffset? LastFireAt, JobId? LastJobId, string? LastError,
    DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt);
public interface IRecurringJobManager { AddOrUpdateAsync<TArgs>; AddOrUpdateAsync(id, jobName, cron, JsonElement, …); RemoveAsync; PauseAsync;
    ResumeAsync; TriggerAsync → ValueTask<EnqueueResult?>; GetAsync → ValueTask<RecurringJobInfo?> }
// JobContext(…, ILogger logger, DateTimeOffset? scheduledAt = null, string? recurringJobId = null): ScheduledAt, RecurringJobId
// JobInfo(…, IReadOnlyList<JobEventInfo> Events, string? RecurringJobId = null, DateTimeOffset? ScheduledAt = null)

// PgBolt.Postgres.Storage
public sealed record EnqueueBatch(…) { DateTime?[]? ScheduledAtUtc { get; init; } string[]? RecurringIds { get; init; } }      // Task 3
public sealed record LeasedJob(…, string? TraceParent, string? RecurringId = null, DateTime? ScheduledAtUtc = null);           // Task 3
internal static class P { static void AddValue<T>(NpgsqlParameterCollection ps, string name, T? value, NpgsqlDbType type) where T : struct; } // Task 6
internal enum RecurringUpsertResult { Inserted, Updated, OwnedByCodeFirst }                                                   // Task 6
internal enum RecurringRemoveResult { NotFound, Removed, OwnedByCodeFirst }                                                   // Task 6
internal sealed record RecurringTriggerRow(string Type, string ArgsJson, string? Queue, short? Priority, int? MaxAttempts,
    int[]? RetryDelaysSeconds, OverlapPolicy Overlap, bool UnknownType);                                                       // Task 6
internal sealed record RecurringRowError(string Id, string Error);                                                            // Task 7
internal sealed record RecurringMissed(string Id, DateTime FromUtc, DateTime ToUtc);                                          // Task 7
internal sealed record RecurringTickResult(int Claimed, int Fired, bool Full, IReadOnlyCollection<string> Queues,
    IReadOnlyList<RecurringRowError> Errors, IReadOnlyList<RecurringMissed> Missed, IReadOnlyList<string> OverlapSkipped,
    IReadOnlyList<string> Waiting, IReadOnlyList<string> Exhausted) { static RecurringTickResult Empty; }                      // Task 7
internal sealed record RecurringFieldChange(string Id, IReadOnlyList<string> Fields);                                         // Task 8
internal sealed record RecurringSyncResult(IReadOnlyList<string> Inserted, IReadOnlyList<RecurringFieldChange> Updated,
    IReadOnlyList<string> Deleted, IReadOnlyList<string> Collisions) { int Writes; }                                           // Task 8
public sealed partial class PgBoltStore
{
    internal string EnqueueSqlFor(EnqueueBatch batch);   // RecurringIds null — прежний SQL клиента, иначе вариант с колонками  // Task 3
    internal NpgsqlBatchCommand CreateEnqueueCommand(EnqueueBatch batch);                                                      // Task 3
    internal Task<RecurringUpsertResult> UpsertRecurringAsync(RecurringDefinition d, CancellationToken ct);                    // Task 6
    internal Task<RecurringRemoveResult> RemoveRecurringAsync(string id, CancellationToken ct);
    internal Task<bool> PauseRecurringAsync(string id, CancellationToken ct);
    internal Task<bool> ResumeRecurringAsync(string id, CancellationToken ct);
    internal Task<RecurringJobInfo?> GetRecurringAsync(string id, CancellationToken ct);
    internal Task<RecurringTriggerRow?> GetRecurringForTriggerAsync(string id, CancellationToken ct);
    internal NpgsqlBatchCommand InsertRecurringCommand(RecurringDefinition d, string? owner, DateTime? cursor);
    internal NpgsqlBatchCommand UpdateRecurringCommand(RecurringDefinition d, string? owner, bool recalc, DateTime? cursor);
    internal static DateTime Later(DateTime a, DateTime? b);
    internal Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct); // Task 7
    internal Func<CancellationToken, Task>? TickBeforeCommit { get; set; }   // только тесты: перед COMMIT тика               // Task 7
    internal Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger,
        CancellationToken ct);                                                                                                 // Task 8
    internal static Task<T> RetrySyncDeadlockAsync<T>(Func<CancellationToken, Task<T>> op, int maxAttempts, ILogger logger, string owner,
        CancellationToken ct);                                                // 40P01 — повтор транзакции с Warning           // Task 8
}
// tests/PgBolt.Postgres.Tests: RecurringTestSupport.HoldTickAsync(PgBoltStore, CancellationToken) → HeldTick,
//     RecurringTestSupport.WaitForRowLockWaiterAsync(TestDb, CancellationToken)                                             // Task 7

// PgBolt.Postgres.Client
internal static class JobMetadataAccess { static (JobDescriptor Descriptor, JsonTypeInfo<TArgs> TypeInfo) Get<TArgs>(); }   // Task 3
internal sealed class RecurringJobManager(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup) : IRecurringJobManager; // Task 6

// PgBolt.Postgres.Recurring
internal static class ReservedUniqueKeys { const string Prefix = "pgbolt:"; static string Recurring(string id);
    static void EnsureUserKeys(string?[] keys); }                                                                             // Task 3
internal sealed class CronSchedule { static CronSchedule Parse(string cron, string timeZoneId); static CronFormat FormatOf(string cron);
    static string IanaId(TimeZoneInfo zone); TimeZoneInfo Zone; DateTime? After(DateTime utc);
    IEnumerable<DateTime> Between(DateTime fromUtc, DateTime toUtc); }                                                         // Task 4
internal sealed record RecurringDefinition(string Id, string Type, string ArgsJson, string Cron, string TimeZone, MisfirePolicy Misfire,
    TimeSpan CatchUpWindow, OverlapPolicy Overlap, string? Queue, short? Priority, int? MaxAttempts, int[]? RetryDelaysSeconds)
{ const int MaxIdLength = 200; static void Validate(string id, string cron, RecurringJobOptions? options);
  static RecurringDefinition Create(string id, string type, string argsJson, string cron, RecurringJobOptions? options);
  CronSchedule Schedule(); static bool SameDelays(int[]? a, int[]? b); }                                                       // Task 4
internal readonly record struct PlanInput(CronSchedule Schedule, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap,
    DateTime? NextFireAt);                                                                                                     // Task 5
internal readonly record struct MissedRange(DateTime FromUtc, DateTime ToUtc);                                                // Task 5
internal sealed record FirePlan(IReadOnlyList<DateTime> Fires, DateTime? NextFireAt, DateTime? WakeAt, MissedRange? Missed);  // Task 5
internal static class RecurringPlanner { const int MaxCatchUpFires = 100; static readonly TimeSpan OverlapRecheckDelay, RowErrorDelay;
    static FirePlan Plan(PlanInput row, DateTime dbNow, TimeSpan misfireThreshold); static FirePlan OnConflict(PlanInput row, DateTime dbNow);
    static DateTime LastDue(CronSchedule schedule, DateTime next, DateTime dbNow); }                                            // Task 5
internal sealed record RecurringDeclaration(string Id, string Cron, RecurringJobOptions? Options, Func<(string Type, string ArgsJson)> Resolve); // Task 9
internal sealed class RecurringDeclarations(PgBoltOptions options, IHostEnvironment? environment)
{ bool HasSync; string Owner; IReadOnlyList<RecurringDefinition> Definitions; }                                               // Task 9
internal static class RecurringSyncLog { static void Write(ILogger logger, string owner, RecurringSyncResult result); }       // Task 9

// PgBolt.Generators (Task 9, Steps 8–12)
internal static class KnownNames { const string RecurringManagerMetadata = "PgBolt.IRecurringJobManager";
    const string OptionsMetadata = "PgBolt.Postgres.PgBoltOptions";
    static bool IsRecurringMethod(IMethodSymbol method, INamedTypeSymbol? manager, INamedTypeSymbol? options); }

// PgBolt.Postgres — публичные (Task 9)
public sealed class PgBoltOptions { string? RecurringOwner; AddRecurring<TArgs>(id, cron, args, options); AddRecurring(id, jobName, cron,
    JsonElement args, options); ClearRecurring(); internal IReadOnlyList<RecurringDeclaration> Recurring; internal bool RecurringCleared; }
// PgBoltInitializer(PgBoltOptions, NpgsqlDataSource, MigrationRunner, PartitionManager, PgBoltStore store, RecurringDeclarations recurring,
//     ILogger<PgBoltInitializer>)                                                                                            // Task 1, 9

// PgBolt.Server (Task 10)
public sealed class PgBoltProcessingOptions { bool RunScheduler; TimeSpan SchedulerInterval; TimeSpan? MisfireThreshold;
    TimeSpan RecurringSyncInterval; internal TimeSpan EffectiveMisfireThreshold; }
internal interface IServerStore
{
    Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct);
    Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger, CancellationToken ct);
    // остальные члены — без изменений
}
internal sealed class ProcessingMaintenance(IServerStore store, IWakeupSignal wakeup, PgBoltProcessingOptions options,
    IPgBoltMaintenance maintenance, PgBoltOptions core, MonoClock clock, ILogger logger, RecurringDeclarations? recurring = null)
{ internal const int SchedulerBatchSize = 100; internal const int SchedulerMaxFires = 1000;
  internal Task<int> TickOnceAsync(CancellationToken ct); internal Task<int> SyncOnceAsync(CancellationToken ct); }
```

## Review Focus

Входы и условия, которые спека подразумевает, а тесты задач легко пропустить; тест на каждое уже добавлен в задачу-владельца:

1. **Строка расписания с таймзоной или cron, которые под не может разобрать** (записаны другим сервисом или SQL'ем) — остальные строки пакета срабатывают, у битой `last_error` и `wake_at ≈ now()+1 мин`, тик не откатывается (Task 7, `Broken_rows_do_not_block_the_batch`).
2. **`CatchUp + Skip` при живом предыдущем запуске** — срабатывание не теряется и не дублируется: курсор стоит, строка перепроверяется через 5 с, после завершения запуска срабатывает следующее по порядку (Task 7, `CatchUp_skip_waits_for_running_job_and_keeps_order`).
3. **Смена cron через API или синхронизацию, пока тик держит строку** (тик удерживается хуком `TickBeforeCommit` между захватом и `COMMIT`) — изменение ждёт `FOR UPDATE`, читает свежий `last_fire_at`, совпадающее время не ставится дважды (`after(max(db_now, last_fire_at))`); `Remove` / `Pause` под удерживаемым тиком — тик завершается, задача создана, строка удалена / на паузе (Task 7, `Api_cron_change_while_tick_holds_row_fires_once`, `Remove_while_tick_holds_row_lets_tick_finish`, `Pause_while_tick_holds_row_lets_tick_finish_and_stops_next_ticks`; Task 8, `Cron_change_while_tick_holds_row_fires_once`; в тестах под удерживаемым тиком `db_now` API позже `last_fire_at`, поэтому свежесть чтения `last_fire_at` (а не только ожидание `FOR UPDATE` и отсутствие дубля) проверяет отдельно Task 6, `Cron_change_after_tick_does_not_repeat_fired_time`).
4. **Две синхронизации разных владельцев с пересекающимися id в обратном порядке** — без `40P01`: хранилище с `retryOnConflict: false` (одна попытка: deadlock не гасится повтором и проявился бы исключением `40P01`); каждый id у одного владельца, у другого — коллизия (Task 8, `Cross_owner_collisions_in_reverse_order_do_not_deadlock`). Повтор `40P01` синхронизации — до 3 раз с Warning (Task 8, `RecurringSyncRetryTests`).
5. **Схема, созданная прошлой сборкой** (версия 1 или 2 в `schema_version`, нет `recurring_jobs`) — старт падает с понятным сообщением при `AutoMigrate = true` и `false` (Task 1, `Pre_release_schema_fails_start`).
6. **Продюсер без исполнителя** (API-сервис с контрактами из другой сборки и `[PgBoltJsonContext]`, без `EnqueueAsync` того же типа) объявляет `AddRecurring<TArgs>` или вызывает `AddOrUpdateAsync<TArgs>` — метаданные `TArgs` зарегистрированы генератором по месту вызова, нет «No PgBolt metadata»; тип вне JSON-контекста — BOLT001 (Task 9, `Typed_recurring_call_sites_register_types_from_referenced_assemblies`, `Recurring_args_missing_in_context_BOLT001`).

## Карта файлов

| Файл | Ответственность |
|---|---|
| `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql` | вся схема, включая `maintenance_tasks`, `recurring_jobs`, `jobs.recurring_id/scheduled_at` |
| `src/PgBolt.Abstractions/RecurringJobOptions.cs`, `RecurringJobInfo.cs`, `IRecurringJobManager.cs` | публичные контракты §4.1 |
| `src/PgBolt.Postgres/Recurring/CronSchedule.cs` | разбор cron (формат по числу полей / макрос), таймзона → IANA, `After`, `Between` |
| `src/PgBolt.Postgres/Recurring/RecurringDefinition.cs` | валидация и нормализация определения (§4.2) |
| `src/PgBolt.Postgres/Recurring/RecurringPlanner.cs` | чистая функция правил срабатывания (§5.3), бинарный поиск последнего `due` |
| `src/PgBolt.Postgres/Recurring/RecurringDeclarations.cs` | code-first объявления процесса: владелец, участие, построенные определения |
| `src/PgBolt.Postgres/Recurring/RecurringSyncLog.cs` | логи записей и коллизий синхронизации (§7) |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Recurring.cs` | CRUD расписаний (API) |
| `src/PgBolt.Postgres/Storage/PgBoltStore.RecurringTick.cs` | транзакция тика (§5.2); тестовый хук `TickBeforeCommit` |
| `src/PgBolt.Postgres/Storage/PgBoltStore.RecurringSync.cs` | транзакция синхронизации (§6.2); повтор `40P01` с Warning (§7) |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs` | прежний `EnqueueTemplate` клиента и внутренний `RecurringEnqueueTemplate` с `recurring_id` / `scheduled_at` (§8) |
| `src/PgBolt.Postgres/Storage/RecurringRecords.cs` | результаты тика/синхронизации/CRUD |
| `src/PgBolt.Postgres/Client/RecurringJobManager.cs` | реализация `IRecurringJobManager` |
| `src/PgBolt.Postgres/Client/JobMetadataAccess.cs` | общий доступ к `JobMetadata<TArgs>` для клиента и recurring |
| `src/PgBolt.Postgres/Recurring/ReservedUniqueKeys.cs` | префикс `pgbolt:` и ключ наложения |
| `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs` | циклы `scheduler`, `recurring-sync` |

Изменяемые файлы (без новой ответственности; подробности — в разделе **Files** задачи):

| Файл | Задача | Изменение |
|---|---|---|
| `src/PgBolt.Postgres/Schema/Migrations/V002__maintenance_tasks.sql` | 1 | удаляется (влит в V001) |
| `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` | 1, 9 | проверка схемы прошлых сборок; синхронизация на старте |
| `src/PgBolt.Abstractions/JobContext.cs`, `JobInfo.cs` | 2 | `ScheduledAt`, `RecurringJobId` |
| `src/PgBolt.Postgres/Storage/StoreRecords.cs` | 3 | `EnqueueBatch.ScheduledAtUtc/RecurringIds`, `LeasedJob.RecurringId/ScheduledAtUtc` |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Dequeue.cs`, `PgBoltStore.GetJob.cs` | 3 | чтение `recurring_id`, `scheduled_at` |
| `src/PgBolt.Postgres/Client/PgBoltJobClient.cs` | 3 | `JobMetadataAccess`, запрет префикса `pgbolt:` |
| `src/PgBolt.Server/Runtime/JobRunner.cs` | 3 | поля расписания в `JobContext` |
| `Directory.Packages.props`, `src/PgBolt.Postgres/PgBolt.Postgres.csproj` | 4 | пакет Cronos 0.13.0 |
| `src/PgBolt.Postgres/Storage/P.cs` | 6 | `AddValue<T>` |
| `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs` | 6, 9 | `IRecurringJobManager`, `RecurringDeclarations`, параметры инициализатора |
| `src/PgBolt.Postgres/PgBoltOptions.cs` | 9 | `RecurringOwner`, `AddRecurring`, `ClearRecurring` |
| `src/PgBolt.Generators/KnownNames.cs`, `JobGenerator.cs`, `Models/EnqueueCallModel.cs`, `Analysis/AnalysisState.cs` | 9 | метаданные и проверки JSON для типизированных recurring-вызовов |
| `src/PgBolt.Server/PgBoltProcessingOptions.cs` | 10 | `RunScheduler`, `SchedulerInterval`, `MisfireThreshold`, `RecurringSyncInterval` |
| `src/PgBolt.Server/Runtime/IServerStore.cs`, `PgBoltServerStore.cs` | 10 | `TickRecurringAsync`, `SyncRecurringAsync` |
| `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs` | 10 | условие запуска `ProcessingMaintenance` |
| `CHANGELOG.md`, roadmap, строка статуса спеки | 12 | документация |

---
### Task 1: Объединённая миграция V001 и проверка схемы прошлых сборок

**Files:**
- Modify: `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql` (полная замена)
- Delete: `src/PgBolt.Postgres/Schema/Migrations/V002__maintenance_tasks.sql`
- Modify: `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` (`InitializeAsync`, новый метод)
- Test: `tests/PgBolt.Postgres.Tests/MigrationTests.cs`, `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Produces: таблица `{schema}.recurring_jobs` (колонки §3 спеки), индексы `recurring_due (wake_at) WHERE NOT paused`, `recurring_owner (owner) WHERE owner IS NOT NULL`; колонки `jobs.recurring_id text`, `jobs.scheduled_at timestamptz`; `PgBoltMigrations.LatestVersion == 1`.

- [x] **Step 1: Переписать тесты миграций под одну версию**

В `tests/PgBolt.Postgres.Tests/MigrationTests.cs`:
- в `Migrate_creates_all_tables_once` добавить `"recurring_jobs"` в массив таблиц;
- в `Maintenance_tasks_has_spec_columns_and_no_rows` заменить `Assert.Equal(2, PgBoltMigrations.LatestVersion);` на `Assert.Equal(1, PgBoltMigrations.LatestVersion);`;
- удалить тесты `Script_from_version_1_contains_only_v002` и `Upgrade_from_v001_with_data_applies_v002` (второй версии больше нет);
- добавить тесты:

```csharp
    [Fact]
    public async Task Recurring_jobs_has_spec_columns_and_indexes()
    {
        await Runner().MigrateAsync(Ct);
        await using var cmd = _db.Admin.CreateCommand("""
            SELECT string_agg(attname || ':' || format_type(atttypid, atttypmod) || ':' || attnotnull, ',' ORDER BY attnum)
            FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND NOT attisdropped
            """);
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.recurring_jobs");
        Assert.Equal(
            "id:text:true,type:text:true,args:jsonb:true,cron:text:true,time_zone:text:true,misfire:smallint:true," +
            "catch_up_window:interval:true,overlap:smallint:true,queue:text:false,priority:smallint:false," +
            "max_attempts:integer:false,retry_delays:integer[]:false,owner:text:false,paused:boolean:true," +
            "next_fire_at:timestamp with time zone:false,wake_at:timestamp with time zone:false," +
            "last_fire_at:timestamp with time zone:false,last_job_id:uuid:false,last_error:text:false," +
            "created_at:timestamp with time zone:true,updated_at:timestamp with time zone:true",
            (string)(await cmd.ExecuteScalarAsync(Ct))!);
        Assert.Equal("recurring_due,recurring_owner", await _db.ScalarAsync<string>(
            "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes WHERE schemaname = @s AND tablename = 'recurring_jobs' AND indexname <> 'recurring_jobs_pkey'",
            ("s", _db.Schema)));
    }

    [Fact]
    public async Task Jobs_partitions_have_recurring_columns()
    {
        await Runner().MigrateAsync(Ct);
        await new PartitionManager(_db.Admin, _db.Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(1, Ct);

        var missing = await _db.ScalarAsync<long>("""
            SELECT count(*) FROM pg_inherits i
            WHERE i.inhparent = to_regclass(@t)
              AND (SELECT count(*) FROM pg_attribute a
                   WHERE a.attrelid = i.inhrelid AND a.attname IN ('recurring_id', 'scheduled_at') AND NOT a.attisdropped) <> 2
            """, ("t", $"{_db.Schema}.jobs"));

        Assert.Equal(0L, missing);
        Assert.Equal(2L, await _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
    }

    [Fact]
    public void Script_contains_single_merged_migration()
    {
        var script = PgBoltMigrations.GetScript("my_schema");
        Assert.Contains("-- V001 initial", script);
        Assert.DoesNotContain("-- V002", script);
        Assert.Contains("CREATE TABLE my_schema.maintenance_tasks", script);
        Assert.Contains("CREATE TABLE my_schema.recurring_jobs", script);
        Assert.Contains("VALUES (1, now())", script);
    }
```

В `tests/PgBolt.Postgres.Tests/HostingTests.cs`:
- в `Needs_migration_only_for_missing_or_older_schema` заменить `[InlineData(true, 1, true)]` на `[InlineData(true, 1, false)]` (версия 1 теперь последняя, не устаревшая); остальные строки не менять;
- заменить тест `AutoMigrate_off_on_v001_schema_fails_start_with_version` на:

```csharp
    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    [InlineData(false, 2)]
    public async Task Pre_release_schema_fails_start(bool autoMigrate, int version)   // часть 4 §3, §10
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("DROP TABLE {schema}.recurring_jobs");        // схема прошлой сборки: версия есть, объектов части 4 нет
        if (version == 2)                                                  // прошлая сборка с V002__maintenance_tasks
            await _db.ExecAsync("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (2, now())");
        await using var sp = Build(o => o.AutoMigrate = autoMigrate);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));

        Assert.Contains("pre-release", e.Message);
        Assert.Contains($"DROP SCHEMA {_db.Schema} CASCADE", e.Message);
    }
```

Если `MigrationTests.cs` не импортирует `PgBolt.Postgres.Schema` для `PartitionManager` — он уже импортирован (`using PgBolt.Postgres.Schema;`).

- [x] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests"`
Expected: FAIL — нет `recurring_jobs`, `LatestVersion` = 2 (в том числе `Needs_migration_only_for_missing_or_older_schema(true, 1, false)`), старт на схеме без `recurring_jobs` не падает.

- [x] **Step 3: Объединить миграции**

Удалить `src/PgBolt.Postgres/Schema/Migrations/V002__maintenance_tasks.sql` (`git rm`). Заменить содержимое `V001__initial.sql` целиком:

```sql
CREATE TABLE {schema}.queue (
    job_id           uuid        PRIMARY KEY,
    queue            text        NOT NULL,
    priority         smallint    NOT NULL DEFAULT 0,
    run_at           timestamptz NOT NULL,
    ready            boolean     NOT NULL,
    attempt          int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    lease_owner      uuid,
    lease_id         uuid,
    lease_until      timestamptz,
    leased_at        timestamptz,
    unique_key       text,
    cancel_requested boolean     NOT NULL DEFAULT false
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 5000,
        autovacuum_vacuum_cost_limit = 2000);

CREATE INDEX queue_fetch   ON {schema}.queue (queue, priority DESC, run_at) WHERE lease_owner IS NULL AND ready;
CREATE INDEX queue_delayed ON {schema}.queue (run_at)                       WHERE NOT ready;
CREATE INDEX queue_owner   ON {schema}.queue (lease_owner)                  WHERE lease_owner IS NOT NULL;
CREATE INDEX queue_expiry  ON {schema}.queue (lease_until)                  WHERE lease_owner IS NOT NULL;
CREATE UNIQUE INDEX queue_unique ON {schema}.queue (unique_key)             WHERE unique_key IS NOT NULL;

CREATE TABLE {schema}.jobs (
    id            uuid        NOT NULL,
    type          text        NOT NULL,
    queue         text        NOT NULL,
    priority      smallint    NOT NULL,
    args          jsonb       NOT NULL,
    state         smallint    NOT NULL,
    attempt       int         NOT NULL DEFAULT 0,
    max_attempts  int         NOT NULL,
    retry_delays  int[],
    unique_key    text,
    parent_id     uuid,
    batch_id      uuid,
    trace_parent  text,
    created_at    timestamptz NOT NULL,
    started_at    timestamptz,
    finished_at   timestamptz,
    server_id     uuid,
    recurring_id  text,
    scheduled_at  timestamptz,
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);

CREATE TABLE {schema}.job_events (
    job_id     uuid        NOT NULL,
    at         timestamptz NOT NULL,
    kind       smallint    NOT NULL,
    server_id  uuid,
    attempt    int,
    message    text,
    exception  text
) PARTITION BY RANGE (at);

CREATE INDEX job_events_job_id_idx ON {schema}.job_events (job_id);

CREATE TABLE {schema}.job_types (
    name                 text PRIMARY KEY,
    args_schema          jsonb       NOT NULL,
    default_queue        text        NOT NULL,
    default_max_attempts int         NOT NULL,
    default_priority     smallint    NOT NULL,
    default_retry_delays int[],
    published_by         text        NOT NULL,
    last_seen_at         timestamptz NOT NULL
);

CREATE TABLE {schema}.servers (
    id           uuid PRIMARY KEY,
    name         text        NOT NULL,
    host         text        NOT NULL,
    queues       text[]      NOT NULL,
    capacity     int         NOT NULL,
    started_at   timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL
);

CREATE TABLE {schema}.maintenance_tasks (
    name             text        PRIMARY KEY,
    next_run_at      timestamptz NOT NULL,
    last_run_at      timestamptz,
    last_finished_at timestamptz,
    last_error       text
);

CREATE TABLE {schema}.recurring_jobs (
    id              text        PRIMARY KEY,
    type            text        NOT NULL,
    args            jsonb       NOT NULL,
    cron            text        NOT NULL,
    time_zone       text        NOT NULL,
    misfire         smallint    NOT NULL,
    catch_up_window interval    NOT NULL,
    overlap         smallint    NOT NULL,
    queue           text,
    priority        smallint,
    max_attempts    int,
    retry_delays    int[],
    owner           text,
    paused          boolean     NOT NULL DEFAULT false,
    next_fire_at    timestamptz,
    wake_at         timestamptz,
    last_fire_at    timestamptz,
    last_job_id     uuid,
    last_error      text,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE INDEX recurring_due   ON {schema}.recurring_jobs (wake_at) WHERE NOT paused;
CREATE INDEX recurring_owner ON {schema}.recurring_jobs (owner)   WHERE owner IS NOT NULL;
```

- [x] **Step 4: Проверка схемы прошлых сборок в `PgBoltInitializer`**

В `InitializeAsync` сразу после блока `if (version > PgBoltMigrations.LatestVersion) _logger.LogWarning(...)` вставить вызов:

```csharp
        await EnsureNotPreReleaseSchemaAsync(cancellationToken);
```

и добавить метод рядом с `EnsureReadCommittedDefaultAsync`:

```csharp
    /// <summary>
    /// Часть 4 §3: миграции объединены в одну V001, схемы прошлых сборок (версия 1 или 2 без объектов части 4) не обновляются.
    /// Признак такой схемы — нет recurring_jobs после миграций и проверки версии.
    /// </summary>
    private async Task EnsureNotPreReleaseSchemaAsync(CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT to_regclass(@t) IS NULL", conn);
        cmd.Parameters.AddWithValue("t", $"{_options.Schema}.recurring_jobs");
        if ((bool)(await cmd.ExecuteScalarAsync(ct))!)
            throw new InvalidOperationException(
                $"PgBolt schema '{_options.Schema}' was created by a pre-release PgBolt build and cannot be upgraded; " +
                $"drop it (DROP SCHEMA {_options.Schema} CASCADE) and restart.");
    }
```

- [x] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests"`
Expected: PASS. Затем весь проект: `dotnet test tests/PgBolt.Postgres.Tests` — PASS (остальные тесты не зависят от номера версии; если какой-то падает из-за `LatestVersion`/`V002` — исправить в нём ожидание на единственную версию 1).

- [x] **Step 6: Commit**

```bash
git add -A src/PgBolt.Postgres/Schema/Migrations src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(schema): merge migrations into V001 with recurring_jobs"
```

---

### Task 2: Публичные контракты recurring и поля `JobContext` / `JobInfo`

**Files:**
- Create: `src/PgBolt.Abstractions/RecurringJobOptions.cs`, `src/PgBolt.Abstractions/RecurringJobInfo.cs`, `src/PgBolt.Abstractions/IRecurringJobManager.cs`
- Modify: `src/PgBolt.Abstractions/JobContext.cs`, `src/PgBolt.Abstractions/JobInfo.cs`
- Test: `tests/PgBolt.Abstractions.Tests/RecurringContractsTests.cs`

**Interfaces:**
- Produces: `enum MisfirePolicy { FireOnce, Skip, CatchUp }`, `enum OverlapPolicy { Skip, Allow }`, `sealed record RecurringJobOptions`, `sealed record RecurringJobInfo(...)`, `interface IRecurringJobManager` (сигнатуры — ниже); `JobContext.ScheduledAt : DateTimeOffset?`, `JobContext.RecurringJobId : string?` (параметры конструктора `scheduledAt = null, recurringJobId = null` в конце); `JobInfo.RecurringJobId : string?`, `JobInfo.ScheduledAt : DateTimeOffset?` (последние позиционные параметры со значением `null` по умолчанию).

- [x] **Step 1: Тесты контрактов**

`tests/PgBolt.Abstractions.Tests/RecurringContractsTests.cs`:

```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;

namespace PgBolt.Abstractions.Tests;

public sealed class RecurringContractsTests
{
    [Fact]
    public void Options_have_spec_defaults()
    {
        var o = new RecurringJobOptions();
        Assert.Null(o.TimeZone);
        Assert.Equal(MisfirePolicy.FireOnce, o.Misfire);
        Assert.Equal(TimeSpan.FromDays(1), o.CatchUpWindow);
        Assert.Equal(OverlapPolicy.Skip, o.Overlap);
        Assert.Null(o.Queue);
        Assert.Null(o.Priority);
        Assert.Null(o.MaxAttempts);
        Assert.Null(o.RetryDelays);
    }

    [Fact]
    public void Policy_codes_match_stored_values()
    {
        Assert.Equal(0, (int)MisfirePolicy.FireOnce);
        Assert.Equal(1, (int)MisfirePolicy.Skip);
        Assert.Equal(2, (int)MisfirePolicy.CatchUp);
        Assert.Equal(0, (int)OverlapPolicy.Skip);
        Assert.Equal(1, (int)OverlapPolicy.Allow);
    }

    [Fact]
    public void Job_context_carries_schedule_fields()
    {
        var at = new DateTimeOffset(2026, 9, 30, 3, 0, 0, TimeSpan.Zero);
        var plain = new JobContext(JobId.New(), "n", 1, 3, null, null, NullLogger.Instance);
        var recurring = new JobContext(JobId.New(), "n", 1, 3, null, null, NullLogger.Instance, at, "daily");

        Assert.Null(plain.ScheduledAt);
        Assert.Null(plain.RecurringJobId);
        Assert.Equal(at, recurring.ScheduledAt);
        Assert.Equal("daily", recurring.RecurringJobId);
    }

    [Fact]
    public void Job_info_schedule_fields_default_to_null()
    {
        using var args = JsonDocument.Parse("{}");
        var info = new JobInfo(JobId.New(), "n", "q", 0, JobState.Enqueued, 0, 3, DateTimeOffset.UtcNow, null, null, null,
            null, null, false, args.RootElement.Clone(), []);

        Assert.Null(info.RecurringJobId);
        Assert.Null(info.ScheduledAt);
    }
}
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Abstractions.Tests --filter "FullyQualifiedName~RecurringContractsTests"`
Expected: FAIL — `MisfirePolicy`, `RecurringJobOptions`, `JobContext.ScheduledAt` не найдены.

- [x] **Step 3: Контракты**

`src/PgBolt.Abstractions/RecurringJobOptions.cs`:

```csharp
namespace PgBolt;

/// <summary>Обработка срабатываний, пропущенных за простой или паузу (часть 4 §5.3). Значения хранятся в recurring_jobs.misfire.</summary>
public enum MisfirePolicy
{
    /// <summary>Одно срабатывание за все пропущенные; ScheduledAt — последнее из них.</summary>
    FireOnce = 0,

    /// <summary>Только если последнее пропущенное не старше MisfireThreshold; иначе ничего.</summary>
    Skip = 1,

    /// <summary>Каждое пропущенное срабатывание не старше CatchUpWindow.</summary>
    CatchUp = 2,
}

/// <summary>Поведение при живом предыдущем запуске (часть 4 §5.3). Значения хранятся в recurring_jobs.overlap.</summary>
public enum OverlapPolicy
{
    /// <summary>Не ставить, пока жив предыдущий запуск (ключ pgbolt:recurring:{id}); при CatchUp — ждать.</summary>
    Skip = 0,

    /// <summary>Ставить всегда.</summary>
    Allow = 1,
}

public sealed record RecurringJobOptions
{
    /// <summary>Таймзона cron; null — UTC. Хранится как IANA id.</summary>
    public TimeZoneInfo? TimeZone { get; init; }

    public MisfirePolicy Misfire { get; init; } = MisfirePolicy.FireOnce;

    /// <summary>CatchUp догоняет срабатывания не старше окна; (0, 366 суток].</summary>
    public TimeSpan CatchUpWindow { get; init; } = TimeSpan.FromDays(1);

    public OverlapPolicy Overlap { get; init; } = OverlapPolicy.Skip;

    /// <summary>null — умолчание задачи из job_types на момент срабатывания (как и остальные поля ниже).</summary>
    public string? Queue { get; init; }

    public short? Priority { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}
```

`src/PgBolt.Abstractions/RecurringJobInfo.cs`:

```csharp
using System.Text.Json;

namespace PgBolt;

/// <summary>Строка recurring_jobs (часть 4 §4.1). Owner = null — расписание создано через API.</summary>
public sealed record RecurringJobInfo(
    string Id,
    string JobName,
    JsonElement Args,
    string Cron,
    string TimeZone,
    MisfirePolicy Misfire,
    TimeSpan CatchUpWindow,
    OverlapPolicy Overlap,
    string? Queue,
    short? Priority,
    int? MaxAttempts,
    IReadOnlyList<TimeSpan>? RetryDelays,
    string? Owner,
    bool Paused,
    DateTimeOffset? NextFireAt,
    DateTimeOffset? LastFireAt,
    JobId? LastJobId,
    string? LastError,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);
```

`src/PgBolt.Abstractions/IRecurringJobManager.cs`:

```csharp
using System.Text.Json;

namespace PgBolt;

/// <summary>
/// Runtime API расписаний (часть 4 §4.2). Строки code-first владельцев (Owner != null) через API только
/// приостанавливаются, возобновляются, запускаются и читаются; AddOrUpdate и Remove для них — InvalidOperationException.
/// </summary>
public interface IRecurringJobManager
{
    /// <remarks>Метаданные TArgs (JobMetadata&lt;TArgs&gt;) генератор регистрирует по месту этого вызова, как для EnqueueAsync.</remarks>
    ValueTask AddOrUpdateAsync<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null, CancellationToken ct = default);

    ValueTask AddOrUpdateAsync(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null,
        CancellationToken ct = default);

    /// <returns>false — расписания нет.</returns>
    ValueTask<bool> RemoveAsync(string id, CancellationToken ct = default);

    ValueTask<bool> PauseAsync(string id, CancellationToken ct = default);

    /// <summary>Пропущенное за паузу обрабатывается misfire-политикой расписания.</summary>
    ValueTask<bool> ResumeAsync(string id, CancellationToken ct = default);

    /// <summary>Ставит задачу сейчас, курсор не сдвигает; null — расписания нет.</summary>
    ValueTask<EnqueueResult?> TriggerAsync(string id, CancellationToken ct = default);

    ValueTask<RecurringJobInfo?> GetAsync(string id, CancellationToken ct = default);
}
```

`src/PgBolt.Abstractions/JobContext.cs` — заменить целиком:

```csharp
using Microsoft.Extensions.Logging;

namespace PgBolt;

public sealed class JobContext(JobId jobId, string jobName, int attempt, int maxAttempts, JobId? parentId, Guid? batchId, ILogger logger,
    DateTimeOffset? scheduledAt = null, string? recurringJobId = null)
{
    public JobId JobId { get; } = jobId;
    public string JobName { get; } = jobName;
    public int Attempt { get; } = attempt;
    public int MaxAttempts { get; } = maxAttempts;
    public JobId? ParentId { get; } = parentId;
    public Guid? BatchId { get; } = batchId;
    public ILogger Logger { get; } = logger;

    /// <summary>Момент срабатывания cron, за который поставлена задача (часть 4); null — задача не из расписания.</summary>
    public DateTimeOffset? ScheduledAt { get; } = scheduledAt;

    /// <summary>Id расписания (часть 4); null — задача не из расписания.</summary>
    public string? RecurringJobId { get; } = recurringJobId;
}
```

`src/PgBolt.Abstractions/JobInfo.cs` — в конец позиционных параметров `JobInfo` после `IReadOnlyList<JobEventInfo> Events` добавить:

```csharp
    IReadOnlyList<JobEventInfo> Events,
    string? RecurringJobId = null,
    DateTimeOffset? ScheduledAt = null);
```

- [x] **Step 4: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Abstractions.Tests`
Expected: PASS. `dotnet build PgBolt.slnx` — без ошибок (существующие вызовы `new JobContext(...)`/`new JobInfo(...)` совместимы по исходному коду).

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Abstractions tests/PgBolt.Abstractions.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(abstractions): recurring job contracts and schedule fields in JobContext"
```

---
### Task 3: Колонки расписания в enqueue / dequeue / GetJob, резерв префикса `pgbolt:`

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/StoreRecords.cs` (`EnqueueBatch`, `LeasedJob`)
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs`, `PgBoltStore.Dequeue.cs`, `PgBoltStore.GetJob.cs`
- Modify: `src/PgBolt.Postgres/Client/PgBoltJobClient.cs`
- Create: `src/PgBolt.Postgres/Recurring/ReservedUniqueKeys.cs`, `src/PgBolt.Postgres/Client/JobMetadataAccess.cs`
- Modify: `src/PgBolt.Server/Runtime/JobRunner.cs:87-88`
- Test: `tests/PgBolt.Postgres.Tests/EnqueueTests.cs`, `DequeueTests.cs`, `GetJobTests.cs`, `ClientEnqueueTests.cs`, `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs`

**Interfaces:**
- Consumes: колонки `jobs.recurring_id`, `jobs.scheduled_at` (Task 1); `JobContext(…, scheduledAt, recurringJobId)`, `JobInfo.RecurringJobId/ScheduledAt` (Task 2).
- Produces:
  - `EnqueueBatch.ScheduledAtUtc : DateTime?[]?` и `EnqueueBatch.RecurringIds : string[]?` (init). `RecurringIds = null` — задачи не из расписания: прежний SQL клиента (`EnqueueTemplate` без изменений, спека §8 «внешний enqueue клиента использует прежний SQL»). `RecurringIds != null` — внутренний вариант `RecurringEnqueueTemplate` (тик, Trigger) с массивами `scheduled_at` / `recurring_id` и `scheduled_at = COALESCE(i.scheduled_at, now())`: `ScheduledAtUtc = null` или `ScheduledAtUtc[i] == null` — `now()` БД. `ScheduledAtUtc` без `RecurringIds` — `ArgumentException`;
  - `internal string PgBoltStore.EnqueueSqlFor(EnqueueBatch batch)` — выбор шаблона по `batch.RecurringIds is not null`;
  - `internal NpgsqlBatchCommand PgBoltStore.CreateEnqueueCommand(EnqueueBatch batch)` — statement enqueue для `NpgsqlBatch` в чужой транзакции; результат — строки `(id uuid, created bool, existed bool)`;
  - `LeasedJob.RecurringId : string?`, `LeasedJob.ScheduledAtUtc : DateTime?` (последние позиционные параметры, по умолчанию `null`);
  - `internal static class ReservedUniqueKeys { const string Prefix = "pgbolt:"; static string Recurring(string id); static void EnsureUserKeys(string?[] keys); }`;
  - `internal static class JobMetadataAccess { static (JobDescriptor Descriptor, JsonTypeInfo<TArgs> TypeInfo) Get<TArgs>(); }`.

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/EnqueueTests.cs` — добавить в абстрактный класс:

```csharp
    [Fact]
    public async Task Recurring_variant_writes_schedule_columns()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        Guid[] ids = [Guid.CreateVersion7(), Guid.CreateVersion7()];
        var batch = new EnqueueBatch("test.job", "q", 0, 3, null, null, TimeSpan.Zero, ids, ["{}", "{}"],
            [null, null], [null, null])
        {
            RecurringIds = ["daily", "hourly"],
            ScheduledAtUtc = [at, null],
        };

        var rows = await Store.EnqueueAsync(batch, null, Ct);

        Assert.All(rows, r => Assert.True(r.Created));
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT recurring_id = 'daily' AND scheduled_at = @at FROM {schema}.jobs WHERE id = @id", ("id", ids[0]), ("at", at)));
        Assert.True(await Db.ScalarAsync<bool>(   // null момент — COALESCE(scheduled_at, now()): время БД (Trigger, §5.4, §8)
            "SELECT recurring_id = 'hourly' AND scheduled_at = created_at FROM {schema}.jobs WHERE id = @id", ("id", ids[1])));
    }

    [Fact]
    public async Task Recurring_variant_without_scheduled_at_uses_database_now()
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id) with { RecurringIds = ["daily"] }, null, Ct);
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT recurring_id = 'daily' AND scheduled_at = created_at FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Plain_enqueue_leaves_recurring_columns_null()
    {
        var id = await EnqueueAsync();
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT recurring_id IS NULL AND scheduled_at IS NULL FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public void Client_enqueue_keeps_original_sql()                     // §8: внешний enqueue — прежний SQL, колонки — внутренний вариант
    {
        var plain = Store.EnqueueSqlFor(Batch(Guid.CreateVersion7()));
        var recurring = Store.EnqueueSqlFor(Batch(Guid.CreateVersion7()) with { RecurringIds = ["daily"] });

        Assert.DoesNotContain("recurring_id", plain);
        Assert.DoesNotContain("scheduled_at", plain);
        Assert.Contains("@recurring_ids::text[]", recurring);
        Assert.Contains("COALESCE(i.scheduled_at, now())", recurring);
    }

    [Fact]
    public async Task Recurring_arrays_must_match_ids() =>
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Store.EnqueueAsync(Batch(Guid.CreateVersion7()) with { RecurringIds = ["a", "b"] }, null, Ct));

    [Fact]
    public async Task Scheduled_at_requires_recurring_ids() =>
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Store.EnqueueAsync(Batch(Guid.CreateVersion7()) with { ScheduledAtUtc = [DateTime.UtcNow] }, null, Ct));
```

`tests/PgBolt.Postgres.Tests/DequeueTests.cs`:

```csharp
    [Fact]
    public async Task Leased_job_carries_recurring_columns()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        await Store.EnqueueAsync(Batch(Guid.CreateVersion7()) with { RecurringIds = ["daily"], ScheduledAtUtc = [at] }, null, Ct);

        var job = await LeaseOneAsync();

        Assert.Equal("daily", job.RecurringId);
        Assert.Equal(at, job.ScheduledAtUtc);
        Assert.Equal(DateTimeKind.Utc, job.ScheduledAtUtc!.Value.Kind);
    }
```

`tests/PgBolt.Postgres.Tests/GetJobTests.cs`:

```csharp
    [Fact]
    public async Task Get_returns_recurring_columns()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id) with { RecurringIds = ["daily"], ScheduledAtUtc = [at] }, null, Ct);

        var info = await Store.GetJobAsync(id, Ct);

        Assert.Equal("daily", info!.RecurringJobId);
        Assert.Equal(new DateTimeOffset(at), info.ScheduledAt);
    }
```

`tests/PgBolt.Postgres.Tests/ClientEnqueueTests.cs`:

```csharp
    [Theory]
    [InlineData("pgbolt:recurring:x")]
    [InlineData("pgbolt:")]
    public async Task Reserved_unique_key_prefix_is_rejected(string key)
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { UniqueKey = key }, Ct));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await Client.EnqueueManyAsync([new ClientArgs(1)], uniqueKeySelector: _ => key, ct: Ct));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }
```

`tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs`:

```csharp
    [Fact]
    public async Task Recurring_fields_reach_job_context()
    {
        var at = new DateTime(2026, 9, 30, 3, 0, 0, DateTimeKind.Utc);
        var job = Leased.Job(type: "runner.job", args: """{"N":1}""") with { RecurringId = "daily", ScheduledAtUtc = at };

        await Runner().RunAsync(new Execution(job, "q", 0, ExecutionState.Running), CancellationToken.None);

        var context = Assert.Single(_probe.Contexts);
        Assert.Equal(new DateTimeOffset(at), context.ScheduledAt);
        Assert.Equal("daily", context.RecurringJobId);
    }
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet build PgBolt.slnx`
Expected: FAIL — нет `EnqueueBatch.RecurringIds`, `LeasedJob.RecurringId`, `PgBoltStore.EnqueueSqlFor`.

- [x] **Step 3: Записи хранилища**

В `StoreRecords.cs` в `EnqueueBatch` добавить свойства и проверки (в конец `Validate`):

```csharp
    /// <summary>Часть 4: момент срабатывания по строкам (только вместе с RecurringIds); null — now() БД.</summary>
    public DateTime?[]? ScheduledAtUtc { get; init; }

    /// <summary>
    /// Часть 4: id расписания по строкам. null — задачи не из расписания (прежний SQL клиента); иначе — внутренний вариант
    /// enqueue с колонками recurring_id / scheduled_at (тик, Trigger; §8).
    /// </summary>
    public string[]? RecurringIds { get; init; }
```

```csharp
        if (ScheduledAtUtc is { } scheduled && scheduled.Length != Ids.Length || RecurringIds is { } recurring && recurring.Length != Ids.Length)
            throw new ArgumentException("Batch arrays must have equal length.");
        if (ScheduledAtUtc is not null && RecurringIds is null)
            throw new ArgumentException("ScheduledAtUtc requires RecurringIds.");
        if (RecurringIds is not null)
            foreach (var id in RecurringIds)
                if (string.IsNullOrEmpty(id))
                    throw new ArgumentException("RecurringIds values must be non-empty.");
        if (ScheduledAtUtc is not null)
            foreach (var at in ScheduledAtUtc)
                if (at is { Kind: not DateTimeKind.Utc })
                    throw new ArgumentException("ScheduledAtUtc values must have DateTimeKind.Utc.");
```

`LeasedJob` — добавить последние параметры:

```csharp
    string? TraceParent,
    string? RecurringId = null,
    DateTime? ScheduledAtUtc = null);
```

- [x] **Step 4: Enqueue**

В `PgBoltStore.Enqueue.cs` `EnqueueTemplate` **не менять** (спека §8: внешний enqueue клиента — прежний SQL, горячий путь не затрагивается, §1). Рядом с ним добавить внутренний вариант с колонками расписания — тот же statement, отличаются только CTE `src` (два массива) и `j` (две колонки, `scheduled_at = COALESCE(i.scheduled_at, now())`, §8):

```csharp
    /// <summary>
    /// Часть 4 §8: вариант EnqueueTemplate для тика и Trigger — дополнительные массивы scheduled_at и recurring_id;
    /// scheduled_at = COALESCE(i.scheduled_at, now()) (Trigger передаёт NULL — время БД, §5.4). Клиент им не пользуется.
    /// </summary>
    private const string RecurringEnqueueTemplate = """
        WITH src AS (
            SELECT * FROM unnest(@ids::uuid[], @args::jsonb[], @run_at::timestamptz[], @unique_keys::text[],
                                 @scheduled_at::timestamptz[], @recurring_ids::text[])
                     AS t(id, args, run_at, unique_key, scheduled_at, recurring_id)
        ),
        existing AS (
            SELECT id FROM {schema}.jobs WHERE id = ANY(@ids)
        ),
        input AS (
            SELECT * FROM src WHERE id NOT IN (SELECT id FROM existing)
        ),
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key)
            SELECT id, @queue, @priority, r.run_at, r.run_at <= now(), @max_attempts, unique_key
            FROM input CROSS JOIN LATERAL (SELECT COALESCE(input.run_at, now() + @delay) AS run_at) r
            ORDER BY unique_key, id
            ON CONFLICT DO NOTHING
            RETURNING job_id
        ),
        j AS (
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, retry_delays,
                                       unique_key, trace_parent, created_at, recurring_id, scheduled_at)
            SELECT i.id, @type, @queue, @priority, i.args, 0, @max_attempts, @retry_delays,
                   i.unique_key, @trace_parent, now(), i.recurring_id, COALESCE(i.scheduled_at, now())
            FROM input i JOIN q ON q.job_id = i.id
        )
        SELECT s.id,
               q.job_id IS NOT NULL AS created,
               e.id     IS NOT NULL AS existed
        FROM src s
        LEFT JOIN q        ON q.job_id = s.id
        LEFT JOIN existing e ON e.id = s.id
        """;

    private string RecurringEnqueueSql => field ??= Render(RecurringEnqueueTemplate);

    /// <summary>§8: RecurringIds = null — прежний SQL клиента; иначе — вариант с колонками расписания (тик, Trigger).</summary>
    internal string EnqueueSqlFor(EnqueueBatch batch) => batch.RecurringIds is null ? EnqueueSql : RecurringEnqueueSql;
```

В `EnqueueOnceAsync` заменить `new NpgsqlCommand(EnqueueSql, conn, tx)` на `new NpgsqlCommand(EnqueueSqlFor(b), conn, tx)` (путь Trigger через `EnqueueAsync` с `RecurringIds`), а блок `var ps = cmd.Parameters; P.Add(...)...` — на `BindEnqueue(cmd.Parameters, b);`. Запрос разрешения конфликта (`ResolveTemplate`) не меняется. Вынести привязку параметров в метод; массивы расписания привязываются только для варианта с колонками (в прежнем SQL их нет):

```csharp
    private static void BindEnqueue(NpgsqlParameterCollection ps, EnqueueBatch b)
    {
        P.Add(ps, "ids", b.Ids);
        P.Add(ps, "args", b.ArgsJson, NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
        P.Add(ps, "run_at", b.RunAtUtc, NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "unique_keys", b.UniqueKeys, NpgsqlDbType.Array | NpgsqlDbType.Text);
        if (b.RecurringIds is { } recurringIds)
        {
            P.Add(ps, "scheduled_at", b.ScheduledAtUtc ?? new DateTime?[b.Ids.Length], NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
            P.Add(ps, "recurring_ids", recurringIds, NpgsqlDbType.Array | NpgsqlDbType.Text);
        }
        P.Add(ps, "type", b.Type);
        P.Add(ps, "queue", b.Queue);
        P.Add(ps, "priority", b.Priority);
        P.Add(ps, "max_attempts", b.MaxAttempts);
        P.AddNullable(ps, "retry_delays", b.RetryDelaysSeconds, NpgsqlDbType.Array | NpgsqlDbType.Integer);
        P.AddNullable(ps, "trace_parent", b.TraceParent, NpgsqlDbType.Text);
        P.Add(ps, "delay", b.Delay);
    }

    /// <summary>
    /// Часть 4 §5.2: statement enqueue для NpgsqlBatch тика — в его транзакции, без запроса разрешения конфликта и без
    /// повторов. Строки результата — (id, created, existed); id свежие, поэтому !created — конфликт по unique_key.
    /// </summary>
    internal NpgsqlBatchCommand CreateEnqueueCommand(EnqueueBatch batch)
    {
        batch.Validate();
        var cmd = new NpgsqlBatchCommand(EnqueueSqlFor(batch));
        BindEnqueue(cmd.Parameters, batch);
        return cmd;
    }
```

- [x] **Step 5: Dequeue и GetJob**

`PgBoltStore.Dequeue.cs`: в последнем `SELECT` шаблона добавить колонки `, j.recurring_id, j.scheduled_at` после `j.trace_parent`; в чтении — два аргумента:

```csharp
            r.GetNullable<int[]>(6), r.GetNullableValue<Guid>(7), r.GetNullableValue<Guid>(8), r.GetNullable<string>(9),
            r.GetNullable<string>(10), r.GetNullableValue<DateTime>(11)), c), ct);
```

`PgBoltStore.GetJob.cs`: в `GetJobTemplate` дописать `, j.recurring_id, j.scheduled_at` в конец списка колонок (индексы 20, 21). Перед `await r.NextResultAsync(c);` прочитать:

```csharp
            var recurringId = r.GetNullable<string>(20);
            var scheduledAt = r.GetNullableValue<DateTimeOffset>(21);
```

и передать в конструктор: `…, cancelRequested, argsElement, events, recurringId, scheduledAt);`.

- [x] **Step 6: Клиент — резерв префикса и общий доступ к метаданным**

`src/PgBolt.Postgres/Recurring/ReservedUniqueKeys.cs` (в `Recurring`, а не `Client`: ключ наложения нужен и хранилищу — тику):

```csharp
namespace PgBolt.Postgres.Recurring;

/// <summary>Часть 4 §4.4: префикс pgbolt: зарезервирован для внутренних ключей (наложение recurring).</summary>
internal static class ReservedUniqueKeys
{
    public const string Prefix = "pgbolt:";

    public static string Recurring(string id) => "pgbolt:recurring:" + id;

    public static void EnsureUserKeys(string?[] keys)
    {
        foreach (var key in keys)
            if (key is not null && key.StartsWith(Prefix, StringComparison.Ordinal))
                throw new ArgumentException($"Unique key '{key}' uses the reserved prefix '{Prefix}'.");
    }
}
```

`src/PgBolt.Postgres/Client/JobMetadataAccess.cs` (перенос приватного `Metadata<TArgs>()` из `PgBoltJobClient`):

```csharp
using System.Text.Json.Serialization.Metadata;

namespace PgBolt.Postgres.Client;

internal static class JobMetadataAccess
{
    public static (JobDescriptor Descriptor, JsonTypeInfo<TArgs> TypeInfo) Get<TArgs>() =>
        JobMetadata<TArgs>.Descriptor is { } d && JobMetadata<TArgs>.TypeInfo is { } ti
            ? (d, ti)
            : throw new InvalidOperationException(
                $"No PgBolt metadata for '{typeof(TArgs).FullName}'. Mark the type with [Job] and use it from an assembly " +
                "that references the PgBolt source generator and declares [assembly: PgBoltJsonContext(...)] including this type.");
}
```

В `PgBoltJobClient.cs`: удалить приватный метод `Metadata<TArgs>()`, вызовы `Metadata<TArgs>()` заменить на `JobMetadataAccess.Get<TArgs>()`; первой строкой `EnqueueCoreAsync` добавить `ReservedUniqueKeys.EnsureUserKeys(uniqueKeys);` (и `using PgBolt.Postgres.Recurring;`).

- [x] **Step 7: JobRunner**

`src/PgBolt.Server/Runtime/JobRunner.cs`, создание контекста:

```csharp
        var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts,
            job.ParentId is { } parent ? new JobId(parent) : null, job.BatchId, _jobLogger,
            job.ScheduledAtUtc is { } scheduled ? new DateTimeOffset(scheduled, TimeSpan.Zero) : null, job.RecurringId);
```

- [x] **Step 8: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~EnqueueTests|FullyQualifiedName~DequeueTests|FullyQualifiedName~GetJobTests|FullyQualifiedName~ClientEnqueueTests"` и `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~JobRunnerTests"`
Expected: PASS.

- [x] **Step 9: Commit**

```bash
git add src tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(core): recurring columns in enqueue, dequeue and job info; reserve pgbolt: unique keys"
```

---
### Task 4: `CronSchedule` и `RecurringDefinition` (Cronos, валидация, нормализация)

**Files:**
- Modify: `Directory.Packages.props`, `src/PgBolt.Postgres/PgBolt.Postgres.csproj`
- Create: `src/PgBolt.Postgres/Recurring/CronSchedule.cs`, `src/PgBolt.Postgres/Recurring/RecurringDefinition.cs`
- Test: `tests/PgBolt.Postgres.Tests/CronScheduleTests.cs`, `tests/PgBolt.Postgres.Tests/RecurringDefinitionTests.cs`

**Interfaces:**
- Consumes: `RecurringJobOptions`, `MisfirePolicy`, `OverlapPolicy` (Task 2).
- Produces:
  - `internal sealed class CronSchedule { static CronSchedule Parse(string cron, string timeZoneId); static CronFormat FormatOf(string cron); static string IanaId(TimeZoneInfo zone); TimeZoneInfo Zone; DateTime? After(DateTime utc); IEnumerable<DateTime> Between(DateTime fromUtc, DateTime toUtc); }` — `Parse` бросает `FormatException` (в т. ч. `CronFormatException`) и `TimeZoneNotFoundException`;
  - `internal sealed record RecurringDefinition(string Id, string Type, string ArgsJson, string Cron, string TimeZone, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap, string? Queue, short? Priority, int? MaxAttempts, int[]? RetryDelaysSeconds)` с `static void Validate(string id, string cron, RecurringJobOptions? options)`, `static RecurringDefinition Create(string id, string type, string argsJson, string cron, RecurringJobOptions? options)`, `CronSchedule Schedule()`, `const int MaxIdLength = 200`. Ошибки валидации — `ArgumentException`.

- [x] **Step 1: Зависимость Cronos**

`Directory.Packages.props` — в `ItemGroup` добавить `<PackageVersion Include="Cronos" Version="0.13.0" />`. `src/PgBolt.Postgres/PgBolt.Postgres.csproj` — в `ItemGroup` с `Npgsql` добавить `<PackageReference Include="Cronos" />`. Run: `dotnet build src/PgBolt.Postgres` — без предупреждений (AOT-анализ включён; Cronos без reflection).

- [x] **Step 2: Тесты**

`tests/PgBolt.Postgres.Tests/CronScheduleTests.cs`:

```csharp
using Cronos;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Tests;

public sealed class CronScheduleTests
{
    private static DateTime Utc(int y, int mo, int d, int h = 0, int mi = 0, int s = 0) => new(y, mo, d, h, mi, s, DateTimeKind.Utc);

    [Theory]
    [InlineData("0 3 * * *", CronFormat.Standard)]
    [InlineData("  */15 * * * *  ", CronFormat.Standard)]
    [InlineData("0 0 3 * * *", CronFormat.IncludeSeconds)]
    [InlineData("@daily", CronFormat.Standard)]
    [InlineData("@every_second", CronFormat.Standard)]
    public void Format_is_chosen_by_field_count_or_macro(string cron, CronFormat expected) =>
        Assert.Equal(expected, CronSchedule.FormatOf(cron));

    [Theory]
    [InlineData("* * * *")]
    [InlineData("0 0 0 * * * *")]
    public void Wrong_field_count_is_format_error(string cron) =>
        Assert.ThrowsAny<FormatException>(() => CronSchedule.Parse(cron, "UTC"));

    [Fact]
    public void Macros_parse_and_fire()
    {
        Assert.Equal(Utc(2026, 10, 1), CronSchedule.Parse("@daily", "UTC").After(Utc(2026, 9, 30, 12)));
        Assert.Equal(Utc(2026, 9, 30, 12, 0, 1), CronSchedule.Parse("@every_second", "UTC").After(Utc(2026, 9, 30, 12)));
    }

    [Fact]
    public void Unknown_time_zone_throws() =>
        Assert.Throws<TimeZoneNotFoundException>(() => CronSchedule.Parse("0 3 * * *", "Mars/Olympus"));

    [Fact]
    public void After_is_strict_and_between_includes_both_bounds()
    {
        var s = CronSchedule.Parse("* * * * *", "UTC");
        Assert.Equal(Utc(2026, 9, 30, 12, 1), s.After(Utc(2026, 9, 30, 12)));
        Assert.Equal([Utc(2026, 9, 30, 12), Utc(2026, 9, 30, 12, 1), Utc(2026, 9, 30, 12, 2)],
            s.Between(Utc(2026, 9, 30, 12), Utc(2026, 9, 30, 12, 2)).ToList());
    }

    [Fact]
    public void Time_zone_is_applied()
    {
        var s = CronSchedule.Parse("0 3 * * *", "Europe/Moscow");   // UTC+3
        Assert.Equal(Utc(2026, 10, 1, 0), s.After(Utc(2026, 9, 30, 12)));
    }

    [Fact]
    public void Dst_spring_forward_fires_at_transition()                // America/New_York, 2026-03-08 02:00 → 03:00
    {
        var s = CronSchedule.Parse("30 2 * * *", "America/New_York");
        Assert.Equal(Utc(2026, 3, 8, 7), s.After(Utc(2026, 3, 8, 5)));  // 03:00 EDT
    }

    [Fact]
    public void Dst_fall_back_fires_fixed_once_and_interval_twice()    // 2026-11-01 02:00 EDT → 01:00 EST
    {
        var from = Utc(2026, 11, 1, 4);
        var to = Utc(2026, 11, 1, 8);
        Assert.Single(CronSchedule.Parse("0 1 * * *", "America/New_York").Between(from, to));
        Assert.Equal(4, CronSchedule.Parse("*/30 1 * * *", "America/New_York").Between(from, to).Count());
    }

    [Fact]
    public void Iana_id_is_kept_and_windows_id_is_converted()
    {
        Assert.Equal("Europe/Moscow", CronSchedule.IanaId(TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow")));
        Assert.Equal("Europe/Moscow", CronSchedule.IanaId(TimeZoneInfo.FindSystemTimeZoneById("Russian Standard Time")));
    }
}
```

`tests/PgBolt.Postgres.Tests/RecurringDefinitionTests.cs`:

```csharp
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Tests;

public sealed class RecurringDefinitionTests
{
    private static RecurringDefinition Create(string cron = "0 3 * * *", RecurringJobOptions? options = null, string id = "daily") =>
        RecurringDefinition.Create(id, "rec.job", """{"N":1}""", cron, options);

    [Fact]
    public void Defaults_are_normalized()
    {
        var d = Create();
        Assert.Equal(("daily", "rec.job", """{"N":1}""", "0 3 * * *", "UTC"), (d.Id, d.Type, d.ArgsJson, d.Cron, d.TimeZone));
        Assert.Equal((MisfirePolicy.FireOnce, TimeSpan.FromDays(1), OverlapPolicy.Skip), (d.Misfire, d.CatchUpWindow, d.Overlap));
        Assert.Null(d.Queue);
        Assert.Null(d.Priority);
        Assert.Null(d.MaxAttempts);
        Assert.Null(d.RetryDelaysSeconds);
    }

    [Fact]
    public void Options_are_normalized_to_stored_form()
    {
        var d = Create(options: new RecurringJobOptions
        {
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Russian Standard Time"),
            CatchUpWindow = TimeSpan.FromDays(1) + TimeSpan.FromTicks(5),      // доли микросекунды усекаются (точность interval)
            RetryDelays = [TimeSpan.FromSeconds(1.5), TimeSpan.Zero],          // секунды вверх, как EnqueueOptionsResolver
            Queue = "q", Priority = 3, MaxAttempts = 2, Misfire = MisfirePolicy.CatchUp, Overlap = OverlapPolicy.Allow,
        });

        Assert.Equal("Europe/Moscow", d.TimeZone);
        Assert.Equal(TimeSpan.FromDays(1), d.CatchUpWindow);
        Assert.Equal([2, 0], d.RetryDelaysSeconds);
        Assert.Equal(("q", (short?)3, (int?)2), (d.Queue, d.Priority, d.MaxAttempts));
        Assert.Equal(d, RecurringDefinition.Create(d.Id, d.Type, d.ArgsJson, d.Cron, new RecurringJobOptions   // идемпотентно
        {
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow"), CatchUpWindow = d.CatchUpWindow,
            RetryDelays = [TimeSpan.FromSeconds(2), TimeSpan.Zero], Queue = "q", Priority = 3, MaxAttempts = 2,
            Misfire = MisfirePolicy.CatchUp, Overlap = OverlapPolicy.Allow,
        }));
    }

    [Theory]
    [InlineData("")]
    [InlineData("bad cron")]
    [InlineData("* * * *")]
    [InlineData("0 0 30 2 *")]          // нет будущих срабатываний
    public void Invalid_cron_is_rejected(string cron) =>
        Assert.Throws<ArgumentException>(() => Create(cron));

    [Fact]
    public void Id_length_is_checked()
    {
        Assert.Throws<ArgumentException>(() => Create(id: ""));
        Assert.Throws<ArgumentException>(() => Create(id: new string('x', 201)));
        Assert.Equal(200, Create(id: new string('x', 200)).Id.Length);
    }

    public static TheoryData<RecurringJobOptions> InvalidOptions() =>
    [
        new() { CatchUpWindow = TimeSpan.Zero },
        new() { CatchUpWindow = TimeSpan.FromTicks(5) },                 // после усечения — 0
        new() { CatchUpWindow = TimeSpan.FromDays(367) },
        new() { Queue = "" },
        new() { MaxAttempts = 0 },
        new() { RetryDelays = [TimeSpan.FromSeconds(-1)] },
        new() { Misfire = (MisfirePolicy)7 },
        new() { Overlap = (OverlapPolicy)7 },
    ];

    [Theory]
    [MemberData(nameof(InvalidOptions))]
    public void Invalid_options_are_rejected(RecurringJobOptions options) =>
        Assert.Throws<ArgumentException>(() => Create(options: options));

    [Fact]
    public void Validate_checks_everything_but_type_and_args()
    {
        RecurringDefinition.Validate("daily", "0 3 * * *", null);
        Assert.Throws<ArgumentException>(() => RecurringDefinition.Validate("daily", "bad", null));
    }
}
```

- [x] **Step 3: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~CronScheduleTests|FullyQualifiedName~RecurringDefinitionTests"`
Expected: FAIL — нет `CronSchedule`, `RecurringDefinition`.

- [x] **Step 4: `CronSchedule`**

`src/PgBolt.Postgres/Recurring/CronSchedule.cs`:

```csharp
using Cronos;

namespace PgBolt.Postgres.Recurring;

/// <summary>
/// Cron-выражение в таймзоне (часть 4 §2 «Cron», §5.4). Формат выбирается по строке одинаково в API, синхронизации и тике:
/// макрос @… — Standard (макросы разбираются одинаково в обоих форматах), иначе 5 полей — Standard, 6 — IncludeSeconds.
/// </summary>
internal sealed class CronSchedule
{
    private readonly CronExpression _expression;

    private CronSchedule(CronExpression expression, TimeZoneInfo zone)
    {
        _expression = expression;
        Zone = zone;
    }

    public TimeZoneInfo Zone { get; }

    /// <summary>FormatException (включая CronFormatException) — cron не разбирается; TimeZoneNotFoundException — нет таймзоны на поде.</summary>
    public static CronSchedule Parse(string cron, string timeZoneId)
    {
        ArgumentNullException.ThrowIfNull(cron);
        return new CronSchedule(CronExpression.Parse(cron.Trim(), FormatOf(cron)), TimeZoneInfo.FindSystemTimeZoneById(timeZoneId));
    }

    public static CronFormat FormatOf(string cron)
    {
        var trimmed = cron.Trim();
        if (trimmed.StartsWith('@'))
            return CronFormat.Standard;
        return trimmed.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length switch
        {
            5 => CronFormat.Standard,
            6 => CronFormat.IncludeSeconds,
            var n => throw new FormatException($"Cron expression '{cron}' has {n} fields; expected 5, 6 or a @macro."),
        };
    }

    /// <summary>IANA id таймзоны (хранимая форма, §4.2).</summary>
    public static string IanaId(TimeZoneInfo zone)
    {
        if (zone.HasIanaId)
            return zone.Id;
        return TimeZoneInfo.TryConvertWindowsIdToIanaId(zone.Id, out var iana)
            ? iana
            : throw new ArgumentException($"Time zone '{zone.Id}' has no IANA id.");
    }

    /// <summary>after(t) §5.3: первое срабатывание строго после t; null — срабатываний больше нет.</summary>
    public DateTime? After(DateTime utc) => _expression.GetNextOccurrence(AsUtc(utc), Zone, inclusive: false);

    /// <summary>Срабатывания в [fromUtc, toUtc]: обе границы включены (§5.4).</summary>
    public IEnumerable<DateTime> Between(DateTime fromUtc, DateTime toUtc) =>
        _expression.GetOccurrences(AsUtc(fromUtc), AsUtc(toUtc), Zone, fromInclusive: true, toInclusive: true);

    /// <summary>timestamptz из Npgsql приходит с Kind = Utc; Cronos требует его явно.</summary>
    private static DateTime AsUtc(DateTime value) =>
        value.Kind == DateTimeKind.Utc ? value : DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
```

- [x] **Step 5: `RecurringDefinition`**

`src/PgBolt.Postgres/Recurring/RecurringDefinition.cs`:

```csharp
namespace PgBolt.Postgres.Recurring;

/// <summary>Определение расписания в хранимой (нормализованной) форме — строка recurring_jobs без курсора (часть 4 §3, §4.2).</summary>
internal sealed record RecurringDefinition(
    string Id,
    string Type,
    string ArgsJson,
    string Cron,
    string TimeZone,
    MisfirePolicy Misfire,
    TimeSpan CatchUpWindow,
    OverlapPolicy Overlap,
    string? Queue,
    short? Priority,
    int? MaxAttempts,
    int[]? RetryDelaysSeconds)
{
    public const int MaxIdLength = 200;
    private static readonly TimeSpan MaxCatchUpWindow = TimeSpan.FromDays(366);
    private static readonly RecurringJobOptions Defaults = new();

    /// <summary>§4.2: всё, кроме типа задачи и аргументов (code-first проверяет их при первой синхронизации).</summary>
    public static void Validate(string id, string cron, RecurringJobOptions? options) => _ = Normalize(id, cron, options);

    public static RecurringDefinition Create(string id, string type, string argsJson, string cron, RecurringJobOptions? options)
    {
        var n = Normalize(id, cron, options);
        ArgumentException.ThrowIfNullOrEmpty(type);
        ArgumentNullException.ThrowIfNull(argsJson);
        return new RecurringDefinition(id, type, argsJson, cron, n.TimeZone, n.Misfire, n.CatchUpWindow, n.Overlap,
            n.Queue, n.Priority, n.MaxAttempts, n.RetryDelaysSeconds);
    }

    public CronSchedule Schedule() => CronSchedule.Parse(Cron, TimeZone);

    private static Normalized Normalize(string id, string cron, RecurringJobOptions? options)
    {
        options ??= Defaults;
        if (string.IsNullOrEmpty(id) || id.Length > MaxIdLength)
            throw new ArgumentException($"Recurring job id must be 1..{MaxIdLength} characters.", nameof(id));
        if (string.IsNullOrWhiteSpace(cron))
            throw new ArgumentException("Cron expression must not be empty.", nameof(cron));

        var timeZone = options.TimeZone is null ? "UTC" : CronSchedule.IanaId(options.TimeZone);
        CronSchedule schedule;
        try
        {
            schedule = CronSchedule.Parse(cron, timeZone);
        }
        catch (Exception e) when (e is FormatException or TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            throw new ArgumentException($"Invalid cron '{cron}' in time zone '{timeZone}': {e.Message}", nameof(cron), e);
        }
        if (schedule.After(DateTime.UtcNow) is null)
            throw new ArgumentException($"Cron '{cron}' has no future occurrences.", nameof(cron));

        var window = TimeSpan.FromTicks(options.CatchUpWindow.Ticks / TimeSpan.TicksPerMicrosecond * TimeSpan.TicksPerMicrosecond);
        if (window <= TimeSpan.Zero || window > MaxCatchUpWindow)
            throw new ArgumentException("CatchUpWindow must be in (0, 366 days].", nameof(options));
        if (!Enum.IsDefined(options.Misfire))
            throw new ArgumentException($"Unknown misfire policy {options.Misfire}.", nameof(options));
        if (!Enum.IsDefined(options.Overlap))
            throw new ArgumentException($"Unknown overlap policy {options.Overlap}.", nameof(options));
        if (options.Queue is { Length: 0 })
            throw new ArgumentException("Queue must not be empty.", nameof(options));
        if (options.MaxAttempts < 1)
            throw new ArgumentException("MaxAttempts must be >= 1.", nameof(options));

        var retry = options.RetryDelays?.Select(d => d < TimeSpan.Zero
            ? throw new ArgumentException("Retry delays must be non-negative.", nameof(options))
            : checked((int)Math.Ceiling(d.TotalSeconds))).ToArray();
        return new Normalized(timeZone, options.Misfire, window, options.Overlap, options.Queue, options.Priority, options.MaxAttempts, retry);
    }

    private readonly record struct Normalized(string TimeZone, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap,
        string? Queue, short? Priority, int? MaxAttempts, int[]? RetryDelaysSeconds);

    /// <summary>Равенство по значению, включая содержимое RetryDelaysSeconds (массив в record сравнивается по ссылке).</summary>
    public bool Equals(RecurringDefinition? other) =>
        other is not null && Id == other.Id && Type == other.Type && ArgsJson == other.ArgsJson && Cron == other.Cron
        && TimeZone == other.TimeZone && Misfire == other.Misfire && CatchUpWindow == other.CatchUpWindow && Overlap == other.Overlap
        && Queue == other.Queue && Priority == other.Priority && MaxAttempts == other.MaxAttempts
        && SameDelays(RetryDelaysSeconds, other.RetryDelaysSeconds);

    public override int GetHashCode() => HashCode.Combine(Id, Type, Cron, TimeZone);

    internal static bool SameDelays(int[]? a, int[]? b) =>
        (a is null) == (b is null) && (a ?? []).AsSpan().SequenceEqual(b ?? []);
}
```

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~CronScheduleTests|FullyQualifiedName~RecurringDefinitionTests"`
Expected: PASS. Если падают только тесты DST (`Dst_*`) — это расхождение с семантикой Cronos 0.13.0, а не ошибка кода: проверить поведение по README Cronos (раздел «Daylight Saving Time»), привести ожидания теста к документированной семантике и отразить её в XML-doc `CronSchedule`; код планировщика не менять.

- [x] **Step 7: Commit**

```bash
git add Directory.Packages.props src/PgBolt.Postgres tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(recurring): cron schedule and recurring definition validation"
```

---

### Task 5: `RecurringPlanner` — правила срабатывания

**Files:**
- Create: `src/PgBolt.Postgres/Recurring/RecurringPlanner.cs`
- Test: `tests/PgBolt.Postgres.Tests/RecurringPlannerTests.cs`

**Interfaces:**
- Consumes: `CronSchedule` (Task 4).
- Produces:
  - `internal readonly record struct PlanInput(CronSchedule Schedule, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap, DateTime? NextFireAt)`;
  - `internal readonly record struct MissedRange(DateTime FromUtc, DateTime ToUtc)`;
  - `internal sealed record FirePlan(IReadOnlyList<DateTime> Fires, DateTime? NextFireAt, DateTime? WakeAt, MissedRange? Missed)`;
  - `internal static class RecurringPlanner { const int MaxCatchUpFires = 100; static readonly TimeSpan OverlapRecheckDelay /*5 с*/, RowErrorDelay /*1 мин*/; static FirePlan Plan(PlanInput row, DateTime dbNow, TimeSpan misfireThreshold); static FirePlan OnConflict(PlanInput row, DateTime dbNow); static DateTime LastDue(CronSchedule schedule, DateTime next, DateTime dbNow); }`.

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/RecurringPlannerTests.cs`:

```csharp
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Tests;

public sealed class RecurringPlannerTests
{
    private static readonly DateTime Now = new(2026, 9, 30, 12, 0, 30, 500, DateTimeKind.Utc);
    private static readonly TimeSpan T = TimeSpan.FromMinutes(1);

    private static DateTime Utc(int y, int mo, int d, int h = 0, int mi = 0, int s = 0) => new(y, mo, d, h, mi, s, DateTimeKind.Utc);

    private static PlanInput Row(string cron, DateTime? next, MisfirePolicy misfire = MisfirePolicy.FireOnce,
        OverlapPolicy overlap = OverlapPolicy.Skip, TimeSpan? window = null) =>
        new(CronSchedule.Parse(cron, "UTC"), misfire, window ?? TimeSpan.FromDays(1), overlap, next);

    [Fact]
    public void Row_without_cursor_does_nothing()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", null), Now, T);
        Assert.Empty(plan.Fires);
        Assert.Null(plan.NextFireAt);
        Assert.Null(plan.WakeAt);
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Future_cursor_is_kept()
    {
        var next = Utc(2026, 9, 30, 12, 5);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next), Now, T);
        Assert.Empty(plan.Fires);
        Assert.Equal((next, next), (plan.NextFireAt, plan.WakeAt));
    }

    [Fact]
    public void Fire_once_fires_single_due_time()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 30, 12)), Now, T);
        Assert.Equal([Utc(2026, 9, 30, 12)], plan.Fires);
        Assert.Equal((Utc(2026, 9, 30, 12, 1), Utc(2026, 9, 30, 12, 1)), (plan.NextFireAt, plan.WakeAt));
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Fire_once_after_long_idle_fires_latest_due_time()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 27, 12)), Now, T);
        Assert.Equal([Utc(2026, 9, 30, 12)], plan.Fires);
        Assert.Equal(Utc(2026, 9, 30, 12, 1), plan.NextFireAt);
    }

    [Fact]
    public void Skip_fires_fresh_due_time()
    {
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 30, 11, 59), MisfirePolicy.Skip), Now, T);
        Assert.Equal([Utc(2026, 9, 30, 12)], plan.Fires);
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Skip_drops_stale_due_time_and_reports_it()
    {
        var plan = RecurringPlanner.Plan(Row("0 * * * *", Utc(2026, 9, 30, 10), MisfirePolicy.Skip), Now.AddMinutes(30), T);
        Assert.Empty(plan.Fires);
        Assert.Equal(new MissedRange(Utc(2026, 9, 30, 10), Utc(2026, 9, 30, 12)), plan.Missed);
        Assert.Equal(Utc(2026, 9, 30, 13), plan.NextFireAt);
    }

    [Fact]
    public void Catch_up_allow_fires_first_100_and_wakes_now()
    {
        var next = Utc(2026, 9, 30, 12).AddMinutes(-250);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next, MisfirePolicy.CatchUp, OverlapPolicy.Allow), Now, T);
        Assert.Equal(100, plan.Fires.Count);
        Assert.Equal(next, plan.Fires[0]);
        Assert.Equal(next.AddMinutes(99), plan.Fires[^1]);
        Assert.Equal(next.AddMinutes(100), plan.NextFireAt);
        Assert.Equal(Now, plan.WakeAt);                                 // догон встаёт в общую очередь (§5.3)
    }

    [Fact]
    public void Catch_up_starts_at_window_and_reports_older_fires()
    {
        var next = Utc(2026, 9, 30, 9);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next, MisfirePolicy.CatchUp, OverlapPolicy.Allow, TimeSpan.FromHours(1)), Now, T);
        var windowStart = Now - TimeSpan.FromHours(1);
        Assert.Equal(Utc(2026, 9, 30, 11, 1), plan.Fires[0]);
        Assert.Equal(new MissedRange(next, windowStart), plan.Missed);
    }

    [Fact]
    public void Catch_up_skip_fires_one_and_advances_by_one()
    {
        var next = Utc(2026, 9, 30, 11, 55);
        var plan = RecurringPlanner.Plan(Row("* * * * *", next, MisfirePolicy.CatchUp), Now, T);
        Assert.Equal([next], plan.Fires);
        Assert.Equal(next.AddMinutes(1), plan.NextFireAt);
        Assert.Equal(Now, plan.WakeAt);
    }

    [Fact]
    public void Catch_up_without_fires_in_window_moves_cursor_after_now()
    {
        var next = Utc(2025, 1, 1);
        foreach (var overlap in new[] { OverlapPolicy.Skip, OverlapPolicy.Allow })
        {
            var plan = RecurringPlanner.Plan(Row("0 0 1 1 *", next, MisfirePolicy.CatchUp, overlap), Now, T);
            Assert.Empty(plan.Fires);
            Assert.Equal(Utc(2027, 1, 1), plan.NextFireAt);
            Assert.Equal(next, plan.Missed!.Value.FromUtc);
        }
    }

    [Fact]
    public void Conflict_keeps_cursor_and_rechecks_in_5_seconds()
    {
        var row = Row("* * * * *", Utc(2026, 9, 30, 11, 55), MisfirePolicy.CatchUp);
        var plan = RecurringPlanner.OnConflict(row, Now);
        Assert.Empty(plan.Fires);
        Assert.Equal((row.NextFireAt, Now.AddSeconds(5)), (plan.NextFireAt, plan.WakeAt));
        Assert.Null(plan.Missed);
    }

    [Fact]
    public void Due_time_equal_to_now_is_included()
    {
        var now = Utc(2026, 9, 30, 12, 1);
        var plan = RecurringPlanner.Plan(Row("* * * * *", Utc(2026, 9, 30, 12)), now, T);
        Assert.Equal([now], plan.Fires);
        Assert.Equal(Utc(2026, 9, 30, 12, 2), plan.NextFireAt);
    }

    [Fact]
    public void Last_due_after_100_years_of_every_second()
    {
        var s = CronSchedule.Parse("* * * * * *", "UTC");
        Assert.Equal(Utc(2026, 9, 30, 12, 0, 30), RecurringPlanner.LastDue(s, Utc(1926, 9, 30), Now));
    }

    [Fact]
    public void Last_due_of_burst_cron_is_exact()                     // каждую секунду в январе; простой 10.01–01.03
    {
        var s = CronSchedule.Parse("* * * * 1 *", "UTC");
        Assert.Equal(Utc(2026, 1, 31, 23, 59, 59), RecurringPlanner.LastDue(s, Utc(2026, 1, 10), Utc(2026, 3, 1)));
        var plan = RecurringPlanner.Plan(new PlanInput(s, MisfirePolicy.FireOnce, TimeSpan.FromDays(1), OverlapPolicy.Skip, Utc(2026, 1, 10)),
            Utc(2026, 3, 1), T);
        Assert.Equal((Utc(2026, 1, 31, 23, 59, 59), Utc(2027, 1, 1)), (plan.Fires.Single(), plan.NextFireAt!.Value));
    }
}
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringPlannerTests"`
Expected: FAIL — нет `RecurringPlanner`.

- [x] **Step 3: Реализация**

`src/PgBolt.Postgres/Recurring/RecurringPlanner.cs`:

```csharp
namespace PgBolt.Postgres.Recurring;

internal readonly record struct PlanInput(CronSchedule Schedule, MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap,
    DateTime? NextFireAt);

/// <summary>Срабатывания, отброшенные misfire-политикой (Warning §7): CatchUp — [FromUtc, ToUtc), ToUtc — начало окна; Skip — [FromUtc, ToUtc], ToUtc — последнее отброшенное.</summary>
internal readonly record struct MissedRange(DateTime FromUtc, DateTime ToUtc);

/// <summary>Результат планирования строки: что ставить и новые next_fire_at / wake_at.</summary>
internal sealed record FirePlan(IReadOnlyList<DateTime> Fires, DateTime? NextFireAt, DateTime? WakeAt, MissedRange? Missed);

/// <summary>
/// Правила срабатывания (часть 4 §5.3). Чистая функция: все расчёты от db_now; due = срабатывания в [next_fire_at, db_now].
/// </summary>
internal static class RecurringPlanner
{
    public const int MaxCatchUpFires = 100;
    public static readonly TimeSpan OverlapRecheckDelay = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan RowErrorDelay = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan SearchResolution = TimeSpan.FromSeconds(1);

    public static FirePlan Plan(PlanInput row, DateTime dbNow, TimeSpan misfireThreshold)
    {
        if (row.NextFireAt is not { } next)
            return new FirePlan([], null, null, null);
        if (next > dbNow)
            return new FirePlan([], next, next, null);

        var s = row.Schedule;
        switch (row.Misfire)
        {
            case MisfirePolicy.FireOnce:
                return Moved([LastDue(s, next, dbNow)], s.After(dbNow), dbNow, null);

            case MisfirePolicy.Skip:
            {
                var last = LastDue(s, next, dbNow);
                return last >= dbNow - misfireThreshold
                    ? Moved([last], s.After(dbNow), dbNow, null)
                    : Moved([], s.After(dbNow), dbNow, new MissedRange(next, last));
            }

            case MisfirePolicy.CatchUp:
            {
                var windowStart = dbNow - row.CatchUpWindow;
                var missed = next < windowStart ? new MissedRange(next, windowStart) : (MissedRange?)null;
                var take = row.Overlap == OverlapPolicy.Allow ? MaxCatchUpFires : 1;
                var fires = s.Between(next < windowStart ? windowStart : next, dbNow).Take(take).ToList();
                return fires.Count == 0
                    ? Moved([], s.After(dbNow), dbNow, missed)
                    : Moved(fires, s.After(fires[^1]), dbNow, missed);
            }

            default:
                throw new InvalidOperationException($"Unknown misfire policy {row.Misfire}.");
        }
    }

    /// <summary>CatchUp + Skip, конфликт по ключу: курсор стоит, перепроверка через 5 с; пропуск не логируется (§5.3).</summary>
    public static FirePlan OnConflict(PlanInput row, DateTime dbNow) =>
        new([], row.NextFireAt, dbNow + OverlapRecheckDelay, null);

    /// <summary>
    /// Последнее срабатывание в [next, dbNow] (next ≤ dbNow — срабатывание курсора). Поиск опирается только на After
    /// (GetPreviousOccurrence Cronos в CronSchedule не выставлен, его поведение на DST не проверено), поэтому бинарный поиск
    /// по монотонному предикату after(t) ≤ dbNow: истинен до последнего due, ложен начиная с него. Срабатывания — на
    /// целых секундах, поэтому при hi − lo ≤ 1 с в (lo, hi] не больше одного, и ответ — after(lo). ≤ ~42 вызова при любом cron.
    /// </summary>
    public static DateTime LastDue(CronSchedule schedule, DateTime next, DateTime dbNow)
    {
        bool Due(DateTime t) => schedule.After(t) is { } a && a <= dbNow;

        if (!Due(next))
            return next;
        var lo = next;
        var hi = dbNow;
        while (hi - lo > SearchResolution)
        {
            var mid = lo + TimeSpan.FromTicks((hi - lo).Ticks / 2);
            if (Due(mid))
                lo = mid;
            else
                hi = mid;
        }
        return schedule.After(lo)!.Value;
    }

    private static FirePlan Moved(IReadOnlyList<DateTime> fires, DateTime? next, DateTime dbNow, MissedRange? missed) =>
        new(fires, next, next is { } n ? (n > dbNow ? n : dbNow) : null, missed);
}
```

- [x] **Step 4: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringPlannerTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Recurring tests/PgBolt.Postgres.Tests/RecurringPlannerTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(recurring): misfire and catch-up planner"
```

---
### Task 6: CRUD расписаний в хранилище и `RecurringJobManager`

**Files:**
- Create: `src/PgBolt.Postgres/Storage/RecurringRecords.cs`, `src/PgBolt.Postgres/Storage/PgBoltStore.Recurring.cs`, `src/PgBolt.Postgres/Client/RecurringJobManager.cs`
- Modify: `src/PgBolt.Postgres/Storage/P.cs` (метод `AddValue`), `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs` (регистрация менеджера)
- Test: `tests/PgBolt.Postgres.Tests/RecurringTestSupport.cs`, `tests/PgBolt.Postgres.Tests/RecurringApiTests.cs`

**Interfaces:**
- Consumes: `RecurringDefinition`, `CronSchedule` (Task 4), `ReservedUniqueKeys`, `JobMetadataAccess`, `EnqueueBatch.RecurringIds/ScheduledAtUtc` (Task 3), `IRecurringJobManager` (Task 2).
- Produces (всё `internal`):
  - `enum RecurringUpsertResult { Inserted, Updated, OwnedByCodeFirst }`, `enum RecurringRemoveResult { NotFound, Removed, OwnedByCodeFirst }`, `record RecurringTriggerRow(string Type, string ArgsJson, string? Queue, short? Priority, int? MaxAttempts, int[]? RetryDelaysSeconds, OverlapPolicy Overlap, bool UnknownType)`;
  - `PgBoltStore`: `Task<RecurringUpsertResult> UpsertRecurringAsync(RecurringDefinition d, CancellationToken ct)`, `Task<RecurringRemoveResult> RemoveRecurringAsync(string id, CancellationToken ct)`, `Task<bool> PauseRecurringAsync(string id, CancellationToken ct)`, `Task<bool> ResumeRecurringAsync(string id, CancellationToken ct)`, `Task<RecurringJobInfo?> GetRecurringAsync(string id, CancellationToken ct)`, `Task<RecurringTriggerRow?> GetRecurringForTriggerAsync(string id, CancellationToken ct)`;
  - общие для синхронизации (Task 8): `NpgsqlBatchCommand InsertRecurringCommand(RecurringDefinition d, string? owner, DateTime? cursor)` (результат — строка `id`, если вставлено), `NpgsqlBatchCommand UpdateRecurringCommand(RecurringDefinition d, string? owner, bool recalc, DateTime? cursor)`, `static DateTime Later(DateTime a, DateTime? b)`;
  - `P.AddValue<T>(NpgsqlParameterCollection ps, string name, T? value, NpgsqlDbType type) where T : struct`;
  - `sealed class RecurringJobManager(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup) : IRecurringJobManager`, зарегистрирован в `AddPgBolt` как `IRecurringJobManager`.

- [x] **Step 1: Тестовая поддержка и тесты**

`tests/PgBolt.Postgres.Tests/RecurringTestSupport.cs`:

```csharp
using System.Text.Json.Serialization;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Tests;

public sealed record RecArgs(int N);

[JsonSerializable(typeof(RecArgs))]
internal sealed partial class RecJson : JsonSerializerContext;

internal static class RecurringTestSupport
{
    /// <summary>Тип задачи расписаний в тестах: очередь rq, 3 попытки, приоритет 1.</summary>
    public static readonly JobDescriptor Descriptor = new("rec.job", "rq", 3, 1, null);

    public static void EnsureRegistered() => JobMetadataRegistry.Register(Descriptor, RecJson.Default.RecArgs);

    public static Task PublishAsync(PgBoltStore store, CancellationToken ct) =>
        store.PublishJobTypesAsync([new JobTypeRecord("rec.job", "{}", "rq", 3, 1, null)], "test", ct);

    public static RecurringJobManager Manager(PgBoltStore store, IWakeupSignal wakeup) =>
        new(store, new JobTypesCache(store, TimeSpan.FromSeconds(30), TimeProvider.System), wakeup);
}
```

`tests/PgBolt.Postgres.Tests/RecurringApiTests.cs`:

```csharp
using System.Text.Json;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RecurringApiTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private readonly RecordingWakeup _wakeup = new();
    private IRecurringJobManager _manager = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        RecurringTestSupport.EnsureRegistered();
        await RecurringTestSupport.PublishAsync(Store, Ct);
        _manager = RecurringTestSupport.Manager(Store, _wakeup);
    }

    private Task<bool> RowAsync(string condition, string id = "a") =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.recurring_jobs WHERE id = @id", ("id", id));

    [Fact]
    public async Task Add_creates_api_row_with_cursor_after_now()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);

        var info = await _manager.GetAsync("a", Ct);
        Assert.NotNull(info);
        Assert.Equal(("rec.job", "0 3 * * *", "UTC", (string?)null, false), (info.JobName, info.Cron, info.TimeZone, info.Owner, info.Paused));
        Assert.Equal(1, info.Args.GetProperty("N").GetInt32());
        Assert.True(info.NextFireAt > DateTimeOffset.UtcNow);
        Assert.True(await RowAsync("wake_at = next_fire_at AND (next_fire_at AT TIME ZONE 'UTC')::time = '03:00' AND last_fire_at IS NULL"));
    }

    [Fact]
    public async Task Update_keeps_cursor_unless_cron_changes()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = '2099-01-01', wake_at = '2099-01-01' WHERE id = 'a'");

        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(2), new RecurringJobOptions { Queue = "other" }, Ct);
        Assert.True(await RowAsync("""next_fire_at = '2099-01-01' AND args = '{"N":2}' AND queue = 'other'"""));

        await _manager.AddOrUpdateAsync("a", "0 4 * * *", new RecArgs(2), ct: Ct);
        Assert.True(await RowAsync(
            "next_fire_at < '2099-01-01' AND wake_at = next_fire_at AND (next_fire_at AT TIME ZONE 'UTC')::time = '04:00' AND queue IS NULL"));
    }

    [Fact]
    public async Task Cron_change_after_tick_does_not_repeat_fired_time()     // Review Focus 3
    {
        await _manager.AddOrUpdateAsync("a", "* * * * *", new RecArgs(1), ct: Ct);
        // тик уже поставил срабатывание позже момента, от которого API считает курсор (db_now до ожидания блокировки)
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET last_fire_at = date_trunc('minute', now()) + interval '2 minutes' WHERE id = 'a'");

        await _manager.AddOrUpdateAsync("a", "*/2 * * * *", new RecArgs(1), ct: Ct);

        Assert.True(await RowAsync("next_fire_at > last_fire_at"));
    }

    [Fact]
    public async Task Paused_survives_update()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        Assert.True(await _manager.PauseAsync("a", Ct));

        await _manager.AddOrUpdateAsync("a", "0 5 * * *", new RecArgs(1), ct: Ct);

        Assert.True((await _manager.GetAsync("a", Ct))!.Paused);
    }

    [Fact]
    public async Task Code_first_row_allows_only_pause_resume_trigger_get()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET owner = 'svc' WHERE id = 'a'");

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await _manager.AddOrUpdateAsync("a", "0 4 * * *", new RecArgs(2), ct: Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await _manager.RemoveAsync("a", Ct));
        Assert.True(await _manager.PauseAsync("a", Ct));
        Assert.True(await _manager.ResumeAsync("a", Ct));
        Assert.NotNull(await _manager.TriggerAsync("a", Ct));
        Assert.Equal("svc", (await _manager.GetAsync("a", Ct))!.Owner);
        Assert.True(await RowAsync("""cron = '0 3 * * *' AND args = '{"N":1}'"""));
    }

    [Fact]
    public async Task Missing_rows_and_remove()
    {
        Assert.False(await _manager.RemoveAsync("a", Ct));
        Assert.False(await _manager.PauseAsync("a", Ct));
        Assert.False(await _manager.ResumeAsync("a", Ct));
        Assert.Null(await _manager.TriggerAsync("a", Ct));
        Assert.Null(await _manager.GetAsync("a", Ct));

        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);

        Assert.True(await _manager.RemoveAsync("a", Ct));
        Assert.Null(await _manager.GetAsync("a", Ct));
    }

    [Fact]
    public async Task Resume_wakes_now_and_keeps_null_cursor()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        await _manager.PauseAsync("a", Ct);

        await _manager.ResumeAsync("a", Ct);
        Assert.True(await RowAsync("NOT paused AND wake_at <= now() AND next_fire_at > now()"));

        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = NULL, wake_at = NULL, paused = true WHERE id = 'a'");
        await _manager.ResumeAsync("a", Ct);
        Assert.True(await RowAsync("wake_at IS NULL"));
    }

    [Fact]
    public async Task Trigger_enqueues_recurring_job_and_respects_overlap()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);

        var first = (await _manager.TriggerAsync("a", Ct))!.Value;
        var second = (await _manager.TriggerAsync("a", Ct))!.Value;

        Assert.True(first.Created);
        Assert.Equal(new EnqueueResult(first.Id, false), second);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT j.recurring_id = 'a' AND j.scheduled_at = j.created_at AND j.queue = 'rq' AND j.priority = 1
                   AND j.unique_key = 'pgbolt:recurring:a' AND j.args = '{"N":1}'
            FROM {schema}.jobs j WHERE j.id = @id
            """, ("id", first.Id!.Value.Value)));
        Assert.Equal(new[] { "rq" }, _wakeup.Signals);
        Assert.True(await RowAsync("last_fire_at IS NULL AND last_job_id IS NULL"));   // Trigger строку не меняет (§5.5)
    }

    [Fact]
    public async Task Trigger_with_overlap_allow_enqueues_every_time()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), new RecurringJobOptions { Overlap = OverlapPolicy.Allow }, Ct);

        Assert.True((await _manager.TriggerAsync("a", Ct))!.Value.Created);
        Assert.True((await _manager.TriggerAsync("a", Ct))!.Value.Created);

        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a' AND unique_key IS NULL"));
    }

    [Fact]
    public async Task Requeue_of_failed_run_conflicts_while_new_run_is_alive()   // core §5.9: ключ восстанавливается из jobs
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);
        var failed = (await _manager.TriggerAsync("a", Ct))!.Value.Id!.Value.Value;
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = 3, finished_at = now() WHERE id = @id",
            ("id", failed));
        var live = (await _manager.TriggerAsync("a", Ct))!.Value.Id!.Value.Value;

        Assert.Equal(RequeueResult.Conflict, (await Store.RequeueAsync(failed, Ct)).Result);

        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = 2, finished_at = now() WHERE id = @id",
            ("id", live));
        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(failed, Ct)).Result);
    }

    [Fact]
    public async Task Unknown_job_type_is_rejected_by_add_and_trigger()
    {
        using var args = JsonDocument.Parse("""{"N":1}""");
        await Assert.ThrowsAsync<UnknownJobException>(async () =>
            await _manager.AddOrUpdateAsync("a", "nope", "0 3 * * *", args.RootElement, ct: Ct));

        await _manager.AddOrUpdateAsync("b", "rec.job", "0 3 * * *", args.RootElement, ct: Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET type = 'nope' WHERE id = 'b'");

        await Assert.ThrowsAsync<UnknownJobException>(async () => await _manager.TriggerAsync("b", Ct));
    }

    [Fact]
    public async Task Invalid_definition_is_rejected_before_database()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await _manager.AddOrUpdateAsync("a", "bad", new RecArgs(1), ct: Ct));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs"));
    }

    [Fact]
    public async Task Parallel_add_of_new_id_creates_one_row()
    {
        await Task.WhenAll(Enumerable.Range(0, 8).Select(i => _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(i), ct: Ct).AsTask()));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs"));
    }

    [Fact]
    public async Task Get_returns_normalized_definition()
    {
        await _manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), new RecurringJobOptions
        {
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow"), Misfire = MisfirePolicy.CatchUp,
            CatchUpWindow = TimeSpan.FromHours(6), RetryDelays = [TimeSpan.FromSeconds(1.5)], MaxAttempts = 2, Priority = 4,
        }, Ct);

        var info = (await _manager.GetAsync("a", Ct))!;

        Assert.Equal(("Europe/Moscow", MisfirePolicy.CatchUp, TimeSpan.FromHours(6), OverlapPolicy.Skip),
            (info.TimeZone, info.Misfire, info.CatchUpWindow, info.Overlap));
        Assert.Equal([TimeSpan.FromSeconds(2)], info.RetryDelays!);
        Assert.Equal(((short?)4, (int?)2, (string?)null), (info.Priority, info.MaxAttempts, info.Queue));
    }
}

public sealed class RecurringApiTests_Direct(PostgresFixture pg) : RecurringApiTests(pg, ConnectionMode.Direct);
public sealed class RecurringApiTests_Bouncer(PostgresFixture pg) : RecurringApiTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringApiTests"`
Expected: FAIL — нет `RecurringJobManager`.

- [x] **Step 3: `P.AddValue` и записи**

В `src/PgBolt.Postgres/Storage/P.cs` добавить:

```csharp
    public static void AddValue<T>(NpgsqlParameterCollection ps, string name, T? value, NpgsqlDbType type) where T : struct =>
        ps.Add(value is { } v
            ? new NpgsqlParameter<T>(name, type) { TypedValue = v }
            : new NpgsqlParameter(name, type) { Value = DBNull.Value });
```

`src/PgBolt.Postgres/Storage/RecurringRecords.cs`:

```csharp
namespace PgBolt.Postgres.Storage;

internal enum RecurringUpsertResult { Inserted, Updated, OwnedByCodeFirst }

internal enum RecurringRemoveResult { NotFound, Removed, OwnedByCodeFirst }

/// <summary>Строка расписания с умолчаниями job_types для Trigger (часть 4 §4.2); UnknownType — типа нет в job_types.</summary>
internal sealed record RecurringTriggerRow(
    string Type,
    string ArgsJson,
    string? Queue,
    short? Priority,
    int? MaxAttempts,
    int[]? RetryDelaysSeconds,
    OverlapPolicy Overlap,
    bool UnknownType);
```

- [x] **Step 4: SQL CRUD**

`src/PgBolt.Postgres/Storage/PgBoltStore.Recurring.cs`:

```csharp
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Storage;

/// <summary>Расписания: runtime API (часть 4 §4.2) и общие команды вставки/изменения для синхронизации (§6.2).</summary>
public sealed partial class PgBoltStore
{
    private const string InsertRecurringTemplate = """
        INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
                                             queue, priority, max_attempts, retry_delays, owner, next_fire_at, wake_at,
                                             created_at, updated_at)
        VALUES (@id, @type, @args, @cron, @time_zone, @misfire, @catch_up_window, @overlap,
                @queue, @priority, @max_attempts, @retry_delays, @owner, @cursor, @cursor, now(), now())
        ON CONFLICT (id) DO NOTHING
        RETURNING id
        """;

    private const string UpdateRecurringTemplate = """
        UPDATE {schema}.recurring_jobs
        SET type = @type, args = @args, cron = @cron, time_zone = @time_zone, misfire = @misfire,
            catch_up_window = @catch_up_window, overlap = @overlap, queue = @queue, priority = @priority,
            max_attempts = @max_attempts, retry_delays = @retry_delays, updated_at = now(),
            next_fire_at = CASE WHEN @recalc THEN @cursor ELSE next_fire_at END,
            wake_at      = CASE WHEN @recalc THEN @cursor ELSE wake_at END
        WHERE id = @id AND owner IS NOT DISTINCT FROM @owner
        """;

    private const string LockRecurringTemplate = """
        SELECT owner, cron, time_zone, last_fire_at FROM {schema}.recurring_jobs WHERE id = @id FOR UPDATE
        """;

    private const string RemoveRecurringTemplate = """
        WITH d AS (DELETE FROM {schema}.recurring_jobs WHERE id = @id AND owner IS NULL RETURNING 1)
        SELECT EXISTS (SELECT 1 FROM d), (SELECT owner FROM {schema}.recurring_jobs WHERE id = @id)
        """;

    private const string PauseRecurringTemplate = """
        UPDATE {schema}.recurring_jobs SET paused = true WHERE id = @id
        """;

    private const string ResumeRecurringTemplate = """
        UPDATE {schema}.recurring_jobs
        SET paused = false, wake_at = CASE WHEN wake_at IS NULL THEN NULL ELSE LEAST(wake_at, now()) END
        WHERE id = @id
        """;

    private const string GetRecurringTemplate = """
        SELECT id, type, args, cron, time_zone, misfire, catch_up_window, overlap, queue, priority, max_attempts, retry_delays,
               owner, paused, next_fire_at, last_fire_at, last_job_id, last_error, created_at, updated_at
        FROM {schema}.recurring_jobs WHERE id = @id
        """;

    private const string GetRecurringForTriggerTemplate = """
        SELECT r.type, r.args,
               COALESCE(r.queue,        t.default_queue),
               COALESCE(r.priority,     t.default_priority),
               COALESCE(r.max_attempts, t.default_max_attempts),
               COALESCE(r.retry_delays, t.default_retry_delays),
               r.overlap, t.name IS NULL
        FROM {schema}.recurring_jobs r
        LEFT JOIN {schema}.job_types t ON t.name = r.type
        WHERE r.id = @id
        """;

    private string InsertRecurringSql => field ??= Render(InsertRecurringTemplate);
    private string UpdateRecurringSql => field ??= Render(UpdateRecurringTemplate);
    private string LockRecurringSql => field ??= Render(LockRecurringTemplate);
    private string RemoveRecurringSql => field ??= Render(RemoveRecurringTemplate);
    private string PauseRecurringSql => field ??= Render(PauseRecurringTemplate);
    private string ResumeRecurringSql => field ??= Render(ResumeRecurringTemplate);
    private string GetRecurringSql => field ??= Render(GetRecurringTemplate);
    private string GetRecurringForTriggerSql => field ??= Render(GetRecurringForTriggerTemplate);

    /// <summary>
    /// §4.2 AddOrUpdate: транзакция в два round-trip'а. Курсор — after(max(db_now, last_fire_at)) по строке под FOR UPDATE:
    /// срабатывание, уже поставленное тиком по старому cron, не повторится. Пустой RETURNING вставки — строку создали
    /// параллельно: повтор с начала видит её под блокировкой.
    /// </summary>
    internal Task<RecurringUpsertResult> UpsertRecurringAsync(RecurringDefinition d, CancellationToken ct)
    {
        var schedule = d.Schedule();
        return RunAsync(async c =>
        {
            while (true)
            {
                await using var conn = await _dataSource.OpenConnectionAsync(c);
                await using var tx = await conn.BeginTransactionAsync(c);
                var current = await LockRecurringAsync(conn, tx, d.Id, c);
                if (current.Exists && current.Owner is not null)
                {
                    await tx.RollbackAsync(c);
                    return RecurringUpsertResult.OwnedByCodeFirst;
                }

                var cursor = schedule.After(Later(current.DbNow, current.LastFireAt));
                if (!current.Exists)
                {
                    await using var insert = new NpgsqlBatch(conn, tx) { BatchCommands = { InsertRecurringCommand(d, owner: null, cursor) } };
                    if (await insert.ExecuteScalarAsync(c) is null)
                    {
                        await tx.RollbackAsync(c);
                        continue;
                    }
                    await tx.CommitAsync(c);
                    return RecurringUpsertResult.Inserted;
                }

                var recalc = current.Cron != d.Cron || current.TimeZone != d.TimeZone;
                await using (var update = new NpgsqlBatch(conn, tx) { BatchCommands = { UpdateRecurringCommand(d, owner: null, recalc, cursor) } })
                    await update.ExecuteNonQueryAsync(c);
                await tx.CommitAsync(c);
                return RecurringUpsertResult.Updated;
            }
        }, ct);
    }

    internal Task<RecurringRemoveResult> RemoveRecurringAsync(string id, CancellationToken ct) =>
        QueryAsync(RemoveRecurringSql, ps => P.Add(ps, "id", id), async (r, c) =>
        {
            await r.ReadAsync(c);
            if (r.GetBoolean(0))
                return RecurringRemoveResult.Removed;
            return r.IsDBNull(1) ? RecurringRemoveResult.NotFound : RecurringRemoveResult.OwnedByCodeFirst;
        }, ct);

    internal async Task<bool> PauseRecurringAsync(string id, CancellationToken ct) =>
        await NonQueryAsync(PauseRecurringSql, ps => P.Add(ps, "id", id), ct) == 1;

    /// <summary>§4.2: wake_at = LEAST(wake_at, now()), кроме NULL (LEAST игнорирует NULL и оживил бы строку без курсора).</summary>
    internal async Task<bool> ResumeRecurringAsync(string id, CancellationToken ct) =>
        await NonQueryAsync(ResumeRecurringSql, ps => P.Add(ps, "id", id), ct) == 1;

    internal Task<RecurringJobInfo?> GetRecurringAsync(string id, CancellationToken ct) =>
        QueryAsync<RecurringJobInfo?>(GetRecurringSql, ps => P.Add(ps, "id", id), async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return null;
            using var args = JsonDocument.Parse(r.GetString(2));
            return new RecurringJobInfo(
                r.GetString(0), r.GetString(1), args.RootElement.Clone(), r.GetString(3), r.GetString(4),
                (MisfirePolicy)r.GetInt16(5), r.GetFieldValue<TimeSpan>(6), (OverlapPolicy)r.GetInt16(7),
                r.GetNullable<string>(8), r.GetNullableValue<short>(9), r.GetNullableValue<int>(10),
                r.GetNullable<int[]>(11)?.Select(s => TimeSpan.FromSeconds(s)).ToArray(),
                r.GetNullable<string>(12), r.GetBoolean(13),
                r.GetNullableValue<DateTimeOffset>(14), r.GetNullableValue<DateTimeOffset>(15),
                r.GetNullableValue<Guid>(16) is { } job ? new JobId(job) : null, r.GetNullable<string>(17),
                r.GetFieldValue<DateTimeOffset>(18), r.GetFieldValue<DateTimeOffset>(19));
        }, ct);

    internal Task<RecurringTriggerRow?> GetRecurringForTriggerAsync(string id, CancellationToken ct) =>
        QueryAsync<RecurringTriggerRow?>(GetRecurringForTriggerSql, ps => P.Add(ps, "id", id), async (r, c) =>
            await r.ReadAsync(c)
                ? new RecurringTriggerRow(r.GetString(0), r.GetString(1), r.GetNullable<string>(2), r.GetNullableValue<short>(3),
                    r.GetNullableValue<int>(4), r.GetNullable<int[]>(5), (OverlapPolicy)r.GetInt16(6), r.GetBoolean(7))
                : null, ct);

    /// <summary>INSERT … ON CONFLICT (id) DO NOTHING RETURNING id: строка результата — вставлено.</summary>
    internal NpgsqlBatchCommand InsertRecurringCommand(RecurringDefinition d, string? owner, DateTime? cursor)
    {
        var cmd = new NpgsqlBatchCommand(InsertRecurringSql);
        BindDefinition(cmd.Parameters, d, owner, cursor);
        return cmd;
    }

    /// <summary>UPDATE полей определения строки этого владельца; recalc — курсор и wake_at = cursor (смена cron / таймзоны).</summary>
    internal NpgsqlBatchCommand UpdateRecurringCommand(RecurringDefinition d, string? owner, bool recalc, DateTime? cursor)
    {
        var cmd = new NpgsqlBatchCommand(UpdateRecurringSql);
        BindDefinition(cmd.Parameters, d, owner, cursor);
        P.Add(cmd.Parameters, "recalc", recalc);
        return cmd;
    }

    internal static DateTime Later(DateTime a, DateTime? b) => b is { } v && v > a ? v : a;

    private static void BindDefinition(NpgsqlParameterCollection ps, RecurringDefinition d, string? owner, DateTime? cursor)
    {
        P.Add(ps, "id", d.Id);
        P.Add(ps, "type", d.Type);
        P.Add(ps, "args", d.ArgsJson, NpgsqlDbType.Jsonb);
        P.Add(ps, "cron", d.Cron);
        P.Add(ps, "time_zone", d.TimeZone);
        P.Add(ps, "misfire", (short)d.Misfire);
        P.Add(ps, "catch_up_window", d.CatchUpWindow, NpgsqlDbType.Interval);
        P.Add(ps, "overlap", (short)d.Overlap);
        P.AddNullable(ps, "queue", d.Queue, NpgsqlDbType.Text);
        P.AddValue(ps, "priority", d.Priority, NpgsqlDbType.Smallint);
        P.AddValue(ps, "max_attempts", d.MaxAttempts, NpgsqlDbType.Integer);
        P.AddNullable(ps, "retry_delays", d.RetryDelaysSeconds, NpgsqlDbType.Array | NpgsqlDbType.Integer);
        P.AddNullable(ps, "owner", owner, NpgsqlDbType.Text);
        P.AddValue(ps, "cursor", cursor, NpgsqlDbType.TimestampTz);
    }

    private readonly record struct LockedRecurring(DateTime DbNow, bool Exists, string? Owner, string? Cron, string? TimeZone, DateTime? LastFireAt);

    private async Task<LockedRecurring> LockRecurringAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string id, CancellationToken ct)
    {
        await using var batch = new NpgsqlBatch(conn, tx)
        {
            BatchCommands = { new NpgsqlBatchCommand("SELECT now()"), new NpgsqlBatchCommand(LockRecurringSql) },
        };
        P.Add(batch.BatchCommands[1].Parameters, "id", id);
        await using var r = await batch.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        var dbNow = r.GetDateTime(0);
        await r.NextResultAsync(ct);
        return await r.ReadAsync(ct)
            ? new LockedRecurring(dbNow, true, r.GetNullable<string>(0), r.GetString(1), r.GetString(2), r.GetNullableValue<DateTime>(3))
            : new LockedRecurring(dbNow, false, null, null, null, null);
    }
}
```

- [x] **Step 5: `RecurringJobManager` и регистрация**

`src/PgBolt.Postgres/Client/RecurringJobManager.cs`:

```csharp
using System.Text.Json;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client;

/// <summary>Runtime API расписаний (часть 4 §4.2). Валидация — до обращения к БД.</summary>
internal sealed class RecurringJobManager(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup) : IRecurringJobManager
{
    public async ValueTask AddOrUpdateAsync<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null,
        CancellationToken ct = default)
    {
        RecurringDefinition.Validate(id, cron, options);
        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        await UpsertAsync(RecurringDefinition.Create(id, descriptor.Name, JsonSerializer.Serialize(args, typeInfo), cron, options), ct);
    }

    public async ValueTask AddOrUpdateAsync(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null,
        CancellationToken ct = default)
    {
        var definition = RecurringDefinition.Create(id, jobName, args.GetRawText(), cron, options);
        if (await jobTypes.GetAsync(jobName, ct) is null)
            throw new UnknownJobException(jobName);
        await UpsertAsync(definition, ct);
    }

    public async ValueTask<bool> RemoveAsync(string id, CancellationToken ct = default) =>
        await store.RemoveRecurringAsync(id, ct) switch
        {
            RecurringRemoveResult.Removed => true,
            RecurringRemoveResult.NotFound => false,
            _ => throw OwnedByCodeFirst(id),
        };

    public async ValueTask<bool> PauseAsync(string id, CancellationToken ct = default) => await store.PauseRecurringAsync(id, ct);

    public async ValueTask<bool> ResumeAsync(string id, CancellationToken ct = default) => await store.ResumeRecurringAsync(id, ct);

    public async ValueTask<RecurringJobInfo?> GetAsync(string id, CancellationToken ct = default) => await store.GetRecurringAsync(id, ct);

    /// <summary>
    /// §4.2: обычный путь клиента (включая разрешение конфликта) со scheduled_at = now() БД; строку расписания не меняет
    /// (§5.5: иначе порядок блокировок был бы обратен тику). paused не учитывается.
    /// </summary>
    public async ValueTask<EnqueueResult?> TriggerAsync(string id, CancellationToken ct = default)
    {
        if (await store.GetRecurringForTriggerAsync(id, ct) is not { } row)
            return null;
        if (row.UnknownType)
            throw new UnknownJobException(row.Type);

        var batch = new EnqueueBatch(row.Type, row.Queue!, row.Priority!.Value, row.MaxAttempts!.Value, row.RetryDelaysSeconds,
            TraceParent: null, TimeSpan.Zero, [Guid.CreateVersion7()], [row.ArgsJson], [null],
            [row.Overlap == OverlapPolicy.Skip ? ReservedUniqueKeys.Recurring(id) : null])
        {
            RecurringIds = [id],
            ScheduledAtUtc = [null],
        };
        var result = (await store.EnqueueAsync(batch, null, ct))[0];
        if (result.Created)
            wakeup.Signal(row.Queue!);
        return new EnqueueResult(result.Id is { } jobId ? new JobId(jobId) : null, result.Created);
    }

    private async Task UpsertAsync(RecurringDefinition definition, CancellationToken ct)
    {
        if (await store.UpsertRecurringAsync(definition, ct) == RecurringUpsertResult.OwnedByCodeFirst)
            throw OwnedByCodeFirst(definition.Id);
    }

    private static InvalidOperationException OwnedByCodeFirst(string id) =>
        new($"Recurring job '{id}' is declared in code (AddRecurring) and managed by its owner's synchronization; " +
            "only Pause, Resume, Trigger and Get are allowed through IRecurringJobManager.");
}
```

В `PgBoltServiceCollectionExtensions.AddPgBolt` после регистрации `IJobClient` добавить:

```csharp
        services.AddSingleton<IRecurringJobManager>(sp => new RecurringJobManager(sp.GetRequiredService<PgBoltStore>(),
            sp.GetRequiredService<JobTypesCache>(), sp.GetRequiredService<IWakeupSignal>()));
```

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringApiTests"`
Expected: PASS (Direct и Bouncer).

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(recurring): runtime API over recurring_jobs"
```

---
### Task 7: Транзакция тика

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/RecurringRecords.cs` (результат тика)
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.RecurringTick.cs`
- Modify (тесты): `tests/PgBolt.Postgres.Tests/RecurringTestSupport.cs` (удержание тика хуком, ожидание блокировки строки)
- Test: `tests/PgBolt.Postgres.Tests/RecurringTickTests.cs`

**Interfaces:**
- Consumes: `CronSchedule` (Task 4), `RecurringPlanner`, `PlanInput`, `FirePlan` (Task 5), `CreateEnqueueCommand`, `ReservedUniqueKeys.Recurring` (Task 3), `RecurringTestSupport` (Task 6).
- Produces (`internal`):
  - `record RecurringRowError(string Id, string Error)`, `record RecurringMissed(string Id, DateTime FromUtc, DateTime ToUtc)`;
  - `record RecurringTickResult(int Claimed, int Fired, bool Full, IReadOnlyCollection<string> Queues, IReadOnlyList<RecurringRowError> Errors, IReadOnlyList<RecurringMissed> Missed, IReadOnlyList<string> OverlapSkipped, IReadOnlyList<string> Waiting, IReadOnlyList<string> Exhausted)` со `static RecurringTickResult Empty`; `Exhausted` — у cron кончились срабатывания (`next_fire_at` стал `NULL`, Warning §7); `Full` — захвачено `batchSize` строк или исчерпан лимит срабатываний (цикл повторяет тик);
  - `Task<RecurringTickResult> PgBoltStore.TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct)`;
  - `internal Func<CancellationToken, Task>? PgBoltStore.TickBeforeCommit { get; set; }` — только для тестов (§10 «тик удерживается хуком между захватом и `COMMIT`»): вызывается в транзакции тика с захваченными строками после enqueue и обновления курсоров, непосредственно перед `COMMIT`; в рабочем коде всегда `null`;
  - тестовые помощники `RecurringTestSupport.HoldTickAsync(PgBoltStore, CancellationToken) : Task<HeldTick>`, `RecurringTestSupport.WaitForRowLockWaiterAsync(TestDb, CancellationToken)`, `record HeldTick(Task<RecurringTickResult> Tick, TaskCompletionSource Release)` (используются и в Task 8).

- [x] **Step 1: Тесты**

В `tests/PgBolt.Postgres.Tests/RecurringTestSupport.cs` добавить `using System.Diagnostics;`, `using PgBolt.Testing;` и в класс `RecurringTestSupport`:

```csharp
    /// <summary>
    /// Тик, удерживаемый хуком TickBeforeCommit (§10): строки захвачены FOR UPDATE, задачи вставлены, курсоры обновлены,
    /// COMMIT ещё не выполнен. Хук снимается сразу после захвата — следующие тики не удерживаются.
    /// </summary>
    public static async Task<HeldTick> HoldTickAsync(PgBoltStore store, CancellationToken ct)
    {
        var claimed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.TickBeforeCommit = async c =>
        {
            claimed.TrySetResult();
            await release.Task.WaitAsync(c);
        };
        try
        {
            var tick = store.TickRecurringAsync(100, 1000, TimeSpan.FromMinutes(1), ct);
            if (await Task.WhenAny(claimed.Task, tick).WaitAsync(TimeSpan.FromSeconds(10), ct) != claimed.Task)
                throw new InvalidOperationException($"The tick committed without holding rows: {await tick}.");
            return new HeldTick(tick, release);
        }
        finally
        {
            store.TickBeforeCommit = null;
        }
    }

    /// <summary>Ждёт сессию, вставшую в ожидание блокировки строки recurring_jobs схемы теста (pg_stat_activity по тексту statement'а).</summary>
    public static async Task WaitForRowLockWaiterAsync(TestDb db, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        while (await db.ScalarAsync<long>(
                   "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p",
                   ("p", $"%{db.Schema}.recurring_jobs%")) == 0)
        {
            if (Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException("No session waits for a recurring_jobs row lock.");
            await Task.Delay(20, ct);
        }
    }
```

и рядом с классом:

```csharp
/// <summary>Удерживаемый тик: ReleaseAsync отпускает COMMIT и возвращает результат тика.</summary>
internal sealed record HeldTick(Task<RecurringTickResult> Tick, TaskCompletionSource Release)
{
    public Task<RecurringTickResult> ReleaseAsync()
    {
        Release.TrySetResult();
        return Tick;
    }
}
```

`tests/PgBolt.Postgres.Tests/RecurringTickTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RecurringTickTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly TimeSpan Threshold = TimeSpan.FromMinutes(1);
    private const string LastMinute = "date_trunc('minute', now()) - interval '1 minute'";

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        RecurringTestSupport.EnsureRegistered();
        await RecurringTestSupport.PublishAsync(Store, Ct);
    }

    private Task<RecurringTickResult> TickAsync(int batchSize = 100, int maxFires = 1000) =>
        Store.TickRecurringAsync(batchSize, maxFires, Threshold, Ct);

    /// <summary>Строка в обход API: курсор и пробуждение — SQL-выражения от now().</summary>
    private Task InsertAsync(string id, string cron = "* * * * *", string next = LastMinute,
        MisfirePolicy misfire = MisfirePolicy.FireOnce, OverlapPolicy overlap = OverlapPolicy.Allow, string window = "1 day",
        string type = "rec.job", string timeZone = "UTC", string? wake = null) =>
        Db.ExecAsync($$"""
            INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
                                                 next_fire_at, wake_at, created_at, updated_at)
            VALUES (@id, @type, '{"N":1}', @cron, @tz, @misfire, interval '{{window}}', @overlap,
                    {{next}}, {{wake ?? next}}, now(), now())
            """, ("id", id), ("type", type), ("cron", cron), ("tz", timeZone), ("misfire", (short)misfire), ("overlap", (short)overlap));

    private Task<bool> RowAsync(string id, string condition) =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.recurring_jobs WHERE id = @id", ("id", id));

    private Task<long> JobsAsync(string id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = @id", ("id", id));

    [Fact]
    public async Task Due_fire_once_row_enqueues_one_job_and_moves_cursor()
    {
        await InsertAsync("a", next: "date_trunc('minute', now()) - interval '3 minutes'");

        var result = await TickAsync();

        Assert.Equal((1, 1, false), (result.Claimed, result.Fired, result.Full));
        Assert.Equal(new[] { "rq" }, result.Queues);
        Assert.Equal(1L, await JobsAsync("a"));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT j.scheduled_at = r.last_fire_at AND j.id = r.last_job_id AND j.queue = 'rq' AND j.priority = 1
                   AND j.unique_key IS NULL AND j.args = '{"N":1}'
                   AND r.next_fire_at > now() AND r.wake_at = r.next_fire_at AND r.last_error IS NULL
                   AND r.last_fire_at >= date_trunc('minute', now()) - interval '1 minute'
            FROM {schema}.recurring_jobs r JOIN {schema}.jobs j ON j.recurring_id = r.id WHERE r.id = 'a'
            """));
    }

    [Fact]
    public async Task Future_and_paused_rows_are_not_claimed()
    {
        await InsertAsync("future", next: "now() + interval '1 hour'");
        await InsertAsync("paused");
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET paused = true WHERE id = 'paused'");

        var result = await TickAsync();

        Assert.Equal(0, result.Claimed);
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Overlap_skip_fire_once_skips_while_previous_is_alive()
    {
        await InsertAsync("a", overlap: OverlapPolicy.Skip);
        Assert.Equal(1, (await TickAsync()).Fired);
        await Db.ExecAsync($"UPDATE {{schema}}.recurring_jobs SET next_fire_at = {LastMinute}, wake_at = now() WHERE id = 'a'");

        var result = await TickAsync();

        Assert.Equal(0, result.Fired);
        Assert.Equal(new[] { "a" }, result.OverlapSkipped);
        Assert.Equal(1L, await JobsAsync("a"));
        Assert.True(await RowAsync("a", "next_fire_at > now()"));
    }

    [Fact]
    public async Task CatchUp_skip_waits_for_running_job_and_keeps_order()   // Review Focus 2
    {
        // фиксированные моменты в прошлом: тест не зависит от границы минуты
        await InsertAsync("a", next: "'2026-01-01 00:00:00+00'", misfire: MisfirePolicy.CatchUp, overlap: OverlapPolicy.Skip,
            window: "3650 days");

        Assert.Equal(1, (await TickAsync()).Fired);
        var blocked = await TickAsync();                                      // первый запуск жив: курсор стоит

        Assert.Equal(new[] { "a" }, blocked.Waiting);
        Assert.True(await RowAsync("a", "next_fire_at = '2026-01-01 00:01:00+00' AND wake_at > now() + interval '4 seconds'"));

        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE unique_key = 'pgbolt:recurring:a'");   // запуск завершился
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'a'");
        Assert.Equal(1, (await TickAsync()).Fired);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT array_agg(scheduled_at ORDER BY id) = ARRAY['2026-01-01 00:00:00+00', '2026-01-01 00:01:00+00']::timestamptz[]
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));                                                                 // UUIDv7: порядок id = порядок постановки
    }

    [Fact]
    public async Task CatchUp_allow_catches_up_in_chunks_of_100()
    {
        await InsertAsync("a", next: "date_trunc('minute', now()) - interval '250 minutes'", misfire: MisfirePolicy.CatchUp);

        var first = await TickAsync();
        Assert.True(await RowAsync("a", "wake_at <= now()"));                  // догон встаёт в общую очередь
        var second = await TickAsync();
        var third = await TickAsync();

        Assert.Equal((100, 100), (first.Fired, second.Fired));
        Assert.InRange(third.Fired, 50, 52);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) = count(DISTINCT scheduled_at)
                   AND max(scheduled_at) - min(scheduled_at) = (count(*) - 1) * interval '1 minute'
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));
    }

    [Fact]
    public async Task Batch_fire_budget_leaves_rest_untouched()
    {
        for (var i = 0; i < 15; i++)
            await InsertAsync($"r{i:D2}", next: "date_trunc('minute', now()) - interval '150 minutes'", misfire: MisfirePolicy.CatchUp,
                wake: $"now() - interval '{100 - i} seconds'");

        var result = await TickAsync();

        Assert.Equal((1000, true), (result.Fired, result.Full));
        Assert.Equal(new[] { "r10", "r11", "r12", "r13", "r14" }, await Db.ScalarAsync<string[]>(   // лимит: курсоры не тронуты
            "SELECT array_agg(id ORDER BY id) FROM {schema}.recurring_jobs WHERE last_fire_at IS NULL"));
    }

    [Fact]
    public async Task Catch_up_rows_do_not_starve_due_rows()                  // §5.3: wake_at = max(next_fire_at, db_now)
    {
        for (var i = 0; i < 15; i++)
            await InsertAsync($"c{i:D2}", cron: "* * * * * *", next: "date_trunc('second', now()) - interval '1 day'",
                misfire: MisfirePolicy.CatchUp, window: "2 days");
        await InsertAsync("normal", wake: "now() - interval '1 second'");

        await TickAsync();                                                     // лимит исчерпан на 10-й догоняющей строке
        Assert.Equal(0L, await JobsAsync("normal"));
        await TickAsync();                                                     // 5 старых догоняющих, затем normal раньше сдвинутых

        Assert.Equal(1L, await JobsAsync("normal"));
    }

    [Fact]
    public async Task Broken_rows_do_not_block_the_batch()                    // Review Focus 1
    {
        await InsertAsync("unknown", type: "nope", wake: "now() - interval '4 seconds'");
        await InsertAsync("tz", timeZone: "Mars/Olympus", wake: "now() - interval '3 seconds'");
        await InsertAsync("cron", cron: "not a cron", wake: "now() - interval '2 seconds'");
        await InsertAsync("good", wake: "now() - interval '1 second'");

        var result = await TickAsync();

        Assert.Equal(new[] { "cron", "tz", "unknown" }, result.Errors.Select(e => e.Id).Order());
        Assert.Equal(1, result.Fired);
        foreach (var id in new[] { "unknown", "tz", "cron" })
            Assert.True(await RowAsync(id, "last_error IS NOT NULL AND wake_at > now() + interval '50 seconds' AND next_fire_at < now()"));
        Assert.Contains("nope", result.Errors.Single(e => e.Id == "unknown").Error);
        Assert.Equal(0, (await TickAsync()).Claimed);                           // битые строки спят, хорошая сдвинута
    }

    [Fact]
    public async Task Defaults_come_from_job_types_at_fire_time()
    {
        await InsertAsync("a");
        await Db.ExecAsync("UPDATE {schema}.job_types SET default_queue = 'rq2', default_max_attempts = 7 WHERE name = 'rec.job'");
        await TickAsync();
        Assert.True(await Db.ScalarAsync<bool>("SELECT queue = 'rq2' AND max_attempts = 7 FROM {schema}.jobs WHERE recurring_id = 'a'"));

        await Db.ExecAsync($"UPDATE {{schema}}.recurring_jobs SET queue = 'own', next_fire_at = {LastMinute}, wake_at = now() WHERE id = 'a'");
        await TickAsync();
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a' AND queue = 'own'"));
    }

    [Fact]
    public async Task Skip_misfire_drops_stale_fire_and_reports_it()
    {
        await InsertAsync("stale", cron: "0 0 1 1 *", next: "'2020-01-01 00:00:00+00'", misfire: MisfirePolicy.Skip);
        await InsertAsync("fresh", misfire: MisfirePolicy.Skip);

        var result = await TickAsync();

        Assert.Equal(1, result.Fired);
        Assert.Equal(1L, await JobsAsync("fresh"));
        var missed = Assert.Single(result.Missed);
        Assert.Equal(("stale", new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc)), (missed.Id, missed.FromUtc));
        Assert.True(await RowAsync("stale", "next_fire_at > now() AND last_fire_at IS NULL"));
    }

    [Fact]
    public async Task Parallel_tickers_create_exactly_one_job_per_second()
    {
        await InsertAsync("a", cron: "* * * * * *", next: "date_trunc('second', now())", misfire: MisfirePolicy.CatchUp);
        var until = DateTime.UtcNow.AddSeconds(10);                         // §10: 8 тикеров, 10 с

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            while (DateTime.UtcNow < until)
            {
                await TickAsync();
                await Task.Delay(50, Ct);
            }
        }, Ct)));

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) >= 9 AND count(*) = count(DISTINCT scheduled_at)
                   AND max(scheduled_at) - min(scheduled_at) = (count(*) - 1) * interval '1 second'
            FROM {schema}.jobs WHERE recurring_id = 'a'
            """));
    }

    [Fact]
    public async Task Thousand_rows_due_together_fire_in_batches_of_100()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
                                                 next_fire_at, wake_at, created_at, updated_at)
            SELECT 'r' || g, 'rec.job', '{}', '* * * * *', 'UTC', 0, interval '1 day', 1,
                   date_trunc('minute', now()), date_trunc('minute', now()), now(), now()
            FROM generate_series(1, 1000) g
            """);

        var fired = new List<int>();
        RecurringTickResult r;
        do
        {
            r = await TickAsync();
            fired.Add(r.Fired);
        }
        while (r.Full);

        Assert.All(fired, f => Assert.True(f <= 100));
        Assert.Equal(1000, fired.Sum());
        Assert.Equal(1000L, await Db.ScalarAsync<long>("SELECT count(DISTINCT recurring_id) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Trigger_and_tick_on_same_id_do_not_deadlock_or_duplicate()
    {
        await InsertAsync("a", cron: "* * * * * *", next: "date_trunc('second', now())", overlap: OverlapPolicy.Skip);
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var until = DateTime.UtcNow.AddSeconds(3);

        await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
        {
            while (DateTime.UtcNow < until)
            {
                if (i % 2 == 0)
                    await TickAsync();
                else
                    await manager.TriggerAsync("a", Ct);
                await Db.ExecAsync("DELETE FROM {schema}.queue WHERE unique_key = 'pgbolt:recurring:a' AND random() < 0.3");
            }
        }, Ct)));

        Assert.True(await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE unique_key = 'pgbolt:recurring:a'") <= 1);
    }

    /// <summary>Задач расписания на момент его последнего поставленного срабатывания (last_fire_at).</summary>
    private Task<long> JobsAtLastFireAsync(string id) =>
        Db.ScalarAsync<long>("""
            SELECT count(*) FROM {schema}.jobs j JOIN {schema}.recurring_jobs r ON r.last_fire_at = j.scheduled_at
            WHERE j.recurring_id = @id AND r.id = @id
            """, ("id", id));

    [Fact]
    public async Task Api_cron_change_while_tick_holds_row_fires_once()      // §10: конкуренция API и тика; Review Focus 3
    {
        await InsertAsync("a");                                              // "* * * * *", FireOnce, Allow, курсор в прошлом
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);     // тик поставил срабатывание X, COMMIT не выполнен

        // Новый cron с тем же множеством моментов (каждую минуту в 0 с): X совпадает с его срабатыванием.
        var update = manager.AddOrUpdateAsync("a", "0 * * * * *", new RecArgs(1), new RecurringJobOptions { Overlap = OverlapPolicy.Allow }, Ct)
            .AsTask();
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);        // AddOrUpdate ждёт FOR UPDATE строки
        Assert.False(update.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        await update;

        Assert.True(await RowAsync("a", "cron = '0 * * * * *' AND last_fire_at IS NOT NULL AND last_job_id IS NOT NULL " +
                                        "AND next_fire_at > last_fire_at AND wake_at = next_fire_at"));
        Assert.Equal(1L, await JobsAtLastFireAsync("a"));
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'a'");
        await TickAsync();                                                   // курсор после X: X второй раз не ставится
        Assert.Equal(1L, await Db.ScalarAsync<long>("""
            SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a'
              AND scheduled_at = (SELECT min(scheduled_at) FROM {schema}.jobs WHERE recurring_id = 'a')
            """));
    }

    [Fact]
    public async Task Remove_while_tick_holds_row_lets_tick_finish()        // §5.5, §10
    {
        await InsertAsync("a");
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);

        var remove = manager.RemoveAsync("a", Ct).AsTask();
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);
        Assert.False(remove.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        Assert.True(await remove);

        Assert.Equal(1L, await JobsAsync("a"));                              // задача тика создана
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs WHERE id = 'a'"));
        Assert.Equal(0, (await TickAsync()).Claimed);
    }

    [Fact]
    public async Task Pause_while_tick_holds_row_lets_tick_finish_and_stops_next_ticks()   // §5.5, §10
    {
        await InsertAsync("a");
        var manager = RecurringTestSupport.Manager(Store, new RecordingWakeup());
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);

        var pause = manager.PauseAsync("a", Ct).AsTask();
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);
        Assert.False(pause.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        Assert.True(await pause);

        Assert.Equal(1L, await JobsAsync("a"));
        Assert.True(await RowAsync("a", "paused AND last_fire_at IS NOT NULL AND next_fire_at > last_fire_at"));   // курсор тика сохранён
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() - interval '1 second' WHERE id = 'a'");
        Assert.Equal(0, (await TickAsync()).Claimed);                        // на паузе строку не берут
        Assert.Equal(1L, await JobsAsync("a"));
    }
}

public sealed class RecurringTickTests_Direct(PostgresFixture pg) : RecurringTickTests(pg, ConnectionMode.Direct);
public sealed class RecurringTickTests_Bouncer(PostgresFixture pg) : RecurringTickTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringTickTests"`
Expected: FAIL — нет `TickRecurringAsync`, `TickBeforeCommit`.

- [x] **Step 3: Результат тика**

В `src/PgBolt.Postgres/Storage/RecurringRecords.cs` добавить:

```csharp
/// <summary>Ошибка строки (часть 4 §5.2 п. 2): строка не поставлена, курсор стоит, wake_at = db_now + 1 мин.</summary>
internal sealed record RecurringRowError(string Id, string Error);

/// <summary>Срабатывания, отброшенные misfire-политикой при сдвиге курсора (Warning §7): CatchUp — [FromUtc, ToUtc), Skip — [FromUtc, ToUtc] (как MissedRange).</summary>
internal sealed record RecurringMissed(string Id, DateTime FromUtc, DateTime ToUtc);

/// <summary>
/// Итог транзакции тика. Full — захвачено batchSize строк или исчерпан лимит срабатываний: цикл повторяет тик сразу.
/// OverlapSkipped — FireOnce/Skip пропущены из-за живого запуска; Waiting — CatchUp + Skip ждут его завершения;
/// Exhausted — у cron больше нет срабатываний, строка больше не сработает.
/// </summary>
internal sealed record RecurringTickResult(
    int Claimed,
    int Fired,
    bool Full,
    IReadOnlyCollection<string> Queues,
    IReadOnlyList<RecurringRowError> Errors,
    IReadOnlyList<RecurringMissed> Missed,
    IReadOnlyList<string> OverlapSkipped,
    IReadOnlyList<string> Waiting,
    IReadOnlyList<string> Exhausted)
{
    public static readonly RecurringTickResult Empty = new(0, 0, false, [], [], [], [], [], []);
}
```

- [x] **Step 4: Транзакция тика**

`src/PgBolt.Postgres/Storage/PgBoltStore.RecurringTick.cs`:

```csharp
using Npgsql;
using NpgsqlTypes;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Тик расписаний (часть 4 §5.2): захват FOR UPDATE SKIP LOCKED, планирование в .NET, enqueue групп одним NpgsqlBatch и
/// сдвиг курсоров — в одной транзакции, поэтому на срабатывание в кластере ставится не больше одной задачи.
/// </summary>
public sealed partial class PgBoltStore
{
    private const string TickClaimTemplate = """
        SELECT r.id, r.type, r.args, r.cron, r.time_zone, r.misfire, r.catch_up_window, r.overlap, r.next_fire_at,
               COALESCE(r.queue,        t.default_queue),
               COALESCE(r.priority,     t.default_priority),
               COALESCE(r.max_attempts, t.default_max_attempts),
               COALESCE(r.retry_delays, t.default_retry_delays),
               t.name IS NULL,
               now()
        FROM {schema}.recurring_jobs r
        LEFT JOIN {schema}.job_types t ON t.name = r.type
        WHERE r.wake_at <= now() AND NOT r.paused
        ORDER BY r.wake_at
        LIMIT @n
        FOR UPDATE OF r SKIP LOCKED
        """;

    private const string TickUpdateTemplate = """
        UPDATE {schema}.recurring_jobs r
        SET next_fire_at = u.next_fire_at, wake_at = u.wake_at,
            last_fire_at = COALESCE(u.last_fire_at, r.last_fire_at),
            last_job_id  = COALESCE(u.last_job_id,  r.last_job_id),
            -- §3, §7: ошибка строки пишется, успешное срабатывание очищает; пропуск и ожидание без срабатывания не трогают
            last_error   = CASE WHEN u.last_error IS NOT NULL OR u.last_fire_at IS NOT NULL THEN u.last_error ELSE r.last_error END
        FROM unnest(@ids::text[], @next::timestamptz[], @wake::timestamptz[],
                    @fired::timestamptz[], @job_ids::uuid[], @errors::text[])
             AS u(id, next_fire_at, wake_at, last_fire_at, last_job_id, last_error)
        WHERE r.id = u.id
        """;

    private string TickClaimSql => field ??= Render(TickClaimTemplate);
    private string TickUpdateSql => field ??= Render(TickUpdateTemplate);

    private sealed record TickRow(string Id, string Type, string ArgsJson, string Cron, string TimeZone, MisfirePolicy Misfire,
        TimeSpan CatchUpWindow, OverlapPolicy Overlap, DateTime? NextFireAt, string? Queue, short? Priority, int? MaxAttempts,
        int[]? RetryDelaysSeconds, bool UnknownType);

    private sealed record PlannedRow(TickRow Row, PlanInput Input, FirePlan Plan, Guid[] JobIds);

    private readonly record struct CursorUpdate(string Id, DateTime? Next, DateTime? Wake, DateTime? FiredAt, Guid? JobId, string? Error);

    /// <summary>
    /// Только для тестов (§10): вызывается в транзакции тика с захваченными строками — после enqueue и обновления курсоров,
    /// перед COMMIT. Позволяет удержать тик и проверить конкуренцию с API и синхронизацией. В рабочем коде — null.
    /// </summary>
    internal Func<CancellationToken, Task>? TickBeforeCommit { get; set; }

    internal Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxFires);
        return RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);

            var (rows, dbNow) = await ClaimRecurringAsync(conn, tx, batchSize, c);
            if (rows.Count == 0)
            {
                await tx.CommitAsync(c);
                return RecurringTickResult.Empty;
            }

            // 2. Планирование: ошибка строки не откатывает пакет; строка, не поместившаяся в лимит, и все следующие не трогаются.
            var full = rows.Count == batchSize;
            var budget = maxFires;
            var planned = new List<PlannedRow>();
            var updates = new List<CursorUpdate>();
            var errors = new List<RecurringRowError>();
            foreach (var row in rows)
            {
                PlanInput input;
                FirePlan plan;
                try
                {
                    if (row.UnknownType)
                        throw new UnknownJobException(row.Type);
                    input = new PlanInput(CronSchedule.Parse(row.Cron, row.TimeZone), row.Misfire, row.CatchUpWindow, row.Overlap, row.NextFireAt);
                    plan = RecurringPlanner.Plan(input, dbNow, misfireThreshold);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    var error = $"{e.GetType().Name}: {e.Message}";
                    errors.Add(new RecurringRowError(row.Id, error));
                    updates.Add(new CursorUpdate(row.Id, row.NextFireAt,
                        row.NextFireAt is null ? null : dbNow + RecurringPlanner.RowErrorDelay, null, null, error));
                    continue;
                }
                if (plan.Fires.Count > budget)
                {
                    full = true;
                    break;
                }
                budget -= plan.Fires.Count;
                planned.Add(new PlannedRow(row, input, plan, plan.Fires.Select(_ => Guid.CreateVersion7()).ToArray()));
            }

            // 3. Enqueue: одна команда на группу (type, queue, priority, max_attempts, retry_delays), все — одним NpgsqlBatch.
            var created = await EnqueueFiresAsync(conn, tx, planned, c);

            // 4. Курсоры: при CatchUp + Skip курсор зависит от конфликта по ключу, поэтому после enqueue.
            var queues = new HashSet<string>(StringComparer.Ordinal);
            var missed = new List<RecurringMissed>();
            var overlapSkipped = new List<string>();
            var waiting = new List<string>();
            var exhausted = new List<string>();
            var fired = 0;
            foreach (var p in planned)
            {
                DateTime? lastFired = null;
                Guid? lastJob = null;
                var conflict = false;
                for (var i = 0; i < p.JobIds.Length; i++)
                {
                    if (created.GetValueOrDefault(p.JobIds[i]))
                    {
                        lastFired = p.Plan.Fires[i];
                        lastJob = p.JobIds[i];
                        fired++;
                        queues.Add(p.Row.Queue!);
                    }
                    else
                    {
                        conflict = true;
                    }
                }

                if (conflict && p.Row.Misfire == MisfirePolicy.CatchUp && p.Row.Overlap == OverlapPolicy.Skip)
                {
                    var wait = RecurringPlanner.OnConflict(p.Input, dbNow);
                    updates.Add(new CursorUpdate(p.Row.Id, wait.NextFireAt, wait.WakeAt, null, null, null));
                    waiting.Add(p.Row.Id);
                    continue;
                }
                if (conflict)
                    overlapSkipped.Add(p.Row.Id);
                if (p.Plan.Missed is { } m)
                    missed.Add(new RecurringMissed(p.Row.Id, m.FromUtc, m.ToUtc));
                if (p.Plan.NextFireAt is null && p.Row.NextFireAt is not null)
                    exhausted.Add(p.Row.Id);
                updates.Add(new CursorUpdate(p.Row.Id, p.Plan.NextFireAt, p.Plan.WakeAt, lastFired, lastJob, null));
            }

            if (updates.Count > 0)
                await UpdateCursorsAsync(conn, tx, updates, c);
            if (TickBeforeCommit is { } hook)
                await hook(c);
            await tx.CommitAsync(c);
            return new RecurringTickResult(rows.Count, fired, full, queues, errors, missed, overlapSkipped, waiting, exhausted);
        }, ct);
    }

    private async Task<(List<TickRow> Rows, DateTime DbNow)> ClaimRecurringAsync(NpgsqlConnection conn, NpgsqlTransaction tx, int batchSize,
        CancellationToken ct)
    {
        var rows = new List<TickRow>();
        DateTime dbNow = default;
        await using var cmd = new NpgsqlCommand(TickClaimSql, conn, tx);
        P.Add(cmd.Parameters, "n", batchSize);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            rows.Add(new TickRow(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4),
                (MisfirePolicy)r.GetInt16(5), r.GetFieldValue<TimeSpan>(6), (OverlapPolicy)r.GetInt16(7),
                r.GetNullableValue<DateTime>(8), r.GetNullable<string>(9), r.GetNullableValue<short>(10),
                r.GetNullableValue<int>(11), r.GetNullable<int[]>(12), r.GetBoolean(13)));
            dbNow = r.GetDateTime(14);
        }
        return (rows, dbNow);
    }

    /// <summary>id → created. Id свежие, поэтому !created — конфликт по unique_key (жив предыдущий запуск), без запроса разрешения.</summary>
    private async Task<Dictionary<Guid, bool>> EnqueueFiresAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<PlannedRow> planned,
        CancellationToken ct)
    {
        var created = new Dictionary<Guid, bool>();
        var groups = planned
            .Where(p => p.JobIds.Length > 0)
            .GroupBy(p => (p.Row.Type, Queue: p.Row.Queue!, Priority: p.Row.Priority!.Value, MaxAttempts: p.Row.MaxAttempts!.Value,
                Retry: p.Row.RetryDelaysSeconds is { } d ? string.Join(',', d) : "-"))
            .ToList();
        if (groups.Count == 0)
            return created;

        await using var batch = new NpgsqlBatch(conn, tx);
        foreach (var g in groups)
        {
            var fires = g.SelectMany(p => p.Plan.Fires.Select((at, i) => (p.Row, At: at, Id: p.JobIds[i]))).ToList();
            batch.BatchCommands.Add(CreateEnqueueCommand(new EnqueueBatch(g.Key.Type, g.Key.Queue, g.Key.Priority, g.Key.MaxAttempts,
                g.First().Row.RetryDelaysSeconds, TraceParent: null, TimeSpan.Zero,
                fires.Select(f => f.Id).ToArray(), fires.Select(f => f.Row.ArgsJson).ToArray(), new DateTime?[fires.Count],
                fires.Select(f => f.Row.Overlap == OverlapPolicy.Skip ? ReservedUniqueKeys.Recurring(f.Row.Id) : null).ToArray())
            {
                RecurringIds = fires.Select(f => f.Row.Id).ToArray(),
                ScheduledAtUtc = fires.Select(f => (DateTime?)f.At).ToArray(),
            }));
        }

        await using var r = await batch.ExecuteReaderAsync(ct);
        do
        {
            while (await r.ReadAsync(ct))
                created[r.GetGuid(0)] = r.GetBoolean(1);
        }
        while (await r.NextResultAsync(ct));
        return created;
    }

    private async Task UpdateCursorsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<CursorUpdate> updates, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(TickUpdateSql, conn, tx);
        var ps = cmd.Parameters;
        P.Add(ps, "ids", updates.Select(u => u.Id).ToArray());
        P.Add(ps, "next", updates.Select(u => u.Next).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "wake", updates.Select(u => u.Wake).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "fired", updates.Select(u => u.FiredAt).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        P.Add(ps, "job_ids", updates.Select(u => u.JobId).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "errors", updates.Select(u => u.Error).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Text);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

- [x] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringTickTests"`
Expected: PASS (Direct и Bouncer). Тесты, зависящие от границы минуты (`LastMinute`), устойчивы: курсор всегда ≤ `now()` и совпадает со срабатыванием.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests/RecurringTickTests.cs tests/PgBolt.Postgres.Tests/RecurringTestSupport.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(recurring): leaderless tick transaction"
```

---
### Task 8: Транзакция синхронизации code-first

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/RecurringRecords.cs` (результат синхронизации)
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.RecurringSync.cs`
- Test: `tests/PgBolt.Postgres.Tests/RecurringSyncTests.cs`, `tests/PgBolt.Postgres.Tests/RecurringSyncRetryTests.cs`

**Interfaces:**
- Consumes: `RecurringDefinition` (Task 4), `InsertRecurringCommand`, `UpdateRecurringCommand`, `Later` (Task 6), `RecurringTestSupport.HoldTickAsync`, `WaitForRowLockWaiterAsync`, `PublishAsync` (Task 6, 7).
- Produces (`internal`):
  - `record RecurringFieldChange(string Id, IReadOnlyList<string> Fields)`;
  - `record RecurringSyncResult(IReadOnlyList<string> Inserted, IReadOnlyList<RecurringFieldChange> Updated, IReadOnlyList<string> Deleted, IReadOnlyList<string> Collisions)` с `int Writes`;
  - `Task<RecurringSyncResult> PgBoltStore.SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger, CancellationToken ct)` — пустой список = `ClearRecurring()` (удаляет строки владельца). Имена полей в `Fields`: `type`, `args`, `cron`, `time_zone`, `misfire`, `catch_up_window`, `overlap`, `queue`, `priority`, `max_attempts`, `retry_delays`. `logger` — куда писать Warning о повторе при `40P01` (§6.2, §7); вызывающие передают свой логгер (`PgBoltInitializer` — Task 9, `ProcessingMaintenance` — Task 10);
  - `static Task<T> PgBoltStore.RetrySyncDeadlockAsync<T>(Func<CancellationToken, Task<T>> op, int maxAttempts, ILogger logger, string owner, CancellationToken ct)` — повтор транзакции синхронизации при `40P01` до `maxAttempts` попыток (`DbRetry.MaxAttempts = 3`; у хранилища с `retryOnConflict: false` — 1, для тестов отсутствия deadlock), Warning на каждый повтор; исчерпание и другие ошибки — исключение наружу. Синхронизация больше не идёт через `RunAsync`/`DbRetry` (тот повторяет молча).

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/RecurringSyncRetryTests.cs` (без БД):

```csharp
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Tests;

public sealed class RecurringSyncRetryTests
{
    private readonly ListLogger<RecurringSyncRetryTests> _log = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static PostgresException Error(string sqlState) => new("test", "ERROR", "ERROR", sqlState);

    private IEnumerable<LogEntry> DeadlockWarnings =>
        _log.Entries.Where(e => e.Level == LogLevel.Warning && e.Message.Contains("40P01") && e.Message.Contains("svc"));

    [Fact]
    public async Task Deadlock_is_retried_with_warning()                    // §6.2, §7: 40P01 — повтор транзакции, Warning
    {
        var calls = 0;
        var result = await PgBoltStore.RetrySyncDeadlockAsync(_ =>
            ++calls < 3 ? Task.FromException<int>(Error(PostgresErrorCodes.DeadlockDetected)) : Task.FromResult(42), 3, _log, "svc", Ct);

        Assert.Equal((42, 3), (result, calls));
        Assert.Equal(2, DeadlockWarnings.Count());
    }

    [Fact]
    public async Task Deadlock_after_last_attempt_is_thrown()
    {
        var calls = 0;
        var e = await Assert.ThrowsAsync<PostgresException>(() => PgBoltStore.RetrySyncDeadlockAsync<int>(_ =>
        {
            calls++;
            throw Error(PostgresErrorCodes.DeadlockDetected);
        }, 3, _log, "svc", Ct));

        Assert.Equal(PostgresErrorCodes.DeadlockDetected, e.SqlState);
        Assert.Equal(3, calls);
        Assert.Equal(2, DeadlockWarnings.Count());
    }

    [Fact]
    public async Task Other_errors_and_single_attempt_are_not_retried()
    {
        var calls = 0;
        await Assert.ThrowsAsync<PostgresException>(() => PgBoltStore.RetrySyncDeadlockAsync<int>(_ =>
        {
            calls++;
            throw Error(PostgresErrorCodes.UniqueViolation);
        }, 3, _log, "svc", Ct));
        await Assert.ThrowsAsync<PostgresException>(() => PgBoltStore.RetrySyncDeadlockAsync<int>(_ =>
        {
            calls++;
            throw Error(PostgresErrorCodes.DeadlockDetected);
        }, 1, _log, "svc", Ct));

        Assert.Equal(2, calls);
        Assert.Empty(_log.Entries);
    }
}
```

(`ListLogger<T>` и `LogEntry` объявлены в `HostingTests.cs` той же сборки.)

`tests/PgBolt.Postgres.Tests/RecurringSyncTests.cs`:

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class RecurringSyncTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly ILogger Log = NullLogger.Instance;

    private static RecurringDefinition Def(string id, string cron = "0 3 * * *", string args = """{"N":1}""",
        RecurringJobOptions? options = null) =>
        RecurringDefinition.Create(id, "rec.job", args, cron, options);

    private Task<bool> RowAsync(string id, string condition) =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.recurring_jobs WHERE id = @id", ("id", id));

    private Task<string[]> IdsAsync(string owner) =>
        Db.ScalarAsync<string[]>("SELECT coalesce(array_agg(id ORDER BY id), '{}') FROM {schema}.recurring_jobs WHERE owner = @o", ("o", owner));

    [Fact]
    public async Task Inserts_declared_rows_with_owner_and_cursor()
    {
        var result = await Store.SyncRecurringAsync("svc", [Def("a"), Def("b")], Log, Ct);

        Assert.Equal(new[] { "a", "b" }, result.Inserted);
        Assert.Equal(2, result.Writes);
        Assert.Equal(new[] { "a", "b" }, await IdsAsync("svc"));
        Assert.True(await RowAsync("a", "next_fire_at > now() AND wake_at = next_fire_at AND NOT paused"));
    }

    [Fact]
    public async Task Second_sync_without_changes_writes_nothing()
    {
        var options = new RecurringJobOptions
        {
            RetryDelays = [TimeSpan.FromSeconds(1.5)], CatchUpWindow = TimeSpan.FromDays(1) + TimeSpan.FromTicks(5),
        };
        await Store.SyncRecurringAsync("svc", [Def("a", args: """{"a":1,"b":2}""", options: options)], Log, Ct);
        var updatedAt = await Db.ScalarAsync<DateTime>("SELECT updated_at FROM {schema}.recurring_jobs WHERE id = 'a'");

        var result = await Store.SyncRecurringAsync("svc", [Def("a", args: """{ "b": 2, "a": 1 }""", options: options)], Log, Ct);

        Assert.Equal(0, result.Writes);
        Assert.Equal(updatedAt, await Db.ScalarAsync<DateTime>("SELECT updated_at FROM {schema}.recurring_jobs WHERE id = 'a'"));
    }

    [Fact]
    public async Task Changed_args_update_row_and_keep_cursor()
    {
        await Store.SyncRecurringAsync("svc", [Def("a")], Log, Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = '2099-01-01', wake_at = '2099-01-01', paused = true");

        var result = await Store.SyncRecurringAsync("svc", [Def("a", args: """{"N":2}""")], Log, Ct);

        Assert.Equal("args", Assert.Single(Assert.Single(result.Updated).Fields));
        Assert.True(await RowAsync("a", """next_fire_at = '2099-01-01' AND paused AND args = '{"N":2}'"""));
    }

    [Fact]
    public async Task Changed_cron_recalculates_cursor_after_last_fire()
    {
        await Store.SyncRecurringAsync("svc", [Def("a", cron: "* * * * *")], Log, Ct);
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET last_fire_at = date_trunc('minute', now()) + interval '2 minutes'");

        var result = await Store.SyncRecurringAsync("svc", [Def("a", cron: "*/2 * * * *")], Log, Ct);

        Assert.Contains("cron", Assert.Single(result.Updated).Fields);
        Assert.True(await RowAsync("a", "next_fire_at > last_fire_at AND wake_at = next_fire_at"));
    }

    [Fact]
    public async Task Undeclared_rows_of_owner_are_deleted_and_others_untouched()
    {
        await Store.SyncRecurringAsync("svc", [Def("a"), Def("b")], Log, Ct);
        await Store.SyncRecurringAsync("other", [Def("x")], Log, Ct);

        Assert.Equal(new[] { "b" }, (await Store.SyncRecurringAsync("svc", [Def("a")], Log, Ct)).Deleted);
        Assert.Equal(new[] { "a" }, (await Store.SyncRecurringAsync("svc", [], Log, Ct)).Deleted);   // ClearRecurring()

        Assert.Empty(await IdsAsync("svc"));
        Assert.Equal(new[] { "x" }, await IdsAsync("other"));
    }

    [Fact]
    public async Task Api_and_foreign_rows_are_collisions()
    {
        await Store.SyncRecurringAsync("other", [Def("x")], Log, Ct);
        await Store.UpsertRecurringAsync(Def("api"), Ct);

        var result = await Store.SyncRecurringAsync("svc", [Def("x", cron: "0 5 * * *"), Def("api", cron: "0 5 * * *")], Log, Ct);

        Assert.Equal(new[] { "api", "x" }, result.Collisions.Order());
        Assert.Equal(0, result.Writes);
        Assert.True(await RowAsync("x", "owner = 'other' AND cron = '0 3 * * *'"));
        Assert.True(await RowAsync("api", "owner IS NULL AND cron = '0 3 * * *'"));
    }

    [Fact]
    public async Task Parallel_syncs_of_one_owner_are_serialized()
    {
        RecurringDefinition[] defs = [Def("a"), Def("b"), Def("c")];

        await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => Store.SyncRecurringAsync("svc", defs, Log, Ct)));

        Assert.Equal(new[] { "a", "b", "c" }, await IdsAsync("svc"));
    }

    [Fact]
    public async Task Cross_owner_collisions_in_reverse_order_do_not_deadlock()   // Review Focus 4; §10: без 40P01
    {
        // Без повтора при 40P01 (retryOnConflict: false — одна попытка): deadlock вышел бы исключением, а не погасился
        // повтором, после которого проигравший увидел бы закоммиченные строки как коллизии.
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false);
        var log = new ListLogger<RecurringSyncTests>();
        RecurringDefinition[] forward = [Def("p"), Def("q"), Def("r"), Def("s")];
        RecurringDefinition[] backward = [.. forward.Reverse()];
        for (var i = 0; i < 20; i++)
        {
            await Db.ExecAsync("DELETE FROM {schema}.recurring_jobs");

            var results = await Task.WhenAll(
                store.SyncRecurringAsync("A", forward, log, Ct),
                store.SyncRecurringAsync("B", backward, log, Ct));

            Assert.Equal(4, results[0].Inserted.Count + results[1].Inserted.Count);
            Assert.Equal(results[0].Inserted.Order(), results[1].Collisions.Order());
            Assert.Equal(4L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.recurring_jobs WHERE owner IN ('A', 'B')"));
        }
        Assert.DoesNotContain(log.Entries, e => e.Message.Contains("40P01"));
    }

    [Fact]
    public async Task Cron_change_while_tick_holds_row_fires_once()         // §10: смена cron при удерживаемом тиком захвате; Review Focus 3
    {
        await RecurringTestSupport.PublishAsync(Store, Ct);
        var allow = new RecurringJobOptions { Overlap = OverlapPolicy.Allow };
        await Store.SyncRecurringAsync("svc", [Def("a", cron: "* * * * *", options: allow)], Log, Ct);
        await Db.ExecAsync("""
            UPDATE {schema}.recurring_jobs
            SET next_fire_at = date_trunc('minute', now()) - interval '1 minute', wake_at = now() - interval '1 second'
            WHERE id = 'a'
            """);
        var held = await RecurringTestSupport.HoldTickAsync(Store, Ct);     // тик поставил срабатывание X, COMMIT не выполнен

        // Новый cron с тем же множеством моментов: X совпадает с его срабатыванием; синхронизация ждёт FOR UPDATE строки.
        var sync = Store.SyncRecurringAsync("svc", [Def("a", cron: "0 * * * * *", options: allow)], Log, Ct);
        await RecurringTestSupport.WaitForRowLockWaiterAsync(Db, Ct);
        Assert.False(sync.IsCompleted);

        Assert.Equal(1, (await held.ReleaseAsync()).Fired);
        Assert.Contains("cron", Assert.Single((await sync).Updated).Fields);

        Assert.True(await RowAsync("a", "cron = '0 * * * * *' AND last_fire_at IS NOT NULL " +
                                        "AND next_fire_at > last_fire_at AND wake_at = next_fire_at"));
        await Db.ExecAsync("UPDATE {schema}.recurring_jobs SET wake_at = now() WHERE id = 'a'");
        await Store.TickRecurringAsync(100, 1000, TimeSpan.FromMinutes(1), Ct);   // X второй раз не ставится
        Assert.Equal(1L, await Db.ScalarAsync<long>("""
            SELECT count(*) FROM {schema}.jobs WHERE recurring_id = 'a'
              AND scheduled_at = (SELECT min(scheduled_at) FROM {schema}.jobs WHERE recurring_id = 'a')
            """));
    }
}

public sealed class RecurringSyncTests_Direct(PostgresFixture pg) : RecurringSyncTests(pg, ConnectionMode.Direct);
public sealed class RecurringSyncTests_Bouncer(PostgresFixture pg) : RecurringSyncTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringSync"`
Expected: FAIL — нет `SyncRecurringAsync`, `RetrySyncDeadlockAsync`.

- [x] **Step 3: Результат синхронизации**

В `src/PgBolt.Postgres/Storage/RecurringRecords.cs` добавить:

```csharp
internal sealed record RecurringFieldChange(string Id, IReadOnlyList<string> Fields);

/// <summary>Записи синхронизации владельца (часть 4 §6.2); Collisions — id занят другим владельцем или API, строка не тронута.</summary>
internal sealed record RecurringSyncResult(
    IReadOnlyList<string> Inserted,
    IReadOnlyList<RecurringFieldChange> Updated,
    IReadOnlyList<string> Deleted,
    IReadOnlyList<string> Collisions)
{
    public int Writes => Inserted.Count + Updated.Count + Deleted.Count;
}
```

- [x] **Step 4: Транзакция синхронизации**

`src/PgBolt.Postgres/Storage/PgBoltStore.RecurringSync.cs`:

```csharp
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Синхронизация code-first набора владельца (часть 4 §6.2): advisory xact lock владельца, чтение, дифф в .NET, запись одним
/// NpgsqlBatch только при разнице. args сравниваются в SQL как jsonb (форматирование JSON не даёт записей), остальные поля —
/// в нормализованной форме (§4.2). Вставки — в порядке id (Ordinal) с ON CONFLICT DO NOTHING: разные владельцы с
/// пересекающимися id ждут друг друга в одном порядке; невставленный id — коллизия. Отклонение от текста §6.2 (один
/// INSERT … ORDER BY d.id с collation базы): порядок задаёт ordinal-сортировка в .NET, statement'ы идут в batch по одному —
/// для отсутствия deadlock это эквивалентно, так как все процессы используют один компаратор. 40P01 (двойная коллизия, §6.2) —
/// повтор транзакции до 3 раз с Warning (§7), не молча через RunAsync/DbRetry.
/// </summary>
public sealed partial class PgBoltStore
{
    /// <summary>
    /// §6.2, §7: 40P01 синхронизации — повтор всей транзакции, не больше maxAttempts попыток, Warning на каждый повтор.
    /// Исчерпание и любые другие ошибки — исключение наружу (на старте — падение, в цикле — Error и следующий интервал).
    /// </summary>
    internal static async Task<T> RetrySyncDeadlockAsync<T>(Func<CancellationToken, Task<T>> op, int maxAttempts, ILogger logger,
        string owner, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await op(ct);
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.DeadlockDetected && attempt < maxAttempts)
            {
                logger.LogWarning(e,
                    "PgBolt: recurring sync of owner {Owner} hit a deadlock (40P01); retrying the transaction (attempt {Attempt} of {MaxAttempts}).",
                    owner, attempt + 1, maxAttempts);
                await Task.Delay(TimeSpan.FromMilliseconds(5 * attempt + Random.Shared.Next(10)), ct);
            }
        }
    }

    private const string SyncLockSql = "SELECT pg_advisory_xact_lock(hashtext(@key))";

    private const string SyncReadTemplate = """
        SELECT r.id, r.owner, r.type, r.args = d.args, r.cron, r.time_zone, r.misfire, r.catch_up_window, r.overlap,
               r.queue, r.priority, r.max_attempts, r.retry_delays
        FROM {schema}.recurring_jobs r
        LEFT JOIN unnest(@ids::text[], @args::jsonb[]) AS d(id, args) ON d.id = r.id
        WHERE r.owner = @owner OR r.id = ANY(@ids)
        """;

    private const string SyncLockRowsTemplate = """
        SELECT id, last_fire_at FROM {schema}.recurring_jobs WHERE id = ANY(@ids) AND owner = @owner ORDER BY id FOR UPDATE
        """;

    private const string SyncDeleteTemplate = """
        DELETE FROM {schema}.recurring_jobs WHERE owner = @owner AND id = ANY(@ids)
        """;

    private string SyncReadSql => field ??= Render(SyncReadTemplate);
    private string SyncLockRowsSql => field ??= Render(SyncLockRowsTemplate);
    private string SyncDeleteSql => field ??= Render(SyncDeleteTemplate);

    private sealed record SyncRow(string? Owner, string Type, bool ArgsEqual, string Cron, string TimeZone, MisfirePolicy Misfire,
        TimeSpan CatchUpWindow, OverlapPolicy Overlap, string? Queue, short? Priority, int? MaxAttempts, int[]? RetryDelaysSeconds);

    internal Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(owner);
        ArgumentNullException.ThrowIfNull(logger);
        var declared = definitions.OrderBy(d => d.Id, StringComparer.Ordinal).ToList();
        var ids = declared.Select(d => d.Id).ToArray();
        var args = declared.Select(d => d.ArgsJson).ToArray();

        // retryOnConflict: false (тесты отсутствия deadlock) — одна попытка: 40P01 выходит исключением.
        return RetrySyncDeadlockAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);

            DateTime dbNow;
            var existing = new Dictionary<string, SyncRow>(StringComparer.Ordinal);
            await using (var read = new NpgsqlBatch(conn, tx)
                         {
                             BatchCommands = { new NpgsqlBatchCommand(SyncLockSql), new NpgsqlBatchCommand("SELECT now()"), new NpgsqlBatchCommand(SyncReadSql) },
                         })
            {
                P.Add(read.BatchCommands[0].Parameters, "key", $"{Schema}:recurring:{owner}");
                P.Add(read.BatchCommands[2].Parameters, "ids", ids);
                P.Add(read.BatchCommands[2].Parameters, "args", args, NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
                P.Add(read.BatchCommands[2].Parameters, "owner", owner);
                await using var r = await read.ExecuteReaderAsync(c);
                await r.NextResultAsync(c);                                   // результат pg_advisory_xact_lock
                await r.ReadAsync(c);
                dbNow = r.GetDateTime(0);
                await r.NextResultAsync(c);
                while (await r.ReadAsync(c))
                    existing[r.GetString(0)] = new SyncRow(r.GetNullable<string>(1), r.GetString(2), r.GetNullableValue<bool>(3) ?? false,
                        r.GetString(4), r.GetString(5), (MisfirePolicy)r.GetInt16(6), r.GetFieldValue<TimeSpan>(7),
                        (OverlapPolicy)r.GetInt16(8), r.GetNullable<string>(9), r.GetNullableValue<short>(10),
                        r.GetNullableValue<int>(11), r.GetNullable<int[]>(12));
            }

            var inserts = new List<RecurringDefinition>();
            var updates = new List<(RecurringDefinition Definition, List<string> Fields, bool Recalc)>();
            var collisions = new List<string>();
            foreach (var d in declared)
            {
                if (!existing.TryGetValue(d.Id, out var row))
                    inserts.Add(d);
                else if (row.Owner != owner)
                    collisions.Add(d.Id);
                else if (Diff(row, d) is { Count: > 0 } fields)
                    updates.Add((d, fields, fields.Contains("cron") || fields.Contains("time_zone")));
            }
            var declaredIds = ids.ToHashSet(StringComparer.Ordinal);
            var deletes = existing.Where(e => e.Value.Owner == owner && !declaredIds.Contains(e.Key))
                .Select(e => e.Key).Order(StringComparer.Ordinal).ToArray();

            if (inserts.Count == 0 && updates.Count == 0 && deletes.Length == 0)
            {
                await tx.CommitAsync(c);
                return new RecurringSyncResult([], [], [], collisions);
            }

            // Смена cron / таймзоны: курсор от last_fire_at под FOR UPDATE, как в AddOrUpdate (§4.2). Остальные строки не блокируются.
            var lastFire = new Dictionary<string, DateTime?>(StringComparer.Ordinal);
            var recalcIds = updates.Where(u => u.Recalc).Select(u => u.Definition.Id).ToArray();
            if (recalcIds.Length > 0)
            {
                await using var lockRows = new NpgsqlCommand(SyncLockRowsSql, conn, tx);
                P.Add(lockRows.Parameters, "ids", recalcIds);
                P.Add(lockRows.Parameters, "owner", owner);
                await using var r = await lockRows.ExecuteReaderAsync(c);
                while (await r.ReadAsync(c))
                    lastFire[r.GetString(0)] = r.GetNullableValue<DateTime>(1);
            }

            var inserted = new List<string>();
            await using (var write = new NpgsqlBatch(conn, tx))
            {
                foreach (var d in inserts)
                    write.BatchCommands.Add(InsertRecurringCommand(d, owner, d.Schedule().After(dbNow)));
                foreach (var (d, _, recalc) in updates)
                    write.BatchCommands.Add(UpdateRecurringCommand(d, owner, recalc,
                        recalc ? d.Schedule().After(Later(dbNow, lastFire.GetValueOrDefault(d.Id))) : null));
                if (deletes.Length > 0)
                {
                    var delete = new NpgsqlBatchCommand(SyncDeleteSql);
                    P.Add(delete.Parameters, "owner", owner);
                    P.Add(delete.Parameters, "ids", deletes);
                    write.BatchCommands.Add(delete);
                }

                await using var r = await write.ExecuteReaderAsync(c);
                foreach (var d in inserts)
                {
                    if (await r.ReadAsync(c))
                        inserted.Add(d.Id);
                    else
                        collisions.Add(d.Id);                                // параллельно создал API или другой владелец
                    await r.NextResultAsync(c);
                }
            }

            await tx.CommitAsync(c);
            return new RecurringSyncResult(inserted, updates.Select(u => new RecurringFieldChange(u.Definition.Id, u.Fields)).ToList(),
                deletes, collisions);
        }, _retryOnConflict ? DbRetry.MaxAttempts : 1, logger, owner, ct);
    }

    private static List<string> Diff(SyncRow row, RecurringDefinition d)
    {
        var fields = new List<string>();
        if (row.Type != d.Type) fields.Add("type");
        if (!row.ArgsEqual) fields.Add("args");
        if (row.Cron != d.Cron) fields.Add("cron");
        if (row.TimeZone != d.TimeZone) fields.Add("time_zone");
        if (row.Misfire != d.Misfire) fields.Add("misfire");
        if (row.CatchUpWindow != d.CatchUpWindow) fields.Add("catch_up_window");
        if (row.Overlap != d.Overlap) fields.Add("overlap");
        if (row.Queue != d.Queue) fields.Add("queue");
        if (row.Priority != d.Priority) fields.Add("priority");
        if (row.MaxAttempts != d.MaxAttempts) fields.Add("max_attempts");
        if (!RecurringDefinition.SameDelays(row.RetryDelaysSeconds, d.RetryDelaysSeconds)) fields.Add("retry_delays");
        return fields;
    }
}
```

Если форматтер проекта (`dotnet format` / `.editorconfig`) требует фигурные скобки у однострочных `if` в `Diff` — оформить их блоками; поведение не меняется.

- [x] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringSync"`
Expected: PASS (Direct и Bouncer).

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests/RecurringSyncTests.cs tests/PgBolt.Postgres.Tests/RecurringSyncRetryTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(recurring): code-first owner set synchronization"
```

---
### Task 9: Code-first объявления, синхронизация на старте, регистрация

**Files:**
- Create: `src/PgBolt.Postgres/Recurring/RecurringDeclarations.cs`, `src/PgBolt.Postgres/Recurring/RecurringSyncLog.cs`
- Modify: `src/PgBolt.Postgres/PgBoltOptions.cs`, `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs`, `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`
- Modify (генератор, Steps 8–12): `src/PgBolt.Generators/KnownNames.cs`, `src/PgBolt.Generators/JobGenerator.cs`, `src/PgBolt.Generators/Models/EnqueueCallModel.cs`, `src/PgBolt.Generators/Analysis/AnalysisState.cs`
- Test: `tests/PgBolt.Postgres.Tests/RecurringHostingTests.cs`; `tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs`, `JsonCoverageTests.cs`, `AnalyzerTests.cs`

**Interfaces:**
- Consumes: `RecurringDefinition` (Task 4), `JobMetadataAccess` (Task 3), `SyncRecurringAsync`, `RecurringSyncResult` (Task 8); `IRecurringJobManager` (Task 2) — для генератора.
- Produces:
  - `PgBoltOptions`: `string? RecurringOwner { get; set; }`, `PgBoltOptions AddRecurring<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null)`, `PgBoltOptions AddRecurring(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null)`, `PgBoltOptions ClearRecurring()`; internal `IReadOnlyList<RecurringDeclaration> Recurring`, `bool RecurringCleared`;
  - `internal sealed record RecurringDeclaration(string Id, string Cron, RecurringJobOptions? Options, Func<(string Type, string ArgsJson)> Resolve)`;
  - `internal sealed class RecurringDeclarations(PgBoltOptions options, IHostEnvironment? environment) { bool HasSync; string Owner; IReadOnlyList<RecurringDefinition> Definitions; }` — singleton в DI;
  - `internal static class RecurringSyncLog { static void Write(ILogger logger, string owner, RecurringSyncResult result); }` — Information на каждую запись, Error на коллизию;
  - `PgBoltInitializer` синхронизирует набор до `Ready`, если `HasSync`;
  - генератор: `KnownNames.IsRecurringMethod(IMethodSymbol, INamedTypeSymbol? manager, INamedTypeSymbol? options)` — места вызова `AddOrUpdateAsync<TArgs>` / `AddRecurring<TArgs>` регистрируют `JobMetadata<TArgs>` и проверяются анализатором (BOLT001 / BOLT006), как `EnqueueAsync`.

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/RecurringHostingTests.cs`:

```csharp
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Hosting;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed record UnregisteredRecurringArgs(int N);

internal sealed class TestHostEnvironment(string applicationName) : IHostEnvironment
{
    public string EnvironmentName { get; set; } = "Test";
    public string ApplicationName { get; set; } = applicationName;
    public string ContentRootPath { get; set; } = "/";
    public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
}

public abstract class RecurringHostingTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static JsonElement Args => JsonDocument.Parse("""{"N":1}""").RootElement.Clone();

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private ServiceProvider Build(Action<PgBoltOptions> configure, IHostEnvironment? environment = null, ILogger<PgBoltInitializer>? logger = null)
    {
        var services = new ServiceCollection();
        if (environment is not null)
            services.AddSingleton(environment);
        if (logger is not null)
            services.AddSingleton(logger);
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            configure(o);
        });
        return services.BuildServiceProvider();
    }

    private static Task StartAsync(ServiceProvider sp) => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

    private Task<string[]> IdsAsync(string owner) =>
        _db.ScalarAsync<string[]>("SELECT coalesce(array_agg(id ORDER BY id), '{}') FROM {schema}.recurring_jobs WHERE owner = @o", ("o", owner));

    [Fact]
    public async Task Start_syncs_declarations_before_ready_without_job_types()
    {
        var log = new ListLogger<PgBoltInitializer>();
        await using var sp = Build(o =>
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("h.daily", "hosting.job", "0 3 * * *", Args);   // тип ещё не опубликован исполнителем — старт не падает
        }, logger: log);

        await StartAsync(sp);

        Assert.Equal(new[] { "h.daily" }, await IdsAsync("svc"));
        Assert.True(await _db.ScalarAsync<bool>(
            """SELECT type = 'hosting.job' AND args = '{"N":1}' AND next_fire_at > now() FROM {schema}.recurring_jobs WHERE id = 'h.daily'"""));
        Assert.Contains(log.Entries, e => e.Level == LogLevel.Information && e.Message.Contains("h.daily") && e.Message.Contains("inserted"));
        Assert.NotNull(sp.GetRequiredService<IRecurringJobManager>());
    }

    [Fact]
    public async Task Typed_declaration_uses_job_metadata()
    {
        RecurringTestSupport.EnsureRegistered();
        await using var sp = Build(o =>
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("typed", "0 3 * * *", new RecArgs(5), new RecurringJobOptions { Overlap = OverlapPolicy.Allow });
        });

        await StartAsync(sp);

        Assert.True(await _db.ScalarAsync<bool>(
            """SELECT type = 'rec.job' AND args = '{"N":5}' AND overlap = 1 FROM {schema}.recurring_jobs WHERE id = 'typed'"""));
    }

    [Fact]
    public async Task Default_owner_is_application_name()
    {
        await using var sp = Build(o => o.AddRecurring("a", "hosting.job", "0 3 * * *", Args), new TestHostEnvironment("orders"));
        await StartAsync(sp);
        Assert.Equal(new[] { "a" }, await IdsAsync("orders"));
    }

    [Fact]
    public async Task Missing_owner_fails_start()
    {
        await using var sp = Build(o => o.AddRecurring("a", "hosting.job", "0 3 * * *", Args));
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => StartAsync(sp));
        Assert.Contains("RecurringOwner", e.Message);
    }

    [Fact]
    public async Task Unregistered_typed_args_fail_start()
    {
        await using var sp = Build(o => { o.RecurringOwner = "svc"; o.AddRecurring("a", "0 3 * * *", new UnregisteredRecurringArgs(1)); });
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => StartAsync(sp));
        Assert.Contains("No PgBolt metadata", e.Message);
    }

    [Fact]
    public async Task Clear_deletes_owner_rows_and_absent_declarations_do_not()
    {
        await using (var first = Build(o => { o.RecurringOwner = "svc"; o.AddRecurring("a", "hosting.job", "0 3 * * *", Args); }))
            await StartAsync(first);

        await using (var api = Build(o => o.RecurringOwner = "svc"))           // api-процесс того же владельца без объявлений
            await StartAsync(api);
        Assert.Equal(new[] { "a" }, await IdsAsync("svc"));

        await using (var cleared = Build(o => { o.RecurringOwner = "svc"; o.ClearRecurring(); }))
            await StartAsync(cleared);
        Assert.Empty(await IdsAsync("svc"));
    }

    [Fact]
    public void Invalid_declarations_are_rejected_at_registration()
    {
        Assert.Throws<ArgumentException>(() => Build(o => o.AddRecurring("a", "hosting.job", "bad cron", Args)));
        Assert.Throws<InvalidOperationException>(() => Build(o =>
        {
            o.AddRecurring("a", "hosting.job", "0 3 * * *", Args);
            o.ClearRecurring();
        }));
        Assert.Throws<InvalidOperationException>(() => Build(o =>
        {
            o.AddRecurring("a", "hosting.job", "0 3 * * *", Args);
            o.AddRecurring("a", "hosting.job", "0 4 * * *", Args);
        }));
        Assert.Throws<InvalidOperationException>(() => Build(o => o.RecurringOwner = ""));
    }
}

public sealed class RecurringHostingTests_Direct(PostgresFixture pg) : RecurringHostingTests(pg, ConnectionMode.Direct);
public sealed class RecurringHostingTests_Bouncer(PostgresFixture pg) : RecurringHostingTests(pg, ConnectionMode.Bouncer);
```

(`ListLogger<T>` и `LogEntry` уже объявлены в `HostingTests.cs` той же сборки.)

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringHostingTests"`
Expected: FAIL — нет `PgBoltOptions.AddRecurring`.

- [x] **Step 3: Опции**

`src/PgBolt.Postgres/Recurring/RecurringDeclarations.cs`:

```csharp
using Microsoft.Extensions.Hosting;

namespace PgBolt.Postgres.Recurring;

/// <summary>Объявление AddRecurring: аргументы и имя типа разрешаются при первой синхронизации (метаданные [Job] могут появиться позже).</summary>
internal sealed record RecurringDeclaration(string Id, string Cron, RecurringJobOptions? Options, Func<(string Type, string ArgsJson)> Resolve);

/// <summary>Code-first расписания процесса (часть 4 §4.3, §6.1).</summary>
internal sealed class RecurringDeclarations(PgBoltOptions options, IHostEnvironment? environment)
{
    /// <summary>Процесс синхронизирует набор владельца: есть AddRecurring или ClearRecurring(). RecurringOwner участие не включает.</summary>
    public bool HasSync => options.Recurring.Count > 0 || options.RecurringCleared;

    public string Owner => field ??= options.RecurringOwner
        ?? (environment?.ApplicationName is { Length: > 0 } name
            ? name
            : throw new InvalidOperationException(
                "PgBolt: recurring jobs are declared but RecurringOwner is not set and IHostEnvironment.ApplicationName is unavailable; set PgBoltOptions.RecurringOwner."));

    public IReadOnlyList<RecurringDefinition> Definitions => field ??= options.Recurring
        .Select(d =>
        {
            var (type, json) = d.Resolve();
            return RecurringDefinition.Create(d.Id, type, json, d.Cron, d.Options);
        })
        .ToList();
}
```

В `src/PgBolt.Postgres/PgBoltOptions.cs` добавить `using System.Text.Json; using PgBolt.Postgres.Client; using PgBolt.Postgres.Recurring;`, поле и члены:

```csharp
    private readonly List<RecurringDeclaration> _recurring = [];

    /// <summary>
    /// Владелец code-first расписаний (часть 4 §4.3): все процессы одного владельца объявляют одинаковый набор. По умолчанию —
    /// IHostEnvironment.ApplicationName. Только имя: синхронизацию включают AddRecurring или ClearRecurring().
    /// </summary>
    public string? RecurringOwner { get; set; }

    internal IReadOnlyList<RecurringDeclaration> Recurring => _recurring;
    internal bool RecurringCleared { get; private set; }

    /// <summary>Code-first расписание; синхронизируется на старте и периодически (часть 4 §6). Проверки — сразу, кроме аргументов.</summary>
    /// <remarks>Метаданные TArgs генератор регистрирует по месту этого вызова; аргументы сериализуются при первой синхронизации.</remarks>
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
    public PgBoltOptions AddRecurring(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobName);
        RecurringDefinition.Validate(id, cron, options);
        var json = args.GetRawText();
        _recurring.Add(new RecurringDeclaration(id, cron, options, () => (jobName, json)));
        return this;
    }

    /// <summary>Явно пустой набор: строки владельца удаляются. Несовместим с AddRecurring (часть 4 §4.3).</summary>
    public PgBoltOptions ClearRecurring()
    {
        RecurringCleared = true;
        return this;
    }
```

В конец `Validate()`:

```csharp
        if (RecurringCleared && _recurring.Count > 0)
            throw new InvalidOperationException("PgBolt: ClearRecurring() cannot be combined with AddRecurring.");
        if (_recurring.GroupBy(r => r.Id, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1) is { } duplicate)
            throw new InvalidOperationException($"PgBolt: recurring job '{duplicate.Key}' is declared more than once.");
        if (RecurringOwner is { } owner && (owner.Length == 0 || owner.Length > RecurringDefinition.MaxIdLength))
            throw new InvalidOperationException($"PgBolt: RecurringOwner must be 1..{RecurringDefinition.MaxIdLength} characters.");
```

- [x] **Step 4: Лог синхронизации**

`src/PgBolt.Postgres/Recurring/RecurringSyncLog.cs`:

```csharp
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Recurring;

/// <summary>Часть 4 §7: каждая запись синхронизации — Information, коллизия — Error. Проход без разницы не пишет ничего.</summary>
internal static class RecurringSyncLog
{
    public static void Write(ILogger logger, string owner, RecurringSyncResult result)
    {
        foreach (var id in result.Inserted)
            logger.LogInformation("PgBolt: recurring job {Id} of owner {Owner} inserted.", id, owner);
        foreach (var change in result.Updated)
            logger.LogInformation("PgBolt: recurring job {Id} of owner {Owner} updated ({Fields}).", change.Id, owner, string.Join(", ", change.Fields));
        foreach (var id in result.Deleted)
            logger.LogInformation("PgBolt: recurring job {Id} of owner {Owner} deleted (no longer declared).", id, owner);
        foreach (var id in result.Collisions)
            logger.LogError(
                "PgBolt: recurring job {Id} declared by owner {Owner} belongs to another owner or was created through the API; left untouched.",
                id, owner);
    }
}
```

- [x] **Step 5: Инициализатор и DI**

`PgBoltInitializer`: добавить поля `private readonly PgBoltStore _store; private readonly RecurringDeclarations _recurring;`, параметры конструктора `PgBoltStore store, RecurringDeclarations recurring` перед `ILogger<PgBoltInitializer> logger` и их присваивание; `using PgBolt.Postgres.Recurring; using PgBolt.Postgres.Storage;`. В `InitializeAsync` заменить хвост от `var result = await _partitions.EnsureAsync(...)` до конца метода (проверки `TimedOut` / `TodayPresent` переставлены так, чтобы синхронизация шла после партиций, но до запуска фонового повтора):

```csharp
        var result = await _partitions.EnsureAsync(_options.PartitionDaysAhead, cancellationToken);
        if (result.TimedOut && !result.TodayPresent)
            throw new InvalidOperationException(
                $"PgBolt could not create today's partitions in '{_options.Schema}' within {_options.PartitionLockTimeout} (lock_timeout). " +
                "Enqueue is impossible without them; retry the start when long-running DDL/locks on jobs are gone.");

        // Часть 4 §6.1: после миграций и партиций, до Ready; ошибка — падение старта. До запуска фонового повтора партиций:
        // иначе при ошибке здесь он остался бы запущенным (хост не вызывает StopAsync после ошибки StartAsync).
        // Категория лога — PgBolt.Postgres.Hosting.PgBoltInitializer: под фильтр «PgBolt.Postgres» (§7) попадает по префиксу.
        if (_recurring.HasSync)
            RecurringSyncLog.Write(_logger, _recurring.Owner,
                await _store.SyncRecurringAsync(_recurring.Owner, _recurring.Definitions, _logger, cancellationToken));

        if (!result.TimedOut)
            return;
        _logger.LogWarning("PgBolt: future partitions were not created (lock_timeout); retrying in background.");
        _stop = new CancellationTokenSource();
        BackgroundRetry = RetryAsync(_stop.Token);
```

`PgBoltServiceCollectionExtensions.AddPgBolt`: перед регистрацией `PgBoltInitializer` добавить

```csharp
        services.AddSingleton(sp => new RecurringDeclarations(options, sp.GetService<IHostEnvironment>()));
```

и передать в конструктор инициализатора `sp.GetRequiredService<PgBoltStore>(), sp.GetRequiredService<RecurringDeclarations>()` перед логгером; `using PgBolt.Postgres.Recurring;`.

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringHostingTests|FullyQualifiedName~HostingTests"`
Expected: PASS.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres tests/PgBolt.Postgres.Tests/RecurringHostingTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(recurring): code-first declarations synced on startup"
```

- [x] **Step 8: Генератор — тесты типизированных recurring-вызовов**

Генератор регистрирует `JobMetadata<TArgs>` (дескриптор и `JsonTypeInfo`) только для [Job]-типов своей сборки, исполнителей и мест вызова `IJobClient.EnqueueAsync` / `EnqueueManyAsync`. Продюсер без исполнителя, который объявляет расписание через `AddRecurring<TArgs>` / `AddOrUpdateAsync<TArgs>` для контракта из другой сборки, получил бы «No PgBolt metadata». Эти вызовы становятся такими же местами вызова — и для генератора, и для анализатора (BOLT001 / BOLT006).

В `tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs` добавить (тесты генератора не подключают `PgBolt.Postgres`, поэтому `PgBoltOptions` — заглушка в исходнике: генератор сверяет только имя типа и метода; `IRecurringJobManager` — настоящий, из `PgBolt.Abstractions`, Task 2):

```csharp
    /// <summary>Заглушка PgBolt.Postgres.PgBoltOptions с перегрузками AddRecurring (часть 4 §4.3).</summary>
    internal const string OptionsStub = """
        namespace PgBolt.Postgres
        {
            public sealed class PgBoltOptions
            {
                public PgBoltOptions AddRecurring<TArgs>(string id, string cron, TArgs args, PgBolt.RecurringJobOptions? options = null) => this;
                public PgBoltOptions AddRecurring(string id, string jobName, string cron, System.Text.Json.JsonElement args,
                    PgBolt.RecurringJobOptions? options = null) => this;
            }
        }
        """;

    [Fact]
    public void Typed_recurring_call_sites_register_types_from_referenced_assemblies()   // часть 4: продюсер без исполнителя
    {
        var contracts = GeneratorHarness.Library("""
            namespace Contracts;

            [PgBolt.Job("a.job")] public sealed record AArgs(int X);
            [PgBolt.Job("b.job")] public sealed record BArgs(int X);
            [PgBolt.Job("c.job")] public sealed record CArgs(int X);
            [PgBolt.Job("d.job")] public sealed record DArgs(int X);
            """, "Contracts");

        var (driver, output) = GeneratorHarness.Run($$"""
            using System.Text.Json;
            using System.Threading.Tasks;
            using Contracts;
            using PgBolt;

            [assembly: PgBoltJsonContext(typeof(AppJson))]

            public static class Producer
            {
                public static void Configure(PgBolt.Postgres.PgBoltOptions o) =>
                    o.AddRecurring("a", "0 3 * * *", new AArgs(1))
                     .AddRecurring("d", "d.job", "* * * * *", default(JsonElement));        // нетипизированная — метаданные не нужны

                public static async Task ScheduleAsync(IRecurringJobManager manager, JsonElement raw)
                {
                    await manager.AddOrUpdateAsync("b", "*/5 * * * *", new BArgs(1));
                    await manager.AddOrUpdateAsync("c", "c.job", "* * * * *", raw);            // нетипизированная — метаданные не нужны
                }
            }

            {{OptionsStub}}

            [System.Text.Json.Serialization.JsonSerializable(typeof(AArgs))]
            [System.Text.Json.Serialization.JsonSerializable(typeof(BArgs))]
            {{HandWrittenContext}}
            """, library: contracts);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Contracts.AArgs>(new global::PgBolt.JobDescriptor("a.job", "default", 10, (short)0, null), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::Contracts.AArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::Contracts.AArgs))!);""", code);
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Contracts.BArgs>(new global::PgBolt.JobDescriptor("b.job", "default", 10, (short)0, null), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::Contracts.BArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::Contracts.BArgs))!);""", code);
        Assert.DoesNotContain("CArgs", code);
        Assert.DoesNotContain("DArgs", code);
        GeneratorHarness.AssertClean(output);
    }

    [Fact]
    public void Type_parameter_recurring_call_sites_are_ignored()
    {
        var (driver, _) = GeneratorHarness.Run($$"""
            using System.Threading.Tasks;
            using PgBolt;

            public static class Producer
            {
                public static ValueTask ScheduleAsync<T>(IRecurringJobManager manager, T args) => manager.AddOrUpdateAsync("x", "* * * * *", args);
                public static void Configure<T>(PgBolt.Postgres.PgBoltOptions o, T args) => o.AddRecurring("y", "* * * * *", args);
            }

            {{OptionsStub}}
            """);

        Assert.Empty(GeneratorHarness.HintNames(driver));
    }
```

В `tests/PgBolt.Generators.Tests/JsonCoverageTests.cs` добавить:

```csharp
    [Fact]
    public Task Recurring_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public static class S
        {
            public static async Task Go(IRecurringJobManager m) => await {|#0:m.AddOrUpdateAsync("b", "* * * * *", new B(1))|};
            public static async Task Ok(IRecurringJobManager m) => await m.AddOrUpdateAsync("a", "* * * * *", new A(1));
        }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));
```

В `tests/PgBolt.Generators.Tests/AnalyzerTests.cs` добавить:

```csharp
    [Fact]
    public Task Code_first_recurring_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public static class S { public static void Go(PgBolt.Postgres.PgBoltOptions o) => o.AddRecurring("a", "* * * * *", new A(1)); }
        """ + MetadataGenerationTests.OptionsStub,
        Diagnostic(Diagnostics.MissingJsonContext));
```

- [x] **Step 9: Запустить — падают**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: FAIL — `Typed_recurring_call_sites_register_types_from_referenced_assemblies` (нет `AArgs`/`BArgs` в выводе), `Recurring_args_missing_in_context_BOLT001`, `Code_first_recurring_without_context_BOLT006` (нет диагностик); `Type_parameter_recurring_call_sites_are_ignored` проходит и до изменения.

- [x] **Step 10: Генератор и анализатор**

`src/PgBolt.Generators/KnownNames.cs` — константы и предикат рядом с `IsClientMethod`:

```csharp
        public const string RecurringManagerMetadata = "PgBolt.IRecurringJobManager";
        public const string OptionsMetadata = "PgBolt.Postgres.PgBoltOptions";

        /// <summary>
        /// Часть 4: типизированные AddOrUpdateAsync&lt;TArgs&gt; / AddRecurring&lt;TArgs&gt; берут JsonTypeInfo из JobMetadata&lt;TArgs&gt;,
        /// как EnqueueAsync, — места их вызова тоже регистрируют метаданные (продюсер без исполнителя).
        /// </summary>
        public static bool IsRecurringMethod(IMethodSymbol method, INamedTypeSymbol? manager, INamedTypeSymbol? options) =>
            method.IsGenericMethod
            && ((method.Name == "AddOrUpdateAsync" && manager is not null
                    && (SymbolEqualityComparer.Default.Equals(method.ContainingType, manager)
                        || method.ContainingType.AllInterfaces.Contains(manager, SymbolEqualityComparer.Default)))
                || (method.Name == "AddRecurring" && options is not null
                    && SymbolEqualityComparer.Default.Equals(method.ContainingType, options)));
```

`src/PgBolt.Generators/JobGenerator.cs` — в предикате `calls` расширить оба списка имён:

```csharp
                        Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "EnqueueAsync" or "EnqueueManyAsync" or "AddOrUpdateAsync" or "AddRecurring" }
                            or MemberBindingExpressionSyntax { Name.Identifier.ValueText: "EnqueueAsync" or "EnqueueManyAsync" or "AddOrUpdateAsync" or "AddRecurring" },
```

`src/PgBolt.Generators/Models/EnqueueCallModel.cs` — тело `From` заменить:

```csharp
            if (ctx.SemanticModel.GetSymbolInfo(ctx.Node, ct).Symbol is not IMethodSymbol method)
                return null;
            var compilation = ctx.SemanticModel.Compilation;
            var client = compilation.GetTypeByMetadataName(KnownNames.JobClientMetadata);
            var callSite = (client is not null && KnownNames.IsClientMethod(method, client))
                || KnownNames.IsRecurringMethod(method, compilation.GetTypeByMetadataName(KnownNames.RecurringManagerMetadata),
                    compilation.GetTypeByMetadataName(KnownNames.OptionsMetadata));
            if (!callSite)
                return null;
            return method.TypeArguments[0] is INamedTypeSymbol args ? JobTypeModel.FromType(args) : null;
```

и дописать к классу summary: `/// <summary>Места вызова, требующие метаданных TArgs: EnqueueAsync / EnqueueManyAsync и типизированные recurring-вызовы (часть 4).</summary>`.

`src/PgBolt.Generators/Analysis/AnalysisState.cs`:
- поля `private readonly INamedTypeSymbol? _recurringManager; private readonly INamedTypeSymbol? _options;` — последние параметры конструктора и присваивание; в `TryCreate` передать `compilation.GetTypeByMetadataName(KnownNames.RecurringManagerMetadata)`, `compilation.GetTypeByMetadataName(KnownNames.OptionsMetadata)` (необязательны: без `PgBolt.Postgres` анализатор работает как раньше);
- в `AnalyzeInvocation` условие заменить на:

```csharp
            if (!KnownNames.IsClientMethod(method, _jobClient) && !KnownNames.IsRecurringMethod(method, _recurringManager, _options))
                return;
```

Текст BOLT006 («executors or enqueue calls») не менять: recurring-вызов — та же постановка задачи; правила в `AnalyzerReleases.*.md` не меняются.

- [x] **Step 11: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Generators.Tests` и `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RecurringHostingTests|FullyQualifiedName~HostingTests"`
Expected: PASS; снапшоты Verify не меняются (вывод для прежних входов тот же).

- [x] **Step 12: Commit**

```bash
git add src/PgBolt.Generators tests/PgBolt.Generators.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(generators): register metadata for typed recurring call sites"
```

---
### Task 10: Циклы `scheduler` и `recurring-sync` в `PgBolt.Server`

**Files:**
- Modify: `src/PgBolt.Server/PgBoltProcessingOptions.cs`, `src/PgBolt.Server/Runtime/IServerStore.cs`, `src/PgBolt.Server/Runtime/PgBoltServerStore.cs`, `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs`, `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`
- Modify (тесты): `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`, `tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs`, `tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs`, `tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs`, `tests/PgBolt.Server.Tests/Unit/HousekeepingTests.cs`, `tests/PgBolt.Server.Tests/Integration/ServerTestHost.cs`, `tests/PgBolt.Server.Tests/Integration/LeaseFailureTests.cs`, `tests/PgBolt.Server.Tests/Integration/ShutdownTests.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/SchedulerLoopTests.cs`

**Interfaces:**
- Consumes: `TickRecurringAsync`, `RecurringTickResult` (Task 7); `SyncRecurringAsync`, `RecurringSyncResult` (Task 8); `RecurringDeclarations`, `RecurringSyncLog` (Task 9); `RecurringPlanner.RowErrorDelay` (Task 5).
- Produces:
  - `PgBoltProcessingOptions`: `bool RunScheduler = true`, `TimeSpan SchedulerInterval = 1 с`, `TimeSpan? MisfireThreshold = null`, `TimeSpan RecurringSyncInterval = 1 мин`, `internal TimeSpan EffectiveMisfireThreshold`;
  - `IServerStore.TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct) : Task<RecurringTickResult>`, `IServerStore.SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger, CancellationToken ct) : Task<RecurringSyncResult>` (логгер — для Warning о повторе при `40P01`, Task 8);
  - `ProcessingMaintenance(…, ILogger logger, RecurringDeclarations? recurring = null)`: `internal const int SchedulerBatchSize = 100`, `internal const int SchedulerMaxFires = 1000`, `internal Task<int> TickOnceAsync(CancellationToken ct)`, `internal Task<int> SyncOnceAsync(CancellationToken ct)`; циклы: promoter/sweeper/housekeeping — при `RunMaintenance`, `scheduler` — при `RunScheduler`, `recurring-sync` — при `recurring.HasSync`;
  - `PgBoltProcessingService` запускает `ProcessingMaintenance`, если есть серверы и `RunMaintenance || RunScheduler || HasSync`.

- [x] **Step 1: Фейк хранилища**

В `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs` добавить `using Microsoft.Extensions.Logging; using PgBolt.Postgres.Recurring;` и:

```csharp
    public Func<int, int, TimeSpan, CancellationToken, Task<RecurringTickResult>> OnTick { get; set; } =
        (_, _, _, _) => Task.FromResult(RecurringTickResult.Empty);

    public Func<string, IReadOnlyList<RecurringDefinition>, CancellationToken, Task<RecurringSyncResult>> OnSync { get; set; } =
        (_, _, _) => Task.FromResult(new RecurringSyncResult([], [], [], []));

    public Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Tick", Detail: (batchSize, maxFires, misfireThreshold)));
        return OnTick(batchSize, maxFires, misfireThreshold, ct);
    }

    public Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger,
        CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Sync", Detail: owner));
        return OnSync(owner, definitions, ct);
    }
```

- [x] **Step 2: Тесты**

`tests/PgBolt.Server.Tests/Unit/SchedulerLoopTests.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class SchedulerLoopTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly ListLoggerFactory _logs = new();
    private readonly PgBoltProcessingOptions _options = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create(RecurringDeclarations? recurring = null) =>
        new(_store, _wakeup, _options, new FakeMaintenance(), new PgBoltOptions(), new MonoClock(_time), _logs.CreateLogger("test"), recurring)
        {
            NextRandom = () => 0.5,
        };

    private static RecurringDeclarations Cleared(string owner) =>
        new(new PgBoltOptions { RecurringOwner = owner }.ClearRecurring(), environment: null);

    private static RecurringTickResult Tick(int fired, bool full, params string[] queues) =>
        RecurringTickResult.Empty with { Claimed = fired, Fired = fired, Full = full, Queues = queues };

    [Fact]
    public async Task Tick_repeats_while_full_and_signals_queues()
    {
        var signals = new ConcurrentQueue<string>();
        foreach (var q in new[] { "a", "b" })
            _wakeup.Subscribe(q, () => signals.Enqueue(q));
        var results = new Queue<RecurringTickResult>([Tick(100, true, "a"), Tick(3, false, "b")]);
        _store.OnTick = (_, _, _, _) => Task.FromResult(results.Dequeue());

        Assert.Equal(103, await Create().TickOnceAsync(Ct));

        Assert.Equal(["a", "b"], signals);
        Assert.All(_store.CallsOf("Tick"), c => Assert.Equal((100, 1000, TimeSpan.FromMinutes(1)), ((int, int, TimeSpan))c.Detail!));
    }

    [Fact]
    public async Task Tick_logs_row_errors_and_missed_fires()
    {
        var from = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        _store.OnTick = (_, _, _, _) => Task.FromResult(RecurringTickResult.Empty with
        {
            Claimed = 3,
            Errors = [new RecurringRowError("broken", "TimeZoneNotFoundException: Mars/Olympus")],
            Missed = [new RecurringMissed("stale", from, from.AddHours(5))],
            OverlapSkipped = ["busy"],
        });

        await Create().TickOnceAsync(Ct);

        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("broken") && e.Message.Contains("Mars/Olympus"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("stale"));
        Assert.Contains(_logs.At(LogLevel.Debug), e => e.Message.Contains("busy"));
    }

    [Fact]
    public void Effective_misfire_threshold_follows_scheduler_interval()
    {
        Assert.Equal(TimeSpan.FromMinutes(1), new PgBoltProcessingOptions().EffectiveMisfireThreshold);
        Assert.Equal(TimeSpan.FromSeconds(75), new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.FromSeconds(15) }.EffectiveMisfireThreshold);
        Assert.Equal(TimeSpan.FromMinutes(3), new PgBoltProcessingOptions { MisfireThreshold = TimeSpan.FromMinutes(3) }.EffectiveMisfireThreshold);
    }

    [Fact]
    public async Task Sync_once_logs_every_write_and_collisions()
    {
        _store.OnSync = (_, _, _) => Task.FromResult(new RecurringSyncResult(
            ["a"], [new RecurringFieldChange("b", ["cron"])], ["c"], ["d"]));

        Assert.Equal(3, await Create(Cleared("svc")).SyncOnceAsync(Ct));

        Assert.Equal("svc", Assert.Single(_store.CallsOf("Sync")).Detail);
        Assert.Equal(3, _logs.At(LogLevel.Information).Count(e => e.Message.Contains("of owner svc")));
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("recurring job d declared by owner svc"));
    }

    [Fact]
    public async Task Without_maintenance_only_scheduler_runs()
    {
        _options.RunMaintenance = false;
        var maintenance = Create();

        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Tick").Any(), "scheduler ticks");
        await maintenance.StopAsync();

        Assert.Empty(_store.CallsOf("Promote"));
        Assert.Empty(_store.CallsOf("Sweep"));
        Assert.Empty(_store.CallsOf("Claim"));
        Assert.Empty(_store.CallsOf("Sync"));
    }

    [Fact]
    public async Task Declarations_start_sync_loop_even_without_scheduler()
    {
        _options.RunMaintenance = false;
        _options.RunScheduler = false;
        var maintenance = Create(Cleared("svc"));

        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Sync").Any(), "recurring-sync runs");
        await maintenance.StopAsync();

        Assert.Empty(_store.CallsOf("Tick"));
    }
}
```

`tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs` — в `Defaults_match_spec` после проверок `p` добавить:

```csharp
        Assert.True(p.RunScheduler);
        Assert.Equal(TimeSpan.FromSeconds(1), p.SchedulerInterval);
        Assert.Null(p.MisfireThreshold);
        Assert.Equal(TimeSpan.FromMinutes(1), p.RecurringSyncInterval);
```

и новый тест:

```csharp
    [Fact]
    public void Explicit_misfire_threshold_must_cover_five_scheduler_intervals()
    {
        new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.FromSeconds(15) }.Validate();   // умолчание подстраивается
        Assert.Throws<InvalidOperationException>(() =>
            new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.FromSeconds(15), MisfireThreshold = TimeSpan.FromMinutes(1) }.Validate());
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.Zero }.Validate());
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { RecurringSyncInterval = TimeSpan.Zero }.Validate());
    }
```

`tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs` — в `Maintenance_runs_only_when_enabled` заменить `ConfigurePgBoltProcessing(p => p.RunMaintenance = false)` на `ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; })` и добавить тест:

```csharp
    [Fact]
    public async Task Scheduler_runs_without_maintenance()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => p.RunMaintenance = false);

        var service = await StartAsync();

        Assert.NotNull(service.Maintenance);
        await Eventually.True(() => _store.CallsOf("Tick").Any(), "scheduler ticks");
        Assert.Empty(_store.CallsOf("Promote"));
    }
```

**Существующие тесты циклов обслуживания (B1).** С `RunScheduler = true` по умолчанию `ProcessingMaintenance.Start()` запускает четвёртый цикл `scheduler`; с `FakeTimeProvider` он тоже встаёт на паузу, и `IdleWaits` быстро проходит 3 и останавливается на 4. Ожидания `IdleWaits == 3` («promoter, sweeper and housekeeping are waiting») опрашиваются раз в 1 мс и почти всегда проскакивают значение 3 — таймаут `Eventually` (5 с). Эти тесты проверяют только promoter / sweeper / housekeeping, поэтому тикер в них выключается, ожидания не меняются:

- `tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs` — заменить
  ```csharp
      private readonly PgBoltProcessingOptions _options = new();
  ```
  на
  ```csharp
      // Только promoter / sweeper / housekeeping: IdleWaits == 3 считает их паузы, тикер (RunScheduler, часть 4) — в SchedulerLoopTests.
      private readonly PgBoltProcessingOptions _options = new() { RunScheduler = false };
  ```
  (затрагивает `Loop_error_is_logged_and_retried_after_interval` и `Stop_ends_both_loops`);
- `tests/PgBolt.Server.Tests/Unit/HousekeepingTests.cs` — та же замена `_options` (затрагивает `Claim_failure_is_logged_by_the_loop_and_retried_after_poll_interval`).

**Интеграционные сценарии без фонового трафика.** Тесты, которые выключают `RunMaintenance`, чтобы в сценарии не было фоновых обращений к БД (пул на 2 соединения у `PoolHogJob`, прокси на паузе), теперь получили бы тикер раз в секунду. Тестовому хосту — отдельный флаг, сценариям — выключенный тикер:

- `tests/PgBolt.Server.Tests/Integration/ServerTestHost.cs`: в `TestHostSettings` после `public bool RunMaintenance { get; set; } = true;` добавить
  ```csharp
      /// <summary>PgBoltProcessingOptions.RunScheduler (часть 4); сценарии без фонового трафика выключают его вместе с RunMaintenance.</summary>
      public bool RunScheduler { get; set; } = true;
  ```
  и в `ConfigurePgBoltProcessing` после `p.RunMaintenance = settings.RunMaintenance;` — `p.RunScheduler = settings.RunScheduler;`;
- `tests/PgBolt.Server.Tests/Integration/LeaseFailureTests.cs` (`Dequeue_completing_after_heartbeat_loss_releases_its_jobs_without_running_them`, `Job_exhausting_the_pool_is_cancelled_by_HeartbeatLost_and_fails_after_max_attempts`) и `tests/PgBolt.Server.Tests/Integration/ShutdownTests.cs` (`Jobs_returned_by_inflight_dequeue_after_stop_signal_are_released_without_spending_attempts`): после строки `h.RunMaintenance = false;` добавить `h.RunScheduler = false;`.

- [x] **Step 3: Запустить — не компилируется**

Run: `dotnet build PgBolt.slnx`
Expected: FAIL — `FakeServerStore` реализует методы, которых нет в `IServerStore`; нет `RunScheduler` (в том числе в инициализаторах `_options` `ProcessingMaintenanceTests` / `HousekeepingTests`).

- [x] **Step 4: Опции**

`src/PgBolt.Server/PgBoltProcessingOptions.cs` — добавить свойства:

```csharp
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

    internal TimeSpan EffectiveMisfireThreshold =>
        MisfireThreshold ?? TimeSpan.FromTicks(Math.Max(TimeSpan.FromMinutes(1).Ticks, (5 * SchedulerInterval).Ticks));
```

в `Validate()`:

```csharp
        Check(SchedulerInterval, nameof(SchedulerInterval));
        Check(RecurringSyncInterval, nameof(RecurringSyncInterval));
        if (MisfireThreshold is { } threshold && threshold < 5 * SchedulerInterval)
            throw new InvalidOperationException(
                $"PgBolt processing: MisfireThreshold must be >= 5 × SchedulerInterval ({5 * SchedulerInterval}).");
```

- [x] **Step 5: `IServerStore`**

`IServerStore.cs` (+ `using Microsoft.Extensions.Logging; using PgBolt.Postgres.Recurring;`):

```csharp
    Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct);
    Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger, CancellationToken ct);
```

`PgBoltServerStore.cs` (+ `using Microsoft.Extensions.Logging; using PgBolt.Postgres.Recurring;`):

```csharp
    public Task<RecurringTickResult> TickRecurringAsync(int batchSize, int maxFires, TimeSpan misfireThreshold, CancellationToken ct) =>
        _store.TickRecurringAsync(batchSize, maxFires, misfireThreshold, ct);

    public Task<RecurringSyncResult> SyncRecurringAsync(string owner, IReadOnlyList<RecurringDefinition> definitions, ILogger logger,
        CancellationToken ct) =>
        _store.SyncRecurringAsync(owner, definitions, logger, ct);
```

- [x] **Step 6: `ProcessingMaintenance`**

Первичный конструктор — добавить последний параметр `RecurringDeclarations? recurring = null` (+ `using PgBolt.Postgres.Recurring;` и `using PgBolt.Postgres.Storage;` — для `RecurringTickResult` в `LogTick`); summary класса дополнить: «тикер recurring и синхронизация code-first (часть 4 §5.1, §6.1)». Заменить `Start()`:

```csharp
    internal const int SchedulerBatchSize = 100;
    internal const int SchedulerMaxFires = 1000;

    public void Start()
    {
        var loops = new List<Task>();
        if (options.RunMaintenance)
        {
            loops.Add(Task.Run(() => LoopAsync("promoter", PromoteOnceAsync, options.PromoterInterval)));
            loops.Add(Task.Run(() => LoopAsync("sweeper", SweepOnceAsync, options.SweeperInterval)));
            loops.Add(Task.Run(() => LoopAsync("housekeeping", HousekeepingOnceAsync, options.MaintenancePollInterval)));
        }
        if (options.RunScheduler)
            loops.Add(Task.Run(() => LoopAsync("scheduler", TickOnceAsync, options.SchedulerInterval)));
        if (recurring is { HasSync: true })
            loops.Add(Task.Run(() => LoopAsync("recurring-sync", SyncOnceAsync, options.RecurringSyncInterval)));
        _loops = Task.WhenAll(loops);
    }
```

и добавить методы:

```csharp
    /// <summary>Часть 4 §5.1: транзакции тика подряд, пока пакет полный; очереди поставленных задач получают сигнал.</summary>
    internal async Task<int> TickOnceAsync(CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var result = await store.TickRecurringAsync(SchedulerBatchSize, SchedulerMaxFires, options.EffectiveMisfireThreshold, ct);
            total += result.Fired;
            foreach (var queue in result.Queues)
                wakeup.Signal(queue);
            LogTick(result);
            if (!result.Full)
                return total;
        }
    }

    /// <summary>Часть 4 §6.1: периодическая синхронизация набора владельца (сходимость после rolling deploy).</summary>
    internal async Task<int> SyncOnceAsync(CancellationToken ct)
    {
        var declarations = recurring ?? throw new InvalidOperationException("PgBolt: recurring declarations are not registered.");
        var result = await store.SyncRecurringAsync(declarations.Owner, declarations.Definitions, logger, ct);
        RecurringSyncLog.Write(logger, declarations.Owner, result);
        return result.Writes;
    }

    /// <summary>Часть 4 §7: ошибка строки — Error (не чаще раза в минуту: wake_at), пропуск misfire и конец срабатываний — Warning, наложение и ожидание — Debug.</summary>
    private void LogTick(RecurringTickResult result)
    {
        foreach (var error in result.Errors)
            logger.LogError("PgBolt: recurring job {Id} was not fired: {Error}; retrying in {Delay}.", error.Id, error.Error,
                RecurringPlanner.RowErrorDelay);
        foreach (var missed in result.Missed)
            logger.LogWarning("PgBolt: recurring job {Id} skipped fires from {From:O} to {To:O} by its misfire policy.",
                missed.Id, missed.FromUtc, missed.ToUtc);
        foreach (var id in result.OverlapSkipped)
            logger.LogDebug("PgBolt: recurring job {Id} fire skipped: previous run is still active.", id);
        foreach (var id in result.Waiting)
            logger.LogDebug("PgBolt: recurring job {Id} waits for its previous run (catch-up).", id);
        foreach (var id in result.Exhausted)
            logger.LogWarning("PgBolt: recurring job {Id} has no future occurrences and will not fire again.", id);
    }
```

- [x] **Step 7: `PgBoltProcessingService`**

(+ `using PgBolt.Postgres.Recurring;`) заменить условие запуска обслуживания:

```csharp
            var recurring = _services.GetService<RecurringDeclarations>();
            if (_servers.Count > 0 && (processing.RunMaintenance || processing.RunScheduler || recurring is { HasSync: true }))
            {
                Maintenance = new ProcessingMaintenance(store, wakeup, processing, _services.GetRequiredService<IPgBoltMaintenance>(),
                    core, clock, _logger, recurring);
                Maintenance.Start();
            }
```

Сообщение в `StopAsync` «while stopping promoter/sweeper» заменить на «while stopping maintenance loops».

- [x] **Step 8: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Unit"`
Expected: PASS (включая существующие `ProcessingMaintenanceTests` и `HousekeepingTests` с `RunScheduler = false` из Step 2 — `IdleWaits == 3` по-прежнему считает ровно promoter, sweeper и housekeeping; `ProcessingServiceTests` с правкой `Maintenance_runs_only_when_enabled`).
Затем: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~LeaseFailureTests|FullyQualifiedName~ShutdownTests"` — PASS (сценарии с `h.RunScheduler = false`).

- [x] **Step 9: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): scheduler and recurring sync loops"
```

---
### Task 11: Интеграция с сервером и E2E

**Files:**
- Modify: `tests/PgBolt.Server.Tests/Integration/TestJobs.cs` (`JobRun`: поля расписания)
- Test: `tests/PgBolt.Server.Tests/Integration/RecurringTests.cs`, `tests/E2E/PgBolt.E2E.Tests/RecurringEndToEndTests.cs`

**Interfaces:**
- Consumes: всё из Task 1–10; `ServerIntegrationTestBase.StartHostAsync`, `WaitUntilAsync`, `TestHostSettings.Core/Processing/RunMaintenance`, `JobProbe.Runs(JobId)`.
- Produces: `JobRun.ScheduledAt : DateTimeOffset?`, `JobRun.RecurringJobId : string?` (тестовая инфраструктура).

- [x] **Step 1: `JobRun` фиксирует поля расписания**

В `TestJobs.cs` в класс `JobRun` добавить:

```csharp
    public DateTimeOffset? ScheduledAt { get; init; }
    public string? RecurringJobId { get; init; }
```

и в `JobProbe.RunAsync` создание прогона заменить на:

```csharp
        var run = new JobRun(context.JobId.Value, context.Attempt, host.Name)
        {
            ScheduledAt = context.ScheduledAt,
            RecurringJobId = context.RecurringJobId,
        };
```

- [x] **Step 2: Интеграционные тесты сервера**

`tests/PgBolt.Server.Tests/Integration/RecurringTests.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class RecurringTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    /// <summary>Code-first расписание «каждую секунду», тикер раз в 200 мс.</summary>
    private static void EverySecond(TestHostSettings s)
    {
        s.Core = o =>
        {
            o.RecurringOwner = "svc";
            o.AddRecurring("tick", "* * * * * *", new SuccessArgs(1),
                new RecurringJobOptions { Misfire = MisfirePolicy.CatchUp, Overlap = OverlapPolicy.Allow });
        };
        s.Processing = p => p.SchedulerInterval = TimeSpan.FromMilliseconds(200);
    }

    private Task<long> SucceededAsync(string id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE recurring_id = @id AND state = @s", ("id", id), ("s", DbState.Succeeded));

    [Fact]
    public async Task Code_first_schedule_fires_and_executor_sees_schedule_fields()
    {
        await StartHostAsync(EverySecond);

        await WaitUntilAsync(async () => await SucceededAsync("tick") >= 2, Timeout, "two scheduled runs succeed");

        var jobId = await Db.ScalarAsync<Guid>("SELECT id FROM {schema}.jobs WHERE recurring_id = 'tick' AND state = 2 ORDER BY id LIMIT 1");
        var scheduledAt = await Db.ScalarAsync<DateTime>("SELECT scheduled_at FROM {schema}.jobs WHERE id = @id", ("id", jobId));
        var run = Assert.Single(Probe.Runs(new JobId(jobId)));
        Assert.Equal("tick", run.RecurringJobId);
        Assert.Equal(new DateTimeOffset(scheduledAt, TimeSpan.Zero), run.ScheduledAt);
    }

    [Fact]
    public async Task Two_hosts_fire_each_second_once()
    {
        await StartHostAsync(s => { s.Name = "a"; EverySecond(s); });
        await StartHostAsync(s => { s.Name = "b"; EverySecond(s); });

        await WaitUntilAsync(async () => await SucceededAsync("tick") >= 4, Timeout, "four scheduled runs succeed");

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT count(*) = count(DISTINCT scheduled_at)
                   AND max(scheduled_at) - min(scheduled_at) = (count(*) - 1) * interval '1 second'
            FROM {schema}.jobs WHERE recurring_id = 'tick'
            """));
    }

    [Fact]
    public async Task Scheduler_fires_without_maintenance()
    {
        await StartHostAsync(s => { EverySecond(s); s.RunMaintenance = false; });

        await WaitUntilAsync(async () => await SucceededAsync("tick") >= 1, Timeout, "scheduled run succeeds");
    }

    [Fact]
    public async Task Api_schedule_is_fired_by_server()
    {
        var host = await StartHostAsync(s => s.Processing = p => p.SchedulerInterval = TimeSpan.FromMilliseconds(200));
        var manager = host.Services.GetRequiredService<IRecurringJobManager>();

        await manager.AddOrUpdateAsync("api", "* * * * * *", new SuccessArgs(2), new RecurringJobOptions { Overlap = OverlapPolicy.Allow }, Ct);

        await WaitUntilAsync(async () => await SucceededAsync("api") >= 1, Timeout, "API schedule runs");
        Assert.True((await manager.GetAsync("api", Ct))!.LastJobId is not null);
    }
}

public sealed class RecurringTests_Direct(PostgresFixture pg) : RecurringTests(pg, ConnectionMode.Direct);
public sealed class RecurringTests_Bouncer(PostgresFixture pg) : RecurringTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 3: E2E со сгенерированными метаданными**

`tests/E2E/PgBolt.E2E.Tests/RecurringEndToEndTests.cs`:

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.E2E.Contracts;
using PgBolt.E2E.JobsA;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

public abstract class RecurringEndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Typed_code_first_schedule_reaches_executor_with_schedule_fields()
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register);
            o.RecurringOwner = "e2e";
            o.AddRecurring("e2e.ping.daily", "0 3 * * *", new PingArgs("scheduled"));
        });
        await using var sp = services.BuildServiceProvider();
        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);
        var store = sp.GetRequiredService<PgBoltStore>();
        var registry = sp.GetRequiredService<JobRegistry>();
        await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), "e2e-server", Ct);
        await _db.ExecAsync("UPDATE {schema}.recurring_jobs SET next_fire_at = '2026-01-01 03:00:00+00', wake_at = now()");

        Assert.Equal(1, (await store.TickRecurringAsync(100, 1000, TimeSpan.FromMinutes(1), Ct)).Fired);

        var job = Assert.Single(await store.DequeueAsync("e2e", 10, Guid.NewGuid(), TimeSpan.FromMinutes(1), Ct));
        Assert.Equal("e2e.ping.daily", job.RecurringId);
        Assert.Equal(await _db.ScalarAsync<DateTime>("SELECT last_fire_at FROM {schema}.recurring_jobs"), job.ScheduledAtUtc);
        var info = await sp.GetRequiredService<IRecurringJobManager>().GetAsync("e2e.ping.daily", Ct);   // §10: GetAsync в E2E
        Assert.NotNull(info);
        Assert.Equal(new DateTimeOffset(job.ScheduledAtUtc!.Value, TimeSpan.Zero), info.LastFireAt);
        Assert.Equal(new JobId(job.Id), info.LastJobId);
        Assert.Equal("e2e", info.Owner);
        Assert.True(registry.TryGet(job.Type, out var executor));
        await using var scope = sp.CreateAsyncScope();
        var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts, null, null, NullLogger.Instance,
            new DateTimeOffset(job.ScheduledAtUtc!.Value, TimeSpan.Zero), job.RecurringId);
        await executor.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes(job.ArgsJson), context, Ct);
        Assert.Contains("scheduled", PingProbe.Messages);
    }
}

public sealed class RecurringEndToEndTests_Direct(PostgresFixture pg) : RecurringEndToEndTests(pg, ConnectionMode.Direct);
public sealed class RecurringEndToEndTests_Bouncer(PostgresFixture pg) : RecurringEndToEndTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 4: Запустить**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~RecurringTests"` и `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~RecurringEndToEndTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(recurring): server integration and end-to-end scenarios"
```

---

### Task 12: Документация и полный прогон

**Files:**
- Modify: `CHANGELOG.md`, `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md`, `docs/superpowers/specs/2026-09-30-pgbolt-scheduler-design.md` (строка статуса)

- [x] **Step 1: CHANGELOG**

В `CHANGELOG.md` в раздел `## Unreleased`:
- в `### Breaking changes` добавить пункт:

```markdown
- Миграции объединены в одну начальную `V001` (включая `maintenance_tasks` и объекты части 4); `PgBoltMigrations.LatestVersion = 1`.
  Схемы, созданные предыдущими сборками, не обновляются: старт падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- `EnqueueOptions.UniqueKey` и результаты `uniqueKeySelector` с префиксом `pgbolt:` отклоняются (`ArgumentException`): префикс
  зарезервирован для внутренних ключей.
- `PgBoltProcessingOptions.RunMaintenance = false` больше не выключает обработку целиком: тикер recurring управляется
  `RunScheduler` (по умолчанию `true`).
```

- в `### Added`:

```markdown
- Recurring-задачи (часть 4): `IRecurringJobManager` (`AddOrUpdateAsync`, `RemoveAsync`, `PauseAsync`, `ResumeAsync`,
  `TriggerAsync`, `GetAsync`), code-first `PgBoltOptions.AddRecurring` / `ClearRecurring` / `RecurringOwner`, cron (Cronos, с
  секундами) в таймзоне, `MisfirePolicy` (`FireOnce`, `Skip`, `CatchUp` с `CatchUpWindow`), `OverlapPolicy` (`Skip`, `Allow`);
  `JobContext.ScheduledAt`, `JobContext.RecurringJobId`, `JobInfo.ScheduledAt`, `JobInfo.RecurringJobId`;
  `PgBoltProcessingOptions.RunScheduler`, `SchedulerInterval`, `MisfireThreshold`, `RecurringSyncInterval`.
- Генератор регистрирует метаданные `[Job]`-типа и для мест вызова `AddRecurring<TArgs>` / `IRecurringJobManager.AddOrUpdateAsync<TArgs>`
  (продюсер без исполнителя); анализатор проверяет их покрытие JSON-контекстом (BOLT001 / BOLT006), как у `EnqueueAsync`.
```

- [x] **Step 2: Roadmap и статус спеки**

В `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md` в части 4 заменить `**Статус: спека на ревью.**` на ``план: `docs/superpowers/plans/2026-09-30-pgbolt-scheduler.md`. **Статус: реализовано.**``. В строке статуса спеки части 4 заменить «ожидает ревью пользователя» на «реализовано (план docs/superpowers/plans/2026-09-30-pgbolt-scheduler.md)».

- [x] **Step 3: Полный прогон**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: все тесты проходят. Падение — остановиться и доложить с выводом.

- [x] **Step 4: Commit**

```bash
git add CHANGELOG.md docs/superpowers/specs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "docs: scheduler (part 4) changelog and status"
```
