# PgBolt — часть 3b: кластерный лимит и веса очередей — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Кластерный лимит одновременно выполняемых задач очереди (с паузой) и веса очередей в общем пуле сервера; значения по умолчанию — в коде, приоритетные переопределения — в `queue_settings`, управление — `IQueueSettingsManager` и dashboard.

**Architecture:** Схема (`queue_settings`, `queue_leased` вместо `queue_owner`) — в `V001`; SQL — в `PgBoltStore`: синхронизация значений по умолчанию, снимок активных настроек, лимитированный dequeue (`NpgsqlBatch`: `pg_advisory_xact_lock` + dequeue с подсчётом взятых строк), CRUD переопределений. `PgBolt.Postgres` — `IQueueSettingsManager` с необязательным TTL-кэшем. `PgBolt.Server` — процессный `QueueSettingsCache` (неизменяемый снимок `Limited`/`Weights`, цикл обновления), выбор варианта dequeue в `DequeueFetchStrategy`, реакция диспетчера на `LimitReached` и сигнал `leaseFreed` после коммита исхода, новый контракт `ISharedPoolDistribution` и `WeightedDistribution` (stride scheduling), смена стратегии `SlotAllocator` на границе плана. Анализатор — BOLT010/BOLT011; dashboard — колонки и действия на экране очередей.

**Tech Stack:** .NET 10 (C# 14), Npgsql 10, PostgreSQL 16+, `System.Collections.Frozen`, Roslyn analyzers (`Microsoft.CodeAnalysis.CSharp.Analyzer.Testing`), ASP.NET Core 10 Minimal API + Razor `HtmlRenderer` + htmx, xUnit v3, Testcontainers (PostgreSQL + PgBouncer transaction mode), `Microsoft.Extensions.TimeProvider.Testing`.

**Spec:** `docs/superpowers/specs/2026-10-10-pgbolt-queue-limits-design.md` (далее «спека», ссылки «§6.2»). Основа: `2026-09-26-pgbolt-core-design.md` («core §5.2»), `2026-09-28-pgbolt-worker-design.md` («worker §4.3»), `2026-10-03-pgbolt-dashboard-design.md` («часть 6 §6.1»). Образец стиля кода и тестов — `src/PgBolt.Postgres/Storage/*.cs`, `src/PgBolt.Server/Runtime/*.cs`, `tests/PgBolt.Postgres.Tests`, `tests/PgBolt.Server.Tests`.

## Global Constraints

- Только `net10.0`; `TreatWarningsAsErrors=true`. `PgBolt.Abstractions`, `PgBolt.Postgres`, `PgBolt.Server` — `IsAotCompatible=true`, без reflection.
- SQL — строковые константы с `{schema}`, подстановка `Render`; свойство `…Sql => field ??= Render(…)`, как в `PgBoltStore`. Подготовка — только при `_prepare`. Повтор — `RunAsync` (`DbRetry`).
- PgBouncer transaction mode: **только** `pg_advisory_xact_lock`; сессионный `pg_advisory_lock` запрещён (§6.2). Лимитированный dequeue — `NpgsqlBatch` без `BeginTransaction` (неявная транзакция, один round trip), образец — `PgBoltStore.GetJobAsync`; `RecurringSync` образцом не служит.
- Ключ advisory lock — `hashtextextended('pgbolt:{schema}:' || @queue, 0)` (§6.2).
- Пределы: `ClusterMaxConcurrency` — `null` или ≥ 1; `Weight` — `null` или 1…1000; переопределение лимита — `null` (без лимита), 0 (пауза), ≥ 1; переопределение веса — `null` (без веса) или 1…1000; `QueueSettingsRefreshInterval` — по умолчанию 10 с, > 0 и ≤ 1 сут, jitter ±10 %; `QueueSettingsCacheDuration` — `null` (по умолчанию, без кэша) или > 0 и ≤ 1 сут; окно свежести предупреждения синхронизации — 5 минут.
- Валидация опций бросает `InvalidOperationException` в стиле существующих сообщений (`PgBoltServerOptions.Invalid`, `PgBoltProcessingOptions.Check`, `PgBoltOptions.Validate`); валидация аргументов `IQueueSettingsManager` — `ArgumentException` / `ArgumentOutOfRangeException`.
- `published_by` синхронизации — `$"{Environment.MachineName}/{Environment.ProcessId}"`, как у `job_types`.
- Диспетчер: при `LimitReached` очередь не опрашивается по `signaled` и `slotFreed`; будит только `leaseFreed` (§6.3).
- Dashboard: действия — только через `IQueueSettingsManager`; `updatedBy = $"dashboard: {User.Identity.Name ?? "anonymous"}"`; кнопки и POST скрыты/запрещены при `!DashboardContext.CanAct` (как остальные действия части 6); имя очереди — в query string `?queue=` (отклонение 8 части 6).
- Изменения схемы — в `V001__initial.sql`; совместимость со схемами прошлых сборок не поддерживается (§2).
- Коммиты: Conventional Commits; автор и коммиттер — `NightNo1r <57496218+NightNo1r@users.noreply.github.com>` (`git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit …`); строк `Co-Authored-By` **нет**.
- Тесты: `dotnet test tests/<Проект> --filter "FullyQualifiedName~<Класс>"`. Интеграционные классы — абстрактные с наследниками `_Direct` и `_Bouncer` (как `DequeueTests`); `EXPLAIN`-тесты — только `Direct`, `[Trait("Category", "Slow")]` (как `WorkflowResolverPlanTests`). Docker недоступен — остановиться и доложить, не заменять базу моками.

### Решения плана (уточнения спеки)

1. **Записи вместо кортежей в публичном API store.** `SyncQueueDefaultsAsync` принимает `IReadOnlyList<QueueDefault>` и возвращает только строки для предупреждения — `IReadOnlyList<QueueDefaultConflict>` (условие §4.2 п. 3 считается в SQL, как `JobTypeMismatchTemplate`); `GetActiveQueueSettingsAsync` возвращает `IReadOnlyList<ActiveQueueSetting>`. Спека §10 называет кортежи — смысл тот же.
2. **`onLeaseFreed` — необязательный последний параметр конструктора `CompletionSink`** (`Action<string>? onLeaseFreed = null`): существующие тесты не меняются; вызывается в `Finish` только при успешном переходе `OutcomeInFlight → Removed`.
3. **Кэш менеджера — отдельный класс** `QueueSettingsReadCache` (`PgBolt.Postgres.Client`), менеджер `QueueSettingsManager` его использует; поколения, схлопывание промахов и `WaitAsync(ct)` — в кэше.
4. **Анализатор настроек очередей — отдельный объект** `QueueOptionsAnalysis` (как `WorkflowSubmitAnalysis`), регистрируется в `PgBoltAnalyzer` независимо от `AnalysisState` (тот требует `PgBolt.Abstractions`, а BOLT011 по `PgBoltOptions` — нет). `PgBolt.Generators.Tests` получает ссылку на `PgBolt.Server` (и транзитивно `PgBolt.Postgres`).
5. **Экран очередей** строит строки `QueueLine(QueueRow Stats, QueueSettings? Settings, int? Leased)`: снимок — `DashboardStore.ListQueuesAsync` (объединение расширено `queue_settings` с переопределением), настройки — `IQueueSettingsManager.ListAsync`, живое число — `DashboardStore.CountLeasedAsync`.
6. **Внутренний конструктор `SlotAllocator(int, IReadOnlyList<QueueOptions>, ISharedPoolDistribution)`** остаётся для тестов (фиксированная стратегия, без источника весов); публичный — `SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, Func<FrozenDictionary<string, int>>? weights = null)`.

## Review Focus

1. **Лимит понижен ниже числа уже взятых строк** (оператор поставил 2 при 5 выполняющихся): `room < 0` → 0 строк, `LimitReached`, кандидаты не читаются; выполняющиеся дорабатывают. Тест — Task 3 (`Lowered_limit_below_leased_takes_nothing`).
2. **Менеджер в процессе без сервера** (клиент, dashboard): запись не падает на отсутствии `IQueueSettingsRefresher`. Тест — Task 4 (`Write_without_refresher_succeeds`).
3. **Ошибка старта до запуска цикла `QueueSettingsCache`** (синхронизация или первая загрузка упала): `StopAsync` после неё не бросает и не ждёт вечно. Тест — Task 8 (`Stop_after_failed_sync_completes`).
4. **Имя очереди с символами вне ASCII и регистром** (`"Отчёты"`, `"emails"` vs `"Emails"`): ключ lock, `queue_settings`, снимок и диспетчер сравнивают ordinal; разные регистры — разные очереди. Тест — Task 3 (`Queue_names_are_ordinal_and_unicode`).
5. **Переопределение для очереди, которую не обслуживает ни один сервер** (опечатка в имени): строка создаётся, на чужие очереди не влияет, видна в dashboard и снимается из UI. Тест — Task 15 (`Override_only_queue_is_listed_and_resettable`).

---

### Task 1: Схема `queue_settings` и индекс `queue_leased`

**Files:**
- Modify: `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql:24`
- Modify: `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs:139-147`
- Test: `tests/PgBolt.Postgres.Tests/MigrationTests.cs`, `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Produces: таблица `{schema}.queue_settings` (DDL — спека §3.1 дословно), индекс `queue_leased ON {schema}.queue (queue) WHERE lease_owner IS NOT NULL`; `queue_owner` удалён.

- [ ] **Step 1: Падающие тесты.** В `MigrationTests`: в массив таблиц `Migrate_creates_all_tables_once` добавить `"queue_settings"`; ожидаемый список индексов `queue` — `"queue_await_jobs,queue_awaiting,queue_awaiting_list,queue_delayed,queue_expiry,queue_fetch,queue_leased,queue_unique"`; `Queue_settings_columns_and_checks` — `ColumnsAsync("queue_settings")` (формат `имя:тип:attnotnull`) равно `"queue:text:true,default_max_concurrency:integer:false,default_weight:integer:false,default_published_by:text:false,default_updated_at:timestamp with time zone:false,override_concurrency_set:boolean:true,override_max_concurrency:integer:false,override_concurrency_updated_by:text:false,override_concurrency_updated_at:timestamp with time zone:false,override_weight_set:boolean:true,override_weight:integer:false,override_weight_updated_by:text:false,override_weight_updated_at:timestamp with time zone:false"`; `INSERT` с `default_max_concurrency = 0`, `default_weight = 1001`, `override_max_concurrency = -1`, `override_weight = 0`, `override_max_concurrency = 3` при `override_concurrency_set = false` — каждый падает с `23514`; `pg_get_indexdef(queue_leased)` оканчивается на `(queue) WHERE (lease_owner IS NOT NULL)`. В `HostingTests` в теорию `Schema_without_pre_release_tables_fails_start` добавить `[InlineData("queue_settings", true)]` и `[InlineData("queue_settings", false)]` (отдельного теста не нужно: теория уже делает `DROP TABLE`, проверяет `InvalidOperationException` с текстом `pre-release` и покрывает `AutoMigrate = false`).
- [ ] **Step 2:** `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests"` — FAIL.
- [ ] **Step 3:** В `V001__initial.sql` заменить `queue_owner` на `queue_leased`, добавить `CREATE TABLE {schema}.queue_settings` (§3.1). В `EnsureNotPreReleaseSchemaAsync` добавить `"queue_settings"` в массив имён и в XML-doc («часть 3b: то же для схем без `queue_settings`»).
- [ ] **Step 4:** Тот же прогон и `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~PlanContractTests"` — PASS (Processing работает на `queue_leased`).
- [ ] **Step 5:** `git commit -m "feat(postgres): queue_settings и индекс queue_leased"`

### Task 2: Синхронизация значений по умолчанию и снимок активных настроек (store)

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.QueueSettings.cs`, `src/PgBolt.Postgres/Storage/QueueSettingsRecords.cs`
- Test: `tests/PgBolt.Postgres.Tests/QueueSettingsSyncTests.cs`

**Interfaces:**
- Produces (namespace `PgBolt.Postgres.Storage`):
  - `public readonly record struct QueueDefault(string Queue, int? MaxConcurrency, int? Weight);`
  - `public sealed record QueueDefaultConflict(string Queue, int? PreviousMaxConcurrency, int? PreviousWeight, string PreviousPublishedBy);`
  - `public readonly record struct ActiveQueueSetting(string Queue, bool Limited, int? Weight);`
  - `PgBoltStore.SyncQueueDefaultsAsync(IReadOnlyList<QueueDefault> defaults, string publishedBy, CancellationToken ct) → Task<IReadOnlyList<QueueDefaultConflict>>` — `defaults` уникальны по `Queue` и упорядочены ordinal (контракт вызывающего, Task 6); пустой список — без обращения к БД.
  - `PgBoltStore.GetActiveQueueSettingsAsync(CancellationToken ct) → Task<IReadOnlyList<ActiveQueueSetting>>` — строки с эффективным лимитом или весом не `NULL`; `Limited` = эффективный лимит не `NULL`, `Weight` = эффективный вес.

- [ ] **Step 1: Падающие тесты** (`QueueSettingsSyncTests`, `_Direct`/`_Bouncer`):
  - `Sync_creates_row_when_any_default_set` — `[("a", 5, null), ("b", null, 3), ("c", null, null)]` → строки `a` (5, NULL), `b` (NULL, 3); `c` нет.
  - `Sync_nulls_existing_row_and_keeps_overrides` — строка `a` с `override_concurrency_set = true, override_max_concurrency = 0` и `override_weight_set = true, override_weight = 7`; синхронизация `("a", null, null)` → `default_* = NULL`, `override_*` прежние, `default_published_by` = новый.
  - `Sync_reports_fresh_conflict_from_other_publisher` — прошлая запись `("a", 5, 2)` от `"other/1"`; новая `("a", 6, 2)` от `"me/2"` → один `QueueDefaultConflict("a", 5, 2, "other/1")`; тот же сценарий с `default_updated_at = now() - interval '6 minutes'` → пусто; от того же `published_by` → пусто; одинаковые значения → пусто.
  - `Sync_into_override_created_row_reports_nothing` — строка создана только переопределением (`default_published_by IS NULL`) → пусто.
  - `Active_settings_follow_effective_values` (правило §6.1): только вес из кода → `(q, false, w)`; переопределение «без лимита» поверх лимита из кода → `Limited = false`; «без веса» поверх веса из кода → `Weight = null`; пауза 0 → `Limited = true`; оба эффективных `NULL` → строки нет.
- [ ] **Step 2:** `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~QueueSettingsSyncTests"` — FAIL (не компилируется).
- [ ] **Step 3: Реализация.** Синхронизация — один statement:

```sql
WITH u AS (
    SELECT * FROM unnest(@queues::text[], @limits::int[], @weights::int[]) AS u(queue, lim, weight)
),
prev AS (
    SELECT s.queue, s.default_max_concurrency, s.default_weight, s.default_published_by, s.default_updated_at
    FROM {schema}.queue_settings s JOIN u ON u.queue = s.queue
    ORDER BY s.queue FOR UPDATE OF s
),
ins AS (
    INSERT INTO {schema}.queue_settings AS s (queue, default_max_concurrency, default_weight, default_published_by, default_updated_at)
    SELECT queue, lim, weight, @published_by, now() FROM u WHERE lim IS NOT NULL OR weight IS NOT NULL ORDER BY queue
    ON CONFLICT (queue) DO UPDATE SET default_max_concurrency = EXCLUDED.default_max_concurrency,
        default_weight = EXCLUDED.default_weight, default_published_by = EXCLUDED.default_published_by,
        default_updated_at = EXCLUDED.default_updated_at
),
upd AS (
    UPDATE {schema}.queue_settings s SET default_max_concurrency = NULL, default_weight = NULL,
        default_published_by = @published_by, default_updated_at = now()
    FROM u WHERE s.queue = u.queue AND u.lim IS NULL AND u.weight IS NULL
)
SELECT p.queue, p.default_max_concurrency, p.default_weight, p.default_published_by
FROM prev p JOIN u ON u.queue = p.queue
WHERE p.default_published_by IS NOT NULL AND p.default_published_by <> @published_by
  AND p.default_updated_at > now() - interval '5 minutes'
  AND (p.default_max_concurrency IS DISTINCT FROM u.lim OR p.default_weight IS DISTINCT FROM u.weight)
```

  Снимок: `SELECT queue, (CASE … лимит END) IS NOT NULL, CASE … вес END FROM {schema}.queue_settings WHERE <эфф. лимит> IS NOT NULL OR <эфф. вес> IS NOT NULL ORDER BY queue` (выражения — §3.1). Чтение — `QueryAsync` + `ReadAllAsync`, как в `PgBoltStore.JobTypes.cs`.
- [ ] **Step 4:** Тот же прогон — PASS.
- [ ] **Step 5:** `git commit -m "feat(postgres): синхронизация значений по умолчанию и снимок настроек очередей"`

### Task 3: Лимитированный dequeue (store)

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.DequeueLimited.cs`
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.cs:6` (XML-doc: «Каждая операция — один round trip»), `src/PgBolt.Postgres/Storage/StoreRecords.cs`
- Test: `tests/PgBolt.Postgres.Tests/DequeueLimitedTests.cs`, `tests/PgBolt.Postgres.Tests/DequeueLimitedPlanTests.cs`, `tests/PgBolt.Postgres.Tests/DequeueLimitedConcurrencyTests.cs`

**Interfaces:**
- Consumes: `queue_settings` (Task 1).
- Produces: `public sealed record LimitedDequeueResult(IReadOnlyList<LeasedJob> Jobs, bool LimitReached);` (`StoreRecords.cs`); `PgBoltStore.DequeueLimitedAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct) → Task<LimitedDequeueResult>` (`max ≤ 0` → `ArgumentOutOfRangeException`, как `DequeueAsync`).

- [ ] **Step 1: Падающие тесты** (`DequeueLimitedTests`, `_Direct`/`_Bouncer`; лимит задаётся `INSERT INTO queue_settings`):
  - `Takes_up_to_room` — лимит 2, backlog 5, `max` 4 → 2 задачи, `LimitReached = true`; повтор → 0, `true`.
  - `Takes_max_when_unlimited_or_no_row` — нет строки → 3 из `max` 3, `false`; строка с эффективным `NULL` (переопределение «без лимита») → то же.
  - `Pause_takes_nothing` — переопределение 0, backlog → 0, `true`.
  - `Empty_queue_is_not_limit_reached` — лимит 5, пусто → 0, `false`.
  - `Lowered_limit_below_leased_takes_nothing` — 3 взятых строки, лимит 2 → 0, `true`.
  - `Expired_lease_occupies_room` — лимит 1, одна взятая строка с `lease_until` в прошлом → 0, `true`; после `SweepExpiredLeasesAsync` → 1.
  - `Extra_candidates_are_unlocked_after_dequeue` — лимит 1, backlog 5, `max` 5 → 1; затем из другого соединения `SELECT … FOR UPDATE NOWAIT` по остальным 4 — без ошибки.
  - `Lock_is_released_with_the_batch` (только `_Bouncer` значимо, выполняется в обоих) — проверяется только ключ своей очереди (общая БД, advisory-lock'и берут и соседние классы, `MigrationRunner`, `PartitionManager`, `RecurringSync`, поэтому общий подсчёт `pg_locks` нестабилен): после вызова из другого соединения `SELECT pg_try_advisory_xact_lock(hashtextextended('pgbolt:<схема>:' || @q, 0))` возвращает `true` сразу, без ожидания (`DequeueLimitedAsync` возвращает результат после Sync-коммита батча).
  - `Queue_names_are_ordinal_and_unicode` — лимит 1 у `"Отчёты"`, лимита нет у `"отчёты"`: в `"Отчёты"` берётся 1, в `"отчёты"` — все.
  - `Returned_jobs_match_DequeueAsync_columns` — все поля `LeasedJob`, кроме `Id` и `LeaseId` (type, args, attempt, `MaxAttempts`, `RetryDelaysSeconds` — по элементам, parent_id, batch_id, trace_parent, recurring_id, scheduled_at, continuation), совпадают с результатом `DequeueAsync` для такой же задачи; у задачи непустые `retry_delays` и `max_attempts`, отличный от значения по умолчанию.
- [ ] **Step 2: Падающие тесты конкурентности** (`DequeueLimitedConcurrencyTests`, `[Trait("Category", "Slow")]`, `_Direct`/`_Bouncer`):
  - `Never_exceeds_limit_under_contention` — лимит 4, backlog 2000, 8 параллельных воркеров 5 с: `DequeueLimitedAsync(max: 1..4)`, затем через случайные 0–20 мс `AckAsync` или `ReleaseAsync` взятых; монитор раз в 5 мс в явной транзакции: `SELECT pg_advisory_xact_lock(<ключ>)`, затем отдельной командой `SELECT count(*) … WHERE queue = @q AND lease_owner IS NOT NULL`; максимум ≤ 4 и хотя бы раз = 4.
  - `Single_statement_variant_overshoots` (детерминированный, контроль §6.2): A — явная транзакция: `pg_advisory_xact_lock`, `UPDATE` 2 строк в взятые (лимит 2); B — одним statement'ом `WITH k AS (SELECT count(*) AS leased FROM … WHERE (SELECT pg_advisory_xact_lock(…)) IS NOT NULL AND …) … LIMIT 2 - (SELECT leased FROM k)` (копия формы §6.2 без отдельного lock; вызов lock стоит в `WHERE` подзапроса, на который ссылается основной запрос, — отдельная CTE `lk`, не упомянутая в основном запросе, не исполняется, и B не заблокировался бы), ждём `pg_locks` с `granted = false`; A коммитит; B взял 2 → всего 4 > 2.
- [ ] **Step 3: Падающий тест плана** (`DequeueLimitedPlanTests`, только `Direct`, `Slow`): 300 000 готовых строк очереди, `jobs` на 10 дневных партиций (≥ 1,4 млн строк в сумме — заполнить все партиции), `ANALYZE`; для `@n` 1 и 100 `EXPLAIN (FORMAT JSON)` statement'а 2: нет узлов `Seq Scan` по `queue` и `jobs_*`, нет `Hash Join`; подсчёт — `Index Only Scan` по `queue_leased`, `c` — `Index Scan` по `queue_fetch`; при паузе (`EXPLAIN ANALYZE`) узел по `queue_fetch` — `never executed`. Плюс тот же контроль для `DequeueAsync` (прежний план).
- [ ] **Step 4:** `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~DequeueLimited"` — FAIL.
- [ ] **Step 5: Реализация.** SQL — спека §6.2 дословно (statement 2 — колонки `SET` и итогового `SELECT` как в `DequeueTemplate`); батч по образцу `GetJobAsync`; чтение: `NextResultAsync` мимо результата lock, затем строки: первая колонка `room` (`int?`), остальные — `LeasedJob` со сдвигом индекса на 1; строка с `NULL` вместо задачи (`id IS NULL`) пропускается; `LimitReached = room is { } r && jobs.Count >= r`.
- [ ] **Step 6:** Тот же прогон (с `Slow`) — PASS; `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~DequeueTests|FullyQualifiedName~HotUpdateTests"` — PASS.
- [ ] **Step 7:** `git commit -m "feat(postgres): лимитированный dequeue под pg_advisory_xact_lock"`

### Task 4: `IQueueSettingsManager` без кэша

**Files:**
- Create: `src/PgBolt.Abstractions/IQueueSettingsManager.cs`, `src/PgBolt.Abstractions/QueueSettings.cs`, `src/PgBolt.Postgres/Client/QueueSettingsManager.cs`, `src/PgBolt.Postgres/Client/IQueueSettingsRefresher.cs`
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.QueueSettings.cs`, `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs:55`
- Test: `tests/PgBolt.Postgres.Tests/QueueSettingsManagerTests.cs`

**Interfaces:**
- Produces:
  - `namespace PgBolt`: `IQueueSettingsManager` и `QueueSettings` — сигнатуры спеки §5.1 дословно, XML-doc с допущением §6.1 (лимит, впервые заданный очереди, действует в пределах `QueueSettingsRefreshInterval`).
  - `namespace PgBolt.Postgres.Client`: `internal interface IQueueSettingsRefresher { void RefreshNow(); }`.
  - `PgBoltStore`: `ListQueueSettingsAsync(ct) → Task<IReadOnlyList<QueueSettings>>` (по `queue`), `GetQueueSettingsAsync(string queue, ct) → Task<QueueSettings?>`, `SetConcurrencyOverrideAsync(string queue, int? maxConcurrency, string? updatedBy, ct) → Task`, `ResetConcurrencyOverrideAsync(string queue, ct) → Task`, `SetWeightOverrideAsync(string queue, int? weight, string? updatedBy, ct) → Task`, `ResetWeightOverrideAsync(string queue, ct) → Task`.
  - `internal sealed class QueueSettingsManager(PgBoltStore store, IServiceProvider services) : IQueueSettingsManager` (кэш добавит Task 5); регистрация `services.AddSingleton<IQueueSettingsManager>(sp => new QueueSettingsManager(sp.GetRequiredService<PgBoltStore>(), sp))`.

- [ ] **Step 1: Падающие тесты** (`QueueSettingsManagerTests`, `_Direct`/`_Bouncer`, менеджер из `ServiceCollection` с `AddPgBolt(o => { o.ConnectionString = Db.ConnectionString; o.Schema = Db.Schema; })` — источник данных принадлежит провайдеру (`owned: true`); с `o.DataSource = Db.DataSource` `Dispose` источника ничего не закрывает, и тест ниже проверял бы не то):
  - `Concurrency_override_beats_default_and_resets` — строка с `default_max_concurrency = 5`; `SetConcurrencyOverrideAsync("a", 2, "op")` → `EffectiveMaxConcurrency = 2`, `OverrideConcurrencyUpdatedBy = "op"`, `OverrideConcurrencyUpdatedAt` не `null`; `Set…(null)` → `EffectiveMaxConcurrency = null`, `OverrideConcurrencySet = true`; `Set…(0)` → `Paused`; `ResetConcurrencyOverrideAsync` → 5, автор и время `null`.
  - `Weight_override_is_independent` — `SetWeightOverrideAsync("a", 7)` не меняет колонки лимита; `ResetConcurrencyOverrideAsync` не меняет вес; `Set…Weight(null)` снимает вес из кода; `ResetWeightOverrideAsync` возвращает его.
  - `Override_creates_row_without_defaults` — для очереди без строки: `DefaultMaxConcurrency = null`, `DefaultWeight = null`, `DefaultPublishedBy = null`.
  - `Reset_without_row_is_noop` — нет исключения, строки нет.
  - `Arguments_are_validated` — `null`, `""`, `" "` имя → `ArgumentException`; лимит −1 → `ArgumentOutOfRangeException`; вес 0 и 1001 → `ArgumentOutOfRangeException`; БД не тронута.
  - `Write_calls_refresher_even_on_failure` — `[Theory]` по четырём методам (`SetConcurrencyOverrideAsync`, `ResetConcurrencyOverrideAsync`, `SetWeightOverrideAsync`, `ResetWeightOverrideAsync`); в DI зарегистрирован фейк `IQueueSettingsRefresher`; успешная запись → 1 вызов; ошибка записи — `await sp.GetRequiredService<PgBoltDataSource>().DisposeAsync()` перед вызовом (источник принадлежит провайдеру, поэтому следующее открытие соединения бросает, а сам провайдер жив и `services.GetService<IQueueSettingsRefresher>()` работает; `PgBoltDataSource` — internal, IVT для `PgBolt.Postgres.Tests` есть; провайдер не освобождать до конца вызова) → тоже вызов refresher, исключение проброшено. Тот же способ — «ошибка записи» в задаче 5.
  - `Write_without_refresher_succeeds` — без регистрации — без исключения.
- [ ] **Step 2:** `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~QueueSettingsManagerTests"` — FAIL.
- [ ] **Step 3: Реализация.** SQL: `Set…` — `INSERT … ON CONFLICT (queue) DO UPDATE SET override_<x>_set = true, override_<x> = @v, override_<x>_updated_by = @by, override_<x>_updated_at = now()`; `Reset…` — `UPDATE … SET override_<x>_set = false, override_<x> = NULL, …_updated_by = NULL, …_updated_at = NULL WHERE queue = @queue`. Менеджер: валидация до БД; запись в `try`, в `finally` — `services.GetService<IQueueSettingsRefresher>()?.RefreshNow()` (берётся при вызове: `AddPgBoltServer` может идти после `AddPgBolt`). `InvalidateCache()` — пока пустой.
- [ ] **Step 4:** Тот же прогон — PASS.
- [ ] **Step 5:** `git commit -m "feat(postgres): IQueueSettingsManager — переопределения лимита и веса"`

### Task 5: TTL-кэш менеджера

**Files:**
- Create: `src/PgBolt.Postgres/Client/QueueSettingsReadCache.cs`
- Modify: `src/PgBolt.Postgres/PgBoltOptions.cs`, `src/PgBolt.Postgres/Client/QueueSettingsManager.cs`, `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`
- Test: `tests/PgBolt.Postgres.Tests/QueueSettingsReadCacheTests.cs`, `tests/PgBolt.Postgres.Tests/OptionsTests.cs`

**Interfaces:**
- Produces: `PgBoltOptions.QueueSettingsCacheDuration { get; set; }` (`TimeSpan?`, `null`); `internal sealed class QueueSettingsReadCache(TimeSpan? duration, TimeProvider time)` с `bool Enabled` (`duration is not null`), `ValueTask<IReadOnlyList<QueueSettings>> GetAsync(Func<CancellationToken, Task<IReadOnlyList<QueueSettings>>> load, CancellationToken ct)` и `void Invalidate()`; `QueueSettingsManager(PgBoltStore store, QueueSettingsReadCache cache, IServiceProvider services)`.

- [ ] **Step 1: Падающие тесты** (unit, `FakeTimeProvider`, `load` — счётчик вызовов с управляемым `TaskCompletionSource`):
  - `Without_duration_always_loads`; `Serves_snapshot_until_ttl` (TTL 10 с: 2 вызова в пределах — 1 загрузка; после `Advance(10 s)` — 2-я).
  - `Concurrent_misses_share_one_load`; `Error_is_not_cached` (первая загрузка бросает — второй вызов грузит заново).
  - `Load_started_before_invalidate_is_not_cached` — загрузка A в полёте, `Invalidate()`, A завершилась → следующий вызов грузит заново; вызывающий после инвалидации не ждёт A (новая загрузка).
  - `Cancelling_one_waiter_keeps_shared_load` — два ожидающих, отмена токена первого → первый `OperationCanceledException`, второй получает результат, загрузка одна.
  - В `QueueSettingsManagerTests` (интеграция): `Get_without_cache_reads_db` — без `QueueSettingsCacheDuration` прямой `UPDATE` в БД виден `GetAsync` сразу; `Get_reads_from_list_snapshot_and_write_invalidates` — `[Theory]` по четырём методам `Set…`/`Reset…`: TTL 1 ч: `ListAsync`, прямой `UPDATE` в БД, `GetAsync` — старое; вызов метода записи → `GetAsync` — новое; запись, бросившая исключение, тоже инвалидирует: отдельный `[Theory]` по тем же четырём методам, TTL 1 ч, `ListAsync` (снимок в кэше) → ошибка записи тем же способом, что в `Write_calls_refresher_even_on_failure` (задача 4: `DisposeAsync` `PgBoltDataSource`), → повторный `ListAsync` бросает (кэш сброшен и загрузка не удалась), а не возвращает старый снимок.
  - В `OptionsTests`: `QueueSettingsCacheDuration` `TimeSpan.Zero`, `-1 с`, `1 сут + 1 тик` → `InvalidOperationException`; `null` и `1 мс` — валидно.
- [ ] **Step 2:** `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~QueueSettingsReadCache|FullyQualifiedName~QueueSettingsManagerTests|FullyQualifiedName~OptionsTests"` — FAIL.
- [ ] **Step 3: Реализация** по §5.2: поле поколения (`long`), `(Task<IReadOnlyList<QueueSettings>> Load, long Generation)` в полёте, снимок с моментом `TimeProvider.GetTimestamp()`; общая загрузка запускается с `CancellationToken.None`, ожидание — `WaitAsync(ct)`; результат кладётся под lock, только если поколение не изменилось; `Invalidate` — `++generation`, забыть загрузку и снимок. Без `duration` — прямой вызов `load(ct)`. Менеджер: `ListAsync` → кэш; `GetAsync` → при `cache.Enabled` из снимка списка (`FirstOrDefault` ordinal), без кэша — `store.GetQueueSettingsAsync(queue, ct)` (одна строка, без чтения всей таблицы, §5.2); запись — `finally { cache.Invalidate(); refresher?.RefreshNow(); }`; `InvalidateCache()` → `cache.Invalidate()`. Валидация опции — в `PgBoltOptions.Validate` (сообщение `"PgBolt: QueueSettingsCacheDuration must be null or in (0, 1 day]."`).
- [ ] **Step 4:** Тот же прогон — PASS.
- [ ] **Step 5:** `git commit -m "feat(postgres): TTL-кэш IQueueSettingsManager"`

### Task 6: Опции очередей и процесса, сбор значений по умолчанию

**Files:**
- Modify: `src/PgBolt.Server/QueueOptions.cs`, `src/PgBolt.Server/PgBoltProcessingOptions.cs`
- Create: `src/PgBolt.Server/Runtime/QueueDefaults.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs`, `tests/PgBolt.Server.Tests/Unit/QueueDefaultsTests.cs`

**Interfaces:**
- Produces: `QueueOptions.ClusterMaxConcurrency` (`int?`), `QueueOptions.Weight` (`int?`) с XML-doc (§4.1; у `ClusterMaxConcurrency` — допущение §6.1); `PgBoltProcessingOptions.QueueSettingsRefreshInterval` (`TimeSpan`, 10 с); `internal static class QueueDefaults { public static IReadOnlyList<QueueDefault> Collect(IReadOnlyList<ServerDefinition> servers); }` — одна запись на очередь, упорядочено ordinal; конфликт → `InvalidOperationException`: `$"PgBolt: queue '{queue}' has different {option} on servers '{a}' ({va}) and '{b}' ({vb})."` (значение `null` печатается как `null`).

- [ ] **Step 1: Падающие тесты.** `ServerOptionsTests`: `ClusterMaxConcurrency = 0` → `"… Queue('q').ClusterMaxConcurrency must be null or >= 1."`; `Weight = 0` и `1001` → `"… Queue('q').Weight must be null or in [1, 1000]."`; `1` и `1000` валидны. `ProcessingOptions`: `QueueSettingsRefreshInterval` 0 и `1 сут + 1 тик` → `InvalidOperationException`. `QueueDefaultsTests`: одна очередь на двух серверах с равными значениями → одна запись; `ClusterMaxConcurrency` 5 и `null` → исключение с `ClusterMaxConcurrency`, именами серверов и `null`; `Weight` 2 и 3 → исключение с `Weight`; порядок `["b", "a", "B"]` → `["B", "a", "b"]`.
- [ ] **Step 2:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ServerOptionsTests|FullyQualifiedName~QueueDefaultsTests"` — FAIL.
- [ ] **Step 3:** Свойства и проверки в `QueueOptions.Validate` и `PgBoltProcessingOptions.Validate` (`Check(QueueSettingsRefreshInterval, …)`); `QueueDefaults.Collect`.
- [ ] **Step 4:** Тот же прогон — PASS.
- [ ] **Step 5:** `git commit -m "feat(server): ClusterMaxConcurrency, Weight и QueueSettingsRefreshInterval"`

### Task 7: `QueueSettingsCache` процесса

**Files:**
- Create: `src/PgBolt.Server/Runtime/QueueSettingsCache.cs`
- Modify: `src/PgBolt.Server/Runtime/IServerStore.cs`, `src/PgBolt.Server/Runtime/PgBoltServerStore.cs`, `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/QueueSettingsCacheTests.cs`

**Interfaces:**
- Consumes: `ActiveQueueSetting`, `GetActiveQueueSettingsAsync` (Task 2), `IQueueSettingsRefresher` (Task 4).
- Produces:
  - `IServerStore` + `Task<LimitedDequeueResult> DequeueLimitedAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)`, `Task<IReadOnlyList<QueueDefaultConflict>> SyncQueueDefaultsAsync(IReadOnlyList<QueueDefault> defaults, string publishedBy, CancellationToken ct)`, `Task<IReadOnlyList<ActiveQueueSetting>> GetActiveQueueSettingsAsync(CancellationToken ct)`; `FakeServerStore` — `OnDequeueLimited`, `OnSyncQueueDefaults`, `OnGetActiveQueueSettings` по образцу `OnDequeue`.
  - `internal sealed record QueueSettingsSnapshot(FrozenSet<string> Limited, FrozenDictionary<string, int> Weights) { public static QueueSettingsSnapshot Empty { get; } }` (ordinal-компараторы).
  - `internal sealed class QueueSettingsCache(IServerStore store, PgBoltProcessingOptions options, MonoClock clock, ILogger logger) : IQueueSettingsRefresher` — `QueueSettingsSnapshot Current { get; }` (`Volatile.Read`), `Task LoadAsync(CancellationToken ct)` (первая загрузка, бросает), `void Start()`, `Task StopAsync()`, `void RefreshNow()`, `internal Func<double> NextRandom`, `internal int Waits` (инкремент после взведения ожидания цикла, как `Dispatcher.IdleWaits`; для тестов).

- [ ] **Step 1: Падающие тесты** (`FakeServerStore`, `FakeTimeProvider` через `MonoClock`):
  - `Load_builds_snapshot` — `[("a", true, null), ("b", false, 3), ("c", true, 2), ("Emails", true, null), ("emails", false, 1)]` → `Limited = {a, c, Emails}`, `Weights = {b: 3, c: 2, emails: 1}`; лимит есть только у `Emails`, не у `emails` (ordinal, Review Focus 4).
  - `Load_failure_throws_and_keeps_empty`.
  - `Loop_refreshes_every_interval_with_jitter` — после `Start()` и ожидания `Waits ≥ 1` (иначе `Advance` до взведения таймера теряет такт) `Advance(11 с)` (jitter `NextRandom = () => 1` → 11 с) — второе чтение и новый снимок.
  - `Refresh_failure_keeps_previous_snapshot_and_logs_warning` (`ListLogger`).
  - `RefreshNow_during_refresh_schedules_one_more` — обновление в полёте (TCS), два `RefreshNow()` → после завершения ровно одно повторное чтение; `RefreshNow` не ждёт.
  - `RefreshNow_before_start_and_after_stop_is_noop` — 0 чтений, без исключений.
  - `Stop_completes_loop` — `StopAsync` завершается при обновлении в полёте (токен цикла отменён).
- [ ] **Step 2:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~QueueSettingsCacheTests"` — FAIL.
- [ ] **Step 3: Реализация.** Цикл — `Task.Run` с `CancellationTokenSource`, ожидание `_signal.WaitAsync(interval × (1 ± 0,1), clock.Time, token)` на `AsyncAutoResetEvent` (ждёт с таймаутом на `TimeProvider` и не бросает при таймауте — `Task.WhenAny` не нужен); `RefreshNow` — `Set()` события только между `Start` и `StopAsync`; флаг «ещё одно» — latched событие уже даёт ровно одно отложенное. Публикация — `Volatile.Write`. Ошибка обновления — `LogWarning("PgBolt: refreshing queue settings failed; using the previous snapshot.")`.
- [ ] **Step 4:** Тот же прогон и `dotnet build PgBolt.slnx` — PASS.
- [ ] **Step 5:** `git commit -m "feat(server): QueueSettingsCache — снимок лимитов и весов процесса"`

### Task 8: Старт и остановка процесса

**Files:**
- Modify: `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`, `src/PgBolt.Server/PgBoltServerServiceCollectionExtensions.cs`, `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs` (хук `Func<Guid, CancellationToken, Task> OnRemoveServer` по образцу `OnRegisterServer`; `RemoveServerAsync` вызывает его)
- Test: `tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs`, `tests/PgBolt.Server.Tests/Integration/QueueSettingsStartupTests.cs`

**Interfaces:**
- Consumes: `QueueDefaults.Collect` (Task 6), `QueueSettingsCache` (Task 7).
- Produces: регистрация в `AddPgBoltServer` (первый вызов): `services.TryAddSingleton(sp => new QueueSettingsCache(sp.GetRequiredService<IServerStore>(), sp.GetRequiredService<PgBoltProcessingOptions>(), sp.GetRequiredService<MonoClock>(), (sp.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance).CreateLogger("PgBolt.Processing")))` и `services.TryAddSingleton<IQueueSettingsRefresher>(sp => sp.GetRequiredService<QueueSettingsCache>())`; `PgBoltProcessingService.QueueSettings` (`internal QueueSettingsCache?`, для тестов).

- [ ] **Step 1: Падающие тесты** (`ProcessingServiceTests`, фейки):
  - `Start_syncs_defaults_then_loads_before_creating_servers` — порядок вызовов фейка: `PublishJobTypes` → `SyncQueueDefaults` (аргумент — `QueueDefaults.Collect`, `publishedBy` = `"{MachineName}/{ProcessId}"`) → `GetActiveQueueSettings` → `RegisterServer`.
  - `Conflict_warning_is_logged_per_queue` — фейк возвращает `QueueDefaultConflict("a", 5, null, "other/1")` → Warning с `a`, `other/1`, прежним и новым значением.
  - `Conflicting_defaults_fail_start` — два сервера, `Weight` 2 и 3 → `StartAsync` бросает `InvalidOperationException`, `RegisterServer` не вызван.
  - `Initial_load_failure_fails_start`.
  - `Stop_after_failed_sync_completes` — `SyncQueueDefaults` бросает; `StartAsync` бросает; `StopAsync` завершается (таймаут теста 5 с).
  - `Loop_starts_first_and_stops_after_servers` — цикл кэша запущен до `server.Start()`: в первом вызове хука `OnDequeue` (диспетчер уже запущен) вызвать `QueueSettings.RefreshNow()` и дождаться чтения `GetActiveQueueSettings` (до `Start()` цикла `RefreshNow` — no-op, задача 7, поэтому чтение доказывает порядок); `FlushAndStopAsync` серверов задержан воротами на хуке `OnRemoveServer` фейка (`RemoveServerAsync` вызывается в `FlushAndStopAsync` каждого сервера), и пока серверы останавливаются, `RefreshNow()` приводит к чтению `GetActiveQueueSettings` (цикл ещё жив); после ворот и `StopAsync` — ни одного чтения (различает остановку цикла до серверов и после них).
  - `Loop_stops_when_start_fails_after_cache_start` — ошибка в п. 6 после `cache.Start()` останавливает цикл: после ошибки старта и `StopAsync` чтений нет.
  - `Loop_runs_when_maintenance_is_disabled` — при `RunMaintenance = false` цикл обновляет снимок (перед `Advance` ждать `QueueSettings.Waits ≥ 1`, как в `Loop_refreshes_every_interval_with_jitter`, Task 7).
  - `Process_without_servers_does_not_sync` — провайдер собран вручную: фейк `IServerStore`, `MonoClock`, `PgBoltProcessingOptions`, без `ServerDefinition` и без `QueueSettingsCache`; `StartAsync` не запрашивает `QueueSettingsCache` при отсутствии серверов → ни `SyncQueueDefaults`, ни `GetActiveQueueSettings`, без исключения.
- [ ] **Step 2: Падающий интеграционный тест** (`QueueSettingsStartupTests`, `ServerTestHost`): два сервера процесса, очередь `"a"` с `ClusterMaxConcurrency = 3, Weight = 2` на обоих → после старта в `queue_settings` одна строка `(3, 2)`; `IServiceProvider.GetService<IQueueSettingsRefresher>()` — тот же экземпляр, что `QueueSettingsCache`.
- [ ] **Step 3:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ProcessingServiceTests|FullyQualifiedName~QueueSettingsStartupTests"` — FAIL.
- [ ] **Step 4: Реализация.** В `StartAsync` после публикации `job_types`, если есть `ServerDefinition`: `QueueDefaults.Collect` → `SyncQueueDefaultsAsync` → предупреждения → `cache.LoadAsync`; затем создание серверов. В блоке п. 6 первым — `cache.Start()`. В `StopAsync` — `await cache.StopAsync()` после `FlushAndStopAsync` (и при отменённом токене). Ветка ошибки старта уже вызывает `StopAsync`; `StopAsync` должен быть безопасен, если кэш не стартовал.
- [ ] **Step 5:** Тот же прогон — PASS; `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Unit"` — PASS.
- [ ] **Step 6:** `git commit -m "feat(server): синхронизация настроек очередей при старте и цикл обновления"`

### Task 9: Выбор варианта dequeue

**Files:**
- Modify: `src/PgBolt.Server/Runtime/IFetchStrategy.cs`, `src/PgBolt.Server/Runtime/Dispatcher.cs:111-117`, `src/PgBolt.Server/Runtime/PgBoltServer.cs:29-46`, `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs` (передача кэша в `PgBoltServer`)
- Create: `tests/PgBolt.Server.Tests/Unit/FetchStrategyTests.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs` (адаптация)

**Interfaces:**
- Consumes: `QueueSettingsCache.Current` (Task 7), `IServerStore.DequeueLimitedAsync` (Task 7).
- Produces: `internal readonly record struct FetchResult(IReadOnlyList<LeasedJob> Jobs, bool LimitReached);` `IFetchStrategy.FetchAsync(string queue, int max, CancellationToken ct) → Task<FetchResult>`; `DequeueFetchStrategy(IServerStore store, Guid serverId, TimeSpan lease, Func<QueueSettingsSnapshot> settings)`; `PgBoltServer` получает `QueueSettingsCache settings` последним параметром конструктора.

- [ ] **Step 1: Падающие тесты** (`FetchStrategyTests`, `FakeServerStore`): очередь вне `Limited` → `OnDequeue`, `OnDequeueLimited` не вызван, `LimitReached = false`; в `Limited` → `OnDequeueLimited`, признак из результата; в `Weights`, но не в `Limited` → `OnDequeue`; подмена снимка между вызовами → второй вызов идёт по новому снимку.
- [ ] **Step 2:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~FetchStrategyTests"` — FAIL.
- [ ] **Step 3:** Реализация; диспетчер пока использует только `result.Jobs` (поведение не меняется); фикстура `DispatcherTests` создаёт настоящий `DequeueFetchStrategy` поверх `FakeServerStore.OnDequeue` и передаёт ему `() => Settings` — изменяемое поле фикстуры (по умолчанию `QueueSettingsSnapshot.Empty`), чтобы тесты задачи 10 подменяли снимок.
- [ ] **Step 4:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Unit"` — PASS.
- [ ] **Step 5:** `git commit -m "feat(server): выбор лимитированного dequeue по снимку настроек"`

### Task 10: `LimitReached` в диспетчере и сигнал `leaseFreed`

**Files:**
- Modify: `src/PgBolt.Server/Runtime/Dispatcher.cs`, `src/PgBolt.Server/Runtime/CompletionSink.cs:33,346`, `src/PgBolt.Server/Runtime/PgBoltServer.cs:42-45`
- Test: `tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs`, `tests/PgBolt.Server.Tests/Unit/CompletionSinkTests.cs`, `tests/PgBolt.Server.Tests/Integration/QueueLimitTests.cs` (с `[Collection(TimingSensitiveCollection.Name)]`: пропускная способность зависит от времени, а интеграционные классы идут параллельно)

**Interfaces:**
- Consumes: `FetchResult` (Task 9).
- Produces: `Dispatcher.OnLeaseFreed(string queue)`; `CompletionSink(…, OutcomeStats stats, Action<string>? onLeaseFreed = null)`.

- [ ] **Step 1: Падающие unit-тесты** (`DispatcherTests`; `LimitReached` задаётся снимком фикстуры, где очередь входит в `Limited` (`Settings = new QueueSettingsSnapshot(["q"].ToFrozenSet(StringComparer.Ordinal), …)`), и ответом `_store.OnDequeueLimited` → `new LimitedDequeueResult(jobs, true)`; по образцу `Respond` в фикстуре добавить помощник `RespondLimited` с тем же поведением по умолчанию — пусто, `LimitReached = false`):
  - `Limit_reached_backs_off_even_when_full` — `LimitedDequeueResult(n задач, true)` → следующий опрос не раньше `interval × 2 × 0,9` (не `now`).
  - `Signals_and_slot_freed_are_absorbed_while_limit_reached` — после `LimitReached`: `IWakeupSignal.Signal(q)` и `OnSlotFreed(q)` не дают dequeue до `nextPoll`; `interval` не сброшен к `Min`.
  - `Lease_freed_polls_immediately_while_limit_reached` — `OnLeaseFreed(q)` → dequeue сразу; `interval` не изменён.
  - `Lease_freed_is_ignored_without_limit_reached`.
  - `Lease_freed_during_dequeue_is_not_lost` — сигнал во время `FetchAsync`, результат `LimitReached = true` → следующий dequeue сразу после итерации.
  - `Without_limit_reached_signals_behave_as_before` — прежние правила `signaled`/`slotFreed`.
  - `CompletionSinkTests`: `On_lease_freed_after_each_outcome_statement` — для ack-пакета из 3 записей — 3 вызова после возврата `AckAsync` (не раньше: фейк держит `AckAsync` на TCS, вызовов 0); retry, fail, confirm-cancel, release и `LeaseLost` — по одному вызову с очередью выполнения; `Not_called_for_abandoned` — бюджет исчерпан и остановка с отменой → 0 вызовов.
- [ ] **Step 2: Падающий интеграционный тест** (`QueueLimitTests` с `[Collection(TimingSensitiveCollection.Name)]`, `ServerTestHost`, PostgreSQL):
  - `Throughput_uses_full_limit` — один сервер, `Workers = 16`, очередь с `ClusterMaxConcurrency = 4` объявлена без `FastQueue` (`MaxPollInterval` по умолчанию 10 с — плановые опросы не подменяют `leaseFreed`), `AckFlushInterval = 20 мс` явно в `configureServer` (`ServerTestHost.Fast` ставит 10 мс), 200 задач по 200 мс: за 10 с выполнено ≥ 160; монитор (как в Task 3) — одновременно взятых ≤ 4.
  - `Pause_stops_and_resume_continues` — `SetConcurrencyOverrideAsync(q, 0)` в этом процессе; `RefreshNow()` не ждёт обновления (§6.1), поэтому ждать (`Eventually`), пока `QueueSettings.Current.Limited` содержит `q`; затем поток enqueue в `q` 2 с: ни одна задача `q` не стартовала; `ResetConcurrencyOverrideAsync` → задачи выполняются.
  - `Limit_appears_after_refresh` — очередь без лимита, прямой `INSERT` в `queue_settings` с лимитом 1 (мимо менеджера), `QueueSettingsCache.RefreshNow()` и ожидание (`Eventually`), пока `Current.Limited` содержит очередь → дальше одновременно выполняется ≤ 1 задачи.
- [ ] **Step 3:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~DispatcherTests|FullyQualifiedName~CompletionSinkTests|FullyQualifiedName~QueueLimitTests"` — FAIL.
- [ ] **Step 4: Реализация** по §6.3: `QueueState.LimitReached` (обновляется каждым dequeue), `QueueState.LeaseFreed` (latched `int`); после dequeue при `LimitReached` — ветка «пусто»; `ApplyFlags`: при `LimitReached` `Signaled` и `SlotFreed` сбрасываются без действия, `LeaseFreed` → `NextPoll = now`; без признака `LeaseFreed` сбрасывается без действия. `CompletionSink.Finish` — после удаления из таблицы `onLeaseFreed?.Invoke(execution.Queue)` (исключение колбэка — `LogError`, не мешает отправке). `PgBoltServer`: `onLeaseFreed: q => dispatcher?.OnLeaseFreed(q)`.
- [ ] **Step 5:** Тот же прогон — PASS.
- [ ] **Step 6:** `git commit -m "feat(server): диспетчер у кластерного лимита — backoff и пробуждение после коммита исхода"`

### Task 11: Новый контракт `ISharedPoolDistribution`

**Files:**
- Modify: `src/PgBolt.Server/Runtime/SlotAllocator.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/SlotAllocatorTests.cs`, `tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs` (Modify: тестовая стратегия `DecliningDistribution`)

**Interfaces:**
- Produces: контракт спеки §7.2 дословно (`BeginPlan`, `Next` с `freeReserved` и `freeShared`, `OnTaken`); `StrictOrderDistribution` — прежнее поведение, новые члены пустые. `QueueSlots` получает `Index`.

- [ ] **Step 1: Падающие тесты** (записывающая фейковая стратегия): `BeginPlan` — ровно один раз на план, до первого `Next`, с теми же кандидатками (в том числе план без записей); `OnTaken(index, fromReserved)` — на каждый `Take` с его результатом; `freeShared` = `Workers − занятые общие`; `freeReserved` — свободные Reserved очереди; существующие тесты строгого порядка проходят; тестовые стратегии `Declining` и `ReverseOrder` в `SlotAllocatorTests` и `DecliningDistribution` в `DispatcherTests` переводятся на новую сигнатуру `Next` с пустыми `BeginPlan`/`OnTaken`, их поведение не меняется.
- [ ] **Step 2:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~SlotAllocatorTests"` — FAIL.
- [ ] **Step 3:** Реализация: в `PlanIterator` первый `NextEntry` вызывает `BeginPlan` после вычисления `candidates`; `Take` вызывает `OnTaken` под lock; тестовые стратегии (`SlotAllocatorTests`, `DispatcherTests`) — на новую сигнатуру с пустыми `BeginPlan`/`OnTaken`.
- [ ] **Step 4:** Тот же прогон и `--filter "FullyQualifiedName~DispatcherTests"` — PASS.
- [ ] **Step 5:** `git commit -m "refactor(server): контракт ISharedPoolDistribution с границей плана и учётом взятия"`

### Task 12: `WeightedDistribution`

**Files:**
- Create: `src/PgBolt.Server/Runtime/WeightedDistribution.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/WeightedDistributionTests.cs`, `tests/PgBolt.Server.Tests/Unit/PoolSimulation.cs`

**Interfaces:**
- Produces: `internal sealed class WeightedDistribution(IReadOnlyList<int> weights) : ISharedPoolDistribution` — алгоритм спеки §7.3 (состояние, `OnTaken`, `BeginPlan` шаги 1–5, `Next` шаги 1–4); `pass` — `double`. `PoolSimulation` (тестовый): настоящий `SlotAllocator` с заданной стратегией + цикл «план → dequeue (задаваемое число задач на очередь) → `Take` по слоту → освобождение через k итераций», счётчики взятых общих слотов по очередям на границах плана.

- [ ] **Step 1: Падающие тесты** (через `PoolSimulation`, 20 000 итераций, если не сказано иное):
  - `Shares_follow_weights` — `[Theory]` веса 1:1:1, 3:1, 5:2:1 × свободных общих слотов 1, 2, 3, 4, 8, backlog во всех: на каждой границе плана `|взято_q − доля_q · всего| ≤ 1,25`.
  - `Example_sequence` — `critical: 3, default: 1`, 1 слот → первые 8 выдач `c d c c c d c c`.
  - `Next_draw_is_local_per_entry` — 1:1:1, 4 слота, все берут целиком → записи 2, 1, 1.
  - `Idle_queue_gets_no_burst` — очередь пуста 3000 итераций (вернувшая 0; отдельно — неготовая; отдельно — сосед упёрся в `MaxConcurrency`), затем backlog: за первые 20 итераций её доля в пределах 1,25 слота от веса; пока пуста — остальные делят пул по весам.
  - `Empty_F_resets_plan_counters` — несколько планов, где все недобирают (взяли половину записи): `pass` не меняется, `took` не копится; затем backlog — доли в пределах 1,25.
  - `Reserved_freed_during_dequeue_is_not_short` — Reserved-слот очереди освобождается между `Next` и `Take`: запись не недобор, общим зачтено ровно занятое.
  - `Shared_slot_freed_mid_plan_goes_to_next_entry`; `Queue_ready_mid_plan_gets_remainder`; `Interrupted_plan_is_settled_by_next_BeginPlan`; `Reserved_does_not_change_pass`; `MaxConcurrency_caps_share`; `Ties_follow_declaration_order`.
  - `Pass_is_normalized` — на границе плана с непустым `F`: `min pass` по `F` = 0, остальные в `[−1, 2]` (через `internal double[] Pass` для тестов).
- [ ] **Step 2:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~WeightedDistributionTests"` — FAIL.
- [ ] **Step 3:** Реализация по §7.3 (тело алгоритма — спека §7.3).
- [ ] **Step 4:** Тот же прогон — PASS.
- [ ] **Step 5:** `git commit -m "feat(server): WeightedDistribution — взвешенный round-robin слотов общего пула"`

### Task 13: Веса из снимка и смена стратегии на лету

**Files:**
- Modify: `src/PgBolt.Server/Runtime/SlotAllocator.cs`, `src/PgBolt.Server/Runtime/PgBoltServer.cs:41`, `tests/PgBolt.Server.Tests/Unit/PoolSimulation.cs` (конструктор от готового `SlotAllocator` и хук «перед итерацией k»)
- Test: `tests/PgBolt.Server.Tests/Unit/SlotAllocatorWeightsTests.cs`, `tests/PgBolt.Server.Tests/Integration/QueueWeightsTests.cs`

**Interfaces:**
- Consumes: `WeightedDistribution` (Task 12), `QueueSettingsCache.Current.Weights` (Task 7).
- Produces: `SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, Func<FrozenDictionary<string, int>>? weights = null)`; `internal ISharedPoolDistribution Distribution { get; }` (для тестов); `PgBoltServer`: `new SlotAllocator(Options.Workers, Options.Queues, () => settings.Current.Weights)`.

- [ ] **Step 1: Падающие unit-тесты** (`SlotAllocatorWeightsTests`, подменяемый источник):
  - `No_weights_uses_strict_order`; `Any_weight_switches_to_weighted_with_default_one` (`Distribution is WeightedDistribution`, вектор `[w, 1, …]`); `Foreign_queue_weights_are_ignored`.
  - `Snapshot_applies_only_at_plan_boundary` — смена источника между `Next` одного плана не меняет `Distribution` до следующего плана.
  - `Changed_vector_resets_state` — strict → weighted → другие веса → strict: каждый раз новый экземпляр, `Pass` нулевые.
  - `Same_vector_in_new_snapshot_keeps_state` — новый объект словаря с теми же значениями → тот же экземпляр.
  - `Shares_after_switch` — `PoolSimulation` поверх `SlotAllocator` с подменяемым источником `Func<FrozenDictionary<string, int>>` (новый конструктор `PoolSimulation` принимает готовый `SlotAllocator` и необязательный хук `beforeIteration(int k)`, в котором тест меняет источник весов): смены 3:1 → 1:3, 5:2:1 → 1:2:5, 1:2:3:4 → 4:3:2:1, строгий порядок → 1:2:3 при 1, 2, 3, 4, 8 слотах — после смены на каждой границе плана отклонение ≤ 1,25 слота; очередь, пустая до смены, без залпа.
- [ ] **Step 2: Падающий интеграционный тест** (`QueueWeightsTests` с `[Collection(TimingSensitiveCollection.Name)]`: доля за 3 с зависит от времени, `ServerTestHost`): сервер `Workers = 4`, очереди `a`, `b` с backlog и задачами по 50 мс; `SetWeightOverrideAsync("a", 3)` и `("b", 1)` в этом процессе, затем ожидание (`Eventually`), пока `QueueSettings.Current.Weights` не станет `{a: 3, b: 1}` (`RefreshNow()` не ждёт обновления); замер 3 с — доля `a` считается только по задачам, завершённым после применения весов (счётчики обнуляются в момент, когда снимок отразил веса), ≈ 0,75 (±0,1); `ResetWeightOverrideAsync` обеих и ожидание `Weights` пустым → строгий порядок (`a` первой объявлена — `b` почти не выполняется).
- [ ] **Step 3:** `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~SlotAllocatorWeightsTests|FullyQualifiedName~QueueWeightsTests"` — FAIL.
- [ ] **Step 4:** Реализация по §7.1: поля «применённый вектор», «ссылка на снимок», «стратегия» под lock; в первом `NextEntry` плана — сравнение ссылки, затем вектора по значениям, затем `BeginPlan`.
- [ ] **Step 5:** Тот же прогон и `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Unit"` — PASS.
- [ ] **Step 6:** `git commit -m "feat(server): веса очередей из queue_settings со сменой стратегии на границе плана"`

### Task 14: Анализатор BOLT010 и BOLT011

**Files:**
- Create: `src/PgBolt.Generators/Analysis/QueueOptionsAnalysis.cs`
- Modify: `src/PgBolt.Generators/Analysis/PgBoltAnalyzer.cs`, `src/PgBolt.Generators/Analysis/Diagnostics.cs`, `src/PgBolt.Generators/KnownNames.cs`, `src/PgBolt.Generators/AnalyzerReleases.Unshipped.md`, `tests/PgBolt.Generators.Tests/PgBolt.Generators.Tests.csproj`, `tests/PgBolt.Generators.Tests/AnalyzerVerifier.cs`
- Test: `tests/PgBolt.Generators.Tests/QueueOptionsAnalyzerTests.cs`

**Interfaces:**
- Produces: `Diagnostics.ConflictingQueueSetting` — `"BOLT010"`, «Conflicting queue setting», сообщение `"Queue '{0}' sets {1} = {2} here and {3} at another call"` (дополнительная локация — другой вызов), Error, `WellKnownDiagnosticTags.CompilationEnd`; `Diagnostics.InvalidOptionValue` — `"BOLT011"`, «Invalid option value», `"{0} = {1} is invalid: {2}"`, Error. Правила BOLT011 (третий аргумент): `Reserved` — `must be >= 0`; `MaxConcurrency` — `must be >= 1` / `must be >= Reserved ({n})`; `ClusterMaxConcurrency` — `must be >= 1`; `Weight` — `must be in [1, 1000]`; `QueueSettingsCacheDuration` — `must be > 0 (use null to disable the cache)`. Аргумент `{1}`/`{2}` — текст выражения справа / значение (`null` — `null`).

- [ ] **Step 1: Падающие тесты** (`QueueOptionsAnalyzerTests`; `AnalyzerVerifier.AddPgBoltReferences(state, bool server = true, bool postgres = true)` по умолчанию добавляет сборки `PgBolt.Server` и `PgBolt.Postgres` (существующие тесты анализатора продолжают вызывать её без аргументов); добавляется перегрузка `VerifyAsync(string source, bool server, bool postgres, params DiagnosticResult[] expected)`, существующая `VerifyAsync(string source, params DiagnosticResult[] expected)` вызывает её с `true, true` (необязательные параметры после `params` недопустимы, перед ним сломали бы существующие вызовы); вариант «только Abstractions + Postgres» — `VerifyAsync(src, server: false, postgres: true, …)`): все случаи спеки §11 «Анализатор» — для `ClusterMaxConcurrency` и `Weight` по отдельности: разные константы; константа против лямбды без присваивания, вызова без `configure`, литерала `null`, присваивания `null`; последнее из нескольких присваиваний; одинаковые значения молчат; молчат при method group (присваивание 5 против лямбды с 5 и с 3), переменной-делегате, лямбде с `if` или вызовом, неконстантном значении, правой части с параметром лямбды; диагностика на аргументе имени у вызова без присваивания; расхождение только по `Weight` — на каждом вызове одна диагностика, по обеим — две на каждом; BOLT011 — каждая строка таблицы §4.3 и границы (`Weight` 1 и 1000 молчат; `MaxConcurrency = 2` при `Reserved = 3` в одной лямбде; `TimeSpan.FromSeconds(0)`, `TimeSpan.Zero`, `new TimeSpan(0, 0, -1)`); компиляция без `PgBolt.Server` (`server: false`) — правила BOLT010 молчат, BOLT011 по `PgBoltOptions` работает; существующие `AnalyzerTests` проходят.
- [ ] **Step 2:** `dotnet test tests/PgBolt.Generators.Tests --filter "FullyQualifiedName~QueueOptionsAnalyzerTests"` — FAIL.
- [ ] **Step 3:** Реализация по §4.3: `QueueOptionsAnalysis.TryCreate(Compilation)` ищет типы по metadata name (`PgBolt.Server.QueueOptions`, `PgBolt.Server.PgBoltServerOptions`, `PgBolt.Postgres.PgBoltOptions`); BOLT010 — `RegisterOperationAction(Invocation)` собирает вызовы `Queue(name, configure)` в потокобезопасную коллекцию, `RegisterCompilationEndAction` сравнивает; BOLT011 — `RegisterOperationAction(SimpleAssignment)` по свойствам. `PgBoltAnalyzer.Initialize` регистрирует `QueueOptionsAnalysis` до проверки `AnalysisState is null`.
- [ ] **Step 4:** `dotnet test tests/PgBolt.Generators.Tests` — PASS.
- [ ] **Step 5:** `git commit -m "feat(generators): BOLT010 и BOLT011 для настроек очередей"`

### Task 15: Dashboard — экран очередей

**Files:**
- Modify: `src/PgBolt.Dashboard/Data/DashboardStore.Overview.cs:46-58`, `src/PgBolt.Dashboard/Views/ListRows.cs`, `src/PgBolt.Dashboard/Components/Pages/QueuesPage.razor`, `src/PgBolt.Dashboard/Components/Parts/QueueRows.razor`, `src/PgBolt.Dashboard/Endpoints/OverviewEndpoints.cs:30-40`, `src/PgBolt.Dashboard/Endpoints/ActionEndpoints.cs`
- Create: `src/PgBolt.Dashboard/Components/Parts/QueueSettingsActions.razor`
- Test: `tests/PgBolt.Dashboard.Tests/Data/OverviewStoreTests.cs`, `tests/PgBolt.Dashboard.Tests/Data/PlanContractTests.cs`, `tests/PgBolt.Dashboard.Tests/Endpoints/QueueSettingsActionsTests.cs` (Create), `tests/PgBolt.Dashboard.Tests/Endpoints/OverviewPagesTests.cs`

**Interfaces:**
- Consumes: `IQueueSettingsManager`, `QueueSettings` (Task 4), `IQueueSettingsRefresher` (Task 4).
- Produces: `public sealed record QueueLine(QueueRow Stats, QueueSettings? Settings, int? Leased);` (`Views/ListRows.cs`); `DashboardStore.CountLeasedAsync(IReadOnlyList<string> queues, CancellationToken ct) → Task<IReadOnlyDictionary<string, int>>`; `QueueRows` и `QueuesPage` принимают `IReadOnlyList<QueueLine>` и `DashboardContext`; POST `actions/queues/limit?queue=…` (форма: `op` ∈ `set|unlimited|pause|reset`, `value`), `actions/queues/weight?queue=…` (`op` ∈ `set|none|reset`, `value`), `actions/queues/cache/reset`.

- [ ] **Step 1: Падающие тесты.**
  - `OverviewStoreTests` и `QueueSettingsActionsTests`: `Override_only_queue_is_listed_and_resettable` — после «Сбросить к значению из кода» из UI очередь пропадает из списка; строка `queue_settings` с переопределением лимита (без задач и серверов) входит в `ListQueuesAsync`, только со значениями из кода — нет; `CountLeasedAsync(["a", "b"])` — число взятых строк по очередям (план — Index Only Scan по `queue_leased`, добавить случай в `PlanContractTests`).
  - `QueueSettingsActionsTests`: каждая операция вызывает соответствующий метод менеджера с `updatedBy = "dashboard: <имя>"` и возвращает строку `<tr>` с новым значением и `HX-Trigger` (если очередь после действия выпала из списка — пустой фрагмент с `HX-Trigger`); `value` вне пределов или нечисло → 400; неизвестный `op` → 404; без `queue` → 400; при `ReadOnly` → 403 и кнопок в разметке нет; `cache/reset` вызывает `InvalidateCache()` и `RefreshNow()` фейка `IQueueSettingsRefresher` (без регистрации — без ошибки); лог Information.
  - `OverviewPagesTests`: колонки «Лимит» (`N / лимит`, «из кода» / «переопределён: кто, когда», «—»), бейдж «Пауза», «Вес»; подсказка у кнопки сброса кэша; обе страницы, `GET /queues` и `GET /queues/rows` (polling), отдают те же колонки (снапшота страницы очередей в репозитории нет — `OverviewSnapshotTests.Overview` относится к `OverviewPage` и не меняется, новый снапшот не заводится).
  - `PlanContractTests`: добавить `CountLeasedAsync` в `Every_screen_statement_avoids_seq_scans` и поднять число statement'ов `45 → 46` (комментарий к счётчику: обзор 8 → 9 с `CountLeased`).
- [ ] **Step 2:** `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~OverviewStoreTests|FullyQualifiedName~QueueSettingsActionsTests|FullyQualifiedName~OverviewPagesTests|FullyQualifiedName~PlanContractTests"` — FAIL.
- [ ] **Step 3: Реализация** по §8.1: `QueuesTemplate` — имена из `queue_stats ∪ live ∪ queue_settings WHERE override_concurrency_set OR override_weight_set`; `CountLeasedAsync` — `SELECT queue, count(*)::int FROM {schema}.queue WHERE lease_owner IS NOT NULL AND queue = ANY(@queues) GROUP BY queue` только по очередям со строкой настроек; оба endpoint'а, `GET /queues` и `GET /queues/rows` (в `OverviewEndpoints.cs`; второй сейчас не передаёт `Context`, добавить), строят `QueueLine` из трёх источников и передают `DashboardContext`; ответ действия — `QueueRows` из одной строки (`partial: true`), настройки строки перечитываются `GetAsync`; `QueueLine` одной очереди строится так: `Stats` — строка `ListQueuesAsync` с этим именем (ordinal), `Leased` — `CountLeasedAsync([queue])`, если строка `queue_settings` есть (иначе `null`); если очереди в `ListQueuesAsync` больше нет (после «Сбросить к значению из кода» у очереди без задач и серверов) — пустой фрагмент с `HX-Trigger`, htmx удаляет строку.
- [ ] **Step 4:** Тот же прогон — PASS.
- [ ] **Step 5:** `git commit -m "feat(dashboard): лимит, пауза и вес очередей на экране очередей"`

### Task 16: E2E, PgBouncer, документация

**Files:**
- Modify: `tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj` (`<ProjectReference Include="..\..\..\src\PgBolt.Server\PgBolt.Server.csproj" />`: сейчас сборка обрабатывает задачи без сервера, а тесты ниже требуют `AddPgBoltServer`), `tests/E2E/PgBolt.E2E.Tests/LocalJob.cs` (задача лимита), `tests/E2E/PgBolt.E2E.Tests/EndToEndTests.cs`, `tests/E2E/PgBolt.E2E.Tests/DashboardEndToEndTests.cs`, `CHANGELOG.md`
- Modify (документация, §10 спеки): `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md` (3b), `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` (`queue_owner` → `queue_leased`, `queue_settings` в списке таблиц), `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md` (`QueueOptions`, `PgBoltProcessingOptions`, шаг старта и остановка цикла, `IFetchStrategy`/`FetchResult`, диспетчер, `CompletionSink`, `ISharedPoolDistribution`, `IServerStore`), `docs/superpowers/specs/2026-10-03-pgbolt-dashboard-design.md` (экран очередей, индекс Processing), `docs/superpowers/specs/2026-10-10-pgbolt-queue-limits-design.md` (строка статуса; сигнатуры §10 приведены к решению плана 1: `QueueDefault`, `QueueDefaultConflict`, `ActiveQueueSetting` вместо кортежей)
- Test: те же E2E

**Interfaces:**
- Produces (тестовые): задача `e2e.limited` (`[Job("e2e.limited", Queue = "e2e-limited")] LimitedArgs(string Run, int N)` + `LimitedJob` в `LocalJob.cs`, `LimitedArgs` добавить в `E2EJson`): `Task.Delay(300, ct)`; статический `LimitedProbe.For(string run)` → счётчик прогона из `ConcurrentDictionary<string, Counter>` с текущими выполнениями, максимумом и числом выполненных (`Interlocked.Increment` на входе с обновлением максимума через `Interlocked.CompareExchange`, `Interlocked.Decrement` в `finally`); счётчики изолированы по метке прогона `Run` (классы xUnit v3 идут параллельно, `_Direct`/`_Bouncer` выполняются одновременно, `xunit.runner.json` нет), каждый тест берёт уникальную метку (`Guid.NewGuid().ToString("N")`) и ставит задачи только со своей меткой; счётчик общий для хостов одного теста (один процесс). Очередь `e2e-limited` общая для тестов, поэтому тесты, использующие её, выполняются поочерёдно: оба режима и оба теста — в одной коллекции с `DisableParallelization = true` (`[Collection("e2e-limited")]` с `[CollectionDefinition("e2e-limited", DisableParallelization = true)]`). «Два сервера» — два `ServiceProvider`/`IHost` в процессе теста с одной схемой и общим `AddPgBolt`/`AddPgBoltServer`, у каждого очередь `e2e-limited` с `ClusterMaxConcurrency = 3`. Все вызовы `Queue("e2e-limited", …)` в проекте (оба теста) объявляют очередь одним общим методом `static void LimitedQueue(QueueOptions q) => q.ClusterMaxConcurrency = 3;` — иначе BOLT010 (задача 14) остановит сборку: правило сравнивает все вызовы проекта.

- [ ] **Step 1: E2E-тесты** (оба режима подключения; в `Jobs_from_three_executor_assemblies_run_end_to_end` ожидание `Assert.Equal(3, registry.All.Count)` заменить на 4: задача `e2e.limited` попадает в `PgBoltJobRegistry` сборки): `Cluster_limit_holds_across_two_servers` — два хоста (`ServiceProvider`/`IHost` в процессе теста, одна схема, см. Interfaces) с одной очередью `e2e-limited` и `ClusterMaxConcurrency = 3`, 60 задач `e2e.limited` по 300 мс: максимум `LimitedProbe.For(run)` ≤ 3 и хотя бы раз 3, все выполнены; `Dashboard_limit_pause_and_weight` — хост с `AddPgBoltServer` (очередь `e2e-limited`) и dashboard; действия dashboard «Пауза» → выполнение остановилось: ждать (локальный цикл опроса с дедлайном 5 с и паузой 10 мс: в `PgBolt.E2E.Tests` помощника `Eventually` нет, он есть только в `PgBolt.Server.Tests`), пока число текущих выполнений `LimitedProbe.For(run)` станет 0 (до трёх начатых задач дорабатывают, §6.3), затем в течение 1 с счётчик выполненных не меняется; «Сбросить к значению из кода» → счётчик выполненных растёт снова; «Задать лимит» (2) → колонка показывает `N / 2`; «Задать вес» отражён в колонке; POST «Сбросить кэш настроек» → 200.
- [ ] **Step 2:** `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~Cluster_limit|FullyQualifiedName~Dashboard_limit"` — PASS (функциональность готова в задачах 1–15; падение — дефект этих задач, исправить в их коде).
- [ ] **Step 3:** `CHANGELOG.md` (Unreleased): breaking — `queue_settings` и `queue_leased` вместо `queue_owner` в `V001` (схемы части 6 отвергаются), `IFetchStrategy`/`ISharedPoolDistribution` внутренние — не упоминать; новое — `IQueueSettingsManager`, `QueueOptions.ClusterMaxConcurrency`/`Weight`, `PgBoltProcessingOptions.QueueSettingsRefreshInterval`, `PgBoltOptions.QueueSettingsCacheDuration`, `PgBoltStore` (`DequeueLimitedAsync`, `SyncQueueDefaultsAsync`, `GetActiveQueueSettingsAsync`, CRUD переопределений), BOLT010/BOLT011, dashboard; допущение §6.1. Roadmap §3b — «план: `docs/superpowers/plans/2026-10-12-pgbolt-queue-limits.md`. **Статус: реализовано.**»; в спеках core, worker и dashboard — правки по §10 (минимально: те, что перечислены в Files); строка статуса спеки 3b — «реализовано, план …»; в §10 спеки 3b кортежи заменить записями (решение плана 1).
- [ ] **Step 4:** `dotnet test PgBolt.slnx --filter "Category!=Load"` — PASS.
- [ ] **Step 5:** `git commit -m "test(e2e): кластерный лимит и веса; docs: changelog и roadmap 3b"`
