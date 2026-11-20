# PgBolt Dashboard (часть 6) — план реализации

> **Для исполнителя-агента.** План самодостаточен и не требует скиллов: протокол исполнения через субагентов, шаблоны их заданий, выбор моделей и требования к окружению описаны ниже («Протокол исполнения»). Шаги задач размечены чекбоксами (`- [ ]`).

**Goal:** Встраиваемый dashboard `PgBolt.Dashboard` (Minimal API + `.razor` через `HtmlRenderer` + htmx) с экранами очередей, задач, batches, расписаний, серверов и типов, действиями и запуском задачи по имени; статистика исходов от серверов (`stats_minutes`) и снимок живых состояний (`queue_stats`) без новых statement'ов на горячем пути.

**Architecture:** Схема (таблицы статистики, индексы выборок) — в `V001`; SQL статистики — в `PgBoltStore` (core). `PgBolt.Server`: `AckAsync` возвращает подтверждённые `job_id`, `CompletionSink` считает подтверждённые исходы в процессном `OutcomeStats`, `OutcomeStatsFlusher` пишет их одним upsert раз в `StatsFlushInterval` и финальным сбросом при остановке; кластерная задача `stats` (снимок `queue_stats` и retention `stats_minutes`) — отдельный цикл `ProcessingMaintenance`. Новый пакет `PgBolt.Dashboard` (Razor SDK, зависит только от `PgBolt.Postgres`): read-only `DashboardStore` (транзакция на statement, `READ ONLY`, `SET LOCAL statement_timeout`, keyset, обход порциями с бюджетом), endpoints одной `RouteGroup` с фильтрами доступа и ошибок, компоненты рендерятся `HtmlRenderer` на запрос, действия — только через `IJobClient` / `IRecurringJobManager`.

**Tech Stack:** .NET 10 (C# 14), ASP.NET Core 10 Minimal API, `Microsoft.AspNetCore.Components.Web.HtmlRenderer`, Razor SDK, htmx 2.0.4 (embedded), Npgsql 10, `System.Text.Json.Schema` (`JsonSchemaExporter`-вывод), xUnit v3, Verify.XunitV3, Testcontainers (PostgreSQL 17 + PgBouncer transaction mode), `Microsoft.AspNetCore.TestHost` 10.0.0, `Microsoft.Extensions.TimeProvider.Testing`.

**Spec:** `docs/superpowers/specs/2026-10-03-pgbolt-dashboard-design.md` (далее «спека», ссылки вида «§6.3»). Основа: `2026-09-26-pgbolt-core-design.md` («core §5.4»), `2026-09-28-pgbolt-worker-design.md` («worker §6.1»), `2026-09-29-pgbolt-storage-maintenance-design.md` («часть 2 §5.7»), `2026-09-30-pgbolt-scheduler-design.md` («часть 4 §9»), `2026-10-02-pgbolt-workflows-design.md` («часть 5 §10»). Образец стиля кода и тестов — `src/PgBolt.Postgres/Storage/*.cs`, `src/PgBolt.Server/Runtime/*.cs`, `tests/PgBolt.Postgres.Tests`, `tests/PgBolt.Server.Tests`.

## Global Constraints

- Только .NET 10 (`net10.0`); `TreatWarningsAsErrors=true` (`Directory.Build.props`). `PgBolt.Abstractions`, `PgBolt.Postgres`, `PgBolt.Server` — `IsAotCompatible=true`, без reflection. `PgBolt.Dashboard` — **не** AOT и не trimmable (§2), `Microsoft.NET.Sdk.Razor`, `FrameworkReference Microsoft.AspNetCore.App`, единственная зависимость-проект — `PgBolt.Postgres` (не `PgBolt.Server`).
- SQL — строковые константы с `{schema}`, подставляемым `Render` (как в `PgBoltStore`). Подключение напрямую или через PgBouncer transaction mode: без `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` (кроме `SET LOCAL` и `SET TRANSACTION …`: оба действуют до конца транзакции и совместимы с PgBouncer transaction mode), temp tables; транзакции только `BeginTransactionAsync` / `CommitAsync`; `BEGIN`/`COMMIT` в тексте SQL рабочего кода запрещены.
- Чтение dashboard (§6): каждый statement — в собственной транзакции READ COMMITTED, первой командой `SET TRANSACTION READ ONLY`, затем `SET LOCAL statement_timeout = <QueryTimeout в мс>`; `PrepareAsync` только при `PgBoltOptions.UsePreparedStatements = true`; никакого REPEATABLE READ. Ни один запрос не делает Seq Scan по `jobs`, `queue`, `job_events`. Соединение множества ключей (порция §6.3, Processing) с `jobs` — поиск по PK: `CROSS JOIN LATERAL (SELECT … FROM {schema}.jobs j WHERE j.id = k.job_id AND <фильтры> OFFSET 0) j` (отклонение 19).
- Константы (§6, §6.3, §7): страница — 50 строк; порция обхода — 2000 ключей; выбранных задач за запрос — до 100; порция retention `stats_minutes` — 10 000 строк; захват `stats` — `0,8 × StatsSnapshotInterval`; лог ошибок сброса счётчиков и задачи `stats` — не чаще раза в минуту.
- Коды: `job_events.kind` — `JobEventKind` (1 Retry, 2 Failed, 3 LeaseExpired, 4 Cancelled, 5 Requeued, 6 Triggered, 7 NoExecutor); Failed-список — `kind IN (2, 3, 7)` + `jobs.state = 3`; Cancelled — `kind = 4` + `jobs.state = 4`; оба — `jobs.finished_at = job_events.at`. Продолжения-задачи — `jobs.continuation IN (1, 2)` (родитель-задача) и `IN (3, 4)` (родитель-batch); код 5 продолжением не считается.
- Контракт HOT (core §4.2): новых индексов `jobs` по изменяемым колонкам нет; `jobs_parent` (`parent_id, id`) и `jobs_recurring` (`recurring_id, id`) — по неизменяемым.
- Горячий путь (§1): новых statement'ов нет; счётчики исходов — в памяти, сброс — один statement на процесс раз в `StatsFlushInterval`, не больше одного сброса одновременно.
- Хост (§1, §5.1): никаких `AddRazorComponents`, `MapRazorComponents`, `UseAntiforgery`, статических файлов; группа dashboard вызывает `.DisableAntiforgery()`; компоненты фреймворка, требующие сервисов `AddRazorComponents` (`NavLink`, `NavigationManager`, `EditForm`, `AntiforgeryToken`, `@rendermode`), не используются; `@layout` не используется — оболочка `Shell`.
- CSP-совместимость (§9): inline-скриптов и `hx-on` / фильтров `hx-trigger` нет; `<meta name="htmx-config">` — `allowEval: false`, `historyCacheSize: 0`, `includeIndicatorStyles: false`, `responseHandling` со свапом `[45]..`. `MarkupString` для данных из БД не используется; args / message / exception — текстом в `<pre>`.
- HTML-ответы — `Vary: HX-Request, HX-History-Restore-Request`, `Cache-Control: no-store`; ассеты — путь с хешем содержимого, `Cache-Control: public, max-age=31536000, immutable`.
- Razor-компоненты: у `[Parameter]` с не-nullable ссылочным типом — инициализатор (`= default!` / `= null!` / `= ""`), иначе CS8618 при `TreatWarningsAsErrors` (проверено сборкой Razor-проекта).
- htmx: наследование атрибутов отключено глобально (`"disableInheritance":true` в `htmx-config`; htmx 2.0.4 проверен прогоном под jsdom). Причина: `hx-target` и `hx-swap` по умолчанию наследуются от любого предка, и boosted-ссылка (`<a href>` под `hx-boost`) внутри `<tbody hx-target="this">`, `<ul hx-target="this">` продолжений или формы с `hx-target="#bulk-result"` свапала бы страницу в этот контейнер, а унаследованный `show:window:top` прокручивал бы страницу наверх при каждом ответе polling. `hx-disinherit="hx-target hx-swap"` не годится: в htmx 2.0.4 он обрывает поиск значением `'unset'` и ссылка получает цель `<body>` (оболочка уничтожается), а не `#main`. Оболочка: `<body hx-boost="true" hx-target="#main" hx-swap="innerHTML show:window:top" hx-inherit="hx-boost hx-target hx-swap">` — только `<body>` передаёт потомкам эти три атрибута (навигация: ссылки, формы фильтров, пейджер); никакой другой элемент `hx-inherit` / `hx-disinherit` не использует. Следствие: каждый элемент с `hx-get` / `hx-post` / `hx-trigger` вне навигации (polling, подгрузка `load`, кнопки действий — в том числе кнопки массовых действий внутри формы, формы «Поставить» / JSON, ссылки «ещё») задаёт собственные `hx-target` и `hx-swap` (`innerHTML` или `outerHTML`, без модификаторов `show:` / `scroll:`); остальные наследуемые атрибуты (`hx-push-url`, `hx-include`, `hx-confirm`, `hx-vals`, `hx-headers`, `hx-indicator`, `hx-select`, `hx-sync`, `hx-params`) тоже только на самом элементе — сейчас план использует `hx-push-url` (форма фильтра, ссылки пейджера, выбор очереди) и `hx-include` (кнопка «JSON») и ни разу не полагается на их наследование; `hx-confirm` / `hx-vals` / `hx-headers` / `hx-indicator` не применяются.
- Частичные htmx-ответы (панели polling, `queues/rows`, строки и карточки после действий, продолжения, результаты форм) — без `<title>` (`DashboardView.PageAsync(…, partial: true)`, отклонение 23): htmx 2 обновляет `document.title` при любом свапе, содержащем `<title>`.
- Коммиты: Conventional Commits; автор и коммиттер — `NightNo1r <57496218+NightNo1r@users.noreply.github.com>` (`git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit ...`); строк `Co-Authored-By` **нет**.

### Отклонения от спеки и уточнения (решения плана)

1. **Бенчмарки не меняются.** Спека §11 говорит «обновляются тесты и бенчмарки» для новой сигнатуры `AckAsync`, но `benchmarks/PgBolt.Benchmarks` не вызывает `AckAsync` (копии `CompletionSink` работают через заглушку `AckSendStub`, `Copies/CompletionSinkContracts.cs`). Обновляются только тесты (Task 2).
2. **`AckAsync` возвращает `IReadOnlyList<Guid>`** (массив `acked` из SQL), остаётся публичным; `FlushOutcomeStatsAsync`, `SnapshotQueueStatsAsync`, `TrimStatsAsync` — `internal` (как методы resolver'а), запись строки сброса — `internal readonly record struct OutcomeStatsRow` в `StoreRecords.cs`.
3. **`OutcomeStats` — поле `PgBoltProcessingService`**, а не регистрация в DI: сервис — единственный hosted service процесса (worker §4.1), он создаёт один экземпляр и передаёт его в каждый `PgBoltServer` → `CompletionSink`. Это и есть «процессный синглтон» §4.1 без новой регистрации.
4. **`ShutdownReserve` остаётся 2 с.** Финальный сброс — один upsert (миллисекунды) после `FlushAndStopAsync`; резерв в 2 с его покрывает. Константе добавляется XML-комментарий (в резерв входит финальный сброс; сейчас комментария нет), значение и текст предупреждения не меняются.
5. **Retention `stats_minutes`:** store удаляет одну порцию (`TrimStatsAsync(retention, limit)` → удалено строк), цикл «пока порция полная» — в `ProcessingMaintenance.StatsOnceAsync`, как `SweepOnceAsync`. Ошибка прохода `stats` — `Complete` с `last_error` и без ускоренного повтора (`errorRetryDelay: null`): следующий проход — через интервал.
6. **Ограничитель логов** — общий класс `LogThrottle(MonoClock, TimeSpan)` в `PgBolt.Server/Runtime`; им пользуются `OutcomeStatsFlusher` (Warning ошибки сброса) и `RunTaskAsync` задачи `stats` (Error). Остальные задачи обслуживания логируют, как раньше.
7. **Компоненты Razor и модели представлений — `public`.** Razor генерирует `public partial class` для `.razor`, а параметр компонента не может иметь менее доступный тип (CS0051). Компоненты — в `PgBolt.Dashboard.Components`, модели (`Views/*.cs`) — в `PgBolt.Dashboard.Views`, все с `[EditorBrowsable(EditorBrowsableState.Never)]` и XML-doc «внутренний тип dashboard, не входит в контракт пакета». `DashboardStore`, курсоры, фильтры, endpoints — `internal`.
8. **Текстовые идентификаторы — в query string.** id расписания (до 200 любых символов, `RecurringDefinition`) и имя типа задачи передаются как `?id=` / `?name=` (`recurring/item?id=…`, `types/enqueue?name=…`, `actions/recurring/pause?id=…`), а не сегментом пути: `/` и `%2F` в сегменте ломают маршрутизацию.
9. **Без пагинации — ограниченные таблицы:** очереди, серверы, задачи обслуживания, типы задач читаются целиком (их число ограничено конфигурацией кластера, а не данными). Recurring — keyset по `id` (курсор-строка, форма `Text`).
10. **Члены batch** (`jobs WHERE batch_id = @id`, `jobs_batch`) дополнительно ограничены `id >= @lower` (UUIDv7 от `created_at` batch минус сутки), как продолжения §6.2: члены создаются в той же отправке workflow, что и batch; граница отсекает партиции. Batch-продолжения и вложенные batches — keyset по `id` (по 50).
11. **Подмножество схемы §8 — уточнения** (сверено с выводом `JsonSchemaExporter` .NET 10, который учитывает аннотации nullability): `type` — строка либо массив `"string"` + `"integer"` / `"number"` с `pattern` и необязательным `"null"` (вывод при `JsonNumberHandling.AllowReadingFromString`, контексты с `JsonSerializerDefaults.Web`; `int?` → `["string","integer","null"]`) — integer / number, Nullable при `"null"`; `type` с `"null"` в массиве и `enum` с `null` — nullable; `string` без `"null"` (аннотация `string`, не `string?`) и вложенный объект без `"null"` — не nullable; `enum` без `type` (`{"enum":["Red","Green"]}`) — Enum; `required` экспортёра перечисляет все параметры конструктора, в том числе nullable: обязательное nullable-поле с пустым значением даёт `null`, а не ошибку; корень `["object","null"]` — объект; `format` кроме `date-time` и `uuid` — строка без проверки формата; аннотации (`title`, `description`, `default`, `$comment`) игнорируются; схема `true` / `{}` (любое значение) и любые ключевые слова сверх перечисленных (`items`, `anyOf`, `oneOf`, `allOf`, `additionalProperties`, `$ref`, `const`, `not`, `if`, …) — только режим JSON.
12. **Опции dashboard валидируются** (спека пределов не задаёт): все интервалы `> 0` и `≤ 1 сут`, `RefreshInterval ≥ 1 с`, `QueryTimeout ≥ 1 мс`; `ActionsPolicy` — `null` или непустая. Ошибка — `InvalidOperationException` из `AddPgBoltDashboard`.
13. **«Документация пакета» (§5.3: mesh-sidecar, `ForwardedHeaders` за TLS-прокси)** — XML-doc `<remarks>` у `MapPgBoltDashboard` и `PgBoltDashboardOptions.AllowAnonymousRemote` плюс строки CHANGELOG; отдельный README не создаётся.
14. **Базовый путь** вычисляется на запросе: `PathBase` + первые `N` сегментов пути, где `N` = число сегментов шаблона маршрута endpoint'а минус число сегментов его локального шаблона (метаданные `DashboardEndpoint`). Работает и при `UsePathBase`, и при вложении в группу хоста. Ссылки и ассеты в разметке — относительные, оболочка задаёт `<base href="{базовый путь}/">`.
15. **Результат действия, неприменимого к текущему состоянию** (гонка §7, «задача уже завершена»), — 200 с сообщением; 404 — только отсутствующий объект (§10). `UnknownJobException` при «Поставить» (тип исчез) — 200 с сообщением в форме; `ArgumentException` из `EnqueueAsync` (зарезервированный префикс `pgbolt:` и т. п.) — 400 с ошибкой у поля.
16. **Хук наблюдения statement'ов.** `DashboardStore.OnStatement` (`internal`, только тесты) вызывается перед каждым statement'ом с его SQL и привязкой параметров: тесты контракта планов (Task 11) выполняют `EXPLAIN (FORMAT JSON)` ровно тех statement'ов, которые выполняет экран.
17. **`PgBoltStore.Derive`** (производное состояние, core §2) становится `internal static` и переиспользуется `DashboardStore` для строк продолжений, членов batch и истории расписаний.
18. **Endpoint- и snapshot-тесты — только `Direct`** (поведение HTTP от режима подключения не зависит); интеграционные тесты `DashboardStore` — `_Direct` и `_Bouncer`; E2E — оба режима (§12); тесты планов — `Direct`, `[Trait("Category", "Slow")]`.
19. **Форма соединения с `jobs` и фикстура планов.** Порции (§6.3) и Processing соединяют множество ключей с `jobs` как `CROSS JOIN LATERAL (SELECT … FROM {schema}.jobs j WHERE j.id = k.job_id AND <фильтры> OFFSET 0) j`. Обычный `JOIN` по множеству строк на PG17 уходит в Hash / Merge Join с Seq Scan по партициям `jobs` (проверено `EXPLAIN` при заполненных партициях), а `OFFSET 0` ограждает подзапрос и оставляет поиск по PK партиций: `Nested Loop → CTE Scan → Append → Index Scan jobs_*_pkey` (так читается «nested loop с runtime pruning» §6.1). Простые выборки с `LIMIT 51` (Scheduled, Enqueued, Awaiting без порции; продолжения, история) остаются обычным `JOIN`: nested loop по PK проверен. Фикстура Task 11 заполняет **все** присоединённые партиции `jobs` и `job_events` (§12: «на заполненной схеме»): у пустой партиции после `ANALYZE` `relpages = 0`, и планировщик выбирает для неё Seq Scan даже в параметризованном Nested Loop.
20. **«Без Sort» у Failed / Cancelled — как в §12: без Sort над диапазоном индекса.** Порция читает ≤ 2000 ключей в CTE `k`; сортировка этого CTE (последний просмотренный ключ, внешний `ORDER BY` по строкам `m`) допустима, Sort / Incremental Sort над Index Scan по `job_events_*` — нет. Тест Task 11 проверяет именно это (с учётом того, что тело CTE в JSON-плане — дочерний узел `InitPlan` корня).
21. **Ошибки циклов `stats` в `LoopAsync`.** §4.2 («Ошибка в лог — не чаще раза в минуту») распространяется и на исключения захвата / `Complete`, выходящие из `RunTaskAsync` в `LoopAsync` при недоступной БД: `LoopAsync` получает необязательный `LogThrottle`, цикл `stats` передаёт тот же ограничитель, что и `RunTaskAsync`.
22. **Вкладка Enqueued без очереди.** Страница `jobs/enqueued` без `queue` — 200 с выбором очереди (§6.1: список всегда в разрезе очереди; §10: 400 — для невалидного параметра, а клик по вкладке не должен вести на страницу ошибки). `DashboardStore.ListJobsAsync(Enqueued, queue: null)` по-прежнему бросает `DashboardBadRequestException`.
23. **Частичные ответы без `<title>`.** `DashboardView.PageAsync` получает `bool partial = false`: при `partial` фрагмент — без `<title>` (панели polling, `queues/rows`, строки и карточки после действий, продолжения, результаты форм). Ошибки и отказ доступа в htmx-запросе несут `<title>`, только если цель свапа — `#main` (`DashboardView.TargetsMain`: заголовок `HX-Target: main`), иначе polling менял бы заголовок вкладки на «Ошибка».
24. **Повторная отмена batch.** `CancelBatchAsync` при повторе возвращает тот же `CancellationRequested`, поэтому «отмена уже запрошена» выводится из `GetBatchAsync().CancellationRequested` до вызова.
25. **`QueryTimeout` — только для чтений `DashboardStore`.** Критерий §1 «каждый запрос ограничен `QueryTimeout`» относится к SQL самого dashboard. Карточки задачи, batch и расписания, `parentCreatedAt` для продолжений и перечитывание после действий идут через API core (`IJobClient.GetAsync` / `GetBatchAsync`, `IRecurringJobManager.GetAsync`, как велит §6.2) — это поиски по PK без `SET LOCAL statement_timeout`; core не меняется. Ограничение — строкой в CHANGELOG (Task 17).

## Требования к окружению

Проверить до Task 1; при невыполнении — остановиться и доложить.

| Требование | Проверка | Ожидаемо |
|---|---|---|
| .NET SDK 10 | `dotnet --version` | `10.0.*` |
| Docker | `docker info` | без ошибок |
| Сеть до unpkg (Task 5) | `curl -sSfI https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js` | `HTTP/2 200` |
| Сборка зелёная | `dotnet test PgBolt.slnx --filter "Category!=Load"` | все тесты проходят |
| Ветка | `git branch --show-current` | `feat/dashboard` (`git switch feat/dashboard`; `git pull --ff-only` — только если `git rev-parse --abbrev-ref @{upstream}` успешен: ветка может не иметь upstream) |

Docker недоступен — не заменять базу моками и не пропускать интеграционные тесты; остановиться и доложить. Тесты — VSTest: `dotnet test tests/<Проект> --filter "FullyQualifiedName~<Класс>"`. Интеграционные тестовые классы — абстрактные, с наследниками `_Direct` и `_Bouncer` (как `MaintenanceTasksTests`); тесты на `EXPLAIN` — только `Direct`, с `[Trait("Category", "Slow")]` (как `DequeuePlanTests`).

## Протокол исполнения

Исполняется основной сессией (**координатор**) через субагентов — инструментом запуска агента (в Claude Code — `Agent`, тип `general-purpose`, запуск в foreground: следующий шаг зависит от результата). Скиллы не нужны. Состояние хранится в самом плане (чекбоксы) и в git, поэтому прерванное исполнение продолжается с первой неотмеченной задачи.

### Роли
- **Координатор** — основная сессия. Ведёт задачи строго по порядку (Task 1 → Task 17), запускает исполнителя и ревьюера по шаблонам ниже, проверяет их отчёты, отмечает чекбоксы. Код задач сам не пишет.
- **Исполнитель** — свежий субагент на задачу (и на каждый раунд исправлений), модель из таблицы.
- **Ревьюер** — свежий субагент на задачу, всегда Opus 5.5.

Если среда не поддерживает субагентов, координатор выполняет обе роли сам, последовательно, с тем же чек-листом.

### Модели

| Задачи | Исполнитель | Почему |
|---|---|---|
| 1, 17 | Sonnet 5.5 (`claude-sonnet-5-5`, параметр `model: "sonnet"`) | DDL и тесты схемы; документация |
| 2–16 | Opus 5.5 (`claude-opus-5-5`, `model: "opus"`) | Финализация и счётчики, конкурентный сброс и остановка, захват задач обслуживания, keyset и обход порциями, планы PostgreSQL, ASP.NET Core (авторизация, antiforgery, HtmlRenderer), CSRF, PgBouncer |
| Ревью каждой задачи и итоговое ревью ветки | Opus 5.5 (`model: "opus"`) | Сверка со спекой, Seq Scan / сортировки, удержание снимков, безопасность HTTP |

### Подготовка (один раз)
1. Проверить «Требования к окружению»; при невыполнении — остановиться и доложить.
2. Ветка исполнения — `feat/dashboard` (в ней же спека и этот план): `git switch feat/dashboard`, затем `if git rev-parse --abbrev-ref @{upstream} >/dev/null 2>&1; then git pull --ff-only; fi`. Рабочее дерево чистое (`git status --short` пуст).
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
7. Конец фазы (после Task 4, 11, 16) — `dotnet test PgBolt.slnx --filter "Category!=Load"`; падение — остановиться и доложить.

### Завершение
1. **Итоговое ревью ветки** — ревьюер (Opus) по шаблону «Ревьюер», но предмет — `git diff main..HEAD` (код; изменения `docs/` — только на согласованность), текст задачи заменяется ссылкой на весь план и спеку; фокус — сквозные проблемы: согласованность задач, учёт исходов и остановка, планы запросов и удержание снимков, безопасность HTTP (локальность, CSRF, политики, экранирование), соответствие спеке целиком. `CHANGES` → исправление и повторное ревью по тем же правилам (не более двух раундов).
2. `dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный.
3. `git push -u origin feat/dashboard`; PR не создавать без запроса пользователя. Доложить: SHA, итог тестов, отклонения от плана из отчётов исполнителей.

### Шаблон «Исполнитель»

```text
Ты реализуешь одну задачу плана в репозитории <абсолютный путь>, ветка feat/dashboard (текущий HEAD: <SHA>).
План: docs/superpowers/plans/2026-10-05-pgbolt-dashboard.md. Спека: docs/superpowers/specs/2026-10-03-pgbolt-dashboard-design.md.
Прочитай в плане разделы «Global Constraints» (включая «Отклонения от спеки»), «Контракт внутренних типов», «Правила для
исполнителя» и задачу «Task N» целиком; разделы спеки, на которые ссылается задача. Выполни шаги задачи строго по порядку:
падающий тест → убедиться, что падает → реализация → тесты проходят → коммит командой из плана (автор NightNo1r, без
Co-Authored-By). Чекбоксы плана не отмечай. Не делай работу других задач; SQL, имена и сигнатуры плана — эталон, отклонения
только ради компиляции, несовместимости API Npgsql/ASP.NET Core/PostgreSQL или явной ошибки плана. Две безуспешные попытки
исправить один падающий тест → остановись.
Ответ строго в формате:
STATUS: DONE | BLOCKED
SHA: <HEAD после коммита>
TESTS: <команда> → passed N / failed M
DEVIATIONS: <список отклонений от плана с причинами или «нет»>
NOTES: <для BLOCKED — вывод ошибки и гипотезы>
```

### Шаблон «Ревьюер»

```text
Ты ревьюер задачи Task N плана docs/superpowers/plans/2026-10-05-pgbolt-dashboard.md в репозитории <абсолютный путь>.
Прочитай задачу, «Global Constraints», «Контракт внутренних типов», «Чек-лист ревьюера» и разделы спеки
docs/superpowers/specs/2026-10-03-pgbolt-dashboard-design.md, на которые ссылается задача.
Изменения: `git diff <BASE>..HEAD`. Отчёт исполнителя: <отчёт целиком>.
Проверь по чек-листу ревьюера; сам запусти тесты задачи (команды из шагов «Запустить»). Ничего не редактируй.
Ответ: APPROVE или CHANGES; для CHANGES — нумерованный список «файл:строка — что не так — чем подтверждается — как исправить».
Стиль не оценивай, если он не нарушает Global Constraints.
```

### Шаблон «Исправление»

```text
Ты исправляешь замечания ревью по задаче Task N плана docs/superpowers/plans/2026-10-05-pgbolt-dashboard.md
в репозитории <абсолютный путь>, ветка feat/dashboard (HEAD: <SHA>). Прочитай задачу, «Global Constraints», «Контракт
внутренних типов», «Правила для исполнителя». Замечания: <список ревьюера целиком>.
Исправь каждое новым коммитом (Conventional Commits, автор через git -c user.name=NightNo1r
-c user.email=57496218+NightNo1r@users.noreply.github.com, без Co-Authored-By); не ослабляй тесты. С замечанием не согласен —
не исправляй, а объясни в отчёте с доказательством. Ответ — в формате шаблона «Исполнитель» плюс FIXED/REJECTED по номерам замечаний.
```

### Чек-лист ревьюера
- Реализовано всё из шагов; тесты задачи есть и проходят (интеграционные — в `_Direct` и `_Bouncer`, кроме явно помеченных Direct-only).
- Имена и сигнатуры совпадают с «Контрактом внутренних типов».
- Счётчики (§4.1): учитывается только ответ store — ack по возвращённым id, retry / fail по `FailOutcome.Retried` / `Failed`; `Cancelled`, `LeaseLost`, release, `ConfirmCancel`, sweeper — нет; бакет — минута UTC по `TimeProvider` в момент подтверждения; одновременно не больше одного сброса; ошибка сброса возвращает значения слиянием; финальный сброс — после остановки всех серверов и цикла, пропускается при отменённом токене.
- Задача `stats` (§4.2): только при `RunMaintenance`, отдельный цикл, захват `0,8 × StatsSnapshotInterval`, `Complete` с ошибкой каждым проходом, Error в лог не чаще раза в минуту.
- Чтение (§6): транзакция на statement, `READ ONLY`, `SET LOCAL statement_timeout`, `PrepareAsync` только при `UsePreparedStatements`; keyset без `OFFSET` для пагинации (`OFFSET 0` как барьер оптимизатора в `CROSS JOIN LATERAL` допустим и обязателен, отклонение 19); списки с предикатом вне индекса — порциями по 2000 ключей с проверкой бюджета между порциями; курсор при исчерпании бюджета — последний просмотренный ключ.
- Ни одного Seq Scan по `jobs` / `queue` / `job_events` в тестах планов; условия индексов совпадают с частичными предикатами (`kind IN (2, 3, 7)`, `kind = 4`, `continuation IN (1, 2)` / `(3, 4)`, `run_at < 'infinity'`).
- HTTP (§5): без авторизации на группе и без `AllowAnonymousRemote` — только loopback без `Forwarded` / `X-Forwarded-For` / `X-Real-IP`; POST — `HX-Request: true` и `Sec-Fetch-Site: same-origin` либо совпадающий `Origin`; `ReadOnly` и отказ `ActionsPolicy` — кнопки не рендерятся, POST → 403; группа с `DisableAntiforgery()`.
- Разметка: экранирование Razor, без `MarkupString` для данных БД, без inline-скриптов, `htmx-config` по §9, `Vary` / `Cache-Control` у HTML; у каждого элемента с `hx-get` / `hx-post` / `hx-trigger`, кроме навигации, явный `hx-swap` без `show:`; частичные ответы без `<title>`; в `htmx-config` `disableInheritance: true`, у `<body>` `hx-inherit="hx-boost hx-target hx-swap"`, нигде нет `hx-disinherit` / `hx-inherit` (кроме `<body>`) и атрибутов htmx, на наследование которых рассчитано; у не-nullable `[Parameter]` инициализатор.
- Нет `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` вне `SET LOCAL` / `SET TRANSACTION`, `BEGIN`/`COMMIT` в тексте SQL рабочего кода; горячий путь без новых statement'ов.
- В runtime `PgBolt.Server` нет `Task.Delay`/таймеров мимо `TimeProvider`; unit-тесты не зависят от реального времени (кроме `Eventually`).
- Тесты не ослаблены: нет `Skip`, удалённых assert'ов, подгонки ожидаемых значений; снимки Verify просмотрены, а не приняты вслепую.
- Сборка без предупреждений (`TreatWarningsAsErrors`).
- Коммиты — Conventional Commits, автор NightNo1r, без `Co-Authored-By`.

### Правила для исполнителя
- Не делать работу будущих задач. Не менять тесты ради прохождения, если задача этого не предписывает.
- SQL, имена и сигнатуры плана — эталон; отклонения — только для устранения ошибки компиляции, несовместимости API или явной ошибки плана; каждое — в отчёт `DEVIATIONS`.
- Если имя или сигнатура из «Контракта внутренних типов» не компилируется — не переименовывать, а остановиться (`BLOCKED`): на них опираются соседние задачи.
- Если `EXPLAIN`-тест показывает другой план, чем ожидает задача, — не ослаблять assert, а доложить в `NOTES` с текстом плана (`BLOCKED`); исключение — «Допустимые исправления» Task 11.
- Снимки Verify: первый прогон создаёт `*.received.html`; просмотреть, убедиться, что разметка соответствует задаче, переименовать в `*.verified.html` и закоммитить.
- Две безуспешные попытки исправить один падающий тест → `STATUS: BLOCKED` с выводом и гипотезами.

## Контракт внутренних типов

Соседние задачи опираются на эти имена и сигнатуры; менять их нельзя. В скобках — задача, которая вводит член.

```csharp
// PgBolt.Postgres
public sealed partial class PgBoltStore
{
    public Task<IReadOnlyList<Guid>> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct);       // Task 2 (было Task<int>)
    internal Task FlushOutcomeStatsAsync(IReadOnlyList<OutcomeStatsRow> rows, CancellationToken ct);                     // Task 2
    internal Task SnapshotQueueStatsAsync(CancellationToken ct);                                                         // Task 2
    internal Task<int> TrimStatsAsync(TimeSpan retention, int limit, CancellationToken ct);                              // Task 2: удалено строк
    internal static JobState Derive(short stored, bool inQueue, bool ready, Guid? leaseOwner, bool awaiting);           // Task 2: private → internal
}
internal readonly record struct OutcomeStatsRow(DateTime BucketUtc, Guid ServerId, string Queue, int Succeeded, int Failed, int Retried); // Task 2
internal static class UuidV7 { static Guid LowerBound(DateTimeOffset at); }   // Task 10: 48-битный префикс ms от at, остальное нули

// PgBolt.Server
internal interface IServerStore   // изменённый и новые члены
{
    Task<IReadOnlyList<Guid>> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct);           // Task 2
    Task FlushOutcomeStatsAsync(IReadOnlyList<OutcomeStatsRow> rows, CancellationToken ct);                             // Task 2
    Task SnapshotQueueStatsAsync(CancellationToken ct);                                                                 // Task 2
    Task<int> TrimStatsAsync(TimeSpan retention, int limit, CancellationToken ct);                                      // Task 2
}
internal sealed class OutcomeStats(TimeProvider time)                                                                    // Task 3
{ void Record(Guid serverId, string queue, int succeeded = 0, int failed = 0, int retried = 0);
  IReadOnlyList<OutcomeStatsRow> Take(); void Restore(IReadOnlyList<OutcomeStatsRow> rows); }
internal sealed class LogThrottle(MonoClock clock, TimeSpan period) { bool TryEnter(); }                                 // Task 3
internal sealed class OutcomeStatsFlusher(IServerStore store, OutcomeStats stats, TimeSpan interval, MonoClock clock, ILogger logger) // Task 3
{ void Start(); Task StopAsync(CancellationToken ct); internal Task FlushOnceAsync(CancellationToken ct); internal int IdleWaits { get; } }
// CompletionSink(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options, MonoClock clock, ILogger logger, OutcomeStats stats)  // Task 3
// PgBoltServer(ServerDefinition, IServerStore, JobRegistry, IServiceScopeFactory, IReadOnlyList<Type>, IWakeupSignal, MonoClock, ILoggerFactory, OutcomeStats stats) // Task 3
// PgBoltProcessingService: internal OutcomeStats Stats { get; }  internal OutcomeStatsFlusher? Flusher { get; }       // Task 3
public sealed class PgBoltProcessingOptions { TimeSpan StatsFlushInterval = 15 с /*Task 3*/; TimeSpan StatsSnapshotInterval = 10 с; TimeSpan StatsRetention = 7 сут /*Task 4*/; }
internal sealed class ProcessingMaintenance                                                                               // Task 4
{ internal const int StatsTrimBatchSize = 10_000; internal const double StatsClaimFactor = 0.8;
  internal static readonly TimeSpan StatsErrorLogPeriod /* 1 мин */; internal Task<int> StatsOnceAsync(CancellationToken ct); }

// PgBolt.Dashboard — публичный API (Task 5)
public sealed class PgBoltDashboardOptions { string? ActionsPolicy; bool ReadOnly; bool AllowAnonymousRemote; TimeSpan QueryTimeout = 5 с;
    TimeSpan ScanBudget = 2 с; TimeSpan RefreshInterval = 5 с; TimeSpan ServerStaleAfter = 1 мин; TimeSpan StatsStaleAfter = 1 мин; internal void Validate(); }
// namespace Microsoft.Extensions.DependencyInjection: public static IServiceCollection AddPgBoltDashboard(this IServiceCollection services, Action<PgBoltDashboardOptions>? configure = null);
// namespace Microsoft.AspNetCore.Builder:            public static RouteGroupBuilder MapPgBoltDashboard(this IEndpointRouteBuilder endpoints, string prefix = "/pgbolt");

// PgBolt.Dashboard — internal (Task 5–6)
internal sealed record DashboardEndpoint(int LocalSegments);                       // метаданные каждого endpoint'а dashboard (§5.3 маркер, базовый путь)
internal static class DashboardRoutes { // MapGet / MapPost + .WithMetadata(new DashboardEndpoint(число сегментов pattern))
    static RouteHandlerBuilder Get(RouteGroupBuilder g, string pattern, Delegate handler); static RouteHandlerBuilder Post(RouteGroupBuilder g, string pattern, Delegate handler); }
internal static class DashboardPaths { static string BasePath(HttpContext http); }
internal sealed class DashboardAssets { static DashboardAssets Load(); string RelativeUrl(string file); bool TryGet(string hash, string file, out DashboardAsset asset); }
internal sealed record DashboardAsset(string File, string Hash, string ContentType, byte[] Content);
internal static class DashboardView
{ static bool WantsFragment(HttpRequest request); static bool TargetsMain(HttpRequest request) /* HX-Target == "main" */;
  static Task<IResult> PageAsync<TPage>(HttpContext http, string title, IReadOnlyDictionary<string, object?> parameters, int status = 200, bool partial = false) where TPage : IComponent;   // partial — фрагмент без <title> (отклонение 23)
  static Task<string> RenderAsync(HttpContext http, Type component, IReadOnlyDictionary<string, object?> parameters);
  static Task<IResult> ErrorAsync(HttpContext http, int status, string message); }   // Task 5: PageAsync<ErrorView>(…, partial: !TargetsMain) — единственный рендер ошибки (фильтр ошибок, отказ доступа)
internal static class DashboardHtmx { const string Config; }                        // JSON для <meta name="htmx-config">
internal sealed class DashboardBadRequestException(string message) : Exception;    // → 400
internal sealed class DashboardNotFoundException(string message) : Exception;      // → 404
internal sealed class DashboardErrorFilter(ILogger<DashboardErrorFilter> logger) : IEndpointFilter;                     // §10
internal static class DashboardAccess                                                                                   // Task 6
{ static bool HasAuthorization(EndpointMetadataCollection metadata, bool hasFallbackPolicy); static bool IsLocal(HttpContext http);
  static bool IsAllowedPost(HttpRequest request); }
internal sealed class DashboardAccessFilter : IEndpointFilter;                                                           // Task 6
public sealed record DashboardContext(string BasePath, bool CanAct, bool ReadOnly, TimeSpan RefreshInterval, TimeSpan? EventsRetention, string User,
    string CssUrl, string HtmxUrl, string ScriptUrl) { internal static DashboardContext Of(HttpContext http); }        // Task 5 (из Items, иначе собирает и кеширует: CanAct = !ReadOnly && ActionsPolicy is null), Task 6 (фильтр кладёт Items с политикой)

// PgBolt.Dashboard.Data — internal (Task 7–10)
internal enum CursorShape { Id, AtId, PriorityAtId, Text }
internal static class Cursor { static string Encode(ListKey key, CursorShape shape); static ListKey? Decode(string? value, CursorShape shape); } // невалидный → DashboardBadRequestException
internal readonly record struct ListKey(Guid Id, DateTime? At = null, short? Priority = null, string? Text = null);
internal sealed record Portion<T>(IReadOnlyList<T> Matches, int Scanned, ListKey? LastScanned);
internal sealed record ScanResult<T>(IReadOnlyList<T> Rows, ListKey? Next, bool Incomplete);
internal static class ChunkedScan { const int PortionSize = 2000;
  static Task<ScanResult<T>> RunAsync<T>(ListKey? after, int pageSize, Func<ListKey?, int, CancellationToken, Task<Portion<T>>> next,
      Func<T, ListKey> keyOf, TimeSpan budget, TimeProvider time, CancellationToken ct); }
internal sealed partial class DashboardStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements,
    PgBoltDashboardOptions options, TimeProvider time)
{ internal const int PageSize = 50;
  internal Func<string, Action<NpgsqlParameterCollection>, CancellationToken, Task>? OnStatement { get; set; }          // Task 7, только тесты
  Task<OverviewData> GetOverviewAsync(CancellationToken ct); Task<IReadOnlyList<ThroughputPoint>> GetThroughputDayAsync(CancellationToken ct);
  Task<IReadOnlyList<QueueRow>> ListQueuesAsync(CancellationToken ct); Task<ServersData> GetServersAsync(CancellationToken ct);
  Task<IReadOnlyList<JobTypeRow>> ListJobTypesAsync(CancellationToken ct); Task<JobTypeRow?> GetJobTypeAsync(string name, CancellationToken ct); // Task 7
  Task<Page<JobListRow>> ListJobsAsync(JobListKind kind, string? queue, string? type, string? after, CancellationToken ct);  // Task 8 (Scheduled..Awaiting), Task 9 (Succeeded..Cancelled)
  Task<Page<RelatedJobRow>> ListContinuationsAsync(Guid parentId, bool batchParent, DateTimeOffset parentCreatedAt, string? after, CancellationToken ct);
  Task<Page<BatchRow>> ListBatchContinuationsAsync(Guid parentId, string? after, CancellationToken ct);
  Task<Page<RelatedJobRow>> ListBatchJobsAsync(Guid batchId, DateTimeOffset batchCreatedAt, string? after, CancellationToken ct);
  Task<Page<BatchRow>> ListNestedBatchesAsync(Guid batchId, string? after, CancellationToken ct);
  Task<Page<BatchRow>> ListBatchesAsync(BatchState? state, string? after, CancellationToken ct);
  Task<Page<RecurringRow>> ListRecurringAsync(string? after, CancellationToken ct);
  Task<Page<RelatedJobRow>> ListRecurringRunsAsync(string recurringId, string? after, CancellationToken ct); }        // Task 10

// PgBolt.Dashboard.Views — public, [EditorBrowsable(Never)] (отклонение 7)
public enum JobListKind { Scheduled, Enqueued, Processing, Awaiting, Succeeded, Failed, Cancelled }
public sealed record Page<T>(IReadOnlyList<T> Rows, string? Next, DateTimeOffset? ScannedTo);   // ScannedTo != null — неполная страница (бюджет), Next — «продолжить поиск»
public sealed record JobListRow(Guid Id, string Type, string Queue, short Priority, int Attempt, int MaxAttempts, DateTime CreatedAt,
    DateTime? RunAt, DateTime? StartedAt, Guid? ServerId, DateTime? FinishedAt, bool CancelRequested, JobEventKind? EventKind, string? Message);
public sealed record RelatedJobRow(Guid Id, string Type, string Queue, JobState State, short? Continuation, DateTime CreatedAt, DateTime? FinishedAt);
public sealed record BatchRow(Guid Id, BatchState State, int Total, int Succeeded, int Failed, int Cancelled, short? Condition,
    bool CancelRequested, DateTime CreatedAt, DateTime? FinishedAt);
public sealed record QueueRow(string Name, long Scheduled, long Enqueued, long Processing, long Awaiting, int Servers, DateTime? MeasuredAt);
public sealed record ServerRow(Guid Id, string Name, string Host, string[] Queues, int Capacity, DateTime StartedAt, DateTime HeartbeatAt,
    bool Stale, long Succeeded, long Failed, long Retried);
public sealed record MaintenanceTaskRow(string Name, DateTime NextRunAt, DateTime? LastRunAt, DateTime? LastFinishedAt, string? LastError, bool Running);
public sealed record ServersData(IReadOnlyList<ServerRow> Servers, IReadOnlyList<MaintenanceTaskRow> Tasks);
public sealed record ThroughputPoint(DateTime BucketUtc, long Succeeded, long Failed, long Retried);
public sealed record QueueTotals(long Scheduled, long Enqueued, long Processing, long Awaiting);
public sealed record StatsTaskRow(DateTime NextRunAt, DateTime? LastRunAt, string? LastError);
public enum StatsFreshnessKind { Fresh, NotRunning, Failing }
public sealed record StatsFreshness(StatsFreshnessKind Kind, string? Error)
{ public static StatsFreshness Evaluate(StatsTaskRow? row, DateTime dbNowUtc, TimeSpan staleAfter); }
public sealed record OverviewData(QueueTotals Totals, int LiveServers, StatsFreshness Freshness, IReadOnlyList<ThroughputPoint> LastHour);
public sealed record JobTypeRow(string Name, string ArgsSchemaJson, string DefaultQueue, int DefaultMaxAttempts, short DefaultPriority,
    int[]? DefaultRetryDelaysSeconds, string PublishedBy, DateTime LastSeenAt);
public sealed record RecurringRow(string Id, string Type, string Cron, string TimeZone, string? Owner, bool Paused, DateTime? NextFireAt,
    DateTime? LastFireAt, Guid? LastJobId, string? LastError);

// PgBolt.Dashboard.Forms (Task 15) — модели формы public (параметры компонентов), разбор internal
public enum FieldKind { String, DateTime, Uuid, Integer, Number, Boolean, Enum, Object }
public sealed record FormField(string Path, string Name, FieldKind Kind, bool Required, bool Nullable, IReadOnlyList<string?> EnumValues,
    IReadOnlyList<FormField> Children);
public sealed record FieldError(string Path, string Message);
internal static class SchemaForm { static IReadOnlyList<FormField>? TryBuild(string argsSchemaJson); }   // null — только JSON
internal static class ArgsValidator { static IReadOnlyList<FieldError> Validate(IReadOnlyList<FormField> fields, JsonElement args); }
internal static class FormArgs { static (JsonObject Args, IReadOnlyList<FieldError> Errors) Read(IReadOnlyList<FormField> fields, IFormCollection form, bool bestEffort); }

// tests/PgBolt.Dashboard.Tests (Task 5, 6, 7)
public sealed class DashboardHost : IAsyncDisposable   // WebApplication на TestServer
{ static Task<DashboardHost> StartAsync(TestDb db, Action<PgBoltDashboardOptions>? dashboard = null, Action<WebApplicationBuilder>? builder = null,
      Action<WebApplication>? beforeMap = null, Func<WebApplication, RouteGroupBuilder>? map = null, IPAddress? remote = null /* loopback по умолчанию */,
      bool nullRemote = false /* RemoteIpAddress = null, как у TestServer без middleware */,
      Action<PgBoltOptions>? core = null /* настройка PgBoltOptions: вызывается внутри лямбды AddPgBolt после DataSource / Schema / AutoMigrate; второй AddPgBolt бросает InvalidOperationException */);
  HttpClient Client; IServiceProvider Services; Task<HttpResponseMessage> GetAsync(string path, bool htmx = false, bool historyRestore = false, string? htmxTarget = null /* заголовок HX-Target */);
  Task<HttpResponseMessage> PostAsync(string path, IEnumerable<KeyValuePair<string, string>> form, bool htmx = true, string? fetchSite = "same-origin", string? origin = null); }
public abstract class DashboardTestBase(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{ TestDb Db; PgBoltStore Core; IJobClient Client; ServiceProvider Services; StepTimeProvider Time; JobSeeder Seeder;
  internal DashboardStore Store { get; private set; }   // DashboardStore — internal, а базовый класс public (его наследники — public-классы xUnit): protected/public-член вернул бы CS0053; вспомогательные методы с internal-типами — тоже internal
  Task PublishTypeAsync(string name = "dash.job", string queue = "default", string schemaJson = """{"type":"object"}""");
  Task<Guid> EnqueueAsync(string type = "dash.job", string queue = "default", TimeSpan? delay = null, short priority = 0, int maxAttempts = 3);
  Task<Guid[]> SeedJobsAsync(int count, string type, string queue, short state, string queueRow, DateTime? runAtUtc = null, short priority = 0, TimeSpan? runAtStep = null);   // делегат к Seeder.SeedJobsAsync (параметры — как у JobSeeder ниже)
  Task<LeasedJob> LeaseAsync(string queue = "default");   // делегат к Seeder.LeaseAsync
  Task ResolveAllAsync(); }   // = ResolveAll.RunAsync(Core, ct)

// tests/PgBolt.Dashboard.Tests/Support — общие помощники, доступные и наследникам DashboardTestBase, и тестам на DashboardHost (Task 7, 13, 14)
public sealed class JobSeeder(TestDb db, PgBoltStore core)                                                                  // Task 7
{ Task<Guid[]> SeedJobsAsync(int count, string type, string queue, short state, string queueRow /* "none"|"scheduled"|"enqueued"|"awaiting" */, DateTime? runAtUtc = null,
      short priority = 0 /* пишется в jobs.priority и queue.priority */, TimeSpan? runAtStep = null /* run_at = runAtUtc + i × runAtStep */);
  Task<LeasedJob> LeaseAsync(string queue = "default"); }                       // один lease, самый ранний run_at
public static class ResolveAll { static Task RunAsync(PgBoltStore core, CancellationToken ct); }                              // Task 7: Count → Resolve → CancelMembers, ≤ 200 проходов
public static class TestAuthentication                                                                                       // Task 6: схема "Test", заголовки X-Test-User / X-Test-Roles
{ static IServiceCollection Add(IServiceCollection services, Action<AuthorizationOptions>? authorization = null); }
public sealed class StepTimeProvider : TimeProvider { TimeSpan Step; }   // GetUtcNow — системное; GetTimestamp — системное + (число вызовов × Step): бюджет в интеграционных тестах
```

## Review Focus

Входы и условия, которые спека подразумевает, а тесты задач легко пропустить; тест на каждое уже добавлен в задачу-владельца:

1. **Массовая постановка с одним `run_at` / приоритетом** (сотни Scheduled или Enqueued с равным ключом сортировки) — keyset-страницы не пропускают и не повторяют задачи (Task 8, `Scheduled_pages_with_equal_run_at_cover_each_job_once`, `Enqueued_pages_with_equal_priority_and_run_at_cover_each_job_once`).
2. **Разметка и скрипт в args / message / exception** (`<script>`, `"><img onerror>`) — выводятся экранированным текстом в `<pre>` (Task 13, `Card_escapes_markup_in_args_message_and_exception`).
3. **id расписания и имя типа со `/`, пробелами, `?`, кириллицей** — ссылки и действия работают (Task 13, `Recurring_with_slash_and_cyrillic_id_opens`; Task 14, `Recurring_actions_accept_any_id`; Task 15, `Enqueue_form_opens_for_type_name_with_special_characters`).
4. **Хост с `UsePathBase("/app")` и dashboard во вложенной группе** — `<base href>`, ассеты и ссылки ведут внутрь `/app/admin/pgbolt/` (Task 5, `Base_path_includes_path_base_and_outer_group`).
5. **Пустая схема** (нет строки `stats` в `maintenance_tasks`, пустые `queue_stats` и `stats_minutes`) — обзор рендерится с баннером «снимок не обновляется» и подписью «нет данных», без исключений (Task 7, `Overview_of_empty_schema_reports_not_running_and_no_data`; Task 12, `Overview_page_of_empty_schema_shows_banner`).

## Карта файлов

| Файл | Ответственность |
|---|---|
| `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql` | `stats_minutes`, `queue_stats`, индексы §3.2 |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Stats.cs` | сброс счётчиков, снимок `queue_stats`, retention `stats_minutes` |
| `src/PgBolt.Server/Runtime/OutcomeStats.cs` | процессные счётчики исходов (§4.1) |
| `src/PgBolt.Server/Runtime/OutcomeStatsFlusher.cs` | цикл сброса и финальный сброс |
| `src/PgBolt.Server/Runtime/LogThrottle.cs` | «не чаще раза в период» для логов |
| `src/PgBolt.Dashboard/PgBolt.Dashboard.csproj` | Razor SDK, embedded `Assets/` |
| `src/PgBolt.Dashboard/PgBoltDashboardOptions.cs` | опции §5.2 |
| `src/PgBolt.Dashboard/DashboardServiceCollectionExtensions.cs`, `DashboardEndpointRouteBuilderExtensions.cs` | `AddPgBoltDashboard`, `MapPgBoltDashboard` |
| `src/PgBolt.Dashboard/Endpoints/*.cs` | маршруты (`DashboardRoutes`), рендер (`DashboardView`), пути, ассеты, ошибки, страницы, действия, запуск |
| `src/PgBolt.Dashboard/Security/*.cs` | авторизация, локальность, CSRF, фильтр доступа, предупреждение на старте |
| `src/PgBolt.Dashboard/Data/*.cs` | `DashboardStore` (partial по экранам), `Cursor`, `ChunkedScan` |
| `src/PgBolt.Dashboard/Views/*.cs` | модели представлений (public, отклонение 7), `StatsFreshness` |
| `src/PgBolt.Dashboard/Forms/*.cs` | форма из схемы, проверка args, чтение формы |
| `src/PgBolt.Dashboard/Components/**/*.razor` | `Shell`, страницы, части |
| `src/PgBolt.Dashboard/Assets/htmx.min.js`, `pgbolt.css`, `pgbolt.js` | htmx 2.0.4, стили, локальное время / polling / уведомления |
| `tests/PgBolt.Dashboard.Tests/**` | unit, интеграционные, endpoint, snapshot, планы |

Изменяемые файлы (без новой ответственности; подробности — в разделе **Files** задачи):

| Файл | Задача | Изменение |
|---|---|---|
| `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` | 1 | детектор схемы проверяет `stats_minutes`, `queue_stats` |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Ack.cs`, `PgBoltStore.GetJob.cs`, `StoreRecords.cs` | 2 | `acked` массивом; `Derive` internal; `OutcomeStatsRow` |
| `src/PgBolt.Postgres/PgBolt.Postgres.csproj` | 2 | `InternalsVisibleTo` `PgBolt.Dashboard`, `PgBolt.Dashboard.Tests` |
| `src/PgBolt.Server/Runtime/IServerStore.cs`, `PgBoltServerStore.cs` | 2 | новая сигнатура ack, методы статистики |
| `src/PgBolt.Server/Runtime/CompletionSink.cs`, `PgBoltServer.cs`, `PgBoltProcessingService.cs` | 2, 3 | учёт исходов, цикл и финальный сброс |
| `src/PgBolt.Server/PgBoltProcessingOptions.cs`, `Runtime/ProcessingMaintenance.cs` | 3, 4 | опции §4.3, задача `stats` |
| `src/PgBolt.Postgres/UuidV7.cs` | 10 | `LowerBound(DateTimeOffset)` |
| `Directory.Packages.props`, `PgBolt.slnx` | 5 | `Microsoft.AspNetCore.TestHost`, новые проекты |
| `tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj` | 16 | ASP.NET Core, TestHost, Dashboard |
| `CHANGELOG.md`, roadmap, спеки частей 1–5, спека части 6 | 17 | документация |

---
### Task 1: Схема части 6 в V001 и детектор схемы прошлой сборки

**Files:**
- Modify: `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql`
- Modify: `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` (`EnsureNotPreReleaseSchemaAsync`)
- Test: `tests/PgBolt.Postgres.Tests/MigrationTests.cs`, `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Produces: таблицы `stats_minutes`, `queue_stats` и индексы `queue_awaiting_list`, `job_events_failed`, `job_events_cancelled`, `jobs_parent`, `jobs_recurring`, `batches_parent` — точно §3.1–§3.2. `PgBoltMigrations.LatestVersion` остаётся 1.

- [ ] **Step 1: Тесты схемы**

В `MigrationTests`:
- `Migrate_creates_all_tables_once`: добавить `"stats_minutes", "queue_stats"` в массив таблиц;
- `Script_contains_single_merged_migration`: `Assert.Contains("CREATE TABLE my_schema.stats_minutes", script);`, то же для `queue_stats`;
- `Queue_and_jobs_have_workflow_columns_and_indexes`: ожидаемые списки индексов заменить на `"queue_await_jobs,queue_awaiting,queue_awaiting_list,queue_delayed,queue_expiry,queue_fetch,queue_owner,queue_unique"` и `"jobs_batch,jobs_parent,jobs_recurring"`;
- `Batches_and_completions_have_spec_columns_and_indexes`: список индексов `batches` — `"batches_await_jobs,batches_awaiting,batches_finished,batches_nested,batches_open,batches_parent"`;
- `Hot_contract_no_jobs_index_uses_mutable_columns`: `Assert.True(count >= 1)` → `Assert.Equal(4, count)` (PK, `jobs_batch`, `jobs_parent`, `jobs_recurring`);
- новые тесты:

```csharp
    [Fact]
    public async Task Dashboard_tables_and_indexes_match_spec()   // часть 6 §3
    {
        await Runner().MigrateAsync(Ct);

        Assert.Equal(
            "bucket:timestamp with time zone:true,server_id:uuid:true,queue:text:true,succeeded:integer:true," +
            "failed:integer:true,retried:integer:true", await ColumnsAsync("stats_minutes"));
        Assert.Equal("stats_minutes_pkey", await _db.ScalarAsync<string>(
            "SELECT string_agg(indexname, ',') FROM pg_indexes WHERE schemaname = @s AND tablename = 'stats_minutes'", ("s", _db.Schema)));
        Assert.Contains("(bucket, server_id, queue)", await _db.ScalarAsync<string>(
            "SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.stats_minutes_pkey")));
        Assert.Equal(
            "queue:text:true,scheduled:bigint:true,enqueued:bigint:true,processing:bigint:true,awaiting:bigint:true," +
            "measured_at:timestamp with time zone:true", await ColumnsAsync("queue_stats"));
        Assert.Equal("job_events_cancelled,job_events_failed,job_events_job_id_idx", await IndexesAsync("job_events", ""));
        foreach (var (index, definition) in new[]
        {
            ("queue_awaiting_list", "(job_id) WHERE (await_condition IS NOT NULL)"),
            ("job_events_failed", "(at, job_id) WHERE (kind = ANY (ARRAY[2, 3, 7]))"),
            ("job_events_cancelled", "(at, job_id) WHERE (kind = 4)"),
            ("jobs_parent", "(parent_id, id) WHERE (continuation = ANY (ARRAY[1, 2, 3, 4]))"),
            ("jobs_recurring", "(recurring_id, id) WHERE (recurring_id IS NOT NULL)"),
            ("batches_parent", "(parent_id, id) WHERE (parent_id IS NOT NULL)"),
        })
            Assert.EndsWith(definition, await _db.ScalarAsync<string>("SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.{index}")));
    }

    [Fact]
    public async Task Partitions_inherit_dashboard_indexes()   // §3.2: ATTACH / LIKE родителя
    {
        await Runner().MigrateAsync(Ct);
        await new PartitionManager(_db.Admin, _db.Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(1, Ct);

        foreach (var (parent, index) in new[] { ("jobs", "jobs_parent"), ("jobs", "jobs_recurring"),
                     ("job_events", "job_events_failed"), ("job_events", "job_events_cancelled") })
            Assert.Equal(0L, await _db.ScalarAsync<long>("""
                SELECT count(*) FROM pg_inherits i
                WHERE i.inhparent = to_regclass(@t)
                  AND NOT EXISTS (SELECT 1 FROM pg_index x JOIN pg_inherits p ON p.inhrelid = x.indexrelid
                                  WHERE x.indrelid = i.inhrelid AND p.inhparent = to_regclass(@x))
                """, ("t", $"{_db.Schema}.{parent}"), ("x", $"{_db.Schema}.{index}")));
    }
```

`IndexesAsync(table, "")` уже исключает только `indexname = ''` — у `job_events` нет PK. Ожидаемые хвосты `pg_get_indexdef` — так PostgreSQL 17 печатает `IN (…)` по `smallint` с целыми литералами; если фактический вывод отличается только формой записи (касты, скобки), привести строку теста к выводу без изменения смысла и указать в `DEVIATIONS`.

В `HostingTests` существующий theory `Schema_without_workflow_tables_fails_start(bool autoMigrate)` (удаляет `batches`) не копируется, а параметризуется именем таблицы и переименовывается — тело остаётся прежним:

```csharp
    [Theory]
    [InlineData("batches", true)]
    [InlineData("batches", false)]
    [InlineData("stats_minutes", true)]
    [InlineData("stats_minutes", false)]
    [InlineData("queue_stats", true)]
    [InlineData("queue_stats", false)]
    public async Task Schema_without_pre_release_tables_fails_start(string table, bool autoMigrate)   // часть 5 §4 (batches), часть 6 §11 (stats_minutes, queue_stats)
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync($"DROP TABLE {{schema}}.{table}");      // схема прошлой сборки: версия 1, объектов частей 5 / 6 нет
        await using var sp = Build(o => o.AutoMigrate = autoMigrate);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));

        Assert.Contains("pre-release", e.Message);
        Assert.Contains($"DROP SCHEMA {_db.Schema} CASCADE", e.Message);
    }
```

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests"`
Expected: FAIL — нет таблиц и индексов части 6; случаи `stats_minutes` / `queue_stats` в `Schema_without_pre_release_tables_fails_start` падают на `DROP TABLE` (случаи `batches` проходят — детектор части 5 уже есть).

- [ ] **Step 3: V001**

В `V001__initial.sql` добавить (каждый индекс — сразу после индексов своей таблицы, таблицы — в конец файла):
- после `CREATE INDEX queue_await_jobs …`: `CREATE INDEX queue_awaiting_list ON {schema}.queue (job_id) WHERE await_condition IS NOT NULL;`
- после `CREATE INDEX jobs_batch …`: `jobs_parent` и `jobs_recurring` — текст §3.2 вместе с комментариями;
- после `CREATE INDEX job_events_job_id_idx …`: `job_events_failed`, `job_events_cancelled` (§3.2 с комментарием);
- после индексов `batches` (до `ALTER TABLE … n_distinct`): `batches_parent`;
- в конец: `stats_minutes` и `queue_stats` — DDL §3.1 дословно, с комментариями.

- [ ] **Step 4: Детектор схемы прошлой сборки**

В `PgBoltInitializer.EnsureNotPreReleaseSchemaAsync`: условие — `SELECT to_regclass(@t) IS NULL OR to_regclass(@b) IS NULL OR to_regclass(@m) IS NULL OR to_regclass(@q) IS NULL` с параметрами `m` = `{schema}.stats_minutes`, `q` = `{schema}.queue_stats`; XML-doc дополнить «часть 6 §11: … или stats_minutes / queue_stats». Текст исключения не меняется.

- [ ] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests|FullyQualifiedName~Retention|FullyQualifiedName~PartitionTests|FullyQualifiedName~HotUpdateTests|FullyQualifiedName~DequeuePlanTests|FullyQualifiedName~WorkflowResolverPlanTests"`
Expected: PASS (Direct и Bouncer). Retention / партиции / HOT — регрессия: компактирование строит новые индексы на партиции при `ATTACH`, HOT-обновления не ломаются; `DequeuePlanTests` и `WorkflowResolverPlanTests` — новые индексы `jobs` / `queue` не меняют планы горячего пути и resolver'а (риск спеки §13).

- [ ] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs tests/PgBolt.Postgres.Tests/MigrationTests.cs tests/PgBolt.Postgres.Tests/HostingTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): stats tables and listing indexes in V001"
```

---
### Task 2: Ack с подтверждёнными id, SQL статистики в `PgBoltStore`, инвариант финализации

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.Ack.cs`, `PgBoltStore.GetJob.cs` (`Derive` → `internal static`), `StoreRecords.cs` (`OutcomeStatsRow`)
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Stats.cs`
- Modify: `src/PgBolt.Postgres/PgBolt.Postgres.csproj` (`InternalsVisibleTo`: `PgBolt.Dashboard`, `PgBolt.Dashboard.Tests`)
- Modify: `src/PgBolt.Server/Runtime/IServerStore.cs`, `PgBoltServerStore.cs`, `CompletionSink.cs` (лог по длине массива)
- Modify (тесты, механически `AckAsync` → `.Count`; только файлы, где результат используется): `tests/PgBolt.Postgres.Tests/{AckTests,WorkflowTestSupport,WorkflowRaceTests,WorkflowHotPathTests,ConcurrencyInvariantTests,RetentionCompactionTests,RetentionWorkflowTests,RetentionJobsTests,HeartbeatTests,SweeperTests,WorkflowCancelTests}.cs`, `tests/PgBolt.Server.Tests/{Fakes/FakeServerStore.cs,Unit/FakeServerStoreTests.cs,Unit/CompletionSinkTests.cs,Integration/WorkflowResolverLoopTests.cs}`, `tests/E2E/PgBolt.E2E.Tests/{EndToEndTests,SlimDataSourceTests,WorkflowEndToEndTests}.cs`
- Без правок (результат `AckAsync` отбрасывается, код компилируется как есть): `GetJobTests`, `TriggerRequeueTests`, `CancelTests`, `HotUpdateTests`, `RetentionScenarioTests` — но они входят в прогон Step 6
- Test: `tests/PgBolt.Postgres.Tests/AckTests.cs`, `tests/PgBolt.Postgres.Tests/StatsStoreTests.cs`, `tests/PgBolt.Postgres.Tests/FinalizationInvariantTests.cs`

**Interfaces:**
- Consumes: Task 1 (таблицы статистики).
- Produces: `AckAsync → IReadOnlyList<Guid>`, `FlushOutcomeStatsAsync`, `SnapshotQueueStatsAsync`, `TrimStatsAsync`, `OutcomeStatsRow`, `PgBoltStore.Derive` (internal) — сигнатуры из «Контракта»; те же члены в `IServerStore` / `PgBoltServerStore`; `FakeServerStore`: `OnAck : Func<IReadOnlyList<LeaseRef>, CancellationToken, Task<IReadOnlyList<Guid>>>` (по умолчанию — id всех lease), `OnFlushStats : Func<IReadOnlyList<OutcomeStatsRow>, CancellationToken, Task>`, `OnSnapshot : Func<CancellationToken, Task>`, `OnTrim : Func<TimeSpan, int, CancellationToken, Task<int>>` (по умолчанию 0), вызовы пишутся в `Calls` с `Op` = `"FlushStats"` (Detail — массив строк), `"Snapshot"`, `"Trim"` (Detail — `(retention, limit)`).

- [ ] **Step 1: Тесты store**

`AckTests` (абстрактный класс, Direct/Bouncer):

```csharp
    [Fact]
    public async Task Ack_returns_ids_of_confirmed_jobs_only()   // часть 6 §4.1
    {
        var good = await EnqueueAsync();
        var lost = await EnqueueAsync();
        var leased = await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);
        var leases = leased.Select(j => new LeaseRef(j.Id, j.Id == lost ? Guid.NewGuid() : j.LeaseId)).ToList();

        Assert.Equal([good], await Store.AckAsync(ServerA, leases, Ct));
        Assert.Empty(await Store.AckAsync(ServerA, [], Ct));
    }
```

`StatsStoreTests.cs` (`StatsStoreTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase`, наследники `_Direct`, `_Bouncer`):
- `Flush_inserts_and_adds_to_existing_rows` — `FlushOutcomeStatsAsync([new(b, ServerA, "q", 2, 1, 0), new(b, ServerB, "q", 1, 0, 0)])`, затем `[new(b, ServerA, "q", 3, 0, 4)]` (`b` — `new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc)`); строки `stats_minutes` — `(ServerA: 5, 1, 4)`, `(ServerB: 1, 0, 0)`; пустой список — без обращения к БД: вызов после `DROP TABLE {schema}.stats_minutes` не бросает.
- `Snapshot_counts_every_derived_state_per_queue` — очередь `a`: Scheduled (`EnqueueAsync("a", delay: 1 ч)`), два Enqueued, один Processing (`LeaseOneAsync("a")` из двух Enqueued — итого Enqueued 1, Processing 1); Awaiting — прямой `INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition) VALUES (@id, 'a', 0, 'infinity', false, 3, @p, 1)`; очередь `b` — один Enqueued. После `SnapshotQueueStatsAsync` — `a: (1, 1, 1, 1)`, `b: (0, 1, 0, 0)`, `measured_at` не `NULL`.
- `Snapshot_removes_vanished_queues_and_empty_queue_gives_empty_table` — снимок с очередью `a`, затем `DELETE FROM {schema}.queue`, повторный снимок → `queue_stats` пуста.
- `Trim_deletes_old_buckets_in_portions` — 25 строк с `bucket = now() - interval '8 days' - g * interval '1 minute'` и 3 свежие; `TrimStatsAsync(7 сут, 10)` трижды → `10, 10, 5`, четвёртый → `0`; свежие остались.

`FinalizationInvariantTests.cs` (`: WorkflowTestBase`, Direct/Bouncer) — §6.1, §12 «инвариант `jobs.finished_at = job_events.at`». Помощник:

```csharp
    /// <summary>jobs.state = state и есть событие из kinds с at = jobs.finished_at.</summary>
    private Task<bool> TerminalEventMatchesAsync(Guid id, short state, params short[] kinds) =>
        Db.ScalarAsync<bool>("""
            SELECT j.state = @state AND j.finished_at IS NOT NULL
                   AND EXISTS (SELECT 1 FROM {schema}.job_events e WHERE e.job_id = j.id AND e.kind = ANY(@kinds) AND e.at = j.finished_at)
            FROM {schema}.jobs j WHERE j.id = @id
            """, ("id", id), ("state", state), ("kinds", kinds));
```

Тесты (каждый — `Assert.True(await TerminalEventMatchesAsync(...))`):
- `Fail_writes_finished_at_and_failed_event_together` — `FailFinallyAsync` → `(3, 2)`;
- `No_executor_fail_matches_event_7` — `Flow.FailAsync(id, lease, ServerA, JobEventKind.NoExecutor, …)` → `(3, 7)`;
- `Sweeper_on_last_attempt_matches_event_3` — `Store.EnqueueAsync(Batch(id, maxAttempts: 1))`, `LeaseOneAsync`, `ExpireLeaseAsync`, `SweepExpiredLeasesAsync(100)` → `(3, 3)`;
- `Sweeper_with_cancel_request_matches_event_4_and_3_shares_at` — lease, `CancelAsync` (ставит `cancel_requested`), expire, sweep → `(4, 4)` и событие 3 с тем же `at`;
- `Retry_with_cancel_request_matches_event_4` — lease, `CancelAsync`, `RetryAsync(…, JobEventKind.Retry …)` возвращает `FailOutcome.Cancelled` → `(4, 4)`;
- `Cancel_of_pending_job_matches_event_4` — `CancelJobAsync` → `(4, 4)`;
- `Release_with_cancel_request_matches_event_4` — lease, `CancelAsync`, `ReleaseAsync(id, lease, ServerA, ran: true, null)` → `ReleaseOutcome.Cancelled`, `(4, 4)`;
- `Batch_cancel_matches_event_4_for_members` — `Client.Workflow().StartBatch(b => { b.Enqueue(A(1)); b.Enqueue(A(2)); })`, `SubmitAsync`, `Client.CancelBatchAsync`, `ResolveAllAsync` → оба члена `(4, 4)`;
- `Resolver_cascade_cancel_matches_event_4` — родитель `A(1)` с продолжением `ContinueWith(A(2))` (OnSuccess), `CancelJobAsync(parent)` (каскадная отмена — только при Cancelled-родителе: Failed-родитель с OnSuccess-продолжением продолжение не отменяет, часть 5 §2.2), `ResolveAllAsync` → продолжение `(4, 4)`;
- `Requeue_resets_finished_at` — после `FailFinallyAsync` и `Client.RequeueAsync` → `finished_at IS NULL`, `state = 0`; повторный `FailFinallyAsync` → снова `(3, 2)`, и событий kind 2 с `at = finished_at` ровно одно.

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~AckTests|FullyQualifiedName~StatsStoreTests|FullyQualifiedName~FinalizationInvariantTests"`
Expected: FAIL — компиляция (`AckAsync` возвращает `int`, методов статистики нет). `FinalizationInvariantTests` после компиляции проходят сразу (инвариант существующих путей) — это регрессионная фиксация §11.

- [ ] **Step 3: Ack**

`PgBoltStore.Ack.cs`: последняя строка SQL — `SELECT array(SELECT id FROM j) AS acked, (SELECT count(*) FROM s) AS signals`; метод `public Task<IReadOnlyList<Guid>> AckAsync(...)`: пустой список — `Task.FromResult<IReadOnlyList<Guid>>([])`; чтение — `r.GetFieldValue<Guid[]>(0)`; XML-doc: «Возвращает id подтверждённых задач (часть 6 §4.1)».

`PgBoltStore.GetJob.cs`: `private static JobState Derive` → `internal static JobState Derive`.

`StoreRecords.cs`:

```csharp
/// <summary>Строка сброса счётчиков исходов (часть 6 §4.1): бакет — начало минуты UTC.</summary>
internal readonly record struct OutcomeStatsRow(DateTime BucketUtc, Guid ServerId, string Queue, int Succeeded, int Failed, int Retried);
```

- [ ] **Step 4: `PgBoltStore.Stats.cs`**

```sql
-- FlushOutcomeStatsTemplate (§4.1)
INSERT INTO {schema}.stats_minutes AS s (bucket, server_id, queue, succeeded, failed, retried)
SELECT * FROM unnest(@buckets::timestamptz[], @servers::uuid[], @queues::text[], @succeeded::int[], @failed::int[], @retried::int[])
ON CONFLICT (bucket, server_id, queue) DO UPDATE
    SET succeeded = s.succeeded + EXCLUDED.succeeded,
        failed    = s.failed    + EXCLUDED.failed,
        retried   = s.retried   + EXCLUDED.retried;

-- SnapshotQueueStatsTemplate (§4.2 п. 1) — текст спеки дословно (WITH s … up … DELETE … NOT EXISTS)

-- TrimStatsTemplate (§4.2 п. 2)
DELETE FROM {schema}.stats_minutes
WHERE ctid IN (SELECT ctid FROM {schema}.stats_minutes WHERE bucket < now() - @retention LIMIT @limit);
```

Методы — через `NonQueryAsync` (повтор 40P01 уже в `RunAsync`); `FlushOutcomeStatsAsync` при `rows.Count == 0` — `Task.CompletedTask`; массивы параметров — `P.Add(ps, "buckets", rows.Select(r => r.BucketUtc).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)` и аналогично. `TrimStatsAsync` проверяет `retention > 0`, `limit > 0` (`ArgumentOutOfRangeException`).

- [ ] **Step 5: `PgBolt.Server` и тесты-потребители**

- `IServerStore` / `PgBoltServerStore`: сигнатура ack и три метода — проброс.
- `CompletionSink.SendAckBatchAsync`: `var acked = (await _store.AckAsync(...)).Count;` — логика логов без изменений (учёт исходов — Task 3).
- `FakeServerStore`: члены из **Interfaces**; `FakeServerStoreTests` — `Assert.Equal([job.Id], await store.AckAsync(Server, [lease], Ct))`.
- `CompletionSinkTests`: `OnAck`-лямбды возвращают `IReadOnlyList<Guid>`: `Task.FromResult(leases.Count)` (`Any_send_error_is_retried_with_doubling_backoff_capped_at_5s`) → `FakeServerStore.AckAll(leases)` (новый `public static Task<IReadOnlyList<Guid>> AckAll(IReadOnlyList<LeaseRef> leases)`), `Task.FromResult(leases.Count - 1)` → `Task.FromResult<IReadOnlyList<Guid>>([.. leases.Skip(1).Select(l => l.JobId)])` (id всех, кроме первого; `Ack_batch_skips_records_removed_by_heartbeat_and_logs_partial_ack`); `gate` в `Execution_stays_in_table_until_send_completes` — `new TaskCompletionSource<IReadOnlyList<Guid>>(TaskCreationOptions.RunContinuationsAsynchronously)` вместо `<int>`, `gate.SetResult(1)` → `gate.SetResult([execution.Job.Id])`; `OnAck`, который бросает (`throw new IOException`), правок не требует.
- Остальные тесты из списка «Modify» в **Files**: `Assert.Equal(n, await X.AckAsync(...))` → `Assert.Equal(n, (await X.AckAsync(...)).Count)`; `processed += await …AckAsync(…)` → `processed += (await …AckAsync(…)).Count`; `HeartbeatTests`: `acked.Sum()` → `acked.Sum(a => a.Count)`.

- [ ] **Step 6: Запустить — проходят**

Run: `dotnet build PgBolt.slnx` и `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~AckTests|FullyQualifiedName~StatsStoreTests|FullyQualifiedName~FinalizationInvariantTests|FullyQualifiedName~Workflow|FullyQualifiedName~Retention|FullyQualifiedName~HeartbeatTests|FullyQualifiedName~CancelTests|FullyQualifiedName~GetJobTests|FullyQualifiedName~HotUpdateTests|FullyQualifiedName~SweeperTests|FullyQualifiedName~TriggerRequeueTests|FullyQualifiedName~ConcurrencyInvariantTests"` и `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~CompletionSink|FullyQualifiedName~FakeServerStore|FullyQualifiedName~WorkflowResolverLoopTests"` и `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~EndToEndTests|FullyQualifiedName~SlimDataSourceTests"`
Expected: сборка без предупреждений; PASS (в прогоне — все классы из **Files**, включая изменённые только по компиляции).

- [ ] **Step 7: Commit**

```bash
git add src tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): ack returns confirmed ids, stats SQL in PgBoltStore"
```

---
### Task 3: Счётчики исходов в `PgBolt.Server`: `OutcomeStats`, учёт в `CompletionSink`, цикл и финальный сброс

**Files:**
- Create: `src/PgBolt.Server/Runtime/OutcomeStats.cs`, `OutcomeStatsFlusher.cs`, `LogThrottle.cs`
- Modify: `src/PgBolt.Server/Runtime/CompletionSink.cs`, `PgBoltServer.cs`, `PgBoltProcessingService.cs`, `src/PgBolt.Server/PgBoltProcessingOptions.cs`
- Modify (тесты): `tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs` (конструктор `CompletionSink`), `Unit/CompletionSinkTests.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/OutcomeStatsTests.cs`, `Unit/OutcomeStatsFlusherTests.cs`, `Unit/LogThrottleTests.cs`, `Unit/CompletionSinkTests.cs`, `Unit/ProcessingServiceTests.cs`, `Unit/ServerRegistrationTests.cs`

**Interfaces:**
- Consumes: Task 2 (`IServerStore.AckAsync` → id, `FlushOutcomeStatsAsync`, `OutcomeStatsRow`).
- Produces: `OutcomeStats`, `OutcomeStatsFlusher`, `LogThrottle`, новые параметры конструкторов `CompletionSink` / `PgBoltServer`, `PgBoltProcessingService.Stats` / `Flusher`, `PgBoltProcessingOptions.StatsFlushInterval` — из «Контракта».

- [ ] **Step 1: Тесты**

`OutcomeStatsTests` (FakeTimeProvider, старт `2026-10-05T12:00:30Z`):
- `Record_buckets_by_utc_minute_of_time_provider` — `Record(s, "q", succeeded: 2)`, `Advance(40 с)`, `Record(s, "q", failed: 1)` → `Take()` = `[(12:00, s, q, 2, 0, 0), (12:01, s, q, 0, 1, 0)]` (порядок — по бакету, серверу, очереди), `BucketUtc.Kind == Utc`.
- `Take_empties_and_restore_merges_with_new_counts` — `Record(+2 succ)`, `rows = Take()`, `Record(+1 succ)`, `Restore(rows)` → `Take()` = одна строка с `3`; повторный `Take()` пуст.
- `Servers_and_queues_are_separate_keys` — два сервера × две очереди → четыре строки.
- `Record_is_thread_safe` — 8 потоков × 10 000 `Record(s, "q", succeeded: 1)` → сумма 80 000.

`LogThrottleTests`: первый `TryEnter()` — `true`; повтор до истечения периода — `false`; после `Advance(period)` — `true`.

`CompletionSinkTests` (sink создаётся с `new OutcomeStats(_time)`, поле `_stats`):
- `Ack_counts_only_returned_ids_per_queue` — выполнения в очередях `a`, `a`, `b` (`new Execution(Leased.Job(), queue, …)`), `OnAck` возвращает id первого `a` и `b` → `_stats.Take()` = `a: succeeded 1`, `b: succeeded 1`.
- `Retry_and_fail_are_counted_by_store_outcome` — `[Theory]` по `(outcome kind: Retry | Fail, store answer: FailOutcome)`: `Retried` → `retried 1`; `Failed` → `failed 1`; `Cancelled`, `LeaseLost` → пусто.
- `Confirm_cancel_and_release_are_not_counted` — `ConfirmCancel` и `Release(true)` → `Take()` пуст.
- `Bucket_is_minute_of_confirmation` — `OnAck` ждёт `gate`; `Complete` в 12:00:50, `Advance(20 с)`, открыть gate → бакет 12:01.
- `Ambiguous_retry_after_failed_attempt_counts_only_store_answer` — первая попытка ack бросает `IOException`, вторая возвращает пустой список (применено раньше) → ничего не учтено.

`OutcomeStatsFlusherTests` (FakeServerStore, FakeTimeProvider, `ListLoggerFactory`, интервал 15 с):
- `Flushes_every_interval_one_statement_with_all_rows` — `Record` в два сервера; `Start`; `Advance(15 с)` → один вызов `FlushStats` с двумя строками; без данных следующий `Advance` вызова не делает.
- `Failed_flush_restores_rows_and_merges_with_new` — `OnFlushStats` бросает один раз; после следующего интервала строка содержит сумму старых и новых значений; Warning в логе — один, при постоянной ошибке за 2 мин (8 интервалов) — не больше 3 Warning (раз в минуту).
- `Stop_waits_for_running_flush_then_flushes_once_more` — первый сброс заблокирован на `gate`; `StopAsync` не завершается, пока `gate` закрыт; `Record` во время блокировки; после открытия `gate` — ровно ещё один сброс с новыми строками; максимум одновременных вызовов `FlushStats` — 1 (счётчик в `OnFlushStats`).
- `Stop_with_cancelled_token_skips_final_flush` — `Record`, `StopAsync(cancelled)` → вызовов `FlushStats` нет.

`ProcessingServiceTests`:
- `Stop_flushes_outcome_stats_after_servers_stop` — сервер с очередью, `OneJobPerQueue`, задача `PoliteJob` (ждёт токен) не подходит — использовать тип, завершающийся сразу: добавить `QuickArgs` / `QuickJob` (возвращает `Task.CompletedTask`) в этот файл и `ServerJson`; после `StopAsync` в `_store.Calls` последний `FlushStats` идёт после всех `RemoveServer`, и в нём `succeeded = 1`.
- `Virtual_servers_write_separate_rows` — два сервера, по задаче в каждом → после `StopAsync` строки финального сброса с двумя разными `ServerId`.
- `Flusher_runs_without_maintenance` — `ConfigurePgBoltProcessing(p => { p.RunMaintenance = false; p.RunScheduler = false; })` → `service.Flusher` не `null`.

`ServerRegistrationTests`: `[Theory]` `Invalid_stats_options_throw` для `StatsFlushInterval = 0` и `= 2 сут`.

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~OutcomeStats|FullyQualifiedName~LogThrottle|FullyQualifiedName~CompletionSink|FullyQualifiedName~ProcessingServiceTests|FullyQualifiedName~ServerRegistrationTests"`
Expected: FAIL — компиляция.

- [ ] **Step 3: `OutcomeStats` и `LogThrottle`**

`OutcomeStats` — `Dictionary<(DateTime Bucket, Guid Server, string Queue), (int S, int F, int R)>` под `Lock`; бакет — `var now = time.GetUtcNow().UtcDateTime; new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerMinute, DateTimeKind.Utc)`. `Take()` подменяет словарь новым под блокировкой и возвращает строки, отсортированные по `(BucketUtc, ServerId, Queue)` (детерминированный порядок блокировок upsert). `Restore` складывает со словарём. XML-doc — §4.1.

`LogThrottle` — `long? _last` по `MonoClock`; `TryEnter` — `Interlocked`/`lock`, `true`, если записей не было или прошёл `period`.

- [ ] **Step 4: Учёт в `CompletionSink`**

Конструктор получает `OutcomeStats stats`. После успешного ack: для `inFlight` с `Job.Id ∈ acked` — `stats.Record(_serverId, execution.Queue, succeeded: 1)` (сгруппировать по очереди: один `Record` на очередь с числом). `SendOutcomeAsync` возвращает и ответ store: для `Retry` / `Fail` после ответа — `FailOutcome.Retried` → `Record(…, retried: 1)`, `FailOutcome.Failed` → `Record(…, failed: 1)`; `ConfirmCancel`, `Release` — без учёта. `Record` — после ответа store (момент подтверждения), до `Finish`.

- [ ] **Step 5: `OutcomeStatsFlusher`**

Два источника отмены: `_stop` (пауза цикла) и `_abort` (идущий statement). Цикл: `while (!_stop.IsCancellationRequested) { Interlocked.Increment(ref _idleWaits); await Task.Delay(interval, clock.Time, _stop.Token) (OCE → выход); await FlushOnceAsync(_abort.Token); }`. `FlushOnceAsync`: `rows = stats.Take()`; пусто — выход; `await store.FlushOutcomeStatsAsync(rows, ct)`; исключение (не отмена `ct`) — `stats.Restore(rows)` и Warning `"PgBolt: flushing outcome statistics failed; values are kept for the next flush."` только если `throttle.TryEnter()` (`LogThrottle(clock, 1 мин)`); отмена — тоже `Restore`. `StopAsync(ct)`: `using var reg = ct.Register(() => _abort.Cancel())` (как `CompletionSink.StopAsync`), `_stop.Cancel()`, дождаться задачи цикла (идущий сброс доходит до конца, остановка цикла его не обрывает; отменённый `ct` — обрывает); затем, если `!ct.IsCancellationRequested`, — финальный `FlushOnceAsync(ct)`. Одновременно идёт не больше одного сброса: цикл последовательный, финальный — после завершения цикла.

- [ ] **Step 6: Подключение**

- `PgBoltProcessingOptions`: `StatsFlushInterval` = 15 с, XML-doc «Сброс счётчиков исходов процесса (часть 6 §4.1); работает в любом процессе с AddPgBoltServer, независимо от RunMaintenance»; `Validate` — `Check(StatsFlushInterval, …)`.
- `PgBoltServer`: параметр `OutcomeStats stats`, передаётся в `CompletionSink`.
- `PgBoltProcessingService`: свойство `internal OutcomeStats Stats { get; }` — создаётся в конструкторе: `new OutcomeStats(services.GetRequiredService<TimeProvider>())` (`AddPgBolt` регистрирует `TimeProvider`, тесты — `FakeTimeProvider`); передаётся в каждый `new PgBoltServer(…, Stats)`; `internal OutcomeStatsFlusher? Flusher { get; private set; }` — создаётся и `Start()` в блоке `try` §4.4 п. 6 перед `server.Start()`, если `_servers.Count > 0`. `StopAsync`: после `await Task.WhenAll(_servers.Select(s => s.FlushAndStopAsync(ct)))` — `if (Flusher is not null) await Flusher.StopAsync(cancellationToken)` (до ожидания `maintenance`). `ShutdownReserve` — добавить XML-комментарий (сейчас его нет): «запас сверх ShutdownTimeout + AckFlushInterval серверов в проверке `HostOptions.ShutdownTimeout`; в него входит финальный сброс счётчиков исходов (часть 6 §4.1)».

- [ ] **Step 7: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Unit"`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): confirmed outcome counters with periodic and final flush"
```

---
### Task 4: Кластерная задача `stats` (снимок `queue_stats`, retention `stats_minutes`)

**Files:**
- Modify: `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs`, `src/PgBolt.Server/PgBoltProcessingOptions.cs`
- Modify (тесты): `tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs`, `Unit/HousekeepingTests.cs`, `Unit/ResolverLoopTests.cs` (число циклов в ожидании; захват `stats` в `Claim_failure…`), `Integration/MaintenanceTests.cs`, `Integration/LeaseFailureTests.cs` (четвёртая строка `stats` в `maintenance_tasks`)
- Test: `tests/PgBolt.Server.Tests/Unit/StatsTaskTests.cs`, `Unit/ServerRegistrationTests.cs`

**Interfaces:**
- Consumes: Task 2 (`SnapshotQueueStatsAsync`, `TrimStatsAsync`), Task 3 (`LogThrottle`).
- Produces: `StatsSnapshotInterval`, `StatsRetention`, `ProcessingMaintenance.StatsOnceAsync` и константы из «Контракта»; `LoopAsync(…, LogThrottle? errorLog = null)` (отклонение 21).

- [ ] **Step 1: Тесты**

`StatsTaskTests` (FakeServerStore, FakeTimeProvider, `NextRandom = () => 0.5`, `RunScheduler = false`):
- `Stats_claims_with_four_fifths_of_interval_and_runs_snapshot_then_trim_portions` — `OnClaim` захватывает только `"stats"`; `OnTrim` отдаёт `10_000, 10_000, 3`; `await StatsOnceAsync` → вызовы по порядку: `Claim("stats", 8 с)`, `Snapshot`, три `Trim` с `(7 сут, 10_000)`, `Complete("stats", claimedAt, null, null)`.
- `Not_claimed_stats_does_nothing` — `Claim` → `null`: ни `Snapshot`, ни `Trim`, ни `Complete`.
- `Failing_pass_completes_with_error_each_time_but_logs_once_per_minute` — `OnSnapshot` бросает `IOException("boom")`; цикл запущен (`Start`), семь проходов (`Advance(10 с)` × 6 с ожиданием `IdleWaits`) → 7 `Complete` с `Error = "boom"` и `RetryDelay = null`; записей Error с текстом `stats` — 2 (в 0 с и в 60 с).
- `Stats_loop_runs_only_with_RunMaintenance` — `RunMaintenance = false` → после `Start` и `Advance(1 мин)` вызовов `Claim("stats")` нет.
- `Pass_period_with_four_fifths_claim_is_at_most_six_fifths_interval` — модель захвата: `OnClaim` хранит `nextRunAt` (по `_time.GetUtcNow()`), захватывает, если `now >= nextRunAt`, и ставит `nextRunAt = now + interval` (переданный аргумент); `NextRandom` поочерёдно `0.0` и `0.999`; за 5 мин модельного времени (шаг `Advance` — 100 мс) разность соседних моментов захвата ≤ `1.2 × StatsSnapshotInterval` + 100 мс и ≥ `0.8 × StatsSnapshotInterval`.
- `Two_pods_share_one_pass_per_interval` — два `ProcessingMaintenance` на одном `FakeServerStore` с моделью захвата выше; за 2 мин число `Snapshot` ≤ `2 мин / (0,8 × 10 с) + 1`.
- `Stats_claim_failure_is_logged_once_per_minute` (отклонение 21) — `OnClaim` для `"stats"` всегда бросает `IOException("db down")`, для остальных — `null`; `Start`; семь проходов: после каждого `Advance(10 с)` дождаться очередного вызова `Claim` с именем `stats` (`_store.CallsOf("Claim")`, `((ClaimCall)c.Detail!).Name == "stats"`) → записей Error с текстом `stats iteration failed` — 2 (в 0 с и в 60 с); `Complete` не вызывался. То же для исключения `OnComplete`: захват успешен (`OnClaim` отдаёт срок только `"stats"`), `OnComplete` бросает — записей Error с текстом `stats` (из `LoopAsync`, а не `RunTaskAsync`) не больше 2 за 7 проходов.

`ServerRegistrationTests.Invalid_stats_options_throw` — добавить случаи `StatsSnapshotInterval = 0`, `StatsRetention = 0`, `StatsRetention = 3651 сут`.

Существующие тесты — пятый цикл (`stats`) при `RunMaintenance`: `IdleWaits == 4` → `== 5` (`ProcessingMaintenanceTests` ×2, `HousekeepingTests`), комментарии полей `_options` — «promoter / sweeper / housekeeping / resolver / stats»; `ResolverLoopTests`: `>= 4` → `>= 5`, `>= 8` → `>= 10`, в `Timer_iteration_checks_for_ready_signals_first` — `_options.StatsSnapshotInterval = TimeSpan.FromSeconds(30)` (все пять циклов просыпаются одним `Advance`).

Цикл `stats` тоже вызывает `TryClaimMaintenanceAsync` и добавляет в `maintenance_tasks` четвёртую строку с `last_run_at`, меняющимся каждые 8–12 с, — ещё три существующих теста ломаются, их правят в этой же задаче:
- `HousekeepingTests.Claim_failure_is_logged_by_the_loop_and_retried_after_poll_interval` (строки ~140–155): ошибка `OnClaim` («первый вызов бросает») могла бы достаться циклу `stats`, а `claims` считал бы и его захваты. `OnClaim` не считает и не роняет `stats`: `_store.OnClaim = (name, _, _) => name == "stats" ? Task.FromResult<DateTime?>(null) : Interlocked.Increment(ref claims) == 1 ? Task.FromException<DateTime?>(new TimeoutException("db down")) : Task.FromResult<DateTime?>(null);` — `claims` считает только задачи housekeeping; ожидание `IdleWaits == 5` (вместо 4), `Assert.Equal(1, claims)` и повторное `claims == 4` без изменений; комментарий — «promoter, sweeper, housekeeping, resolver and stats wait for their intervals».
- `Integration/MaintenanceTests.Retention_runs_on_exactly_one_of_two_hosts`: оба запроса к `maintenance_tasks` читают только задачи housekeeping — `SELECT count(*) = 3 AND bool_and(last_finished_at IS NOT NULL AND last_error IS NULL) FROM {schema}.maintenance_tasks WHERE name <> 'stats'` и `claimsSql` = `SELECT string_agg(name || '=' || last_run_at::text, ',' ORDER BY name) FROM {schema}.maintenance_tasks WHERE name <> 'stats'` (иначе `count(*) = 3` не выполнится, а проверка стабильности `claimsSql` за 1 с станет нестабильной: `stats` захватывается каждые 8–12 с).
- `Integration/LeaseFailureTests.Outcome_that_cannot_be_written_within_its_budget_expires_and_sweeper_spends_the_attempt`: `SELECT count(*) = 3 AND bool_and(last_finished_at IS NOT NULL) FROM {schema}.maintenance_tasks WHERE name <> 'stats'`.

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~StatsTaskTests|FullyQualifiedName~ServerRegistrationTests"`
Expected: FAIL — компиляция.

- [ ] **Step 3: Опции**

`PgBoltProcessingOptions`: `StatsSnapshotInterval` = 10 с (XML-doc: интервал задачи `stats`; как у остальных задач, действует значение пода, захватившего задачу последним; захват — `0,8 × интервал`), `StatsRetention` = 7 сут (хранение `stats_minutes`). `Validate`: `Check(StatsSnapshotInterval, …)`; `StatsRetention` — `> 0` и `≤ PgBoltOptions.MaxRetention`, иначе `InvalidOperationException("PgBolt processing: StatsRetention must be > 0 and <= 3650 days.")`.

- [ ] **Step 4: Цикл `stats`**

`ProcessingMaintenance`:
- в `Start` при `RunMaintenance`: `loops.Add(Task.Run(() => LoopAsync("stats", StatsOnceAsync, options.StatsSnapshotInterval, errorLog: _statsErrors)))` (`_statsErrors` — поле класса, см. ниже);
- `StatsOnceAsync(ct)`: `RunTaskAsync("stats", options.StatsSnapshotInterval * StatsClaimFactor, SnapshotStatsAsync, errorRetryDelay: null, ct, _statsErrors)`, возвращает 1 / 0;
- `SnapshotStatsAsync`: `await store.SnapshotQueueStatsAsync(ct)`; затем `while (await store.TrimStatsAsync(options.StatsRetention, StatsTrimBatchSize, ct) == StatsTrimBatchSize) { }`; `TaskOutcome.Ok`;
- `RunTaskAsync` получает необязательный параметр `LogThrottle? errorLog = null`: в ветке ошибки `if (errorLog?.TryEnter() ?? true) logger.LogError(...)`; `_statsErrors = new LogThrottle(clock, StatsErrorLogPeriod)`;
- `LoopAsync` получает необязательный параметр `LogThrottle? errorLog = null` (после `onTimer`): `catch (Exception e)` пишет `LogError("PgBolt: {Loop} iteration failed; …")` только `if (errorLog?.TryEnter() ?? true)`; цикл `stats` (вызов из `Start` выше): исключение захвата / `Complete` (недоступная БД) уходит в тот же ограничитель, что и ошибка прохода, — вместе не больше одной записи Error в минуту (§4.2, отклонение 21);
- XML-doc класса — «… и задача `stats` (часть 6 §4.2) отдельным циклом».

- [ ] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Unit"` и `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~MaintenanceTests|FullyQualifiedName~LeaseFailureTests"` (интеграционные, Direct и Bouncer: цикл `stats` работает рядом с housekeeping на реальной БД)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): stats maintenance task with queue snapshot and retention"
```

---
### Task 5: Пакет `PgBolt.Dashboard`: каркас, рендер, оболочка, ассеты, ошибки

**Files:**
- Create: `src/PgBolt.Dashboard/PgBolt.Dashboard.csproj`, `PgBoltDashboardOptions.cs`, `DashboardServiceCollectionExtensions.cs`, `DashboardEndpointRouteBuilderExtensions.cs`
- Create: `src/PgBolt.Dashboard/Endpoints/DashboardRoutes.cs`, `DashboardPaths.cs`, `DashboardView.cs`, `DashboardHtmx.cs`, `DashboardAssets.cs`, `DashboardErrorFilter.cs`, `DashboardExceptions.cs`, `DashboardContext.cs`
- Create: `src/PgBolt.Dashboard/Components/_Imports.razor`, `Shell.razor`, `Parts/ErrorView.razor`, `Pages/OverviewPage.razor` (заглушка: `<h1>Обзор</h1>`; содержимое — Task 12)
- Create: `src/PgBolt.Dashboard/Assets/htmx.min.js`, `pgbolt.css`, `pgbolt.js`
- Modify: `Directory.Packages.props`, `PgBolt.slnx`
- Create: `tests/PgBolt.Dashboard.Tests/PgBolt.Dashboard.Tests.csproj`, `AssemblyInfo.cs`, `Support/DashboardHost.cs`, `ModuleInit.cs`
- Create (тесты): `tests/PgBolt.Dashboard.Tests/Support/ListLoggerProvider.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Endpoints/RenderingTests.cs`, `Unit/OptionsTests.cs`

**Interfaces:**
- Consumes: `PgBoltOptions`, `PgBoltDataSource` (IVT, Task 2).
- Produces: публичный API пакета, `DashboardRoutes`, `DashboardPaths`, `DashboardView`, `DashboardHtmx.Config`, `DashboardAssets`, исключения, `DashboardErrorFilter`, `DashboardView.ErrorAsync`, `DashboardContext` (`CanAct = !ReadOnly && ActionsPolicy is null` до Task 6; `Of` с запасным путём при пустых `Items`), тестовый `DashboardHost` — из «Контракта».

- [ ] **Step 1: Проекты и пакеты**

`Directory.Packages.props`: `<PackageVersion Include="Microsoft.AspNetCore.TestHost" Version="10.0.0" />`.

`src/PgBolt.Dashboard/PgBolt.Dashboard.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk.Razor">
  <PropertyGroup>
    <RootNamespace>PgBolt.Dashboard</RootNamespace>
    <PackageId>PgBolt.Dashboard</PackageId>
    <Description>Embeddable ASP.NET Core dashboard for PgBolt (Razor components + htmx).</Description>
    <!-- §2: параметры компонентов — reflection; пакет не AOT и не trimmable. -->
    <IsAotCompatible>false</IsAotCompatible>
    <IsTrimmable>false</IsTrimmable>
    <!-- §5.1: без static web assets — нет wwwroot, нет scoped CSS; ассеты — embedded resources, раздаёт endpoint группы. -->
    <ScopedCssEnabled>false</ScopedCssEnabled>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\PgBolt.Postgres\PgBolt.Postgres.csproj" />
  </ItemGroup>
  <ItemGroup>
    <Content Remove="Assets\**" />
    <None Remove="Assets\**" />
    <EmbeddedResource Include="Assets\**" LogicalName="PgBolt.Dashboard.Assets.%(Filename)%(Extension)" />
  </ItemGroup>
  <ItemGroup>
    <InternalsVisibleTo Include="PgBolt.Dashboard.Tests" />
  </ItemGroup>
</Project>
```

`tests/PgBolt.Dashboard.Tests/PgBolt.Dashboard.Tests.csproj` — `Microsoft.NET.Sdk`, `OutputType Exe`, `IsPackable false`, `FrameworkReference Microsoft.AspNetCore.App`; пакеты `Microsoft.NET.Test.Sdk`, `xunit.v3`, `xunit.runner.visualstudio`, `Verify.XunitV3`, `Microsoft.AspNetCore.TestHost`, `Microsoft.Extensions.TimeProvider.Testing`; `<Using Include="Xunit" />`; проекты `..\PgBolt.Testing\PgBolt.Testing.csproj`, `..\..\src\PgBolt.Dashboard\PgBolt.Dashboard.csproj`. `AssemblyInfo.cs` — `[assembly: AssemblyFixture(typeof(PostgresFixture))]`. `ModuleInit.cs` — `[ModuleInitializer]`, задающий каталог снимков `tests/PgBolt.Dashboard.Tests/Snapshots/` (`Verifier.UseProjectRelativeDirectory("Snapshots")` из VerifyXunit; если в Verify 31 API называется иначе — эквивалент через `DerivePathInfo`, отклонение — в `DEVIATIONS`). `Support/ListLoggerProvider.cs` — `ILoggerProvider`, собирающий записи `(Category, Level, Message, Exception)` (как `CollectingLoggerProvider` в `PgBolt.Server.Tests`).

`PgBolt.slnx`: `src/PgBolt.Dashboard/PgBolt.Dashboard.csproj` в `/src/`, `tests/PgBolt.Dashboard.Tests/PgBolt.Dashboard.Tests.csproj` в `/tests/`. `src/PgBolt.Postgres/PgBolt.Postgres.csproj` уже содержит IVT (Task 2).

htmx: `curl -sSfL https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js -o src/PgBolt.Dashboard/Assets/htmx.min.js`; первой строкой файла не трогать содержимое — версию и SHA-256 (`shasum -a 256`) записать в комментарий `DashboardAssets` (`// htmx.min.js — htmx 2.0.4, sha256 <hex>`).

- [ ] **Step 2: Тесты**

`Support/DashboardHost.cs` (сигнатура — «Контракт»): `WebApplication.CreateSlimBuilder()`, `builder.WebHost.UseTestServer()`, `services.AddPgBolt(o => { o.DataSource = db.DataSource; o.Schema = db.Schema; o.AutoMigrate = false; core?.Invoke(o); })` (параметр `core` — для тестов, которым нужны `EventsRetention`, `JobTypesCacheTtl` и т. п.; повторный `AddPgBolt` в `builder` бросил бы исключение), `services.AddPgBoltDashboard(dashboard)`; `builder` (настройка хоста); после `Build()` — middleware `app.Use((ctx, next) => { ctx.Connection.RemoteIpAddress = nullRemote ? null : remote ?? IPAddress.Loopback; return next(ctx); })`, `beforeMap(app)`, затем `map?.Invoke(app) ?? app.MapPgBoltDashboard()`; `await app.StartAsync()`, `Client = app.GetTestClient()`. `GetAsync(path, htmx, historyRestore, htmxTarget)` выставляет `HX-Request: true` / `HX-History-Restore-Request: true` / `HX-Target: {htmxTarget}` (если задан). `PostAsync` — `FormUrlEncodedContent`, заголовки `HX-Request`, `Sec-Fetch-Site`, `Origin` по аргументам.

`RenderingTests` (Direct, `TestDb.CreateAsync(pg, ConnectionMode.Direct)`):
- `Full_page_without_htmx_header` — `GET /pgbolt/` → 200, `text/html; charset=utf-8`, тело начинается с `<!DOCTYPE html>`, содержит `<base href="/pgbolt/">`, `<meta name="htmx-config"`, `<main id="main">`, `<h1>Обзор</h1>`.
- `Fragment_with_htmx_header` — `htmx: true` → тело без `<!DOCTYPE`, начинается с `<title>`, содержит `<h1>Обзор</h1>`.
- `Partial_fragment_has_no_title` (отклонение 23) — тестовый маршрут `DashboardRoutes.Get(g, "/part", (HttpContext http) => DashboardView.PageAsync<ErrorView>(http, "t", new Dictionary<string, object?> { ["Status"] = 200, ["Message"] = "x" }, partial: true))` (через `map`): с `htmx: true` → тело без `<title>` и без `<!DOCTYPE`, содержит `x`; без `HX-Request` — страница целиком (`<!DOCTYPE html>`, `<title>t · PgBolt</title>`).
- `History_restore_gets_full_page` — `htmx: true, historyRestore: true` → `<!DOCTYPE html>`.
- `Html_responses_vary_and_are_not_cached` — `Vary` содержит `HX-Request` и `HX-History-Restore-Request`; `Cache-Control: no-store`.
- `Htmx_config_disables_eval_and_history_cache_and_swaps_errors` — разобрать JSON из `content` мета-тега: `allowEval == false`, `historyCacheSize == 0`, `includeIndicatorStyles == false`, `disableInheritance == true`, в `responseHandling` есть элемент `{"code":"[45]..","swap":true,"error":true}`.
- `Shell_body_inherits_only_navigation_attributes` — `<body>` несёт `hx-boost="true"`, `hx-target="#main"`, `hx-swap="innerHTML show:window:top"` и `hx-inherit="hx-boost hx-target hx-swap"` (при `disableInheritance` без `hx-inherit` потомки не получили бы ни `hx-boost`, ни цель `#main`); во всей оболочке нет `hx-disinherit`.
- `Shell_has_no_inline_scripts` — каждый `<script` имеет `src=`, нет `hx-on`, нет `javascript:`; `<script src=…pgbolt.js>` — с `defer` (скрипт выполняется после разбора документа; без `defer` в `<head>` `document.body` был бы `null`), `<script src=…htmx.min.js>` — без `defer`, после `<meta name="htmx-config">`.
- `Assets_are_served_by_content_hash_and_cached_forever` — URL из `<script src>` htmx (`assets/{hash}/htmx.min.js`, относительный) → 200, `text/javascript`, `Cache-Control: public, max-age=31536000, immutable`, тело — байты embedded-ресурса; тот же файл с другим хешем → 404; CSS — `text/css`.
- `Base_path_includes_path_base_and_outer_group` (Review Focus 4) — `beforeMap: app => app.UsePathBase("/app")`, `map: app => app.MapGroup("/admin").MapPgBoltDashboard("/pgbolt")`; `GET /app/admin/pgbolt/` → `<base href="/app/admin/pgbolt/">`.
- `Map_without_AddPgBolt_throws_with_hint` — без `DashboardHost` (он всегда вызывает `AddPgBolt`): тест сам собирает `WebApplication.CreateSlimBuilder()` + `builder.WebHost.UseTestServer()`; хост без `AddPgBolt`, только `AddPgBoltDashboard()` → `MapPgBoltDashboard` бросает `InvalidOperationException`, сообщение содержит `AddPgBolt`; без `AddPgBoltDashboard` — сообщение содержит `AddPgBoltDashboard`.
- `Errors_render_with_status_as_fragment_and_page` — тестовый маршрут внутри группы через `map: app => { var g = app.MapPgBoltDashboard(); DashboardRoutes.Get(g, "/boom/{kind}", string (string kind) => { switch (kind) { case "404": throw new DashboardNotFoundException("job"); case "400": throw new DashboardBadRequestException("bad cursor"); case "503": throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014"); default: throw new InvalidOperationException("secret detail"); } }); return g; }` (лямбда с явным типом возврата `string` и оператором `switch`: switch-выражение, все ветви которого — `throw`, не имеет типа, CS8506) → статусы 404 / 400 / 503 / 500; с `HX-Request` — фрагмент, без — страница; 503 содержит «запрос не уложился в QueryTimeout, сузьте фильтр»; 500 содержит `InvalidOperationException` и не содержит стека (`   at `); в логе (`ListLoggerProvider` через `builder`) — Error для 500 и 503; фрагмент ошибки начинается с `<title>` только при `htmxTarget: "main"`, без `HX-Target` (polling, строки) — без `<title>` (`TargetsMain`, отклонение 23).

`OptionsTests`: умолчания §5.2 (`QueryTimeout` 5 с, `ScanBudget` 2 с, `RefreshInterval` 5 с, `ServerStaleAfter` / `StatsStaleAfter` 1 мин, `ActionsPolicy` `null`, `ReadOnly` / `AllowAnonymousRemote` `false`); `[Theory]` невалидных значений (отклонение 12) → `InvalidOperationException` из `AddPgBoltDashboard`.

- [ ] **Step 3: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests`
Expected: FAIL — компиляция.

- [ ] **Step 4: Опции, регистрация, маршруты**

- `PgBoltDashboardOptions` — свойства §5.2 с XML-doc из таблицы спеки; `AllowAnonymousRemote` — `<remarks>` про mesh-sidecar (§5.3, отклонение 13); `Validate()` — отклонение 12.
- `AddPgBoltDashboard`: `configure`, `Validate`, `services.AddSingleton(options)`, `services.TryAddSingleton(DashboardAssets.Load())`, `services.AddSingleton(sp => new DashboardStore(sp.GetRequiredService<PgBoltDataSource>().Value, core.Schema, core.UsePreparedStatements, options, sp.GetRequiredService<TimeProvider>()))` (`core` — `sp.GetRequiredService<PgBoltOptions>()`; класс `DashboardStore` — Task 7: до него регистрация без store), повторный вызов — `InvalidOperationException`.
- `MapPgBoltDashboard(prefix)`: проверки (`PgBoltOptions` и `PgBoltDashboardOptions` в `endpoints.ServiceProvider`, иначе `InvalidOperationException` «call services.AddPgBolt(...) before MapPgBoltDashboard» / «call services.AddPgBoltDashboard(...)»); `var group = endpoints.MapGroup(prefix).DisableAntiforgery().AddEndpointFilter<DashboardErrorFilter>()` (§5.1); `DashboardRoutes.Get(group, "/assets/{hash}/{file}", …)`, `DashboardRoutes.Get(group, "/", …OverviewPage…)`; возврат `group`. `<remarks>`: хост за TLS-прокси применяет `ForwardedHeaders` (`X-Forwarded-Proto`, `X-Forwarded-Host`), иначе браузеры без `Sec-Fetch-Site` получат 403 (§5.3).
- `DashboardRoutes.Get/Post(g, pattern, handler)` — `g.MapGet/MapPost(pattern, handler).WithMetadata(new DashboardEndpoint(Segments(pattern)))`, где `Segments` — число непустых сегментов `pattern` (`"/"` → 0).
- `DashboardPaths.BasePath(http)`: `var endpoint = (RouteEndpoint)http.GetEndpoint()!; var local = endpoint.Metadata.GetMetadata<DashboardEndpoint>()!.LocalSegments; var keep = endpoint.RoutePattern.PathSegments.Count - local;` → `http.Request.PathBase + "/" + string.Join('/', segments запроса [0..keep])` (без хвостового `/`; при `keep == 0` — `PathBase`).

- [ ] **Step 5: Рендер, оболочка, ассеты, ошибки**

- `DashboardView.WantsFragment(r)` — `r.Headers["HX-Request"] == "true" && r.Headers["HX-History-Restore-Request"] != "true"`.
- `DashboardView.RenderAsync(http, type, parameters)`: `await using var renderer = new HtmlRenderer(http.RequestServices, http.RequestServices.GetRequiredService<ILoggerFactory>()); return await renderer.Dispatcher.InvokeAsync(async () => (await renderer.RenderComponentAsync(type, ParameterView.FromDictionary(new Dictionary<string, object?>(parameters)))).ToHtmlString());`
- `DashboardView.TargetsMain(r)` — `r.Headers["HX-Target"] == "main"`. `DashboardView.PageAsync<TPage>(http, title, parameters, status, partial)`: фрагмент — при `!partial` `<title>{экранированный title} · PgBolt</title>` + рендер `TPage`, при `partial` — только рендер `TPage` (отклонение 23); страница — рендер `Shell` с параметрами `Title`, `Context` (`DashboardContext.Of(http)`), `Body = typeof(TPage)`, `BodyParameters = parameters`; ответ — `Results.Content(html, "text/html; charset=utf-8", Encoding.UTF8, status)` и заголовки `Vary`, `Cache-Control` (в `http.Response.Headers` до записи).
- `Shell.razor` (`@namespace PgBolt.Dashboard.Components`): `<!DOCTYPE html>`, `<html lang="ru">`, `<meta charset="utf-8">`, viewport, `<base href="@(Context.BasePath)/">`, `<meta name="htmx-config" content="@DashboardHtmx.Config">`, `<title>@Title · PgBolt</title>`, `<link rel="stylesheet" href="@Context.CssUrl">`, `<script src="@Context.HtmxUrl"></script>` (синхронно, после `<meta name="htmx-config">`), `<script src="@Context.ScriptUrl" defer></script>`; `<body hx-boost="true" hx-target="#main" hx-swap="innerHTML show:window:top" hx-inherit="hx-boost hx-target hx-swap">`; навигация §9 (`./`, `queues`, `jobs/scheduled`, `batches`, `recurring`, `servers`, `types` — относительные `href`); `<main id="main"><DynamicComponent Type="Body" Parameters="BodyParameters" /></main>`; `<div id="pgbolt-notify" role="status" aria-live="polite"></div>`.
- `DashboardHtmx.Config` = `{"allowEval":false,"historyCacheSize":0,"includeIndicatorStyles":false,"disableInheritance":true,"responseHandling":[{"code":"204","swap":false},{"code":"[23]..","swap":true},{"code":"[45]..","swap":true,"error":true},{"code":"...","swap":false}]}`.
- `DashboardView.ErrorAsync(http, status, message)` — единственное место рендера ошибки: `PageAsync<ErrorView>(http, "Ошибка", {Status = status, Message = message}, status, partial: !TargetsMain(http.Request))` (отклонение 23); им пользуются `DashboardErrorFilter` (ниже) и фильтр доступа (Task 6, 403).
- `DashboardContext.Of(http)` — из `HttpContext.Items[typeof(DashboardContext)]`, если фильтр доступа (Task 6) его положил; иначе (Task 5 — фильтра ещё нет; Task 6 — путь, где `Items` не заполнен) собирает сам и кладёт в `Items` (кеш): `BasePath`, `CanAct = !options.ReadOnly && options.ActionsPolicy is null` (синхронный запасной путь не вызывает `IAuthorizationService`: при заданной политике кнопки скрыты), `ReadOnly`, `RefreshInterval`, `EventsRetention = PgBoltOptions.EventsRetention`, `User = http.User.Identity?.Name ?? "anonymous"`, URL ассетов `assets.RelativeUrl("pgbolt.css" | "htmx.min.js" | "pgbolt.js")`. `Of` не бросает при пустых `Items` ни в одной задаче.
- `DashboardAssets.Load()` — все ресурсы с префиксом `PgBolt.Dashboard.Assets.`; хеш — первые 16 hex SHA-256 содержимого в нижнем регистре; `RelativeUrl(file)` = `assets/{hash}/{file}`; тип — `.js` → `text/javascript; charset=utf-8`, `.css` → `text/css; charset=utf-8`. Endpoint ассетов: несовпадение хеша или файла — 404 без тела; иначе `Results.Bytes` + `Cache-Control: public, max-age=31536000, immutable`.
- `DashboardErrorFilter` (§10): `DashboardNotFoundException` → 404, `DashboardBadRequestException` и `BadHttpRequestException` → 400 (сообщение исключения), `PostgresException { SqlState: "57014" }` → 503 «запрос не уложился в QueryTimeout, сузьте фильтр», `NpgsqlException` не `PostgresException` либо `PostgresException` с `SqlState` на `08` / `57P0` / `53300` → 503 «база данных недоступна», остальное → 500 «внутренняя ошибка: {тип исключения}»; 500 и 503 — `LogError(e, "PgBolt dashboard: {Method} {Path} failed.")`; ответ — `DashboardView.ErrorAsync(http, status, message)` (выше; отклонение 23). Исключения отмены запроса (`OperationCanceledException` при `RequestAborted`) — пробрасываются.
- `Assets/pgbolt.css` — собственные стили без фреймворков: переменные цветов, тёмная тема через `@media (prefers-color-scheme: dark)`, адаптивная сетка навигации и таблиц (§9).
- `Assets/pgbolt.js` (без `eval`, ~40 строк): (1) `htmx.onLoad(root => …)` — для каждого `time[datetime]` текст — `new Date(dt).toLocaleString()`, `title` — исходный UTC и относительное время («3 мин назад» / «через 2 ч», `Intl.RelativeTimeFormat('ru')`); (2) `document.addEventListener('htmx:beforeRequest', e => { if (document.hidden && e.detail.elt.hasAttribute('data-pgbolt-poll')) e.preventDefault(); })`; (3) `document.addEventListener('visibilitychange', () => { if (!document.hidden) document.querySelectorAll('[data-pgbolt-poll]').forEach(el => htmx.trigger(el, 'pgbolt:refresh')); })`; (4) `document.addEventListener('pgbolt:notify', e => показать e.detail.value в #pgbolt-notify на 5 с)`; (5) выбор всех чекбоксов списка (`data-pgbolt-select-all`). Скрипт подключён с `defer`, слушатели — на `document` (события htmx всплывают до него; при `<script>` в `<head>` без `defer` `document.body === null`, и `document.body.addEventListener` падал бы с `TypeError`, отключая polling-на-скрытой-вкладке, уведомления и «выбрать все»).

- [ ] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Dashboard.Tests` и `dotnet pack src/PgBolt.Dashboard -o /tmp/pgbolt-pack -p:Version=0.0.0-test && ! unzip -l /tmp/pgbolt-pack/PgBolt.Dashboard.0.0.0-test.nupkg | grep -q staticwebassets`
Expected: PASS; обе команды завершаются с кодом 0 — в пакете нет `staticwebassets` (`grep -c` при отсутствии совпадений печатает `0`, но завершается с кодом 1 — не годится как проверка успеха).

- [ ] **Step 7: Commit**

```bash
git add Directory.Packages.props PgBolt.slnx src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): package skeleton, HtmlRenderer views, shell and assets"
```

---
### Task 6: Доступ: признак авторизации, локальность, политика действий, CSRF

**Files:**
- Create: `src/PgBolt.Dashboard/Security/DashboardAccess.cs`, `DashboardAccessFilter.cs`, `DashboardStartupCheck.cs`
- Modify: `src/PgBolt.Dashboard/DashboardEndpointRouteBuilderExtensions.cs` (фильтр доступа перед фильтром ошибок; проверка `ActionsPolicy`; регистрация предупреждения), `Endpoints/DashboardContext.cs` (`CanAct` с политикой)
- Create (тесты): `tests/PgBolt.Dashboard.Tests/Support/TestAuthentication.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Unit/AccessTests.cs`, `Endpoints/AccessEndpointTests.cs`

**Interfaces:**
- Consumes: Task 5 (`DashboardView.ErrorAsync`, `DashboardContext.Of`).
- Produces: `DashboardAccess`, `DashboardAccessFilter`; `DashboardContext.CanAct` = `!ReadOnly && (ActionsPolicy is null || политика выполнена)`; `HttpContext.Items[typeof(DashboardContext)]` заполняет фильтр первой же строкой, до любой проверки доступа (в том числе до отказа 403); тестовый `TestAuthentication` (Task 14 использует его же).

- [ ] **Step 1: Тесты**

`AccessTests` (unit):
- `IsLocal_requires_loopback_without_proxy_headers` — `[Theory]`: `127.0.0.1` → true; `::1` → true; `::ffff:127.0.0.1` → true; `10.0.0.5` → false; `null` → false; `127.0.0.1` + `X-Forwarded-For: 1.2.3.4` / `Forwarded: for=1.2.3.4` / `X-Real-IP: 1.2.3.4` → false (`DefaultHttpContext`).
- `Authorization_is_detected_from_metadata_or_fallback` — `[Theory]` по метаданным: `new AuthorizeAttribute()` → true; `new AuthorizeAttribute("P")` → true; `new AuthorizationPolicyBuilder().RequireAuthenticatedUser().Build()` → true; собственный `IAuthorizationRequirementData` → true; пусто → false; пусто + `hasFallbackPolicy: true` → true; `AuthorizeAttribute` + `AllowAnonymousAttribute` → false; `AllowAnonymous` + fallback → false.
- `Post_requires_htmx_header_and_same_origin` — `[Theory]` (`DefaultHttpContext`, `Scheme = "https"`, `Host = "app.example:8443"`): без `HX-Request` → false; `HX-Request` + `Sec-Fetch-Site: same-origin` → true; `+ same-site` / `cross-site` / `none` → false; без `Sec-Fetch-Site`: `Origin: https://app.example:8443` → true, `https://APP.example:8443` → true, `http://app.example:8443` → false, `https://evil.example:8443` → false, `https://app.example` → false, `null` / отсутствует → false; `Host = "app.example"` + `Origin: https://app.example:443` → true (порт по умолчанию).

`AccessEndpointTests` (Direct):
- `Local_request_is_allowed_without_authorization` — `remote: Loopback` → 200 (хост `DashboardHost` — `CreateSlimBuilder` без `AddAuthorization`: `IAuthorizationPolicyProvider` не зарегистрирован; регрессия на `NullReferenceException` во внешнем фильтре).
- `Remote_request_without_authorization_is_forbidden` — `remote: 10.0.0.5` → 403; `nullRemote` → 403; loopback + `X-Forwarded-For` → 403.
- `AllowAnonymousRemote_allows_remote` — `o.AllowAnonymousRemote = true`, `10.0.0.5` → 200.
- `RequireAuthorization_on_group_allows_remote_authenticated_user` — `builder: b => TestAuthentication.Add(b.Services)`, `map: app => app.MapPgBoltDashboard().RequireAuthorization()`; `10.0.0.5` с `X-Test-User: alice` (через `host.Client.DefaultRequestHeaders`) → 200; без заголовка → 401 (хост).
- `Fallback_policy_counts_as_authorization` — `TestAuthentication.Add(b.Services, o => o.FallbackPolicy = new AuthorizationPolicyBuilder().RequireAuthenticatedUser().Build())`, удалённый аутентифицированный (`X-Test-User`) → 200.
- `AllowAnonymous_on_group_is_not_authorization` — `.RequireAuthorization().AllowAnonymous()`, `10.0.0.5` → 403.
- `ActionsPolicy_without_authorization_services_fails_map` — `o.ActionsPolicy = "Ops"` без `AddAuthorization` → `MapPgBoltDashboard` бросает `InvalidOperationException`.
- `CanAct_reflects_read_only_and_policy` — страница (тестовый маршрут из `map`, рендерящий `ctx.CanAct`) при `ReadOnly = true` → `false`; при `ActionsPolicy = "Ops"` (`TestAuthentication.Add(b.Services, o => o.AddPolicy("Ops", p => p.RequireRole("Ops")))`) и пользователе без роли (`X-Test-User: alice`) → `false`, с ролью (`X-Test-Roles: Ops`) → `true`.
- `Denied_response_is_rendered_with_context` — удалённый запрос без авторизации (`10.0.0.5`) → 403, тело — страница с оболочкой и текстом «доступ только с локальных запросов» (контекст берётся из `Items`, не бросает).
- `Startup_warns_when_dashboard_is_local_only` — без авторизации, `ListLoggerProvider` → после старта Warning с `RequireAuthorization`; с `.RequireAuthorization()` — нет; с `AllowAnonymousRemote` без авторизации — Warning «anonymous remote access».

`Support/TestAuthentication.cs` — общая тестовая аутентификация (её же использует Task 14): `public static class TestAuthentication { public const string Scheme = "Test", UserHeader = "X-Test-User", RolesHeader = "X-Test-Roles"; public static IServiceCollection Add(IServiceCollection services, Action<AuthorizationOptions>? authorization = null); }` — регистрирует схему `Test` схемой по умолчанию (`AuthenticationHandler`: заголовок `X-Test-User` — имя пользователя, `X-Test-Roles` — роли через запятую; без `X-Test-User` — `NoResult`, и хост отвечает 401) и `AddAuthorization(authorization)`. Заголовки тесты выставляют через `host.Client.DefaultRequestHeaders` (у `DashboardHost.GetAsync` / `PostAsync` для них параметра нет).

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~Access"`
Expected: FAIL — компиляция.

- [ ] **Step 3: `DashboardAccess`**

```csharp
    /// <summary>§5.3: авторизация на endpoint'е — IAuthorizeData / AuthorizationPolicy / IAuthorizationRequirementData или fallback-политика; IAllowAnonymous отменяет.</summary>
    public static bool HasAuthorization(EndpointMetadataCollection metadata, bool hasFallbackPolicy)
    {
        if (metadata.GetMetadata<IAllowAnonymous>() is not null)
            return false;
        return hasFallbackPolicy
               || metadata.GetMetadata<IAuthorizeData>() is not null
               || metadata.GetMetadata<AuthorizationPolicy>() is not null
               || metadata.GetMetadata<IAuthorizationRequirementData>() is not null;
    }
```

`IsLocal`: `RemoteIpAddress` не `null`, `IPAddress.IsLoopback(a.IsIPv4MappedToIPv6 ? a.MapToIPv4() : a)`, и нет заголовков `Forwarded`, `X-Forwarded-For`, `X-Real-IP`. `IsAllowedPost`: `HX-Request == "true"`; если есть `Sec-Fetch-Site` — только `same-origin`; иначе `Origin` разбирается `Uri.TryCreate(…, UriKind.Absolute)` и сравнивается со схемой запроса (`OrdinalIgnoreCase`), хостом (`OrdinalIgnoreCase`) и портом (`Request.Host.Port ?? (https ? 443 : 80)` против `uri.Port`).

- [ ] **Step 4: Фильтр, контекст, предупреждение**

`DashboardAccessFilter.InvokeAsync`: первой строкой, до любой проверки доступа, вычисляет `DashboardContext` (`CanAct` — ниже) и кладёт его в `http.Items[typeof(DashboardContext)]`: страница отказа 403 рендерится оболочкой через `DashboardContext.Of(http)`, и контекст должен быть в `Items` к этому моменту (если нет — `Of` соберёт запасной, Task 5); затем endpoint и метаданные; `var policyProvider = http.RequestServices.GetService<IAuthorizationPolicyProvider>(); var hasFallback = policyProvider is not null && await policyProvider.GetFallbackPolicyAsync() is not null;` — хост без `AddAuthorization` (в том числе `WebApplication.CreateSlimBuilder()` и `DashboardHost`) не регистрирует `IAuthorizationPolicyProvider`, а `await null` дал бы `NullReferenceException` во внешнем фильтре (мимо `DashboardErrorFilter`) и 500 на каждый запрос; без авторизации, без `AllowAnonymousRemote` и не `IsLocal` → `DashboardView.ErrorAsync(http, 403, "доступ только с локальных запросов: настройте авторизацию группы dashboard")` (тело 403 нужно htmx-свапу, §10; общий рендер ошибки с `DashboardErrorFilter`). `CanAct`: `!ReadOnly && (ActionsPolicy is null || (await http.RequestServices.GetRequiredService<IAuthorizationService>().AuthorizeAsync(http.User, ActionsPolicy)).Succeeded)` — `IAuthorizationService` запрашивается только при `ActionsPolicy != null` (наличие `AddAuthorization` уже проверено в `MapPgBoltDashboard`). POST: `ReadOnly` или `!CanAct` → `ErrorAsync(http, 403, …)`; `!IsAllowedPost` → `ErrorAsync(http, 403, "запрос отклонён проверкой CSRF")`.

`MapPgBoltDashboard`: `group.AddEndpointFilter<DashboardAccessFilter>()` **до** `DashboardErrorFilter` (фильтр доступа внешний); `ActionsPolicy != null` и нет `IAuthorizationService` → `InvalidOperationException` («ActionsPolicy requires services.AddAuthorization()»); `DashboardStartupCheck.Register(endpoints.ServiceProvider)` — по `IHostApplicationLifetime.ApplicationStarted` обходит `EndpointDataSource.Endpoints` с `DashboardEndpoint` в метаданных (fallback-политика — `GetService<IAuthorizationPolicyProvider>()`, с проверкой на `null` как в фильтре): если у какого-то нет авторизации (`HasAuthorization` с fallback) — Warning `"PgBolt dashboard is reachable only from local requests: add RequireAuthorization(...) to the dashboard group or set AllowAnonymousRemote."` либо, при `AllowAnonymousRemote`, `"PgBolt dashboard allows anonymous remote access (AllowAnonymousRemote = true)."`; одно предупреждение на приложение: callback `ApplicationStarted` пишет не больше одной записи за вызов (не по записи на endpoint), статического состояния нет — тесты поднимают десятки хостов в одном процессе, и статический флаг «уже предупредили» скрыл бы Warning от `ListLoggerProvider` теста.

- [ ] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Dashboard.Tests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): local-only default, authorization detection, actions policy and CSRF"
```

---
### Task 7: `DashboardStore`: транзакции, курсоры, обход порциями, обзор / очереди / серверы / типы

**Files:**
- Create: `src/PgBolt.Dashboard/Data/DashboardStore.cs`, `DashboardStore.Overview.cs`, `Cursor.cs`, `ChunkedScan.cs`, `ListKey.cs`
- Create: `src/PgBolt.Dashboard/Views/Page.cs`, `ListRows.cs`, `Overview.cs`, `StatsFreshness.cs`
- Modify: `src/PgBolt.Dashboard/DashboardServiceCollectionExtensions.cs` (регистрация `DashboardStore`)
- Create (тесты): `tests/PgBolt.Dashboard.Tests/Support/DashboardTestBase.cs`, `Support/JobSeeder.cs`, `Support/ResolveAll.cs`, `Support/StepTimeProvider.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Unit/CursorTests.cs`, `Unit/ChunkedScanTests.cs`, `Unit/StatsFreshnessTests.cs`, `Data/OverviewStoreTests.cs`, `Data/OverviewStorePreparedTests.cs`

**Interfaces:**
- Consumes: Task 1 (таблицы), Task 5.
- Produces: всё «Data» и «Views» из «Контракта», кроме методов Task 8–10; `DashboardTestBase`, `JobSeeder` (засев и lease — и для наследников `DashboardTestBase`, и для тестов на `DashboardHost`, Task 13–14), `ResolveAll` (проходы resolver'а — `DashboardTestBase` и Task 14), `StepTimeProvider`.

- [ ] **Step 1: Тесты**

`CursorTests`:
- `Round_trip_for_every_shape` — `Id`, `AtId` (`At` с тиками, кратными 10 — микросекунды PostgreSQL), `PriorityAtId` (в т. ч. отрицательный приоритет), `Text` (кириллица, `/`, 200 символов); строка — только `[A-Za-z0-9_-]`.
- `Invalid_cursor_is_bad_request` — `[Theory]`: `"!!"`, base64url мусора, курсор формы `Id` при ожидании `AtId`, обрезанный на байт, с лишним байтом, неизвестная версия → `DashboardBadRequestException`; `null` и `""` → `null`.

`ChunkedScanTests` (FakeTimeProvider; делегат порции — скрипт из списка порций, каждый вызов `Advance(1 с)`; бюджет 2 с; `keyOf = x => new ListKey(x)`):
- `Fills_page_from_several_portions` — порции по 2000 ключей с 20 совпадениями → после трёх порций 50 строк; `Next` = ключ 50-й строки; `Incomplete == false`; запрошенное `need` в каждом вызове = `pageSize - собрано + 1`.
- `Extra_match_means_more_data` — порция вернула 51 совпадение → 50 строк, `Next` = ключ 50-й.
- `Short_portion_ends_data` — порция `Scanned = 1500` с 3 совпадениями → 3 строки, `Next == null`, `Incomplete == false`.
- `Budget_exhausted_returns_incomplete_page_with_last_scanned_key` — порции по 2000 с 1 совпадением; после второй порции бюджет исчерпан → 2 строки, `Incomplete == true`, `Next` = `LastScanned` второй порции.
- `Cursor_advances_when_budget_ends_without_matches` — порции без совпадений → 0 строк, `Next` = `LastScanned` последней порции, не равен входному `after`.
- `Budget_is_checked_between_portions_only` — бюджет 0,5 с, одна порция занимает 1 с → выполнена ровно одна порция.

`StatsFreshnessTests` (§6.4):
- `No_row_is_not_running`;
- `Overdue_beyond_stale_after_plus_half_claim_gap_is_not_running` — `last_run = t0`, `next_run = t0 + 8 с` (интервал 10 с), `now = next + 1 мин + 4 с + 1 мс` → `NotRunning`; `now = next + 1 мин + 3 с` → `Fresh`;
- `Long_interval_has_no_false_banner` — интервал 10 мин: `next − last = 8 мин`, `now = next + 3 мин`, `staleAfter = 1 мин` → `Fresh`;
- `Last_error_is_failing_with_text` — свежая строка с `LastError = "boom"` → `Failing`, `Error == "boom"`; просроченная с ошибкой → `NotRunning`.

`OverviewStoreTests` (`DashboardTestBase`, Direct/Bouncer):
- `Overview_of_empty_schema_reports_not_running_and_no_data` (Review Focus 5) — `Totals` нули, `LiveServers == 0`, `Freshness.Kind == NotRunning`, `LastHour` — 60 точек с нулями, последняя — текущая минута БД.
- `Overview_sums_queue_stats_and_last_hour_buckets` — `queue_stats` (две очереди, прямой `INSERT`), `stats_minutes`: бакеты `date_trunc('minute', now())` (2 сервера) и `- 59 мин`, и `- 61 мин` (вне окна); живой сервер (`servers.heartbeat_at = now()`) и мёртвый (`- 2 мин`); строка `stats` (`next_run_at = now() + 8 с`, `last_run_at = now()`) → суммы, `LiveServers == 1`, `Fresh`, точки 0 и 59 с данными, 61-я минута не учтена.
- `Throughput_day_has_96_quarter_hour_points` — бакеты в двух минутах одной четверти часа суммируются в одну точку; 96 точек.
- `Queues_union_snapshot_and_served_queues` — `queue_stats` с `a`, живой сервер с `queues = {a, b}`, мёртвый с `{c}` → `a` (с числами, `Servers = 1`), `b` (нули, `Servers = 1`); `c` нет.
- `Servers_with_hour_stats_and_maintenance_tasks` — сервер со строками `stats_minutes` за час и старше; задачи `retention` (идёт: `last_finished_at < last_run_at`, `next_run_at > now()`), `partitions` (завершена) → `Running` true / false; `Stale` для сервера без heartbeat дольше `ServerStaleAfter`.
- `Job_types_are_listed_by_name_and_found_by_name` — два типа через `PublishTypeAsync`; `GetJobTypeAsync("nope") == null`.
- `Statements_run_read_only_with_statement_timeout` — через `internal ReadAsync` (IVT): store с `QueryTimeout = 50 мс`, SQL `SELECT pg_sleep(1)` → `PostgresException` с `SqlState == "57014"`; SQL `INSERT INTO {schema}.queue_stats VALUES ('x', 0, 0, 0, 0, now()) RETURNING 1` → `PostgresException` `25006` (read-only transaction).

`OverviewStorePreparedTests` (`public sealed class OverviewStorePreparedTests(PostgresFixture pg) : DashboardTestBase(pg, ConnectionMode.Direct)` — отдельный неабстрактный класс: prepared-режим проверяется только Direct, без `Skip` внутри абстрактного класса с `_Direct` / `_Bouncer`):
- `Prepared_statements_return_same_results_as_unprepared` — данные как в `Overview_sums_queue_stats_and_last_hour_buckets` и `Servers_with_hour_stats_and_maintenance_tasks` (`queue_stats`, `stats_minutes`, серверы, `maintenance_tasks`, два типа через `PublishTypeAsync`); второй store — `new DashboardStore(Db.DataSource, Db.Schema, usePreparedStatements: true, new PgBoltDashboardOptions(), Time)`; каждый метод этой задачи (`GetOverviewAsync`, `GetThroughputDayAsync`, `ListQueuesAsync`, `GetServersAsync`, `ListJobTypesAsync`, `GetJobTypeAsync`) вызывается на prepared-store дважды (второй вызов использует подготовленные команды), результат сравнивается с результатом `Store` (без prepared) через `Assert.Equivalent`; у `GetOverviewAsync` / `GetThroughputDayAsync` сравниваются точки с ненулевыми значениями (пустые точки зависят от границы минуты).

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~Cursor|FullyQualifiedName~ChunkedScan|FullyQualifiedName~StatsFreshness|FullyQualifiedName~OverviewStore"`
Expected: FAIL — компиляция.

- [ ] **Step 3: Каркас store**

`DashboardStore.cs`: поля из конструктора; `Render(template)` как в `PgBoltStore`; `internal async Task<T> ReadAsync<T>(string sql, Action<NpgsqlParameterCollection> bind, Func<NpgsqlDataReader, CancellationToken, Task<T>> read, CancellationToken ct)`:

```csharp
        if (OnStatement is { } observe)
            await observe(sql, bind, ct);
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        await using var batch = new NpgsqlBatch(conn, tx)
        {
            BatchCommands = { new("SET TRANSACTION READ ONLY"), new(_timeoutSql), new(sql) },
        };
        bind(batch.BatchCommands[2].Parameters);
        if (_prepare)
            await batch.PrepareAsync(ct);
        T result;
        await using (var r = await batch.ExecuteReaderAsync(ct))
            result = await read(r, ct);          // ридер уже на результате третьей команды: SET не возвращают строк
        await tx.CommitAsync(ct);
        return result;
```

`_timeoutSql = $"SET LOCAL statement_timeout = {(long)Math.Ceiling(options.QueryTimeout.TotalMilliseconds)}"` (инвариантная культура). Если Npgsql при `PrepareAsync` отвергает `SET` — подготавливать только третью команду нельзя; тогда `SET`-команды выполнить первым непредготовленным `NpgsqlCommand` с двумя statement'ами и подготовить только запрос (отклонение — в `DEVIATIONS`). Повтора `DbRetry` нет (read-only statement'ы не дают 40P01).

`ListKey.cs`, `Cursor.cs`: двоичный формат — байт версии `1`, байт формы (`CursorShape`), затем поля формы в big-endian: `Priority` (int16), `At` (тики int64, `Kind = Utc` при разборе), `Id` (16 байт, `Guid.TryWriteBytes(…, bigEndian: true)`), `Text` (UTF-8 до конца, не больше 800 байт); кодирование — `Base64Url.EncodeToString` (`System.Buffers.Text`). Любая ошибка разбора или несовпадение формы — `DashboardBadRequestException("invalid cursor")`.

`ChunkedScan.RunAsync` (§6.3):

```csharp
        var rows = new List<T>(pageSize);
        var cursor = after;
        var started = time.GetTimestamp();
        while (true)
        {
            var portion = await next(cursor, pageSize - rows.Count + 1, ct);
            foreach (var match in portion.Matches)
            {
                if (rows.Count == pageSize)
                    return new ScanResult<T>(rows, keyOf(rows[^1]), Incomplete: false);   // совпадение сверх страницы — данные есть
                rows.Add(match);
            }
            if (portion.Scanned < PortionSize)
                return new ScanResult<T>(rows, null, Incomplete: false);                  // данные кончились
            if (rows.Count == pageSize)
                return new ScanResult<T>(rows, keyOf(rows[^1]), Incomplete: false);
            cursor = portion.LastScanned;
            if (time.GetElapsedTime(started) >= budget)
                return new ScanResult<T>(rows, cursor, Incomplete: true);                 // «продолжить поиск» всегда продвигается
        }
```

Перевод в `Page<T>`: `Next = Cursor.Encode(result.Next, shape)`; `ScannedTo` (только при `Incomplete`) — время ключа: `At` для `AtId` / `PriorityAtId`, `UuidV7.GetTimestamp(Id)` для `Id`. Помощник `Page<T> ToPage<T>(ScanResult<T>, CursorShape)` и `Page<T> LimitPage<T>(List<T> fetched /* PageSize + 1 */, Func<T, ListKey>, CursorShape)` для простых списков (51-я строка → `Next` = ключ 50-й).

- [ ] **Step 4: Экраны задачи**

SQL (`DashboardStore.Overview.cs`):

```sql
-- OverviewTemplate (§6.1, §6.4)
SELECT (SELECT coalesce(sum(scheduled), 0) FROM {schema}.queue_stats)::bigint,
       (SELECT coalesce(sum(enqueued), 0) FROM {schema}.queue_stats)::bigint,
       (SELECT coalesce(sum(processing), 0) FROM {schema}.queue_stats)::bigint,
       (SELECT coalesce(sum(awaiting), 0) FROM {schema}.queue_stats)::bigint,
       (SELECT count(*) FROM {schema}.servers WHERE heartbeat_at > now() - @stale)::int,
       t.next_run_at, t.last_run_at, t.last_error, now()
FROM (SELECT 1) one LEFT JOIN {schema}.maintenance_tasks t ON t.name = 'stats';

-- HourTemplate: 60 поминутных точек; строка c без данных — одна строка с NULL
SELECT c.current, s.bucket, s.succeeded, s.failed, s.retried
FROM (SELECT date_trunc('minute', now()) AS current) c
LEFT JOIN LATERAL (
    SELECT bucket, sum(succeeded)::bigint AS succeeded, sum(failed)::bigint AS failed, sum(retried)::bigint AS retried
    FROM {schema}.stats_minutes WHERE bucket > c.current - interval '60 minutes'
    GROUP BY bucket
) s ON true
ORDER BY s.bucket;

-- DayTemplate: 96 точек по 15 минут
SELECT c.current, s.slot, s.succeeded, s.failed, s.retried
FROM (SELECT date_bin('15 minutes', now(), timestamptz '2000-01-01') AS current) c
LEFT JOIN LATERAL (
    SELECT date_bin('15 minutes', bucket, timestamptz '2000-01-01') AS slot,
           sum(succeeded)::bigint AS succeeded, sum(failed)::bigint AS failed, sum(retried)::bigint AS retried
    FROM {schema}.stats_minutes WHERE bucket >= c.current - interval '1425 minutes'
    GROUP BY 1
) s ON true
ORDER BY s.slot;

-- QueuesTemplate (§6.1 «Очереди»)
WITH live AS (
    SELECT u.queue, count(*)::int AS servers
    FROM {schema}.servers s CROSS JOIN LATERAL unnest(s.queues) AS u(queue)
    WHERE s.heartbeat_at > now() - @stale
    GROUP BY u.queue
)
SELECT coalesce(q.queue, l.queue), coalesce(q.scheduled, 0), coalesce(q.enqueued, 0), coalesce(q.processing, 0),
       coalesce(q.awaiting, 0), coalesce(l.servers, 0), q.measured_at
FROM {schema}.queue_stats q FULL JOIN live l ON l.queue = q.queue
ORDER BY 1;

-- ServersTemplate
SELECT s.id, s.name, s.host, s.queues, s.capacity, s.started_at, s.heartbeat_at, s.heartbeat_at <= now() - @stale,
       coalesce(st.succeeded, 0), coalesce(st.failed, 0), coalesce(st.retried, 0)
FROM {schema}.servers s
LEFT JOIN (SELECT server_id, sum(succeeded)::bigint AS succeeded, sum(failed)::bigint AS failed, sum(retried)::bigint AS retried
           FROM {schema}.stats_minutes WHERE bucket > now() - interval '1 hour' GROUP BY server_id) st ON st.server_id = s.id
ORDER BY s.name, s.id;

-- MaintenanceTasksTemplate (часть 2 §5.7: идёт ли запуск)
SELECT name, next_run_at, last_run_at, last_finished_at, last_error,
       last_run_at IS NOT NULL AND (last_finished_at IS NULL OR last_finished_at < last_run_at) AND next_run_at > now()
FROM {schema}.maintenance_tasks ORDER BY name;

-- JobTypesTemplate / JobTypeTemplate
SELECT name, args_schema::text, default_queue, default_max_attempts, default_priority, default_retry_delays, published_by, last_seen_at
FROM {schema}.job_types {WHERE name = @name} ORDER BY name;
```

`@stale` — `ServerStaleAfter`. `GetOverviewAsync` — два statement'а (сводка, час); точки без данных — нули; `Freshness = StatsFreshness.Evaluate(row, dbNow, StatsStaleAfter)`. `GetServersAsync` — два statement'а. `StatsFreshness.Evaluate` — §6.4: `row is null` или `now - next > staleAfter + 0.5 × (next - (last ?? next))` → `NotRunning`; иначе `LastError` не `null` → `Failing`; иначе `Fresh`.

`DashboardTestBase` (сигнатуры — «Контракт»; `Store` — `internal`-свойство, базовый класс публичный, а `DashboardStore` internal: `protected DashboardStore Store` не компилируется, CS0053): `TestDb.CreateAsync(pg, mode)`; `ServiceCollection` с `AddPgBolt(o => { o.DataSource = Db.DataSource; o.Schema = Db.Schema; o.AutoMigrate = false; })`, `AddPgBoltDashboard(o => o.ScanBudget = …)`, `TimeProvider` → `Time` (`StepTimeProvider`, `Step = 0` по умолчанию); `Core` = `PgBoltStore` из DI; `Store` = `DashboardStore` из DI; `Seeder = new JobSeeder(Db, Core)`. `SeedJobsAsync` / `LeaseAsync` — делегаты к `Seeder`, `ResolveAllAsync()` — `ResolveAll.RunAsync(Core, ct)`: сами реализации живут в общих помощниках `Support/`, потому что тесты Task 13–14 работают на `DashboardHost`, а не наследуют `DashboardTestBase`.

`Support/JobSeeder.cs` — `public sealed class JobSeeder(TestDb db, PgBoltStore core)`: `SeedJobsAsync(...)` (параметры — «Контракт») и `LeaseAsync(string queue = "default")` (один lease задачи с самым ранним `run_at` через `core`). `SeedJobsAsync` — один statement `INSERT … SELECT FROM unnest(@ids)` в `jobs` (ids — `Guid.CreateVersion7()` в C#, по возрастанию) и, по `queueRow`, в `queue` с инвариантами (Scheduled: `ready = false`, `run_at = runAtUtc ?? now() + 1 h`; Enqueued: `ready = true`, `run_at = runAtUtc ?? now()`; Awaiting: `ready = false`, `run_at = 'infinity'`, `await_condition = 1`, `parent_id = gen_random_uuid()`); `priority` пишется в `jobs.priority` и `queue.priority`; при `runAtStep` у Scheduled / Enqueued `run_at = (runAtUtc ?? значение по умолчанию) + i × runAtStep` (`i` — порядковый номер в вызове); у терминальных `state` (2, 3, 4) — `started_at` и `finished_at = clock_timestamp() + i × 1 мс` (`i` — порядковый номер в вызове: время растёт в порядке вставки и между вызовами). Тесты на `DashboardHost` создают его как `new JobSeeder(db, host.Services.GetRequiredService<PgBoltStore>())`.

`Support/ResolveAll.cs` — `public static class ResolveAll { public static Task RunAsync(PgBoltStore core, CancellationToken ct); }`: циклы `core.CountCompletionsAsync(500)` → `core.ResolveAsync(500, 5000, 1 с)` → `core.CancelMembersAsync(1000, 5000, 100, 1 с)` (internal-методы `PgBoltStore`, IVT из Task 2), пока есть работа, не больше 200 проходов, иначе `InvalidOperationException` (как `ResolverPasses.SettleAsync(cancelMembers: true)` в `PgBolt.Postgres.Tests`, который отсюда недоступен: `CancelBatchAsync` только пишет сигнал kind 6, члены отменяет, а batch завершает `CancelMembersAsync` с учётом). Единственная копия цикла в проекте тестов dashboard.

- [ ] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~Cursor|FullyQualifiedName~ChunkedScan|FullyQualifiedName~StatsFreshness|FullyQualifiedName~OverviewStore"`
Expected: PASS (Direct и Bouncer).

- [ ] **Step 6: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): read-only store, keyset cursors, budgeted portion scan, overview data"
```

---
### Task 8: `DashboardStore`: списки из `queue` (Scheduled, Enqueued, Processing, Awaiting)

**Files:**
- Create: `src/PgBolt.Dashboard/Data/DashboardStore.QueueLists.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Data/QueueListsTests.cs`

**Interfaces:**
- Consumes: Task 7 (`ReadAsync`, `ChunkedScan`, `Cursor`, `Page`).
- Produces: `ListJobsAsync` для `Scheduled`, `Enqueued`, `Processing`, `Awaiting` (остальные виды — `NotSupportedException` до Task 9).

- [ ] **Step 1: Тесты**

`QueueListsTests` (`DashboardTestBase`, Direct/Bouncer):
- `Scheduled_is_ordered_by_run_at_and_excludes_awaiting_and_ready` — Scheduled с `run_at` +3 ч, +1 ч, +2 ч; один Enqueued; один Awaiting → три строки по возрастанию `RunAt`.
- `Scheduled_pages_with_equal_run_at_cover_each_job_once` (Review Focus 1) — 120 Scheduled с одним `run_at` (`SeedJobsAsync`) → три страницы по `Next` (50, 50, 20), объединение id = все 120 без повторов, у третьей `Next == null`.
- `Enqueued_requires_queue` — `queue: null` → `DashboardBadRequestException`.
- `Enqueued_is_ordered_by_priority_desc_then_run_at` — приоритеты 0, 5, 5, -1 в очереди `a` и задача в `b` → порядок 5, 5 (по `run_at`), 0, -1; `b` нет.
- `Enqueued_pages_with_equal_priority_and_run_at_cover_each_job_once` (Review Focus 1) — 70 задач приоритета 3 и 40 приоритета 1 с одним `run_at` (`SeedJobsAsync(…, queueRow: "enqueued", runAtUtc: t, priority: 3)` и `priority: 1`) → страницы 50 / 50 / 10 без повторов и пропусков, порядок внутри страницы — по `id`.
- `Processing_is_keyset_by_leased_at_then_id` — 60 задач, `LeaseAsync` всех (по одной, `leased_at` различны) → первая страница — 50 самых ранних по `leased_at`, вторая — 10, `Next` второй `null`.
- `Awaiting_is_newest_first` — 3 Awaiting → `id` по убыванию.
- `Type_filter_scans_in_portions_and_reports_progress` — `Time.Step = 1 с`, `ScanBudget = 2 с`; 4500 Scheduled типа `noise` (`SeedJobsAsync(4500, "noise", …, queueRow: "scheduled", runAtUtc: now + 1 ч, runAtStep: 1 мс)`) и 5 типа `needle` с `run_at` позже всех → первая страница: 0 строк, `ScannedTo` не `null`, `Next` не `null`; переход по `Next` (повторно, пока `Rows` пусто и `Next` есть) находит 5 строк `needle`, последняя страница — `ScannedTo == null`.
- `Queue_filter_on_awaiting_uses_portions` — Awaiting в очередях `a` (3) и `b` (2) → фильтр `b` даёт 2.
- `Enqueued_type_filter_follows_mixed_order` — очередь `a`, приоритеты 2 и 1, тип `x` у части → строки только `x`, порядок priority DESC, run_at, id.
- `Rows_carry_job_columns` — у строки Scheduled `Type`, `Queue`, `Priority`, `Attempt`, `MaxAttempts`, `CreatedAt`, `RunAt`; у Processing — `StartedAt` (= `leased_at`), `ServerId` (= `lease_owner`).

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~QueueListsTests"`
Expected: FAIL.

- [ ] **Step 3: SQL и обход**

Колонки строки (`JobListRow`) для списков из `queue` — `QueueRowColumns`:
`j.id, j.type, j.queue, j.priority, q.attempt, q.max_attempts, j.created_at, CASE WHEN q.run_at < 'infinity' THEN q.run_at END, q.leased_at, q.lease_owner, NULL::timestamptz, q.cancel_requested, NULL::smallint, NULL::text`.

Простые выборки (`LIMIT @limit`, `@limit = PageSize + 1`; `{after}` — пусто для первой страницы):

```sql
-- Scheduled (§6.1): queue_delayed; run_at < 'infinity' — условие индекса
SELECT {cols} FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id
WHERE q.lease_owner IS NULL AND NOT q.ready AND q.run_at < 'infinity' AND q.await_condition IS NULL
  {after: AND q.run_at >= @after_at AND (q.run_at, q.job_id) > (@after_at, @after_id)}
ORDER BY q.run_at, q.job_id LIMIT @limit;

-- Enqueued, первая страница (§6.1): queue_fetch
SELECT {cols} FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id
WHERE q.queue = @queue AND q.lease_owner IS NULL AND q.ready
ORDER BY q.priority DESC, q.run_at, q.job_id LIMIT @limit;

-- Enqueued после курсора (§6.3): UNION ALL двух ветвей, каждая со своим ORDER BY … LIMIT по queue_fetch
SELECT {cols} FROM (
    (SELECT * FROM {schema}.queue
     WHERE queue = @queue AND lease_owner IS NULL AND ready AND priority = @after_priority
       AND run_at >= @after_at AND (run_at, job_id) > (@after_at, @after_id)
     ORDER BY run_at, job_id LIMIT @limit)
    UNION ALL
    (SELECT * FROM {schema}.queue
     WHERE queue = @queue AND lease_owner IS NULL AND ready AND priority < @after_priority
     ORDER BY priority DESC, run_at, job_id LIMIT @limit)
) q JOIN {schema}.jobs j ON j.id = q.job_id
ORDER BY q.priority DESC, q.run_at, q.job_id LIMIT @limit;

-- Processing (§6.1): все взятые строки (≤ ёмкости серверов) — сортировка и курсор (leased_at, job_id) в памяти.
-- Поиск задачи по PK ограждён LATERAL … OFFSET 0 (отклонение 19): обычный JOIN по сотням строк даёт Gather → Hash Join → Seq Scan по jobs_*
SELECT {cols} FROM {schema}.queue q CROSS JOIN LATERAL (
    SELECT j.id, j.type, j.queue, j.priority, j.created_at FROM {schema}.jobs j
    WHERE j.id = q.job_id AND (@type::text IS NULL OR j.type = @type)
    OFFSET 0
) j
WHERE q.lease_owner IS NOT NULL AND (@queue::text IS NULL OR q.queue = @queue);

-- Awaiting (§6.1): queue_awaiting_list
SELECT {cols} FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id
WHERE q.await_condition IS NOT NULL {after: AND q.job_id < @after_id}
ORDER BY q.job_id DESC LIMIT @limit;
```

Порционная форма (§6.3) — для Scheduled и Awaiting с фильтром очереди или типа и для Enqueued с фильтром типа. Общий шаблон (`{keys}` — выборка ключей списка с `LIMIT 2000` и колонками `q.*`, `{order}` / `{desc}` — порядок списка и обратный ему, `{last}` — колонки ключа, `{keycols}` — те же ключевые выражения порядка колонками `m` под псевдонимами `o1, o2, …` (`q.run_at AS o1, q.job_id AS o2`), `{order on m}` — порядок списка по `m.o1, m.o2, …` с теми же направлениями):

```sql
WITH k AS MATERIALIZED ({keys}),
s AS (
    SELECT (SELECT count(*) FROM k)::int AS scanned, l.*
    FROM (SELECT 1) one LEFT JOIN LATERAL (SELECT {last} FROM k ORDER BY {desc} LIMIT 1) l ON true
),
m AS (
    SELECT {cols}, {keycols}
    FROM k q CROSS JOIN LATERAL (
        SELECT j.id, j.type, j.queue, j.priority, j.created_at FROM {schema}.jobs j
        WHERE j.id = q.job_id AND (@type::text IS NULL OR j.type = @type)
        OFFSET 0
    ) j
    WHERE (@queue::text IS NULL OR q.queue = @queue)
    ORDER BY {order} LIMIT @need
)
SELECT s.*, m.* FROM s LEFT JOIN m ON true ORDER BY {order on m};
```

Поиск задачи по PK — `CROSS JOIN LATERAL (… WHERE j.id = q.job_id … OFFSET 0)` (отклонение 19): обычный `JOIN jobs j ON j.id = q.job_id` по множеству из CTE на PG17 уходит в `Hash Join → Append → Seq Scan on jobs_*` (с фильтром типа — `Merge Join → Parallel Seq Scan`); ограждённая форма даёт `Nested Loop → CTE Scan on k → Append → Index Scan using jobs_*_pkey` и передаёт порядок CTE наружу (Sort над `m` нет; на PG17 проверено при заполненных партициях). Строка с `m.id IS NULL` — порция без совпадений; `Scanned` и последний ключ — из первых колонок. `{keys}`:
- Scheduled — тело простой выборки Scheduled без `JOIN` и `{cols}` (`SELECT q.* FROM {schema}.queue q WHERE … {after} ORDER BY q.run_at, q.job_id LIMIT 2000`), ключ `(run_at, job_id)`, форма `AtId`;
- Awaiting — `SELECT q.* FROM {schema}.queue q WHERE q.await_condition IS NOT NULL {after} ORDER BY q.job_id DESC LIMIT 2000`, ключ `job_id`, форма `Id`;
- Enqueued — первая страница или `UNION ALL` выше без `JOIN` с `LIMIT 2000` в ветвях и снаружи, ключ `(priority, run_at, job_id)`, форма `PriorityAtId`.

Простые выборки с `JOIN jobs j ON j.id = q.job_id` и `LIMIT @limit` (Scheduled, Enqueued — первая страница и по курсору, Awaiting без фильтра) остаются обычным `JOIN`: план `Limit → Nested Loop → Index Scan queue_* → Append → Index Scan jobs_*_pkey` проверен на PG17 при заполненных партициях. Если Task 11 покажет Hash / Merge Join или Seq Scan по `jobs_*` в такой выборке — допустимое исправление Task 11: заменить `JOIN` на `CROSS JOIN LATERAL (… OFFSET 0)` как выше.

`ListJobsAsync`: курсор разбирается формой вида списка; Enqueued без очереди — `DashboardBadRequestException("queue is required for Enqueued")` (страница `jobs/enqueued` без `queue` до store не доходит — Task 13, отклонение 22); выбор формы (простая / порционная) — по наличию фильтра вне условия индекса (§6.1 последний абзац).

- [ ] **Step 4: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~QueueListsTests"`
Expected: PASS (Direct и Bouncer).

- [ ] **Step 5: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): scheduled, enqueued, processing and awaiting lists"
```

---
### Task 9: `DashboardStore`: Succeeded, Failed, Cancelled

**Files:**
- Create: `src/PgBolt.Dashboard/Data/DashboardStore.TerminalLists.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Data/TerminalListsTests.cs`

**Interfaces:**
- Consumes: Task 7–8.
- Produces: `ListJobsAsync` для `Succeeded`, `Failed`, `Cancelled`.

- [ ] **Step 1: Тесты**

`TerminalListsTests` (`DashboardTestBase`, Direct/Bouncer; задачи — через `Core`: `EnqueueAsync`, `LeaseAsync`, `Core.AckAsync` / `FailAsync` / `RetryAsync` / `SweepExpiredLeasesAsync`, `Client.CancelAsync` / `RequeueAsync`):
- `Succeeded_is_newest_first` — три успешные → `id` по убыванию; `FinishedAt`, `StartedAt`, `ServerId` заполнены.
- `Failed_from_worker_no_executor_and_sweeper_are_listed` — fail (kind 2, message «boom»), NoExecutor (kind 7), sweeper на последней попытке (`maxAttempts: 1`, expire, sweep; kind 3, `Message == null`) → три строки, `EventKind` соответствующие.
- `Non_terminal_events_do_not_list` — sweeper с оставшимися попытками (kind 3, state 0) и retry NoExecutor (kind 7, state 0) → Failed пуст.
- `Requeue_removes_job_from_failed` — fail, requeue → Failed пуст.
- `Repeated_failure_after_requeue_is_one_row` — fail, requeue, lease, fail → одна строка; то же со вторым отказом через sweeper (`Repeated_failure_through_sweeper_is_one_row`).
- `Sweeper_with_cancel_request_lists_only_in_cancelled` — lease, `CancelAsync`, expire, sweep → Cancelled — одна строка, Failed пуст.
- `Cancelled_lists_cancelled_jobs` — cancel Scheduled-задачи → одна строка `EventKind == Cancelled`, `Message` — причина.
- `Events_of_missing_jobs_give_no_rows` — `INSERT INTO {schema}.job_events (job_id, at, kind) VALUES (gen_random_uuid(), now(), 2)` (задачи нет — как после retention `jobs`) → Failed пуст.
- `Failed_type_filter_and_budget_progress` — `Time.Step = 1 с`, `ScanBudget = 2 с`; сначала 3 задачи типа `needle`, затем 4500 типа `noise` (`SeedJobsAsync(…, state: 3, queueRow: "none")`; затем `UPDATE {schema}.jobs SET finished_at = finished_at + interval '1 minute' WHERE type = 'noise'` — `noise` гарантированно новее) и события kind 2 `INSERT INTO {schema}.job_events (job_id, at, kind) SELECT id, finished_at, 2 FROM {schema}.jobs WHERE state = 3` — `needle` старше всех, Failed идёт по убыванию `at` → первая страница пуста и неполная, `ScannedTo` — время из `at`; продолжение находит 3 строки.
- `Succeeded_portions_skip_other_states` — 2500 Failed-задач (`state = 3`, без событий) новее 2 Succeeded → две строки за две порции (бюджет не исчерпан при `Step = 0`).

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~TerminalListsTests"`
Expected: FAIL.

- [ ] **Step 3: SQL**

Все три — порционная форма Task 8 (state не в индексе, §6.1); Failed и Cancelled — один шаблон `TerminalEventsTemplate` с подстановкой литералов; соединение с `jobs` — ограждённый LATERAL (отклонение 19):

```sql
-- Succeeded: ключи — PK партиций, id DESC (форма Id)
-- {keys}:
SELECT j.id, j.type, j.queue, j.priority, j.attempt, j.max_attempts, j.created_at, j.started_at, j.server_id, j.finished_at, j.state
FROM {schema}.jobs j {after: WHERE j.id < @after_id} ORDER BY j.id DESC LIMIT 2000
-- m (без соединения: jobs уже прочитана в {keys}): WHERE k.state = 2 AND фильтры очереди / типа по k; ORDER BY k.id DESC; ключ порядка — k.id AS o1
-- колонки строки: id, type, queue, priority, attempt, max_attempts, created_at, NULL (RunAt), started_at, server_id, finished_at, false, NULL, NULL

-- TerminalEventsTemplate — один шаблон для Failed и Cancelled: ключи — job_events_failed / job_events_cancelled, (at, job_id) DESC (форма AtId)
-- {keys}:
SELECT e.at, e.job_id, e.kind, e.message FROM {schema}.job_events e
WHERE e.kind {kinds} {after: AND e.at <= @after_at AND (e.at, e.job_id) < (@after_at, @after_id)}
ORDER BY e.at DESC, e.job_id DESC LIMIT 2000
-- m: поиск задачи по PK — LATERAL … OFFSET 0 (Task 8, отклонение 19); ключ порядка — o1 = k.at, o2 = k.job_id (внешний ORDER BY m.o1 DESC, m.o2 DESC)
SELECT j.id, j.type, j.queue, j.priority, j.attempt, j.max_attempts, j.created_at, NULL::timestamptz, j.started_at, j.server_id,
       j.finished_at, false, k.kind, k.message, k.at AS o1, k.job_id AS o2
FROM k CROSS JOIN LATERAL (
    SELECT j.id, j.type, j.queue, j.priority, j.attempt, j.max_attempts, j.created_at, j.started_at, j.server_id, j.finished_at
    FROM {schema}.jobs j
    WHERE j.id = k.job_id AND j.state = {state} AND j.finished_at = k.at
      AND (@queue::text IS NULL OR j.queue = @queue) AND (@type::text IS NULL OR j.type = @type)
    OFFSET 0
) j
ORDER BY k.at DESC, k.job_id DESC LIMIT @need
```

Подстановка при сборке текста запроса (литералы, не параметры): Failed — `{kinds}` = `IN (2, 3, 7)`, `{state}` = `3`; Cancelled — `{kinds}` = `= 4`, `{state}` = `4`. Отдельной копии SQL для Cancelled нет.

`e.at <= @after_at` — простое условие для plan-time pruning партиций `job_events` (row-сравнение pruning не даёт). Предикат `kind IN (2, 3, 7)` / `kind = 4` — литералом, совпадающим с частичным индексом.

- [ ] **Step 4: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~TerminalListsTests|FullyQualifiedName~QueueListsTests"`
Expected: PASS (Direct и Bouncer).

- [ ] **Step 5: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): succeeded, failed and cancelled lists over job_events"
```

---
### Task 10: `DashboardStore`: карточки, batches, recurring

**Files:**
- Create: `src/PgBolt.Dashboard/Data/DashboardStore.Related.cs`
- Modify: `src/PgBolt.Postgres/UuidV7.cs` (`LowerBound(DateTimeOffset)`), `tests/PgBolt.Postgres.Tests/UuidV7Tests.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Data/RelatedStoreTests.cs`

**Interfaces:**
- Consumes: Task 7; `PgBoltStore.Derive` (Task 2); `IJobClient.Workflow`, `IRecurringJobManager`.
- Produces: методы Task 10 из «Контракта»; `UuidV7.LowerBound(DateTimeOffset at)`.

- [ ] **Step 1: Тесты**

`UuidV7Tests.Lower_bound_of_timestamp_precedes_ids_of_that_millisecond` — `LowerBound(t) <= Guid.CreateVersion7(t)` по сравнению PostgreSQL (big-endian байты), `GetTimestamp(LowerBound(t)) == t` с точностью до мс.

`RelatedStoreTests` (`DashboardTestBase`, Direct/Bouncer; workflow — динамическими шагами `Enqueue("dash.job", args)`, `ContinueWith("dash.job", args, condition)`; resolver и отмена batch — `ResolveAllAsync()` из `DashboardTestBase`):
- `Job_continuations_include_launched_ones_with_current_state` — родитель с двумя продолжениями (OnSuccess, OnAnyFinished); до завершения — оба `Awaiting`; после ack родителя и прохода resolver'а — оба `Enqueued`; `Continuation` — 1 и 2.
- `Batch_continuation_members_are_not_continuations_of_parent` — `parent.ContinueWithBatch(b => …два члена…)`: `ListContinuationsAsync(parent, batchParent: false, …)` пуст, `ListBatchContinuationsAsync(parent)` — один batch (`batchCont`). Члены batch-продолжения имеют `continuation = 5` и `parent_id` = id самого `batchCont`, а не родителя (§3.2, часть 5 §2.1): карточка `batchCont` не показывает их продолжениями — `ListContinuationsAsync(batchCont.Id, batchParent: true, created, …)` пуст; они только члены — `ListBatchJobsAsync(batchCont.Id, created, null, …)` возвращает 2 (§6.2). `created` — `(await Client.GetBatchAsync(batchCont.Id))!.CreatedAt` (у `IBatchStep` есть только `Id`).
- `Batch_card_lists_members_nested_and_continuations` — batch с двумя задачами и вложенным batch, продолжение-задача batch (код 3) и batch-продолжение batch → `ListBatchJobsAsync` — 2 (+ члены не вложенного), `ListNestedBatchesAsync` — 1, `ListContinuationsAsync(batch, batchParent: true, …)` — 1, `ListBatchContinuationsAsync(batch)` — 1.
- `Batch_progress_is_never_negative` — `UPDATE batches SET succeeded = -2, failed = -1` → строки `BatchRow` с `0`.
- `Batches_list_newest_first_and_filter_by_state_in_portions` — 3 batches; один отменён (`CancelBatchAsync` + `ResolveAllAsync()`) → без фильтра — по убыванию `id`; `BatchState.Cancelled` — один.
- `Continuations_page_by_fifty` — 55 продолжений одного родителя → 50 + 5.
- `Recurring_list_by_id_with_text_cursor` — 52 расписания (`IRecurringJobManager.AddOrUpdateAsync(id, "dash.job", "0 0 * * *", args)`, id с `/` и кириллицей среди них) → 50 + 2, порядок — `ORDER BY id` PostgreSQL.
- `Recurring_runs_are_newest_first_with_derived_state` — расписание с `new RecurringJobOptions { Overlap = OverlapPolicy.Allow }` (по умолчанию `Skip`: ручной `TriggerAsync` берёт ключ `pgbolt:recurring:{id}`, и второй и третий вызовы вернули бы существующую задачу, а не три запуска); три `TriggerAsync(id)`, затем `leased = await LeaseAsync()` (берёт запуск с самым ранним `run_at`, т. е. самый старый `id`) → `ListRecurringRunsAsync` по убыванию `id`: состояния по фактическому lease — у строки с `Id == leased.Id` `Processing`, у двух других `Enqueued` (то есть `Enqueued`, `Enqueued`, `Processing`).

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~RelatedStoreTests"` и `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~UuidV7Tests"`
Expected: FAIL.

- [ ] **Step 3: SQL**

`RelatedJobColumns` = `j.id, j.type, j.queue, j.state, j.continuation, j.created_at, j.finished_at, q.job_id IS NOT NULL, coalesce(q.ready, false), q.lease_owner, q.await_condition IS NOT NULL` → `RelatedJobRow` с `State = PgBoltStore.Derive(state, inQueue, ready, leaseOwner, awaiting)`. `BatchColumns` = `id, state, total, greatest(0, succeeded), greatest(0, failed), greatest(0, cancelled), condition, cancel_requested, created_at, finished_at` (часть 5 §10). Все списки — `LIMIT PageSize + 1`, `{after}` для первой страницы пуст.

```sql
-- Continuations (§6.2): jobs_parent; IN-список — литерал (1, 2) или (3, 4), совпадает с частичным индексом
SELECT {RelatedJobColumns} FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
WHERE j.parent_id = @parent AND j.continuation IN (1, 2) AND j.id >= @lower {after: AND j.id > @after_id}
ORDER BY j.id LIMIT @limit;

-- Batch jobs (§6.2, отклонение 10): jobs_batch
SELECT {RelatedJobColumns} FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
WHERE j.batch_id = @batch AND j.id >= @lower {after: AND j.id > @after_id}
ORDER BY j.id LIMIT @limit;

-- Batch continuations: batches_parent;  Nested: batches_nested
SELECT {BatchColumns} FROM {schema}.batches WHERE parent_id = @parent {after: AND id > @after_id} ORDER BY id LIMIT @limit;
SELECT {BatchColumns} FROM {schema}.batches WHERE parent_batch_id = @batch {after: AND id > @after_id} ORDER BY id LIMIT @limit;

-- Batches list: PK id DESC; с фильтром состояния — порционная форма Task 8 с {keys} =
--   SELECT {BatchColumns} FROM {schema}.batches {after: WHERE id < @after_id} ORDER BY id DESC LIMIT 2000, m: WHERE state = @state
SELECT {BatchColumns} FROM {schema}.batches {after: WHERE id < @after_id} ORDER BY id DESC LIMIT @limit;

-- Recurring list (форма Text) и история запусков (часть 4 §9): jobs_recurring
SELECT id, type, cron, time_zone, owner, paused, next_fire_at, last_fire_at, last_job_id, last_error
FROM {schema}.recurring_jobs {after: WHERE id > @after_text} ORDER BY id LIMIT @limit;
SELECT {RelatedJobColumns} FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
WHERE j.recurring_id = @id {after: AND j.id < @after_id} ORDER BY j.id DESC LIMIT @limit;
```

`@lower` = `UuidV7.LowerBound(createdAt.AddDays(-1))` (§6.2: запас на расхождение часов клиентов). `UuidV7.LowerBound(DateTimeOffset at)` — 48-битный префикс `at.ToUnixTimeMilliseconds()`, остальные байты нули (как `LowerBound(DateOnly)`); XML-doc.

- [ ] **Step 4: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~RelatedStoreTests"` и `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~UuidV7Tests"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/UuidV7.cs src/PgBolt.Dashboard tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): job and batch relations, batches and recurring lists"
```

---
### Task 11: Контракт планов (`EXPLAIN`)

**Files:**
- Test: `tests/PgBolt.Dashboard.Tests/Data/PlanContractTests.cs`, `Data/PlanFixture.cs`

**Interfaces:**
- Consumes: все методы `DashboardStore` (Task 7–10), `DashboardStore.OnStatement`.

- [ ] **Step 1: Фикстура и тесты**

`PlanFixture(PostgresFixture pg) : IAsyncLifetime` — фикстура класса: схема заполняется **один раз на класс** (~10 с), а не на каждый тест (`InitializeAsync` базового `IAsyncLifetime` выполняется на каждый тест — отсюда `IClassFixture`; в xUnit v3 фикстура класса принимает assembly-фикстуру `PostgresFixture` в конструкторе; если среда этого не допускает — `PlanFixture` берёт `PostgresFixture` из статического поля, заданного конструктором теста, — отклонение в `DEVIATIONS`). `[Trait("Category", "Slow")] public sealed class PlanContractTests(PlanFixture fx) : IClassFixture<PlanFixture>`; `DashboardStore NewStore() => new(fx.Db.DataSource, fx.Db.Schema, usePreparedStatements: false, new PgBoltDashboardOptions(), TimeProvider.System)` — каждый тест создаёт свой store и свой список планов. Члены фикстуры: `TestDb Db` (`TestDb.CreateAsync(pg, ConnectionMode.Direct)`; `DisposeAsync` фикстуры освобождает его), `Guid ParentId` / `DateTimeOffset ParentCreatedAt` (задача-родитель с продолжениями кодов 1–4), `Guid BatchId` / `DateTimeOffset BatchCreatedAt` (batch с членами), `string RecurringId = "r1"`, `string[] Queues`, `string[] Types`.

**Заполняются все присоединённые партиции `jobs` и `job_events`** (отклонение 19). Их сутки — из `pg_inherits` (`TestDb` создаёт сегодня, +1 и +2 по UTC; число партиций не зашито в тест), задача `g` относится к суткам `1 + g % n`. SQL-вставки с `generate_series`; общий CTE суток и выражения, привязывающие id и время к суткам партиции:

```sql
WITH days AS (
    SELECT row_number() OVER (ORDER BY d) AS n, d
    FROM (SELECT to_date(right(c.relname, 8), 'YYYYMMDD') AS d
          FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
          WHERE i.inhparent = to_regclass('{schema}.jobs')) x
)
-- ... FROM generate_series(1, 200000) g JOIN days ON days.n = 1 + g % (SELECT max(n) FROM days)
-- t(g)  = (days.d::timestamp AT TIME ZONE 'UTC') + ((g * 7919) % 80000000) * interval '1 ms'      -- внутри суток партиции (< 22,3 ч)
-- id(g) = (lpad(to_hex((extract(epoch FROM t(g)) * 1000)::bigint), 12, '0') || substr(md5(g::text), 1, 20))::uuid   -- UUIDv7-префикс → партиция jobs своих суток
```

Объём: `jobs` — 200 000 (60 % state 2, 20 % state 3, 10 % state 4, 10 % state 0; `type` из 20 значений; 2 % с `recurring_id` из 50; 5 % с `parent_id` и `continuation` 1–4; 5 % с `batch_id` из 1000); `created_at = t(g)`, у терминальных `finished_at = t(g) + interval '1 second'`; `queue` — 20 000 Scheduled, 20 000 Awaiting, 5 000 Enqueued в 5 очередях с 3 приоритетами и равными `run_at` у половины, 500 Processing (всего 45 500 строк; ссылаются на любые различные задачи фикстуры — `job_id` уникален, состояние `jobs` для планов значения не имеет); `job_events` — kind 2 / 3 / 7 по задачам state 3 и kind 4 по задачам state 4 (`at = finished_at`, т. е. в сутках партиции `jobs` задачи), шумовые kind 1 / 5 (`at = created_at + interval '2 seconds'`) — события есть в каждой партиции `job_events`; `batches` — 5 000 (из них 1 000 с `parent_id`); `recurring_jobs` — 50; `servers` — 3; `stats_minutes` — 24 × 60 × 3 × 5; `queue_stats` — 5. Затем `ANALYZE` всей схемы и проверка заполненности: `SELECT count(*) FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid WHERE i.inhparent IN (to_regclass('{schema}.jobs'), to_regclass('{schema}.job_events')) AND c.relpages = 0` должен вернуть `0` — иначе фикстура бросает исключение (пустая партиция после `ANALYZE` получает Seq Scan в любом параметризованном Nested Loop).

Сбор планов: `store.OnStatement = async (sql, bind, ct) => { await using var cmd = fx.Db.Admin.CreateCommand("EXPLAIN (FORMAT JSON) " + sql); bind(cmd.Parameters); plans.Add((sql, (string)(await cmd.ExecuteScalarAsync(ct))!)); }`. Помощники: `Flatten(JsonElement)` (как в `DequeuePlanTests`; обходит `Plans` всех дочерних узлов, включая `InitPlan` — тела CTE), `AssertNoSeqScan(plan)` — нет узла `"Node Type": "Seq Scan"` с `"Relation Name"` = `queue` или начинающимся с `jobs` / `job_events`.

Тесты:
- `Every_screen_statement_avoids_seq_scans` — вызвать все методы store: обзор, день, очереди, серверы, типы; каждый вид `ListJobsAsync` — первая страница, страница по `Next`, с фильтром очереди, с фильтром типа (для Enqueued — с очередью); продолжения задачи и batch, члены, вложенные, batch-продолжения, список batches с фильтром и без, recurring, история — первая страница и по `Next` → для каждого собранного плана `AssertNoSeqScan`; число собранных statement'ов ≥ 40. Идентификаторы (`parentId`, `batchId`, `recurringId`, очереди, типы) — из членов `PlanFixture`; курсор страницы по `Next` берётся из ответа первой страницы.
- `Scheduled_with_many_awaiting_rows_bounds_queue_delayed_by_infinity` — план первой страницы Scheduled: узел Index Scan по `queue_delayed`, его `Index Cond` содержит `'infinity'`.
- `Failed_and_cancelled_read_index_order_without_sort` (§12: «без Sort над диапазоном индекса», отклонение 20) — планы Failed / Cancelled (первая страница и по курсору, без фильтров и с фильтром типа): (1) нет узла `Sort` / `Incremental Sort`, над которым в поддереве, достижимом через дочерние узлы с `"Parent Relationship"` = `Outer` / `Inner` / `Member` (дочерние `InitPlan` / `SubPlan` — тела CTE — не обходятся), есть Index Scan / Index Only Scan по `job_events_*`: корневой `Sort` внешнего `ORDER BY` и `Sort` над CTE `k` в `s` (до ≤ 2000 строк) допустимы — в JSON-плане CTE `k` с его Index Scan висит дочерним узлом `InitPlan` корня, поэтому наивный обход поддерева ошибочно признал бы их нарушением; (2) в поддереве CTE `k` (`"Subplan Name": "CTE k"`) нет `Sort` — `Limit → Append` / `Merge Append` из Index Scan [Backward] по партициям `job_events_*`; (3) есть Index Scan по индексу родителя `job_events_failed` / `job_events_cancelled` (по партициям; имена — через `ParentIndexAsync`).
- `Deep_enqueued_page_uses_queue_fetch_in_both_branches` — план Enqueued по курсору **без фильтра типа** (простая форма: statement с `UNION ALL` без `MATERIALIZED`; порционная форма с фильтром типа читает до 2000 ключей и вправе брать Bitmap Scan — для неё действует только `AssertNoSeqScan`): два Index Scan по `queue_fetch`, `Index Cond` первого содержит `queue`, `priority` и `run_at`, второго — `queue` и `priority`.
- `Continuations_use_jobs_parent_and_recurring_uses_jobs_recurring` — Index Scan по `jobs_parent` (продолжения) и `jobs_recurring` (история).

Индексы партиций (`jobs_*`, `job_events_*`) получают автоматические имена (два индекса `(at, job_id)` у `job_events` — с суффиксами `_idx` / `_idx1`), поэтому все проверки имени индекса идут через помощник `ParentIndexAsync(string indexName)`: `SELECT coalesce((SELECT p.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = @idx AND n.nspname = @s), @idx)` — имя индекса родителя или само имя.

Тест HOT-контракта core (`MigrationTests.Hot_contract_no_jobs_index_uses_mutable_columns`) уже проверяет `jobs_parent` / `jobs_recurring` (Task 1).

- [ ] **Step 2: Запустить**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~PlanContractTests"`
Expected: PASS. Если план отличается (Seq Scan, Sort) — не ослаблять assert, а `BLOCKED` с текстом плана (правила для исполнителя). Допустимые исправления в коде store без `BLOCKED` (каждое — в `DEVIATIONS`, результат запроса не меняется): (а) переписывание предиката под частичный индекс (тот же смысл); (б) замена соединения с `jobs` в запросе экрана, где план даёт Hash / Merge Join или Seq Scan по партициям `jobs`, на `CROSS JOIN LATERAL (SELECT … FROM {schema}.jobs j WHERE j.id = k.job_id AND <фильтры> OFFSET 0) j` (отклонение 19; в порционных шаблонах Task 8–9 и Processing уже так). Не допускаются: ослабление assert'ов, пропуск отношений по `relpages`, отключение `enable_seqscan`, уменьшение фикстуры.

- [ ] **Step 3: Commit**

```bash
git add tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(dashboard): query plan contract without seq scans"
```

---
### Task 12: Интерфейс: обзор, очереди, серверы, типы задач

**Files:**
- Create: `src/PgBolt.Dashboard/Endpoints/OverviewEndpoints.cs` (обзор, час, сутки, очереди, серверы, типы)
- Modify: `src/PgBolt.Dashboard/DashboardEndpointRouteBuilderExtensions.cs` (вызов `OverviewEndpoints.Map(group)` вместо заглушки)
- Modify: `src/PgBolt.Dashboard/Components/Pages/OverviewPage.razor` (заглушка Task 5 → полная страница)
- Create: `src/PgBolt.Dashboard/Components/Pages/QueuesPage.razor`, `ServersPage.razor`, `JobTypesPage.razor`; `Components/Parts/HourPanel.razor`, `DayPanel.razor`, `ThroughputChart.razor`, `StatsBanner.razor`, `QueueRows.razor`, `Time.razor`
- Test: `tests/PgBolt.Dashboard.Tests/Endpoints/OverviewPagesTests.cs`, `Snapshots/OverviewSnapshotTests.cs`

**Interfaces:**
- Consumes: Task 5–7.
- Produces: маршруты `GET /` (обзор), `/overview/hour`, `/overview/day`, `/queues`, `/queues/rows`, `/servers`, `/types`; компоненты `Time` (`[Parameter] DateTime? Utc` → `<time datetime="yyyy-MM-ddTHH:mm:ss.fffZ">` (формат — `ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)`: в формате `:` — культурный разделитель) с тем же текстом UTC), `ThroughputChart` (`[Parameter] IReadOnlyList<ThroughputPoint> Points`), `StatsBanner` (`[Parameter] StatsFreshness Freshness`).

- [ ] **Step 1: Тесты**

`OverviewPagesTests` (Direct, `DashboardHost` над `TestDb` с данными):
- `Overview_page_of_empty_schema_shows_banner` (Review Focus 5) — 200; текст «снимок не обновляется: ни один процесс не выполняет обслуживание (`RunMaintenance`)»; «нет данных» в панели часа (живой сервер есть, строк `stats_minutes` нет — вставить строку `servers`).
- `Overview_shows_totals_chart_and_polls_hour_panel` — `queue_stats` и `stats_minutes` за последний час; страница содержит суммы, `<svg` графика с 60 группами столбцов, элемент панели часа с `hx-get="overview/hour"`, `hx-trigger="every 5000ms, pgbolt:refresh"` (`RefreshInterval` в мс), `data-pgbolt-poll`; панель суток — `hx-get="overview/day"`, `hx-trigger="load, every 60000ms, pgbolt:refresh"`, `hx-swap="innerHTML"`; подпись «статистика приблизительная»; фрагмент `GET overview/hour` (с `HX-Request`) содержит суммы живых состояний, число живых серверов и баннер устаревшей статистики (при просроченной строке `stats`).
- `Stats_error_banner_shows_last_error_text` — строка `stats` со свежим сроком и `last_error = '<b>boom</b>'` → баннер содержит `&lt;b&gt;boom&lt;/b&gt;`.
- `Hour_and_day_fragments_render_alone` — `GET overview/hour` с `HX-Request` → без `<!DOCTYPE` и без `<title>` (частичный ответ, отклонение 23), содержит `<svg`; `overview/day` — 96 столбцов, тоже без `<title>`; `queues/rows` — без `<title>`.
- `Polling_and_load_elements_have_explicit_swap` — на страницах обзора и очередей каждый элемент с `hx-trigger` (панель часа, панель суток, `tbody queues/rows`) имеет явный `hx-swap="innerHTML"` без `show:` (унаследованный от `<body>` `show:window:top` прокручивал бы страницу наверх при каждом ответе polling) и собственный `hx-target` (`this`); ни у одного элемента страницы нет `hx-disinherit`; ссылки `jobs/enqueued?queue=…` внутри `tbody queues/rows` собственных `hx-target` / `hx-swap` не имеют (при `disableInheritance` они берут `#main` и навигационный `hx-swap` только от `<body>`, а не от `tbody`).
- `Queues_page_lists_served_queue_with_zeros_and_polls_rows` — живой сервер с `{a}`, `queue_stats` пуст, свежая строка `stats` → строка `a` с нулями и числом серверов 1; `tbody` с `hx-get="queues/rows"`, `hx-swap="innerHTML"`, `data-pgbolt-poll`; ссылки `jobs/enqueued?queue=a`.
- `Servers_page_marks_stale_and_shows_maintenance_tasks` — сервер без heartbeat 2 мин помечен «не отвечает»; задачи обслуживания с `last_error` текстом и признаком «идёт».
- `Job_types_page_lists_types_with_enqueue_links` — ссылки `types/enqueue?name=<url-encoded>`.

`OverviewSnapshotTests` (рендер компонента напрямую через `DashboardView.RenderAsync` с фиксированными моделями, `DefaultHttpContext` с `RequestServices` = `ServiceCollection().AddLogging()`): `OverviewPage` с фиксированными `OverviewData` и `DashboardContext` → `Verify(html, "html")`.

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~OverviewPagesTests|FullyQualifiedName~OverviewSnapshotTests"`
Expected: FAIL.

- [ ] **Step 3: Endpoints и компоненты**

Обработчики (`OverviewEndpoints.Map(RouteGroupBuilder g)` через `DashboardRoutes.Get`): получают `HttpContext`, `DashboardStore`, `CancellationToken` (`http.RequestAborted`), читают данные, возвращают `DashboardView.PageAsync<TPage>(http, title, parameters)`; обработчики `overview/hour`, `overview/day`, `queues/rows` — с `partial: true` (без `<title>`, отклонение 23). Параметры страниц — модели из `Views` + `DashboardContext Context`.

Разметка (существенное):
- `OverviewPage`: `HourPanel` (в нём же `StatsBanner`, блок сумм Scheduled / Enqueued / Processing / Awaiting со ссылками на вкладки и живые серверы — всё из `GetOverviewAsync`, поэтому обновляется polling'ом вместе с часом, §9) в контейнере `<section hx-get="overview/hour" hx-trigger="every {(long)RefreshInterval.TotalMilliseconds}ms, pgbolt:refresh" hx-swap="innerHTML" hx-target="this" data-pgbolt-poll>`; контейнер суток `<section hx-get="overview/day" hx-trigger="load, every 60000ms, pgbolt:refresh" hx-swap="innerHTML" hx-target="this" data-pgbolt-poll>`; подпись «статистика приблизительная (§4.1)».
- `ThroughputChart` — `<svg viewBox="0 0 {N × 10} 100" role="img">` с `<g>` на точку: `<rect class="ok">` (succeeded) и `<rect class="fail">` поверх (failed), высоты нормированы на максимум и выводятся целыми (или с `CultureInfo.InvariantCulture`: окружение может быть ru-RU, а `@h` для `double` дал бы `height="12,5"` — невалидный SVG и снимок Verify, зависящий от культуры); `<title>` в каждой `<g>` — время и числа; без JS. Все нули → подпись «нет данных».
- `HourPanel` — `StatsBanner`, суммы живых состояний из `queue_stats`, живые серверы, суммы за час + `ThroughputChart`; `DayPanel` — суммы за 24 ч + график.
- `QueuesPage` — таблица: очередь, Scheduled, Enqueued, Processing, Awaiting, серверы, `measured_at` (`Time`); `<tbody hx-get="queues/rows" hx-trigger="every {RefreshInterval в мс}ms, pgbolt:refresh" hx-swap="innerHTML" hx-target="this" data-pgbolt-poll>` (ссылки `jobs/enqueued?queue=…` внутри — навигация в `#main` от `<body>`) — `QueueRows`.
- `ServersPage` — серверы (имя, хост, очереди, ёмкость, старт, heartbeat, «не отвечает», succeeded / failed / retried за час) и задачи обслуживания (имя, `last_run_at`, `last_finished_at`, `next_run_at`, «идёт», `last_error` в `<pre>`).
- `JobTypesPage` — имя, очередь по умолчанию, попытки, приоритет, опубликовал, `last_seen_at`, ссылка «Поставить» (только при `Context.CanAct`).

- [ ] **Step 4: Запустить — проходят; принять снимок**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~OverviewPagesTests|FullyQualifiedName~OverviewSnapshotTests"`
Expected: первый прогон `OverviewSnapshotTests` создаёт `Snapshots/OverviewSnapshotTests.Overview.received.html` — просмотреть, переименовать в `.verified.html`; повторный прогон — PASS.

- [ ] **Step 5: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): overview, queues, servers and job types pages"
```

---
### Task 13: Интерфейс: задачи по состояниям, карточки задачи и batch, batches, recurring

**Files:**
- Create: `src/PgBolt.Dashboard/Endpoints/JobEndpoints.cs`, `BatchEndpoints.cs`, `RecurringEndpoints.cs`
- Create: `src/PgBolt.Dashboard/Components/Pages/JobsPage.razor`, `JobPage.razor`, `BatchesPage.razor`, `BatchPage.razor`, `RecurringListPage.razor`, `RecurringPage.razor`; `Components/Parts/JobRow.razor`, `JobCard.razor`, `RelatedJobs.razor`, `RelatedBatches.razor`, `Pager.razor`, `StateBadge.razor`
- Modify: `src/PgBolt.Dashboard/DashboardEndpointRouteBuilderExtensions.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Endpoints/JobPagesTests.cs`, `Snapshots/JobSnapshotTests.cs`

**Interfaces:**
- Consumes: Task 5–10, 12 (`Time`); `JobSeeder` из Task 7 (засев и lease для тестов на `DashboardHost`); `IJobClient.GetAsync`, `GetBatchAsync`, `IRecurringJobManager.GetAsync`.
- Produces: маршруты `GET /jobs/{list}` (`scheduled|enqueued|processing|awaiting|succeeded|failed|cancelled`; `?queue=&type=&after=`), `/jobs/{id:guid}`, `/jobs/{id:guid}/continuations?after=`, `/jobs/{id:guid}/batch-continuations?after=`, `/batches?state=&after=`, `/batches/{id:guid}` (+ `/jobs`, `/nested`, `/continuations`, `/batch-continuations` с `?after=`), `/recurring?after=`, `/recurring/item?id=&after=`; компоненты `JobRow` (корневой элемент — `<tr>`; `[Parameter] JobListRow Row`, `JobListKind Kind`, `DashboardContext Context`), `JobCard` (корневой элемент — `<article>`; `[Parameter] JobInfo Job`, `DashboardContext Context`), `Pager` (`[Parameter] string? Next`, `DateTimeOffset? ScannedTo`, `string BaseUrl` — url без `after`). Слоты для кнопок действий и чекбоксов — Task 14.

- [ ] **Step 1: Тесты**

`JobPagesTests` (Direct, `DashboardHost` над своим `TestDb`; данные — `JobSeeder` из Task 7: `var seeder = new JobSeeder(db, host.Services.GetRequiredService<PgBoltStore>())`, остальное — `IJobClient` / `IRecurringJobManager` из `host.Services`; `DashboardTestBase` тесты не наследуют):
- `Each_list_tab_renders` — `[Theory]` по семи видам (Enqueued — с `?queue=default`) → 200, активная вкладка, строки с `jobs/{id}`.
- `Enqueued_without_queue_offers_queue_choice` (отклонение 22) — `jobs/enqueued` и `jobs/enqueued?queue=` → 200 (не 400): вместо таблицы список очередей из `ListQueuesAsync` со ссылками `jobs/enqueued?queue=<Uri.EscapeDataString>` (очередь с кириллицей и пробелом — тоже); `jobs/enqueued?queue=default` — таблица задач.
- `Failed_and_cancelled_tabs_are_captioned_with_events_retention` — `EventsRetention = 7 сут` → «переходы за 7 сут»; `EventsRetention = null` (`DashboardHost.StartAsync(…, core: o => o.EventsRetention = null)`) → «переходы за всё время хранения».
- `Lease_expired_without_message_is_explained` — Failed через sweeper → «lease истёк на последней попытке».
- `Incomplete_page_offers_continue_search` — `ScanBudget` мал (`StepTimeProvider` в DI хоста), 4500 «шумовых» Scheduled и фильтр типа → «Просмотрено до» + `<time datetime=…>` + ссылка «продолжить поиск» с `after=`.
- `Filters_and_cursor_live_in_query_string` — форма фильтра `method="get"` с `queue`, `type`; ссылка следующей страницы `jobs/scheduled?queue=default&amp;type=dash.job&amp;after=…`; у формы фильтра и ссылок пейджера — `hx-push-url="true"` (§9).
- `Invalid_cursor_is_400_fragment` — `jobs/failed?after=!!` с `HX-Request` → 400, без `<!DOCTYPE`.
- `Unknown_job_is_404` — `jobs/{новый Guid}` → 404.
- `Card_shows_args_events_parent_batch_and_continuations` — задача-продолжение члена batch: ссылки `jobs/{parent}` / `batches/{batch}`; события; `RelatedJobs` с `hx-get="jobs/{id}/continuations"`, `hx-trigger="load"`, `hx-target="this"`, `hx-swap="innerHTML"` (ссылки на задачи внутри списка собственных `hx-target` / `hx-swap` не имеют и ведут в `#main`, нет `hx-disinherit`; фрагмент `GET jobs/{id}/continuations` с `HX-Request` — без `<title>`); при 51+ продолжениях — ссылка «ещё» с `after=`, `hx-get`, `hx-target="this"`, `hx-swap="outerHTML"`.
- `Card_escapes_markup_in_args_message_and_exception` (Review Focus 2) — args `{"x":"<script>alert(1)</script>"}`, fail с message `"><img src=x onerror=alert(1)>` и exception `<b>trace</b>` → в теле `&lt;script&gt;`, `&lt;img`, `&lt;b&gt;`, нет `<script>alert`.
- `Batch_card_shows_progress_members_nested_and_continuations` — ссылки на внешний batch и родителя; прогресс `succeeded / total`.
- `Batches_tab_filters_by_state` — `batches?state=Cancelled`.
- `Recurring_with_slash_and_cyrillic_id_opens` (Review Focus 3) — id `"отчёт/ежедневный ?x"` → ссылка из списка `recurring/item?id=%D0%BE…` открывается (200), история запусков пагинируется; 51+ запусков создаются SQL-засевом, а не `TriggerAsync` (при `Overlap = Skip` ручные запуски не множатся): `JobSeeder.SeedJobsAsync(55, "dash.job", "default", state: 2, queueRow: "none")` и `UPDATE {schema}.jobs SET recurring_id = @id WHERE id = ANY(@ids)` (`Db.ExecAsync`).
- `Unknown_recurring_or_batch_is_404`.

`JobSnapshotTests`: `JobRow` (Failed-строка с message) и `JobCard` (фиксированный `JobInfo` с args, двумя событиями, `ParentId`, `BatchId`) → `Verify(html, "html")`.

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~JobPagesTests|FullyQualifiedName~JobSnapshotTests"`
Expected: FAIL.

- [ ] **Step 3: Endpoints и компоненты**

- `list` в маршруте разбирается в `JobListKind` (`Enum.TryParse(ignoreCase: true)`), иначе 404; `queue` / `type` — пустые строки → `null`; `state` batches — `Enum.TryParse<BatchState>`, иначе 400.
- Карточка задачи: `IJobClient.GetAsync(new JobId(id))` → `null` — `DashboardNotFoundException`; продолжения грузятся отдельными фрагментами (`<ul hx-get="jobs/{id}/continuations" hx-trigger="load" hx-target="this" hx-swap="innerHTML">`), batch-продолжения — так же (`RelatedJobs` / `RelatedBatches` — списки `<ul>` из `<li>`; ссылка «ещё» — последний `<li>` с `hx-get` на следующую порцию, `hx-target="this"`, `hx-swap="outerHTML"`: следующий фрагмент заменяет её своими `<li>` и новой ссылкой; обычная ссылка `<a href>` под `hx-boost` заменила бы весь `#main` фрагментом); все фрагменты — `partial: true` (без `<title>`, отклонение 23); `parentCreatedAt` для фрагментов — `JobInfo.CreatedAt` (фрагмент сам вызывает `GetAsync`; `null` — 404).
- Карточка batch — `GetBatchAsync`, четыре фрагмента (`jobs`, `nested`, `continuations` (`batchParent: true`), `batch-continuations`) с теми же атрибутами загрузки (`hx-trigger="load"`, явные `hx-target` / `hx-swap`; ссылки `jobs/{id}`, `batches/{id}` внутри собственных атрибутов не имеют — навигация в `#main` от `<body>`) и `partial: true`.
- `JobCard`: заголовок (тип, `StateBadge`, очередь, приоритет, попытка / максимум), времена `Time`, сервер, unique key, расписание (`recurring/item?id=…` через `Uri.EscapeDataString`), родитель (`jobs/{ParentId}` или `batches/{ParentBatchId}`), batch, условие продолжения; `<pre>` с `JsonSerializer.Serialize(Args, indented)`; события (вид, время, сервер, попытка, `<pre>` message / exception).
- Вкладки (`JobsPage`): Scheduled / Enqueued / Processing / Awaiting / Succeeded / Failed / Cancelled; Enqueued без `queue` (в том числе пустой) — страница выбора очереди (`ListQueuesAsync`, ссылки с `hx-push-url="true"`), `ListJobsAsync(Enqueued, null, …)` не вызывается (он бросает `DashboardBadRequestException`, Task 8; отклонение 22); подписи Failed / Cancelled — «переходы за {N сут | всё время хранения}» (`Context.EventsRetention`); фильтр (`<form method="get" hx-push-url="true">`: очередь, тип, кнопка «Применить»), кнопка «обновить» (ссылка на тот же URL без `after`); таблица `JobRow`; `Pager` (ссылки с `hx-push-url="true"`): `Next` → «следующая страница»; `ScannedTo` → «Просмотрено до <Time> — продолжить поиск»; списки сами не обновляются (§9).
- `JobRow` для kind 3 без message — «lease истёк на последней попытке».
- Корневые элементы фиксируются здесь, а не в Task 14 (на них опираются `hx-target="closest tr"` / `"closest article"` кнопок действий; снимки `JobSnapshotTests` принимаются уже с ними): `JobRow` и строка расписания в `RecurringListPage` — `<tr>`; `JobCard`, `RecurringPage` (карточка расписания) и карточка batch (`BatchPage`) — `<article>`.

- [ ] **Step 4: Запустить — проходят; принять снимки**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~JobPagesTests|FullyQualifiedName~JobSnapshotTests"`
Expected: после просмотра и переименования `*.received.html` → `*.verified.html` — PASS.

- [ ] **Step 5: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): job lists, job and batch cards, batches and recurring pages"
```

---
### Task 14: Действия (§7)

**Files:**
- Create: `src/PgBolt.Dashboard/Endpoints/ActionEndpoints.cs`, `Endpoints/DashboardNotify.cs`
- Create: `src/PgBolt.Dashboard/Components/Parts/JobActions.razor`, `BatchActions.razor`, `RecurringActions.razor`, `ActionSummary.razor`
- Modify: `Components/Parts/JobRow.razor`, `JobCard.razor`, `Pages/JobsPage.razor` (чекбоксы и форма действий над выбранными), `Pages/BatchPage.razor`, `RecurringListPage.razor`, `RecurringPage.razor`; `DashboardEndpointRouteBuilderExtensions.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Endpoints/ActionsTests.cs`, `Endpoints/AntiforgeryHostTests.cs`, `Endpoints/KestrelHeaderTests.cs`

**Interfaces:**
- Consumes: Task 6 (`CanAct`, POST-проверки фильтра, `TestAuthentication`), Task 7 (`JobSeeder`, `ResolveAll`), Task 13; `IJobClient.CancelAsync` / `TriggerAsync` / `RequeueAsync` / `CancelBatchAsync`, `IRecurringJobManager.PauseAsync` / `ResumeAsync` / `TriggerAsync` / `RemoveAsync` / `GetAsync`.
- Produces: `POST /actions/jobs/{id:guid}/{action}?view=row|card` (`cancel|trigger|requeue`), `POST /actions/jobs/{action}` (форма `ids`, до 100), `POST /actions/batches/{id:guid}/cancel`, `POST /actions/recurring/{action}?id=&view=row|card` (`pause|resume|trigger|delete`; без `view` — `row`, как у `actions/jobs/{id}/{action}`, где без `view` тоже `row`); `DashboardNotify.Header(string message)` — значение `HX-Trigger`.

- [ ] **Step 1: Тесты**

`ActionsTests` (Direct, `DashboardHost` над своим `TestDb`; `DashboardTestBase` не наследуют — данные засеваются `JobSeeder` из Task 7: `new JobSeeder(db, host.Services.GetRequiredService<PgBoltStore>())`, а отмена и учёт batch — `ResolveAll.RunAsync(host.Services.GetRequiredService<PgBoltStore>(), ct)`):
- `Cancel_trigger_requeue_single_job` — Scheduled: `trigger` → 200, задача Enqueued, фрагмент строки с новым состоянием без `<title>` (частичный ответ, отклонение 23), `HX-Trigger` = JSON `{"pgbolt:notify":"…"}` (разобрать `JsonDocument`); `cancel` → Cancelled, причина события — `dashboard: anonymous` (`GetAsync` → событие kind 4 с `Message == "dashboard: anonymous"`); Failed → `requeue` → Enqueued.
- `Card_view_returns_card` — `?view=card` → фрагмент карточки.
- `Race_is_reported_not_failed` — `cancel` уже Succeeded-задачи → 200, уведомление «задача уже завершена»; `requeue` Enqueued-задачи → «неприменимо: задача не завершена».
- `Unknown_job_action_is_404` — `cancel` несуществующей → 404 (`CancelResult.NotFound`).
- `Bulk_actions_report_done_and_not_applicable` — три Scheduled + одна Succeeded, `trigger` над четырьмя → «выполнено 3, неприменимо 1» и перечень с id неприменимой; >100 id → 400; не-Guid в `ids` → 400; повторяющиеся id выполняются один раз.
- `Batch_cancel` (отклонение 24) — batch с членами: первый POST → `CancellationRequested` у `GetBatchAsync`, уведомление «отмена запрошена»; повторный POST → «отмена уже запрошена» (`CancelBatchAsync` при повторе возвращает тот же `CancellationRequested`, поэтому признак берётся из `GetBatchAsync().CancellationRequested` до вызова); batch в терминальном состоянии → «batch уже завершён» (отмена и учёт — `ResolveAll.RunAsync(PgBoltStore из host.Services, ct)` из Task 7, собственной копии цикла в тесте нет); несуществующий → 404.
- `Recurring_actions_accept_any_id` (Review Focus 3) — id `"a/b ?c"` (`Uri.EscapeDataString` в query) → `pause` / `resume` / `trigger` (ссылка на новую задачу в уведомлении) / `delete`; delete code-first строки (`owner` выставлен прямым `UPDATE`) → кнопки «удалить» нет, POST → 200 с сообщением «расписание объявлено в коде (owner …)»; trigger расписания, чей `type` удалён из `job_types` прямым `DELETE`, → 200 и уведомление «не опубликован».
- `Bulk_buttons_have_own_target_and_do_not_capture_links` — список с `CanAct`: форма над выбранными — только контейнер чекбоксов `ids` (без `hx-post` / `hx-target` / `hx-swap`); у каждой из трёх кнопок (`cancel` / `trigger` / `requeue`) собственные `hx-post="actions/jobs/{action}"`, `hx-target="#bulk-result"`, `hx-swap="innerHTML"`, `type="button"`; ссылки `jobs/{id}` внутри формы собственных `hx-target` / `hx-swap` не имеют; нет `hx-disinherit`. POST кнопки несёт чекбоксы `ids` (htmx для не-GET включает значения ближайшей формы).
- `Buttons_render_only_for_applicable_states` — Scheduled-строка: `trigger`, `cancel`, без `requeue`; Failed: только `requeue`; Succeeded: нет кнопок; Processing: `cancel`.
- `Read_only_hides_buttons_and_rejects_post` — `ReadOnly = true` → кнопок и чекбоксов нет, POST → 403.
- `Actions_policy_denial_hides_buttons_and_rejects_post` — `ActionsPolicy = "Ops"`, `builder: b => TestAuthentication.Add(b.Services, o => o.AddPolicy("Ops", p => p.RequireRole("Ops")))` (Task 6); `X-Test-User: alice` без `X-Test-Roles` → кнопок нет, POST → 403; с `X-Test-Roles: Ops` — 200 (заголовки — через `host.Client.DefaultRequestHeaders`).
- `Csrf_checks_reject_foreign_posts` — без `HX-Request` → 403; `Sec-Fetch-Site: cross-site` → 403; без `Sec-Fetch-Site` с `Origin: http://evil` → 403; с `Origin: http://localhost` (хост TestServer — `localhost`) → 200.
- `Each_action_is_logged` — Information с пользователем, действием, объектом и результатом.

`AntiforgeryHostTests` (Direct):
- `Form_post_works_in_host_with_razor_components_and_antiforgery` — `builder.Services.AddRazorComponents(); builder.Services.AddAntiforgery();` и `app.MapRazorComponents<HostApp>()` (`HostApp` — класс `ComponentBase` с атрибутом `[Route("/")]` в тестах, `BuildRenderTree` с текстом `host`: `MapRazorComponents` строит маршруты только для компонентов с `[Route]`, без него `GET /` вернул бы 404) → POST действия над выбранными (form-urlencoded) → 200; `GET /` хоста → 200 (`host`).
- `Form_post_works_in_host_without_antiforgery_services` — обычный хост → 200.
- `Host_razor_pages_do_not_conflict` — `AddRazorPages()` + `MapRazorPages()` → dashboard GET 200, POST 200.

`KestrelHeaderTests.Cyrillic_notification_passes_kestrel` — отдельный `WebApplication` прямо в тесте, без `DashboardHost` (он всегда вызывает `UseTestServer()` и `GetTestClient()`, Kestrel через его параметр `builder` не подключить): `WebApplication.CreateSlimBuilder()`, `AddPgBolt` / `AddPgBoltDashboard` как в `DashboardHost`, middleware loopback как там же, `UseKestrel()` + `UseUrls("http://127.0.0.1:0")`, адрес из `app.Urls`, `HttpClient` с заголовками POST как у `DashboardHost.PostAsync` (`HX-Request: true`, `Sec-Fetch-Site: same-origin`); POST `trigger` → 200, `HX-Trigger` содержит `\u` и после разбора JSON — кириллицу.

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~ActionsTests|FullyQualifiedName~AntiforgeryHostTests|FullyQualifiedName~KestrelHeaderTests"`
Expected: FAIL.

- [ ] **Step 3: Endpoints**

- Одиночное действие: `cancel` — `CancelAsync(id, $"dashboard: {http.User.Identity?.Name ?? "anonymous"}")` (`NotFound` → 404; `AlreadyFinished` → «задача уже завершена»; `Cancelled` / `CancellationRequested` → «отменена» / «отмена запрошена»); `trigger` — `TriggerAsync` (`false` → проверить `GetAsync`: `null` → 404, иначе «неприменимо: задача не Scheduled»); `requeue` — `RequeueAsync` (`NotFound` → 404, `NotTerminal` → «неприменимо: задача не завершена», `Conflict` → «конфликт unique_key: есть активная задача с тем же ключом», `Requeued` → «поставлена заново»). Ответ — фрагмент строки (`JobRow` в виде «после действия»: состояние из `GetAsync`) или карточки; `http.Response.Headers["HX-Trigger"] = DashboardNotify.Header(text)`. Все ответы действий (одиночных, над выбранными, batch, recurring, «Поставить») — `partial: true` (без `<title>`, отклонение 23).
- Над выбранными: `IFormCollection form`; `ids` — `form["ids"]`, каждый `Guid.TryParse` (иначе 400), `Distinct`, больше 100 → 400; последовательно; итог `ActionSummary` (выполнено N, неприменимо M, перечень).
- `DashboardNotify.Header(message)` = `JsonSerializer.Serialize(new Dictionary<string, string> { ["pgbolt:notify"] = message })` (кодировщик по умолчанию — не-ASCII как `\uXXXX`, §7).
- Recurring: `id` из query (пусто → 400); `GetAsync(id)` → `null` — 404; `delete` при `Owner != null` — сообщение без вызова API; `trigger` → `UnknownJobException` (типа нет в `job_types`) — 200 с уведомлением «тип задачи {name} не опубликован» и строкой / карточкой из `GetAsync`, как при неприменимом действии (отклонение 15); ответ — фрагмент строки / карточки расписания, состояние берётся из `GetAsync` после действия (pause / resume / trigger). `delete` при `RemoveAsync == true` — строки уже нет и повторный `GetAsync` вернёт `null`, поэтому ответ — заглушка «расписание удалено» (`<tr>` для `view=row`, `<article>` для `view=card`, чтобы `hx-target="closest tr"` / `"closest article"` с `outerHTML` её заменил) с уведомлением; `false` (удалено параллельно) — 404.
- Batch cancel: `GetBatchAsync(new BatchId(id))` (`null` → 404) → `was = info.CancellationRequested`; `CancelBatchAsync`: `NotFound` → 404, `AlreadyFinished` → «batch уже завершён», `CancellationRequested` → `was ? «отмена уже запрошена» : «отмена запрошена»` (отклонение 24).
- Лог: `logger.LogInformation("PgBolt dashboard: {User} {Action} {Target}: {Result}.", …)`.
- Кнопки (`JobActions`): рендерятся только при `Context.CanAct`; `requeue` — Failed / Cancelled; `trigger` — Scheduled; `cancel` — Scheduled / Enqueued / Processing / Awaiting; `<button hx-post="actions/jobs/{id}/{action}?view=row" hx-target="closest tr" hx-swap="outerHTML">`; в карточке и на странице расписания — `?view=card`, `hx-target="closest article"`, `hx-swap="outerHTML"` (корни `<tr>` / `<article>` заданы в Task 13: `JobRow` и строка расписания — `<tr>`; `JobCard`, `RecurringPage`, карточка batch — `<article>`). Чекбоксы `name="ids"` и действия над выбранными — только при `CanAct`: `<form>` без `hx-post` / `hx-target` / `hx-swap` — контейнер чекбоксов (оборачивает таблицу со ссылками `jobs/{id}`); в ней три кнопки `<button type="button" hx-post="actions/jobs/{cancel|trigger|requeue}" hx-target="#bulk-result" hx-swap="innerHTML">` — цель и способ свапа у каждой собственные (при `disableInheritance` кнопка ничего не унаследует от формы; `type="button"` — чтобы не было нативного submit), чекбоксы `ids` уходят с POST кнопки как значения ближайшей формы (`hx-include` не нужен); `#bulk-result` — `<div>` вне формы. Batch: «отменить» при `Active` / `Awaiting` и без `CancellationRequested`. Recurring: pause / resume по `Paused`, trigger, delete при `Owner == null`.

- [ ] **Step 4: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Dashboard.Tests`
Expected: PASS (снимки Task 13 не меняются: они рендерятся с `CanAct = false`, а корни `<tr>` / `<article>` заданы ещё в Task 13).

- [ ] **Step 5: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): job, batch and recurring actions with CSRF and policy checks"
```

---
### Task 15: Запуск задачи по имени (§8)

**Files:**
- Create: `src/PgBolt.Dashboard/Forms/SchemaForm.cs`, `ArgsValidator.cs`, `FormArgs.cs`, `FormModels.cs`
- Create: `src/PgBolt.Dashboard/Endpoints/EnqueueEndpoints.cs`
- Create: `src/PgBolt.Dashboard/Components/Pages/EnqueuePage.razor`; `Components/Parts/SchemaFields.razor`, `EnqueueResultView.razor`
- Modify: `src/PgBolt.Dashboard/DashboardEndpointRouteBuilderExtensions.cs`
- Test: `tests/PgBolt.Dashboard.Tests/Unit/SchemaFormTests.cs`, `Unit/ArgsValidatorTests.cs`, `Endpoints/EnqueueTests.cs`, `Snapshots/SchemaFormSnapshotTests.cs`; Modify: `Endpoints/AntiforgeryHostTests.cs` (из Task 14)

**Interfaces:**
- Consumes: Task 6, 7 (`GetJobTypeAsync`), 12; `IJobClient.EnqueueAsync(string, JsonElement, EnqueueOptions?)`.
- Produces: `GET /types/enqueue?name=&mode=form|json`, `POST /actions/types/enqueue?name=`, `POST /actions/types/json?name=` (форма → JSON для режима JSON); `SchemaForm`, `ArgsValidator`, `FormArgs`, `FormField`, `FieldError` — из «Контракта».

- [ ] **Step 1: Тесты**

Схемы в unit-тестах — `JsonSchemaExporter.GetJsonSchemaAsNode(options, typeof(T)).ToJsonString()` для тестовых типов: `JsonSerializerOptions.Default` и `new JsonSerializerOptions(JsonSerializerDefaults.Web) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }` (без `TypeInfoResolver` `JsonSchemaExporter` бросает исключение в `ValidateOptions`); enum — `[property: JsonConverter(typeof(JsonStringEnumConverter<Color>))] Color Tint` на параметре записи (`Converters` у `Default` не добавить). Ожидания ниже сверены с выводом exporter на SDK 10: он учитывает аннотации nullability (`string` → `{"type":"string"}`, `string?` → `["string","null"]`, вложенный `Address` → `{"type":"object",…}`, `Color` → `{"enum":["Red","Green"]}`), корень — `["object","null"]`, а `required` перечисляет все параметры конструктора, в том числе nullable.

`SchemaFormTests`:
- `Supported_subset_builds_fields` — запись `record Order(string Name, int Count, double Price, bool Urgent, DateTimeOffset Due, Guid Ref, [property: JsonConverter(typeof(JsonStringEnumConverter<Color>))] Color Tint, Address Ship, int? Limit)` (`Address(string City, string? Zip)`), схема `JsonSerializerOptions.Default` → поля `Name` (String, не Nullable), `Count` (Integer), `Price` (Number), `Urgent` (Boolean), `Due` (DateTime), `Ref` (Uuid), `Tint` (Enum, значения `Red`, `Green`), `Ship` (Object, не Nullable; дети `City` — String, не Nullable, и `Zip` — String, Nullable), `Limit` (Integer, Nullable `["integer","null"]`); `Required` — у **всех** полей и детей, включая nullable `Zip` и `Limit`.
- `Web_number_handling_maps_to_numbers` — Web-опции (с `TypeInfoResolver`; имена свойств — camelCase): `count` с `["string","integer"]` + `pattern` → Integer, `price` с `["string","number"]` + `pattern` → Number, `limit` с `["string","integer","null"]` + `pattern` → Integer, Nullable; `name` — String.
- `Unsupported_schemas_fall_back_to_json` — `[Theory]` по типам с `int[]`, `List<Address>`, `Dictionary<string,int>`, `JsonElement` (схема `true`), рекурсивный `Node(Node? Next)` (`$ref`), `anyOf` (вручную) → `TryBuild == null`.
- `Annotations_are_ignored` — схема с `description` / `title` / `default` → поддерживается.

`ArgsValidatorTests`:
- обязательное поле отсутствует → ошибка у `Name`; неверный тип (`"Count":"x"`) → ошибка у `Count`; значение вне `enum` → ошибка; `Due: "2026-13-01"` → ошибка формата; `Ref: "nope"` → ошибка формата; `null` в не-nullable → ошибка, а `null` в обязательном nullable-поле (`Limit`) — не ошибка; вложенное поле → путь `Ship.City`; корректный документ → пусто.

`EnqueueTests` (Direct, тип опубликован `PublishTypeAsync` со схемой `Order`):
- `Form_page_renders_fields_and_options` — `types/enqueue?name=dash.order` → поля с `name="args.Name"`, `args.Ship.City` в `<fieldset>`, `<select name="args.Tint">`, опции очереди (по умолчанию `default_queue`), приоритета, задержки (секунды) / времени запуска, `unique_key`, `max_attempts`; переключатель «Форма / JSON».
- `Unsupported_schema_opens_json_mode_only` — тип со схемой массива → только `<textarea name="json">`.
- `Enqueue_from_form_creates_job_and_links_card` — POST form-urlencoded → 200, ссылка `jobs/{id}`, `GetAsync(id).Args` равны введённым (числа — числами, `Urgent` из чекбокса — `true`/`false`).
- `Validation_errors_are_shown_at_fields` — пустое обязательное `Name`, `Count = "abc"` → 400, ошибки рядом с полями, задача не создана.
- `Json_mode_validates_by_subset` — `json = {"Count":"x"}` → 400 с ошибкой `Count`; невалидный JSON → 400 «некорректный JSON».
- `Json_prefill_comes_from_form_values` — POST `actions/types/json` с частично заполненной формой → `<textarea>` с JSON этих значений (невалидные — строками).
- `Unique_key_conflict_links_existing_job` — активная задача с ключом `k`, повтор с `unique_key = k` → «уже есть активная задача» и ссылка на её id.
- `Finished_unique_key_race_has_no_link` — хост регистрирует после `AddPgBolt` декоратор `IJobClient` (делегирует настоящему, но `EnqueueAsync(string, JsonElement, …)` возвращает `new EnqueueResult(null, false)`) → «задача с этим ключом только что завершилась», ссылок `jobs/` нет.
- `Vanished_type_reports_unknown_job` — тип удалён из `job_types` после открытия формы (кеш `JobTypesCache` с TTL: `DashboardHost.StartAsync(…, core: o => o.JobTypesCacheTtl = TimeSpan.FromMilliseconds(1))`) → сообщение «тип задачи … не опубликован».
- `Enqueue_form_and_json_button_have_own_target_and_swap` — `<form hx-post="actions/types/enqueue…">` имеет `hx-target="#enqueue-result"` и `hx-swap="innerHTML"`; кнопка «JSON» — `hx-post="actions/types/json…"`, `hx-target="#enqueue-body"`, `hx-swap="innerHTML"`, `hx-include="closest form"`; ссылка `jobs/{id}` в ответе успешного POST собственных `hx-target` / `hx-swap` не имеет; нигде нет `hx-disinherit`.
- `Enqueue_responses_contain_exactly_one_form` — страница формы, ответ 400 (ошибки у полей) и ответ 200 после успеха: ровно один `<form` и один `id="enqueue-body"`.
- `Reserved_unique_key_prefix_is_field_error` — `unique_key = "pgbolt:x"` → 400 с ошибкой у поля.
- `Delay_and_run_at_are_mutually_exclusive` — оба заданы → 400.
- `Enqueue_form_opens_for_type_name_with_special_characters` (Review Focus 3) — тип `"отчёт/ежедневный"`.
- `Read_only_has_no_enqueue_form` — `ReadOnly` → страница типа без формы, POST → 403.

`SchemaFormSnapshotTests`: `SchemaFields` для `Order` → `Verify(html, "html")`.

`AntiforgeryHostTests` (дополнить, §12 «Endpoints»: обе формы): в `Form_post_works_in_host_with_razor_components_and_antiforgery` и `Form_post_works_in_host_without_antiforgery_services` — ещё `POST actions/types/enqueue?name=…` (form-urlencoded, тип опубликован, валидные args) → 200, задача создана (`GetAsync` по id из ответа).

- [ ] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Dashboard.Tests --filter "FullyQualifiedName~SchemaForm|FullyQualifiedName~ArgsValidator|FullyQualifiedName~EnqueueTests|FullyQualifiedName~AntiforgeryHostTests"`
Expected: FAIL.

- [ ] **Step 3: Форма из схемы**

`SchemaForm.TryBuild` разбирает `JsonNode` (§8 + отклонение 11): корень — объект (`type` `"object"` или `["object","null"]`) с `properties`; разрешённые ключевые слова узла — `type`, `properties`, `required`, `enum`, `format`, `pattern` (только при integer/number из строкового массива), аннотации `title`, `description`, `default`, `$comment`; любое другое ключевое слово или вид `type` — `null` (только JSON). Вид поля: `enum` (в том числе без `type`) → Enum (значения — строки и `null`; `null` в списке → Nullable); `string` + `format: date-time` → DateTime, `+ uuid` → Uuid, иначе String; `integer` → Integer, `number` → Number (в том числе массивы `["string","integer"]` / `["string","number"]` с `pattern` и необязательным `"null"`); `boolean` → Boolean; `object` с `properties` → Object (рекурсивно, путь `Parent.Child`); `"null"` в массиве `type` → Nullable; `Required` — по массиву `required` (Nullable-поле остаётся обязательным: пустое значение — `null`, не ошибка).

`ArgsValidator.Validate(fields, args)` — по тем же видам: обязательность, тип JSON-значения (Integer — `JsonValueKind.Number` и `TryGetInt64`), принадлежность `enum`, `DateTimeOffset.TryParse(…, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)`, `Guid.TryParse`; лишние свойства не проверяются (окончательная проверка — при выполнении, `JobArgumentsException`).

`FormArgs.Read(fields, form, bestEffort)` — поля формы `args.{Path}`: пустая строка → Nullable-поле — `null` (в том числе обязательное: `required` + nullable даёт `null`, а не ошибку), иначе не обязательное — свойство не пишется, иначе обязательное — ошибка; Integer — `long.TryParse(InvariantCulture)`; Number — `decimal.TryParse(InvariantCulture)`; Boolean — чекбокс (`"true"` или отсутствие → `false`); Enum — значение `<select>` (пустое → `null`); DateTime / Uuid — строки с проверкой; Object — рекурсивно. `bestEffort` (для префилла JSON) — неразобранные значения пишутся строками, ошибок нет.

- [ ] **Step 4: Endpoints и компоненты**

- `GET types/enqueue?name=` — `GetJobTypeAsync(name)` → `null` — 404; `mode=json` или `TryBuild == null` → режим JSON; форма — `EnqueuePage` с `FormField`-деревом (`SchemaFields` рекурсивно: `<fieldset>` для объектов, `<input type=number step=1>` для Integer, `step=any` для Number, `<input type=checkbox value=true>`, `<select>` для Enum с пустым вариантом при Nullable, `<input type=text placeholder="2026-10-05T12:00:00Z">` для DateTime, текст для Uuid / String; `required` по полю). Вёрстка: `<div id="enqueue-result">` **оборачивает форму** (а не стоит после неё: иначе ответ 400, который `responseHandling` свапает, вставил бы вторую копию формы и второй `#enqueue-body`); внутри формы — `<div id="enqueue-body">` с полями или `<textarea name="json">`; ответ POST заменяет всё содержимое `#enqueue-result`: 400 — форма с ошибками у полей, 200 — `EnqueueResultView` и под ним форма заново, всегда одна копия формы. Форма — `hx-post="actions/types/enqueue?name={escaped}" hx-target="#enqueue-result" hx-swap="innerHTML"` (ссылка на карточку из `EnqueueResultView` собственных `hx-target` / `hx-swap` не имеет — навигация в `#main` от `<body>`); кнопка «JSON» — `hx-post="actions/types/json?name=…" hx-include="closest form" hx-target="#enqueue-body" hx-swap="innerHTML"`; ответы обоих POST — `partial: true` (без `<title>`, отклонение 23). Форма рендерится только при `Context.CanAct`.
- `POST actions/types/enqueue?name=` — `IFormCollection`; args — из `json` (режим JSON: `JsonDocument.Parse`, ошибка → 400 «некорректный JSON»; при поддерживаемой схеме — `ArgsValidator`) или из полей (`FormArgs.Read(bestEffort: false)` + `ArgsValidator`); опции — `queue` (пусто → `null`), `priority` (`short`), `delay` (секунды, `int ≥ 0`) / `runAt` (`DateTimeOffset`, взаимоисключимы), `uniqueKey`, `maxAttempts` (`≥ 1`); ошибки → 400 с формой и ошибками у полей; вызов `EnqueueAsync(name, args, options)`: `Created` → `EnqueueResultView` со ссылкой `jobs/{id}`; `!Created && Id != null` → «уже есть активная задача» + ссылка; `Id == null` → «задача с этим ключом только что завершилась» без ссылки; `UnknownJobException` → 200 с сообщением; `ArgumentException` → 400 с ошибкой у поля `uniqueKey` / опций; лог Information как у действий.
- `POST actions/types/json?name=` — `FormArgs.Read(bestEffort: true)` → режим JSON с `<textarea name="json">` (`JsonSerializer.Serialize(args, indented)`).

- [ ] **Step 5: Запустить — проходят; принять снимок**

Run: `dotnet test tests/PgBolt.Dashboard.Tests`
Expected: после принятия `SchemaFormSnapshotTests.*.verified.html` — PASS.

- [ ] **Step 6: Commit**

```bash
git add src/PgBolt.Dashboard tests/PgBolt.Dashboard.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(dashboard): enqueue by name with schema-driven form and validation"
```

---
### Task 16: Интеграция с сервером и E2E

**Files:**
- Modify: `tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj` (`FrameworkReference Microsoft.AspNetCore.App`, `PackageReference Microsoft.AspNetCore.TestHost`, `ProjectReference ..\..\..\src\PgBolt.Dashboard\PgBolt.Dashboard.csproj`)
- Test: `tests/PgBolt.Server.Tests/Integration/StatsTests.cs`, `tests/E2E/PgBolt.E2E.Tests/DashboardEndToEndTests.cs`

**Interfaces:**
- Consumes: Task 1–15; `ServerIntegrationTestBase` (`StartHostAsync`, `WaitForStateAsync`, `WaitUntilAsync`, `EnqueueAsync`), `TestJobs` (`SuccessArgs`, `FailArgs`, `FlakyArgs`), E2E — `PingArgs`, `PgBoltJobRegistry.Register` трёх сборок.

- [ ] **Step 1: Интеграция сервера**

`StatsTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase` (`_Direct`, `_Bouncer`):
- `Confirmed_outcomes_reach_stats_minutes` — `Processing = p => { p.StatsFlushInterval = TimeSpan.FromMilliseconds(200); }`; 3 `SuccessArgs`, 1 `FailArgs` с `MaxAttempts = 1`, 1 `FlakyArgs(1)` с `ImmediateRetry` → дождаться терминальных состояний и (`WaitUntilAsync`) суммы в `stats_minutes` по `server_id = host.ServerId`: `succeeded = 4` (3 + flaky), `failed = 1`, `retried = 1`, очередь — `TestJobs.Queue`.
- `Final_flush_on_stop_records_last_outcomes` — `StatsFlushInterval = 1 ч`; 2 `SuccessArgs` до успеха; `StopAsync()` хоста → `stats_minutes` содержит `succeeded = 2`.
- `Stats_task_snapshots_live_states` — `StatsSnapshotInterval = 1 с`; задача с `Delay = 1 ч` → `queue_stats` строки очереди с `scheduled = 1` (ожидание до 10 с); `maintenance_tasks` строка `stats`, `last_error IS NULL`.

- [ ] **Step 2: E2E**

`DashboardEndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime` (`_Direct`, `_Bouncer`; схема — `TestDb.CreateAsync(pg, mode, migrate: false)` как `EndToEndTests`):
- `Dashboard_pages_and_action_work_over_e2e_schema` — `WebApplication.CreateSlimBuilder()`, `UseTestServer()`, `AddPgBolt(o => { o.ConnectionString = pg.ConnectionString(mode); o.Schema = _db.Schema; o.AddJobs(...три Register...); })`, `AddPgBoltDashboard(o => o.AllowAnonymousRemote = true)`; `app.MapPgBoltDashboard()`; `StartAsync` (инициализатор мигрирует схему); `PublishJobTypesAsync` реестра; задачи: `PingArgs` Enqueued, `PingArgs` с `Delay = 1 ч`, одна обработана как в `EndToEndTests` (dequeue, invoke, ack). GET: `/pgbolt/`, `overview/hour`, `overview/day`, `queues`, `servers`, `types`, `types/enqueue?name=e2e.ping`, все семь вкладок (`enqueued?queue=e2e`), карточка каждой задачи, `batches`, `recurring` → 200. Действие: POST `actions/jobs/{scheduled}/trigger?view=row` с `HX-Request: true`, `Sec-Fetch-Site: same-origin` → 200, задача Enqueued (`IJobClient.GetAsync`). Режим Bouncer — `SET LOCAL` и транзакция на statement через PgBouncer transaction mode, без prepared statements.

- [ ] **Step 3: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Integration.StatsTests"` и `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~DashboardEndToEndTests"`
Expected: PASS (Direct и Bouncer).

- [ ] **Step 4: Commit**

```bash
git add tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(dashboard): server stats integration and end-to-end dashboard smoke"
```

---
### Task 17: Документация и полный прогон

**Files:**
- Modify: `CHANGELOG.md`, `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md`, `docs/superpowers/specs/2026-10-03-pgbolt-dashboard-design.md` (строка статуса), `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md`, `2026-09-28-pgbolt-worker-design.md`, `2026-09-29-pgbolt-storage-maintenance-design.md`, `2026-09-30-pgbolt-scheduler-design.md`, `2026-10-02-pgbolt-workflows-design.md` (§11 спеки части 6)

- [ ] **Step 1: CHANGELOG**

`## Unreleased`, `### Breaking changes`:

```markdown
- Dashboard (часть 6): в начальную миграцию `V001` добавлены `stats_minutes`, `queue_stats` и индексы `queue_awaiting_list`,
  `job_events_failed`, `job_events_cancelled`, `jobs_parent`, `jobs_recurring`, `batches_parent`; схемы сборок части 5 не
  обновляются — старт падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- `PgBoltStore.AckAsync` возвращает `IReadOnlyList<Guid>` (id подтверждённых задач) вместо числа.
- `PgBoltProcessingOptions.RunMaintenance = true` запускает и задачу `stats` (снимок `queue_stats` раз в `StatsSnapshotInterval`,
  retention `stats_minutes`).
```

`### Added`:

```markdown
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
```

- [ ] **Step 2: Roadmap и статус спеки**

Roadmap, часть 6: ``Спека: `2026-10-03-pgbolt-dashboard-design.md`. **Статус: спека.**`` → ``Спека: `2026-10-03-pgbolt-dashboard-design.md`, план: `docs/superpowers/plans/2026-10-05-pgbolt-dashboard.md`. **Статус: реализовано.**``.

Спека части 6, строка статуса: `Статус: черновик, дизайн согласован в брейншторме` → `Статус: одобрено ревью (4 итерации), реализовано (план docs/superpowers/plans/2026-10-05-pgbolt-dashboard.md)`.

- [ ] **Step 3: Изменения других частей (§11 спеки части 6)**

В конец каждой спеки — раздел (дословно):

core:

```markdown
## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§3, §11).
- §4: таблицы `stats_minutes`, `queue_stats`; индексы `queue_awaiting_list`, `job_events_failed`, `job_events_cancelled`,
  `jobs_parent`, `jobs_recurring` (по неизменяемым колонкам — контракт HOT), `batches_parent`. Детектор схемы прошлой сборки
  требует `stats_minutes` и `queue_stats`.
- §5.3: `AckAsync` возвращает подтверждённые `job_id`. Инвариант для всех путей финализации: `jobs.finished_at` и событие
  терминального перехода пишутся одним statement'ом с одним `now()`; requeue обнуляет `finished_at`.
```

worker:

```markdown
## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§4.1).
- §6.1: `CompletionSink` учитывает подтверждённые исходы (ack по возвращённым id, retry / fail по `FailOutcome`) в процессном
  `OutcomeStats`; цикл сброса раз в `StatsFlushInterval` (15 с) в любом процессе с сервером; финальный сброс после остановки
  серверов входит в `ShutdownReserve`.
```

часть 2:

```markdown
## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§4.2, §6).
- §6: задача `stats` в `maintenance_tasks` — отдельный цикл с интервалом `StatsSnapshotInterval` (захват 0,8 × интервала),
  снимок `queue_stats` и retention `stats_minutes` (`StatsRetention`); ошибка в лог — не чаще раза в минуту.
- §11: dashboard читает `maintenance_tasks` и `jobs` только в READ COMMITTED, statement на транзакцию; ручной запуск задач
  обслуживания отложен.
```

часть 4:

```markdown
## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§3.2, §6.1).
- §9: история запусков расписания — по индексу `jobs_recurring (recurring_id, id)`.
```

часть 5:

```markdown
## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§6.1, §6.2).
- §10: Awaiting — по `queue_awaiting_list`, Scheduled — с `run_at < 'infinity'`; дерево и прогресс batch с `greatest(0, …)`;
  продолжения — коды `continuation` 1–4 по `jobs_parent`, члены batch-продолжения (код 5) продолжениями не считаются;
  batch-продолжения — по `batches_parent`.
```

- [ ] **Step 4: Полный прогон**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: все тесты проходят. Падение — остановиться и доложить с выводом.

- [ ] **Step 5: Commit**

```bash
git add CHANGELOG.md docs/superpowers/specs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "docs: dashboard (part 6) changelog and status"
```
