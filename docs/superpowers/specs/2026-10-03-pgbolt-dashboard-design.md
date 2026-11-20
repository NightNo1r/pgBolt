# PgBolt — часть 6: Dashboard

Статус: одобрено ревью (4 итерации), реализовано (план docs/superpowers/plans/2026-10-05-pgbolt-dashboard.md) · Дата: 2026-10-03 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»), `2026-09-28-pgbolt-worker-design.md` (далее «worker»), `2026-09-29-pgbolt-storage-maintenance-design.md` (далее «часть 2»), `2026-09-30-pgbolt-scheduler-design.md` (далее «часть 4»), `2026-10-02-pgbolt-workflows-design.md` (далее «часть 5»)

## 1. Цель и рамки

Встраиваемый в ASP.NET Core dashboard в модели Hangfire: просмотр очередей, задач, расписаний, batches и серверов, ручные действия и запуск задачи по имени. Фронт C#-native: страницы рендерятся на сервере Razor-компонентами, интерактивность — htmx, без Blazor-runtime, SignalR и сборки npm. Dashboard не добавляет записей на горячий путь и не нарушает контракт HOT `jobs` (core §4.2).

**Входит:**
- пакет `PgBolt.Dashboard`: `AddPgBoltDashboard` + `MapPgBoltDashboard`, авторизация через политики, read-only режим;
- экраны: обзор (счётчики, график throughput), очереди, задачи по состояниям, карточка задачи (args, история, родитель, продолжения, batch), batches (список, дерево), recurring (с историей запусков), серверы (с состоянием задач обслуживания), типы задач;
- действия над задачей и над выбранными задачами (cancel / trigger / requeue), отмена batch, pause / resume / trigger / delete расписаний;
- запуск задачи по имени с формой из `job_types.args_schema`;
- статистика: агрегированные счётчики исходов от серверов (`stats_minutes`) и снимок живых состояний (`queue_stats`), изменения `PgBolt.Server` (§4);
- новые индексы для выборок (§3.2).

**Не входит:**
- готовый хост (Docker-образ, `dotnet tool`) — отдельно позже, поверх библиотеки;
- opt-in индексы и поиск по `args` (перенесено из части 2) — отдельной частью: затрагивают генератор и миграции;
- массовые действия по фильтру («requeue всех Failed за сутки»); действия — только над явно выбранными задачами;
- ручной запуск задач обслуживания (контракт части 2 §11): это собственный SQL записи вне `IJobClient` / `IRecurringJobManager` (§7); состояние задач только показывается (§6.1);
- редактирование расписаний (cron, args) из UI;
- общие количества Succeeded / Failed / Cancelled за всё время;
- браузерные тесты (Playwright).

### Критерии успеха
- Ни один запрос dashboard не делает Seq Scan по `jobs`, `queue`, `job_events` (контракт планов, §12); каждый statement `DashboardStore` ограничен `QueryTimeout` (карточки и перечитывание после действий идут через API core — поиск по PK — без него), списки, предикат которых не целиком в условии индекса, читаются порциями с бюджетом (§6.3).
- Горячий путь не получает новых statement'ов: счётчики исходов накапливаются в памяти и сбрасываются одним statement'ом на процесс раз в `StatsFlushInterval`; тест HOT-контракта `jobs` (core §11) проходит с новыми индексами.
- Хосту не нужна настройка сверх `AddPgBoltDashboard` / `MapPgBoltDashboard`: не нужны `AddRazorComponents`, `MapRazorComponents`, `UseAntiforgery`, статические файлы; dashboard не конфликтует с Blazor / MVC / Razor Pages хоста, в том числе с их antiforgery (§5.1).
- Без авторизации на группе dashboard доступен только локальным запросам не через прокси (§5.3).

## 2. Принятые решения

| Решение | Значение | Почему |
|---|---|---|
| Рендеринг | Minimal API + `.razor`-компоненты через `HtmlRenderer` (`Microsoft.AspNetCore.Components.Web`) + htmx | Встроено во фреймворк, типизированные шаблоны; `HtmlRenderer` требует только DI — нулевая настройка хоста (ограничения — §5.1). Blazor (SSR/Server) требует `AddRazorComponents`/`MapRazorComponents` на уровне приложения и конфликтует с Blazor хоста; RazorSlices — внешняя зависимость; Razor Pages смешиваются с маршрутами хоста |
| Поставка | Только библиотека | Хост — ~30 строк, отдельно; в тестах — sample-хост |
| Зависимости | `PgBolt.Postgres` (не `PgBolt.Server`) | Dashboard встраивается в любой сервис кластера, включая не обрабатывающие задачи; типы задач в процесс не загружаются |
| AOT | Пакет не AOT и не trimmable | Параметры компонентов — reflection; требование AOT роадмапа касается горячего пути |
| Статистика Succeeded | Агрегированные счётчики от серверов (§4.1) | Rollup сканированием `jobs` конкурирует с горячим путём; `pg_stat_user_tables` без разбивки и сбрасывается |
| Живые состояния | Снимок `queue_stats` кластерной задачей (§4.2) | Dashboard читает готовые агрегаты, `COUNT(*)` по `queue` — один под раз в интервал |
| Failed / Cancelled | По `job_events`: каждый терминальный переход пишет событие — Failed: `kind` 2 (воркер), 7 (NoExecutor, core §5.4) или 3 (sweeper на последней попытке); Cancelled: 4 | Insert-only таблица, частичные индексы не трогают HOT; набор `kind` расширяется на стороне чтения — протоколы записи прошлых частей не меняются |
| Succeeded | Обратный обход PK `jobs` порциями с бюджетом (§6.3) | Иных следов у успеха нет; индекс по `state` запрещён контрактом HOT |
| CSRF | `HX-Request` + `Sec-Fetch-Site`/`Origin` | Antiforgery ASP.NET Core требует `UseAntiforgery()` у хоста; выводимая Minimal API проверка antiforgery отключается на группе (§5.1) |
| Живые обновления | htmx polling | Без SignalR — работает через любой proxy без sticky sessions |
| Миграции | Изменения вносятся в `V001__initial.sql`; схема прошлой сборки отвергается при старте (§11) | Как в частях 4–5: пользователей нет, совместимость со схемами прошлых сборок не поддерживается |

## 3. Схема (добавляется в `V001__initial.sql`)

### 3.1 Таблицы статистики

```sql
CREATE TABLE {schema}.stats_minutes (
    bucket    timestamptz NOT NULL,           -- начало минуты UTC
    server_id uuid        NOT NULL,           -- виртуальный сервер: свои строки, горячих строк нет
    queue     text        NOT NULL,
    succeeded int         NOT NULL DEFAULT 0,
    failed    int         NOT NULL DEFAULT 0, -- терминальный Failed, подтверждённый воркеру (§4.1)
    retried   int         NOT NULL DEFAULT 0, -- попытка упала, назначен retry
    PRIMARY KEY (bucket, server_id, queue)
);

CREATE TABLE {schema}.queue_stats (           -- снимок живых состояний (§4.2), строка на очередь
    queue       text        PRIMARY KEY,
    scheduled   bigint      NOT NULL,
    enqueued    bigint      NOT NULL,
    processing  bigint      NOT NULL,
    awaiting    bigint      NOT NULL,
    measured_at timestamptz NOT NULL
);
```

### 3.2 Новые индексы

```sql
-- Список Awaiting по job_id DESC (§6.1); в индекс попадают только ждущие строки.
CREATE INDEX queue_awaiting_list ON {schema}.queue (job_id) WHERE await_condition IS NOT NULL;
-- Списки Failed / Cancelled (§6.1), ключ (at, job_id). Терминальный Failed пишет kind 2 (воркер), 7 (NoExecutor)
-- или 3 (sweeper на последней попытке); 3 и 7 бывают и у нетерминальных переходов — их отсекает проверка jobs.state.
CREATE INDEX job_events_failed    ON {schema}.job_events (at, job_id) WHERE kind IN (2, 3, 7);
CREATE INDEX job_events_cancelled ON {schema}.job_events (at, job_id) WHERE kind = 4;
-- Продолжения-задачи в карточках (§6.2), включая уже запущенные: коды 1–2 — родитель-задача, 3–4 — родитель-batch.
-- Члены batch-продолжения (continuation = 5, parent_id = id batch, часть 5 §2.1) в индекс не входят.
CREATE INDEX jobs_parent ON {schema}.jobs (parent_id, id) WHERE continuation IN (1, 2, 3, 4);
-- История запусков расписания (§6.1, контракт части 4 §9); recurring_id неизменяем.
CREATE INDEX jobs_recurring ON {schema}.jobs (recurring_id, id) WHERE recurring_id IS NOT NULL;
-- Batch-продолжения узла в любом состоянии (сейчас индексированы только ждущие: batches_awaiting).
CREATE INDEX batches_parent ON {schema}.batches (parent_id, id) WHERE parent_id IS NOT NULL;
```

- Индексы `job_events` начинаются с `at`, а не с `kind`: при `kind IN (…)` в ведущей колонке индекс не отдаёт строки в порядке `at` без сортировки всего диапазона. Пара `(at, job_id)` уникальна в пределах каждого из них: statement финализации пишет задаче не больше одного события из набора индекса (sweeper с `cancel_requested` пишет 3 и 4 — в разные индексы).
- Тест-контракт HOT (core §11) проверяет отсутствие изменяемых колонок в индексах `jobs`; `jobs_parent` и `jobs_recurring` строятся по неизменяемым `parent_id`, `continuation` (часть 5 §4), `recurring_id` (часть 4 §3), `id`. Компактирование части 2 создаёт партиции `LIKE … INCLUDING …` с последующим `ATTACH`: индексы родителя создаются на новой партиции автоматически.

## 4. Статистика

### 4.1 Счётчики исходов (`PgBolt.Server`)

- Учитываются только исходы, **подтверждённые БД**, по значению, которое вернул store: ack — `job_id` из ответа `AckAsync`; retry / fail — `FailOutcome` (`Retried` → `retried`, `Failed` → `failed`). `FailOutcome.Cancelled` (retry или fail задачи с `cancel_requested` финализирует её как Cancelled, core §5.4) и `LeaseLost` не учитываются. Release по shutdown, `ConfirmCancelAsync` и sweeper не учитываются: отмены видны в списке Cancelled, sweeper переводит задачу в retry или Failed без участия воркера.
- Расхождения со списками — допущение (обзор подписывает статистику как приблизительную): `failed` не включает терминальные Failed от sweeper (lease истёк на последней попытке) — они есть в списке Failed (§6.1); повтор отправки после неоднозначного сбоя получает «lease не найден» (worker §6.1), и применённый исход не учитывается.
- Изменение worker: `AckAsync` (`PgBoltStore` и `IServerStore`) возвращает подтверждённые `job_id` вместо числа — `SELECT array(SELECT id FROM j) AS acked, …` вместо `count(*)` (`j` уже отфильтрован по `lease_id`), чтобы счётчик ack был в разрезе очереди; `CompletionSink` сопоставляет их с пакетом (`Execution.Queue`), лог «ack affected N of M» — по длине массива. Retry / Fail уже возвращают исход на одну задачу.
- Точка учёта — `CompletionSink` после успешной отправки. Ключ — `(bucket, server_id, queue)`, `bucket` — начало минуты UTC по `TimeProvider` в момент подтверждения.
- Процессный синглтон `OutcomeStats` (общий для виртуальных серверов процесса) держит словарь счётчиков. Цикл сброса в `PgBoltProcessingService` раз в `StatsFlushInterval` атомарно забирает словарь и пишет его одним statement'ом (`PgBoltStore.FlushOutcomeStatsAsync`):
  ```sql
  INSERT INTO {schema}.stats_minutes AS s (bucket, server_id, queue, succeeded, failed, retried)
  SELECT * FROM unnest(@buckets, @servers, @queues, @succeeded, @failed, @retried)
  ON CONFLICT (bucket, server_id, queue) DO UPDATE
      SET succeeded = s.succeeded + EXCLUDED.succeeded,
          failed    = s.failed    + EXCLUDED.failed,
          retried   = s.retried   + EXCLUDED.retried;
  ```
  Ошибка — забранные значения возвращаются в словарь (слиянием с накопленными за это время) и уходят следующим сбросом; предупреждение в лог не чаще раза в минуту.
- Остановка: `StopAsync` после `FlushAndStopAsync` всех серверов (последние исходы учтены) останавливает цикл сброса, дожидаясь идущего сброса, и выполняет финальный. В процессе одновременно идёт не больше одного сброса: два upsert одних ключей в разном порядке дали бы `40P01`; ключи разных процессов не пересекаются (`server_id`). Время финального сброса входит в `ShutdownReserve` (проверка `HostOptions.ShutdownTimeout`, worker §4.4); при отменённом токене остановки сброс пропускается. Падение процесса теряет не больше интервала.
- Сброс работает в любом процессе с `AddPgBoltServer` независимо от `RunMaintenance`.

### 4.2 Снимок живых состояний и retention статистики

Кластерная задача `stats` в `maintenance_tasks` (протокол захвата части 2 §5.7) с интервалом `StatsSnapshotInterval`. Housekeeping-цикл опрашивает `maintenance_tasks` раз в `MaintenancePollInterval` (1 мин), поэтому у `stats` — **отдельный цикл** в `ProcessingMaintenance` с паузой `StatsSnapshotInterval` (±20 %), только при `RunMaintenance = true`; задачу за интервал выполняет один под. Захват — с интервалом 0,8 × `StatsSnapshotInterval` (`next_run_at = now() + 0,8 × интервал`): под, проснувшийся не раньше 0,8 интервала после своего прохода, застаёт срок наступившим, и период прохода — 0,8–1,2 интервала (при захвате с полным интервалом половина пробуждений приходилась бы на ещё не наступивший срок и период растягивался до двух интервалов).

Проход:
1. Одна транзакция (`PgBoltStore.SnapshotQueueStatsAsync`):
   ```sql
   WITH s AS (
       SELECT queue,
              count(*) FILTER (WHERE lease_owner IS NULL AND NOT ready AND await_condition IS NULL) AS scheduled,
              count(*) FILTER (WHERE lease_owner IS NULL AND ready)                                 AS enqueued,
              count(*) FILTER (WHERE lease_owner IS NOT NULL)                                       AS processing,
              count(*) FILTER (WHERE await_condition IS NOT NULL)                                   AS awaiting
       FROM {schema}.queue GROUP BY queue
   ), up AS (
       INSERT INTO {schema}.queue_stats SELECT s.*, now() FROM s ORDER BY s.queue
       ON CONFLICT (queue) DO UPDATE SET scheduled = EXCLUDED.scheduled, enqueued = EXCLUDED.enqueued,
           processing = EXCLUDED.processing, awaiting = EXCLUDED.awaiting, measured_at = EXCLUDED.measured_at
   )
   DELETE FROM {schema}.queue_stats q WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.queue = q.queue);
   ```
   `queue` содержит только живые задачи; при миллионах Scheduled — сотни миллисекунд на одном поде раз в интервал (удержание снимка — §13). Предикаты состояний — те же, что в core §2 и части 5 (Awaiting-строка: `ready = false`, без lease, `await_condition IS NOT NULL`). `ORDER BY queue` фиксирует порядок блокировок upsert'а при перекрытии запусков двух подов (часть 2 §6.2); остаточная гонка с удалением исчезнувших очередей — ошибка прохода и повтор (ниже). В простое `queue` пуста и `queue_stats` тоже: свежесть снимка определяется по строке задачи, а не по `queue_stats` (§6.4).
2. Retention (`PgBoltStore.TrimStatsAsync`): `DELETE FROM stats_minutes WHERE ctid IN (SELECT ctid … WHERE bucket < now() - @retention LIMIT 10000)` порциями, пока удаляется полная порция.

Ошибка прохода — `last_error` задачи (каждым проходом) и повтор в следующем интервале. Ошибка в лог (Error) — не чаще раза в минуту, как у сброса счётчиков (§4.1): `ProcessingMaintenance.RunTaskAsync` логирует каждое исключение, и постоянная ошибка при интервале 10 с давала бы ~6 записей в минуту; для `stats` он получает ограничитель.

### 4.3 Опции (`PgBoltProcessingOptions`)

| Опция | По умолчанию | Смысл |
|---|---|---|
| `StatsFlushInterval` | 15 с | Сброс счётчиков исходов процесса |
| `StatsSnapshotInterval` | 10 с | Интервал задачи `stats`; как у остальных задач, действует значение пода, захватившего задачу последним |
| `StatsRetention` | 7 сут | Хранение `stats_minutes` |

Валидация — как у существующих опций: интервалы — `> 0` и `≤ MaxInterval` (1 сут, `PgBoltServerOptions`); `StatsRetention` — `> 0` и `≤ MaxRetention` (3650 сут, `PgBoltOptions`, как у `JobsRetention` / `EventsRetention`).

## 5. Пакет, подключение, безопасность

### 5.1 Пакет `PgBolt.Dashboard`

`Microsoft.NET.Sdk.Razor`, `FrameworkReference Microsoft.AspNetCore.App`, зависимость — `PgBolt.Postgres` (`InternalsVisibleTo`, §11).

- `Endpoints/` — Minimal API в одной `RouteGroup`: страницы (GET) и действия (POST `/actions/…`). Группа вызывает `.DisableAntiforgery()`: с .NET 8 Minimal API для параметров формы (`[FromForm]`, `IFormCollection`) сам добавляет метаданные проверки antiforgery, а `WebApplication` сам вставляет `UseAntiforgery`, если сервисы antiforgery зарегистрированы (`AddRazorComponents`, `AddRazorPages`, `AddControllersWithViews`). Без отключения POST формы от htmx (§7, §8) в таком хосте получил бы 400 без токена, а в хосте без middleware — исключение `EndpointMiddleware`. Метаданные группы идут после выведенных и перекрывают их; CSRF — собственной проверкой (§5.3).
- `Components/` — `.razor`-страницы и фрагменты, рендер в `IResult` (`text/html`):
  - `HtmlRenderer` создаётся на запрос (`await using`) с обёрткой `HttpContext.RequestServices` и `ILoggerFactory`; обёртка отдаёт `HtmlEncoder` (`UnicodeRanges.All`) и скрывает `NavigationManager` хоста (иначе `HtmlRenderer` обращается к неинициализированному `NavigationManager` хоста с Razor Pages / Components); рендер — только внутри `renderer.Dispatcher.InvokeAsync(...)`;
  - `@layout` вне `RouteView` не применяется: полная страница — явная оболочка (`Shell`, свой компонент) с компонентом страницы внутри;
  - компоненты фреймворка, требующие сервисов `AddRazorComponents`, не используются: `NavLink` / `NavigationManager`, `EditForm` / `AntiforgeryToken`, `@rendermode`; ссылки и формы — обычная разметка с атрибутами htmx.
- Фрагмент или страница: запрос с `HX-Request: true` без `HX-History-Restore-Request` получает фрагмент (содержимое страницы); остальные, в том числе восстановление истории htmx при промахе кеша, — страницу целиком. HTML-ответы — с `Vary: HX-Request, HX-History-Restore-Request` и `Cache-Control: no-store` (один URL — два представления; данные задач не кешируются). `historyCacheSize = 0` в конфигурации htmx: снимки страниц с args не попадают в `localStorage`, «назад» всегда идёт restore-запросом.
- `Data/DashboardStore` — read-only SQL экранов (§6); действия — через `IJobClient` и `IRecurringJobManager` (§7), своего SQL на запись нет.
- `Assets/` — embedded resources: `htmx.min.js` (версия фиксируется в репозитории), собственный CSS без фреймворков, скрипт локального времени (§9). Каталог не называется `wwwroot`: Razor SDK публикует `wwwroot` как static web assets (`_content/…`) в хост. Раздаются endpoint'ом группы по пути с хешем содержимого, `Cache-Control: public, max-age=31536000, immutable`.

### 5.2 Подключение

```csharp
services.AddPgBolt(...);
services.AddPgBoltDashboard(o =>
{
    o.ActionsPolicy = "PgBoltOperators"; // null — действия разрешены всем, кто видит dashboard
    o.ReadOnly = false;                  // true — действия скрыты и запрещены
});

app.MapPgBoltDashboard("/pgbolt")        // RouteGroupBuilder
   .RequireAuthorization("PgBoltViewers");
```

`PgBoltDashboardOptions`:

| Опция | По умолчанию | Смысл |
|---|---|---|
| `ActionsPolicy` | `null` | Политика авторизации для действий и запуска задач |
| `ReadOnly` | `false` | Действия не рендерятся, POST → 403 |
| `AllowAnonymousRemote` | `false` | Разрешить нелокальный доступ без авторизации на группе (§5.3) |
| `QueryTimeout` | 5 с | `SET LOCAL statement_timeout` каждого statement'а |
| `ScanBudget` | 2 с | Бюджет обхода порциями (§6.3) |
| `RefreshInterval` | 5 с | Polling обзора за час и счётчиков очередей (§9) |
| `ServerStaleAfter` | 1 мин | Сервер без heartbeat дольше — «не отвечает» |
| `StatsStaleAfter` | 1 мин | Срок задачи `stats` (`next_run_at`) просрочен дольше (сверх допуска по интервалу) — баннер (§6.4) |

`AddPgBoltDashboard` без `AddPgBolt` — `InvalidOperationException` при `MapPgBoltDashboard` с подсказкой.

### 5.3 Авторизация и CSRF

- **Просмотр** — средствами хоста (`RequireAuthorization` и др. на группе). Авторизация есть, если в метаданных endpoint'а есть `IAuthorizeData`, `AuthorizationPolicy` или `IAuthorizationRequirementData` (все перегрузки `RequireAuthorization`, `[Authorize]`) либо хост задал fallback-политику (`IAuthorizationPolicyProvider.GetFallbackPolicyAsync()` не `null`); при `IAllowAnonymous` в метаданных авторизации нет. Conventions группы применяются после возврата `MapPgBoltDashboard`, поэтому признак вычисляет фильтр группы на запросе по `HttpContext.GetEndpoint().Metadata`, а предупреждение в лог пишется по `ApplicationStarted` обходом `EndpointDataSource` (endpoint'ы dashboard помечены своим маркером).
- Без авторизации и при `AllowAnonymousRemote = false` фильтр пропускает только локальные запросы, иначе 403. Локальный — `RemoteIpAddress` loopback (`IPAddress.IsLoopback`, включая IPv4-mapped) **и** нет заголовков `Forwarded`, `X-Forwarded-For`, `X-Real-IP`: reverse proxy на том же хосте без `ForwardedHeaders` приходит с loopback, но с заголовком; с `ForwardedHeaders` `RemoteIpAddress` — адрес клиента. `RemoteIpAddress == null` (`TestServer`, нестандартные транспорты) — нелокальный. Ограничение (документация пакета): sidecar service mesh (Envoy / Istio: 127.0.0.6, 127.0.0.1), не добавляющий заголовков, неотличим от локального клиента — в mesh авторизация на группе обязательна.
- **Действия** дополнительно проверяют `ActionsPolicy` через `IAuthorizationService`; при `ReadOnly` или отказе политики кнопки не рендерятся, POST → 403.
- **CSRF**: POST принимается только при `HX-Request: true` и при `Sec-Fetch-Site: same-origin` либо (если заголовка нет) `Origin`, совпадающем со схемой и хостом запроса; иначе 403. Кросс-сайтовый запрос не может выставить нестандартный заголовок без CORS preflight. За TLS-прокси хост применяет `ForwardedHeaders` (`X-Forwarded-Proto`, `X-Forwarded-Host`), иначе схема запроса не совпадёт с `Origin` и браузеры без `Sec-Fetch-Site` получат 403 (документация пакета).
- Вывод — экранирование Razor по умолчанию; args, message и exception — текстом в `<pre>`. `MarkupString` не используется для данных из БД.

## 6. Чтение

Запросы `DashboardStore` — READ COMMITTED, `READ ONLY`, с `SET LOCAL statement_timeout = QueryTimeout` (совместимо с PgBouncer transaction mode). READ COMMITTED обязателен (часть 2 §11): снимок REPEATABLE READ, взятый до компактирования, не видит живые задачи сжатой партиции; экран, которому нужна согласованность нескольких выборок, собирает их одним statement'ом. Каждый statement — в собственной короткой транзакции: снимок (xmin) и `AccessShareLock` на `jobs` и её партициях держатся миллисекунды, а не весь обход (`LOCK` родителя в retention ждёт с `lock_timeout` 100 мс, часть 2 §2). `DashboardStore` работает через `PgBoltDataSource` и, как `PgBoltStore`, соблюдает `PgBoltOptions.UsePreparedStatements` (`PrepareAsync` только при `true`). Пагинация — keyset: курсор — непрозрачная строка base64url от ключа сортировки (`?after=`); размер страницы — 50. Общих количеств в списках нет.

### 6.1 Списки и экраны

| Экран | Источник | Порядок / индекс |
|---|---|---|
| Обзор | `queue_stats`; суммы и график за 1 ч (60 поминутных точек); суммы и график за 24 ч (96 точек по 15 мин) — отдельный фрагмент (§9); число живых серверов; строка `stats` в `maintenance_tasks` (§6.4) | PK `stats_minutes` (диапазон `bucket`) |
| Очереди | `queue_stats` ∪ `unnest(servers.queues)` живых серверов ∪ строки `queue_settings` хотя бы с одним переопределением (часть 3b): очередь без задач, но обслуживаемая или с переопределением, — с нулями и числом серверов; колонки «Лимит» (`N / лимит`, живое число взятых строк — только у очередей со строкой `queue_settings`; бейдж «Пауза»; «из кода» или «переопределён: кто, когда») и «Вес»; настройки — `IQueueSettingsManager.ListAsync` | маленькие таблицы; живое число — index-only по `queue_leased` |
| Scheduled | `queue` `lease_owner IS NULL AND NOT ready AND run_at < 'infinity' AND await_condition IS NULL` | `run_at, job_id` / `queue_delayed`; `run_at < 'infinity'` — условие индекса, отсекающее Awaiting-строки (`run_at = 'infinity'`, инвариант части 5 §4; при нём `await_condition IS NULL` избыточен) |
| Enqueued | `queue` выбранной очереди, `lease_owner IS NULL AND ready` (список всегда в разрезе очереди) | `priority DESC, run_at, job_id` / `queue_fetch` |
| Processing | `queue` `lease_owner IS NOT NULL` | `leased_at, job_id` / `queue_leased` (с фильтром по очереди — range scan по ключу); каждая страница читает все взятые строки (не больше суммарной ёмкости серверов), сортирует в памяти и отдаёт после курсора `(leased_at, job_id)` |
| Awaiting | `queue` `await_condition IS NOT NULL` | `job_id DESC` / `queue_awaiting_list` |
| Succeeded | `jobs` `state = 2` | `id DESC` (≈ время создания) / PK партиций, порциями (§6.3) |
| Failed | `job_events` `kind IN (2, 3, 7)` → `jobs` по PK, `state = 3`, событие — последний переход задачи (ниже) | `(at, job_id) DESC` / `job_events_failed`, порциями (§6.3) |
| Cancelled | `job_events` `kind = 4` → `jobs` по PK, `state = 4`, событие — последний переход задачи (ниже) | `(at, job_id) DESC` / `job_events_cancelled`, порциями (§6.3) |
| Batches | `batches` по PK с фильтром состояния; прогресс — счётчики, ограниченные снизу нулём (`greatest(0, …)`), как в `GetBatchAsync` (часть 5 §10) | `id DESC`, порциями (§6.3) |
| Recurring | `recurring_jobs`; история запусков расписания — `jobs WHERE recurring_id = @id` + производное состояние из `queue` | `id`; история — `id DESC` / `jobs_recurring`, keyset |
| Серверы | `servers` + суммы `stats_minutes` за 1 ч по `server_id`; задачи обслуживания — `maintenance_tasks` (`last_run_at`, `last_finished_at`, `last_error`, `next_run_at`; идёт ли запуск — по правилу части 2 §5.7) | `name` |
| Типы задач | `job_types` | `name` |

- Строки списков из `queue` дополняются `type`, `created_at` и т. п. из `jobs` по PK (nested loop с runtime pruning партиций по `id`). Фильтры — очередь и тип (`type` неизменяем).
- Failed / Cancelled: событие показывается, только если это событие текущего терминального перехода — `j.state = 3` (Cancelled — `4`) `AND j.finished_at = e.at`. Каждый путь финализации пишет `jobs.finished_at` и событие одним statement'ом с одним `now()` (Fail, Sweeper, Cancel, Release, отмена batch, Resolver), requeue обнуляет `finished_at` (`PgBoltStore.Requeue`): прежний Failed после requeue не совпадает, повторный Failed — одна строка, нетерминальные 3 / 7 отсекаются по `state` или времени; пара 3 + 4 sweeper'а с отменой (одно `at`) попадает только в Cancelled (`state = 4`, 3 — в другом индексе). Второго чтения `job_events` нет. Событие 3 без `message` показывается как «lease истёк на последней попытке».
- Глубина Failed / Cancelled — `EventsRetention`, а не `JobsRetention` (§13); вкладки подписаны «переходы за {EventsRetention}» (значение `PgBoltOptions` процесса; `null` — хранить вечно — «переходы за всё время хранения»).
- Порциями с бюджетом (§6.3) читается каждый список, предикат которого не целиком в условии индекса: Succeeded, Failed, Cancelled (`state`, `finished_at`, события задач с уже удалённой партицией `jobs` — retention `jobs` и `job_events` независимы, часть 2), Batches с фильтром состояния, Scheduled и Awaiting с фильтром очереди или типа, Enqueued с фильтром типа (тип — после join). Остальные списки — индексное условие и `LIMIT`; Processing ограничен ёмкостью серверов.

### 6.2 Карточка задачи и batch

- Задача — `IJobClient.GetAsync` (строка, производное состояние, события). Дополнительно:
  - продолжения: `jobs WHERE parent_id = @id AND continuation IN (1, 2) AND id >= @lower` (`jobs_parent`; `@lower` — UUIDv7 от времени создания родителя минус сутки, отсекает партиции; запас — на расхождение часов клиентов) + текущее производное состояние из `queue`;
  - batch-продолжения: `batches WHERE parent_id = @id` (`batches_parent`);
  - родитель (задача или batch) и batch — ссылками. Продолжений больше 50 — keyset-пагинация.
- Batch — `GetBatchAsync` + члены: задачи `jobs WHERE batch_id = @id` (`jobs_batch`) и вложенные batches (`batches_nested`), keyset по `id`; batch-продолжения (`batches_parent`) и продолжения-задачи (`jobs WHERE parent_id = @id AND continuation IN (3, 4) AND id >= @lower`, `jobs_parent`; `@lower` — от `created_at` batch минус сутки, как у задачи); члены batch-продолжения (`continuation = 5`) — только среди членов; внешний batch и родитель — ссылками.

### 6.3 Обход порциями с бюджетом

Для списков §6.1, предикат которых не целиком в условии индекса:
- порция — один statement (своя транзакция, §6): внутренний подзапрос берёт по индексу списка следующие 2000 ключей после курсора в порядке списка (§6.1; например `WHERE id < @after ORDER BY id DESC LIMIT 2000`), внешний применяет остальные предикаты (join с `jobs`, `state`, `finished_at`, тип, очередь) и возвращает подходящие строки и последний просмотренный ключ. Время statement'а ограничено размером порции, а не плотностью подходящих строк;
- ключ — ключ сортировки списка: `id` для `jobs` и `batches`, `(at, job_id)` для `job_events`, `job_id` для Awaiting, `(run_at, job_id)` для Scheduled, `(priority, run_at, job_id)` для Enqueued;
- ключ Enqueued смешанного направления (`priority DESC`, остальное по возрастанию): row-сравнение дало бы неверный порядок, а `OR`-форма использует индекс только по `queue`. Выборка после курсора — `UNION ALL` двух ветвей, каждая с `ORDER BY … LIMIT` по `queue_fetch`: `queue = @q AND priority = @p AND (run_at, job_id) > (@r, @j)` и `queue = @q AND priority < @p`; внешний `ORDER BY priority DESC, run_at, job_id LIMIT`. Та же форма — у страниц Enqueued без фильтра;
- обход идёт от курсора, пока страница не заполнена, не исчерпан `ScanBudget` (проверяется между порциями) или порция не вернула меньше 2000 ключей (данные кончились);
- курсор: страница заполнена — ключ последней выданной строки; бюджет исчерпан — последний просмотренный ключ, поэтому «продолжить поиск» всегда продвигается. Неполная страница показывается с «Просмотрено до 14:20 — продолжить поиск» (время — из ключа: UUIDv7, `at`, `run_at`).

### 6.4 Устаревшая статистика

Свежесть снимка определяется по строке `stats` в `maintenance_tasks`, а не по `queue_stats` (в простое он пуст, §4.2):
- строки нет или `now() - next_run_at > StatsStaleAfter + 0,5 × (next_run_at - last_run_at)` — задачу никто не захватывает. Захват пишет `last_run_at = now()` и `next_run_at = now() + 0,8 × интервал` (§4.2), поэтому разность — 0,8 интервала пода, захватившего задачу последним, а законное опоздание следующего захвата — до 0,4 интервала плюс длительность прохода: dashboard выводит допуск из строки и не зависит от `StatsSnapshotInterval`, которого не знает, — баннер «снимок не обновляется: ни один процесс не выполняет обслуживание (`RunMaintenance`)»;
- `last_error` не `NULL` — баннер с текстом ошибки последнего прохода;
- свежий снимок и пустой `queue_stats` — живых задач нет, очереди показываются с нулями.

Отсутствие строк `stats_minutes` за последний час при живых серверах — подпись «нет данных».

## 7. Действия

POST `/actions/…`; проверки §5.3. Только существующие API — новых протоколов записи нет.

| Объект | Действие | Вызов |
|---|---|---|
| Задача | cancel, trigger, requeue | `IJobClient.CancelAsync` (reason `dashboard: {User.Identity.Name ?? "anonymous"}`), `TriggerAsync`, `RequeueAsync` |
| Выбранные задачи | те же | до 100 id за запрос, последовательно; ответ — сводка «выполнено N, неприменимо M» с перечнем неприменимых |
| Batch | cancel | `CancelBatchAsync` |
| Recurring | pause, resume, trigger, delete | `IRecurringJobManager`; delete — только `Owner == null` (code-first строки API не удаляет) |
| Очередь (часть 3b) | лимит: задать (≥ 1), без лимита, пауза, сбросить к значению из кода; вес: задать (1…1000), без веса, сбросить к значению из кода | `IQueueSettingsManager` (`updatedBy = dashboard: {User.Identity.Name ?? "anonymous"}`), POST `actions/queues/limit?queue=…` (`op` = `set` / `unlimited` / `pause` / `reset`, `value`) и `actions/queues/weight?queue=…` (`op` = `set` / `none` / `reset`, `value`); ответ — фрагмент строки очереди |
| Настройки очередей (часть 3b) | сбросить кэш | POST `actions/queues/cache/reset`: `IQueueSettingsManager.InvalidateCache()` и `RefreshNow()` снимка настроек процесса, если в нём есть сервер |

- Кнопки рендерятся только для применимых состояний (requeue — Failed / Cancelled, trigger — Scheduled, cancel — нетерминальные); результат API принимается как есть — гонка с изменением состояния показывается в ответе («задача уже завершена»).
- Действие над строкой списка принимает `kind` вкладки (`?view=row&kind=…`): строка после действия рендерится с колонками этой вкладки и бейджем нового состояния; без `kind` — вкладка нового состояния; невалидный `kind` — 400.
- Каждое действие — запись `ILogger` (Information): пользователь, действие, объект, результат.
- Ответ — htmx-фрагмент: обновлённая строка / карточка + уведомление в `HX-Trigger`: JSON `{"pgbolt:notify": "…"}`, сериализованный `System.Text.Json` с кодировщиком по умолчанию (не-ASCII → `\uXXXX`; Kestrel отвергает не-ASCII в заголовках ответа).

## 8. Запуск задачи по имени

Экран «Типы задач» → форма «Поставить» для типа из `job_types`.

- **Форма из схемы** (`args_schema` — вывод `JsonSchemaExporter`). Поддерживаемое подмножество: `type` `string` (в т. ч. `format: date-time`, `uuid`), `integer`, `number`, `boolean`; `enum`; nullable-варианты (`["string", "null"]`); вложенные объекты (`type: object` с `properties`) — fieldset; `required`. Схема с чем-либо сверх подмножества (массивы, `anyOf`/`oneOf`, `additionalProperties`-словари, `$ref`) открывается только в режиме JSON. Переключатель «Форма / JSON» есть всегда; JSON-режим — textarea с предзаполнением из формы.
- **Опции постановки**: очередь (по умолчанию `default_queue`), приоритет, задержка или время запуска, `unique_key`, `max_attempts`.
- **Проверка** перед вызовом: то же подмножество схемы (обязательные поля, типы, enum, формат); ошибки — у полей. Окончательная проверка args — при выполнении (`JobArgumentsException`, core).
- **Вызов**: `IJobClient.EnqueueAsync(name, JsonElement, EnqueueOptions)`. Результат — ссылка на карточку новой задачи; при конфликте `unique_key` — «уже есть активная задача» со ссылкой на неё; `Id = null, Created = false` (задача с этим ключом завершилась между запросами, core §5.1) — «задача с этим ключом только что завершилась» без ссылки; тип исчез из `job_types` — `UnknownJobException` → сообщение.

## 9. Интерфейс

- Навигация: Обзор · Очереди · Задачи (вкладки Scheduled / Enqueued / Processing / Awaiting / Succeeded / Failed / Cancelled) · Batches · Recurring · Серверы · Типы задач. Фильтры и курсоры — в query string (выборкой можно поделиться ссылкой); htmx-навигация с `hx-push-url`.
- График throughput — серверный SVG из Razor-компонента (столбцы succeeded / failed), без JS-библиотек.
- Время — `<time datetime="…">` в UTC; скрипт (~20 строк, `htmx.onLoad`: срабатывает и на загрузке страницы, и на каждом новом содержимом, включая восстановление истории при промахе кеша, где htmx 2 не шлёт `htmx:afterSwap`) переводит в локальное время браузера и добавляет относительное («3 мин назад»). Скрипт — файлом из `Assets/`, конфигурация htmx — `<meta name="htmx-config">`: `allowEval: false`, `historyCacheSize: 0` (§5.1), `responseHandling` со свапом 4xx / 5xx (§10). Inline-скриптов и выражений htmx, вычисляемых через `Function` (фильтры `hx-trigger`, `hx-on`), нет — страница совместима с CSP `script-src 'self'`.
- Polling — `hx-trigger="every …"`, только при видимой вкладке: скрипт из `Assets/` на `htmx:beforeRequest` отменяет (`preventDefault()`) запросы элементов polling при `document.hidden`, а на `visibilitychange` к видимой — запрашивает их сразу. Обзор за час и счётчики очередей — раз в `RefreshInterval`; фрагмент за 24 ч (агрегирует до 1440 × серверы × очереди строк `stats_minutes`; `server_id` новый при каждом старте процесса) — раз в минуту. Списки не обновляются сами (кнопка «обновить»).
- Светлая / тёмная тема по `prefers-color-scheme`; адаптивная вёрстка.

## 10. Ошибки

- Таймаут statement'а (`57014`) — 503, «запрос не уложился в QueryTimeout, сузьте фильтр».
- Недоступная БД — 503, прочие исключения — 500: краткое сообщение (тип ошибки, без stack trace), полностью — в лог (Error).
- Несуществующие задача / batch / расписание — 404.
- Невалидный курсор или параметр — 400; отказ доступа и CSRF — 403 (§5.3).

Ответ на запрос htmx (§5.1) — фрагмент с этим статусом: htmx по умолчанию не свапает 4xx / 5xx, поэтому `responseHandling` (§9) задаёт свап `[45]..` в цель запроса (с событием ошибки). Без `HX-Request` — страница целиком с тем же статусом.

## 11. Изменения других частей

- **Core (`V001__initial.sql`)**: таблицы §3.1, индексы §3.2. `PgBoltInitializer.EnsureNotPreReleaseSchemaAsync` дополнительно требует `stats_minutes` и `queue_stats`: схема сборки части 5 имеет ту же версию 1, но без объектов части 6 (сброс счётчиков падал бы `42P01`, выборки — без индексов). `MigrationTests`: точный список таблиц `Migrate_creates_all_tables_once` (+ `stats_minutes`, `queue_stats`) и точные списки индексов: `queue` (+ `queue_awaiting_list`), `jobs` (+ `jobs_parent`, `jobs_recurring`), а также `job_events`, `batches`.
- **Core (`PgBoltStore`)**: `AckAsync` возвращает подтверждённые `job_id` (`array(SELECT id FROM j)`) — меняется публичная сигнатура, обновляются тесты и бенчмарки; новые методы `FlushOutcomeStatsAsync` (§4.1), `SnapshotQueueStatsAsync`, `TrimStatsAsync` (§4.2). `PgBolt.Postgres.csproj`: `InternalsVisibleTo` для `PgBolt.Dashboard` и `PgBolt.Dashboard.Tests` (`PgBoltDataSource`, `UuidV7`), как у `PgBolt.Server` и его тестов. Протоколы записи (sweeper, NoExecutor, cancel, requeue) не меняются: Failed / Cancelled читаются по существующим `kind` (§3.2). Dashboard опирается на инвариант существующих путей финализации — `jobs.finished_at` и событие терминального перехода пишутся одним statement'ом с одним `now()`, requeue обнуляет `finished_at` (§6.1); новые пути финализации обязаны его сохранять (тест §12).
- **Worker**: `IServerStore` / `PgBoltServerStore` — новая сигнатура `AckAsync` и методы выше; `CompletionSink` учитывает подтверждённые исходы в `OutcomeStats` по ответу store (§4.1); цикл сброса в `PgBoltProcessingService`; финальный сброс после `FlushAndStopAsync` и остановки цикла, его время — в `ShutdownReserve`.
- **Часть 2**: задача `stats` в `maintenance_tasks` с отдельным циклом (§4.2); ограничение логов ошибок `stats` в `RunTaskAsync` (§4.2); опции §4.3. Контракт §11: чтение `maintenance_tasks` — §6.1, §6.4; `jobs` — только READ COMMITTED (§6); ручной запуск задач — отложен (§1).
- **Часть 4**: контракт §9 — история запусков по `jobs_recurring` (§3.2, §6.1).
- **Часть 5**: контракт §10 — Awaiting, Scheduled, дерево и прогресс batch с `greatest(0, …)` (§6.1, §6.2); продолжения — по кодам `continuation` 1–4, члены batch-продолжения (код 5) продолжениями не считаются (§3.2).
- **Роадмап**: части 6 — ссылка на спеку; хост, opt-in индексы по `args` и ручной запуск задач обслуживания — отложены (§1).

## 12. Тестирование

xUnit v3, Testcontainers, Verify — как в остальных частях.

- **Unit**: построение формы из схем `JsonSchemaExporter` (поддерживаемое подмножество и fallback в JSON); проверка args по подмножеству; кодирование и разбор курсоров (невалидный → 400); обход порциями на `FakeTimeProvider` (неполная страница и курсор продолжения; курсор продвигается, когда бюджет исчерпан без единой подходящей строки; конец данных по неполной порции); локальный фильтр (loopback с `X-Forwarded-For` / `Forwarded` / `X-Real-IP` → 403, `RemoteIpAddress == null` → 403); признак авторизации (все перегрузки `RequireAuthorization`, `[Authorize]`, fallback-политика, `AllowAnonymous`); проверки CSRF; баннер устаревания по `next_run_at` / `last_run_at` при разных интервалах (нет ложного баннера при интервале больше `StatsStaleAfter`).
- **Счётчики (`PgBolt.Server`)**: учитываются только подтверждённые исходы (LeaseLost, fencing, release, cancel, `FailOutcome.Cancelled` от retry / fail — нет); ack — по возвращённым `job_id` в разрезе очереди; бакет по `TimeProvider`; ошибка сброса — значения сохраняются и сливаются с новыми; финальный сброс после остановки серверов и цикла, без параллельного сброса; несколько виртуальных серверов — раздельные строки.
- **Задача `stats`**: снимок по всем производным состояниям, удаление исчезнувших очередей, пустая `queue` — пустой `queue_stats` без баннера (свежесть по `next_run_at`), retention порциями, один под за интервал, постоянная ошибка — не больше одной записи Error в минуту при `last_error` каждого прохода; на `FakeTimeProvider` период прохода при захвате с 0,8 интервала — не больше 1,2 интервала.
- **Интеграционные `DashboardStore`** (реальный PG): каждый экран на подготовленных данных; Failed через sweeper (последняя попытка) и через NoExecutor виден в списке; requeue убирает задачу из Failed, повторный Failed (в том числе через sweeper) — одна строка; события задач с удалённой партицией `jobs` не дают строк; неполная страница при исчерпании бюджета и прогресс курсора; продолжения в карточке после запуска; карточка batch-продолжения не показывает его членов как продолжения; инвариант `jobs.finished_at = job_events.at` терминального события во всех путях финализации (fail, NoExecutor, sweeper с отменой и без, cancel, release, отмена batch, каскадная отмена resolver'а) и сброс `finished_at` requeue; дерево batch, прогресс не ниже нуля; история запусков расписания.
- **Контракт планов**: `EXPLAIN` каждого запроса экранов на заполненной схеме не содержит Seq Scan по `jobs`, `queue`, `job_events` (с `enable_seqscan` по умолчанию и статистикой после `ANALYZE`); Scheduled при большом числе Awaiting — `run_at < 'infinity'` в Index Cond `queue_delayed`; Failed / Cancelled — без Sort над диапазоном индекса; глубокая страница Enqueued — обе ветви `UNION ALL` с Index Cond по `queue_fetch` (`queue`, `priority`, `run_at`). Тест HOT-контракта core проходит с `jobs_parent` и `jobs_recurring`.
- **Инициализация**: схема без `stats_minutes` / `queue_stats` → `InvalidOperationException` с подсказкой `DROP SCHEMA`; `MigrationTests` с новыми списками таблиц и индексов.
- **Endpoints** (`TestServer`, `RemoteIpAddress` задаётся тестовым middleware): только локальные запросы по умолчанию; `RequireAuthorization` на группе; `ActionsPolicy` → 403 и кнопки скрыты; `ReadOnly`; POST без `HX-Request` или с чужим `Origin` → 403; `HX-Request` → фрагмент, без — страница, `HX-History-Restore-Request` → страница; ошибки (404, 400, 503 по `57014`) — фрагмент с этим статусом, в оболочке — `htmx-config` с `responseHandling` и `allowEval: false`; `Vary` и `Cache-Control: no-store` у HTML; уведомление с кириллицей в `HX-Trigger` проходит Kestrel; статика с хешем и кешем; хост с собственными Razor Pages / Blazor не конфликтует; POST формы (form-urlencoded, действие над выбранными и «Поставить») проходит и в хосте с `AddRazorComponents` + `AddAntiforgery` (авто-`UseAntiforgery`), и в хосте без сервисов antiforgery; Processing — keyset `(leased_at, job_id)`; запуск по имени при `Id = null` — сообщение без ссылки.
- **Snapshot (Verify)**: HTML ключевых компонентов (обзор, строка списка, карточка, форма из схемы) на фиксированных данных.
- **E2E**: существующий E2E — `ServiceCollection` без ASP.NET Core, поэтому dashboard — отдельный хост `WebApplication` на `TestServer` (пакет `Microsoft.AspNetCore.TestHost` добавляется в `Directory.Packages.props`) над схемой E2E с задачами, в обоих `ConnectionMode` (`Direct` и `Bouncer` — PgBouncer transaction mode: `SET LOCAL`, транзакция на statement, без prepared statements); smoke-проход всех страниц и одного действия по HTTP.

## 13. Риски

- **Стоимость снимка `queue_stats`** растёт с размером `queue` (миллионы Scheduled): Seq Scan на сотни миллисекунд держит снимок, и мёртвые версии `queue` и `jobs` за это время не вычищаются (vacuum, HOT-pruning — `HotUpdateTests`). Мера: один под раз в интервал, интервал настраивается; при необходимости — оценка по `pg_class.reltuples` для больших очередей (часть 7).
- **Удержание снимка и блокировок statement'ами dashboard**: каждый statement до `QueryTimeout` держит xmin и `AccessShareLock` на `jobs`; совпавший с ним `LOCK` retention получает `LockTimeout` и откладывается до следующего запуска (часть 2 §2). Мера: транзакция на statement, порции по 2000 ключей (§6.3).
- **Продолжения-задачи в карточке** читаются Bitmap Index Scan по `jobs_parent` + Sort (планировщик оценивает `parent_id` и `continuation` как независимые): страница читает и сортирует всех детей родителя; огромный fan-out проверяется бенчмарком части 7.
- **Порции обхода Succeeded** при очень высоком throughput с редким фильтром по типу: за `ScanBudget` просматриваются сотни тысяч строк — при 20k/s это секунды истории на нажатие «продолжить». Это ожидаемое поведение бюджета; точный поиск по типу — с opt-in индексами (отложено).
- **Равные ключи сортировки**: `queue_delayed` и `queue_fetch` не содержат `job_id`; при массовой постановке с одним `run_at` страница Scheduled / Enqueued досортировывает группу равных значений (Incremental Sort) — пропорционально её размеру.
- **`jobs_parent` и `jobs_recurring` на горячем пути**: частичные индексы, запись — только у продолжений (коды 1–4, без членов batch-продолжений) и запусков расписаний. Но каждый добавляет индекс на партицию: запросы без plan-time pruning (join dequeue, отсекаемый только при выполнении) планируют и блокируют все партиции с их индексами — время планирования и число блокировок растут (часть 2 §5.3, «Число партиций»); в режиме PgBouncer без prepared statements dequeue планируется на каждом вызове. Влияние на enqueue и dequeue проверяется бенчмарком части 7.
- **Глубина Failed / Cancelled** ограничена `EventsRetention`, а не `JobsRetention` (независимы, часть 2 §5.3–§5.4): при `EventsRetention < JobsRetention`, а также для Failed-родителей с ждущими детьми и Failed-членов batch, которых retention `jobs` удерживает дольше (часть 5 §7.1), задача доступна в карточке и для requeue, но не видна в списке. Мера: подпись вкладок (§6.1), документация; такие задачи находятся по id, из карточки batch или родителя.
- **Точность счётчиков**: падение процесса теряет до `StatsFlushInterval`; исходы после неоднозначного сбоя отправки и терминальные Failed от sweeper не учитываются (§4.1); dashboard показывает статистику как приблизительную.

## Изменения части 3b (кластерный лимит и веса очередей)

Подробности — `2026-10-10-pgbolt-queue-limits-design.md` (§8).
- §6.1, экран очередей: в списке и строки `queue_settings` с переопределением; колонки «Лимит» и «Вес»; действия лимита, веса
  и сброса кэша настроек (§7) — только через `IQueueSettingsManager`, скрыты и запрещены без права на действия.
- §6.1, список Processing: индекс `queue_leased` вместо `queue_owner`.
