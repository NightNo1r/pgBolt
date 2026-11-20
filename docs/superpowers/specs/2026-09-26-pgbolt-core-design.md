# PgBolt — часть 1: Core (модель, схема, горячий путь, Client API)

Статус: одобрено ревью (4 итерации) · Дата: 2026-09-26 · Карта проекта: `2026-09-26-pgbolt-roadmap.md`

## 1. Цель и рамки

Заложить фундамент job runner'а: модель задач, машину состояний, схему PostgreSQL, SQL горячего пути (enqueue / dequeue / ack / fail / heartbeat / sweeper / promoter / cancel / trigger / requeue), публичный API клиента, source generator, миграции и идемпотентное создание партиций.

**Входит:** всё перечисленное выше + контракт для частей 2–8.

**Не входит:** runtime воркеров и планирование периодических операций (часть 3), периодическое обслуживание партиций и retention (часть 2), recurring (4), continuations/batches (5), dashboard (6), OpenTelemetry-инструментирование (7), interceptors (8). Для них в core зарезервированы колонки, состояния и точки расширения.

### Ограничения
- .NET 10, Npgsql (`NpgsqlDataSource`), без EF Core. Без reflection на горячем пути, AOT-совместимо.
- PostgreSQL 13+ (используются `gen_random_uuid()` из ядра и `WITH … AS MATERIALIZED`).
- Если пользователь передаёт свой `NpgsqlDataSource`, собранный через `NpgsqlSlimDataSourceBuilder` (AOT), он обязан вызвать `EnableArrays()`: SQL горячего пути передаёт массивы (`uuid[]`, `jsonb[]`, `text[]`, `timestamptz[]`). `jsonb` передаётся и читается как UTF-8 текст/байты с явным `NpgsqlDbType.Jsonb` (без маппинга на `JsonElement` в драйвере). Полный список необходимых `Enable*` для slim-builder фиксируется интеграционным тестом на этапе плана.
- Подключение напрямую или через PgBouncer в transaction mode. Запрещено: `LISTEN`, session-level advisory locks, `SET`, temp tables, опора на session state. Prepared statements выключены по умолчанию (`Max Auto Prepare=0`, `No Reset On Close=true`), включаются опцией `UsePreparedStatements`.
- Каждая операция горячего пути — один SQL-statement (data-modifying CTE). Многошаговые транзакции допустимы только вне горячего пути (cancel, миграции, транзакционный enqueue пользователя).
- Statement'ы горячего пути вне пользовательской транзакции выполняются на уровне изоляции по умолчанию, а их SQL (§5) рассчитан на READ COMMITTED: `ON CONFLICT` по невидимой в снимке строке, перечитывание строки после ожидания `FOR UPDATE`. Требование к развёртыванию: `default_transaction_isolation` для БД и роли PgBolt — `read committed` (значение PostgreSQL по умолчанию). На старте хоста инициализатор, регистрируемый `AddPgBolt`, проверяет `current_setting('transaction_isolation')` на новом соединении и при другом значении падает с понятным сообщением до миграций.
- Целевая нагрузка: 5–20k jobs/sec. Семантика: at-least-once.

## 2. Машина состояний

```
            ┌──────────► Cancelled ◄──────────┐
Awaiting ──►│ Scheduled ⇄ Enqueued ──► Processing ──► Succeeded
            │                ▲              │
            │                └── retry ─────┤ attempt < max_attempts → backoff
            │                               └──► Failed
```

Хранимые состояния (`jobs.state`, `smallint`):

| Код | Состояние | Где живёт |
|---|---|---|
| 0 | Pending | `jobs` + строка в `queue` |
| 1 | Awaiting | только `jobs` (часть 5) |
| 2 | Succeeded | только `jobs` |
| 3 | Failed | только `jobs` |
| 4 | Cancelled | только `jobs` |

Производные состояния для Pending (вычисляются из `queue`, в `jobs` не пишутся):
- **Scheduled** — `lease_owner IS NULL AND NOT ready` (срок `run_at` мог уже наступить: строка ждёт promoter'а, §5.11)
- **Enqueued** — `lease_owner IS NULL AND ready`
- **Processing** — `lease_owner IS NOT NULL`

Следствие: строка `jobs` обновляется за жизнь задачи **один раз** — при переходе в терминальное состояние (плюс редкие ручные действия: requeue). Чтобы это обновление было HOT, действует контракт §4.2: партиции `jobs` создаются с `fillfactor < 100`, и ни один индекс `jobs` не включает изменяемые колонки.

## 3. Идентификаторы

- `JobId` — `readonly record struct` над UUIDv7 (`Guid.CreateVersion7()`), генерируется на клиенте: bulk enqueue без round-trip за id.
- UUIDv7 упорядочен по времени, PostgreSQL сравнивает `uuid` побайтно → `jobs` партиционируется `RANGE (id)` по суточным границам (UTC). Граница дня = 48-битный unix-ms префикс начала суток + нули.
- Серверы идентифицируются `server_id uuid` (каждый виртуальный сервер — свой id).

## 4. Схема

Все объекты в схеме `{schema}` (по умолчанию `pgbolt`, настраивается; имя валидируется как идентификатор и подставляется в SQL один раз при старте).

### 4.1 `queue` — горячая таблица живых задач

```sql
CREATE TABLE {schema}.queue (
    job_id           uuid        PRIMARY KEY,
    queue            text        NOT NULL,
    priority         smallint    NOT NULL DEFAULT 0,   -- больше = важнее
    run_at           timestamptz NOT NULL,
    ready            boolean     NOT NULL,             -- срок наступил и строка видна dequeue (§5.11)
    attempt          int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    lease_owner      uuid,
    lease_id         uuid,                               -- fencing-токен lease, новый при каждом dequeue
    lease_until      timestamptz,
    leased_at        timestamptz,
    unique_key       text,
    cancel_requested boolean     NOT NULL DEFAULT false
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 5000,
        autovacuum_vacuum_cost_limit = 2000);

CREATE INDEX queue_fetch   ON {schema}.queue (queue, priority DESC, run_at) WHERE lease_owner IS NULL AND ready;
CREATE INDEX queue_delayed ON {schema}.queue (run_at)                       WHERE NOT ready;
CREATE INDEX queue_leased  ON {schema}.queue (queue)                        WHERE lease_owner IS NOT NULL;
CREATE INDEX queue_expiry  ON {schema}.queue (lease_until)                  WHERE lease_owner IS NOT NULL;
CREATE UNIQUE INDEX queue_unique ON {schema}.queue (unique_key)             WHERE unique_key IS NOT NULL;
```

- `ready` отделяет отложенные строки от готовых. Индекс dequeue содержит только готовые строки, поэтому стоимость dequeue не зависит от числа отложенных задач и задач в retry-backoff: без флага условие `run_at <= now()` при отсутствии равенства по `priority` не останавливает btree-скан (до PG 18), и при числе готовых строк меньше `@n` — обычном состоянии, когда воркеры успевают, — каждый poll перебирал бы все отложенные строки очереди. `ready` выставляется при enqueue (`run_at <= now()`), retry-fail (`@delay <= 0`), trigger и promoter'ом (§5.11); сбрасывается только retry-fail с задержкой. Строка с `lease_owner IS NOT NULL` всегда `ready`.
- Строка удаляется при переходе задачи в терминальное состояние → таблица остаётся размером «живые задачи» и держится в shared_buffers.
- `max_attempts` дублируется из `jobs`, чтобы sweeper обходился без join.
- `lease_id` — fencing-токен: генерируется `gen_random_uuid()` при каждом dequeue и обнуляется при любом освобождении lease. Все операции над взятой задачей (ack, fail, подтверждение отмены) адресуют её парой `(job_id, lease_id)`, а не `lease_owner`: тот же сервер может повторно взять задачу, lease которой у него забрал sweeper, и тогда `lease_owner` у старой и новой попытки совпадает.
- Инвариант: строка с `cancel_requested = true` и `lease_owner IS NULL` не существует вне транзакции cancel (§5.7). Все пути освобождения lease (retry-fail, NoExecutor, sweeper, освобождение при graceful shutdown в части 3) финализируют такие строки как Cancelled, а не возвращают в очередь.
- Внешних ключей нет (партиционированный `jobs`, стоимость на горячем пути); согласованность `queue`↔`jobs` обеспечивается тем, что обе таблицы меняются в одном statement.
- Высокий churn (insert/delete) → агрессивный autovacuum задан на уровне таблицы; тонкая настройка — часть 2.

### 4.2 `jobs` — определение и итог задачи

```sql
CREATE TABLE {schema}.jobs (
    id            uuid        NOT NULL,
    type          text        NOT NULL,          -- имя задачи из registry
    queue         text        NOT NULL,
    priority      smallint    NOT NULL,
    args          jsonb       NOT NULL,
    state         smallint    NOT NULL,
    attempt       int         NOT NULL DEFAULT 0, -- число попыток на момент терминального перехода
    max_attempts  int         NOT NULL,
    retry_delays  int[],                          -- секунды; NULL = политика по умолчанию
    unique_key    text,
    parent_id     uuid,                           -- часть 5
    batch_id      uuid,                           -- часть 5
    trace_parent  text,                           -- W3C traceparent (часть 7)
    created_at    timestamptz NOT NULL,
    started_at    timestamptz,                    -- начало последней попытки
    finished_at   timestamptz,
    server_id     uuid,                           -- сервер последней попытки
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);
```

- `args` — `jsonb`: позволяет выборки по аргументам в dashboard и массовые операции. **GIN-индекс по умолчанию не создаётся** (дорого на партиционированной таблице с высоким insert rate). Для частых выборок — opt-in частичные expression-индексы, объявляемые в коде (например `IndexArg<OrderArgs>(a => a.TenantId)` → `CREATE INDEX … ((args->>'tenantId')) WHERE type = '…'`); генерация и применение — часть 2.
- **Контракт HOT (обязателен для частей 2, 5, 6):** терминальное обновление `jobs` должно оставаться HOT. Поэтому:
  - каждая партиция `jobs` создаётся `WITH (fillfactor = @jobs_fillfactor)` (по умолчанию 80, опция `JobsFillFactor`); параметр задаётся на партиции, т. к. PostgreSQL не принимает storage-параметры на партиционированном родителе. Без запаса места на странице обновление, приходящее через миллисекунды после вставки в заполненную страницу, пишет новую версию на другую страницу, новую запись PK и полную строку (с inline `args`) в WAL;
  - ни один индекс `jobs` не включает изменяемые колонки (`state`, `attempt`, `started_at`, `finished_at`, `server_id`, в т. ч. в `WHERE` частичного индекса). Разрешены индексы по неизменяемым колонкам (`type`, `queue`, `unique_key`, `parent_id`, `batch_id`, выражения над `args`).
- Выборки dashboard по состоянию (часть 6) строятся не индексом `jobs.state`, а по `queue` (живые задачи), по периодическому rollup или по отдельной узкой insert-only таблице терминальных переходов — выбор в части 6 в рамках контракта выше.

### 4.3 `job_events` — разреженный журнал

```sql
CREATE TABLE {schema}.job_events (
    job_id     uuid        NOT NULL,
    at         timestamptz NOT NULL,
    kind       smallint    NOT NULL,   -- коды ниже
    server_id  uuid,
    attempt    int,
    message    text,
    exception  text
) PARTITION BY RANGE (at);
CREATE INDEX ON {schema}.job_events (job_id);
```

- Пишутся **только** нештатные и ручные события. Счастливый путь (создание, старт, успех) восстанавливается из колонок `jobs`.
- Коды `kind` фиксируются в core (общие для частей 3–6; новые коды только добавляются): 1 Retry, 2 Failed, 3 LeaseExpired, 4 Cancelled, 5 Requeued, 6 Triggered, 7 NoExecutor.
- Поиск событий задачи ограничивается партициями по времени создания задачи: `WHERE job_id = @id AND at >= @created_from`, где `@created_from` клиент вычисляет из 48-битного unix-ms префикса UUIDv7 (в SQL функция для этого не нужна: встроенной поддержки UUIDv7 в PG до 18 нет) и округляет вниз до начала суток UTC минус одни сутки. Запас нужен, потому что id генерируется по часам клиента, а `at` — по часам сервера: при спешащих часах клиента событие может оказаться «раньше» id.

### 4.4 Партиции и retention (контракт для части 2)

- Партиции `jobs` и `job_events` — суточные (UTC). Default-партиции нет (она замедляла бы `ATTACH` и маскировала отсутствие партиции).
- **Создание партиций — в core, не в миграциях.** Версионные миграции создают только партиционированных родителей: скрипт, выполняемый один раз, не может знать будущих дат, а выгруженный при `AutoMigrate = false` скрипт содержал бы фиксированные даты. Core предоставляет идемпотентную операцию `EnsurePartitionsAsync(int? daysAhead = null)` (публичный API `IPgBoltMaintenance`; `null` — `PgBoltOptions.PartitionDaysAhead`, от текущих суток UTC сервера БД):
  - выполняется в одной транзакции под тем же `pg_advisory_xact_lock(hashtext('{schema}'))`, что и миграции (§4.7): иначе при rolling deploy миграция, меняющая `jobs`, могла бы пройти между `CREATE TABLE (LIKE …)` и `ATTACH`, и `ATTACH` упал бы на несовпадении колонок. Читает существующие партиции из `pg_inherits` и создаёт недостающие;
  - каждая партиция создаётся как `CREATE TABLE {schema}.jobs_YYYYMMDD (LIKE {schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = …)`, затем `ALTER TABLE {schema}.jobs ATTACH PARTITION … FOR VALUES FROM (…) TO (…)` (для `job_events` — так же, без `fillfactor`) после `SET LOCAL lock_timeout = '2s'` (допустимо через PgBouncer — действует до конца транзакции). `lock_timeout` выставляется после взятия advisory lock: ожидание параллельного вызова или миграции таймаутом не ограничивается, таймаут относится только к блокировкам DDL на `jobs`/`job_events`. `ATTACH` берёт на родителе `SHARE UPDATE EXCLUSIVE` и не конфликтует с enqueue/ack; `CREATE TABLE … PARTITION OF` взял бы `ACCESS EXCLUSIVE` и поставил бы весь горячий путь в очередь за любым долгим запросом dashboard. Индексы родителя создаются на пустой партиции при `ATTACH` автоматически. При таймауте — повтор позже, это не ошибка;
  - вызывается автоматически при старте клиента (`AddPgBolt`, после миграций) и сервера. Если на старте `lock_timeout` не дал создать партиции: при отсутствии партиции `jobs` или `job_events` на текущие сутки UTC старт падает с понятным сообщением; если нет только будущих — старт продолжается, а core запускает фоновый повтор с backoff до успеха. Периодический вызов и retention — часть 2 (`2026-09-29-pgbolt-storage-maintenance-design.md`). Core-only развёртывание без части 2 обязано вызывать её периодически (раз в сутки достаточно при `daysAhead ≥ 2`).
- Строку `jobs` нельзя перенести в другую партицию без смены id. Поэтому партиция `jobs` удаляется целиком, только если в ней нет задач в состояниях Pending/Awaiting; иначе она **компактируется**: под блокировкой родителя живые строки переносятся в новую партицию тех же суток, старая таблица с терминальными строками удаляется (часть 2 §5.3; при числе живых больше `RetentionCompactionMaxLiveJobs` или публикации `jobs` в логической репликации партиция ждёт ухода живых задач). Проверка «нет Pending/Awaiting» выполняется по индексам (Pending ⇔ строка в `queue`: range scan по PK `queue` в диапазоне id партиции; Awaiting — индекс части 5) до блокировки и повторно под ней. Протокол (часть 2 §5.3–§5.4): `LOCK TABLE ONLY {schema}.jobs IN ACCESS EXCLUSIVE MODE` после `SET LOCAL lock_timeout` и сразу за ним `SET LOCAL statement_timeout`/`idle_in_transaction_session_timeout` (потолок удержания); группа до 10 партиций под одной блокировкой, `DETACH PARTITION` одним пакетом; `DROP TABLE` — отдельной транзакцией на таблицу и только если таблица с тем же `oid` не присоединена (`42P01` — таблицу уже удалил другой под, пропуск); прерванная обработка оставляет остатки, которые удаляет следующий запуск. Без `ONLY` блокировка рекурсировала бы на все партиции и ждала autovacuum сегодняшних; `DROP` в той же транзакции держал бы блокировку родителя на время удаления файлов. Партиции `job_events` удаляются так же, без проверки задач. Повторы ограничены: не больше одной неудачной попытки на таблицу за запуск, запуски — не чаще `RetentionInterval`; алерт — Warning (часть 2 §8), метрика — часть 7.
- Внешние ключи, ссылающиеся на `jobs` и `job_events`, запрещены (обязательно для частей 5–8): `DETACH` проверял бы ссылки сканированием под блокировкой родителя и падал бы с `23503`. Партиции создаёт только PgBolt (`LIKE` родителя), колонки меняются только `ALTER TABLE` родителя, генерируемых колонок в `jobs` нет (компактирование копирует строки `SELECT *`).
- Горизонт партиций: без партиции `jobs` на текущие сутки падает enqueue, а без партиции `job_events` на текущие сутки — fail/sweeper/cancel/trigger/requeue (они пишут события на `now()`; ack событий не пишет), и задачи застревают в lease. Проверка горизонта при старте — `PgBoltInitializer`; периодическое создание — часть 2 (цикл `housekeeping`).

### 4.5 `job_types` — опубликованный registry

```sql
CREATE TABLE {schema}.job_types (
    name                 text PRIMARY KEY,
    args_schema          jsonb       NOT NULL,   -- JSON Schema из JsonSchemaExporter
    default_queue        text        NOT NULL,
    default_max_attempts int         NOT NULL,
    default_priority     smallint    NOT NULL,
    default_retry_delays int[],                  -- секунды из [Job(RetryDelaysSeconds)]; NULL = политика по умолчанию
    published_by         text        NOT NULL,   -- идентификатор процесса-публикатора ({MachineName}/{ProcessId})
    last_seen_at         timestamptz NOT NULL
);
```

Процесс при старте (один раз на процесс, не на виртуальный сервер — часть 3 §4.4) делает upsert своих задач (все `default_*` — из `[Job]`). Если схема аргументов отличается от опубликованной другим живым процессом (`published_by` другой) — предупреждение в лог (last writer wins).

### 4.6 `servers`

```sql
CREATE TABLE {schema}.servers (
    id           uuid PRIMARY KEY,
    name         text        NOT NULL,
    host         text        NOT NULL,
    queues       text[]      NOT NULL,
    capacity     int         NOT NULL,
    started_at   timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL
);
```

### 4.7 `schema_version` и миграции

- Встроенные SQL-скрипты с номерами версий (создают таблицы и партиционированных родителей, но не партиции — §4.4), таблица `{schema}.schema_version(version int PRIMARY KEY, applied_at timestamptz)`.
- Применение в одной транзакции под `pg_advisory_xact_lock(hashtext('{schema}'))` — работает через PgBouncer в transaction mode.
- Опция `AutoMigrate` (по умолчанию `true`); при `false` — API для выгрузки SQL-скрипта, чтобы применить его своим инструментом.
- Версия схемы новее кода (схему поднял более новый релиз при rolling deploy; миграции аддитивны в пределах мажорной версии) — не ошибка, а Warning при старте. Ошибка — только версия меньше `LatestVersion` при `AutoMigrate = false`. `EnsurePartitionsAsync` мигрирует только при `AutoMigrate` и отсутствующей или более старой схеме (часть 2 §4).

## 5. SQL горячего пути

`{schema}` подставлен при старте; все параметры типизированы (`NpgsqlParameter<T>`), без boxing.

### 5.1 Enqueue (одна или много задач)

Поэлементные массивы: `id`, `args`, `run_at` (`NULL` = не задан), `unique_key`. Скаляры на вызов: `type`, `queue`, `priority`, `max_attempts`, `retry_delays`, `trace_parent`, `delay` (`interval`, по умолчанию `0`).

Время выполнения вычисляется на сервере БД, а не по часам клиента: `run_at = COALESCE(run_at, now() + @delay)`. Абсолютный `RunAt` передаётся как есть (это время, явно заданное пользователем); `Delay` и «немедленно» считаются от `now()` сервера — иначе при спешащих часах клиента «немедленная» задача оставалась бы Scheduled на величину расхождения.

```sql
WITH src AS (
    SELECT * FROM unnest(@ids::uuid[], @args::jsonb[], @run_at::timestamptz[], @unique_keys::text[])
             AS t(id, args, run_at, unique_key)
),
existing AS (                              -- уже созданы предыдущей попыткой этого же вызова (§9)
    SELECT id FROM {schema}.jobs WHERE id = ANY(@ids)   -- ANY даёт partition pruning
),
input AS (
    SELECT * FROM src WHERE id NOT IN (SELECT id FROM existing)
),
q AS (
    INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key)
    SELECT id, @queue, @priority, r.run_at, r.run_at <= now(), @max_attempts, unique_key
    FROM input CROSS JOIN LATERAL (SELECT COALESCE(input.run_at, now() + @delay) AS run_at) r
    ORDER BY unique_key, id                -- единый порядок вставки: пакеты с пересекающимися ключами не дают deadlock
    ON CONFLICT DO NOTHING
    RETURNING job_id
),
j AS (
    INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, retry_delays,
                               unique_key, trace_parent, created_at)
    SELECT i.id, @type, @queue, @priority, i.args, 0, @max_attempts, @retry_delays,
           i.unique_key, @trace_parent, now()
    FROM input i JOIN q ON q.job_id = i.id
)
SELECT s.id,
       q.job_id IS NOT NULL AS created,
       e.id     IS NOT NULL AS existed
FROM src s
LEFT JOIN q        ON q.job_id = s.id
LEFT JOIN existing e ON e.id = s.id;
```

Результат по каждой строке (`JobId` генерируется клиентом на вызов, поэтому совпадение по id возможно только с предыдущей попыткой этого же вызова):
- `created = true` → `Id = id, Created = true`.
- `existed = true` → задача уже создана предыдущей попыткой (ответ на неё был потерян) → `Id = id, Created = true`.
- Иначе строка не вставлена из-за конфликта в `queue`: либо по `unique_key`, либо по PK с ещё не видимой в снимке исходной транзакцией (вставка в `queue` дожидается её завершения). Клиент разрешает такие строки одним запросом с новым снимком:

  ```sql
  SELECT s.id, j.id IS NOT NULL AS exists_by_id, q.job_id AS existing_id
  FROM unnest(@ids::uuid[], @unique_keys::text[]) AS s(id, unique_key)
  LEFT JOIN {schema}.jobs  j ON j.id = s.id AND j.id = ANY(@ids)
  LEFT JOIN {schema}.queue q ON q.unique_key = s.unique_key;
  ```
  `exists_by_id` → `Id = id, Created = true`; иначе `existing_id IS NOT NULL` → `Id = existing_id, Created = false` (активная задача с этим ключом); иначе (задача с ключом успела завершиться между запросами) → `Id = null, Created = false`.
- `ON CONFLICT` на `jobs` намеренно нет: если исходная транзакция закоммитилась между снимком `existing` и вставкой в `jobs`, а задача уже успела уйти из `queue`, statement падает с 23505 и откатывается целиком (без осиротевшей строки в `queue`); клиент повторяет процедуру (§9), и новый снимок уже видит задачу в `existing`.
- `ORDER BY unique_key` исключает deadlock только между отдельными statement'ами. Несколько enqueue с `unique_key` внутри одной пользовательской транзакции могут дать 40P01 с конкурентными пакетами; внутри пользовательской транзакции повтора нет (§9), ошибка откатывает транзакцию пользователя. Рекомендация (в документации): ставить задачи с ключами одной транзакцией одним `EnqueueManyAsync`.
- Если передана транзакция (`EnqueueOptions.Transaction`), statement выполняется на её соединении и становится частью пользовательской транзакции. Поддерживается только уровень изоляции **READ COMMITTED**: при REPEATABLE READ / SERIALIZABLE `ON CONFLICT` по строке, невидимой в снимке транзакции, даёт 40001, а запрос разрешения не видит конкурентные строки. Клиент проверяет `IsolationLevel`: явный уровень, отличный от `ReadCommitted`/`Unspecified`, — `NotSupportedException` без обращения к серверу. Иначе клиент перед каждым таким enqueue выполняет `SELECT current_setting('transaction_isolation')` на соединении транзакции и бросает `NotSupportedException`, если это не `read committed`: `NpgsqlTransaction.IsolationLevel` сообщает `ReadCommitted` и для `BeginTransaction()` без уровня, а `SET TRANSACTION ISOLATION LEVEL` внутри транзакции меняет уровень незаметно для него. Результат не кэшируется: Npgsql переиспользует объект `NpgsqlTransaction` на соединении, поэтому кэш по объекту вернул бы результат прошлой транзакции. Цена — один лёгкий round-trip на транзакционный enqueue; отдельный запрос (а не условие внутри statement enqueue) отклоняет транзакцию до любой записи и даёт типизированное исключение. Enqueue без транзакции выполняется на уровне по умолчанию, который обязан быть READ COMMITTED (§1).
- После успешного enqueue вне пользовательской транзакции клиент подаёт in-process wakeup-сигнал (`IWakeupSignal`), если в процессе есть сервер, слушающий эту очередь. При `Transaction != null` сигнал не подаётся: до коммита пользователя задача не видна воркеру, её подберёт polling (часть 3).

### 5.2 Dequeue (по одной очереди; выбор очереди и размер `@n` — часть 3)

```sql
WITH c AS (
    SELECT job_id FROM {schema}.queue
    WHERE queue = @queue AND lease_owner IS NULL AND ready     -- совпадает с предикатом queue_fetch
    ORDER BY priority DESC, run_at
    LIMIT @n
    FOR UPDATE SKIP LOCKED
),
l AS (
    UPDATE {schema}.queue q
    SET lease_owner = @server, lease_id = gen_random_uuid(), lease_until = now() + @lease,
        leased_at = now(), attempt = q.attempt + 1
    FROM c WHERE q.job_id = c.job_id
    RETURNING q.job_id, q.lease_id, q.attempt
)
SELECT j.id, l.lease_id, j.type, j.args, l.attempt, j.max_attempts, j.retry_delays,
       j.parent_id, j.batch_id, j.trace_parent
FROM l JOIN {schema}.jobs j ON j.id = l.job_id;
```

Partition pruning здесь даёт Nested Loop с параметризованным index scan по PK: партиции, не содержащие id, не исполняются (`never executed`). Условие вида `j.id = ANY(ARRAY(SELECT … FROM l))` добавлять нельзя: `ARRAY(SELECT …)` — InitPlan-параметр, `partprune.c` его не использует, и планировщик переходит на Hash Join с просмотром PK всех партиций (проверено `EXPLAIN ANALYZE` на PG 16).

Воркер не делает prefetch сверх свободных слотов: `@n` = число свободных слотов для очереди. Воркер хранит `lease_id` каждой взятой задачи и передаёт его во все последующие операции над ней.

### 5.3 Ack success (пакетный)

```sql
WITH l AS MATERIALIZED (
    SELECT job_id FROM {schema}.queue
    WHERE job_id = ANY(@ids) AND lease_id = ANY(@lease_ids)
    ORDER BY job_id                                -- единый порядок захвата строк (см. ниже)
    FOR UPDATE
),
d AS (
    DELETE FROM {schema}.queue q USING l
    WHERE q.job_id = l.job_id
    RETURNING q.job_id, q.attempt, q.leased_at
)
UPDATE {schema}.jobs j
SET state = 2, attempt = d.attempt, started_at = d.leased_at, finished_at = now(), server_id = @server
FROM d
WHERE j.id = d.job_id AND j.id = ANY(@ids);   -- второе условие даёт partition pruning
```

- `lease_id = ANY(@lease_ids)` — fencing: `lease_id` уникален для каждого lease каждой задачи, поэтому попарное сопоставление с `@ids` не требуется. Если lease потерян (sweeper сбросил его, задачу забрал другой сервер или этот же сервер в другой слот), `lease_id` в строке уже другой и ack для этой попытки игнорируется.
- Порядок захвата строк: все statement'ы, блокирующие несколько строк `queue` без `SKIP LOCKED` (пакетный ack, heartbeat), берут блокировки через `ORDER BY job_id FOR UPDATE` в материализованной CTE. Иначе heartbeat (порядок сканирования по TID) и ack того же сервера (порядок PK либо TID при bitmap scan) на пересекающихся множествах строк взаимно блокируются. Остальные операции либо используют `SKIP LOCKED` (dequeue, sweeper, promoter), либо блокируют одну строку (fail, cancel, trigger).
- Окно пакетирования (до 10–50 мс или N задач) настраивается в части 3. Падение процесса до отправки ack → повторное выполнение (at-least-once).

### 5.4 Fail

Retry (`attempt < max_attempts`), `@delay` вычисляется на воркере из `retry_delays` или политики по умолчанию. Если за время выполнения пришёл запрос отмены, задача не возвращается в очередь, а финализируется как Cancelled:

```sql
WITH l AS MATERIALIZED (
    SELECT job_id, cancel_requested FROM {schema}.queue
    WHERE job_id = @id AND lease_id = @lease_id
    FOR UPDATE                                     -- после ожидания видим актуальный cancel_requested
),
r AS (
    UPDATE {schema}.queue q
    SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL,
        run_at = now() + @delay, ready = (@delay <= interval '0')
    FROM l WHERE q.job_id = l.job_id AND NOT l.cancel_requested
    RETURNING q.job_id, q.attempt
),
c AS (
    DELETE FROM {schema}.queue q USING l
    WHERE q.job_id = l.job_id AND l.cancel_requested
    RETURNING q.job_id, q.attempt, q.leased_at
),
cj AS (
    UPDATE {schema}.jobs j SET state = 4, attempt = c.attempt, started_at = c.leased_at,
                               finished_at = now(), server_id = @server
    FROM c WHERE j.id = c.job_id AND j.id = @id
)
INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message, exception)
SELECT job_id, now(), @kind, @server, attempt, @message, @exception FROM r          -- Retry или NoExecutor
UNION ALL
SELECT job_id, now(), @kind_cancelled, @server, attempt, @message, @exception FROM c;
```

Окончательный fail (`attempt >= max_attempts` или ошибка, не подлежащая retry — например, не удалось десериализовать аргументы):

```sql
WITH d AS (
    DELETE FROM {schema}.queue WHERE job_id = @id AND lease_id = @lease_id
    RETURNING job_id, attempt, leased_at, cancel_requested
),
j AS (
    UPDATE {schema}.jobs
    SET state = CASE WHEN d.cancel_requested THEN 4 ELSE @state END,
        attempt = d.attempt, started_at = d.leased_at, finished_at = now(), server_id = @server
    FROM d WHERE jobs.id = d.job_id AND jobs.id = @id
)
INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message, exception)
SELECT job_id, now(), CASE WHEN cancel_requested THEN @kind_cancelled ELSE @kind END,
       @server, attempt, @message, @exception
FROM d;
```

Для окончательного fail `@state = 3`, `@kind = Failed`. Этот же statement с `@state = 4`, `@kind = Cancelled` — подтверждение отмены воркером (§5.7).

Единое правило для всех путей финализации (окончательный fail, retry-fail, NoExecutor, sweeper): **если запрошена отмена, итоговое состояние — Cancelled**, независимо от исчерпания попыток и причины ошибки. `DELETE … RETURNING` читает строку после блокировки, поэтому видит актуальный `cancel_requested`.

Если у сервера нет исполнителя для `type` (например, при rolling deploy):
- `attempt < max_attempts` → statement retry выше с задержкой и `@kind = NoExecutor` (включая ветку отмены); попытка засчитывается;
- `attempt >= max_attempts` → statement окончательного fail с `@state = 3`, `@kind = NoExecutor` (или Cancelled, если запрошена отмена).

### 5.5 Heartbeat (один statement на сервер, по списку известных lease)

```sql
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
FROM (VALUES (1)) AS one(x) LEFT JOIN u ON true;
```

- Сигнатура: `HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)`; `@ids`/`@lease_ids` — поэлементные массивы из `leases`.
- Продлеваются только lease из списка — выполнения, известные runtime (выполняются или ждут отправки исхода). Строки сервера вне списка («сиротские»: исход брошен по бюджету, dequeue закоммичен без полученного ответа, выполнение брошено при остановке) не продлеваются, истекают и уходят sweeper'у (§5.6). Без фильтра брошенная строка продлевалась бы, пока жив процесс.
- Воркер сопоставляет строки результата со своими выполнениями по `lease_id`; `cancel_requested = true` → отмена `CancellationToken` соответствующего выполнения.
- Правило потери: lease из списка, отсутствующий в ответе, потерян (забран sweeper'ом или освобождён) — результат такого выполнения не подтверждается. Отдельная проверка «dequeue завершился до отправки heartbeat» не нужна: в список попадают только выполнения, зарегистрированные после полного чтения результата dequeue (reader прочитан до конца, получен ReadyForQuery).
- Условие по `job_id` даёт доступ по PK, как у ack; после ожидания блокировки READ COMMITTED перепроверяет `lease_id`, поэтому строка, освобождённая и взятая заново, не продлевается.
- Пустой список: обновляется только `servers.heartbeat_at`.
- Результат — `HeartbeatResult(bool ServerPresent, IReadOnlyList<HeartbeatLease> Leases)`: всегда хотя бы одна строка (при пустом `u` — с `job_id IS NULL`). `ServerPresent = false` — строки `servers` нет (удалена уборкой части 2 после паузы дольше `DeadServerTimeout`), сервер регистрируется заново upsert'ом (часть 2 §9.2).
- Расписание вызовов, таймаут вызова и проверка срока lease каждого выполнения — часть 3 (`2026-09-28-pgbolt-worker-design.md`, §6.2, §8.1).

### 5.6 Sweeper истёкших lease (любой сервер, периодически с jitter)

```sql
WITH e AS (
    SELECT job_id, attempt, max_attempts, lease_owner, cancel_requested FROM {schema}.queue
    WHERE lease_owner IS NOT NULL AND lease_until < now()
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
),
reset AS (
    UPDATE {schema}.queue q SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL
    FROM e WHERE q.job_id = e.job_id AND e.attempt < e.max_attempts AND NOT e.cancel_requested
),
fin AS (                                           -- попытки исчерпаны (Failed) или запрошена отмена (Cancelled)
    DELETE FROM {schema}.queue q USING e
    WHERE q.job_id = e.job_id AND (e.attempt >= e.max_attempts OR e.cancel_requested)
    RETURNING q.job_id, q.attempt, q.cancel_requested
),
f AS (
    UPDATE {schema}.jobs j
    SET state = CASE WHEN fin.cancel_requested THEN 4 ELSE 3 END, attempt = fin.attempt, finished_at = now()
    FROM fin
    WHERE j.id = fin.job_id                        -- pruning через nested loop, см. §5.2
)
INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt)
SELECT job_id, now(), @kind_lease_expired, lease_owner, attempt FROM e
UNION ALL
SELECT job_id, now(), @kind_cancelled, lease_owner, attempt FROM e WHERE cancel_requested;
```

Задача с `cancel_requested = true` никогда не возвращается sweeper'ом в очередь: отмена, запрошенная у упавшего воркера, завершается как Cancelled.

### 5.7 Cancel (транзакция из двух statement'ов)

```sql
-- 1. пометить и заблокировать строку; вернуть актуального владельца
UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id RETURNING lease_owner;

-- 2. только если lease_owner IS NULL: задача не выполняется — отменить сразу
WITH d AS (DELETE FROM {schema}.queue WHERE job_id = @id RETURNING job_id, attempt),
j AS (UPDATE {schema}.jobs SET state = 4, attempt = d.attempt, finished_at = now()
      FROM d WHERE jobs.id = d.job_id AND jobs.id = @id)
INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
SELECT job_id, now(), @kind_cancelled, attempt, @reason FROM d;
```

- Строка заблокирована первым statement'ом до конца транзакции → конкурентный dequeue её пропустит (`SKIP LOCKED`), гонки «отменили, но успели взять» нет.
- Если `lease_owner IS NOT NULL` — отмена доставляется воркеру через heartbeat (задержка ≈ интервал heartbeat). Воркер подтверждает её statement'ом окончательного fail (§5.4) с `@state = 4`, `@kind = Cancelled` и своим `lease_id`. Если до этого выполнение завершилось ошибкой с retry или lease истёк, пути освобождения lease (§5.4 retry/NoExecutor, §5.6 sweeper) сами финализируют задачу как Cancelled — повторного запуска отменённой задачи нет. Если выполнение успело завершиться успешно до доставки отмены, ack фиксирует Succeeded.
- Результат: `NotFound | AlreadyFinished | Cancelled | CancellationRequested`. Если первый statement не затронул строк (задачи нет в `queue`), в той же транзакции читается `SELECT state FROM jobs WHERE id = @id`: нет строки → `NotFound`; состояние 2/3/4 → `AlreadyFinished`; состояние Awaiting (1) — отмена ожидающей задачи определяется в части 5, до её реализации возвращается `NotFound`.
- Повтор при 40P01/40001 (§9) повторяет **всю** транзакцию из двух statement'ов, а не отдельный statement.

### 5.8 Trigger («запустить сейчас»)

```sql
WITH t AS (
    UPDATE {schema}.queue SET run_at = now(), ready = true
    WHERE job_id = @id AND lease_owner IS NULL AND NOT ready
    RETURNING job_id, attempt
)
INSERT INTO {schema}.job_events (job_id, at, kind, attempt)
SELECT job_id, now(), @kind_triggered, attempt FROM t
RETURNING job_id;
```

`TriggerAsync` возвращает `true`, если строка возвращена (задача была Scheduled и стала Enqueued). Задача, уже готовая, выполняемая или отсутствующая в `queue`, → `false`. После успеха клиент подаёт wakeup-сигнал (§5.1).

### 5.9 Requeue (Failed / Cancelled → Pending)

```sql
WITH s AS MATERIALIZED (
    SELECT id, state, queue, priority, max_attempts, unique_key
    FROM {schema}.jobs WHERE id = @id
    FOR UPDATE                                     -- сериализует конкурентные requeue
),
q AS (
    INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key)
    SELECT id, queue, priority, now(), true, max_attempts, unique_key
    FROM s WHERE state IN (3, 4)
    ON CONFLICT DO NOTHING
    RETURNING job_id
),
j AS (
    UPDATE {schema}.jobs SET state = 0, finished_at = NULL
    WHERE id = @id AND id IN (SELECT job_id FROM q)
),
ev AS (
    INSERT INTO {schema}.job_events (job_id, at, kind) SELECT job_id, now(), @kind_requeued FROM q
)
SELECT (SELECT state FROM s) AS state, EXISTS (SELECT 1 FROM q) AS requeued;
```

`FOR UPDATE` в `s` обязателен. Без него два конкурентных requeue читают `state = 3` из одного снимка; если между ними задача успела выполниться и получить ack, вставка второго в `queue` проходит, и задача, уже завершённая Succeeded, выполняется повторно. С блокировкой второй вызов ждёт коммита первого, в READ COMMITTED перечитывает актуальную версию строки (`state = 0`) и возвращает `NotTerminal`.

`RequeueResult`: `requeued` → `Requeued`; `state IS NULL` → `NotFound`; `state NOT IN (3, 4)` → `NotTerminal`; иначе (вставка в `queue` не прошла из-за активной задачи с тем же `unique_key`) → `Conflict`.

### 5.10 Wakeup

In-process сигнал о появлении работы в очереди:

```csharp
public interface IWakeupSignal
{
    void Signal(string queue);

    /// <summary>callback вызывается синхронно из Signal; должен быть дешёвым и не блокировать.</summary>
    IDisposable Subscribe(string queue, Action callback);
}
```

- `InProcessWakeupSignal` (регистрация по умолчанию через `TryAddSingleton`): подписчики по очереди (copy-on-write массив), `Signal` вызывает их синхронно; исключение callback'а перехватывается и не доходит до вызывающего enqueue. Сигнал до подписки не хранится — latch обеспечивает подписчик (событие диспетчера, часть 3 §4.3).
- Сигнал не гарантирован и не выходит за пределы процесса: воркеры страхуются адаптивным polling'ом и сигналом promoter'а (часть 3 §4.3, §6.3). Межпроцессная реализация (например, Redis pub/sub) — пользовательская, заменой регистрации `IWakeupSignal`.
- `LISTEN`/`NOTIFY` исключены из проекта: `LISTEN` держит постоянное прямое соединение в обход PgBouncer на каждый процесс, `NOTIFY` при коммите берёт глобальный lock и на 5–20k/s сериализует коммиты (часть 3 §2).
- Прежний `WaitAsync(queues, timeout, ct)` удалён (breaking change до 1.0): он не сообщал, какая очередь сработала, и терял сигнал, пришедший без ожидающего.

### 5.11 Promoter (Scheduled → Enqueued; любой сервер, периодически)

```sql
WITH p AS (
    SELECT job_id FROM {schema}.queue
    WHERE NOT ready AND run_at <= now()           -- индекс queue_delayed
    ORDER BY run_at
    LIMIT @batch
    FOR UPDATE SKIP LOCKED
)
UPDATE {schema}.queue q SET ready = true
FROM p WHERE q.job_id = p.job_id
RETURNING q.queue;
```

- Выполняется повторно, пока возвращает полный пакет (`@batch`, по умолчанию 1000). Интервал (по умолчанию 1 с с jitter) определяет точность срабатывания отложенных задач и retry с задержкой: задача становится доступной для dequeue не ранее `run_at` и не позднее ≈ `run_at + интервал`. Планирование вызова и адаптивный интервал (по ближайшему `run_at`) — часть 3.
- Возвращённые имена очередей используются для wakeup-сигнала (§5.10).
- `SKIP LOCKED` позволяет запускать promoter на всех серверах одновременно без координации; строки, заблокированные cancel/trigger, будут повышены следующим проходом.
- Отложенная задача, которую отменяют, удаляется независимо от `ready`; sweeper её не касается (`lease_owner IS NULL`).

## 6. Публичный API

### 6.1 Контракт задачи

```csharp
// PgBolt.Abstractions
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class JobAttribute(string name) : Attribute
{
    public string Name { get; } = name;
    public string Queue { get; init; } = "default";
    public int MaxAttempts { get; init; } = 10;
    public short Priority { get; init; }
    public int[]? RetryDelaysSeconds { get; init; }
}

public interface IJob<in TArgs>
{
    Task ExecuteAsync(TArgs args, JobContext context, CancellationToken cancellationToken);
}

public sealed class JobContext
{
    public JobId JobId { get; }
    public string JobName { get; }
    public int Attempt { get; }
    public int MaxAttempts { get; }
    public JobId? ParentId { get; }
    public Guid? BatchId { get; }
    public ILogger Logger { get; }
}
```

Пример:

```csharp
// Orders.Contracts — зависит только от PgBolt.Abstractions
[Job("orders.send-receipt", Queue = "emails", MaxAttempts = 5)]
public sealed record SendReceiptArgs(long OrderId, string Email);

// Сервис-исполнитель
public sealed class SendReceiptJob(IMailer mailer) : IJob<SendReceiptArgs>
{
    public Task ExecuteAsync(SendReceiptArgs args, JobContext ctx, CancellationToken ct) => mailer.SendAsync(...);
}
```

- Имя задачи — на типе аргументов: сервис-постановщик ссылается только на пакет контрактов.
- Правило: один args-тип ↔ одно имя ↔ не более одного исполнителя в сборке.
- `CancellationToken` срабатывает и при отмене задачи, и при shutdown сервера.

### 6.2 Клиент

```csharp
public readonly record struct JobId(Guid Value);
public readonly record struct EnqueueResult(JobId? Id, bool Created);

public enum CancelResult { NotFound, AlreadyFinished, Cancelled, CancellationRequested }
public enum RequeueResult { NotFound, NotTerminal, Conflict, Requeued }

public sealed record EnqueueOptions
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }
    public DateTimeOffset? RunAt { get; init; }
    public TimeSpan? Delay { get; init; }               // взаимоисключимо с RunAt
    public string? UniqueKey { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
    public DbTransaction? Transaction { get; init; }     // транзакционный enqueue; только NpgsqlTransaction, READ COMMITTED
}

public interface IJobClient
{
    ValueTask<EnqueueResult> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<IReadOnlyList<EnqueueResult>> EnqueueManyAsync<TArgs>(
        IReadOnlyList<TArgs> args, EnqueueOptions? options = null,
        Func<TArgs, string?>? uniqueKeySelector = null, CancellationToken ct = default);

    ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<CancelResult>  CancelAsync(JobId id, string? reason = null, CancellationToken ct = default);
    ValueTask<bool>          TriggerAsync(JobId id, CancellationToken ct = default);
    ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default);
    ValueTask<JobInfo?>      GetAsync(JobId id, CancellationToken ct = default);
}
```

Правила параметров:
- Значения `[Job]` — умолчания; любое поле `EnqueueOptions` их переопределяет. Работает одинаково для типизированного и динамического enqueue.
- `Transaction` объявлен как `System.Data.Common.DbTransaction`, чтобы `PgBolt.Abstractions` (и сборки контрактов) не зависели от Npgsql. Реализация клиента требует `NpgsqlTransaction` (иначе `ArgumentException`) и уровень READ COMMITTED (§5.1).
- Для `EnqueueManyAsync` опции общие на вызов; `UniqueKey` в опциях запрещён (используется `uniqueKeySelector`).
- Динамический `EnqueueAsync(jobName, …)`: умолчания (`queue`, `max_attempts`, `priority`, `retry_delays`) берутся из `job_types` (кэш с TTL).
- Типизированный `EnqueueAsync<TArgs>` в обобщённом коде, где `TArgs` — параметр типа, генератор не видит; если для конкретного типа `JobMetadata<TArgs>` не зарегистрирован, клиент бросает `InvalidOperationException` с именем типа и подсказкой пометить тип `[Job]` / сослаться на него из сборки с генератором. Неизвестное имя → `UnknownJobException`. Аргументы на клиенте не валидируются; некорректные аргументы → задача уходит в Failed без retry при десериализации на воркере.
- Retry по умолчанию: экспоненциальный backoff с jitter (точные значения — часть 3).

`JobInfo` — снимок задачи: id, имя, очередь, производное состояние (§2), attempt, даты, `args` (как `JsonElement`), последние события.

### 6.3 Регистрация

```csharp
services.AddPgBolt(o =>
{
    o.DataSource = dataSource;           // или o.ConnectionString
    o.Schema = "pgbolt";
    o.AutoMigrate = true;
    o.UsePreparedStatements = false;
    o.AddJobs(Orders.Jobs.PgBoltJobRegistry.Register);   // сгенерировано в сборке с исполнителями
    o.AddJobs(Billing.Jobs.PgBoltJobRegistry.Register);  // по одному вызову на каждую такую сборку
});
```

- JSON-контекст задаётся только атрибутом сборки `[assembly: PgBoltJsonContext(…)]` (§7); отдельной опции нет.
- Регистрация исполнителей — явный вызов сгенерированного `Register`, а не generated extension-метод: одинаковые extension-методы из нескольких сборок (сервис + библиотека задач + тестовый проект с `InternalsVisibleTo`) давали бы CS0121. Статический класс в корневом namespace сборки однозначно адресуется полным именем. Повторная регистрация той же сборки идемпотентна.

Серверы (`AddPgBoltServer`, `AddPgBoltJobFilter`, `ConfigurePgBoltProcessing`) — часть 3: `2026-09-28-pgbolt-worker-design.md` (§3.1 регистрация, §7.2 SQL регистрации сервера в `servers`, §7.1 освобождение lease при graceful shutdown по `lease_id` с финализацией `cancel_requested`-строк как Cancelled — инвариант §4.1).

## 7. Source generator и анализаторы

Поставляются в пакете `PgBolt.Abstractions`.

**Генерирует:**
- Метаданные для каждого `[Job]`-типа, который объявлен в сборке, реализован исполнителем или используется в вызовах `EnqueueAsync<TArgs>` / `EnqueueManyAsync<TArgs>` (атрибуты читаются и из referenced-сборок): имя, очередь, лимиты. Регистрация через `[ModuleInitializer]` в статический `JobMetadata<TArgs>` — без reflection.
- Registry исполнителей: имя → `JsonTypeInfo<TArgs>` → invoker `(IServiceProvider, ReadOnlyMemory<byte>, JobContext, CancellationToken) => Task` (исполнитель резолвится из DI scope).
- Только в сборках, где есть исполнители: `public static class {RootNamespace}.PgBoltJobRegistry` с методом `public static void Register(IPgBoltJobsBuilder builder)`, который добавляет записи registry и DI-регистрации исполнителей (scoped). `RootNamespace` берётся из MSBuild через `CompilerVisibleProperty`. В сборках, где только объявлены `[Job]`-типы или только вызывается enqueue, класс не генерируется (метаданные `JobMetadata<TArgs>` регистрируются module initializer'ом, см. выше).

**JSON:** генераторы не видят вывод друг друга, поэтому наш генератор не может сам добавить `[JsonSerializable]` для STJ. Пользователь объявляет свой `partial JsonSerializerContext` и указывает его атрибутом сборки `[assembly: PgBoltJsonContext(typeof(AppJsonContext))]`; генератор проверяет покрытие. JSON Schema аргументов строится на старте через `JsonSchemaExporter` из того же `JsonTypeInfo` и публикуется в `job_types`.

**Диагностики:**

| Код | Условие |
|---|---|
| BOLT001 | args-тип, используемый исполнителем или в enqueue, отсутствует в `PgBoltJsonContext` (code fix: добавить `[JsonSerializable]`) |
| BOLT002 | Дубликат имени задачи в компиляции |
| BOLT003 | `IJob<T>`, где `T` не помечен `[Job]` |
| BOLT004 | Больше одного исполнителя для одного args-типа в сборке |
| BOLT005 | Некорректное имя задачи (разрешено: `[a-z0-9._-]`, до 128 символов) |
| BOLT006 | Нет `[assembly: PgBoltJsonContext]` при наличии в сборке исполнителей или вызовов enqueue (сборка, где только объявлены `[Job]`-типы, не требует контекста) |

**Точка расширения для части 8:** registry адресуется по имени и `TArgs`; interceptor будет переписывать `Enqueue<TJob>(j => j.ExecuteAsync(new(…)))` в `EnqueueAsync<TArgs>` без изменения публичного API.

## 8. Пакеты

| Пакет | Содержимое |
|---|---|
| `PgBolt.Abstractions` | `[Job]`, `IJob<T>`, `JobContext`, `IJobClient`, `EnqueueOptions`, генератор и анализаторы. Зависит только от BCL и `Microsoft.Extensions.*.Abstractions` — **не от Npgsql** |
| `PgBolt.Postgres` | Схема, миграции, SQL горячего пути, реализация `IJobClient`, `IWakeupSignal`, `IPgBoltMaintenance` (`EnsurePartitionsAsync`) |
| `PgBolt.Server` | Часть 3 |
| `PgBolt.Dashboard` | Часть 6 |

## 9. Обработка ошибок

- Транзиентные ошибки Npgsql (`NpgsqlException.IsTransient`) на enqueue — не ретраятся автоматически внутри пользовательской транзакции; вне её — одна повторная попытка тем же statement'ом с теми же `JobId`. Идемпотентность обеспечивает сам statement §5.1: строки, уже созданные исходной попыткой, отфильтровываются через `existing` и возвращаются как `Created = true`; исходная транзакция, ещё не видимая в снимке, дожидается через PK `queue` и распознаётся запросом разрешения из §5.1. 23505 по PK `jobs` при повторной попытке (исходная транзакция закоммитилась между снимком и вставкой) — ещё один повтор того же statement'а.
- Deadlock (40P01) и serialization failure (40001) на statement'ах горячего пути вне пользовательской транзакции — повтор statement'а (statement откатывается целиком, повтор безопасен). Порядок захвата строк (§5.3) делает deadlock между ack и heartbeat невозможным; правило — страховка (например, от конкурентных пакетных enqueue с пересекающимися `unique_key`).
- Ack/fail/heartbeat, потерявшие lease (0 затронутых строк или `lease_id` не совпал), не являются ошибкой: результат выполнения отбрасывается, это логируется.
- Ошибки миграции — старт приложения падает с понятным сообщением.

## 10. Известные риски производительности (проверить в части 7)

- **Отложенные задачи** исключены из индекса dequeue флагом `ready` (§4.1). Остаточный риск — мёртвые записи в `queue_fetch` и `queue_delayed` после массового promotion; покрывается autovacuum `queue`.
- **Churn `queue`.** Dequeue, promoter и heartbeat меняют индексируемые колонки → не-HOT обновления; при нагрузке B нужен агрессивный autovacuum (часть 2). Heartbeat переписывает все взятые строки сервера; если бенчмарк покажет это узким местом — альтернатива: liveness на уровне сервера (sweeper ищет строки, чей `lease_owner` имеет устаревший `servers.heartbeat_at`, heartbeat только читает `cancel_requested`, `queue_expiry` не нужен). Это меняет §5.5/§5.6, поэтому решается до реализации части 3.
- **Коммит на задачу.** Одиночные `EnqueueAsync` вне транзакции — по коммиту на вызов. Для 20k/s может понадобиться коалесинг конкурентных одиночных вызовов клиента в один `unnest`-statement (окно ~1 мс); SQL §5.1 это уже поддерживает. Решение — по бенчмарку части 7.
- **Partition pruning при prepared statements.** `partprune.c` отсекает партиции по `= ANY(...)` только если массив — константа. В custom-плане (по умолчанию: unnamed statements) параметр подставлен как константа, и `ANY(@ids)` отсекает партиции при планировании. В generic-плане (`UsePreparedStatements = true`) `ANY($1)` pruning не даёт; остаётся только runtime pruning nested loop по `j.id = d.job_id` (§5.2), если планировщик выберет nested loop. Проверить планы в обоих режимах; при необходимости в prepared-режиме переписать условие на `JOIN unnest(@ids)`.
- **Стоимость `jsonb`** на insert/чтение — единицы процентов CPU PG; принято ради выборок по аргументам.

## 11. Тестирование

- **Генератор и анализаторы:** snapshot-тесты (Verify) сгенерированного кода; тесты каждой диагностики и code fix.
- **Хранилище:** Testcontainers. Весь интеграционный набор прогоняется дважды: напрямую к PostgreSQL и через контейнер PgBouncer в transaction mode (`max_prepared_statements = 0`).
- **SQL-контракт:** на каждую операцию §5 — тесты переходов состояний, в том числе потеря lease, конфликт `unique_key`, requeue с конфликтом (и `NotFound`/`NotTerminal`), cancel взятой и невзятой задачи.
- **Идемпотентный повтор enqueue:** повтор с теми же `JobId`, когда задача ещё в `queue`, когда уже завершилась и удалена из `queue`, и когда исходная транзакция ещё не закоммичена, — всегда `Created = true` с тем же id, без дубликатов и без исключения; с `unique_key` и без.
- **Fencing:** один сервер берёт задачу, sweeper сбрасывает lease, тот же сервер берёт её снова — ack/fail/retry первой попытки не затрагивают вторую, heartbeat не возвращает `lease_id` первой.
- **Отмена и освобождение lease:** при `cancel_requested = true` retry-fail, NoExecutor, окончательный fail и sweeper финализируют задачу как Cancelled; повторного dequeue нет. Cancel завершённой задачи → `AlreadyFinished`.
- **Requeue под гонкой:** два конкурентных requeue одной Failed-задачи — ровно один `Requeued`, второй `NotTerminal`; завершённая после первого requeue задача не возвращается в очередь.
- **Время:** `Delay` и немедленный enqueue считаются от `now()` сервера (тест со сдвигом часов клиента); транзакционный enqueue с REPEATABLE READ → `NotSupportedException`.
- **Slim data source:** весь SQL горячего пути проходит на `NpgsqlSlimDataSourceBuilder` с минимальным набором `Enable*`.
- **Регистрация:** две сборки с исполнителями и тестовый проект с `InternalsVisibleTo` компилируются без CS0121.
- **Блокировки:** параллельные heartbeat и пакетный ack одного сервера на пересекающихся строках под нагрузкой — без 40P01.
- **Конкурентные инварианты:** N параллельных «fetcher'ов» над M задачами — ни одна задача не потеряна; без искусственных падений нет повторных выполнений; уникальность не нарушается при гонке enqueue.
- **Lease:** задача, взятая «убитым» воркером (heartbeat прекращён), возвращается sweeper'ом, попытка засчитана; при исчерпании попыток — Failed.
- **Миграции:** параллельный старт нескольких экземпляров применяет миграции ровно один раз.
- **Партиции:** `EnsurePartitionsAsync` идемпотентна и безопасна при параллельном вызове; созданные партиции `jobs` имеют заданный `fillfactor`; `ATTACH` при открытой долгой читающей транзакции на `jobs` не блокирует enqueue/ack; enqueue через 8 дней после установки проходит при периодическом вызове.
- **HOT:** при фиксированном размере `args` (~200 байт) после ack пакетами, строки которых разбросаны по страницам партиции (одним statement на страницу приходится не больше строк, чем вмещает запас `fillfactor`: при 80 — около пяти новых версий; ack 200 соседних строк одним statement даёт ~30 % HOT, т. к. старые версии той же транзакции ещё не могут быть вычищены), доля HOT-обновлений партиции `jobs` (`pg_stat_user_tables.n_tup_hot_upd / n_tup_upd`) ≥ 95 %; тест-контракт падает, если в `jobs` появляется индекс по изменяемой колонке.
- **Promoter и `ready`:** отложенная задача не берётся dequeue до promotion; retry с задержкой сбрасывает `ready`; trigger делает задачу готовой сразу; `EXPLAIN` dequeue при 1M отложенных строк читает не больше `@n` записей индекса.
- **Изоляция:** транзакционный enqueue в транзакции, переведённой в REPEATABLE READ командой `SET TRANSACTION ISOLATION LEVEL` (`NpgsqlTransaction.IsolationLevel` при этом остаётся `ReadCommitted`), → `NotSupportedException`; старт при `default_transaction_isolation`, отличном от `read committed`, падает до миграций (§1).
- **Deadlock enqueue:** параллельные `EnqueueManyAsync` с пересекающимися `unique_key` в разном порядке — без 40P01.

## Изменения части 5 (Workflows)

Подробности — `2026-10-02-pgbolt-workflows-design.md` (§9).
- §2: `Awaiting` — производное состояние строки `queue` с `await_condition IS NOT NULL` (`jobs.state = 0`); код 1 не используется.
  Scheduled — `lease_owner IS NULL AND NOT ready AND await_condition IS NULL`.
- §4.1: колонки `queue.parent_id`, `await_condition`, `batch_id`, `notify`; индексы `queue_awaiting`, `queue_await_jobs`; инварианты
  ждущей строки (`ready = false`, `run_at = 'infinity'`, без lease). §4.2: `jobs.continuation`, `jobs.notify`, индекс `jobs_batch`.
- §5.1: внутренний вариант enqueue с массивами `parent_id`, `continuation`, `notify`, `batch_id`. §5.2: dequeue читает `continuation`.
- §5.3, §5.4, §5.6, §5.7: statement'ы финализации переносят `notify` в `jobs` и пишут сигнал в `completions` при подписчиках;
  отмена `Awaiting` — обычная отмена строки без lease. §5.8: trigger не трогает `Awaiting`.
- §5.9: requeue переносит `notify` и `batch_id`, у члена batch синхронно уменьшает счётчики `batches` и цепочки внешних.
- §6.1–§6.2: `JobContext.BatchId: BatchId?`, `ParentBatchId`; поля workflow в `JobInfo`; `IJobClient.Workflow`, `CancelBatchAsync`,
  `GetBatchAsync`. §7: метаданные шагов workflow и BOLT009 (BOLT007 занят).

## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§3, §11).
- §4: таблицы `stats_minutes`, `queue_stats`; индексы `queue_awaiting_list`, `job_events_failed`, `job_events_cancelled`,
  `jobs_parent`, `jobs_recurring` (по неизменяемым колонкам — контракт HOT), `batches_parent`. Детектор схемы прошлой сборки
  требует `stats_minutes` и `queue_stats`.
- §5.3: `AckAsync` возвращает подтверждённые `job_id`. Инвариант для всех путей финализации: `jobs.finished_at` и событие
  терминального перехода пишутся одним statement'ом с одним `now()`; requeue обнуляет `finished_at`.

## Изменения части 3b (кластерный лимит и веса очередей)

Подробности — `2026-10-10-pgbolt-queue-limits-design.md` (§3, §10).
- §4.1: индекс `queue_owner (lease_owner)` заменён на `queue_leased (queue)` с тем же предикатом `lease_owner IS NOT NULL`:
  подсчёт взятых строк очереди в лимитированном dequeue — index-only scan, список Processing dashboard с фильтром по очереди —
  range scan; heartbeat идёт по PK, sweeper — по `queue_expiry`.
- §4: таблица `queue_settings` (кластерный лимит и вес очереди: значения по умолчанию из кода и независимые переопределения).
  Детектор схемы прошлой сборки требует и `queue_settings`.
- §5.2: для очередей с кластерным лимитом — лимитированный dequeue (`PgBoltStore.DequeueLimitedAsync`): батч из
  `pg_advisory_xact_lock` и dequeue с подсчётом взятых строк в одной неявной транзакции; dequeue остальных очередей не меняется.
