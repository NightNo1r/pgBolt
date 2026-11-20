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
    cancel_requested boolean     NOT NULL DEFAULT false,
    parent_id        uuid,                                 -- часть 5: родитель Awaiting-строки (задача или batch); не очищается
    await_condition  smallint,                             -- часть 5: NULL — не ждёт; иначе код jobs.continuation (1–5)
    batch_id         uuid,                                 -- часть 5: копия jobs.batch_id (проверка отмены ветки)
    notify           boolean     NOT NULL DEFAULT false    -- часть 5: у задачи есть подписчики (продолжения или batch)
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 5000,
        autovacuum_vacuum_cost_limit = 2000);

CREATE INDEX queue_fetch   ON {schema}.queue (queue, priority DESC, run_at) WHERE lease_owner IS NULL AND ready;
CREATE INDEX queue_delayed ON {schema}.queue (run_at)                       WHERE NOT ready;
CREATE INDEX queue_leased  ON {schema}.queue (queue)                        WHERE lease_owner IS NOT NULL;
CREATE INDEX queue_expiry  ON {schema}.queue (lease_until)                  WHERE lease_owner IS NOT NULL;
CREATE UNIQUE INDEX queue_unique ON {schema}.queue (unique_key)             WHERE unique_key IS NOT NULL;
CREATE INDEX queue_awaiting   ON {schema}.queue (parent_id, job_id) WHERE await_condition IS NOT NULL;
CREATE INDEX queue_await_jobs ON {schema}.queue (parent_id)         WHERE await_condition IN (1, 2);
-- Список Awaiting по job_id DESC (часть 6 §6.1); в индекс попадают только ждущие строки.
CREATE INDEX queue_awaiting_list ON {schema}.queue (job_id) WHERE await_condition IS NOT NULL;
-- Дети ищутся по runtime parent_id (resolver §6.4): оценка планировщика — (1 − null_frac) / n_distinct. Огромный batch,
-- занимающий почти всю таблицу, свёл бы n_distinct по выборке ANALYZE к 1–2 и поиск детей — к Seq Scan / обходу PK.
-- Отрицательное значение масштабируется с таблицей: оценка — не больше 20 строк на родителя при любом перекосе.
ALTER TABLE {schema}.queue ALTER COLUMN parent_id SET (n_distinct = -0.05);

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
    continuation  smallint,                              -- часть 5: 1/2 — родитель-задача, 3/4 — родитель-batch, 5 — Activation
    notify        boolean     NOT NULL DEFAULT false,    -- часть 5: копия queue.notify при финализации; для requeue
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);

CREATE INDEX jobs_batch ON {schema}.jobs (batch_id, id) WHERE batch_id IS NOT NULL;
-- Продолжения-задачи в карточках (часть 6 §6.2), включая уже запущенные: коды 1–2 — родитель-задача, 3–4 — родитель-batch.
-- Члены batch-продолжения (continuation = 5, parent_id = id batch, часть 5 §2.1) в индекс не входят.
CREATE INDEX jobs_parent ON {schema}.jobs (parent_id, id) WHERE continuation IN (1, 2, 3, 4);
-- История запусков расписания (часть 6 §6.1, контракт части 4 §9); recurring_id неизменяем.
CREATE INDEX jobs_recurring ON {schema}.jobs (recurring_id, id) WHERE recurring_id IS NOT NULL;

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
-- Списки Failed / Cancelled (часть 6 §6.1), ключ (at, job_id). Терминальный Failed пишет kind 2 (воркер), 7 (NoExecutor)
-- или 3 (sweeper на последней попытке); 3 и 7 бывают и у нетерминальных переходов — их отсекает проверка jobs.state.
CREATE INDEX job_events_failed    ON {schema}.job_events (at, job_id) WHERE kind IN (2, 3, 7);
CREATE INDEX job_events_cancelled ON {schema}.job_events (at, job_id) WHERE kind = 4;

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

CREATE TABLE {schema}.batches (
    id               uuid        PRIMARY KEY,         -- UUIDv7, генерируется клиентом
    parent_batch_id  uuid,                            -- внешний batch (вложенность)
    parent_id        uuid,                            -- родитель batch-продолжения (задача или batch)
    condition        smallint,                        -- коды jobs.continuation 1–4; NULL, если parent_id IS NULL
    state            smallint    NOT NULL,            -- 0 Active, 1 Awaiting, 2 Succeeded, 3 Failed, 4 Cancelled
    total            int         NOT NULL,            -- члены: задачи + вложенные batches; > 0
    succeeded        int         NOT NULL DEFAULT 0,  -- счётчики могут временно быть ниже факта (§6.6)
    failed           int         NOT NULL DEFAULT 0,
    cancelled        int         NOT NULL DEFAULT 0,
    cancel_requested boolean     NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL,
    finished_at      timestamptz                      -- момент последнего перехода в терминальное состояние
);
CREATE INDEX batches_awaiting   ON {schema}.batches (parent_id, id)       WHERE parent_id IS NOT NULL AND state = 1;
CREATE INDEX batches_await_jobs ON {schema}.batches (parent_id)           WHERE state = 1 AND condition IN (1, 2);
CREATE INDEX batches_nested     ON {schema}.batches (parent_batch_id, id) WHERE parent_batch_id IS NOT NULL;
CREATE INDEX batches_open       ON {schema}.batches (id)                  WHERE state <> 2;
CREATE INDEX batches_finished   ON {schema}.batches (finished_at, id)     WHERE finished_at IS NOT NULL;
-- Batch-продолжения узла в любом состоянии (часть 6 §6.2; batches_awaiting индексирует только ждущие).
CREATE INDEX batches_parent     ON {schema}.batches (parent_id, id)       WHERE parent_id IS NOT NULL;
-- Как queue.parent_id: fan-out из тысяч batch-продолжений одного родителя или вложенных batches одного внешнего.
ALTER TABLE {schema}.batches ALTER COLUMN parent_id SET (n_distinct = -0.05);
ALTER TABLE {schema}.batches ALTER COLUMN parent_batch_id SET (n_distinct = -0.05);

CREATE TABLE {schema}.completions (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id     uuid        NOT NULL,                 -- задача или batch
    kind        smallint    NOT NULL,                 -- 2 Succeeded, 3 Failed, 4 Cancelled, 5 Activated, 6 CancelMembers, 7 Subscribed
    batch_id    uuid,                                 -- только у исхода (2/3/4) члена batch: batch для счётчиков
    counted     boolean     NOT NULL DEFAULT false,   -- Т1 проверил: счётчики применены, дети есть
    stage       smallint    NOT NULL DEFAULT 0,       -- этап обхода детей: 0 задачи, 1 batches
    cursor      uuid,                                 -- позиция порционного обхода внутри этапа
    retry_after timestamptz,                          -- не захватывать раньше: остаток заблокирован другими
    created_at  timestamptz NOT NULL DEFAULT now()
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);
CREATE INDEX completions_count  ON {schema}.completions (id) WHERE kind <> 6 AND NOT counted;
CREATE INDEX completions_walk   ON {schema}.completions (id) WHERE counted;
CREATE INDEX completions_cancel ON {schema}.completions (id) WHERE kind = 6;

CREATE TABLE {schema}.stats_minutes (
    bucket    timestamptz NOT NULL,           -- начало минуты UTC
    server_id uuid        NOT NULL,           -- виртуальный сервер: свои строки, горячих строк нет
    queue     text        NOT NULL,
    succeeded int         NOT NULL DEFAULT 0,
    failed    int         NOT NULL DEFAULT 0, -- терминальный Failed, подтверждённый воркеру (часть 6 §4.1)
    retried   int         NOT NULL DEFAULT 0, -- попытка упала, назначен retry
    PRIMARY KEY (bucket, server_id, queue)
);

CREATE TABLE {schema}.queue_stats (           -- снимок живых состояний (часть 6 §4.2), строка на очередь
    queue       text        PRIMARY KEY,
    scheduled   bigint      NOT NULL,
    enqueued    bigint      NOT NULL,
    processing  bigint      NOT NULL,
    awaiting    bigint      NOT NULL,
    measured_at timestamptz NOT NULL,
    oldest_ready_seconds double precision    -- возраст старейшей готовой задачи на момент снимка; NULL — готовых нет
);

-- Лимит конкурентности и вес очереди на кластере. Значения по умолчанию пишет синхронизация при старте сервера (всегда оба
-- вместе), переопределения — оператор; у лимита и веса переопределения независимы. Эффективное значение:
-- CASE WHEN override_*_set THEN override_* ELSE default_* END; NULL — без лимита (веса). Строки не удаляются.
CREATE TABLE {schema}.queue_settings (
    queue                           text PRIMARY KEY,
    default_max_concurrency         int,
    default_weight                  int,
    default_published_by            text,
    default_updated_at              timestamptz,
    override_concurrency_set        boolean NOT NULL DEFAULT false,
    override_max_concurrency        int,            -- при override_concurrency_set: NULL — без лимита, 0 — пауза, ≥ 1 — лимит
    override_concurrency_updated_by text,
    override_concurrency_updated_at timestamptz,
    override_weight_set             boolean NOT NULL DEFAULT false,
    override_weight                 int,            -- при override_weight_set: NULL — без веса (снять вес из кода), 1…1000 — вес
    override_weight_updated_by      text,
    override_weight_updated_at      timestamptz,
    CHECK (default_max_concurrency IS NULL OR default_max_concurrency >= 1),
    CHECK (override_max_concurrency IS NULL OR override_max_concurrency >= 0),
    CHECK (override_concurrency_set OR override_max_concurrency IS NULL),
    CHECK (default_weight IS NULL OR default_weight BETWEEN 1 AND 1000),
    CHECK (override_weight IS NULL OR override_weight BETWEEN 1 AND 1000),
    CHECK (override_weight_set OR override_weight IS NULL)
);
