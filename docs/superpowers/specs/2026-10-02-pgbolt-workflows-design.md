# PgBolt — часть 5: Workflows (continuations и batches)

Статус: одобрено ревью (4 итерации), реализовано (план `docs/superpowers/plans/2026-10-02-pgbolt-workflows.md`; диагностика §5.4 выпущена как BOLT009 — BOLT007 занят частью 3) · Дата: 2026-10-02 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»), `2026-09-28-pgbolt-worker-design.md` (далее «worker»), `2026-09-29-pgbolt-storage-maintenance-design.md` (далее «часть 2»), `2026-09-30-pgbolt-scheduler-design.md` (далее «часть 4»)

## 1. Цель и рамки

Продолжения (задача запускается после завершения другой задачи или batch) и batches (группа задач с общим исходом) в модели Hangfire, поверх core и worker. Работает при нескольких подах, через PgBouncer в transaction mode, без лидера, `LISTEN` и session state; стоимость горячего пути для задач без зависимостей практически не меняется.

**Входит:**
- узел workflow — задача или batch; у узла не больше одного родителя-узла с условием `OnSuccess` / `OnAnyFinished`;
- batches: атомарное создание, вложенные batches, batch как продолжение, исход batch из исходов членов, отмена batch;
- fluent API `client.Workflow()…SubmitAsync()`, `CancelBatchAsync`, `GetBatchAsync`;
- `Awaiting`-задачи в `queue` (производное состояние) с участием в `unique_key`;
- сигналы завершения (`completions`) и resolver в `PgBolt.Server`;
- изменения core, worker, части 2 и части 4 (§9).

**Не входит:**
- продолжение с несколькими родителями (DAG): fan-in делается через batch;
- добавление задач в уже созданный batch;
- задержка продолжения после срабатывания родителя;
- экраны workflow в dashboard — часть 6 (контракт чтения — §10); метрики — часть 7 (сейчас логи).

### Критерии успеха
- Продолжение запускается или отменяется ровно по правилу §2.2 при любом порядке конкурентных операций (добавление продолжения против ack / fail / sweeper / cancel / requeue родителя и против retention, requeue члена против учёта сигналов batch, отмена batch против запуска членов), падениях процессов и повторах: ни одна задача не остаётся в `Awaiting` после того, как её условие разрешилось; batch не становится терминальным, пока не терминальны все его члены; ждущий член batch не запускается после запроса отмены его batch или внешнего.
- Задержка запуска продолжения при работающем resolver'е — миллисекунды, если родитель финализирован в процессе с resolver'ом (`RunMaintenance = true`, in-process wakeup), иначе не больше `ResolverInterval × 1.2` плюс время проходов. Многошаговый путь (член → batch → продолжение, каскады) не добавляет интервалов: проход, создавший сигналы, повторяется сразу (§6.4).
- Финализация задачи без подписчиков не вставляет строк в `completions`; план dequeue не меняется (добавляется только чтение `j.continuation`, §6.3), heartbeat и promoter не меняются.
- Счётчик batch не является горячей строкой: не больше одного обновления счётчиков строки `batches` на batch за проход resolver'а (плюс синхронное уменьшение при ручном requeue члена, §6.6).
- Fan-out любого размера (активация batch-продолжения, каскадная отмена, отмена batch) обрабатывается порциями ограниченного размера; проход resolver'а ограничен бюджетом строк, а число его statement'ов не зависит от числа захваченных сигналов.
- Пауза горячего пути от retention остаётся пропорциональной `T` живых (часть 2 §5.3): источники живых строк части 5 под блокировкой родителя ограничены `T + 1` строками.

## 2. Модель

### 2.1 Узлы и связи

- **Задача-продолжение:** `jobs.parent_id` — id родителя (задачи или batch; оба UUIDv7, коллизий нет), условие и вид родителя — `jobs.continuation`.
- **Член batch:** `jobs.batch_id` — id batch. Член может одновременно быть продолжением другой задачи того же batch (цепочка внутри batch).
- **Batch:** строка `batches`. Вложенный batch — член внешнего (`parent_batch_id`) и считается одним членом. Batch-продолжение — `batches.parent_id` + `condition` (условие и вид родителя, коды как у `jobs.continuation`).
- **Член batch-продолжения** ждёт активации своего batch: `parent_id = batch_id`, условие `Activation`. Вложенный batch внутри batch-продолжения тоже ждёт активации (`state = Awaiting`, `parent_id IS NULL`).
- Граф одного workflow — лес деревьев; циклы невозможны: родитель всегда существует раньше ребёнка.

### 2.2 Правило по исходу родителя

| Исход родителя | OnSuccess | OnAnyFinished |
|---|---|---|
| Succeeded | запуск | запуск |
| Failed | ждёт (requeue родителя) | запуск |
| Cancelled | каскадная отмена | запуск |

- Failed в PgBolt не окончателен (requeue, core §5.9): OnSuccess-продолжение ждёт, пока родителю не сделают requeue и он не завершится успехом, или пока продолжение не отменят. Для retention такой родитель — живая строка (§7.1).
- Отмена — явное намерение «не выполнять»: OnSuccess-продолжения отменяются по цепочке.
- Решение принимается по исходу родителя, актуальному на момент обработки его сигнала (§6.4): если родителю уже сделали requeue, устаревший исход к детям не применяется — они ждут следующего. Уже запущенные и отменённые дети не пересматриваются: requeue отменённого родителя их не оживляет.
- Родитель-batch: его исход (§2.3) подставляется в ту же таблицу.

### 2.3 Исход batch

Batch терминален, когда терминальны все его члены (`succeeded + failed + cancelled = total`). Исход: есть Cancelled → Cancelled; иначе есть Failed → Failed; иначе Succeeded.
- Failed- и Cancelled-batch, как и задача, не окончательны: requeue члена возвращает batch (и терминальные внешние) в Active синхронно (§6.6), исход вычисляется заново при следующем завершении. Продолжения, уже запущенные или отменённые, не пересматриваются.
- Член, ждущий Failed-родителя внутри batch, не терминален: batch остаётся Active, пока родителю не сделают requeue или член не отменят.
- Отмена batch (§6.5) не отменяет уже выполняющиеся члены мгновенно: член, успевший завершиться успехом, учитывается как Succeeded (как ack против cancel в core §5.7). Член (задача или вложенный batch), ждущий родителя или активации, после запроса отмены своего batch или любого внешнего не запускается, а отменяется (§6.4).

### 2.4 Состояния

`Awaiting` — производное состояние (как Scheduled): строка в `queue` с `await_condition IS NOT NULL`. `jobs.state` у `Awaiting`-задачи — 0 (Pending); код 1 больше не используется (изменение core §2). Переход `Awaiting → Enqueued` — одно обновление строки `queue`, без записи в `jobs`.

Состояния `batches.state` (коды совпадают с `jobs.state` там, где смысл общий): 0 Active, 1 Awaiting, 2 Succeeded, 3 Failed, 4 Cancelled.

## 3. Принятые решения

| Вопрос | Решение | Причина |
|---|---|---|
| Модель зависимостей | Один родитель (задача или batch); fan-in — через batch | Совпадает с Hangfire и с зарезервированными `jobs.parent_id` / `batch_id`; без таблицы рёбер и счётчика родителей у ребёнка |
| Где живёт `Awaiting` | В `queue`: `ready = false`, `run_at = 'infinity'`, `await_condition` | `unique_key` (`queue_unique`), retention (range scan по PK `queue`), `CancelAsync` и выборки живых задач работают без изменений; запуск — `UPDATE queue`, без второго обновления `jobs`. Цена — `queue` содержит и ждущие строки (§11) |
| Доставка завершения | Флаг `notify` на строке `queue` + сигнал в `completions` из statement'а финализации; обработка — resolver отдельной транзакцией | Statement финализации видит снимок на свой старт: строку зависимости, вставленную конкурентной транзакцией, он не увидит даже после ожидания её блокировки. Флаг на строке, которую финализация блокирует, читается в актуальной версии (`DELETE … RETURNING` после ожидания); resolver читает детей свежим снимком |
| Подписка на batch | Строка batch — `FOR KEY SHARE`; сигнал `Subscribed`, resolver читает состояние batch сам (§6.2) | Все обновления `batches` берут `FOR NO KEY UPDATE`, совместимую с `KEY SHARE`: учёт, requeue и отмена не ждут даже пользовательскую транзакцию. Конфликтует только удаление retention'ом — batch не удаляется из-под подписки |
| Устаревший сигнал | Исход узла перечитывается в том же statement'е, что выборка его детей; несовпадение с сигналом — сигнал удаляется (§6.4) | Один снимок: видимый в нём ребёнок подписан до любого requeue, не видимого в нём. Без блокировок узла и порядка блокировок с requeue |
| Отмена ветки batch | Запуск ждущего члена проверяет `cancel_requested` его batch и внешних; при отмене — отмена вместо запуска | Исход не зависит от того, в какой порции `CancelMembers` оказался член |
| Синхронный запуск в ack | Нет | Два механизма вместо одного, тяжелее ack, счётчик batch в ack — горячая строка |
| Requeue члена batch | Счётчики batch (и цепочки внешних) уменьшаются синхронно в транзакции requeue | Отложенный учёт отрицательной дельты позволял бы resolver'у учесть следующий исход члена раньше неё и завершить batch преждевременно, с необратимым каскадом. Requeue редкий и ручной |
| Сканер без сигналов | Нет | O(всех ожидающих) за проход; batch на миллион задач перечитывался бы каждый тик |
| Failed-родитель | Продолжение ждёт; родитель для retention живой | Решение владельца продукта: requeue после починки должен довести цепочку; стоимость хранения ограничивает компактирование части 2 |
| Retention членов batch | Failed- и Cancelled-члены живы, пока существует строка их batch; строки `batches` удаляются сверху вниз (§7.1) | В Failed / Cancelled член попадает только из `queue`, а строка batch не появляется снова, поэтому живость членов только убывает — под блокировкой родителя её не перепроверять; подписка на любой существующий Failed-batch находит его членов для requeue. Цена — Cancelled-члены хранятся вместе со строкой batch |
| Cancelled-родитель | Каскадная отмена OnSuccess | Отмена цепочки или batch одним вызовом |
| Расширения batches | Вложенные batches и batch-продолжения — да; добавление в существующий batch — нет | Дерево создаётся атомарно, `total` неизменен, гонки «append против последнего ack» нет |
| API | Fluent workflow-построитель, одна транзакция на `SubmitAsync` | Цепочки и ветвления читаются естественно; один способ вместо `EnqueueOptions.After` + `CreateBatchAsync` |
| Продолжение к завершённому родителю | Ребёнок вставляется как `Awaiting`, плюс синтетический сигнал с исходом родителя | Одна логика решения (resolver) для всех случаев; не нужно вычислять каскад на клиенте |
| Миграция | Объекты добавляются в V001, детектор пре-релизной схемы части 4 расширяется | Пользователей нет (часть 4 §3) |

## 4. Схема (добавляется в `V001__initial.sql`)

```sql
-- в CREATE TABLE {schema}.queue (core §4.1) добавляются колонки:
--     parent_id        uuid,       -- родитель Awaiting-строки (задача или batch); NULL вне ожидания не очищается
--     await_condition  smallint,   -- NULL — не ждёт; иначе код jobs.continuation (1–5)
--     batch_id         uuid,       -- копия jobs.batch_id: проверка отмены ветки при запуске (§6.4)
--     notify           boolean NOT NULL DEFAULT false   -- у задачи есть подписчики (продолжения или batch)
CREATE INDEX queue_awaiting   ON {schema}.queue (parent_id, job_id) WHERE await_condition IS NOT NULL;
CREATE INDEX queue_await_jobs ON {schema}.queue (parent_id)         WHERE await_condition IN (1, 2);   -- §7.1
-- Дети ищутся по runtime-значению parent_id (resolver §6.4): при преобладании одного родителя (огромный batch-продолжение) оценка
-- (1 − null_frac) / n_distinct по выборке ANALYZE уводит поиск в Seq Scan. Отрицательное значение масштабируется с таблицей.
ALTER TABLE {schema}.queue ALTER COLUMN parent_id SET (n_distinct = -0.05);

-- в CREATE TABLE {schema}.jobs (core §4.2) добавляются колонки:
--     continuation  smallint,                       -- NULL; 1 OnSuccess задачи, 2 OnAnyFinished задачи,
--                                                   -- 3 OnSuccess batch, 4 OnAnyFinished batch, 5 Activation
--     notify        boolean NOT NULL DEFAULT false   -- копия queue.notify при финализации; для requeue
CREATE INDEX jobs_batch ON {schema}.jobs (batch_id, id) WHERE batch_id IS NOT NULL;

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
CREATE INDEX batches_await_jobs ON {schema}.batches (parent_id)           WHERE state = 1 AND condition IN (1, 2);  -- §7.1
CREATE INDEX batches_nested     ON {schema}.batches (parent_batch_id, id) WHERE parent_batch_id IS NOT NULL;
CREATE INDEX batches_open       ON {schema}.batches (id)                  WHERE state <> 2;                       -- §7.1
CREATE INDEX batches_finished   ON {schema}.batches (finished_at, id)     WHERE finished_at IS NOT NULL;   -- §7.1
-- Как queue.parent_id: fan-out из тысяч batch-продолжений одного родителя или вложенных batches одного внешнего.
ALTER TABLE {schema}.batches ALTER COLUMN parent_id SET (n_distinct = -0.05);
ALTER TABLE {schema}.batches ALTER COLUMN parent_batch_id SET (n_distinct = -0.05);

CREATE TABLE {schema}.completions (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id     uuid        NOT NULL,                 -- задача или batch
    kind        smallint    NOT NULL,                 -- 2 Succeeded, 3 Failed, 4 Cancelled, 5 Activated,
                                                      -- 6 CancelMembers, 7 Subscribed (§6)
    batch_id    uuid,                                 -- только у исхода (2/3/4) члена batch: batch для счётчиков
    counted     boolean     NOT NULL DEFAULT false,   -- Т1 проверил: счётчики применены, дети есть (§6.4)
    stage       smallint    NOT NULL DEFAULT 0,       -- этап обхода детей: 0 задачи, 1 batches (§6.4, §6.5)
    cursor      uuid,                                 -- позиция порционного обхода внутри этапа
    retry_after timestamptz,                          -- не захватывать раньше: остаток заблокирован другими
    created_at  timestamptz NOT NULL DEFAULT now()
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);
CREATE INDEX completions_count  ON {schema}.completions (id) WHERE kind <> 6 AND NOT counted;   -- захват Т1
CREATE INDEX completions_walk   ON {schema}.completions (id) WHERE counted;                     -- захват Т2
CREATE INDEX completions_cancel ON {schema}.completions (id) WHERE kind = 6;                    -- CancelMembers
```

- Инварианты `queue`: `await_condition IS NOT NULL` ⇒ `ready = false`, `run_at = 'infinity'`, `lease_owner IS NULL`. `await_condition` и `jobs.continuation` пишутся одним кодом, `queue.batch_id = jobs.batch_id` (enqueue и requeue). `notify = true` у каждого члена batch и у каждой задачи, к которой когда-либо добавляли продолжение. Ложноположительный `notify` безвреден: лишний сигнал без детей.
- Сигналы `Activated`, `CancelMembers`, `Subscribed` и синтетические (§6.2) пишутся с `batch_id = NULL`: в счётчики попадают только исходы членов, задач и вложенных batches.
- `queue_await_jobs` и `batches_await_jobs` содержат только ожидания родителей-задач (коды 1, 2): по ним retention находит живых родителей без id batch (§7.1).
- Колонки `jobs.continuation`, `jobs.parent_id`, `jobs.batch_id` пишутся один раз; `jobs.notify` меняется, но не индексируется — контракт HOT (core §4.2) сохраняется. Индекс `jobs_batch` — по неизменяемым колонкам.
- Индексы детей упорядочены по id (`queue_awaiting`, `batches_awaiting`, `batches_nested`): порционный обход идёт курсором, без чтения и сортировки всех детей на каждую порцию.
- `queue_delayed` (core §4.1) включает `Awaiting`-строки с `run_at = 'infinity'`; promoter сканирует диапазон `run_at <= now()` и до них не доходит. `queue_fetch` их не содержит (`ready = false`).
- `batches` и `completions` не партиционированы; внешних ключей нет (core §4.4).
- Три частичных индекса `completions` делят сигналы по захвату (`counted` выставляет только Т1, захватывающий `kind <> 6`, поэтому `counted` ⇒ `kind <> 6`): захват Т1 не читает сигналы многопроходного fan-out, отложенные (`retry_after` пишут только Т2 и `CancelMembers`) и `CancelMembers`; захват Т2 — неучтённые и `CancelMembers`. Выставление `counted` не HOT (колонка предиката) — допустимо: один раз на сигнал; `stage`, `cursor`, `retry_after` не индексируются. Мёртвые версии и отложенные сигналы Т2 в голове `completions_walk` — риск §11.
- Детектор пре-релизной схемы (часть 4 §3) дополнительно проверяет `to_regclass('{schema}.batches') IS NULL`; сообщение то же.

## 5. Публичный API

### 5.1 Контракты (`PgBolt.Abstractions`)

```csharp
public readonly record struct BatchId(Guid Value);
public enum ContinuationCondition { OnSuccess, OnAnyFinished }

public interface IJobClient
{
    // … существующие методы core §6.2 …
    IWorkflow Workflow(WorkflowOptions? options = null);
    ValueTask<BatchCancelResult> CancelBatchAsync(BatchId id, CancellationToken ct = default);
    ValueTask<BatchInfo?>        GetBatchAsync(BatchId id, CancellationToken ct = default);
}

public sealed record WorkflowOptions { public DbTransaction? Transaction { get; init; } }

public sealed record StepOptions          // EnqueueOptions без Transaction
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }
    public DateTimeOffset? RunAt { get; init; }
    public TimeSpan? Delay { get; init; }
    public string? UniqueKey { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}

public interface IWorkflowScope           // общий набор для workflow и тела batch
{
    IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null);
    IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null);
    IBatchStep StartBatch(Action<IBatchBuilder> build);
}

public interface IWorkflow : IWorkflowScope
{
    INodeStep After(JobId existing);
    INodeStep After(BatchId existing);
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);
}

public interface IBatchBuilder : IWorkflowScope
{
    IReadOnlyList<IJobStep> EnqueueMany<TArgs>(IReadOnlyList<TArgs> args, StepOptions? options = null);
}

public interface INodeStep
{
    IJobStep ContinueWith<TArgs>(TArgs args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
                                 StepOptions? options = null);
    IJobStep ContinueWith(string jobName, JsonElement args,
                          ContinuationCondition condition = ContinuationCondition.OnSuccess, StepOptions? options = null);
    IBatchStep ContinueWithBatch(Action<IBatchBuilder> build,
                                 ContinuationCondition condition = ContinuationCondition.OnSuccess);
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);   // делегирует workflow
}

public interface IJobStep   : INodeStep { JobId Id { get; } }
public interface IBatchStep : INodeStep { BatchId Id { get; } }

public sealed record WorkflowResult(IReadOnlyList<JobId> Jobs, IReadOnlyList<BatchId> Batches);

public enum BatchCancelResult { NotFound, AlreadyFinished, CancellationRequested }
public enum BatchState { Active, Awaiting, Succeeded, Failed, Cancelled }

public sealed record BatchInfo(BatchId Id, BatchState State, int Total,
    int Succeeded, int Failed, int Cancelled,       // ≥ 0; временно могут быть ниже факта (§6.6)
    BatchId? OuterBatchId,                          // внешний batch (batches.parent_batch_id)
    JobId? ParentId, BatchId? ParentBatchId,        // родитель batch-продолжения: задача или batch, как в JobInfo
    ContinuationCondition? Condition, bool CancellationRequested,
    DateTimeOffset CreatedAt, DateTimeOffset? FinishedAt);

public sealed class ParentNotFoundException : InvalidOperationException { public Guid ParentId { get; } }
public sealed class UniqueKeyConflictException : InvalidOperationException
{
    public string UniqueKey { get; }
    public JobId? ExistingId { get; }
}
```

Пример:

```csharp
// линейная цепочка с fan-out
await client.Workflow()
    .Enqueue(new FetchArgs(1))
    .ContinueWith(new ParseArgs())
    .ContinueWithBatch(b => b.EnqueueMany(items))
    .ContinueWith(new ReportArgs(), ContinuationCondition.OnAnyFinished)
    .SubmitAsync(ct);

// ветвление: шаги — ссылки на узлы общего графа
var wf    = client.Workflow(new WorkflowOptions { Transaction = tx });
var fetch = wf.Enqueue(new FetchArgs(1));
fetch.ContinueWith(new ParseArgs()).ContinueWith(new IndexArgs());
fetch.ContinueWith(new AuditArgs(), ContinuationCondition.OnAnyFinished);
await wf.SubmitAsync(ct);

// продолжение существующего узла; корневой batch с вложенностью
await client.Workflow().After(existingJobId).ContinueWith(new CleanupArgs()).SubmitAsync(ct);
await client.Workflow()
    .StartBatch(b =>
    {
        b.Enqueue(new FetchArgs(1)).ContinueWith(new MergeArgs());   // цепочка внутри batch
        b.StartBatch(inner => inner.Enqueue(new ResizeArgs(2)));      // вложенный batch
    })
    .ContinueWith(new ReportArgs())
    .SubmitAsync(ct);
```

### 5.2 Правила

- Построитель синхронный и не потокобезопасный: шаги только описывают граф, id (`JobId`, `BatchId` — UUIDv7) генерируются при создании шага и доступны до `SubmitAsync`. `ContinueWith` / `ContinueWithBatch` у шага внутри batch создаёт члена того же batch (задачу или вложенный batch-продолжение); у шага корневого batch — продолжение batch вне его.
- **Области.** Шаг принадлежит своему workflow и своей области (корень или тело конкретного batch); его методы добавляют узлы туда, где бы их ни вызвали. Область batch открыта только на время выполнения его лямбды (`StartBatch` / `ContinueWithBatch`): после её возврата вызовы методов `IBatchBuilder` и методов шагов-членов этого batch → `InvalidOperationException` (иначе менялся бы закрытый batch). Методы самого `IBatchStep` (продолжение batch вне его) доступны, пока открыта его внешняя область. После вызова `SubmitAsync` (успешного или нет) любые методы workflow и его шагов → `InvalidOperationException`.
- Параметры задачи: значения `[Job]` (или `job_types` для динамического шага) — умолчания, `StepOptions` их переопределяет (core §6.2).
- `RunAt` / `Delay` разрешены только у корневых задач workflow и у членов batch без родителя (корневого или вложенного в такой). У продолжений и членов batch-продолжений — `ArgumentException`: они стартуют в момент разрешения условия.
- `UniqueKey` разрешён везде, кроме членов batch (`ArgumentException`: конфликт изменил бы `total`). Префикс `pgbolt:` зарезервирован (часть 4 §4.4) — `ArgumentException`. Одинаковый ключ у двух шагов графа — `ArgumentException` при создании второго шага. Конфликт с активной задачей откатывает записи workflow (§6.1) и бросает `UniqueKeyConflictException` с ключом и id активной задачи (`null`, если она успела завершиться — core §5.1).
- `After(id)` на несуществующий узел (или удалённый retention'ом) → `ParentNotFoundException`, записи workflow откатываются (§6.1). Вид узла определяется перегрузкой: `After(JobId)` с id batch (и наоборот) — тоже `ParentNotFoundException`.
- В пользовательской транзакции любое исключение `SubmitAsync` откатывает записи workflow до точки сохранения (§6.1): транзакция пользователя остаётся рабочей и не содержит строк графа.
- Пустой batch (в том числе вложенный) → `ArgumentException`. Workflow без шагов и `After(id)` без продолжений → `ArgumentException`. Повторный `SubmitAsync` → `InvalidOperationException`.
- `Transaction` — те же требования, что core §5.1: `NpgsqlTransaction`, READ COMMITTED, проверка уровня изоляции. `After` на задачу, которая ещё в `queue`, удерживает блокировку её строки до коммита пользовательской транзакции, на терминальную — блокировку строки `jobs`: до этого коммита Pending-родитель не берётся в работу, `RequeueAsync` родителя ждёт, а отмена его batch стоит на нём (§6.2, §6.5): XML-doc `WorkflowOptions.Transaction` требует коммитить сразу после `SubmitAsync`.
- Workflow из одной корневой задачи записывает то же, что `EnqueueAsync`, но конфликт `UniqueKey` — `UniqueKeyConflictException`, а не `Created = false`.
- `After(BatchId)` с OnSuccess-продолжением к Failed-batch ждёт requeue его Failed-членов; они (и Cancelled-члены) хранятся, пока существует строка batch (§7.1).
- `RequeueAsync` члена batch (core §5.9, §6.6): XML-doc описывает возврат batch и терминальных внешних в Active и requeue Cancelled-члена при идущей отмене batch — флаг отмены не сбрасывается, член выполняется или отменяется снова в зависимости от позиции обхода `CancelMembers`, ждущие члены ветки не запускаются; детерминированно — после того, как batch стал терминальным.
- После коммита вне пользовательской транзакции клиент подаёт wakeup очередей корневых готовых задач (core §5.10) и resolver'а, если записаны синтетические сигналы (§6.1).

### 5.3 Изменения существующих типов (breaking до 1.0)

- `JobContext.BatchId`: `Guid?` → `BatchId?`; добавляется `ParentBatchId: BatchId?`. `ParentId: JobId?` заполняется только для родителя-задачи (`continuation` 1, 2); `ParentBatchId` — для родителя-batch (3, 4). У членов batch-продолжения (5) оба `null`, `BatchId` заполнен.
- `JobInfo` получает `ParentId: JobId?`, `ParentBatchId: BatchId?`, `BatchId: BatchId?`, `Continuation: ContinuationCondition?` (`continuation` 1 / 3 → `OnSuccess`, 2 / 4 → `OnAnyFinished`, 5 и `NULL` → `null`; `ParentId` / `ParentBatchId` — как в `JobContext`, у кода 5 оба `null`); `State = Awaiting` для строк `queue` с `await_condition IS NOT NULL`, `RunAt = null` у них.
- В API `ParentBatchId` везде — родитель-batch продолжения; вложенность batch в `BatchInfo` — `OuterBatchId` (колонка `batches.parent_batch_id`).
- `EnqueueOptions` не меняется.

### 5.4 Генератор и анализаторы

- Вызовы `IWorkflowScope.Enqueue<TArgs>`, `INodeStep.ContinueWith<TArgs>` и `IBatchBuilder.EnqueueMany<TArgs>` распознаются так же, как `EnqueueAsync<TArgs>` (core §7): регистрация `JobMetadata<TArgs>`, BOLT001, BOLT006.
- **BOLT009** (Warning; в черновике спеки — BOLT007, занят частью 3): результат `IJobClient.Workflow()` (или шаг, полученный от него), сохранённый в локальную переменную или использованный цепочкой, ни один шаг которого не передан дальше и не дошёл до `SubmitAsync` в том же методе. Эвристика: возврат из метода, передача аргументом или присваивание полю/свойству подавляют диагностику. Шаги, созданные в лямбде-теле `StartBatch` / `ContinueWithBatch` (включая элементы списка `EnqueueMany`), — шаги того же workflow; тело, переданное группой методов, не прослеживается.

## 6. Протоколы

### 6.1 Запись workflow (`SubmitAsync`)

Одна транзакция (пользовательская или своя). В пользовательской перед первой записью — `SAVEPOINT pgbolt_workflow` (savepoint живёт внутри транзакции, PgBouncer transaction mode его поддерживает); любое исключение до конца шага 5 → `ROLLBACK TO SAVEPOINT pgbolt_workflow` и проброс исключения (в том числе после SQL-ошибки, прервавшей транзакцию); успех → `RELEASE SAVEPOINT`. Своя транзакция при ошибке откатывается целиком.
1. **Batches:** `INSERT INTO batches` одним `unnest` для всех batch графа: `state = 1` у batch-продолжений и у вложенных в них (на любой глубине), иначе 0; `total` — число прямых членов.
2. **Задачи:** statement enqueue core §5.1 с дополнительными поэлементными массивами `parent_id`, `continuation` (он же `await_condition`), `notify`, `batch_id` (в `queue` и `jobs`); группировка по `(type, queue, priority, max_attempts, retry_delays)`, пакеты до 10 000 строк, все — одним `NpgsqlBatch` при возможности. Строки с `await_condition IS NOT NULL` пишутся с `ready = false`, `run_at = 'infinity'`. `notify = true` у членов batch и у задач, у которых в графе есть дети. Колонка `jobs.notify` при вставке равна `queue.notify`.
3. **Конфликты:** строка без `created` и без `existed` (core §5.1) → запрос разрешения core §5.1: `exists_by_id` — предыдущая попытка этого же вызова (правило повтора ниже); иначе конфликт `unique_key` → `UniqueKeyConflictException`, откат.
4. **Внешние родители** (`After`) — протокол §6.2 для каждого, в порядке id. Подписка — последняя запись транзакции: блокировки строк родителей держатся только до коммита (дети к этому моменту уже вставлены как `Awaiting`, порядок на корректность не влияет).
5. **Синтетические сигналы** для внешних родителей (§6.2).

Повтор при транзиентной ошибке вне пользовательской транзакции (core §9): транзакция повторяется с теми же id. Признак закоммиченной предыдущей попытки — существует первая строка графа (`batches` по id первого batch, иначе `jobs` по id первой задачи); проверяется перед повтором. Предыдущая попытка может быть ещё в полёте: тогда повтор её не видит, его вставка ждёт её коммита и получает 23505 по PK `batches` / `jobs` или строку `exists_by_id` на шаге 3. В обоих случаях транзакция откатывается и проверка первой строки повторяется новой транзакцией: есть → `SubmitAsync` возвращает успех; нет → обычный повтор.

### 6.2 Подписка на существующего родителя

Шаг 4 §6.1, для каждого `After(id)`. Родитель-задача:
1. `UPDATE {schema}.queue SET notify = true WHERE job_id = @id RETURNING 1` — строка есть (Pending или Awaiting) → готово.
2. Иначе в точке сохранения: `SAVEPOINT pgbolt_after; UPDATE {schema}.jobs SET notify = true WHERE id = @id AND state IN (2, 3, 4) RETURNING state`. Строка есть → `RELEASE SAVEPOINT pgbolt_after`, на шаге 5 §6.1 пишется сигнал `(node_id = @id, kind = state)`; resolver применит правило §2.2. Ноль строк → `ROLLBACK TO SAVEPOINT pgbolt_after`.
3. Ноль строк → `SELECT state FROM {schema}.jobs WHERE id = @id` без блокировки: строка есть (0 — requeue после снимка п. 1; 2 / 3 / 4 — финализация после п. 2) → повтор с п. 1; нет строки → `ParentNotFoundException` (в том числе для id существующего batch: `After(JobId)` ищет только задачи, иначе id batch попал бы в `queue_await_jobs` и в источник (а) retention'а как задача, §7.1, а `JobContext.ParentId` получил бы id batch).

Родитель-batch (только `After(BatchId)`; задачи не ищутся):

4. `SELECT 1 FROM {schema}.batches WHERE id = @id FOR KEY SHARE`: строка есть → на шаге 5 §6.1 пишется сигнал `(node_id = @id, kind = 7 Subscribed)`; нет → `ParentNotFoundException`.

Почему гонки нет:
- Финализация (§6.3) блокирует строку `queue` родителя. Если п. 1 взял её первым, финализация ждёт коммита workflow, после ожидания `DELETE … RETURNING` читает актуальную версию с `notify = true` и пишет сигнал; resolver обрабатывает его позже свежим снимком и видит детей. Если первой была финализация, п. 1 ждёт её коммита, не находит строку, п. 2 видит терминальное состояние.
- Requeue (core §5.9) блокирует строку `jobs` `FOR UPDATE`. Если п. 2 был первым, requeue ждёт коммита подписки, видит `jobs.notify = true` и переносит его в `queue.notify`. Если первым был requeue, п. 2 ждёт его коммита; `UPDATE` в READ COMMITTED блокирует новую версию строки и только затем перепроверяет условие (`state = 0` — не выполнено), а блокировка остаётся до конца (под)транзакции. Откат к точке сохранения её снимает, и п. 3 возвращается к п. 1, не держа строку `jobs`: иначе ack, `CancelAsync` или `CancelMembers`, держащий строку `queue` перезапущенной задачи и ждущий её `jobs`, против п. 1, ждущего эту строку `queue`, дали бы 40P01 (в пользовательской транзакции не повторяется).
- П. 2 держит строку `jobs` только терминальной задачи — её строки в `queue` нет, ack / fail / heartbeat её не держат; поэтому цикла «подписка держит `jobs`, ждёт `queue` ↔ ack держит `queue`, ждёт `jobs`» нет. Несколько `After` берут строки `queue` в порядке id — как пакетный ack (core §5.3).
- Batch: `KEY SHARE` совместима с `FOR NO KEY UPDATE` учёта, requeue и отмены batch и конфликтует только с удалением строки retention'ом (§7.1). Удаление, взявшее строку первым, подписка дожидается и не находит строку → `ParentNotFoundException`; пока подписка не закоммичена, удаление пропускает строку (`SKIP LOCKED`); после её коммита ждущих детей видит проверка удаления, выполняемая отдельным statement'ом после блокировки (§7.1). Сигнал `Subscribed` коммитится вместе с детьми; resolver обрабатывает его транзакцией, начатой после коммита, и читает `batches.state` (§6.4). Терминальный — правило §2.2 с этим исходом. Нетерминальный — сигнал удаляется: переход batch в терминальное состояние закоммитится позже этого чтения, а значит, позже коммита детей, и проход, обрабатывающий его сигнал, детей увидит. Если batch завершился раньше и его сигнал обработан до коммита детей, их разрешит `Subscribed`. Повторная обработка безопасна: запущенные и отменённые дети условию больше не удовлетворяют.

**Блокировка в пользовательской транзакции.** Строка `queue` родителя, найденная п. 1, заблокирована до коммита: в своей транзакции — на время записи сигналов и `COMMIT`; в пользовательской — до коммита или отката пользователя. Всё это время ждут heartbeat, ack и fail сервера, выполняющего родителя, а также cancel / trigger родителя; dequeue и promoter строку пропускают (`SKIP LOCKED`), поэтому Pending-родитель не берётся в работу (и Scheduled не повышается) до коммита пользователя. Если родитель — член batch, `CancelMembers` этого batch стоит на его строке (§6.5). Heartbeat, ждущий дольше `HeartbeatInterval / 2`, обрывается по таймауту (worker §6.2); если ожидание дольше `LeaseDuration − HeartbeatInterval`, сервер переходит в «heartbeat потерян» и отменяет все свои выполнения. Пакетный ack ждёт целиком. Строка `jobs` терминального родителя (п. 2) держится так же: `RequeueAsync` родителя ждёт коммита пользователя. `KEY SHARE` строки batch (п. 4) задерживает только удаление retention'ом, который её пропускает. Resolver такие строки не ждёт (§6.4). Документируется в XML-doc `WorkflowOptions.Transaction` и `After` (§5.2, §11).

### 6.3 Финализация задачи (изменения core)

Все statement'ы, удаляющие строку из `queue` с терминальным исходом — ack (core §5.3), окончательный fail и подтверждение отмены (§5.4), ветка отмены retry-fail / NoExecutor (§5.4), sweeper (§5.6), cancel (§5.7), освобождение при shutdown (worker §7.1), — получают одинаковое дополнение. Пример для ack:

```sql
WITH l AS MATERIALIZED (
    SELECT job_id FROM {schema}.queue
    WHERE job_id = ANY(@ids) AND lease_id = ANY(@lease_ids)
    ORDER BY job_id
    FOR UPDATE
),
d AS (
    DELETE FROM {schema}.queue q USING l
    WHERE q.job_id = l.job_id
    RETURNING q.job_id, q.attempt, q.leased_at, q.notify
),
j AS (
    UPDATE {schema}.jobs j
    SET state = 2, attempt = d.attempt, started_at = d.leased_at, finished_at = now(), server_id = @server,
        notify = j.notify OR d.notify
    FROM d
    WHERE j.id = d.job_id AND j.id = ANY(@ids)
    RETURNING j.id, j.batch_id, d.notify
),
s AS (
    INSERT INTO {schema}.completions (node_id, kind, batch_id)
    SELECT id, 2, batch_id FROM j WHERE notify
    RETURNING 1
)
SELECT (SELECT count(*) FROM j) AS acked, (SELECT count(*) FROM s) AS signals;
```

- `kind` сигнала — итоговое состояние (2 / 3 / 4, с учётом `cancel_requested`).
- `signals > 0` → store подаёт in-process сигнал resolver'у (`ICompletionSignal`, §6.4).
- Statement'ы, освобождающие lease без финализации (retry без отмены, reset sweeper'а, release без отмены), не меняются: строка остаётся в `queue` вместе с `notify`.
- Dequeue (core §5.2) дополнительно читает `j.continuation`. Trigger (core §5.8) получает условие `AND await_condition IS NULL`. Heartbeat и promoter не меняются.

### 6.4 Resolver

Цикл `resolver` в `ProcessingMaintenance` (worker §6.3) — при `RunMaintenance = true`, как promoter и sweeper, на всех таких подах; интервал `ResolverInterval` (1 с, jitter 20 %), дополнительно будится `ICompletionSignal` (in-process, синглтон `PgBolt.Postgres`, как `IWakeupSignal`). Итерация — учёт (Т1), обход (Т2), затем `CancelMembers` (§6.5); этап без работы пропускается. Итоговый `SELECT` Т1 дополнительно возвращает `EXISTS` по предикатам захватов Т2 и `CancelMembers` (пробы частичных индексов `completions_walk` и `completions_cancel`, в снимке statement'а): Т2 выполняется, только если Т1 передал ему сигналы (`walking > 0`) или в снимке есть готовые к обходу; `CancelMembers` — только если в снимке есть готовые `kind = 6` или Т2 этой итерации создал сигналы. Сигналы, закоммиченные другими подами после снимка Т1, берут их собственные Т2 / `CancelMembers` (финализация будит resolver своего процесса) или следующая итерация: задержка — не больше `ResolverInterval`. Итерация, разбуженная таймером (не сигналом и не немедленным повтором), сначала одним запросом проверяет, есть ли сигнал, который взял бы один из захватов (те же предикаты, `EXISTS` по каждому частичному индексу); нет — Т1, Т2 и `CancelMembers` не выполняются, в простое под делает один запрос на интервал (проверка лага §8 — как обычно). Следующая итерация начинается сразу, без паузы, если итерация захватила полный пакет, создала новые сигналы, исчерпала бюджет строк или выполнила хотя бы одну порцию `CancelMembers`.

Захват сигналов везде — `ORDER BY id … FOR UPDATE SKIP LOCKED` с условием `retry_after IS NULL OR retry_after <= now()`, по частичному индексу своего вида (`completions_count` — Т1, `completions_walk` — Т2, `completions_cancel` — `CancelMembers`, §4). Сигнал, чей остаток заблокирован другими транзакциями, получает `retry_after = now() + ResolverInterval`: он не повторяется без паузы и не занимает голову очереди.

**Т1 — учёт** (один statement и `COMMIT`; кроме `CancelMembers`, единственное место, где resolver ждёт строки `batches`, — поэтому транзакция короткая и отделена от обхода):
1. Захват до 500 сигналов `kind <> 6 AND NOT counted` (индекс `completions_count`).
2. Счётчики для сигналов с `batch_id IS NOT NULL`: дельты (2 / 3 / 4 → +1 к `succeeded` / `failed` / `cancelled`) группируются по `batch_id`; строки `batches` блокируются `ORDER BY id FOR NO KEY UPDATE` в материализованной CTE (как ack, core §5.3) и обновляются одним `UPDATE … RETURNING`. Batch с `succeeded + failed + cancelled = total` и `state IN (0, 1)` получает `state` по §2.3, `finished_at = now()` и сигнал со своим исходом (`batch_id = parent_batch_id`). Строки batch нет (удалена retention'ом, §7.1) — дельта отбрасывается, лог Debug.
3. Дети — одной проверкой по всем захваченным узлам: `EXISTS` по `queue_awaiting` (`parent_id = node`), `batches_awaiting` и, для `Activated`, `batches_nested`, с условиями по `kind` (п. Т2.3, у `Subscribed` — любые). Снимок statement'а взят после коммита сигналов, поэтому ребёнок, закоммиченный позже, получит свой сигнал (§6.2).
4. Сигналы без детей удаляются (основная масса — исходы листовых членов batch); остальные получают `counted = true` и дальше обрабатываются только Т2: сигналы с многопроходным fan-out не занимают голову захвата Т1.

**Т2 — обход детей** (одна транзакция, бюджет 5000 строк детей; statement'ы — на этап, а не на узел):
1. Захват до 500 сигналов `counted` (индекс `completions_walk`; `counted` ⇒ `kind <> 6`).
2. **Исход узла** читается в каждом statement'е выборки детей, в том же снимке: `jobs.state` (по PK, pruning по `id = ANY(@nodes)`) или `batches.state`. Сигнал 2 / 3 / 4 применяется, только если состояние узла равно `kind`; `Subscribed` — если `batches.state` терминален (исход — он); `Activated` — всегда. Иначе (родителю или члену batch сделали requeue, строки нет) сигнал удаляется: устаревший исход к детям не применяется, следующий исход даст новый сигнал. Ребёнок, видимый в снимке, закоммичен раньше любого requeue, не видимого в нём, — поэтому гонки «проверили → requeue → подписка» нет и узел не блокируется.
3. **Дети-задачи** (`stage = 0`) — один statement на все узлы этапа:
   ```sql
   … FROM n CROSS JOIN LATERAL (
       SELECT job_id, await_condition, batch_id FROM {schema}.queue
       WHERE parent_id = n.node AND await_condition = ANY(n.conds) AND job_id >= n.cursor   -- без курсора: условие опускается
       ORDER BY job_id LIMIT n.lim
       FOR UPDATE SKIP LOCKED) c
   ```
   `n.conds` по исходу: Succeeded → {1, 2, 3, 4}; Failed → {2, 4}; Cancelled → {1, 2, 3, 4} (OnSuccess 1 / 3 — отмена, OnAnyFinished 2 / 4 — запуск); Activated → {5}. `n.lim = min(1000, max(1, остаток бюджета / число узлов))`; statement повторяется для незавершённых узлов, пока есть бюджет. Обработанные строки условию больше не удовлетворяют (запущены или удалены), поэтому курсор включающий: `cursor` = последний `job_id` порции.
   - **Отмена ветки:** для детей с `batch_id` тот же statement проверяет `cancel_requested` их batch и внешних (рекурсивный CTE вверх по `parent_batch_id` от различных `batch_id` порции). Отмена запрошена — ребёнок отменяется вместо запуска (§2.3). Так же обрабатываются члены `Activated`-batch, которому успели запросить отмену.
   - Запуск: `UPDATE queue SET await_condition = NULL, run_at = now(), ready = true` → wakeup очередей после коммита.
   - Отмена: statement cancel core §5.7 п. 2 для набора строк (с дополнением §6.3), событие `Cancelled` с сообщением `parent {id} cancelled` или `batch {id} cancelled`. Отменённые с `notify` дают новые сигналы.
   - Узел, чья порция меньше `n.lim`, дошёл до конца. Строки, пропущенные `SKIP LOCKED` (подписка §6.2 в открытой транзакции, `CancelAsync`, `CancelMembers`, другой resolver), ищет один запрос по всем таким узлам — первая строка тем же предикатом без курсора и без блокировки. Нет → `stage = 1`, `cursor = NULL`; есть → `cursor` = её id и `retry_after`. Ожидания чужих транзакций нет.
4. **Дети-batches** (`stage = 1`) — так же, один statement на все узлы этапа (`id >= cursor ORDER BY id LIMIT lim FOR NO KEY UPDATE SKIP LOCKED`), с той же проверкой конца:
   - сигналы 2 / 3 / 4 / 7: `batches_awaiting` — `parent_id = node AND state = 1 AND NOT cancel_requested AND condition = ANY(n.conds)`. Запуск — `state = 0` и сигнал `Activated`; отмена — `cancel_requested = true` и сигнал `CancelMembers`;
   - `Activated`: `batches_nested` — `parent_batch_id = node AND parent_id IS NULL AND state = 1 AND NOT cancel_requested` → `state = 0` и свой `Activated`. Вложенный batch-продолжение (`parent_id IS NOT NULL`) ждёт своего родителя, а не активации внешнего;
   - отмена ветки — как в п. 3, от `parent_batch_id` ребёнка: вместо активации — `cancel_requested = true` и `CancelMembers`;
   - batch с собственным `cancel_requested` не трогается: его члены отменяет идущий `CancelMembers`, исход — Cancelled по счётчикам.
5. **Завершение** — один statement: сигналы, прошедшие оба этапа без остатка, удаляются; остальные получают `stage`, `cursor` и при остатке из-за чужих блокировок — `retry_after`. Сигналы, созданные в проходе, обрабатываются следующими проходами (свежий снимок).

### 6.5 Отмена batch

`CancelBatchAsync` — один statement:
```sql
WITH b AS MATERIALIZED (
    SELECT id, state, cancel_requested FROM {schema}.batches WHERE id = @id FOR NO KEY UPDATE
),
u AS (
    UPDATE {schema}.batches t SET cancel_requested = true FROM b
    WHERE t.id = b.id AND b.state IN (0, 1) AND NOT b.cancel_requested
    RETURNING t.id
),
s AS (INSERT INTO {schema}.completions (node_id, kind) SELECT id, 6 FROM u RETURNING 1)
SELECT b.state, EXISTS (SELECT 1 FROM s) AS signalled FROM b;
```
Нет строки → `NotFound`; `state` терминален → `AlreadyFinished`; иначе `CancellationRequested` (повторная отмена — без нового сигнала: `b` читает прежний `cancel_requested` под блокировкой). Новый сигнал → in-process `ICompletionSignal`. Члены отменяются с сообщением `batch {id} cancelled`.

Resolver для `CancelMembers` — короткими транзакциями, чтобы блокировки строк выполняющихся членов не держались весь проход. Транзакция: `SET LOCAL lock_timeout = '1s'` (одним `NpgsqlBatch` с захватом), захват одного сигнала `kind = 6` (индекс `completions_cancel`), до 1000 строк его обхода — этап 0, затем в той же транзакции этап 1, если порция не исчерпана. За итерацию — транзакции подряд, сигнал за сигналом, пока есть готовые сигналы и бюджет (5000 строк или 100 транзакций). Таймаут блокировки — откат и `retry_after = now() + ResolverInterval` у этого сигнала отдельным statement'ом; итерация продолжается со следующего сигнала. Поэтому строка члена, удерживаемая дольше `lock_timeout` (`After` на члена в долгой пользовательской транзакции, §6.2), останавливает обход всего batch: остальные его члены отменяются после её освобождения (ждущие до того не запускаются, §6.4 Т2.3, а Pending и выполняющиеся продолжают работу); другие сигналы `CancelMembers` идут. `SKIP LOCKED` здесь не применяется: пропущенную строку пришлось бы искать повторным обходом всех членов (индекса `queue` по `batch_id` нет). Документируется вместе с `After` (§5.2).
- `stage = 0`: члены по `jobs_batch` (`batch_id = @node AND id > @cursor ORDER BY id LIMIT …`, без блокировки `jobs`); для их строк в `queue` — семантика core §5.7 набором: `cancel_requested = true` под `ORDER BY job_id FOR UPDATE` (порядок ack и heartbeat), невзятые удаляются с исходом Cancelled и сигналом (§6.3), выполняющимся отмена доставляется heartbeat'ом.
- `stage = 1`: вложенные batches по `batches_nested` (`parent_batch_id = @node AND id > @cursor ORDER BY id LIMIT …`, `FOR NO KEY UPDATE` в порядке `id`) с `state IN (0, 1) AND NOT cancel_requested` и в снимке statement'а, и в последней версии под блокировкой получают `cancel_requested = true` и свой `CancelMembers`; закрытые в снимке не блокируются, курсор и размер порции считаются по всем вложенным.
- Каждый statement порции в том же снимке читает `cancel_requested` своего узла (`batches` по PK, без блокировки): `false` или строки нет — порция ничего не меняет, сигнал удаляется. Иначе requeue члена, вернувший batch в Active со сбросом флага (§6.6), откатила бы следующая порция: отменила бы перезапущенного члена за курсором (этап 0) или вложенный batch, вернувшийся в Active (этап 1). Requeue, закоммиченный после снимка порции, ей не мешает: его новая строка `queue` в снимке не видна, а batch, который он вернул в Active, в снимке терминален и фильтр этапа 1 не проходит.
- `cursor` сдвигается; неполная порция завершает этап; сигнал удаляется после этапа 1. Исход batch вычисляется счётчиками (§2.3). Порядок обхода на исход не влияет: ждущих членов ветки с запрошенной отменой resolver не запускает (§6.4 Т2.3).

### 6.6 Requeue (изменение core §5.9)

`s` дополнительно читает `notify`, `batch_id`; вставка в `queue` пишет `notify = s.notify OR s.batch_id IS NOT NULL`, `batch_id = s.batch_id`. Строка `jobs` заблокирована `FOR UPDATE`, поэтому `notify`, выставленный конкурентной подпиской (§6.2), виден.

Член batch (`requeued` и `batch_id IS NOT NULL`): в той же транзакции счётчики уменьшаются синхронно, сигнала нет.
1. Цепочка batch члена и его внешних (рекурсивный CTE по неизменяемому `parent_batch_id`) блокируется `ORDER BY id FOR NO KEY UPDATE` с чтением `state` — порядок как у учёта resolver'а (§6.4 Т1).
2. Снизу вверх, начиная с batch члена, с `@k = s.state`:
   ```sql
   UPDATE {schema}.batches
   SET failed = failed - (@k = 3)::int, cancelled = cancelled - (@k = 4)::int,
       state = CASE WHEN state IN (2, 3, 4) THEN 0 ELSE state END,
       cancel_requested = cancel_requested AND state NOT IN (2, 3, 4),
       finished_at = NULL
   WHERE id = @batch;
   ```
   Если batch был терминальным (по `state` из п. 1), подъём продолжается: внешний batch уменьшает счётчик с `@k` = прежнее состояние вложенного. Нетерминальный batch или отсутствие строки (удалена retention'ом, §7.1) завершают подъём; лога нет — requeue вызывается клиентом без логгера, лог Debug об удалённой строке пишет resolver при учёте сигнала (§6.4 Т1). `cancel_requested` сбрасывается при выходе из терминального состояния, чтобы batch можно было отменить снова; незавершённый `CancelMembers` такого batch по сброшенному флагу останавливается (§6.5).

**Requeue при идущей отмене.** Batch члена нетерминален и `cancel_requested = true` (отмена запрошена, не все члены терминальны): подъём на нём останавливается, флаг не сбрасывается, `CancelMembers` продолжается. Исход перезапущенного члена зависит от позиции обхода: член с id не больше `cursor` этапа 0 (или сигнал уже на этапе 1 либо удалён) обход прошёл — член выполняется; член впереди курсора следующая порция отменяет снова (невзятый — сразу, выполняющийся — через heartbeat). Ждущие члены ветки (продолжения перезапущенного члена внутри batch) по-прежнему не запускаются, а отменяются (§6.4 Т2.3). Так же вложенный batch, вернувшийся в Active под нетерминальным внешним с `cancel_requested`: этап 1 `CancelMembers` внешнего, ещё не дошедший до него, снова ставит ему `cancel_requested` и `CancelMembers`; прошедший — оставляет его Active, но ждущие члены его ветки отменяются проверкой по флагу внешнего. Детерминированный перезапуск — requeue после завершения отмены: batch терминален, флаг сбрасывается (п. 2). Поведение описывается в XML-doc `RequeueAsync` (§5.2).

Почему корректно: сигнал предыдущего завершения члена может быть ещё не учтён resolver'ом, тогда −1 применяется раньше +1 и счётчик временно ниже факта (в том числе отрицателен). Выше факта он не бывает: вклад члена в сумму (учтённые завершения минус requeue) не больше 1 и равен 1, только когда член терминален и все его сигналы учтены; для вложенного batch то же по индукции. Поэтому `succeeded + failed + cancelled = total` ⇒ все члены терминальны и учтены, исход по счётчикам точен. Сигнал исхода batch, вернувшегося в Active, к его детям уже не применяется (§6.4 Т2.2). Повтор при 40P01 / 40001 (core §9) повторяет всю транзакцию requeue.

Requeue отменённого продолжения ставит его сразу в Pending (`await_condition` не восстанавливается): явное ручное действие.

### 6.7 Отмена `Awaiting`-задачи (изменение core §5.7)

`CancelAsync` работает без изменений: строка в `queue` без lease удаляется вторым statement'ом (с дополнением §6.3). Ветка «состояние Awaiting (1) → `NotFound`» удаляется: код 1 не используется.

## 7. Обслуживание

### 7.1 Retention (изменение части 2 §5.3)

Живые строки партиции `jobs` `[lo, hi)`, кроме строк `queue` в диапазоне (без изменений; включают `Awaiting`):
- **(а) родители-задачи с ждущими детьми** (в том числе Failed-родители OnSuccess-продолжений): различные `parent_id` в `[lo, hi)` по `queue_await_jobs` и по `batches_await_jobs`. Индексы содержат только ожидания родителей-задач, поэтому id batch в набор не попадают; различные значения — loose index scan, не больше `T + 1` спусков по индексу на источник:
  ```sql
  WITH RECURSIVE p(id) AS (
      (SELECT parent_id FROM {schema}.queue
       WHERE await_condition IN (1, 2) AND parent_id >= @lo AND parent_id < @hi ORDER BY parent_id LIMIT 1)
      UNION ALL
      SELECT (SELECT parent_id FROM {schema}.queue
              WHERE await_condition IN (1, 2) AND parent_id > p.id AND parent_id < @hi ORDER BY parent_id LIMIT 1)
      FROM p WHERE p.id IS NOT NULL
  )
  SELECT id FROM p WHERE id IS NOT NULL LIMIT @limit;
  -- то же по batches: state = 1 AND condition IN (1, 2)
  ```
- **(б) Failed- и Cancelled-члены существующих batches.** У Succeeded-batch таких членов нет (счётчики терминального batch точны, §6.6; requeue Succeeded-задачи невозможен), поэтому обходятся batch с `state <> 2`:
  ```sql
  SELECT j.id FROM {schema}.batches b                                    -- batches_open
  CROSS JOIN LATERAL (SELECT id FROM {schema}.jobs_YYYYMMDD              -- партиция кандидата по имени, jobs_batch
                      WHERE batch_id = b.id AND id >= @lo AND id < @hi AND state IN (3, 4)) j
  WHERE b.state <> 2
  LIMIT @limit;
  ```
  Читается партиция кандидата по имени, а не родитель `jobs`: чтение родителя ждало бы `ACCESS EXCLUSIVE` от `DETACH` другого пода.
  Живость (б) со временем только убывает: строка batch не появляется снова, а в Failed / Cancelled задача попадает только из `queue` — член, ставший живым по (б) после предпроверки, был в ней строкой `queue`. Поэтому Cancelled-члены живы наравне с Failed: иначе requeue Cancelled-члена Cancelled-batch и его окончательный fail между предпроверкой и блокировкой дали бы живой член, которого нет ни в одном наборе, и OnSuccess-продолжение batch ждало бы requeue удалённой задачи.

Предпроверка (часть 2 §5.3 п. 1) считает `L` по объединению `queue`, (а) и (б), каждый источник — `LIMIT @limit`; больше `T` → `Deferred(LiveJobs)`, пусто → отсоединение. Первый пакет: `lock_timeout`, сверка oid партиции, оба запроса (а). Второй пакет, после сверки oid: `queue` и (б) — одним statement'ом (`UNION ALL`, у каждой ветки свой `LIMIT`) и повторная сверка oid; `42P01` (партицию удалил другой под) → `Gone`. Общий снимок `queue` и (б) нужен для монотонности (б): член, ставший живым по (б) после снимка, был в нём строкой `queue` — при раздельных снимках requeue между ними мог бы скрыть его от обеих выборок. (б) выполняется только в предпроверке, вне блокировки (стоимость — спуск по `jobs_batch` на batch из `batches_open` плюс члены таких batches в партиции). Под блокировкой повторяются только ограниченные запросы: `queue` (как в части 2) и (а) — подписка на старую терминальную задачу могла закоммититься после предпроверки; при отсоединении — `EXISTS` по тем же трём диапазонам. Массив `@ids` компактирования (и прогрева) — объединение набора предпроверки и повторной проверки: строки, переставшие быть живыми, копируются лишними — безвредно; больше `T` → `Deferred(LiveJobs)`. Удержание растёт не больше чем на `2 × (T + 1)` спусков по индексам — граница части 2 сохраняется.

**Строки `batches`** удаляются в задаче `retention` (часть 2 §6.2) пакетами по 1000, транзакцией из трёх statement'ов. Первый — окно кандидатов: `SELECT id, finished_at FROM {schema}.batches WHERE finished_at < @threshold AND (finished_at, id) > (@f, @id) ORDER BY finished_at, id LIMIT 1000` — чистый range scan по `batches_finished` без блокировок и проверок; `@threshold` = `now() - JobsRetention` фиксируется на запуск. Курсор `(@f, @id)` — последняя строка окна, живёт в пределах запуска, каждый запуск начинает с начала; окно короче 1000 завершает запуск. Так каждая строка читается запуском один раз, независимо от того, сколько кандидатов отсеяно проверками или пропущено `SKIP LOCKED`; к ней возвращается следующий запуск. Второй блокирует кандидатов окна с проверками третьего statement'а (только отсев заведомо живых), порог повторяется: строка, перешедшая после окна через requeue и завершившаяся снова, моложе `JobsRetention`:
```sql
SELECT b.id FROM {schema}.batches b
WHERE b.id = ANY(@window) AND b.finished_at < @threshold AND b.state IN (2, 3, 4)
  AND NOT EXISTS (SELECT 1 FROM {schema}.queue q WHERE q.parent_id = b.id AND q.await_condition IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM {schema}.batches c WHERE c.parent_id = b.id AND c.state = 1)
  AND NOT EXISTS (SELECT 1 FROM {schema}.batches o WHERE o.id = b.parent_batch_id)
ORDER BY b.id FOR UPDATE OF b SKIP LOCKED;
```
Третий — решение, новым снимком после блокировок:
```sql
DELETE FROM {schema}.batches b
WHERE b.id = ANY(@locked)
  AND NOT EXISTS (SELECT 1 FROM {schema}.queue  WHERE parent_id = b.id AND await_condition IS NOT NULL)  -- queue_awaiting
  AND NOT EXISTS (SELECT 1 FROM {schema}.batches WHERE parent_id = b.id AND state = 1)                   -- batches_awaiting
  AND NOT EXISTS (SELECT 1 FROM {schema}.batches o WHERE o.id = b.parent_batch_id);
```
Проверки в statement'е блокировки только отсеивают заведомо живые batches (иначе Failed-batch с ждущим продолжением занимал бы пакет каждого запуска). Решает третий: в одном statement'е с блокировкой проверка шла бы в снимке, взятом до блокировки, а `KEY SHARE` подписки не создаёт новой версии строки — после коммита подписки `FOR UPDATE` берёт строку без перепроверки, и batch удалялся бы из-под только что вставленных детей. Подписка, закоммиченная до блокировки, видна третьему statement'у; начатая после ждёт коммита удаления и строку не находит (`ParentNotFoundException`) либо, если строка осталась, подписывается. Удаление идёт сверху вниз (вложенный обычно завершён раньше внешнего, курсор его уже прошёл — удаляется следующим запуском): пока существует внешний batch, к которому могут прийти requeue и продолжения, существуют вложенные — для цепочки requeue (§6.6) и для (б). `FOR UPDATE` конфликтует с `KEY SHARE` подписки (§6.2) и с `FOR NO KEY UPDATE` учёта, requeue и отмены: такие строки удаление пропускает. При `JobsRetention = null` строки `batches` не удаляются. Ссылки на удалённую строку безвредны: requeue оставшегося члена (§6.6) и сигнал завершения с её `batch_id` (§6.4 Т1) пропускают учёт (лог Debug — только у resolver'а при учёте сигнала, §6.4 Т1).

Следствие: Failed- и Cancelled-члены batch хранятся, пока существует строка batch (не меньше `JobsRetention` после его завершения; у Failed-batch с ждущим OnSuccess-продолжением — бессрочно, §2.2), поэтому продолжение, добавленное к любому существующему Failed-batch, может дождаться requeue его членов.

`completions` не обслуживается retention'ом: строка живёт до обработки.

### 7.2 Компактирование и statement'ы части 5

Statement'ы части 5, меняющие строки `jobs` (отмена, финализация, `notify` в §6.2), обращаются к `jobs` через родителя (часть 2 §11) и ждут блокировку компактирования как обычные операции. Запуск `Awaiting`-строки (`UPDATE queue`) и активация batch (`UPDATE batches`) к `jobs` не обращаются — исключение из контракта части 2 §11 (уточняется, §9). Безвредно: запущенная строка остаётся в `queue` (живая по тому же источнику), а активация убирает batch из `batches_await_jobs` — набор живых под блокировкой от них только убывает, лишние строки `@ids` безвредны (§7.1).

## 8. Ошибки и наблюдаемость

- Сбой транзакции resolver'а (Т1, Т2, порция `CancelMembers`) откатывает её: сигналы остаются, повтор — следующей итерацией (лог Error, как другие циклы `ProcessingMaintenance`). 40P01 / 40001 — повтор транзакции. Таймаут блокировки `CancelMembers` — откат порции и `retry_after` сигнала (§6.5).
- **Порядок блокировок.** Строки `batches` пишутся только под `FOR NO KEY UPDATE`; ждут их, в порядке `ORDER BY id`, только учёт Т1 (§6.4), цепочка requeue (§6.6) и вложенные batches `CancelMembers` (§6.5); `CancelBatchAsync` берёт одну строку. Подписка берёт `FOR KEY SHARE` (с ними совместима), удаление retention'ом — `SKIP LOCKED`. Обход Т2 берёт сигналы и строки `queue` и `batches` только `SKIP LOCKED`, узлы не блокирует (§6.4 Т2.2) и ждёт лишь строки `jobs` отменяемых детей, чьи строки `queue` уже держит сам; строки `queue` и `jobs`, которые держат ждущие `batches` (`CancelMembers` — своих членов, requeue — терминального члена), Т2 поэтому не ждёт. Строки `queue` с ожиданием берутся `ORDER BY job_id` (подписка §6.2, `CancelMembers`, ack, heartbeat). Подписка держит строку `jobs` только терминальной задачи, а при нуле строк в п. 2 снимает блокировку откатом к точке сохранения. Циклов ожидания между resolver'ами, requeue, подпиской, `CancelMembers` и горячим путём нет; правило повтора — страховка (в пользовательской транзакции 40P01 не повторяется).
- Warning, если самый старый сигнал (`ORDER BY id LIMIT 1`) старше `10 × ResolverInterval`; проверка — раз в итерацию, не чаще раза в минуту на под. Сигнал, отложенный `retry_after` из-за строки, заблокированной открытой пользовательской транзакцией (§6.2), стареет так же и попадает в это предупреждение.
- Resolver работает только в процессах с сервером и `RunMaintenance = true` (worker §6.3): если таких нет, сигналы копятся и продолжения не стартуют. Документируется: workflows требуют хотя бы одного процесса с `AddPgBoltServer` и `RunMaintenance = true` (XML-doc `RunMaintenance` дополняется).
- `ParentNotFoundException`, `UniqueKeyConflictException`, `ArgumentException` (валидация §5.2), SQL-ошибки — до или вместо коммита; частичного графа не бывает: своя транзакция откатывается, в пользовательской — `ROLLBACK TO SAVEPOINT` (§6.1).

## 9. Изменения других частей

- **Core:** §2 — `Awaiting` производное (`await_condition IS NOT NULL`), код 1 не используется, диаграмма; Scheduled — `lease_owner IS NULL AND NOT ready AND await_condition IS NULL` (иначе `Awaiting`-строки считались бы Scheduled); §4.1 — колонки `parent_id`, `await_condition`, `batch_id`, `notify`, индексы `queue_awaiting`, `queue_await_jobs`, инварианты; §4.2 — колонки `continuation`, `notify`, индекс `jobs_batch`; §5.1 — дополнительные массивы, `batch_id` и в `queue` (внутренний API); §5.2 — `continuation` в результате dequeue; §5.3, §5.4, §5.6, §5.7 — дополнение §6.3; §5.7 — отмена `Awaiting` (§6.7); §5.8 — условие trigger; §5.9 — requeue: `notify`, `batch_id` и синхронный учёт в `batches` (§6.6), повтор всей транзакции, XML-doc `RequeueAsync` (§5.2); §6.1–§6.2 — `JobContext`, `JobInfo`, `IJobClient` (§5); §7 — генератор и BOLT009.
- **Worker:** §7.1 освобождение при shutdown — дополнение §6.3; §6.3 — цикл `resolver` (при `RunMaintenance`), опция `PgBoltProcessingOptions.ResolverInterval` (1 с, проверка как у остальных интервалов); `IServerStore.CountCompletionsAsync` (Т1) / `ResolveAsync` (Т2) / `CancelMembersAsync`; statement'ы финализации возвращают `signals`, store сам подаёт `ICompletionSignal.Signal()` после коммита (публичные типы результата ack / fail / sweeper не меняются); `IServerStore.OldestCompletionAgeAsync` (Warning о лаге); `IServerStore.HasReadyCompletionsAsync` (проверка простоя по таймеру); XML-doc `RunMaintenance` — требование §8.
- **Часть 2:** §5.3 — источники живых строк (а) и (б), повторная проверка (а) под блокировкой, `@ids` — объединение с набором предпроверки (§7.1); §6.2 — удаление строк `batches` сверху вниз в задаче `retention` по `JobsRetention` (блокировка, затем проверка новым снимком); §11 — контракт для части 5 уточняется: к `jobs` через родителя обращаются statement'ы, создающие `Awaiting`-задачи или делающие их терминальными; запуск `Awaiting`-строки (`UPDATE queue`) и активация batch (`UPDATE batches`) к `jobs` не обращаются — безвредно, живость под блокировкой от них только убывает (§7.2).
- **Часть 4:** §3 — детектор пре-релизной схемы проверяет и `batches`; §4.4 — префикс `pgbolt:` отклоняется и в `StepOptions.UniqueKey`.
- **Карта проекта:** часть 5 — ссылка на спеку, статус.

## 10. Контракты для других частей

- **Часть 6 (Dashboard):** `Awaiting`-задачи — `queue WHERE await_condition IS NOT NULL` (по родителю — `queue_awaiting`); Scheduled — `lease_owner IS NULL AND NOT ready AND await_condition IS NULL` (core §2, §9); дерево batch — `batches_nested`; члены — `jobs_batch`; прогресс — счётчики `batches`, ограниченные снизу нулём, как в `GetBatchAsync` (`greatest(0, …)`: между requeue члена и учётом его прежнего сигнала счётчик временно ниже факта, в том числе отрицателен, §6.6). Действия: `CancelBatchAsync`; requeue членов — core §5.9.
- **Часть 7:** метрики лага resolver'а (возраст старейшего сигнала, сигналов за проход), стоимость финализации с сигналом; нагрузка «batch-heavy», чтение мёртвых и отложенных сигналов захватами resolver'а, стоимость удаления строк `batches` (§11).
- **Часть 4:** recurring ставит одиночные задачи; запуск workflow по расписанию — задача-обёртка, вызывающая `Workflow()…SubmitAsync()`.

## 11. Риски

- **Ждущие строки в `queue`.** Большие batch-продолжения (миллионы членов) добавляют столько же строк в `queue` и в `queue_delayed`; `queue` остаётся равной «всем живым задачам», но перестаёт быть «тем, что можно выполнить». Dequeue не затрагивается (`queue_fetch`). Проверить в части 7.
- **Churn `completions`.** Каждый член batch даёт вставку и удаление сигнала. Агрессивный autovacuum задан на таблице; бенчмарк — часть 7. Захваты идут по частичным индексам (§4), но мёртвые версии до vacuum и сигналы, отложенные `retry_after` (их число — узлы, чьих детей держат чужие транзакции), читаются захватом своего вида с головы индекса; проверить в части 7.
- **Вечные Failed-цепочки.** По решению §2.2 они удерживают строки от retention; Failed- и Cancelled-члены batches хранятся, пока существует строка batch (§7.1). Стоимость ограничена компактированием, видимость — dashboard (часть 6).
- **Удерживаемые строки `batches`.** Терминальные batches старше `JobsRetention`, которые нельзя удалить (Failed-batch с ждущим OnSuccess-продолжением и его вложенные — бессрочно, §2.2), каждый запуск удаления строк `batches` (§7.1) перечитывает один раз с проверками: 100 000 вложенных такого batch — 100 000 строк индекса и проб `NOT EXISTS` на запуск, вне блокировки родителя `jobs`. Проверить в части 7.
- **Предпроверка retention при многих открытых batches.** Источник (б) (§7.1) делает спуск по `jobs_batch` старой партиции на каждый batch из `batches_open` — вне блокировки родителя, горячий путь не задерживает. Проверить в части 7.
- **`After` в долгой пользовательской транзакции.** Блокировка строки `queue` выполняющегося родителя до коммита пользователя задерживает heartbeat и ack его сервера, вплоть до «heartbeat потерян» и отмены всех выполнений сервера; Pending-родитель до коммита не берётся в работу, `CancelMembers` его batch стоит (§6.2, §6.5). Ограничение — документация (§5.2); resolver от этого не останавливается.

## 12. Тестирование

**SQL-контракт** (Testcontainers, напрямую и через PgBouncer):
- Таблица §2.2: родитель Succeeded / Failed / Cancelled × OnSuccess / OnAnyFinished; каскад по цепочке из трёх звеньев; requeue Failed-родителя с ждущим OnSuccess-ребёнком → после успеха ребёнок запускается.
- `After` к родителю в каждом состоянии: Pending, `Awaiting`, Succeeded, Failed, Cancelled, отсутствует; то же для batch (Active, Awaiting, терминальные, отсутствует); `After(new JobId(batchGuid))` и `After(new BatchId(jobGuid))` → `ParentNotFoundException`.
- `Awaiting` не берётся dequeue и не повышается promoter'ом; trigger → `false`; `CancelAsync` → `Cancelled` и каскад.
- `unique_key`: `Awaiting`-задача блокирует тот же ключ; конфликт в workflow → `UniqueKeyConflictException`, ни одной строки графа.
- Пользовательская транзакция: `UniqueKeyConflictException` и `ParentNotFoundException` пойманы, транзакция закоммичена → строк графа нет, прочие записи пользователя есть.

**Гонки** (два-три соединения с ручным управлением транзакциями):
- Подписка (§6.2) против ack, окончательного fail, sweeper, cancel родителя — в обоих порядках захвата; ребёнок не застревает.
- Подписка на Failed-родителя против requeue в обоих порядках; п. 2 подписки ждёт requeue, затем `CancelAsync`, `CancelMembers` или ack той же задачи против п. 1 — без 40P01, строка `jobs` после отката к точке сохранения не заблокирована; `RequeueAsync` терминального родителя ждёт коммита пользовательской транзакции с `After`.
- Подписка на batch против завершения batch resolver'ом: сигнал batch обработан до коммита подписки → дети разрешаются по `Subscribed`.
- Подписка на batch против удаления его строки retention'ом в обоих порядках и с коммитом подписки между выбором кандидатов удаления и их блокировкой (хук): `ParentNotFoundException` либо строка не удалена и ребёнок разрешается; учёт, requeue члена и `CancelBatchAsync` не ждут открытую транзакцию с `After(BatchId)`.
- Устаревший сигнал: родитель отменён, сигнал не обработан, requeue, новый `After` с OnSuccess → ребёнок не отменён, запускается после успеха; то же для Failed + OnAnyFinished (не запускается, пока родитель выполняется) и для сигнала исхода batch после requeue члена.
- Два resolver'а над сигналами одного batch: счётчики точны, без 40P01.
- Requeue члена против учёта сигналов: batch total = 2; член X отменён и учтён, requeue X, X завершён успехом, Y выполняется; resolver обрабатывает сигнал X раньше и позже requeue → batch остаётся Active, OnSuccess-продолжение не отменено; после успеха Y — Succeeded и запуск. То же для Failed (OnAnyFinished не запускается раньше) и для члена вложенного batch (внешний возвращается в Active).
- Подписка на `Awaiting`-ребёнка в открытой пользовательской транзакции во время разрешения его родителя: остальные сигналы прохода обработаны без ожидания, ребёнок разрешён после коммита.
- `After` в открытой пользовательской транзакции на выполняемую задачу: heartbeat её сервера ждёт до коммита; на Pending-задачу — dequeue её не берёт до коммита (фиксация документированного поведения).

**Batches:**
- Исходы §2.3 (Cancelled > Failed > Succeeded); вложенность на три уровня — исход поднимается вверх.
- Batch-продолжение активирует члены и вложенные batches; вложенный batch-продолжение члена X внутри batch-продолжения не активируется вместе с внешним, а только по завершении X; отмена родителя отменяет batch-продолжение целиком.
- Batch-продолжение с идущим `CancelMembers` при завершении родителя не активируется, исход Cancelled.
- Отмена ветки: batch больше 1000 членов с цепочкой X → Y (OnAnyFinished и OnSuccess при выполнявшемся X), X и Y в разных порциях `CancelMembers` и в одной → Y отменён, не запущен; вложенный batch-продолжение члена X не активируется; `Activated` batch с `cancel_requested` не запускает членов.
- Requeue Failed-члена возвращает batch (и внешний) в Active, после успеха — Succeeded и запуск OnSuccess-продолжения; requeue члена отменённого batch → `CancelBatchAsync` снова возвращает `CancellationRequested` с новым сигналом.
- Requeue Cancelled-члена при идущей отмене (batch из 3000 членов ещё не терминален): члены позади и впереди курсора этапа 0 — первый выполняется, второй отменяется снова, флаг не сброшен; ещё ждущее OnAnyFinished-продолжение выполнившегося члена отменено, а не запущено; то же для вложенного batch, вернувшегося в Active, до и после прохода этапа 1 внешнего.
- Устаревший `CancelMembers`: batch из 3000 членов стал Cancelled до конца обхода; requeue Cancelled-члена за курсором и члена вложенного batch → следующая порция удаляет сигнал, члены выполняются, batch снова не отменяется.
- Fan-out 100 000: все члены запущены, сигнал удалён, порции не больше 1000, строк за проход не больше бюджета; число прочитанных строк индекса на порцию не растёт с позицией курсора.
- 500 сигналов листовых членов удаляются в Т1 одним statement'ом; сигналы с многопроходным fan-out (не меньше 500) не захватываются Т1 повторно, учёт следующих сигналов не ждёт их; 500 сигналов узлов с одним ребёнком — Т2 с числом statement'ов, не зависящим от числа узлов (счётчик команд); `EXPLAIN` захватов Т1 и Т2 — по `completions_count` и `completions_walk`.
- Сигналы, чьи дети заблокированы другими (не меньше 500), получают `retry_after` и не мешают захвату следующих.
- Цепочка член → batch → OnSuccess-продолжение → его batch-продолжение при `ResolverInterval` = 10 с и без in-process сигнала доходит до запуска за один интервал.
- `CancelBatchAsync`: невзятые члены — сразу, выполняющиеся — через heartbeat, вложенные — рекурсивно; `NotFound` / `AlreadyFinished` / `CancellationRequested`, повторная отмена — без нового сигнала; отмена маленького batch и batch с 5000 вложенными идёт итерациями без пауз, а не по сигналу на интервал; строка члена, заблокированная дольше `lock_timeout`, — сигнал получает `retry_after`, отмена других batches идёт, цикл resolver'а не стоит, остальные члены этого batch отменяются после освобождения строки.
- `GetBatchAsync` между requeue члена и учётом его прежнего сигнала — счётчики не отрицательны.

**Падения:** процесс убит после ack с сигналом до прохода resolver'а → другой под доводит; сбой посреди прохода → повтор без двойного запуска и двойного счёта; повтор `SubmitAsync` после потерянного ответа на коммит → успех без дубликатов; повтор, начатый, пока предыдущая попытка ещё не закоммичена (23505 по PK `batches` и `exists_by_id` для графа без batches) → успех.

**Retention:** партиция с Failed-родителем, у которого ждущие дети, с Failed-родителем, единственный ребёнок которого — batch-продолжение, и с Failed- и Cancelled-членами существующего (в том числе Cancelled) batch компактируется, строки сохраняются; подписка на старую терминальную задачу между предпроверкой и блокировкой (хук) — родитель сохранён; requeue Cancelled-члена и его окончательный fail между предпроверкой и блокировкой (хук) — член сохранён; id batch с ждущими детьми не попадают в `LiveJobs` (партиция без живых задач отсоединяется); удержание под блокировкой не растёт с числом ждущих детей одного родителя (миллион); строка batch удаляется по `JobsRetention` сверху вниз (вложенный — после внешнего), при `null` — не удаляется; удерживаемые внешним batch 5000 вложенных при удалении 5000 других читаются запуском один раз (`EXPLAIN` — range scan `batches_finished` без сортировки, число прочитанных строк не растёт с числом пакетов); Failed-член удалённого batch удаляется; продолжение к старому Failed-batch дожидается requeue его члена; requeue члена удалённого batch и его сигнал завершения проходят без ошибок.

**Горячий путь:** тест-контракт HOT core проходит; финализация без `notify` не вставляет в `completions` (`pg_stat_user_tables.n_tup_ins`); `queue_fetch` не содержит `Awaiting`-строк; `EXPLAIN` dequeue не меняется.

**API и генератор:** snapshot-тесты метаданных для `Enqueue<TArgs>` / `ContinueWith<TArgs>` / `EnqueueMany<TArgs>` построителя; BOLT009 (срабатывает и подавляется по правилам §5.4); валидация §5.2 (области и вызовы после `SubmitAsync`, `After` без продолжений, повтор `UniqueKey` в графе, префикс `pgbolt:`); `JobContext` для родителя-задачи, родителя-batch и члена batch-продолжения; `BatchInfo` для batch-продолжения задачи и batch; `JobInfo.State = Awaiting`; `JobInfo.Continuation`, `ParentId`, `ParentBatchId` для кодов 1–5 (у члена batch-продолжения — `null`).

**Миграция:** старт на схеме сборки части 4 (нет `batches`) падает с сообщением о пре-релизной схеме.

## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§6.1, §6.2).
- §10: Awaiting — по `queue_awaiting_list`, Scheduled — с `run_at < 'infinity'`; дерево и прогресс batch с `greatest(0, …)`;
  продолжения — коды `continuation` 1–4 по `jobs_parent`, члены batch-продолжения (код 5) продолжениями не считаются;
  batch-продолжения — по `batches_parent`.
