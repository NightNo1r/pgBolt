# PgBolt Workflows (часть 5) — план реализации

> **Для исполнителя-агента.** План самодостаточен и не требует скиллов: протокол исполнения через субагентов, шаблоны их заданий, выбор моделей и требования к окружению описаны ниже («Протокол исполнения»). Шаги задач размечены чекбоксами (`- [ ]`).

**Goal:** Продолжения и batches в модели Hangfire: `Awaiting`-задачи в `queue`, таблицы `batches` и `completions`, fluent API `client.Workflow()…SubmitAsync()`, `CancelBatchAsync` / `GetBatchAsync`, сигналы завершения из statement'ов финализации и resolver (учёт Т1, обход Т2, `CancelMembers`) в `PgBolt.Server`, синхронный учёт requeue членов, retention с живыми строками части 5, генератор и BOLT009.

**Architecture:** Схема и SQL — в core (`PgBolt.Postgres`): объекты части 5 в V001, дополнение финализации (§6.3) во всех statement'ах, удаляющих строку `queue` с терминальным исходом, запись графа одной транзакцией (`PgBoltStore.Workflow.cs`: `batches` одним `unnest`, задачи — вариантом enqueue с массивами `parent_id` / `continuation` / `notify` / `batch_id`, подписка §6.2, синтетические сигналы, `SAVEPOINT` в пользовательской транзакции, повтор с проверкой первой строки), resolver (`PgBoltStore.Resolver.cs`: Т1 — один statement, Т2 — statement'ы на этап, а не на узел), отмена batch (`PgBoltStore.Batches.cs`), requeue с цепочкой `batches` (§6.6), retention (`RetentionRunner*.cs`). Построитель — синхронный граф узлов в `PgBolt.Postgres/Client/Workflows`. Расписание resolver'а — цикл `resolver` в `ProcessingMaintenance` (`PgBolt.Server`), будится in-process `ICompletionSignal`.

**Tech Stack:** .NET 10 (C# 14), Npgsql 10 (`NpgsqlBatch`, savepoints `NpgsqlTransaction.SaveAsync/RollbackAsync/ReleaseAsync`, slim data source), Microsoft.Extensions.Hosting 10, Roslyn (генератор/анализатор, netstandard2.0), xUnit v3, Verify.SourceGenerators, Testcontainers (PostgreSQL 17 + PgBouncer transaction mode), Microsoft.Extensions.TimeProvider.Testing.

**Spec:** `docs/superpowers/specs/2026-10-02-pgbolt-workflows-design.md` (далее «спека», ссылки вида «§6.4»). Основа: `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` («core §5.1»), `2026-09-28-pgbolt-worker-design.md` («worker §6.3»), `2026-09-29-pgbolt-storage-maintenance-design.md` («часть 2 §5.3»), `2026-09-30-pgbolt-scheduler-design.md` («часть 4 §3»). Образец стиля кода и тестов — существующие `src/PgBolt.Postgres/Storage/*.cs`, `src/PgBolt.Postgres/Schema/RetentionRunner*.cs`, `tests/PgBolt.Postgres.Tests`, `tests/PgBolt.Server.Tests`, `tests/PgBolt.Generators.Tests`.

## Global Constraints

- Только .NET 10 (`net10.0`); `PgBolt.Abstractions`, `PgBolt.Postgres`, `PgBolt.Server` — `IsAotCompatible=true`, `TreatWarningsAsErrors=true` (`Directory.Build.props`). Без reflection. SQL — строковые константы с `{schema}`, подставляемым `Render` (в `RetentionRunner` — интерполяция `_schema`, как в существующем коде).
- Подключение напрямую или через PgBouncer в transaction mode: без `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` (кроме `SET LOCAL`), temp tables. Транзакции — только `BeginTransactionAsync` / `CommitAsync` / `RollbackAsync`; точки сохранения — только `NpgsqlTransaction.SaveAsync` / `RollbackAsync(name)` / `ReleaseAsync(name)`; `BEGIN`/`COMMIT`/`SAVEPOINT` в тексте команд рабочего кода запрещены (в тестах держатели блокировок могут писать `BEGIN` в тексте команды, как `RetentionTestBase.HoldAsync`).
- Уровень изоляции — READ COMMITTED. «Сейчас» — `now()` БД.
- Контракт HOT (core §4.2): новых индексов `jobs` по изменяемым колонкам нет; `jobs.continuation`, `jobs.parent_id`, `jobs.batch_id` пишутся один раз при вставке; `jobs.notify` меняется, не индексируется. Индекс `jobs_batch` — по неизменяемым колонкам.
- Хранимые коды (§2.4, §4): `jobs.continuation` = `queue.await_condition` = `batches.condition`: 1 OnSuccess задачи, 2 OnAnyFinished задачи, 3 OnSuccess batch, 4 OnAnyFinished batch, 5 Activation (только у задач). `completions.kind`: 2 Succeeded, 3 Failed, 4 Cancelled, 5 Activated, 6 CancelMembers, 7 Subscribed. `batches.state`: 0 Active, 1 Awaiting, 2 Succeeded, 3 Failed, 4 Cancelled. `jobs.state` 1 больше не используется.
- Инварианты `queue` (§4): `await_condition IS NOT NULL` ⇒ `ready = false`, `run_at = 'infinity'`, `lease_owner IS NULL`; `queue.batch_id = jobs.batch_id`; `notify = true` у каждого члена batch и у каждой задачи, к которой когда-либо добавляли продолжение. Сигналы `Activated`, `CancelMembers`, `Subscribed` и синтетические — с `batch_id = NULL`.
- Константы (§6.1, §6.4, §6.5, §7.1): пакет команды enqueue графа — до 10 000 строк; захват Т1 и Т2 — по 500 сигналов; бюджет Т2 — 5000 строк детей, порция узла `min(1000, max(1, остаток / число узлов))`; `CancelMembers` — `SET LOCAL lock_timeout = '1s'`, порция до 1000 строк, за итерацию до 5000 строк или 100 транзакций; `retry_after = now() + ResolverInterval`; удаление `batches` пакетами по 1000; Warning о лаге — старейший сигнал старше `10 × ResolverInterval`, проверка не чаще раза в минуту на под.
- Опции (§6.4, worker): `PgBoltProcessingOptions.ResolverInterval = 1 с` (jitter 20 %, `(0, PgBoltServerOptions.MaxInterval]`); resolver работает только при `RunMaintenance = true`.
- Диагностика «workflow не отправлен» — **BOLT009** (Warning): код BOLT007 в спеке §5.4 уже занят (`InvalidTimeout`, часть 3), см. «Отклонения от спеки» ниже.
- Коммиты: Conventional Commits; автор и коммиттер — `NightNo1r <57496218+NightNo1r@users.noreply.github.com>` (`git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit ...`); строк `Co-Authored-By` **нет**.

### Отклонения от спеки и уточнения (решения плана)

1. **BOLT007 → BOLT009.** Спека §5.4 называет диагностику BOLT007, но BOLT007 (`InvalidTimeout`) и BOLT008 (`TimeoutAboveLimit`) уже выпущены частью 3 (`src/PgBolt.Generators/Analysis/Diagnostics.cs`, CHANGELOG). Новая диагностика — BOLT009; правило срабатывания — §5.4 без изменений.
2. **«Результат ack / fail / sweeper сообщает число сигналов» (§9 worker).** Statement'ы возвращают `signals`, store сам подаёт `ICompletionSignal.Signal()` (§6.3: «store подаёт in-process сигнал»). Публичные типы результата (`int` ack, `FailOutcome`, `int` sweeper, `ReleaseOutcome`, `CancelResult`) и `IServerStore` горячего пути не меняются — иначе ломается весь runtime сервера без выигрыша: подписчик сигнала (resolver) в том же процессе получает его от store.
3. **Лог Debug при requeue члена удалённого batch (§6.6, §7.1).** Core логов не пишет (как retention, `RetentionResult`), а requeue вызывается клиентом (`PgBoltJobClient`) без логгера: отсутствующая строка `batches` молча завершает подъём. Лог Debug о строке, удалённой retention'ом, пишет только resolver (Т1, `CompletionCountResult.MissingBatches`, Task 12). Текст спеки §6.6 п. 2 и §7.1 приводится к этому поведению в Task 14 Step 2.
4. **Тест «коммит подписки между выбором кандидатов удаления и их блокировкой» (§12 Гонки).** После п. 9 выбор и блокировка разделены: окно кандидатов без блокировок → хук `RetentionStep.BatchesWindowed` → `BatchLockSql` (`FOR UPDATE SKIP LOCKED` с перепроверкой) → хук `RetentionStep.BatchesLocked` → `DELETE` (statement 2, новым снимком). Настоящая подписка (`After(BatchId)` в пользовательской транзакции) делается на хуке `BatchesWindowed`: открытая держит `KEY SHARE` строки batch — блокировка строку пропускает (`SKIP LOCKED`) и не ждёт; закоммиченная до блокировки — её ждущего ребёнка видит перепроверка `BatchLockSql`; в обоих случаях строка цела, ребёнок разрешается (`RetentionWorkflowTests.Subscription_between_batch_window_and_lock_keeps_batch`). Дополнительно хук `BatchesLocked` коммитит ждущего ребёнка прямой вставкой между блокировкой и statement'ом 2 — его видит только проверка `DELETE` (часть «в» `Batch_subscription_versus_batch_deletion`); настоящая подписка в этой точке ждала бы `FOR UPDATE` удаления.
5. **Пустой batch и исключение лямбды.** `ArgumentException` пустого batch и любое исключение лямбды `StartBatch` / `ContinueWithBatch` делают workflow непригодным: дальнейшие вызовы — `InvalidOperationException` (иначе в графе остался бы недостроенный batch).
6. **Повтор `SubmitAsync`, обнаруживший закоммиченную предыдущую попытку,** возвращает тот же `WorkflowResult` и будит resolver, если у графа есть внешние родители (синтетические сигналы прошлой попытки уже в `completions`).
7. **Закрепление `n_distinct` (решение при исполнении Task 6).** В V001 добавлено `ALTER TABLE … ALTER COLUMN … SET (n_distinct = -0.05)` для `queue.parent_id`, `batches.parent_id`, `batches.parent_batch_id`: поиски детей resolver'а идут по runtime-значению родителя, и при преобладании одного родителя (огромный batch-продолжение) оценка `(1 − null_frac) / n_distinct` уводила Т1/Т2 в Seq Scan / `queue_pkey` (O(N²) на fan-out). Регрессия — `WorkflowResolverPlanTests`. **Task 14 Step 2** дополнительно вносит в спеку §4 после индексов `queue` и `batches` эти три строки с кратким обоснованием.
8. **Источник (б) в предпроверке читается по партиции (решение при исполнении Task 10).** `BatchMembersSql(partition)` читает партицию кандидата по имени, а не родителя `jobs` (иначе предпроверка ждала `ACCESS EXCLUSIVE` от `DETACH` на родителе — регрессия `RetentionJobsTests`). `queue` и (б) читаются одним statement'ом `QueueAndBatchMembersSql` (`UNION ALL`, у каждой ветки свой `LIMIT`) во втором пакете после первой сверки oid, с повторной сверкой oid и 42P01 → `Gone`: один снимок нужен для монотонности (б) (§7.1). Первый пакет — `lock_timeout`, сверка oid, оба (а). Хук `RetentionStep.PrecheckOidChecked` — между пакетами (тесты).
9. **Удаление `batches`: окно, затем блокировка (решение при исполнении Task 10).** Statement 1 разделён: оконный range scan `batches_finished` без блокировок и anti-join (`LIMIT 1000`, курсор — последняя строка окна, конец запуска — неполное окно), затем `BatchLockSql`: `id = ANY`, `finished_at < порог`, `state IN (2,3,4)`, три `NOT EXISTS`, `ORDER BY id FOR UPDATE SKIP LOCKED`. Прежний одностатейный выбор при малом остатке кандидатов уходил в Bitmap + Sort всего остатка (O(удерживаемые × пакеты)). Хук `RetentionStep.BatchesWindowed` — между окном и блокировкой (тесты); statement 2 и `BatchesLocked` без изменений. Task 14 Step 2 переносит пп. 8–9 в спеку §7.1.
10. **Пропуск пустых этапов итерации resolver'а (решение владельца при упрощении ветки).** Итоговый `SELECT` Т1 (`CountSql`) возвращает `walk_ready` и `cancel_ready` — `EXISTS` по предикатам захватов Т2 и `CancelMembers` (`completions_walk`, `completions_cancel`); `CompletionCountResult.WalkReady` / `CancelReady`. `ProcessingMaintenance.ResolveOnceAsync` вызывает `ResolveAsync`, только если `Walking > 0` или `WalkReady`, и `CancelMembersAsync`, только если `CancelReady` или Т2 создал сигналы; правила немедленного повтора, проверка лага и путь по таймеру (`HasReadyCompletionsAsync`) — без изменений. Итерация по сигналу финализации без детей — один statement вместо трёх транзакций. Сигналы других подов, закоммиченные после снимка Т1, подхватываются их resolver'ами или следующей итерацией (не позже `ResolverInterval`). Спека §6.4 обновлена.

## Требования к окружению

Проверить до Task 1; при невыполнении — остановиться и доложить.

| Требование | Проверка | Ожидаемо |
|---|---|---|
| .NET SDK 10 | `dotnet --version` | `10.0.*` |
| Docker | `docker info` | без ошибок |
| Сборка зелёная | `dotnet test PgBolt.slnx --filter "Category!=Load"` | все тесты проходят |
| Ветка | `git branch --show-current` | `feat/workflows` (`git switch feat/workflows`; `git pull --ff-only` — только если `git rev-parse --abbrev-ref @{upstream}` успешен: локальная ветка может не иметь upstream и не существовать на `origin`) |

Docker недоступен — не заменять базу моками и не пропускать интеграционные тесты; остановиться и доложить. Тесты — VSTest: `dotnet test tests/<Проект> --filter "FullyQualifiedName~<Класс>"`. Интеграционные тестовые классы — абстрактные, с наследниками `_Direct` и `_Bouncer` (как `MaintenanceTasksTests`); тесты на `EXPLAIN` и тяжёлые сценарии — только `Direct`, с `[Trait("Category", "Slow")]` (как `DequeuePlanTests`).

## Протокол исполнения

Исполняется основной сессией (**координатор**) через субагентов — инструментом запуска агента (в Claude Code — `Agent`, тип `general-purpose`, запуск в foreground: следующий шаг зависит от результата). Скиллы не нужны. Состояние хранится в самом плане (чекбоксы) и в git, поэтому прерванное исполнение продолжается с первой неотмеченной задачи.

### Роли
- **Координатор** — основная сессия. Ведёт задачи строго по порядку (Task 1 → Task 14), запускает исполнителя и ревьюера по шаблонам ниже, проверяет их отчёты, отмечает чекбоксы. Код задач сам не пишет.
- **Исполнитель** — свежий субагент на задачу (и на каждый раунд исправлений), модель из таблицы.
- **Ревьюер** — свежий субагент на задачу, всегда Opus 5.5.

Если среда не поддерживает субагентов, координатор выполняет обе роли сам, последовательно, с тем же чек-листом.

### Модели

| Задачи | Исполнитель | Почему |
|---|---|---|
| 1, 2, 14 | Sonnet 5.5 (`claude-sonnet-5-5`, параметр `model: "sonnet"`) | Код дан целиком, логика локальная (DDL, контракты, документация) |
| 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 | Opus 5.5 (`claude-opus-5-5`, `model: "opus"`) | Финализация и сигналы, транзакции и точки сохранения, блокировки и снимки READ COMMITTED, PgBouncer, детерминированные гонки, retention под блокировкой родителя, Roslyn-анализ потока, хостинг |
| Ревью каждой задачи и итоговое ревью ветки | Opus 5.5 (`model: "opus"`) | Сверка со спекой, поиск гонок, застревающих `Awaiting`, двойного учёта и циклов блокировок |

### Подготовка (один раз)
1. Проверить «Требования к окружению»; при невыполнении — остановиться и доложить.
2. Ветка исполнения — `feat/workflows` (в ней же спека и этот план): `git switch feat/workflows`, затем `if git rev-parse --abbrev-ref @{upstream} >/dev/null 2>&1; then git pull --ff-only; fi` (upstream может отсутствовать — ветка ещё не опубликована на `origin`; это не ошибка). Рабочее дерево чистое (`git status --short` пуст).
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
7. Конец фазы (после Task 3, 8, 11, 14) — `dotnet test PgBolt.slnx --filter "Category!=Load"`; падение — остановиться и доложить.

### Завершение
1. **Итоговое ревью ветки** — ревьюер (Opus) по шаблону «Ревьюер», но предмет — `git diff main..HEAD` (код; изменения `docs/` — только на согласованность), текст задачи заменяется ссылкой на весь план и спеку; фокус — сквозные проблемы: согласованность задач, гонки подписки / финализации / requeue / retention / resolver'ов, порядок блокировок (§8), соответствие спеке целиком. `CHANGES` → исправление и повторное ревью по тем же правилам (не более двух раундов).
2. `dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный.
3. `git push -u origin feat/workflows` (создаёт ветку на `origin` и настраивает upstream; при уже настроенном upstream — то же действие); PR не создавать без запроса пользователя. Доложить: SHA, итог тестов, отклонения от плана из отчётов исполнителей.

### Шаблон «Исполнитель»

```text
Ты реализуешь одну задачу плана в репозитории <абсолютный путь>, ветка feat/workflows (текущий HEAD: <SHA>).
План: docs/superpowers/plans/2026-10-02-pgbolt-workflows.md. Спека: docs/superpowers/specs/2026-10-02-pgbolt-workflows-design.md.
Прочитай в плане разделы «Global Constraints» (включая «Отклонения от спеки»), «Контракт внутренних типов», «Правила для
исполнителя» и задачу «Task N» целиком; разделы спеки, на которые ссылается задача. Выполни шаги задачи строго по порядку:
падающий тест → убедиться, что падает → реализация → тесты проходят → коммит командой из плана (автор NightNo1r, без
Co-Authored-By). Чекбоксы плана не отмечай. Не делай работу других задач; код плана — эталон, отклонения только ради
компиляции, несовместимости API Npgsql/Roslyn/PostgreSQL или явной ошибки плана. Две безуспешные попытки исправить один
падающий тест → остановись.
Ответ строго в формате:
STATUS: DONE | BLOCKED
SHA: <HEAD после коммита>
TESTS: <команда> → passed N / failed M
DEVIATIONS: <список отклонений от кода плана с причинами или «нет»>
NOTES: <для BLOCKED — вывод ошибки и гипотезы>
```

### Шаблон «Ревьюер»

```text
Ты ревьюер задачи Task N плана docs/superpowers/plans/2026-10-02-pgbolt-workflows.md в репозитории <абсолютный путь>.
Прочитай задачу, «Global Constraints», «Контракт внутренних типов», «Чек-лист ревьюера» и разделы спеки
docs/superpowers/specs/2026-10-02-pgbolt-workflows-design.md, на которые ссылается задача.
Изменения: `git diff <BASE>..HEAD`. Отчёт исполнителя: <отчёт целиком>.
Проверь по чек-листу ревьюера; сам запусти тесты задачи (команды из шагов «Запустить»). Ничего не редактируй.
Ответ: APPROVE или CHANGES; для CHANGES — нумерованный список «файл:строка — что не так — чем подтверждается — как исправить».
Стиль не оценивай, если он не нарушает Global Constraints.
```

### Шаблон «Исправление»

```text
Ты исправляешь замечания ревью по задаче Task N плана docs/superpowers/plans/2026-10-02-pgbolt-workflows.md
в репозитории <абсолютный путь>, ветка feat/workflows (HEAD: <SHA>). Прочитай задачу, «Global Constraints», «Контракт
внутренних типов», «Правила для исполнителя». Замечания: <список ревьюера целиком>.
Исправь каждое новым коммитом (Conventional Commits, автор через git -c user.name=NightNo1r
-c user.email=57496218+NightNo1r@users.noreply.github.com, без Co-Authored-By); не ослабляй тесты. С замечанием не согласен —
не исправляй, а объясни в отчёте с доказательством. Ответ — в формате шаблона «Исполнитель» плюс FIXED/REJECTED по номерам замечаний.
```

### Чек-лист ревьюера
- Реализовано всё из шагов; тесты задачи есть и проходят (интеграционные — в `_Direct` и `_Bouncer`, кроме явно помеченных Direct-only).
- Имена и сигнатуры совпадают с «Контрактом внутренних типов».
- Финализация (§6.3): каждый statement, удаляющий строку `queue` с терминальным исходом, пишет `jobs.notify = jobs.notify OR queue.notify` и сигнал только при `notify`; `kind` — итоговое состояние; statement'ы без финализации (retry без отмены, reset sweeper'а, release без отмены) `notify` не трогают; in-process сигнал — только после коммита (`QueryAndSignalAsync` или после `CommitAsync`), не из делегата чтения при открытом ридере.
- Подписка (§6.2): п. 1 → п. 2 в точке сохранения `pgbolt_after` → п. 3; ноль строк в п. 2 — откат к точке сохранения (строка `jobs` не остаётся заблокированной); родитель-batch — только `FOR KEY SHARE`; родители — в порядке id; подписка — после вставки детей.
- Пользовательская транзакция: `SAVEPOINT pgbolt_workflow` до первой записи, любое исключение — `ROLLBACK TO SAVEPOINT` и проброс, успех — `RELEASE`; ни wakeup, ни in-process сигнала до коммита пользователя.
- Resolver: захваты только `FOR UPDATE SKIP LOCKED` по своему частичному индексу и с `retry_after`; Т1 — один statement, блокирует `batches` `ORDER BY id FOR NO KEY UPDATE`; Т2 — statement'ы на этап, исход узла перечитывается в каждом statement'е выборки детей, строки `queue` / `batches` берутся `SKIP LOCKED`, курсор включающий, `n.lim = min(1000, max(1, остаток / узлов))`; проверка отмены ветки перед запуском.
- `CancelMembers`: `SET LOCAL lock_timeout = '1s'`, один сигнал на транзакцию, без `SKIP LOCKED`, `cancel_requested` узла перечитывается в каждом statement'е порции; таймаут — откат и `retry_after` отдельным statement'ом.
- Requeue (§6.6): цепочка `batches` блокируется `ORDER BY id FOR NO KEY UPDATE`, подъём снизу вверх только через терминальные, `cancel_requested` сбрасывается только при выходе из терминального состояния; всё в одной транзакции requeue.
- Retention (§7.1): источники (а) — loose index scan не больше `T + 1` спусков; (б) — только в предпроверке; под блокировкой — `queue` и (а); `@ids` компактирования — объединение предпроверки и повторной проверки, поэтому на пути компактирования отсоединения нет (набор предпроверки непуст); `batches` удаляются двумя statement'ами (блокировка, затем проверка новым снимком), сверху вниз.
- Порядок блокировок §8 не нарушен; новых ожиданий в горячем пути нет; `dequeue` / heartbeat / promoter не изменены (кроме чтения `j.continuation`).
- Нет `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` вне `SET LOCAL`, `BEGIN`/`COMMIT`/`SAVEPOINT` в тексте SQL рабочего кода.
- Контракт HOT: новых индексов `jobs` по изменяемым колонкам нет.
- В runtime `PgBolt.Server` нет `Task.Delay`/таймеров мимо `TimeProvider`; unit-тесты runtime не зависят от реального времени (кроме `Eventually`).
- Тесты не ослаблены: нет `Skip`, удалённых assert'ов, подгонки ожидаемых значений; гоночные тесты детерминированы (держатели блокировок, ожидание `pg_stat_activity`/`pg_locks`, хуки), без `Task.Delay` как способа упорядочить события.
- Сборка без предупреждений (`TreatWarningsAsErrors`, AOT/trim-анализ).
- Коммиты — Conventional Commits, автор NightNo1r, без `Co-Authored-By`.

### Правила для исполнителя
- Не делать работу будущих задач. Не менять тесты ради прохождения, если задача этого не предписывает.
- Код плана — эталон; отклонения — только для устранения ошибки компиляции, несовместимости API Npgsql / Roslyn / PostgreSQL или явной ошибки плана; каждое — в отчёт `DEVIATIONS`.
- Если имя или сигнатура из «Контракта внутренних типов» не компилируется — не переименовывать, а остановиться (`BLOCKED`): на них опираются соседние задачи.
- Если `EXPLAIN`-тест показывает другой план, чем ожидает задача, — не ослаблять assert, а доложить в `NOTES` с текстом плана (`BLOCKED`).
- Две безуспешные попытки исправить один падающий тест → `STATUS: BLOCKED` с выводом и гипотезами.

## Контракт внутренних типов

Соседние задачи опираются на эти имена и сигнатуры; менять их нельзя. В скобках — задача, которая вводит член.

```csharp
// PgBolt.Abstractions — публичные (Task 2; члены IJobClient — Task 5 и Task 7)
public readonly record struct BatchId(Guid Value) { static BatchId New(); }
public enum ContinuationCondition { OnSuccess, OnAnyFinished }
public sealed record WorkflowOptions { DbTransaction? Transaction; }
public sealed record StepOptions { string? Queue; short? Priority; DateTimeOffset? RunAt; TimeSpan? Delay; string? UniqueKey;
    int? MaxAttempts; IReadOnlyList<TimeSpan>? RetryDelays; }                                        // init-свойства
public interface IWorkflowScope { IJobStep Enqueue<TArgs>(TArgs, StepOptions?); IJobStep Enqueue(string, JsonElement, StepOptions?);
    IBatchStep StartBatch(Action<IBatchBuilder>); }
public interface IWorkflow : IWorkflowScope { INodeStep After(JobId); INodeStep After(BatchId); ValueTask<WorkflowResult> SubmitAsync(CancellationToken); }
public interface IBatchBuilder : IWorkflowScope { IReadOnlyList<IJobStep> EnqueueMany<TArgs>(IReadOnlyList<TArgs>, StepOptions?); }
public interface INodeStep { IJobStep ContinueWith<TArgs>(TArgs, ContinuationCondition, StepOptions?);
    IJobStep ContinueWith(string, JsonElement, ContinuationCondition, StepOptions?); IBatchStep ContinueWithBatch(Action<IBatchBuilder>, ContinuationCondition);
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken); }
public interface IJobStep : INodeStep { JobId Id; }   public interface IBatchStep : INodeStep { BatchId Id; }
public sealed record WorkflowResult(IReadOnlyList<JobId> Jobs, IReadOnlyList<BatchId> Batches);
public enum BatchCancelResult { NotFound, AlreadyFinished, CancellationRequested }
public enum BatchState { Active, Awaiting, Succeeded, Failed, Cancelled }
public sealed record BatchInfo(BatchId Id, BatchState State, int Total, int Succeeded, int Failed, int Cancelled, BatchId? OuterBatchId,
    JobId? ParentId, BatchId? ParentBatchId, ContinuationCondition? Condition, bool CancellationRequested, DateTimeOffset CreatedAt,
    DateTimeOffset? FinishedAt);
public sealed class ParentNotFoundException(Guid parentId) : InvalidOperationException { Guid ParentId; }
public sealed class UniqueKeyConflictException(string uniqueKey, JobId? existingId) : InvalidOperationException { string UniqueKey; JobId? ExistingId; }
// JobContext(JobId jobId, string jobName, int attempt, int maxAttempts, JobId? parentId, BatchId? batchId, ILogger logger,
//     DateTimeOffset? scheduledAt = null, string? recurringJobId = null, BatchId? parentBatchId = null): BatchId (BatchId?), ParentBatchId
// JobInfo(…, string? RecurringJobId = null, DateTimeOffset? ScheduledAt = null, JobId? ParentId = null, BatchId? ParentBatchId = null,
//     BatchId? BatchId = null, ContinuationCondition? Continuation = null)
// IJobClient: IWorkflow Workflow(WorkflowOptions? options = null)                                                        // Task 5
//             ValueTask<BatchCancelResult> CancelBatchAsync(BatchId, CancellationToken); ValueTask<BatchInfo?> GetBatchAsync(BatchId, CancellationToken) // Task 7

// PgBolt.Postgres.Client — публичные (Task 3)
public interface ICompletionSignal { void Signal(); IDisposable Subscribe(Action callback); }
public sealed class InProcessCompletionSignal : ICompletionSignal;

// PgBolt.Postgres.Storage
internal static class WorkflowCodes { const short OnSuccessJob = 1, OnAnyFinishedJob = 2, OnSuccessBatch = 3, OnAnyFinishedBatch = 4,
    Activation = 5, Succeeded = 2, Failed = 3, Cancelled = 4, Activated = 5, CancelMembers = 6, Subscribed = 7, BatchActive = 0,
    BatchAwaiting = 1; static short Code(ContinuationCondition, bool batchParent); static ContinuationCondition? Condition(short?);
    static JobId? ParentJob(short? code, Guid? parentId); static BatchId? ParentBatch(short? code, Guid? parentId); }      // Task 3
public sealed record LeasedJob(…, string? RecurringId = null, DateTime? ScheduledAtUtc = null, short? Continuation = null);   // Task 3
public sealed partial class PgBoltStore { internal ICompletionSignal? Completions { get; init; } }                       // Task 3
public sealed record EnqueueBatch(…) { Guid?[]? ParentIds; short?[]? Continuations; bool[]? Notify; Guid?[]? BatchIds; }  // Task 4, init
internal sealed record WorkflowBatchRow(Guid Id, Guid? OuterBatchId, Guid? ParentId, short? Condition, short State, int Total); // Task 4
internal readonly record struct WorkflowParent(Guid Id, bool IsBatch);                                                   // Task 4
internal sealed record WorkflowPlan(IReadOnlyList<WorkflowBatchRow> Batches, IReadOnlyList<EnqueueBatch> Jobs,
    IReadOnlyList<WorkflowParent> External) { Guid FirstNodeId; }                                                       // Task 4
internal readonly record struct WorkflowWriteResult(bool SignalResolver);                                                // Task 4
internal sealed class WorkflowAlreadyWrittenException : Exception;   // только внутри SubmitWorkflowAsync; наружу — InvalidOperationException // Task 4
internal sealed record CompletionCountResult(int Captured, int Finished, int Walking, IReadOnlyList<Guid> MissingBatches) { static Empty; } // Task 6
internal sealed record ResolveResult(int Captured, int Rows, int Statements, int MaxPortion, bool BudgetExhausted, int NewSignals,
    int Deferred, IReadOnlyCollection<string> Queues) { static Empty; }                                                  // Task 6
internal sealed record CancelMembersResult(int Portions, int Rows, int NewSignals, int TimedOut) { static Empty; }       // Task 7
internal enum ResolverStep { Walked, CancelPortion }                                                                     // Task 6
public sealed partial class PgBoltStore
{
    internal Task<WorkflowWriteResult> SubmitWorkflowAsync(WorkflowPlan plan, NpgsqlTransaction? transaction, CancellationToken ct); // Task 4
    internal Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct);                          // Task 6
    internal Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct);       // Task 6
    internal Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct);                                                // Task 6
    internal Func<ResolverStep, CancellationToken, Task>? ResolverHook { get; set; }   // только тесты: перед COMMIT Т2 / порции  // Task 6
    internal string CountSql { get; }  internal string WalkCaptureSql { get; }  internal string WalkJobsSql { get; }  internal string WalkBatchesSql { get; }   // для EXPLAIN-тестов // Task 6
    internal Task<BatchCancelResult> CancelBatchAsync(Guid batchId, CancellationToken ct);                                  // Task 7
    internal Task<BatchInfo?> GetBatchAsync(Guid batchId, CancellationToken ct);                                            // Task 7
    internal Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay,
        CancellationToken ct);                                                                                              // Task 7
}

// PgBolt.Postgres.Client.Workflows (Task 5)
internal sealed class WorkflowBuilder(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup, NpgsqlTransaction? transaction) : IWorkflow;
internal static class WorkflowPlanner { const int MaxRowsPerCommand = 10_000;
    static (WorkflowPlan Plan, IReadOnlyCollection<string> ReadyQueues) Build(IReadOnlyList<BatchNode>, IReadOnlyList<JobNode>,
        IReadOnlyList<ExternalNode>, IReadOnlyDictionary<string, JobDescriptor> dynamic, string? traceParent); }
// PgBoltJobClient: internal static Task EnsureReadCommittedAsync(NpgsqlTransaction, CancellationToken); internal static string? CurrentTraceParent()

// PgBolt.Postgres.Schema (Task 10)
internal enum RetentionStep { Listed, Prechecked, LocksChecked, WarmedUp, BatchA, BatchB, Committed, BatchesLocked, PrecheckOidChecked, BatchesWindowed }   // два последних — отклонения 8, 9
public sealed record RetentionResult(…, int AttachedJobsPartitions, int DeletedBatches = 0);
internal sealed partial class RetentionRunner { internal string AwaitingParentsSql(bool batches, string lo, string hi, string limit);
    internal string BatchMembersSql(string partition); internal string QueueAndBatchMembersSql(string partition); internal string BatchCandidatesSql(bool withCursor); }   // отклонения 8, 9

// PgBolt.Server (Task 12)
public sealed class PgBoltProcessingOptions { TimeSpan ResolverInterval = 1 с; }
internal interface IServerStore   // + к существующим членам
{
    Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct);
    Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct);
    Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay, CancellationToken ct);
    Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct);
}
internal sealed class ProcessingMaintenance(…, RecurringDeclarations? recurring = null, ICompletionSignal? completions = null)
{ internal const int ResolverBatchSize = 500, ResolverRowBudget = 5000, CancelMembersPortion = 1000, CancelMembersMaxTransactions = 100;
  internal static readonly TimeSpan ResolverLagCheckInterval /* 1 мин */; internal Task<int> ResolveOnceAsync(CancellationToken ct); }

// PgBolt.Generators (Task 11)
internal static class KnownNames { const string WorkflowScopeMetadata = "PgBolt.IWorkflowScope", NodeStepMetadata = "PgBolt.INodeStep",
    BatchBuilderMetadata = "PgBolt.IBatchBuilder", WorkflowMetadata = "PgBolt.IWorkflow";
    static bool IsWorkflowMethod(IMethodSymbol method, INamedTypeSymbol? scope, INamedTypeSymbol? node, INamedTypeSymbol? batchBuilder);
    static bool Implements(INamedTypeSymbol type, INamedTypeSymbol? contract); }
internal static class Diagnostics { static readonly DiagnosticDescriptor WorkflowNotSubmitted; /* BOLT009, Warning */ }
internal sealed class WorkflowSubmitAnalysis { static WorkflowSubmitAnalysis? TryCreate(Compilation); void AnalyzeBlock(OperationBlockAnalysisContext); }

// tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs
public sealed class RecordingCompletionSignal : ICompletionSignal { int Count; }                                         // Task 3
public enum WorkflowOutcome { Succeeded, Failed, Cancelled }                                                              // Task 6
public sealed record OpenTransaction(NpgsqlConnection Connection, NpgsqlTransaction Transaction) : IAsyncDisposable
{ Task CommitAsync(); }                                                                                                   // Task 9
public abstract class WorkflowTestBase : StoreTestBase { RecordingWakeup Wakeup; RecordingCompletionSignal Signal; PgBoltStore Flow;
    IJobClient Client; LeaseAsync, SucceedAsync, FailFinallyAsync, CancelJobAsync, AwaitAsync, ReadyAsync, StateAsync,
    BatchStateAsync, SignalsAsync;                                                                                          // Task 5
    Task FinishAsync(Guid id, WorkflowOutcome outcome); Task<string> FateAsync(Guid id);
    Task<(CompletionCountResult Counted, ResolveResult Walked)> ResolveOnceAsync(); Task ResolveAllAsync();                  // Task 6 (ResolveAllAsync заменяется в Task 7)
    Task ResolveWithoutCancelMembersAsync(); static void NextMillisecond(); Task<string> EventMessageAsync(Guid id);         // Task 7
    Task SucceedManyAsync(Guid[] ids);                                                                                      // Task 8
    Task<OpenTransaction> BeginAsync(); Task<NpgsqlConnection> HoldRowsAsync(string sql, params (string Name, object Value)[] ps);
    Task WaitForLockWaitersAsync(int count = 1); }                                                                          // Task 9
```

## Review Focus

Входы и условия, которые спека подразумевает, а тесты задач легко пропустить; тест на каждое уже добавлен в задачу-владельца:

1. **Ошибка `SubmitAsync` в пользовательской транзакции после SQL-ошибки, прервавшей её** (дубликат PK, конфликт `unique_key`, отсутствующий родитель после вставки детей) — пользователь ловит исключение и коммитит свою транзакцию: его строки есть, строк графа нет (Task 4, `User_transaction_survives_sql_error_inside_submit`; Task 5, `User_transaction_keeps_working_after_workflow_exceptions`).
2. **Повтор `SubmitAsync` после потерянного ответа на `COMMIT` и при ещё не закоммиченной предыдущей попытке** — успех без дубликатов, для графа с batches (23505 по `batches_pkey`) и без них (`exists_by_id`) (Task 4, `Retry_after_lost_commit_returns_success_without_duplicates`, `Retry_while_previous_attempt_is_in_flight_succeeds`).
3. **Ребёнок, строку которого держит открытая пользовательская транзакция (`After` на `Awaiting`-ребёнка)** — проход resolver'а не ждёт её, остальные сигналы обработаны, ребёнок разрешается после коммита (Task 6, `Signals_with_locked_children_are_deferred_and_do_not_block_next`; Task 9, `Subscription_holding_awaiting_child_does_not_stall_resolver`).
4. **Requeue члена во время идущей отмены batch** — член позади курсора выполняется, впереди — отменяется снова, флаг отмены не сброшен, ждущие продолжения ветки отменяются, а не запускаются (Task 8, `Requeue_during_cancel_behind_and_ahead_of_cursor`, `Nested_batch_requeued_during_outer_cancel`).
5. **Строка `batches`, удалённая retention'ом, на которую ещё ссылаются** (requeue оставшегося члена, сигнал завершения члена, подписка `After(BatchId)`) — без ошибок, `ParentNotFoundException` только для подписки (Task 8, `Requeue_of_member_of_deleted_batch_succeeds`; Task 10, `Retention_deleted_batch_references_are_harmless`, `Batch_subscription_versus_batch_deletion`).
6. **Динамический шаг с неизвестным именем задачи** (`Enqueue("nope", …)`) — `UnknownJobException` до любой записи; пользовательская транзакция не тронута (Task 5, `Unknown_dynamic_job_throws_before_writing`).
7. **Член batch, ставший живым по (б) между предпроверкой и блокировкой** (Pending-член отменён, Cancelled-член прошёл requeue и окончательный fail) — под блокировкой (б) не повторяется, `queue` и (а) пусты; партиция компактируется с членом (объединение с набором предпроверки), а не отсоединяется (Task 10, `Cancel_of_pending_member_between_precheck_and_lock_keeps_member`, `Requeue_and_final_fail_of_cancelled_member_between_precheck_and_lock_keeps_member`).

## Карта файлов

| Файл | Ответственность |
|---|---|
| `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql` | вся схема, включая колонки `queue`/`jobs` части 5, `batches`, `completions` и их индексы |
| `src/PgBolt.Abstractions/BatchId.cs`, `Workflow.cs`, `BatchInfo.cs`, `WorkflowExceptions.cs` | публичные контракты §5.1 |
| `src/PgBolt.Postgres/Client/ICompletionSignal.cs`, `InProcessCompletionSignal.cs` | in-process сигнал resolver'у (§6.4) |
| `src/PgBolt.Postgres/Storage/WorkflowCodes.cs` | хранимые коды части 5 и их отображение в API |
| `src/PgBolt.Postgres/Storage/WorkflowRecords.cs` | записи плана графа и результатов resolver'а / отмены |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Workflow.cs` | транзакция записи графа (§6.1), подписка (§6.2), повтор |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Resolver.cs` | Т1 (учёт), Т2 (обход), возраст старейшего сигнала, тестовый хук |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Batches.cs` | `CancelBatchAsync`, `GetBatchAsync`, `CancelMembers` (§6.5) |
| `src/PgBolt.Postgres/Client/Workflows/WorkflowGraph.cs` | узлы графа и области построителя |
| `src/PgBolt.Postgres/Client/Workflows/WorkflowBuilder.cs` | `IWorkflow`, шаги, `IBatchBuilder`, правила §5.2 |
| `src/PgBolt.Postgres/Client/Workflows/WorkflowPlanner.cs` | граф → `WorkflowPlan` (коды, состояния, `notify`, группы enqueue) |
| `src/PgBolt.Postgres/Schema/RetentionRunner.Batches.cs` | источники живых строк (а)/(б) и удаление строк `batches` (§7.1) |
| `src/PgBolt.Generators/Analysis/WorkflowSubmitAnalysis.cs` | BOLT009 (поток значений workflow в методе) |

Изменяемые файлы (без новой ответственности; подробности — в разделе **Files** задачи):

| Файл | Задача | Изменение |
|---|---|---|
| `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` | 1 | детектор пре-релизной схемы проверяет `batches` |
| `src/PgBolt.Abstractions/JobContext.cs`, `JobInfo.cs` | 2 | `BatchId?`, `ParentBatchId`, поля workflow в `JobInfo` |
| `src/PgBolt.Abstractions/IJobClient.cs` | 5, 7, 8 | `Workflow`; `CancelBatchAsync`, `GetBatchAsync`; XML-doc `RequeueAsync` |
| `src/PgBolt.Postgres/Storage/PgBoltStore.cs` | 3 | `Completions`, `SignalCompletions`, `QueryAndSignalAsync` |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Ack.cs`, `.Fail.cs`, `.Sweeper.cs`, `.Cancel.cs`, `.Release.cs` | 3 | дополнение финализации §6.3 |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Dequeue.cs`, `.Trigger.cs`, `.GetJob.cs`, `StoreRecords.cs` | 3, 4 | `continuation`, условие trigger, `Awaiting`, массивы workflow в `EnqueueBatch` |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs` | 4 | вариант enqueue с колонками части 5 |
| `src/PgBolt.Postgres/Storage/PgBoltStore.Requeue.cs` | 8 | `notify`, `batch_id`, синхронный учёт в `batches` (§6.6) |
| `src/PgBolt.Postgres/Client/PgBoltJobClient.cs` | 5, 7 | `Workflow`, `CancelBatchAsync`, `GetBatchAsync`; общие помощники |
| `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs` | 3 | `ICompletionSignal`, `PgBoltStore.Completions` |
| `src/PgBolt.Postgres/Schema/RetentionRunner.cs`, `RetentionRunner.Jobs.cs`, `RetentionTuning.cs`, `RetentionResult.cs` | 10 | (а)/(б), удаление `batches`, шаг хука, `DeletedBatches` |
| `src/PgBolt.Server/Runtime/JobRunner.cs` | 2, 3 | ссылки workflow в `JobContext` |
| `src/PgBolt.Server/PgBoltProcessingOptions.cs`, `Runtime/IServerStore.cs`, `PgBoltServerStore.cs`, `ProcessingMaintenance.cs`, `PgBoltProcessingService.cs`, `RetentionReport.cs` | 12 | цикл `resolver`, опция, лог удалённых `batches` |
| `src/PgBolt.Generators/KnownNames.cs`, `JobGenerator.cs`, `Models/EnqueueCallModel.cs`, `Analysis/AnalysisState.cs`, `Analysis/Diagnostics.cs`, `Analysis/PgBoltAnalyzer.cs` | 11 | метаданные workflow-вызовов, BOLT009 |
| `src/PgBolt.Generators/AnalyzerReleases.Unshipped.md` | 11 | строка BOLT009 |
| `CHANGELOG.md`, roadmap, спеки частей 1–5 | 14 | документация |

---
### Task 1: Объекты части 5 в V001 и детектор пре-релизной схемы

**Files:**
- Modify: `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql` (полная замена)
- Modify: `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` (`EnsureNotPreReleaseSchemaAsync`)
- Test: `tests/PgBolt.Postgres.Tests/MigrationTests.cs`, `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Produces: колонки `queue.parent_id uuid`, `queue.await_condition smallint`, `queue.batch_id uuid`, `queue.notify boolean NOT NULL DEFAULT false`; `jobs.continuation smallint`, `jobs.notify boolean NOT NULL DEFAULT false` (в конце списков колонок); индексы `queue_awaiting`, `queue_await_jobs`, `jobs_batch`; таблицы `batches` (индексы `batches_awaiting`, `batches_await_jobs`, `batches_nested`, `batches_open`, `batches_finished`) и `completions` (`completions_count`, `completions_walk`, `completions_cancel`) — точно §4 спеки. `PgBoltMigrations.LatestVersion` остаётся 1.

- [x] **Step 1: Тесты схемы**

В `tests/PgBolt.Postgres.Tests/MigrationTests.cs`:
- в `Migrate_creates_all_tables_once` добавить `"batches", "completions"` в массив таблиц;
- в `Script_contains_single_merged_migration` добавить `Assert.Contains("CREATE TABLE my_schema.batches", script);` и `Assert.Contains("CREATE TABLE my_schema.completions", script);`;
- добавить в класс помощники и тесты:

```csharp
    private async Task<string> ColumnsAsync(string table)
    {
        await using var cmd = _db.Admin.CreateCommand("""
            SELECT string_agg(attname || ':' || format_type(atttypid, atttypmod) || ':' || attnotnull, ',' ORDER BY attnum)
            FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND NOT attisdropped
            """);
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.{table}");
        return (string)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private Task<string> IndexesAsync(string table, string pkey) =>
        _db.ScalarAsync<string>(
            "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes WHERE schemaname = @s AND tablename = @t AND indexname <> @p",
            ("s", _db.Schema), ("t", table), ("p", pkey));

    [Fact]
    public async Task Queue_and_jobs_have_workflow_columns_and_indexes()   // часть 5 §4
    {
        await Runner().MigrateAsync(Ct);

        Assert.EndsWith(
            ",cancel_requested:boolean:true,parent_id:uuid:false,await_condition:smallint:false,batch_id:uuid:false,notify:boolean:true",
            await ColumnsAsync("queue"));
        Assert.EndsWith(
            ",recurring_id:text:false,scheduled_at:timestamp with time zone:false,continuation:smallint:false,notify:boolean:true",
            await ColumnsAsync("jobs"));
        Assert.Equal("queue_await_jobs,queue_awaiting,queue_delayed,queue_expiry,queue_fetch,queue_owner,queue_unique",
            await IndexesAsync("queue", "queue_pkey"));
        Assert.Equal("jobs_batch", await IndexesAsync("jobs", "jobs_pkey"));
        Assert.True(await _db.ScalarAsync<bool>("""
            SELECT pg_get_indexdef(to_regclass(@a)) LIKE '%(parent_id, job_id) WHERE (await_condition IS NOT NULL)'
               AND pg_get_indexdef(to_regclass(@j)) LIKE '%(batch_id, id) WHERE (batch_id IS NOT NULL)'
            """, ("a", $"{_db.Schema}.queue_awaiting"), ("j", $"{_db.Schema}.jobs_batch")));
    }

    [Fact]
    public async Task Batches_and_completions_have_spec_columns_and_indexes()   // часть 5 §4
    {
        await Runner().MigrateAsync(Ct);

        Assert.Equal(
            "id:uuid:true,parent_batch_id:uuid:false,parent_id:uuid:false,condition:smallint:false,state:smallint:true," +
            "total:integer:true,succeeded:integer:true,failed:integer:true,cancelled:integer:true,cancel_requested:boolean:true," +
            "created_at:timestamp with time zone:true,finished_at:timestamp with time zone:false",
            await ColumnsAsync("batches"));
        Assert.Equal("batches_await_jobs,batches_awaiting,batches_finished,batches_nested,batches_open",
            await IndexesAsync("batches", "batches_pkey"));
        Assert.Equal(
            "id:bigint:true,node_id:uuid:true,kind:smallint:true,batch_id:uuid:false,counted:boolean:true,stage:smallint:true," +
            "cursor:uuid:false,retry_after:timestamp with time zone:false,created_at:timestamp with time zone:true",
            await ColumnsAsync("completions"));
        Assert.Equal("completions_cancel,completions_count,completions_walk", await IndexesAsync("completions", "completions_pkey"));
        Assert.Contains("autovacuum_vacuum_threshold=1000",
            await _db.ScalarAsync<string[]>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.completions")));
    }

    [Fact]
    public async Task Jobs_partitions_inherit_workflow_columns_and_batch_index()
    {
        await Runner().MigrateAsync(Ct);
        await new PartitionManager(_db.Admin, _db.Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(1, Ct);

        Assert.Equal(0L, await _db.ScalarAsync<long>("""
            SELECT count(*) FROM pg_inherits i
            WHERE i.inhparent = to_regclass(@t)
              AND ((SELECT count(*) FROM pg_attribute a
                    WHERE a.attrelid = i.inhrelid AND a.attname IN ('continuation', 'notify') AND NOT a.attisdropped) <> 2
                   OR NOT EXISTS (SELECT 1 FROM pg_index x JOIN pg_inherits p ON p.inhrelid = x.indexrelid
                                  WHERE x.indrelid = i.inhrelid AND p.inhparent = to_regclass(@b)))
            """, ("t", $"{_db.Schema}.jobs"), ("b", $"{_db.Schema}.jobs_batch")));
        Assert.Equal(2L, await _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
    }
```

В `tests/PgBolt.Postgres.Tests/HostingTests.cs` рядом с `Pre_release_schema_fails_start` добавить:

```csharp
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Schema_without_workflow_tables_fails_start(bool autoMigrate)   // часть 5 §4: детектор проверяет batches
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await _db.ExecAsync("DROP TABLE {schema}.batches");                // схема сборки части 4: версия 1, объектов части 5 нет
        await using var sp = Build(o => o.AutoMigrate = autoMigrate);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));

        Assert.Contains("pre-release", e.Message);
        Assert.Contains($"DROP SCHEMA {_db.Schema} CASCADE", e.Message);
    }
```

- [x] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests"`
Expected: FAIL — нет таблиц `batches`/`completions` и колонок части 5; `Schema_without_workflow_tables_fails_start` падает на `DROP TABLE` (таблицы нет).

- [x] **Step 3: V001**

`src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql` — заменить целиком:

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
    cancel_requested boolean     NOT NULL DEFAULT false,
    parent_id        uuid,                                 -- часть 5: родитель Awaiting-строки (задача или batch); не очищается
    await_condition  smallint,                             -- часть 5: NULL — не ждёт; иначе код jobs.continuation (1–5)
    batch_id         uuid,                                 -- часть 5: копия jobs.batch_id (проверка отмены ветки)
    notify           boolean     NOT NULL DEFAULT false    -- часть 5: у задачи есть подписчики (продолжения или batch)
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 5000,
        autovacuum_vacuum_cost_limit = 2000);

CREATE INDEX queue_fetch   ON {schema}.queue (queue, priority DESC, run_at) WHERE lease_owner IS NULL AND ready;
CREATE INDEX queue_delayed ON {schema}.queue (run_at)                       WHERE NOT ready;
CREATE INDEX queue_owner   ON {schema}.queue (lease_owner)                  WHERE lease_owner IS NOT NULL;
CREATE INDEX queue_expiry  ON {schema}.queue (lease_until)                  WHERE lease_owner IS NOT NULL;
CREATE UNIQUE INDEX queue_unique ON {schema}.queue (unique_key)             WHERE unique_key IS NOT NULL;
CREATE INDEX queue_awaiting   ON {schema}.queue (parent_id, job_id) WHERE await_condition IS NOT NULL;
CREATE INDEX queue_await_jobs ON {schema}.queue (parent_id)         WHERE await_condition IN (1, 2);

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
```

- [x] **Step 4: Детектор пре-релизной схемы**

В `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` заменить метод `EnsureNotPreReleaseSchemaAsync`:

```csharp
    /// <summary>
    /// Часть 4 §3, часть 5 §4: миграции объединены в одну V001, схемы прошлых сборок (версия 1 или 2 без объектов частей 4–5)
    /// не обновляются. Признак такой схемы — нет recurring_jobs или batches после миграций и проверки версии.
    /// </summary>
    private async Task EnsureNotPreReleaseSchemaAsync(CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT to_regclass(@t) IS NULL OR to_regclass(@b) IS NULL", conn);
        cmd.Parameters.AddWithValue("t", $"{_options.Schema}.recurring_jobs");
        cmd.Parameters.AddWithValue("b", $"{_options.Schema}.batches");
        if ((bool)(await cmd.ExecuteScalarAsync(ct))!)
            throw new InvalidOperationException(
                $"PgBolt schema '{_options.Schema}' was created by a pre-release PgBolt build and cannot be upgraded; " +
                $"drop it (DROP SCHEMA {_options.Schema} CASCADE) and restart.");
    }
```

- [x] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~HostingTests|FullyQualifiedName~Retention|FullyQualifiedName~PartitionTests"`
Expected: PASS (Direct и Bouncer). Retention и партиции — регрессия: компактирование `SELECT *` и `LIKE` родителя переносят новые колонки, `ATTACH` строит `jobs_batch` на партиции.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs tests/PgBolt.Postgres.Tests/MigrationTests.cs tests/PgBolt.Postgres.Tests/HostingTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): batches, completions and workflow columns in V001"
```

---
### Task 2: Публичные контракты workflow, `JobContext` и `JobInfo`

**Files:**
- Create: `src/PgBolt.Abstractions/BatchId.cs`, `src/PgBolt.Abstractions/Workflow.cs`, `src/PgBolt.Abstractions/BatchInfo.cs`, `src/PgBolt.Abstractions/WorkflowExceptions.cs`
- Modify: `src/PgBolt.Abstractions/JobContext.cs`, `src/PgBolt.Abstractions/JobInfo.cs`, `src/PgBolt.Server/Runtime/JobRunner.cs` (тип `BatchId`)
- Modify (тесты): `tests/PgBolt.Abstractions.Tests/ContractsTests.cs`, `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs`
- Test: `tests/PgBolt.Abstractions.Tests/WorkflowContractsTests.cs`

**Interfaces:**
- Produces: все публичные типы §5.1 из «Контракта внутренних типов», кроме членов `IJobClient` (Task 5, Task 7); `JobContext(…, BatchId? batchId, …, BatchId? parentBatchId = null)`; `JobInfo(…, JobId? ParentId = null, BatchId? ParentBatchId = null, BatchId? BatchId = null, ContinuationCondition? Continuation = null)`.

- [x] **Step 1: Тесты контрактов**

`tests/PgBolt.Abstractions.Tests/WorkflowContractsTests.cs`:

```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;

namespace PgBolt.Abstractions.Tests;

public sealed class WorkflowContractsTests
{
    [Fact]
    public void BatchId_is_a_value_with_uuid_v7_factory()
    {
        var a = BatchId.New();
        Assert.Equal(7, a.Value.Version);
        Assert.Equal(a, new BatchId(a.Value));
        Assert.Equal(a.Value.ToString(), a.ToString());
    }

    [Fact]
    public void Enums_follow_stored_codes()   // §2.4: batches.state 0–4
    {
        Assert.Equal([0, 1, 2, 3, 4], Enum.GetValues<BatchState>().Select(s => (int)s));
        Assert.Equal([BatchState.Active, BatchState.Awaiting, BatchState.Succeeded, BatchState.Failed, BatchState.Cancelled],
            Enum.GetValues<BatchState>());
        Assert.Equal([ContinuationCondition.OnSuccess, ContinuationCondition.OnAnyFinished], Enum.GetValues<ContinuationCondition>());
        Assert.Equal([BatchCancelResult.NotFound, BatchCancelResult.AlreadyFinished, BatchCancelResult.CancellationRequested],
            Enum.GetValues<BatchCancelResult>());
    }

    [Fact]
    public void Exceptions_carry_ids()
    {
        var parent = Guid.CreateVersion7();
        var missing = new ParentNotFoundException(parent);
        Assert.IsAssignableFrom<InvalidOperationException>(missing);
        Assert.Equal(parent, missing.ParentId);
        Assert.Contains(parent.ToString(), missing.Message);

        var existing = JobId.New();
        var conflict = new UniqueKeyConflictException("k", existing);
        Assert.IsAssignableFrom<InvalidOperationException>(conflict);
        Assert.Equal("k", conflict.UniqueKey);
        Assert.Equal(existing, conflict.ExistingId);
        Assert.Contains("'k'", conflict.Message);
        Assert.Null(new UniqueKeyConflictException("k", null).ExistingId);
    }

    [Fact]
    public void JobContext_exposes_workflow_links()
    {
        var batch = BatchId.New();
        var parentBatch = BatchId.New();
        var ctx = new JobContext(JobId.New(), "n", 1, 3, null, batch, NullLogger.Instance, parentBatchId: parentBatch);
        Assert.Equal(batch, ctx.BatchId);
        Assert.Equal(parentBatch, ctx.ParentBatchId);
        Assert.Null(new JobContext(JobId.New(), "n", 1, 3, null, null, NullLogger.Instance).ParentBatchId);
    }

    [Fact]
    public void JobInfo_workflow_fields_default_to_null()
    {
        var info = new JobInfo(JobId.New(), "n", "q", 0, JobState.Awaiting, 0, 3, DateTimeOffset.UtcNow, null, null, null,
            null, null, false, default(JsonElement), []);
        Assert.Null(info.ParentId);
        Assert.Null(info.ParentBatchId);
        Assert.Null(info.BatchId);
        Assert.Null(info.Continuation);
    }

    [Fact]
    public void Options_and_results_are_value_records()
    {
        Assert.Equal(new StepOptions { Queue = "q", UniqueKey = "k" }, new StepOptions { Queue = "q", UniqueKey = "k" });
        Assert.Null(new WorkflowOptions().Transaction);
        var result = new WorkflowResult([JobId.New()], [BatchId.New()]);
        Assert.Single(result.Jobs);
        Assert.Single(result.Batches);
        var info = new BatchInfo(BatchId.New(), BatchState.Active, 3, 1, 0, 0, null, JobId.New(), null,
            ContinuationCondition.OnAnyFinished, false, DateTimeOffset.UtcNow, null);
        Assert.Equal((3, 1, 0, 0), (info.Total, info.Succeeded, info.Failed, info.Cancelled));
        Assert.Null(info.OuterBatchId);
        Assert.Equal(ContinuationCondition.OnAnyFinished, info.Condition);
    }
}
```

В `tests/PgBolt.Abstractions.Tests/ContractsTests.cs`, тест `JobContext_exposes_constructor_values`: заменить `var batch = Guid.NewGuid();` на `var batch = BatchId.New();`.

В `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs`, тест `Successful_job_is_acked_with_job_context`: заменить `(JobId?)null, (Guid?)null)` на `(JobId?)null, (BatchId?)null)`.

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet build PgBolt.slnx`
Expected: FAIL — нет `BatchId`, `ContinuationCondition`, `StepOptions`, `ParentNotFoundException` и др.

- [x] **Step 3: Контракты**

`src/PgBolt.Abstractions/BatchId.cs`:

```csharp
namespace PgBolt;

/// <summary>Идентификатор batch (UUIDv7, генерируется на клиенте при создании шага, часть 5 §5.2).</summary>
public readonly record struct BatchId(Guid Value)
{
    public static BatchId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString();
}
```

`src/PgBolt.Abstractions/Workflow.cs`:

```csharp
using System.Data.Common;
using System.Text.Json;

namespace PgBolt;

/// <summary>Условие продолжения (часть 5 §2.2): по исходу родителя — задачи или batch.</summary>
public enum ContinuationCondition
{
    /// <summary>Запуск после Succeeded; Failed — ждать requeue родителя; Cancelled — каскадная отмена.</summary>
    OnSuccess,

    /// <summary>Запуск после любого терминального исхода.</summary>
    OnAnyFinished,
}

public sealed record WorkflowOptions
{
    /// <summary>
    /// Запись графа в транзакции пользователя: только NpgsqlTransaction с уровнем READ COMMITTED (core §5.1). Любое исключение
    /// SubmitAsync откатывает записи графа до точки сохранения pgbolt_workflow — транзакция остаётся рабочей и не содержит строк
    /// графа. Коммитить сразу после SubmitAsync: After на задачу в queue держит блокировку её строки, на терминальную — строки
    /// jobs до коммита: до него Pending-родитель не берётся в работу, heartbeat, ack и fail сервера, выполняющего родителя, ждут
    /// (ожидание дольше LeaseDuration − HeartbeatInterval — «heartbeat потерян» и отмена всех выполнений сервера), RequeueAsync
    /// родителя ждёт, а отмена batch родителя стоит на его строке (часть 5 §6.2, §6.5).
    /// </summary>
    public DbTransaction? Transaction { get; init; }
}

/// <summary>EnqueueOptions без Transaction (часть 5 §5.1). Значения [Job] (или job_types) — умолчания.</summary>
public sealed record StepOptions
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }

    /// <summary>Только у корневых задач и членов batch без родителя; взаимоисключимо с Delay.</summary>
    public DateTimeOffset? RunAt { get; init; }

    /// <summary>Только у корневых задач и членов batch без родителя; отсчитывается от now() сервера БД.</summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>Не у членов batch; префикс pgbolt: зарезервирован; ключ уникален в пределах графа.</summary>
    public string? UniqueKey { get; init; }

    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}

/// <summary>Общий набор методов workflow и тела batch (часть 5 §5.1). Шаги описывают граф синхронно; запись — SubmitAsync.</summary>
public interface IWorkflowScope
{
    /// <remarks>Метаданные TArgs генератор регистрирует по месту вызова, как для EnqueueAsync.</remarks>
    IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null);

    IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null);

    /// <summary>Batch в этой области; его члены добавляются только внутри <paramref name="build"/>. Пустой batch — ArgumentException.</summary>
    IBatchStep StartBatch(Action<IBatchBuilder> build);
}

public interface IWorkflow : IWorkflowScope
{
    /// <summary>
    /// Продолжения существующей задачи. Задачи нет (или удалена retention'ом, или это id batch) — ParentNotFoundException при
    /// SubmitAsync. В пользовательской транзакции блокирует строку родителя до её коммита (см. WorkflowOptions.Transaction);
    /// если родитель — член batch, CancelMembers этого batch стоит на ней, остальные члены отменяются после освобождения.
    /// </summary>
    INodeStep After(JobId existing);

    /// <summary>
    /// Продолжения существующего batch. OnSuccess к Failed-batch ждёт requeue его Failed-членов: они хранятся, пока существует
    /// строка batch (часть 5 §7.1). Нет batch (или это id задачи) — ParentNotFoundException при SubmitAsync.
    /// </summary>
    INodeStep After(BatchId existing);

    /// <summary>Пишет граф одной транзакцией. Повторный вызов и любые методы workflow и его шагов после него — InvalidOperationException.</summary>
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);
}

public interface IBatchBuilder : IWorkflowScope
{
    IReadOnlyList<IJobStep> EnqueueMany<TArgs>(IReadOnlyList<TArgs> args, StepOptions? options = null);
}

/// <summary>Узел графа: задача, batch или существующий узел (After). Продолжения добавляются в область этого узла.</summary>
public interface INodeStep
{
    IJobStep ContinueWith<TArgs>(TArgs args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
        StepOptions? options = null);

    IJobStep ContinueWith(string jobName, JsonElement args,
        ContinuationCondition condition = ContinuationCondition.OnSuccess, StepOptions? options = null);

    IBatchStep ContinueWithBatch(Action<IBatchBuilder> build,
        ContinuationCondition condition = ContinuationCondition.OnSuccess);

    /// <summary>Делегирует workflow.</summary>
    ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default);
}

public interface IJobStep : INodeStep
{
    JobId Id { get; }
}

public interface IBatchStep : INodeStep
{
    BatchId Id { get; }
}

/// <summary>Id всех задач и batch графа в порядке создания шагов.</summary>
public sealed record WorkflowResult(IReadOnlyList<JobId> Jobs, IReadOnlyList<BatchId> Batches);
```

`src/PgBolt.Abstractions/BatchInfo.cs`:

```csharp
namespace PgBolt;

public enum BatchCancelResult { NotFound, AlreadyFinished, CancellationRequested }

/// <summary>Состояние batch (часть 5 §2.4); значения совпадают с batches.state.</summary>
public enum BatchState { Active, Awaiting, Succeeded, Failed, Cancelled }

/// <param name="Succeeded">Счётчики ≥ 0; между requeue члена и учётом его прежнего сигнала временно могут быть ниже факта (§6.6).</param>
/// <param name="OuterBatchId">Внешний batch (batches.parent_batch_id).</param>
/// <param name="ParentId">Родитель-задача batch-продолжения.</param>
/// <param name="ParentBatchId">Родитель-batch batch-продолжения.</param>
public sealed record BatchInfo(
    BatchId Id,
    BatchState State,
    int Total,
    int Succeeded,
    int Failed,
    int Cancelled,
    BatchId? OuterBatchId,
    JobId? ParentId,
    BatchId? ParentBatchId,
    ContinuationCondition? Condition,
    bool CancellationRequested,
    DateTimeOffset CreatedAt,
    DateTimeOffset? FinishedAt);
```

`src/PgBolt.Abstractions/WorkflowExceptions.cs`:

```csharp
namespace PgBolt;

/// <summary>Родителя After нет: не создавался, другого вида (задача ↔ batch) или удалён retention'ом. Записи графа откатаны.</summary>
public sealed class ParentNotFoundException(Guid parentId)
    : InvalidOperationException($"Workflow parent {parentId} does not exist: it was never created, is of the other kind (job vs batch) " +
                                "or was removed by retention. Nothing of the workflow was written.")
{
    public Guid ParentId { get; } = parentId;
}

/// <summary>Ключ шага workflow занят активной задачей (ExistingId; null — она успела завершиться). Записи графа откатаны.</summary>
public sealed class UniqueKeyConflictException(string uniqueKey, JobId? existingId)
    : InvalidOperationException($"Unique key '{uniqueKey}' is held by active job {existingId?.ToString() ?? "(already finished)"}; " +
                                "nothing of the workflow was written.")
{
    public string UniqueKey { get; } = uniqueKey;
    public JobId? ExistingId { get; } = existingId;
}
```

`src/PgBolt.Abstractions/JobContext.cs` — заменить целиком:

```csharp
using Microsoft.Extensions.Logging;

namespace PgBolt;

public sealed class JobContext(JobId jobId, string jobName, int attempt, int maxAttempts, JobId? parentId, BatchId? batchId, ILogger logger,
    DateTimeOffset? scheduledAt = null, string? recurringJobId = null, BatchId? parentBatchId = null)
{
    public JobId JobId { get; } = jobId;
    public string JobName { get; } = jobName;
    public int Attempt { get; } = attempt;
    public int MaxAttempts { get; } = maxAttempts;

    /// <summary>Родитель-задача продолжения (часть 5 §5.3); null у задач без родителя, у продолжений batch и членов batch-продолжения.</summary>
    public JobId? ParentId { get; } = parentId;

    /// <summary>Batch, членом которого задача является (часть 5).</summary>
    public BatchId? BatchId { get; } = batchId;

    public ILogger Logger { get; } = logger;

    /// <summary>Момент срабатывания cron, за который поставлена задача (часть 4); null — задача не из расписания.</summary>
    public DateTimeOffset? ScheduledAt { get; } = scheduledAt;

    /// <summary>Id расписания (часть 4); null — задача не из расписания.</summary>
    public string? RecurringJobId { get; } = recurringJobId;

    /// <summary>Родитель-batch продолжения (часть 5 §5.3); у членов batch-продолжения null (их batch — BatchId).</summary>
    public BatchId? ParentBatchId { get; } = parentBatchId;
}
```

`src/PgBolt.Abstractions/JobInfo.cs` — в записи `JobInfo` после `DateTimeOffset? ScheduledAt = null` добавить параметры (запятая после `ScheduledAt = null`):

```csharp
    JobId? ParentId = null,
    BatchId? ParentBatchId = null,
    BatchId? BatchId = null,
    ContinuationCondition? Continuation = null);
```

и XML-комментарий над записью:

```csharp
/// <summary>
/// Задача для клиента. Awaiting — строка queue ждёт родителя (RunAt = null). ParentId — родитель-задача (continuation 1, 2),
/// ParentBatchId — родитель-batch (3, 4); у члена batch-продолжения (5) оба null, BatchId заполнен. Continuation: 1/3 — OnSuccess,
/// 2/4 — OnAnyFinished, 5 и NULL — null (часть 5 §5.3).
/// </summary>
```

`src/PgBolt.Server/Runtime/JobRunner.cs`, метод `ExecuteAsync`: в создании `JobContext` заменить аргумент `job.BatchId` на `job.BatchId is { } batch ? new BatchId(batch) : null` (родитель по коду continuation — Task 3).

- [x] **Step 4: Запустить — проходят**

Run: `dotnet build PgBolt.slnx` → без ошибок и предупреждений. Затем `dotnet test tests/PgBolt.Abstractions.Tests` и `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~JobRunnerTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Abstractions src/PgBolt.Server/Runtime/JobRunner.cs tests/PgBolt.Abstractions.Tests tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): public workflow contracts"
```

---
### Task 3: Сигналы завершения в финализации, `ICompletionSignal`, `Awaiting` в горячем пути

**Files:**
- Create: `src/PgBolt.Postgres/Client/ICompletionSignal.cs`, `src/PgBolt.Postgres/Client/InProcessCompletionSignal.cs`, `src/PgBolt.Postgres/Storage/WorkflowCodes.cs`
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.cs`, `PgBoltStore.Ack.cs`, `PgBoltStore.Fail.cs`, `PgBoltStore.Sweeper.cs`, `PgBoltStore.Cancel.cs`, `PgBoltStore.Release.cs`, `PgBoltStore.Dequeue.cs`, `PgBoltStore.Trigger.cs`, `PgBoltStore.GetJob.cs`, `StoreRecords.cs`
- Modify: `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`, `src/PgBolt.Server/Runtime/JobRunner.cs`
- Test: `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` (новый), `tests/PgBolt.Postgres.Tests/WorkflowHotPathTests.cs`, `tests/PgBolt.Postgres.Tests/CompletionSignalTests.cs`; Modify: `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs`

**Interfaces:**
- Consumes: схема Task 1; `BatchId`, `ContinuationCondition`, поля `JobInfo` / `JobContext` (Task 2).
- Produces: `ICompletionSignal`, `InProcessCompletionSignal` (публичные, `PgBolt.Postgres.Client`); `WorkflowCodes`; `PgBoltStore.Completions { get; init; }`; приватные `SignalCompletions(long)` и `QueryAndSignalAsync<T>` (сигнал после завершения autocommit-statement'а, не из делегата чтения); `LeasedJob.Continuation`; DI: синглтон `ICompletionSignal` и `PgBoltStore.Completions` = он; тестовый `RecordingCompletionSignal`.

- [x] **Step 1: Тестовая поддержка и тесты**

`tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs`:

```csharp
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Tests;

/// <summary>Считает in-process сигналы resolver'у (§6.3: signals &gt; 0 → ICompletionSignal.Signal()).</summary>
public sealed class RecordingCompletionSignal : ICompletionSignal
{
    private int _count;

    public int Count => Volatile.Read(ref _count);

    public void Signal() => Interlocked.Increment(ref _count);

    public IDisposable Subscribe(Action callback) => throw new NotSupportedException("Tests only count signals.");
}
```

`tests/PgBolt.Postgres.Tests/CompletionSignalTests.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Tests;

public sealed class CompletionSignalTests
{
    [Fact]
    public void Signal_calls_subscribers_until_disposed_and_isolates_failures()
    {
        var signal = new InProcessCompletionSignal();
        var calls = 0;
        using var broken = signal.Subscribe(() => throw new InvalidOperationException("subscriber failure"));
        var subscription = signal.Subscribe(() => calls++);

        signal.Signal();
        subscription.Dispose();
        subscription.Dispose();          // идемпотентно
        signal.Signal();

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AddPgBolt_shares_completion_signal_with_store()
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o => o.ConnectionString = "Host=unused");
        await using var sp = services.BuildServiceProvider();

        var signal = sp.GetRequiredService<ICompletionSignal>();

        Assert.IsType<InProcessCompletionSignal>(signal);
        Assert.Same(signal, sp.GetRequiredService<PgBoltStore>().Completions);
    }
}
```

`tests/PgBolt.Postgres.Tests/WorkflowHotPathTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.3: дополнение финализации, Awaiting в dequeue / promoter / trigger / GetJob / cancel.</summary>
public abstract class WorkflowHotPathTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private readonly RecordingCompletionSignal _signal = new();
    private PgBoltStore _store = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        _store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true) { Completions = _signal };
    }

    /// <summary>Pending-задача с подписчиками: queue.notify = true; batch — jobs.batch_id и queue.batch_id.</summary>
    private async Task<Guid> SubscribedAsync(Guid? batch = null, int maxAttempts = 3)
    {
        var id = await EnqueueAsync(maxAttempts: maxAttempts);
        await Db.ExecAsync("UPDATE {schema}.queue SET notify = true, batch_id = @b WHERE job_id = @id", ("id", id), ("b", batch));
        await Db.ExecAsync("UPDATE {schema}.jobs SET batch_id = @b WHERE id = @id", ("id", id), ("b", batch));
        return id;
    }

    /// <summary>Берёт в lease именно эту задачу (в обход порядка очереди); возвращает lease_id.</summary>
    private Task<Guid> LeaseAsync(Guid id) =>
        Db.ScalarAsync<Guid>("""
            UPDATE {schema}.queue
            SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '5 minutes', leased_at = now(),
                attempt = attempt + 1
            WHERE job_id = @id AND lease_owner IS NULL AND ready
            RETURNING lease_id
            """, ("id", id), ("s", ServerA));

    private Task<string?> SignalsAsync() =>
        Db.ScalarAsync<string?>("""
            SELECT string_agg(node_id || ':' || kind || ':' || coalesce(batch_id::text, '-') || ':' || counted || ':' || stage, ',' ORDER BY id)
            FROM {schema}.completions
            """);

    [Fact]
    public async Task Ack_with_notify_writes_signal_with_batch_and_wakes_resolver()
    {
        var batch = Guid.CreateVersion7();
        var id = await SubscribedAsync(batch);
        var lease = await LeaseAsync(id);

        Assert.Equal(1, await _store.AckAsync(ServerA, [new LeaseRef(id, lease)], Ct));

        Assert.Equal($"{id}:2:{batch}:false:0", await SignalsAsync());
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.jobs WHERE id = @id", ("id", id)));
        Assert.Equal(1, _signal.Count);
    }

    [Fact]
    public async Task Finalization_without_notify_writes_no_signal()   // §12 «Горячий путь»
    {
        var acked = await EnqueueAsync();
        var failed = await EnqueueAsync(maxAttempts: 1);
        var cancelled = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        var a = await LeaseAsync(acked);
        var f = await LeaseAsync(failed);

        Assert.Equal(1, await _store.AckAsync(ServerA, [new LeaseRef(acked, a)], Ct));
        Assert.Equal(FailOutcome.Failed, await _store.FailAsync(failed, f, ServerA, JobEventKind.Failed, "x", null, Ct));
        Assert.Equal(CancelResult.Cancelled, await _store.CancelAsync(cancelled, null, Ct));

        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions"));
        Assert.Equal(0, _signal.Count);
    }

    [Theory]
    [InlineData(false, false, 3)]   // окончательный fail
    [InlineData(true, false, 4)]    // fail при запрошенной отмене
    [InlineData(false, true, 4)]    // подтверждение отмены
    public async Task Final_fail_and_confirm_cancel_signal_final_state(bool cancelRequested, bool confirm, short kind)
    {
        var id = await SubscribedAsync();
        var lease = await LeaseAsync(id);
        if (cancelRequested)
            await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

        _ = confirm
            ? await _store.ConfirmCancelAsync(id, lease, ServerA, "stop", Ct)
            : await _store.FailAsync(id, lease, ServerA, JobEventKind.Failed, "x", null, Ct);

        Assert.Equal($"{id}:{kind}:-:false:0", await SignalsAsync());
        Assert.Equal(kind, await JobStateAsync(id));
        Assert.Equal(1, _signal.Count);
    }

    [Fact]
    public async Task Retry_signals_only_its_cancel_branch()
    {
        var retried = await SubscribedAsync();
        var r = await LeaseAsync(retried);
        Assert.Equal(FailOutcome.Retried, await _store.RetryAsync(retried, r, ServerA, TimeSpan.Zero, JobEventKind.Retry, "x", null, Ct));
        Assert.Null(await SignalsAsync());
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", retried)));   // строка осталась вместе с notify

        var c = await LeaseAsync(retried);
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", retried));
        Assert.Equal(FailOutcome.Cancelled, await _store.RetryAsync(retried, c, ServerA, TimeSpan.Zero, JobEventKind.Retry, "x", null, Ct));

        Assert.Equal($"{retried}:4:-:false:0", await SignalsAsync());
    }

    [Fact]
    public async Task Sweeper_signals_final_and_cancelled_expiry_but_not_reset()
    {
        var exhausted = await SubscribedAsync(maxAttempts: 1);
        var cancelled = await SubscribedAsync();
        var reset = await SubscribedAsync();
        foreach (var id in new[] { exhausted, cancelled, reset })
        {
            await LeaseAsync(id);
            await ExpireLeaseAsync(id);
        }
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", cancelled));

        Assert.Equal(3, await _store.SweepExpiredLeasesAsync(100, Ct));

        Assert.Equal(new[] { $"{exhausted}:3", $"{cancelled}:4" }.Order(),
            (await Db.ScalarAsync<string>("SELECT string_agg(node_id || ':' || kind, ',') FROM {schema}.completions")).Split(',').Order());
        Assert.True(await InQueueAsync(reset));
        Assert.Equal(1, _signal.Count);
    }

    [Fact]
    public async Task Cancel_of_unleased_job_signals_cancelled()
    {
        var id = await SubscribedAsync();

        Assert.Equal(CancelResult.Cancelled, await _store.CancelAsync(id, "stop", Ct));

        Assert.Equal($"{id}:4:-:false:0", await SignalsAsync());
        Assert.Equal(1, _signal.Count);
    }

    [Fact]
    public async Task Release_signals_only_when_cancelled()
    {
        var id = await SubscribedAsync();
        var first = await LeaseAsync(id);
        Assert.Equal(ReleaseOutcome.Released, await _store.ReleaseAsync(id, first, ServerA, ran: true, null, Ct));
        Assert.Null(await SignalsAsync());

        var second = await LeaseAsync(id);
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));
        Assert.Equal(ReleaseOutcome.Cancelled, await _store.ReleaseAsync(id, second, ServerA, ran: true, "shutdown", Ct));

        Assert.Equal($"{id}:4:-:false:0", await SignalsAsync());
    }

    [Fact]
    public async Task Dequeue_returns_continuation_code()
    {
        var parent = Guid.CreateVersion7();
        var id = await EnqueueAsync();
        await Db.ExecAsync("UPDATE {schema}.jobs SET continuation = 3, parent_id = @p WHERE id = @id", ("id", id), ("p", parent));

        var job = await LeaseOneAsync();

        Assert.Equal(((short?)3, (Guid?)parent), (job.Continuation, job.ParentId));
    }

    /// <summary>Awaiting-строка в обход API (инвариант §4): ready = false, run_at = infinity, await_condition = code.</summary>
    private async Task<Guid> AwaitingAsync(Guid parent, short code = 1, Guid? batch = null)
    {
        var id = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition, batch_id)
            VALUES (@id, 'q', 0, 'infinity', false, 3, @p, @c, @b);
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, parent_id, batch_id, continuation)
            VALUES (@id, 'test.job', 'q', 0, '{}', 0, 3, now(), @p, @b, @c);
            """, ("id", id), ("p", parent), ("c", code), ("b", batch));
        return id;
    }

    [Fact]
    public async Task Awaiting_row_is_not_dequeued_promoted_or_triggered_and_reads_as_awaiting()   // §12 SQL-контракт
    {
        var parent = Guid.CreateVersion7();
        var id = await AwaitingAsync(parent);

        Assert.Empty(await _store.DequeueAsync("q", 10, ServerA, Lease, Ct));
        Assert.Equal(0, (await _store.PromoteDueAsync(100, Ct)).Promoted);
        Assert.Null(await _store.TriggerAsync(id, Ct));
        Assert.Equal(0L, await Db.ScalarAsync<long>(
            "SELECT count(*) FROM {schema}.queue WHERE lease_owner IS NULL AND ready AND await_condition IS NOT NULL"));   // queue_fetch

        var info = await _store.GetJobAsync(id, Ct);
        Assert.NotNull(info);
        Assert.Equal(JobState.Awaiting, info.State);
        Assert.Null(info.RunAt);
        Assert.Equal(new JobId(parent), info.ParentId);
        Assert.Equal(ContinuationCondition.OnSuccess, info.Continuation);
    }

    [Fact]
    public async Task Cancel_of_awaiting_job_cancels_it()   // §6.7
    {
        var id = await AwaitingAsync(Guid.CreateVersion7());

        Assert.Equal(CancelResult.Cancelled, await _store.CancelAsync(id, "no", Ct));

        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.False(await InQueueAsync(id));
    }

    [Theory]
    [InlineData((short)1, true, false, ContinuationCondition.OnSuccess)]
    [InlineData((short)2, true, false, ContinuationCondition.OnAnyFinished)]
    [InlineData((short)3, false, true, ContinuationCondition.OnSuccess)]
    [InlineData((short)4, false, true, ContinuationCondition.OnAnyFinished)]
    [InlineData((short)5, false, false, null)]
    public async Task JobInfo_maps_continuation_codes(short code, bool jobParent, bool batchParent, ContinuationCondition? condition)   // §5.3
    {
        var parent = Guid.CreateVersion7();
        var batch = Guid.CreateVersion7();
        var id = await AwaitingAsync(code == 5 ? batch : parent, code, batch);

        var info = (await _store.GetJobAsync(id, Ct))!;

        Assert.Equal(jobParent ? new JobId(parent) : null, info.ParentId);
        Assert.Equal(batchParent ? new BatchId(parent) : null, info.ParentBatchId);
        Assert.Equal(new BatchId(batch), info.BatchId);
        Assert.Equal(condition, info.Continuation);
    }

    [Fact]
    public async Task Scheduled_job_is_still_scheduled()
    {
        var id = await EnqueueAsync(delay: TimeSpan.FromHours(1));

        Assert.Equal(JobState.Scheduled, (await _store.GetJobAsync(id, Ct))!.State);
    }
}

public sealed class WorkflowHotPathTests_Direct(PostgresFixture pg) : WorkflowHotPathTests(pg, ConnectionMode.Direct);
public sealed class WorkflowHotPathTests_Bouncer(PostgresFixture pg) : WorkflowHotPathTests(pg, ConnectionMode.Bouncer);
```

В `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs` рядом с тестом полей расписания (`Leased.Job(...) with { RecurringId = ... }`) добавить:

```csharp
    [Theory]
    [InlineData((short)1, true, false)]
    [InlineData((short)2, true, false)]
    [InlineData((short)3, false, true)]
    [InlineData((short)4, false, true)]
    [InlineData((short)5, false, false)]
    public async Task Workflow_links_follow_continuation_code(short code, bool jobParent, bool batchParent)   // часть 5 §5.3
    {
        var parent = Guid.CreateVersion7();
        var batch = Guid.CreateVersion7();
        var job = Leased.Job(type: "runner.job", args: """{"N":1}""") with { ParentId = parent, BatchId = batch, Continuation = code };

        await Runner().RunAsync(new Execution(job, "q", 0, ExecutionState.Running), CancellationToken.None);

        var context = Assert.Single(_probe.Contexts);
        Assert.Equal(jobParent ? new JobId(parent) : null, context.ParentId);
        Assert.Equal(batchParent ? new BatchId(parent) : null, context.ParentBatchId);
        Assert.Equal(new BatchId(batch), context.BatchId);
    }
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet build PgBolt.slnx`
Expected: FAIL — нет `ICompletionSignal`, `InProcessCompletionSignal`, `PgBoltStore.Completions`, `LeasedJob.Continuation`.

- [x] **Step 3: Сигнал и коды**

`src/PgBolt.Postgres/Client/ICompletionSignal.cs`:

```csharp
namespace PgBolt.Postgres.Client;

/// <summary>
/// Пробуждение resolver'а процесса после записи сигналов завершения (часть 5 §6.4): финализация с подписчиками,
/// CancelBatchAsync, синтетические сигналы SubmitAsync. Сигнал не гарантирован — resolver страхуется интервалом.
/// </summary>
public interface ICompletionSignal
{
    void Signal();

    /// <summary>callback вызывается синхронно из Signal; должен быть дешёвым и не блокировать.</summary>
    /// <returns>Подписка; Dispose отписывает (идемпотентно).</returns>
    IDisposable Subscribe(Action callback);
}
```

`src/PgBolt.Postgres/Client/InProcessCompletionSignal.cs`:

```csharp
namespace PgBolt.Postgres.Client;

/// <summary>In-process сигнал resolver'у: подписчики в copy-on-write массиве, Signal без блокировок и аллокаций.</summary>
public sealed class InProcessCompletionSignal : ICompletionSignal
{
    private readonly Lock _gate = new();
    private Action[] _subscribers = [];

    public void Signal()
    {
        foreach (var callback in Volatile.Read(ref _subscribers))
        {
            try
            {
                callback();
            }
            catch (Exception)
            {
                // Ошибка подписчика не должна доходить до вызывающей финализации; остальные подписчики вызываются.
            }
        }
    }

    public IDisposable Subscribe(Action callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        lock (_gate)
            Volatile.Write(ref _subscribers, [.. _subscribers, callback]);
        return new Subscription(this, callback);
    }

    private void Unsubscribe(Action callback)
    {
        lock (_gate)
        {
            var index = Array.IndexOf(_subscribers, callback);
            if (index >= 0)
                Volatile.Write(ref _subscribers, [.. _subscribers.AsSpan(0, index), .. _subscribers.AsSpan(index + 1)]);
        }
    }

    private sealed class Subscription(InProcessCompletionSignal owner, Action callback) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                owner.Unsubscribe(callback);
        }
    }
}
```

`src/PgBolt.Postgres/Storage/WorkflowCodes.cs`:

```csharp
namespace PgBolt.Postgres.Storage;

/// <summary>
/// Хранимые коды части 5 (§2.4, §4): jobs.continuation = queue.await_condition = batches.condition; completions.kind;
/// batches.state (Active/Awaiting; терминальные совпадают с jobs.state и с kind исхода).
/// </summary>
internal static class WorkflowCodes
{
    public const short OnSuccessJob = 1;
    public const short OnAnyFinishedJob = 2;
    public const short OnSuccessBatch = 3;
    public const short OnAnyFinishedBatch = 4;
    public const short Activation = 5;

    public const short Succeeded = 2;
    public const short Failed = 3;
    public const short Cancelled = 4;
    public const short Activated = 5;
    public const short CancelMembers = 6;
    public const short Subscribed = 7;

    public const short BatchActive = 0;
    public const short BatchAwaiting = 1;

    public static short Code(ContinuationCondition condition, bool batchParent) => (condition, batchParent) switch
    {
        (ContinuationCondition.OnSuccess, false) => OnSuccessJob,
        (ContinuationCondition.OnAnyFinished, false) => OnAnyFinishedJob,
        (ContinuationCondition.OnSuccess, true) => OnSuccessBatch,
        (ContinuationCondition.OnAnyFinished, true) => OnAnyFinishedBatch,
        _ => throw new ArgumentOutOfRangeException(nameof(condition), condition, "Unknown continuation condition."),
    };

    /// <summary>1/3 → OnSuccess, 2/4 → OnAnyFinished, 5 и NULL → null (§5.3).</summary>
    public static ContinuationCondition? Condition(short? code) => code switch
    {
        OnSuccessJob or OnSuccessBatch => ContinuationCondition.OnSuccess,
        OnAnyFinishedJob or OnAnyFinishedBatch => ContinuationCondition.OnAnyFinished,
        _ => null,
    };

    /// <summary>Родитель-задача: только коды 1, 2.</summary>
    public static JobId? ParentJob(short? code, Guid? parentId) =>
        code is OnSuccessJob or OnAnyFinishedJob && parentId is { } parent ? new JobId(parent) : null;

    /// <summary>Родитель-batch: только коды 3, 4 (у члена batch-продолжения, код 5, — null).</summary>
    public static BatchId? ParentBatch(short? code, Guid? parentId) =>
        code is OnSuccessBatch or OnAnyFinishedBatch && parentId is { } parent ? new BatchId(parent) : null;
}
```

- [x] **Step 4: Store: свойство сигнала и записи**

В `src/PgBolt.Postgres/Storage/PgBoltStore.cs` добавить `using PgBolt.Postgres.Client;` и члены класса:

```csharp
    /// <summary>In-process сигнал resolver'у (часть 5 §6.3); null — не подавать (хранилище без DI, тесты).</summary>
    internal ICompletionSignal? Completions { get; init; }

    /// <summary>§6.3: statement финализации записал сигналы — разбудить resolver процесса.</summary>
    private void SignalCompletions(long signals)
    {
        if (signals > 0)
            Completions?.Signal();
    }

    /// <summary>
    /// QueryAsync autocommit-statement'а финализации (§6.3): делегат возвращает результат и число записанных сигналов, сигнал
    /// подаётся после завершения QueryAsync. Внутри делегата ридер открыт, а коммит неявной транзакции наступает только на Sync
    /// после строк результата: resolver, разбуженный раньше, сигнала не увидит и прождёт ResolverInterval.
    /// </summary>
    private async Task<T> QueryAndSignalAsync<T>(string sql, Action<NpgsqlParameterCollection> bind,
        Func<NpgsqlDataReader, CancellationToken, Task<(T Result, long Signals)>> read, CancellationToken ct)
    {
        var (result, signals) = await QueryAsync(sql, bind, read, ct);
        SignalCompletions(signals);
        return result;
    }
```

`SignalCompletions` вызывается только после коммита: в `QueryAndSignalAsync` (после `QueryAsync`, ридер закрыт) или после `CommitAsync` явной транзакции; из делегата чтения ридера — никогда.

В `src/PgBolt.Postgres/Storage/StoreRecords.cs` в `LeasedJob` после `DateTime? ScheduledAtUtc = null` добавить параметр `short? Continuation = null` (часть 5: код `jobs.continuation`, по нему `JobContext.ParentId` / `ParentBatchId`).

- [x] **Step 5: Финализация (§6.3)**

`src/PgBolt.Postgres/Storage/PgBoltStore.Ack.cs` — заменить шаблон и метод:

```csharp
    private const string AckTemplate = """
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
        SELECT (SELECT count(*) FROM j) AS acked, (SELECT count(*) FROM s) AS signals
        """;

    private string AckSql => field ??= Render(AckTemplate);

    /// <summary>Пакетный ack (§5.3). Задачи с потерянным lease игнорируются. Возвращает число подтверждённых; сигналы — часть 5 §6.3.</summary>
    public Task<int> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct)
    {
        if (leases.Count == 0)
            return Task.FromResult(0);
        var ids = new Guid[leases.Count];
        var leaseIds = new Guid[leases.Count];
        for (var i = 0; i < leases.Count; i++)
            (ids[i], leaseIds[i]) = (leases[i].JobId, leases[i].LeaseId);

        return QueryAndSignalAsync<int>(AckSql, ps =>
        {
            P.Add(ps, "ids", ids);
            P.Add(ps, "lease_ids", leaseIds);
            P.Add(ps, "server", serverId);
        }, async (r, c) =>
        {
            await r.ReadAsync(c);
            return ((int)r.GetInt64(0), r.GetInt64(1));
        }, ct);
    }
```

`src/PgBolt.Postgres/Storage/PgBoltStore.Fail.cs` — заменить оба шаблона и чтение результата в `FailStatementAsync`:

```csharp
    private const string RetryTemplate = """
        WITH l AS MATERIALIZED (
            SELECT job_id, cancel_requested FROM {schema}.queue
            WHERE job_id = @id AND lease_id = @lease_id
            FOR UPDATE
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
            RETURNING q.job_id, q.attempt, q.leased_at, q.notify
        ),
        cj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = c.attempt, started_at = c.leased_at,
                                       finished_at = now(), server_id = @server, notify = j.notify OR c.notify
            FROM c WHERE j.id = c.job_id AND j.id = @id
            RETURNING j.id, j.batch_id, c.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 4, batch_id FROM cj WHERE notify
            RETURNING 1
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message, exception)
            SELECT job_id, now(), @kind, @server, attempt, @message, @exception FROM r
            UNION ALL
            SELECT job_id, now(), @kind_cancelled, @server, attempt, @message, @exception FROM c
            RETURNING kind
        )
        SELECT kind, (SELECT count(*) FROM s) AS signals FROM ev
        """;

    private const string FinalFailTemplate = """
        WITH d AS (
            DELETE FROM {schema}.queue WHERE job_id = @id AND lease_id = @lease_id
            RETURNING job_id, attempt, leased_at, cancel_requested, notify
        ),
        j AS (
            UPDATE {schema}.jobs
            SET state = CASE WHEN d.cancel_requested THEN 4 ELSE @state END,
                attempt = d.attempt, started_at = d.leased_at, finished_at = now(), server_id = @server,
                notify = jobs.notify OR d.notify
            FROM d WHERE jobs.id = d.job_id AND jobs.id = @id
            RETURNING jobs.id, jobs.batch_id, jobs.state, d.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, state, batch_id FROM j WHERE notify
            RETURNING 1
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message, exception)
            SELECT job_id, now(), CASE WHEN cancel_requested THEN @kind_cancelled ELSE @kind END,
                   @server, attempt, @message, @exception
            FROM d
            RETURNING kind
        )
        SELECT kind, (SELECT count(*) FROM s) AS signals FROM ev
        """;
```

и в `FailStatementAsync` заменить вызов `QueryAsync(sql, ps =>` на `QueryAndSignalAsync<FailOutcome>(sql, ps =>` (привязка параметров без изменений) и делегат чтения:

```csharp
        }, async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return (FailOutcome.LeaseLost, 0L);
            var outcome = r.GetInt16(0) == (short)JobEventKind.Cancelled && kind != JobEventKind.Cancelled
                ? FailOutcome.Cancelled
                : normalOutcome;
            return (outcome, r.GetInt64(1));
        }, ct);
```

`src/PgBolt.Postgres/Storage/PgBoltStore.Sweeper.cs` — заменить шаблон, вызов `QueryAsync(SweepSql, ps =>` на `QueryAndSignalAsync<int>(SweepSql, ps =>` и делегат чтения:

```csharp
    private const string SweepTemplate = """
        WITH e AS (
            SELECT job_id, attempt, max_attempts, lease_owner, cancel_requested FROM {schema}.queue
            WHERE lease_owner IS NOT NULL AND lease_until < now()
            LIMIT @batch
            FOR UPDATE SKIP LOCKED
        ),
        reset AS (
            UPDATE {schema}.queue q SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL
            FROM e WHERE q.job_id = e.job_id AND e.attempt < e.max_attempts AND NOT e.cancel_requested
        ),
        fin AS (
            DELETE FROM {schema}.queue q USING e
            WHERE q.job_id = e.job_id AND (e.attempt >= e.max_attempts OR e.cancel_requested)
            RETURNING q.job_id, q.attempt, q.cancel_requested, q.notify
        ),
        f AS (
            UPDATE {schema}.jobs j
            SET state = CASE WHEN fin.cancel_requested THEN 4 ELSE 3 END, attempt = fin.attempt, finished_at = now(),
                notify = j.notify OR fin.notify
            FROM fin
            WHERE j.id = fin.job_id
            RETURNING j.id, j.batch_id, j.state, fin.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, state, batch_id FROM f WHERE notify
            RETURNING 1
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt)
            SELECT job_id, now(), @kind_lease_expired, lease_owner, attempt FROM e
            UNION ALL
            SELECT job_id, now(), @kind_cancelled, lease_owner, attempt FROM e WHERE cancel_requested
            RETURNING kind
        )
        SELECT (SELECT count(*) FROM ev WHERE kind = @kind_lease_expired) AS expired, (SELECT count(*) FROM s) AS signals
        """;
```

```csharp
        }, async (r, c) =>
        {
            await r.ReadAsync(c);
            return ((int)r.GetInt64(0), r.GetInt64(1));
        }, ct);
```

`src/PgBolt.Postgres/Storage/PgBoltStore.Cancel.cs` — заменить `CancelNowTemplate`, ветку `owner is null` и комментарий ветки `!found`:

```csharp
    private const string CancelNowTemplate = """
        WITH d AS (DELETE FROM {schema}.queue WHERE job_id = @id RETURNING job_id, attempt, notify),
        j AS (UPDATE {schema}.jobs SET state = 4, attempt = d.attempt, finished_at = now(), notify = jobs.notify OR d.notify
              FROM d WHERE jobs.id = d.job_id AND jobs.id = @id
              RETURNING jobs.id, jobs.batch_id, d.notify),
        s AS (INSERT INTO {schema}.completions (node_id, kind, batch_id)
              SELECT id, 4, batch_id FROM j WHERE notify
              RETURNING 1),
        ev AS (INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
               SELECT job_id, now(), @kind_cancelled, attempt, @reason FROM d)
        SELECT count(*) FROM s
        """;
```

```csharp
                result = await state.ExecuteScalarAsync(c) switch
                {
                    short s when s is 2 or 3 or 4 => CancelResult.AlreadyFinished,
                    _ => CancelResult.NotFound,   // нет задачи; Awaiting — строка queue без lease, отменяется ниже (часть 5 §6.7)
                };
```

```csharp
            else if (owner is null)
            {
                await using var now = new NpgsqlCommand(CancelNowSql, conn, tx);
                P.Add(now.Parameters, "id", jobId);
                P.Add(now.Parameters, "kind_cancelled", (short)JobEventKind.Cancelled);
                P.AddNullable(now.Parameters, "reason", reason, NpgsqlDbType.Text);
                await PrepareAsync(now, c);
                signals = (long)(await now.ExecuteScalarAsync(c))!;
                result = CancelResult.Cancelled;
            }
```

(объявить `long signals = 0;` рядом с `CancelResult result;`, после `await tx.CommitAsync(c);` вызвать `SignalCompletions(signals);` — сигнал только после коммита).

`src/PgBolt.Postgres/Storage/PgBoltStore.Release.cs` — заменить шаблон, вызов `QueryAsync(ReleaseSql, ps =>` на `QueryAndSignalAsync<ReleaseOutcome>(ReleaseSql, ps =>` и делегат чтения:

```csharp
    private const string ReleaseTemplate = """
        WITH l AS MATERIALIZED (
            SELECT job_id, cancel_requested FROM {schema}.queue
            WHERE job_id = @id AND lease_id = @lease_id
            FOR UPDATE
        ),
        r AS (
            UPDATE {schema}.queue q
            SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL,
                attempt = q.attempt - 1, ready = true
            FROM l WHERE q.job_id = l.job_id AND NOT l.cancel_requested
            RETURNING q.job_id
        ),
        c AS (
            DELETE FROM {schema}.queue q USING l
            WHERE q.job_id = l.job_id AND l.cancel_requested
            RETURNING q.job_id, q.attempt - CASE WHEN @ran THEN 0 ELSE 1 END AS attempt, q.leased_at, q.notify
        ),
        cj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = c.attempt,
                                       started_at = CASE WHEN @ran THEN c.leased_at ELSE j.started_at END,
                                       finished_at = now(), server_id = @server, notify = j.notify OR c.notify
            FROM c WHERE j.id = c.job_id AND j.id = @id
            RETURNING j.id, j.batch_id, c.notify
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 4, batch_id FROM cj WHERE notify
            RETURNING 1
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message)
            SELECT job_id, now(), @kind_cancelled, @server, attempt, @message FROM c
        )
        SELECT (SELECT count(*) FROM r) AS released, (SELECT count(*) FROM c) AS cancelled, (SELECT count(*) FROM s) AS signals;
        """;
```

```csharp
        }, async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                throw new InvalidOperationException("Release statement returned no row.");
            var released = r.GetInt64(0);
            var cancelled = r.GetInt64(1);
            var outcome = cancelled > 0 ? ReleaseOutcome.Cancelled
                : released > 0 ? ReleaseOutcome.Released
                : ReleaseOutcome.LeaseLost;
            return (outcome, r.GetInt64(2));
        }, ct);
```

- [x] **Step 6: Dequeue, trigger, GetJob**

`PgBoltStore.Dequeue.cs`: в финальном `SELECT` шаблона после `j.recurring_id, j.scheduled_at` добавить `, j.continuation`; в `new LeasedJob(...)` после `r.GetNullableValue<DateTime>(11)` добавить аргумент `r.GetNullableValue<short>(12)`.

`PgBoltStore.Trigger.cs`: условие `WHERE job_id = @id AND lease_owner IS NULL AND NOT ready` заменить на `WHERE job_id = @id AND lease_owner IS NULL AND NOT ready AND await_condition IS NULL` (§6.3: trigger `Awaiting` → `false`).

`PgBoltStore.GetJob.cs`:
- в `GetJobTemplate` после `j.recurring_id, j.scheduled_at` добавить `, q.await_condition, j.parent_id, j.batch_id, j.continuation`;
- заменить вычисление состояния и `runAt`:

```csharp
            var stored = r.GetInt16(5);
            var inQueue = r.GetBoolean(13);
            var ready = inQueue && r.GetBoolean(15);
            var leaseOwner = r.GetNullableValue<Guid>(16);
            var awaiting = inQueue && !r.IsDBNull(22);
            var state = Derive(stored, inQueue, ready, leaseOwner, awaiting);
```

```csharp
            var runAt = inQueue && !awaiting ? r.GetFieldValue<DateTimeOffset>(14) : (DateTimeOffset?)null;
```

- после `var scheduledAt = r.GetNullableValue<DateTimeOffset>(21);` добавить:

```csharp
            var parentId = r.GetNullableValue<Guid>(23);
            var batchId = r.GetNullableValue<Guid>(24);
            var continuation = r.GetNullableValue<short>(25);
```

- в `return new JobInfo(...)` после `recurringId, scheduledAt` добавить аргументы
  `WorkflowCodes.ParentJob(continuation, parentId), WorkflowCodes.ParentBatch(continuation, parentId), batchId is { } b ? new BatchId(b) : null, WorkflowCodes.Condition(continuation)`;
- заменить `Derive`:

```csharp
    /// <summary>
    /// Core §2 с частью 5: Awaiting — строка queue с await_condition (jobs.state = 0); Scheduled — без lease, не ready и не Awaiting.
    /// Код 1 в jobs.state больше не используется.
    /// </summary>
    private static JobState Derive(short stored, bool inQueue, bool ready, Guid? leaseOwner, bool awaiting) => stored switch
    {
        0 when leaseOwner is not null => JobState.Processing,
        0 when awaiting => JobState.Awaiting,
        0 when inQueue && !ready => JobState.Scheduled,
        0 => JobState.Enqueued,
        2 => JobState.Succeeded,
        3 => JobState.Failed,
        4 => JobState.Cancelled,
        _ => throw new InvalidOperationException($"Unknown stored job state {stored}."),
    };
```

- [x] **Step 7: DI и `JobRunner`**

`src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`: перед регистрацией `PgBoltStore` добавить
`services.TryAddSingleton<ICompletionSignal>(_ => new InProcessCompletionSignal());`, а регистрацию store заменить на:

```csharp
        services.AddSingleton(sp => new PgBoltStore(sp.GetRequiredService<PgBoltDataSource>().Value, options.Schema, options.UsePreparedStatements)
        {
            Completions = sp.GetRequiredService<ICompletionSignal>(),
        });
```

`src/PgBolt.Server/Runtime/JobRunner.cs`, `ExecuteAsync` (+ `using PgBolt.Postgres.Storage;`, если его нет): создание контекста заменить на:

```csharp
        var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts,
            WorkflowCodes.ParentJob(job.Continuation, job.ParentId), job.BatchId is { } batch ? new BatchId(batch) : null, _jobLogger,
            job.ScheduledAtUtc is { } scheduled ? new DateTimeOffset(scheduled, TimeSpan.Zero) : null, job.RecurringId,
            WorkflowCodes.ParentBatch(job.Continuation, job.ParentId));
```

- [x] **Step 8: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowHotPathTests|FullyQualifiedName~CompletionSignalTests|FullyQualifiedName~AckTests|FullyQualifiedName~FailTests|FullyQualifiedName~SweeperTests|FullyQualifiedName~CancelTests|FullyQualifiedName~ReleaseTests|FullyQualifiedName~DequeueTests|FullyQualifiedName~TriggerRequeueTests|FullyQualifiedName~GetJobTests|FullyQualifiedName~HotUpdateTests|FullyQualifiedName~ClientManagementTests"`
Expected: PASS (Direct и Bouncer); `HotUpdateTests` — контракт HOT сохраняется (`notify` не индексирован).
Затем: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~JobRunnerTests"` — PASS.

- [x] **Step 9: Commit**

```bash
git add src/PgBolt.Postgres src/PgBolt.Server/Runtime/JobRunner.cs tests/PgBolt.Postgres.Tests tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): completion signals on finalization"
```

Конец фазы 1: `dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный.

---
### Task 4: Запись графа в хранилище: `SubmitWorkflowAsync` (§6.1, §6.2)

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/StoreRecords.cs` (`EnqueueBatch`: массивы части 5 и проверка), `src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs` (вариант enqueue графа)
- Create: `src/PgBolt.Postgres/Storage/WorkflowRecords.cs`, `src/PgBolt.Postgres/Storage/PgBoltStore.Workflow.cs`
- Test: `tests/PgBolt.Postgres.Tests/WorkflowWriteTests.cs`

**Interfaces:**
- Consumes: `WorkflowCodes`, `Completions`, `SignalCompletions` (Task 3; вызывать только после коммита); `ResolveSql`, `JobStateSql`, `IsJobsPrimaryKeyViolation`, `CreateEnqueueCommand`, `RunAsync`-правило повтора `EnqueueAsync` (core).
- Produces: `EnqueueBatch.ParentIds / Continuations / Notify / BatchIds` (init, все четыре вместе); `WorkflowBatchRow`, `WorkflowParent`, `WorkflowPlan` (+ `FirstNodeId`), `WorkflowWriteResult`, `WorkflowAlreadyWrittenException`; `PgBoltStore.SubmitWorkflowAsync(WorkflowPlan, NpgsqlTransaction?, CancellationToken)` — своя транзакция с повтором по правилу §6.1 и in-process сигналом после коммита, если записаны синтетические сигналы; пользовательская — `SAVEPOINT pgbolt_workflow` / `ROLLBACK TO` / `RELEASE`, без повторов и сигналов; `WorkflowAlreadyWrittenException` наружу не уходит (в пользовательской транзакции и после исчерпания попыток — `InvalidOperationException` с ним во `InnerException`).

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/WorkflowWriteTests.cs`:

```csharp
using System.Data;
using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.1–§6.2 на уровне хранилища: план графа собирается вручную.</summary>
public abstract class WorkflowWriteTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private readonly RecordingCompletionSignal _signal = new();
    private PgBoltStore _store = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        _store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true) { Completions = _signal };
    }

    private sealed record Row(Guid Id, Guid? Parent = null, short? Code = null, bool Notify = false, Guid? Batch = null, string? Key = null);

    private static EnqueueBatch Rows(params Row[] rows) =>
        new("wf.job", "wf", 0, 3, null, null, TimeSpan.Zero, [.. rows.Select(r => r.Id)], [.. rows.Select(_ => "{}")],
            new DateTime?[rows.Length], [.. rows.Select(r => r.Key)])
        {
            ParentIds = [.. rows.Select(r => r.Parent)],
            Continuations = [.. rows.Select(r => r.Code)],
            Notify = [.. rows.Select(r => r.Notify)],
            BatchIds = [.. rows.Select(r => r.Batch)],
        };

    private static WorkflowPlan Plan(EnqueueBatch jobs, WorkflowBatchRow[]? batches = null, WorkflowParent[]? external = null) =>
        new(batches ?? [], [jobs], external ?? []);

    private Task<long> CountAsync(string table) => Db.ScalarAsync<long>($"SELECT count(*) FROM {{schema}}.{table}");

    private Task<string?> SignalsAsync() =>
        Db.ScalarAsync<string?>("SELECT string_agg(node_id || ':' || kind, ',' ORDER BY id) FROM {schema}.completions");

    [Fact]
    public async Task Plan_rows_are_written_with_workflow_columns()
    {
        var (batch, root, member, child) = (Guid.CreateVersion7(), Guid.CreateVersion7(), Guid.CreateVersion7(), Guid.CreateVersion7());
        var plan = Plan(Rows(new Row(root, Notify: true), new Row(member, Notify: true, Batch: batch), new Row(child, root, 1)),
            [new WorkflowBatchRow(batch, null, null, null, WorkflowCodes.BatchActive, 1)]);

        Assert.False((await _store.SubmitWorkflowAsync(plan, null, Ct)).SignalResolver);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.ready AND q.await_condition IS NULL AND q.notify AND j.notify AND j.continuation IS NULL
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", root)));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.ready AND q.batch_id = @b AND j.batch_id = @b AND q.notify AND j.notify
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", member), ("b", batch)));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT NOT q.ready AND q.run_at = 'infinity' AND q.await_condition = 1 AND q.parent_id = @p AND NOT q.notify
                   AND j.continuation = 1 AND j.parent_id = @p AND NOT j.notify AND j.state = 0
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", child), ("p", root)));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 0 AND total = 1 AND succeeded = 0 AND NOT cancel_requested AND finished_at IS NULL AND parent_id IS NULL
            FROM {schema}.batches WHERE id = @b
            """, ("b", batch)));
        Assert.Null(await SignalsAsync());
    }

    [Fact]
    public async Task Subscription_to_live_parent_sets_queue_notify_without_signal()   // §6.2 п. 1
    {
        var parent = await EnqueueAsync();
        var child = Guid.CreateVersion7();

        await _store.SubmitWorkflowAsync(Plan(Rows(new Row(child, parent, 1)), external: [new WorkflowParent(parent, false)]), null, Ct);

        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", parent)));
        Assert.Null(await SignalsAsync());
        Assert.Equal(0, _signal.Count);
    }

    [Theory]
    [InlineData((short)2)]
    [InlineData((short)3)]
    [InlineData((short)4)]
    public async Task Subscription_to_terminal_parent_writes_synthetic_signal(short state)   // §6.2 п. 2
    {
        var parent = await EnqueueAsync();
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = @s WHERE id = @id",
            ("id", parent), ("s", state));
        var child = Guid.CreateVersion7();

        Assert.True((await _store.SubmitWorkflowAsync(Plan(Rows(new Row(child, parent, 2)), external: [new WorkflowParent(parent, false)]),
            null, Ct)).SignalResolver);

        Assert.Equal($"{parent}:{state}", await SignalsAsync());
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.jobs WHERE id = @id", ("id", parent)));
        Assert.Equal(1, _signal.Count);
    }

    [Fact]
    public async Task Subscription_to_batch_writes_subscribed_signal()   // §6.2 п. 4
    {
        var batch = Guid.CreateVersion7();
        await Db.ExecAsync("INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, 1, now())", ("b", batch));
        var child = Guid.CreateVersion7();

        await _store.SubmitWorkflowAsync(Plan(Rows(new Row(child, batch, 3)), external: [new WorkflowParent(batch, true)]), null, Ct);

        Assert.Equal($"{batch}:7", await SignalsAsync());
    }

    [Theory]
    [InlineData(false, false)]   // нет задачи
    [InlineData(true, false)]    // After(JobId) с id batch
    [InlineData(false, true)]    // After(BatchId) с id задачи
    public async Task Missing_or_wrong_kind_parent_throws_and_writes_nothing(bool batchExists, bool asBatch)   // §6.2 п. 3, 4
    {
        Guid parent;
        if (batchExists)
        {
            parent = Guid.CreateVersion7();
            await Db.ExecAsync("INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, 1, now())", ("b", parent));
        }
        else
        {
            parent = asBatch ? await EnqueueAsync() : Guid.CreateVersion7();
        }
        var jobsBefore = await CountAsync("jobs");

        var e = await Assert.ThrowsAsync<ParentNotFoundException>(() => _store.SubmitWorkflowAsync(
            Plan(Rows(new Row(Guid.CreateVersion7(), parent, asBatch ? (short)3 : (short)1)), external: [new WorkflowParent(parent, asBatch)]),
            null, Ct));

        Assert.Equal(parent, e.ParentId);
        Assert.Equal(jobsBefore, await CountAsync("jobs"));
        Assert.Null(await SignalsAsync());
    }

    [Fact]
    public async Task Unique_conflict_throws_with_existing_id_and_writes_no_graph_rows()
    {
        var existing = await EnqueueAsync(uniqueKey: "u");
        var batch = Guid.CreateVersion7();
        var plan = Plan(Rows(new Row(Guid.CreateVersion7(), Key: "u"), new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)),
            [new WorkflowBatchRow(batch, null, null, null, WorkflowCodes.BatchActive, 1)]);

        var e = await Assert.ThrowsAsync<UniqueKeyConflictException>(() => _store.SubmitWorkflowAsync(plan, null, Ct));

        Assert.Equal(("u", (JobId?)new JobId(existing)), (e.UniqueKey, e.ExistingId));
        Assert.Equal(1L, await CountAsync("jobs"));
        Assert.Equal(0L, await CountAsync("batches"));
    }

    [Fact]
    public async Task User_transaction_survives_sql_error_inside_submit()   // Review Focus 1, §6.1
    {
        var mine = Guid.CreateVersion7();
        var batch = Guid.CreateVersion7();
        var broken = Plan(Rows(new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)),
            [new WorkflowBatchRow(batch, null, null, null, 0, 1), new WorkflowBatchRow(batch, null, null, null, 0, 1)]);   // 23505 внутри
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, Ct);
        await Store.EnqueueAsync(Batch(mine), tx, Ct);

        var e = await Assert.ThrowsAsync<PostgresException>(() => _store.SubmitWorkflowAsync(broken, tx, Ct));
        Assert.Equal(PostgresErrorCodes.UniqueViolation, e.SqlState);
        await Store.EnqueueAsync(Batch(Guid.CreateVersion7()), tx, Ct);   // транзакция рабочая после ROLLBACK TO SAVEPOINT
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(mine));
        Assert.Equal(2L, await CountAsync("jobs"));
        Assert.Equal(0L, await CountAsync("batches"));
        Assert.Equal(0, _signal.Count);
    }

    [Fact]
    public async Task User_transaction_commit_publishes_graph_without_signal()
    {
        var parent = await EnqueueAsync();
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id; UPDATE {schema}.jobs SET state = 2 WHERE id = @id", ("id", parent));
        var child = Guid.CreateVersion7();
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);

        Assert.True((await _store.SubmitWorkflowAsync(Plan(Rows(new Row(child, parent, 1)), external: [new WorkflowParent(parent, false)]),
            tx, Ct)).SignalResolver);
        Assert.False(await InQueueAsync(child));   // не закоммичено
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(child));
        Assert.Equal($"{parent}:2", await SignalsAsync());
        Assert.Equal(0, _signal.Count);              // в пользовательской транзакции store не сигналит
    }

    [Fact]
    public async Task Repeated_plan_in_user_transaction_throws_invalid_operation()   // §6.1: внутренний признак повтора не уходит наружу
    {
        var plan = Plan(Rows(new Row(Guid.CreateVersion7()), new Row(Guid.CreateVersion7())));
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, Ct);
        await _store.SubmitWorkflowAsync(plan, tx, Ct);

        // exists_by_id видит строки первой записи этой же транзакции; без повтора — InvalidOperationException, не внутренний тип.
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => _store.SubmitWorkflowAsync(plan, tx, Ct));
        Assert.IsType<WorkflowAlreadyWrittenException>(e.InnerException);
        await tx.CommitAsync(Ct);   // транзакция рабочая после ROLLBACK TO SAVEPOINT второй записи

        Assert.Equal(2L, await CountAsync("jobs"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Retry_after_lost_commit_returns_success_without_duplicates(bool withBatch)   // Review Focus 2, §6.1 «Повтор»
    {
        var batch = Guid.CreateVersion7();
        var plan = withBatch
            ? Plan(Rows(new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)), [new WorkflowBatchRow(batch, null, null, null, 0, 1)])
            : Plan(Rows(new Row(Guid.CreateVersion7()), new Row(Guid.CreateVersion7())));

        await _store.SubmitWorkflowAsync(plan, null, Ct);
        await _store.SubmitWorkflowAsync(plan, null, Ct);   // повтор той же попытки после потерянного ответа на COMMIT

        Assert.Equal(withBatch ? 1L : 2L, await CountAsync("jobs"));
        Assert.Equal(withBatch ? 1L : 0L, await CountAsync("batches"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Retry_while_previous_attempt_is_in_flight_succeeds(bool withBatch)   // §12 «Падения»
    {
        var batch = Guid.CreateVersion7();
        var plan = withBatch
            ? Plan(Rows(new Row(Guid.CreateVersion7(), Batch: batch, Notify: true)), [new WorkflowBatchRow(batch, null, null, null, 0, 1)])
            : Plan(Rows(new Row(Guid.CreateVersion7()), new Row(Guid.CreateVersion7())));
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await _store.SubmitWorkflowAsync(plan, tx, Ct);                  // первая попытка: записала, COMMIT ещё не дошёл

        var retry = _store.SubmitWorkflowAsync(plan, null, Ct);          // повтор ждёт её строк (PK batches / queue)
        await WaitForLockWaiterAsync();
        Assert.False(retry.IsCompleted);
        await tx.CommitAsync(Ct);
        await retry;                                                     // 23505 / exists_by_id → проверка первой строки → успех

        Assert.Equal(withBatch ? 1L : 2L, await CountAsync("jobs"));
        Assert.Equal(withBatch ? 1L : 0L, await CountAsync("batches"));
    }

    private async Task WaitForLockWaiterAsync()
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (await Db.ScalarAsync<long>("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p",
                   ("p", $"%{Db.Schema}.%")) == 0)
        {
            if (System.Diagnostics.Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException("The retried submit does not wait for the first attempt.");
            await Task.Delay(20, Ct);
        }
    }

    [Fact]
    public async Task Workflow_and_recurring_columns_are_exclusive()
    {
        var plan = Rows(new Row(Guid.CreateVersion7())) with { RecurringIds = ["r"] };
        Assert.Throws<ArgumentException>(plan.Validate);
        Assert.Throws<ArgumentException>((Rows(new Row(Guid.CreateVersion7())) with { Notify = null }).Validate);
    }
}

public sealed class WorkflowWriteTests_Direct(PostgresFixture pg) : WorkflowWriteTests(pg, ConnectionMode.Direct);
public sealed class WorkflowWriteTests_Bouncer(PostgresFixture pg) : WorkflowWriteTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowWriteTests"`
Expected: FAIL — нет `WorkflowPlan`, `WorkflowBatchRow`, `WorkflowParent`, `SubmitWorkflowAsync`, `EnqueueBatch.ParentIds`.

- [x] **Step 3: `EnqueueBatch` и вариант enqueue графа**

В `src/PgBolt.Postgres/Storage/StoreRecords.cs` в `EnqueueBatch` после `RecurringIds` добавить:

```csharp
    /// <summary>Часть 5 §6.1 п. 2: родитель (задача или batch; у кода 5 — свой batch) по строкам. Только вместе с остальными массивами графа.</summary>
    public Guid?[]? ParentIds { get; init; }

    /// <summary>Часть 5: jobs.continuation = queue.await_condition (1–5) по строкам; не NULL — строка пишется Awaiting.</summary>
    public short?[]? Continuations { get; init; }

    /// <summary>Часть 5: queue.notify = jobs.notify по строкам (член batch или есть дети в графе).</summary>
    public bool[]? Notify { get; init; }

    /// <summary>Часть 5: batch члена по строкам (в queue и jobs).</summary>
    public Guid?[]? BatchIds { get; init; }
```

и в конец `Validate()`:

```csharp
        if (ParentIds is not null || Continuations is not null || Notify is not null || BatchIds is not null)
        {
            if (ParentIds is null || Continuations is null || Notify is null || BatchIds is null)
                throw new ArgumentException("Workflow arrays (ParentIds, Continuations, Notify, BatchIds) must be set together.");
            if (ParentIds.Length != Ids.Length || Continuations.Length != Ids.Length || Notify.Length != Ids.Length
                || BatchIds.Length != Ids.Length)
                throw new ArgumentException("Batch arrays must have equal length.");
            if (RecurringIds is not null)
                throw new ArgumentException("Workflow and recurring columns are mutually exclusive.");
        }
```

В `src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs` после `RecurringEnqueueSql` добавить:

```csharp
    /// <summary>
    /// Часть 5 §6.1 п. 2: вариант EnqueueTemplate для графа — поэлементные parent_id, continuation (= await_condition), notify,
    /// batch_id (в queue и jobs). Строка с continuation пишется Awaiting: ready = false, run_at = 'infinity' (инвариант §4).
    /// </summary>
    private const string WorkflowEnqueueTemplate = """
        WITH src AS (
            SELECT * FROM unnest(@ids::uuid[], @args::jsonb[], @run_at::timestamptz[], @unique_keys::text[],
                                 @parent_ids::uuid[], @continuations::smallint[], @notify::boolean[], @batch_ids::uuid[])
                     AS t(id, args, run_at, unique_key, parent_id, continuation, notify, batch_id)
        ),
        existing AS (
            SELECT id FROM {schema}.jobs WHERE id = ANY(@ids)
        ),
        input AS (
            SELECT * FROM src WHERE id NOT IN (SELECT id FROM existing)
        ),
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key,
                                        parent_id, await_condition, batch_id, notify)
            SELECT id, @queue, @priority, r.run_at, input.continuation IS NULL AND r.run_at <= now(), @max_attempts, unique_key,
                   parent_id, continuation, batch_id, notify
            FROM input CROSS JOIN LATERAL (
                SELECT CASE WHEN input.continuation IS NULL THEN COALESCE(input.run_at, now() + @delay)
                            ELSE 'infinity'::timestamptz END AS run_at) r
            ORDER BY unique_key, id
            ON CONFLICT DO NOTHING
            RETURNING job_id
        ),
        j AS (
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, retry_delays,
                                       unique_key, trace_parent, created_at, parent_id, batch_id, continuation, notify)
            SELECT i.id, @type, @queue, @priority, i.args, 0, @max_attempts, @retry_delays,
                   i.unique_key, @trace_parent, now(), i.parent_id, i.batch_id, i.continuation, i.notify
            FROM input i JOIN q ON q.job_id = i.id
        )
        SELECT s.id,
               q.job_id IS NOT NULL AS created,
               e.id     IS NOT NULL AS existed
        FROM src s
        LEFT JOIN q        ON q.job_id = s.id
        LEFT JOIN existing e ON e.id = s.id
        """;

    private string WorkflowEnqueueSql => field ??= Render(WorkflowEnqueueTemplate);
```

заменить `EnqueueSqlFor`:

```csharp
    /// <summary>§8 части 4, §6.1 части 5: массивы графа — вариант с колонками workflow; RecurringIds — с колонками расписания; иначе прежний SQL клиента.</summary>
    internal string EnqueueSqlFor(EnqueueBatch batch) =>
        batch.ParentIds is not null ? WorkflowEnqueueSql : batch.RecurringIds is null ? EnqueueSql : RecurringEnqueueSql;
```

и в `BindEnqueue` после блока `RecurringIds` добавить:

```csharp
        if (b.ParentIds is { } parents)
        {
            P.Add(ps, "parent_ids", parents, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
            P.Add(ps, "continuations", b.Continuations!, NpgsqlDbType.Array | NpgsqlDbType.Smallint);
            P.Add(ps, "notify", b.Notify!);
            P.Add(ps, "batch_ids", b.BatchIds!, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        }
```

- [x] **Step 4: Записи графа**

`src/PgBolt.Postgres/Storage/WorkflowRecords.cs`:

```csharp
namespace PgBolt.Postgres.Storage;

/// <summary>
/// Строка batches графа (§6.1 п. 1). State — WorkflowCodes.BatchAwaiting у batch-продолжений и вложенных в них на любой глубине,
/// иначе BatchActive; Condition — код 1–4 при ParentId; Total — число прямых членов (задачи + вложенные batches).
/// </summary>
internal sealed record WorkflowBatchRow(Guid Id, Guid? OuterBatchId, Guid? ParentId, short? Condition, short State, int Total);

/// <summary>Существующий родитель After (§6.2): задача или batch.</summary>
internal readonly record struct WorkflowParent(Guid Id, bool IsBatch);

/// <summary>Граф одного SubmitAsync: batches, группы задач (EnqueueBatch с массивами графа), внешние родители в порядке id.</summary>
internal sealed record WorkflowPlan(IReadOnlyList<WorkflowBatchRow> Batches, IReadOnlyList<EnqueueBatch> Jobs, IReadOnlyList<WorkflowParent> External)
{
    /// <summary>§6.1 «Повтор»: первая строка графа — первый batch, иначе первая задача.</summary>
    public Guid FirstNodeId => Batches.Count > 0 ? Batches[0].Id : Jobs[0].Ids[0];
}

/// <summary>Итог записи графа: SignalResolver — у графа есть синтетические сигналы (§6.1 п. 5).</summary>
internal readonly record struct WorkflowWriteResult(bool SignalResolver);

/// <summary>
/// Строки графа уже записаны (или пишутся) предыдущей попыткой того же SubmitAsync (§6.1 «Повтор»). Только внутренний сигнал
/// повтора: наружу SubmitWorkflowAsync его не пропускает, а оборачивает в InvalidOperationException.
/// </summary>
internal sealed class WorkflowAlreadyWrittenException()
    : Exception("The workflow graph has already been written by a previous attempt of the same SubmitAsync.");
```

- [x] **Step 5: Транзакция записи графа**

`src/PgBolt.Postgres/Storage/PgBoltStore.Workflow.cs`:

```csharp
using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Запись графа workflow (часть 5 §6.1): batches одним unnest, задачи — вариантом enqueue с колонками графа (все команды одним
/// NpgsqlBatch), разрешение конфликтов, подписка на внешних родителей в порядке id (§6.2), синтетические сигналы — одна транзакция.
/// </summary>
public sealed partial class PgBoltStore
{
    private const string WorkflowSavepoint = "pgbolt_workflow";
    private const string AfterSavepoint = "pgbolt_after";

    private const string InsertBatchesTemplate = """
        INSERT INTO {schema}.batches (id, parent_batch_id, parent_id, condition, state, total, created_at)
        SELECT id, parent_batch_id, parent_id, condition, state, total, now()
        FROM unnest(@ids::uuid[], @outer_ids::uuid[], @parent_ids::uuid[], @conditions::smallint[], @states::smallint[], @totals::int[])
             AS t(id, parent_batch_id, parent_id, condition, state, total)
        """;

    private const string SubscribeQueueTemplate = "UPDATE {schema}.queue SET notify = true WHERE job_id = @id RETURNING 1";

    private const string SubscribeTerminalTemplate =
        "UPDATE {schema}.jobs SET notify = true WHERE id = @id AND state IN (2, 3, 4) RETURNING state";

    private const string SubscribeBatchTemplate = "SELECT 1 FROM {schema}.batches WHERE id = @id FOR KEY SHARE";

    private const string SyntheticSignalsTemplate = """
        INSERT INTO {schema}.completions (node_id, kind)
        SELECT node_id, kind FROM unnest(@nodes::uuid[], @kinds::smallint[]) AS t(node_id, kind)
        """;

    private const string GraphWrittenTemplate = """
        SELECT EXISTS (SELECT 1 FROM {schema}.batches WHERE id = @id) OR EXISTS (SELECT 1 FROM {schema}.jobs WHERE id = @id)
        """;

    private string InsertBatchesSql => field ??= Render(InsertBatchesTemplate);
    private string SubscribeQueueSql => field ??= Render(SubscribeQueueTemplate);
    private string SubscribeTerminalSql => field ??= Render(SubscribeTerminalTemplate);
    private string SubscribeBatchSql => field ??= Render(SubscribeBatchTemplate);
    private string SyntheticSignalsSql => field ??= Render(SyntheticSignalsTemplate);
    private string GraphWrittenSql => field ??= Render(GraphWrittenTemplate);

    /// <summary>
    /// §6.1. transaction — пользовательская: SAVEPOINT pgbolt_workflow, любое исключение — ROLLBACK TO SAVEPOINT и проброс, без
    /// повторов и сигналов. null — своя транзакция: при 40P01/40001, транзиентной ошибке или признаке предыдущей попытки (23505 по
    /// PK batches/jobs, exists_by_id) — проверка первой строки графа новой транзакцией: есть → успех, нет → повтор.
    /// </summary>
    internal async Task<WorkflowWriteResult> SubmitWorkflowAsync(WorkflowPlan plan, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(plan);
        if (plan.Batches.Count == 0 && plan.Jobs.Count == 0)
            throw new ArgumentException("The workflow plan is empty.", nameof(plan));
        foreach (var jobs in plan.Jobs)
        {
            jobs.Validate();
            if (jobs.ParentIds is null)
                throw new ArgumentException("Workflow job groups must carry workflow arrays.", nameof(plan));
        }

        if (transaction is not null)
        {
            var conn = transaction.Connection ?? throw new InvalidOperationException("The transaction is already completed.");
            await transaction.SaveAsync(WorkflowSavepoint, ct);
            try
            {
                var written = await WriteWorkflowAsync(conn, transaction, plan, ct);
                await transaction.ReleaseAsync(WorkflowSavepoint, ct);
                return written;
            }
            catch (WorkflowAlreadyWrittenException e)
            {
                // Без повтора внутренний признак не наружу: строки с id графа уже есть в видимых этой транзакции данных.
                await RollbackToSavepointAsync(transaction, WorkflowSavepoint);
                throw new InvalidOperationException(
                    "Rows with the workflow's job or batch ids already exist: the workflow was already submitted "
                    + "(possibly earlier in this transaction).", e);
            }
            catch
            {
                await RollbackToSavepointAsync(transaction, WorkflowSavepoint);
                throw;
            }
        }

        var transientRetried = false;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var conn = await _dataSource.OpenConnectionAsync(ct);
                await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
                var written = await WriteWorkflowAsync(conn, tx, plan, ct);
                await tx.CommitAsync(ct);
                if (written.SignalResolver)
                    Completions?.Signal();
                return written;
            }
            catch (Exception e) when (IsPreviousAttempt(e))
            {
                // Предыдущая попытка закоммичена (возможно, пока эта ждала её строк): транзакция откатана, проверка новой.
                if (await GraphWrittenAsync(plan.FirstNodeId, ct))
                    return Recovered(plan);
                if (attempt >= MaxStatementAttempts)
                {
                    // Внутренний признак повтора не уходит пользователю; 23505 по PK — пробрасывается как есть.
                    if (e is WorkflowAlreadyWrittenException)
                        throw new InvalidOperationException(
                            "Rows with the workflow's job or batch ids already exist, but the workflow's first row was not found "
                            + $"after {attempt} attempts: the ids are used by another submission.", e);
                    throw;
                }
                continue;
            }
            catch (PostgresException e) when (attempt < MaxStatementAttempts && _retryOnConflict && DbRetry.IsRetryable(e))
            {
            }
            catch (NpgsqlException e) when (e is not PostgresException && e.IsTransient && !transientRetried)
            {
                transientRetried = true;
            }

            // Перед повтором: ответ на COMMIT мог потеряться после фиксации (§6.1 «Повтор»).
            if (await GraphWrittenAsync(plan.FirstNodeId, ct))
                return Recovered(plan);
        }
    }

    /// <summary>Граф записан предыдущей попыткой: её синтетические сигналы (если есть внешние родители) уже в completions.</summary>
    private WorkflowWriteResult Recovered(WorkflowPlan plan)
    {
        var result = new WorkflowWriteResult(plan.External.Count > 0);
        if (result.SignalResolver)
            Completions?.Signal();
        return result;
    }

    private static bool IsPreviousAttempt(Exception e) =>
        e is WorkflowAlreadyWrittenException
        || e is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation, TableName: "batches" }
        || (e is PostgresException p && IsJobsPrimaryKeyViolation(p));

    private async Task<bool> GraphWrittenAsync(Guid firstNodeId, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(GraphWrittenSql, conn);
        P.Add(cmd.Parameters, "id", firstNodeId);
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>Шаги 1–5 §6.1 в транзакции tx.</summary>
    private async Task<WorkflowWriteResult> WriteWorkflowAsync(NpgsqlConnection conn, NpgsqlTransaction tx, WorkflowPlan plan,
        CancellationToken ct)
    {
        // 1–2. Batches и задачи — одним NpgsqlBatch.
        var rows = new Dictionary<Guid, (bool Created, bool Existed)>();
        await using (var batch = new NpgsqlBatch(conn, tx))
        {
            if (plan.Batches.Count > 0)
                batch.BatchCommands.Add(InsertBatchesCommand(plan.Batches));
            foreach (var jobs in plan.Jobs)
                batch.BatchCommands.Add(CreateEnqueueCommand(jobs));
            await using var r = await batch.ExecuteReaderAsync(ct);
            do
            {
                while (await r.ReadAsync(ct))
                    rows[r.GetGuid(0)] = (r.GetBoolean(1), r.GetBoolean(2));
            }
            while (await r.NextResultAsync(ct));
        }

        // 3. Конфликты: existed — строку уже записала предыдущая попытка; иначе запрос разрешения core §5.1.
        if (rows.Values.Any(v => v.Existed))
            throw new WorkflowAlreadyWrittenException();
        var unresolved = plan.Jobs
            .SelectMany(b => b.Ids.Select((id, i) => (Id: id, Key: b.UniqueKeys[i])))
            .Where(x => !rows[x.Id].Created)
            .ToList();
        if (unresolved.Count > 0)
            await ThrowWorkflowConflictAsync(conn, tx, unresolved, ct);

        // 4. Подписка на внешних родителей (§6.2) — последняя запись, в порядке id.
        var nodes = new List<Guid>();
        var kinds = new List<short>();
        foreach (var parent in plan.External)
        {
            var kind = parent.IsBatch
                ? await SubscribeBatchAsync(conn, tx, parent.Id, ct)
                : await SubscribeJobAsync(conn, tx, parent.Id, ct);
            if (kind is { } k)
            {
                nodes.Add(parent.Id);
                kinds.Add(k);
            }
        }

        // 5. Синтетические сигналы.
        if (nodes.Count > 0)
        {
            await using var signals = new NpgsqlCommand(SyntheticSignalsSql, conn, tx);
            P.Add(signals.Parameters, "nodes", nodes.ToArray());
            P.Add(signals.Parameters, "kinds", kinds.ToArray());
            await signals.ExecuteNonQueryAsync(ct);
        }
        return new WorkflowWriteResult(nodes.Count > 0);
    }

    private NpgsqlBatchCommand InsertBatchesCommand(IReadOnlyList<WorkflowBatchRow> batches)
    {
        var cmd = new NpgsqlBatchCommand(InsertBatchesSql);
        var ps = cmd.Parameters;
        P.Add(ps, "ids", batches.Select(b => b.Id).ToArray());
        P.Add(ps, "outer_ids", batches.Select(b => b.OuterBatchId).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "parent_ids", batches.Select(b => b.ParentId).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "conditions", batches.Select(b => b.Condition).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Smallint);
        P.Add(ps, "states", batches.Select(b => b.State).ToArray());
        P.Add(ps, "totals", batches.Select(b => b.Total).ToArray());
        return cmd;
    }

    /// <summary>§6.1 п. 3: exists_by_id — предыдущая попытка; иначе конфликт unique_key → UniqueKeyConflictException.</summary>
    private async Task ThrowWorkflowConflictAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<(Guid Id, string? Key)> unresolved,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(ResolveSql, conn, tx);
        P.Add(cmd.Parameters, "ids", unresolved.Select(u => u.Id).ToArray());
        P.Add(cmd.Parameters, "unique_keys", unresolved.Select(u => u.Key).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Text);
        var keys = unresolved.ToDictionary(u => u.Id, u => u.Key);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            if (r.GetBoolean(1) || keys[r.GetGuid(0)] is not { } key)
                throw new WorkflowAlreadyWrittenException();
            var existing = r.GetNullableValue<Guid>(2);
            throw new UniqueKeyConflictException(key, existing is { } e ? new JobId(e) : null);
        }
        throw new WorkflowAlreadyWrittenException();
    }

    /// <summary>
    /// §6.2 пп. 1–3 для родителя-задачи. Возвращает kind синтетического сигнала (состояние терминального родителя) или null —
    /// подписка на строку queue (Pending, Awaiting или выполняется). Нет задачи — ParentNotFoundException.
    /// </summary>
    private async Task<short?> SubscribeJobAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, CancellationToken ct)
    {
        while (true)
        {
            await using (var queue = new NpgsqlCommand(SubscribeQueueSql, conn, tx))
            {
                P.Add(queue.Parameters, "id", id);
                if (await queue.ExecuteScalarAsync(ct) is not null)
                    return null;
            }

            await tx.SaveAsync(AfterSavepoint, ct);
            await using (var terminal = new NpgsqlCommand(SubscribeTerminalSql, conn, tx))
            {
                P.Add(terminal.Parameters, "id", id);
                if (await terminal.ExecuteScalarAsync(ct) is short state)
                {
                    await tx.ReleaseAsync(AfterSavepoint, ct);
                    return state;
                }
            }
            // Ноль строк: UPDATE мог дождаться requeue и заблокировать новую версию строки jobs — снять блокировку (§6.2).
            await tx.RollbackAsync(AfterSavepoint, ct);

            await using var exists = new NpgsqlCommand(JobStateSql, conn, tx);
            P.Add(exists.Parameters, "id", id);
            if (await exists.ExecuteScalarAsync(ct) is null)
                throw new ParentNotFoundException(id);
            // Строка есть: requeue после снимка п. 1 (state 0) или финализация после п. 2 — повтор с п. 1.
        }
    }

    /// <summary>§6.2 п. 4: KEY SHARE строки batch; есть → сигнал Subscribed, нет → ParentNotFoundException.</summary>
    private async Task<short?> SubscribeBatchAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(SubscribeBatchSql, conn, tx);
        P.Add(cmd.Parameters, "id", id);
        return await cmd.ExecuteScalarAsync(ct) is null ? throw new ParentNotFoundException(id) : WorkflowCodes.Subscribed;
    }

    /// <summary>Откат к точке сохранения после любой ошибки; ошибка самого отката (разрыв соединения) не подменяет исходную.</summary>
    private static async Task RollbackToSavepointAsync(NpgsqlTransaction tx, string savepoint)
    {
        try
        {
            await tx.RollbackAsync(savepoint, CancellationToken.None);
        }
        catch (Exception e) when (e is NpgsqlException or InvalidOperationException)
        {
            // Соединение разорвано: транзакцию откатит сервер, исходное исключение важнее.
        }
    }
}
```

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowWriteTests|FullyQualifiedName~EnqueueTests|FullyQualifiedName~EnqueueIdempotencyTests|FullyQualifiedName~RecurringTickTests"`
Expected: PASS (Direct и Bouncer); прежние варианты enqueue не изменились.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests/WorkflowWriteTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): transactional workflow graph write with subscription"
```

---
### Task 5: Fluent-построитель и `IJobClient.Workflow` (§5.1, §5.2, §6.1)

**Files:**
- Create: `src/PgBolt.Postgres/Client/Workflows/WorkflowGraph.cs`, `src/PgBolt.Postgres/Client/Workflows/WorkflowBuilder.cs`, `src/PgBolt.Postgres/Client/Workflows/WorkflowPlanner.cs`
- Modify: `src/PgBolt.Abstractions/IJobClient.cs` (`Workflow`), `src/PgBolt.Postgres/Client/PgBoltJobClient.cs` (`Workflow`, общие помощники)
- Modify (тесты): `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` (`WorkflowTestBase`)
- Test: `tests/PgBolt.Postgres.Tests/WorkflowBuilderTests.cs`, `tests/PgBolt.Postgres.Tests/WorkflowSubmitTests.cs`

**Interfaces:**
- Consumes: `SubmitWorkflowAsync`, `WorkflowPlan`, `WorkflowBatchRow`, `WorkflowParent`, `EnqueueBatch` с массивами графа (Task 4); `WorkflowCodes` (Task 3); `JobMetadataAccess`, `ReservedUniqueKeys`, `JobTypesCache` (core / часть 4).
- Produces: `IJobClient.Workflow(WorkflowOptions? options = null)`; `WorkflowBuilder`, `WorkflowPlanner` (см. «Контракт»); `PgBoltJobClient.EnsureReadCommittedAsync` и `CurrentTraceParent` — `internal static`; тестовая база `WorkflowTestBase` (`Flow` — store с `Completions = Signal`, `Client`, `A(n)`, `LeaseAsync`, `SucceedAsync`, `FailFinallyAsync`, `CancelJobAsync`, `AwaitAsync`, `ReadyAsync`, `StateAsync`, `BatchStateAsync`, `SignalsAsync`).

- [x] **Step 1: Тестовая база**

В `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` добавить `using PgBolt.Postgres.Storage; using PgBolt.Testing;` и класс:

```csharp
/// <summary>Клиент и хранилище с записывающими сигналами; помощники финализации конкретной задачи в обход порядка очереди.</summary>
public abstract class WorkflowTestBase(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    protected RecordingWakeup Wakeup { get; } = new();
    protected RecordingCompletionSignal Signal { get; } = new();
    protected PgBoltStore Flow { get; private set; } = null!;
    protected IJobClient Client { get; private set; } = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        Flow = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true) { Completions = Signal };
        Client = new PgBoltJobClient(Flow, new JobTypesCache(Flow, TimeSpan.FromSeconds(30), TimeProvider.System), Wakeup);
    }

    /// <summary>Аргументы зарегистрированного типа client.test (очередь client, 4 попытки, приоритет 2).</summary>
    protected static ClientArgs A(int n) => new(n);

    /// <summary>Берёт в lease именно эту готовую задачу; возвращает lease_id.</summary>
    protected Task<Guid> LeaseAsync(Guid id) =>
        Db.ScalarAsync<Guid>("""
            UPDATE {schema}.queue
            SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '5 minutes', leased_at = now(),
                attempt = attempt + 1
            WHERE job_id = @id AND lease_owner IS NULL AND ready AND await_condition IS NULL
            RETURNING lease_id
            """, ("id", id), ("s", ServerA));

    protected async Task SucceedAsync(Guid id) =>
        Assert.Equal(1, await Flow.AckAsync(ServerA, [new LeaseRef(id, await LeaseAsync(id))], Ct));

    protected async Task FailFinallyAsync(Guid id) =>
        Assert.Equal(FailOutcome.Failed, await Flow.FailAsync(id, await LeaseAsync(id), ServerA, JobEventKind.Failed, "boom", null, Ct));

    protected async Task CancelJobAsync(Guid id) => Assert.Equal(CancelResult.Cancelled, await Flow.CancelAsync(id, "test", Ct));

    /// <summary>queue.await_condition; null — не ждёт или строки нет.</summary>
    protected Task<short?> AwaitAsync(Guid id) =>
        Db.ScalarAsync<short?>("SELECT await_condition FROM {schema}.queue WHERE job_id = @id", ("id", id));

    /// <summary>Запущена: строка queue готова к dequeue (ready, не ждёт, без lease).</summary>
    protected Task<bool> ReadyAsync(Guid id) =>
        Db.ScalarAsync<bool>("""
            SELECT coalesce((SELECT ready AND await_condition IS NULL AND lease_owner IS NULL FROM {schema}.queue WHERE job_id = @id), false)
            """, ("id", id));

    protected Task<short?> StateAsync(Guid id) => JobStateAsync(id);

    protected Task<short?> BatchStateAsync(Guid id) =>
        Db.ScalarAsync<short?>("SELECT state FROM {schema}.batches WHERE id = @id", ("id", id));

    protected Task<long> SignalsAsync() => Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions");
}
```

- [x] **Step 2: Тесты правил построителя**

`tests/PgBolt.Postgres.Tests/WorkflowBuilderTests.cs`:

```csharp
using System.Text.Json;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §5.2: правила построителя; БД нужна только для SubmitAsync.</summary>
public abstract class WorkflowBuilderTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    private static readonly StepOptions Delayed = new() { Delay = TimeSpan.FromMinutes(1) };
    private static readonly StepOptions At = new() { RunAt = DateTimeOffset.UtcNow.AddMinutes(1) };

    [Fact]
    public void Steps_get_uuid_v7_ids_before_submit()
    {
        var wf = Client.Workflow();
        var job = wf.Enqueue(A(1));
        var batch = wf.StartBatch(b => b.Enqueue(A(2)));

        Assert.Equal(7, job.Id.Value.Version);
        Assert.Equal(7, batch.Id.Value.Version);
        Assert.NotEqual(job.Id.Value, batch.Id.Value);
    }

    [Fact]
    public void Run_at_and_delay_are_allowed_only_without_parent_and_outside_awaiting_batches()
    {
        var wf = Client.Workflow();
        var root = wf.Enqueue(A(1), Delayed);
        wf.StartBatch(b =>
        {
            b.Enqueue(A(2), At);
            b.StartBatch(n => n.Enqueue(A(3), Delayed));
            var member = b.Enqueue(A(4));
            Assert.Throws<ArgumentException>(() => member.ContinueWith(A(5), options: Delayed));
        });
        Assert.Throws<ArgumentException>(() => root.ContinueWith(A(6), options: At));
        root.ContinueWithBatch(b =>
        {
            b.Enqueue(A(7));
            Assert.Throws<ArgumentException>(() => b.Enqueue(A(8), Delayed));
            b.StartBatch(n =>
            {
                n.Enqueue(A(9));
                Assert.Throws<ArgumentException>(() => n.Enqueue(A(10), At));
            });
        });
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(11), new StepOptions { RunAt = DateTimeOffset.UtcNow, Delay = TimeSpan.Zero }));
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(12), new StepOptions { Delay = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(13), new StepOptions { MaxAttempts = 0 }));
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(14), new StepOptions { Queue = "" }));
    }

    [Fact]
    public void Unique_keys_are_rejected_for_members_reserved_prefix_and_duplicates()
    {
        var wf = Client.Workflow();
        var root = wf.Enqueue(A(1), new StepOptions { UniqueKey = "a" });
        root.ContinueWith(A(2), options: new StepOptions { UniqueKey = "b" });
        wf.StartBatch(b =>
        {
            Assert.Throws<ArgumentException>(() => b.Enqueue(A(3), new StepOptions { UniqueKey = "c" }));
            Assert.Throws<ArgumentException>(() => b.EnqueueMany(new[] { A(4) }, new StepOptions { UniqueKey = "d" }));
            b.Enqueue(A(5));
        });

        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(6), new StepOptions { UniqueKey = "pgbolt:x" }));
        Assert.Throws<ArgumentException>(() => root.ContinueWith(A(7), options: new StepOptions { UniqueKey = "a" }));
    }

    [Fact]
    public void Closed_batch_rejects_its_builder_and_member_steps_but_not_the_batch_step()   // §5.2 «Области»
    {
        var wf = Client.Workflow();
        IBatchBuilder? builder = null;
        IJobStep? member = null;
        var batch = wf.StartBatch(b =>
        {
            builder = b;
            member = b.Enqueue(A(1));
            member.ContinueWith(A(2));                       // член того же batch, пока его область открыта
            member.ContinueWithBatch(c => c.Enqueue(A(3)));  // вложенный batch-продолжение
        });

        Assert.Throws<InvalidOperationException>(() => builder!.Enqueue(A(4)));
        Assert.Throws<InvalidOperationException>(() => builder!.StartBatch(n => n.Enqueue(A(5))));
        Assert.Throws<InvalidOperationException>(() => member!.ContinueWith(A(6)));
        batch.ContinueWith(A(7));                            // продолжение batch вне его: корневая область открыта
    }

    [Fact]
    public void Empty_batch_throws_and_makes_workflow_unusable()
    {
        var wf = Client.Workflow();
        Assert.Throws<ArgumentException>(() => wf.StartBatch(_ => { }));
        Assert.Throws<InvalidOperationException>(() => wf.Enqueue(A(1)));

        var nested = Client.Workflow();
        Assert.Throws<ArgumentException>(() => nested.StartBatch(b =>
        {
            b.Enqueue(A(1));
            b.StartBatch(_ => { });
        }));
    }

    [Fact]
    public async Task Submit_requires_steps_and_continuations_of_After()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await Client.Workflow().SubmitAsync(Ct));
        var wf = Client.Workflow();
        wf.After(JobId.New());
        await Assert.ThrowsAsync<ArgumentException>(async () => await wf.SubmitAsync(Ct));
    }

    [Fact]
    public async Task Methods_after_submit_throw()
    {
        var wf = Client.Workflow();
        var step = wf.Enqueue(A(1));
        await step.SubmitAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await wf.SubmitAsync(Ct));
        Assert.Throws<InvalidOperationException>(() => wf.Enqueue(A(2)));
        Assert.Throws<InvalidOperationException>(() => step.ContinueWith(A(3)));
        Assert.Throws<InvalidOperationException>(() => wf.After(JobId.New()));

        var failed = Client.Workflow();
        await Assert.ThrowsAsync<ArgumentException>(async () => await failed.SubmitAsync(Ct));
        Assert.Throws<InvalidOperationException>(() => failed.Enqueue(A(4)));   // и после неуспешного SubmitAsync
    }

    [Fact]
    public void Typed_step_without_metadata_throws_on_creation() =>
        Assert.Throws<InvalidOperationException>(() => Client.Workflow().Enqueue(new UnregisteredArgs(1)));

    [Fact]
    public async Task Unknown_dynamic_job_throws_before_writing()   // Review Focus 6
    {
        using var args = JsonDocument.Parse("{}");
        var wf = Client.Workflow();
        wf.Enqueue(A(1)).ContinueWith("nope.job", args.RootElement);

        await Assert.ThrowsAsync<UnknownJobException>(async () => await wf.SubmitAsync(Ct));

        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Dynamic_step_takes_defaults_from_job_types()
    {
        await Flow.PublishJobTypesAsync([new PgBolt.Postgres.Storage.JobTypeRecord("dyn.job", "{}", "dyn", 6, 3, [7])], "test", Ct);
        using var args = JsonDocument.Parse("""{"x":1}""");

        var r = await Client.Workflow().Enqueue("dyn.job", args.RootElement).SubmitAsync(Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.queue = 'dyn' AND q.priority = 3 AND q.max_attempts = 6 AND j.retry_delays = ARRAY[7] AND j.args = '{"x":1}'::jsonb
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", r.Jobs[0].Value)));
    }

    [Fact]
    public void Transaction_must_be_npgsql() =>
        Assert.Throws<ArgumentException>(() => Client.Workflow(new WorkflowOptions { Transaction = new FakeTransaction() }));

    [Fact]
    public void Undefined_condition_is_rejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Client.Workflow().Enqueue(A(1)).ContinueWith(A(2), (ContinuationCondition)7));
}

public sealed class WorkflowBuilderTests_Direct(PostgresFixture pg) : WorkflowBuilderTests(pg, ConnectionMode.Direct);
public sealed class WorkflowBuilderTests_Bouncer(PostgresFixture pg) : WorkflowBuilderTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 3: Тесты записи через клиента**

`tests/PgBolt.Postgres.Tests/WorkflowSubmitTests.cs`:

```csharp
using System.Data;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.1–§6.2 через клиента: форма графа в queue/jobs/batches, After в каждом состоянии, транзакции пользователя.</summary>
public abstract class WorkflowSubmitTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    public enum ParentKind { Pending, Awaiting, Succeeded, Failed, Cancelled }

    private sealed record QueueState(bool Ready, short? Await, Guid? Parent, Guid? Batch, bool Notify, bool Infinity);

    private async Task<QueueState?> QueueOf(Guid id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql(
            "SELECT ready, await_condition, parent_id, batch_id, notify, run_at = 'infinity' FROM {schema}.queue WHERE job_id = @id"));
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        if (!await r.ReadAsync(Ct))
            return null;
        return new QueueState(r.GetBoolean(0), r.IsDBNull(1) ? null : r.GetInt16(1), r.IsDBNull(2) ? null : r.GetGuid(2),
            r.IsDBNull(3) ? null : r.GetGuid(3), r.GetBoolean(4), r.GetBoolean(5));
    }

    /// <summary>state:total:condition:parent_id:parent_batch_id.</summary>
    private Task<string> BatchOf(Guid id) =>
        Db.ScalarAsync<string>("""
            SELECT state || ':' || total || ':' || coalesce(condition::text, '-') || ':' || coalesce(parent_id::text, '-')
                   || ':' || coalesce(parent_batch_id::text, '-')
            FROM {schema}.batches WHERE id = @id
            """, ("id", id));

    private Task<string?> CompletionsAsync() =>
        Db.ScalarAsync<string?>("SELECT string_agg(node_id || ':' || kind, ',' ORDER BY id) FROM {schema}.completions");

    [Fact]
    public async Task Chain_with_fan_out_writes_awaiting_graph()   // §5.1, пример 1
    {
        var fetch = Client.Workflow().Enqueue(A(1));
        var parse = fetch.ContinueWith(A(2));
        IReadOnlyList<IJobStep> items = [];
        var fan = parse.ContinueWithBatch(b => items = b.EnqueueMany(new[] { A(3), A(4) }));
        var report = fan.ContinueWith(A(5), ContinuationCondition.OnAnyFinished);

        var result = await report.SubmitAsync(Ct);

        Assert.Equal([fetch.Id, parse.Id, items[0].Id, items[1].Id, report.Id], result.Jobs);
        Assert.Equal([fan.Id], result.Batches);
        Assert.Equal(new QueueState(true, null, null, null, true, false), await QueueOf(fetch.Id.Value));
        Assert.Equal(new QueueState(false, 1, fetch.Id.Value, null, true, true), await QueueOf(parse.Id.Value));
        Assert.Equal($"1:2:1:{parse.Id}:-", await BatchOf(fan.Id.Value));
        foreach (var item in items)
            Assert.Equal(new QueueState(false, 5, fan.Id.Value, fan.Id.Value, true, true), await QueueOf(item.Id.Value));
        Assert.Equal(new QueueState(false, 4, fan.Id.Value, null, false, true), await QueueOf(report.Id.Value));
        Assert.Equal(["client"], Wakeup.Signals);
        Assert.Equal(0, Signal.Count);
    }

    [Fact]
    public async Task Root_batch_with_inner_chain_and_nested_batch_is_active()   // §5.1, пример 3
    {
        IJobStep? fetch = null, merge = null, resize = null;
        IBatchStep? inner = null;
        var outer = Client.Workflow().StartBatch(b =>
        {
            fetch = b.Enqueue(A(1));
            merge = fetch.ContinueWith(A(2));
            inner = b.StartBatch(n => resize = n.Enqueue(A(3)));
        });
        var report = outer.ContinueWith(A(4));

        await report.SubmitAsync(Ct);

        Assert.Equal("0:3:-:-:-", await BatchOf(outer.Id.Value));
        Assert.Equal($"0:1:-:-:{outer.Id}", await BatchOf(inner!.Id.Value));
        Assert.Equal(new QueueState(true, null, null, outer.Id.Value, true, false), await QueueOf(fetch!.Id.Value));
        Assert.Equal(new QueueState(false, 1, fetch.Id.Value, outer.Id.Value, true, true), await QueueOf(merge!.Id.Value));
        Assert.Equal(new QueueState(true, null, null, inner.Id.Value, true, false), await QueueOf(resize!.Id.Value));
        Assert.Equal(new QueueState(false, 3, outer.Id.Value, null, false, true), await QueueOf(report.Id.Value));
    }

    [Fact]
    public async Task Batches_inside_batch_continuation_await_activation_or_their_parent()   // §2.1
    {
        var x = Client.Workflow().Enqueue(A(1));
        IBatchStep? nested = null, chained = null;
        IJobStep? y = null, z = null, w = null;
        var outer = x.ContinueWithBatch(b =>
        {
            nested = b.StartBatch(n => y = n.Enqueue(A(2)));
            z = b.Enqueue(A(3));
            chained = z.ContinueWithBatch(c => w = c.Enqueue(A(4)));
        });

        await outer.SubmitAsync(Ct);

        Assert.Equal($"1:3:1:{x.Id}:-", await BatchOf(outer.Id.Value));
        Assert.Equal($"1:1:-:-:{outer.Id}", await BatchOf(nested!.Id.Value));
        Assert.Equal($"1:1:1:{z!.Id}:{outer.Id}", await BatchOf(chained!.Id.Value));
        Assert.Equal(new QueueState(false, 5, nested.Id.Value, nested.Id.Value, true, true), await QueueOf(y!.Id.Value));
        Assert.Equal(new QueueState(false, 5, outer.Id.Value, outer.Id.Value, true, true), await QueueOf(z.Id.Value));
        Assert.Equal(new QueueState(false, 5, chained.Id.Value, chained.Id.Value, true, true), await QueueOf(w!.Id.Value));
    }

    private async Task<Guid> ParentAsync(ParentKind kind)
    {
        if (kind == ParentKind.Awaiting)
            return (await Client.Workflow().Enqueue(A(0)).ContinueWith(A(0)).SubmitAsync(Ct)).Jobs[1].Value;
        var id = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        switch (kind)
        {
            case ParentKind.Succeeded:
                await SucceedAsync(id);
                break;
            case ParentKind.Failed:
                await FailFinallyAsync(id);
                break;
            case ParentKind.Cancelled:
                await CancelJobAsync(id);
                break;
        }
        return id;
    }

    [Theory]
    [InlineData(ParentKind.Pending, null)]
    [InlineData(ParentKind.Awaiting, null)]
    [InlineData(ParentKind.Succeeded, (short)2)]
    [InlineData(ParentKind.Failed, (short)3)]
    [InlineData(ParentKind.Cancelled, (short)4)]
    public async Task After_job_in_each_state(ParentKind kind, short? signal)   // §12 SQL-контракт
    {
        var parent = await ParentAsync(kind);
        var before = Signal.Count;

        var child = (await Client.Workflow().After(new JobId(parent)).ContinueWith(A(1)).SubmitAsync(Ct)).Jobs[0].Value;

        Assert.Equal((short?)1, await AwaitAsync(child));
        Assert.Equal(signal is { } k ? $"{parent}:{k}" : null, await CompletionsAsync());
        if (signal is null)
            Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", parent)));
        Assert.Equal(signal is null ? before : before + 1, Signal.Count);
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1)]
    [InlineData((short)2)]
    [InlineData((short)3)]
    [InlineData((short)4)]
    public async Task After_batch_in_each_state_writes_subscribed_signal(short state)   // §12 SQL-контракт
    {
        var batch = (await Client.Workflow().StartBatch(b => b.Enqueue(A(0))).SubmitAsync(Ct)).Batches[0].Value;
        await Db.ExecAsync("UPDATE {schema}.batches SET state = @s WHERE id = @id", ("id", batch), ("s", state));

        var child = (await Client.Workflow().After(new BatchId(batch)).ContinueWith(A(1)).SubmitAsync(Ct)).Jobs[0].Value;

        Assert.Equal((short?)3, await AwaitAsync(child));
        Assert.Equal($"{batch}:7", await CompletionsAsync());
    }

    [Fact]
    public async Task After_missing_or_other_kind_throws_ParentNotFound_and_writes_nothing()   // §12 SQL-контракт
    {
        var job = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0];
        var batch = (await Client.Workflow().StartBatch(b => b.Enqueue(A(0))).SubmitAsync(Ct)).Batches[0];
        var before = await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs");

        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(JobId.New()).ContinueWith(A(1)).SubmitAsync(Ct));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(new JobId(batch.Value)).ContinueWith(A(1)).SubmitAsync(Ct));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(new BatchId(job.Value)).ContinueWith(A(1)).SubmitAsync(Ct));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () =>
            await Client.Workflow().After(BatchId.New()).ContinueWithBatch(b => b.Enqueue(A(1))).SubmitAsync(Ct));

        Assert.Equal(before, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
        Assert.Null(await CompletionsAsync());
    }

    [Fact]
    public async Task Awaiting_job_holds_its_unique_key()   // §12 unique_key
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2), options: new StepOptions { UniqueKey = "u" }).SubmitAsync(Ct);

        var again = await Client.EnqueueAsync(A(3), new EnqueueOptions { UniqueKey = "u" }, Ct);

        Assert.Equal(new EnqueueResult(r.Jobs[1], false), again);
    }

    [Fact]
    public async Task Unique_conflict_throws_and_writes_no_graph_rows()   // §12 unique_key
    {
        var existing = await Client.EnqueueAsync(A(1), new EnqueueOptions { UniqueKey = "u" }, Ct);
        var wf = Client.Workflow();
        wf.StartBatch(b => b.EnqueueMany(new[] { A(2), A(3) })).ContinueWith(A(4), options: new StepOptions { UniqueKey = "u" });

        var e = await Assert.ThrowsAsync<UniqueKeyConflictException>(async () => await wf.SubmitAsync(Ct));

        Assert.Equal("u", e.UniqueKey);
        Assert.Equal(existing.Id, e.ExistingId);
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
    }

    [Fact]
    public async Task Single_root_job_matches_enqueue_but_conflict_throws()   // §5.2
    {
        var r = await Client.Workflow().Enqueue(A(1), new StepOptions { UniqueKey = "k", Priority = 7 }).SubmitAsync(Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.ready AND q.priority = 7 AND q.queue = 'client' AND q.unique_key = 'k' AND NOT q.notify
                   AND j.continuation IS NULL AND j.max_attempts = 4 AND j.retry_delays = ARRAY[1,5] AND j.args = '{"Order":1}'::jsonb
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", r.Jobs[0].Value)));
        await Assert.ThrowsAsync<UniqueKeyConflictException>(async () =>
            await Client.Workflow().Enqueue(A(2), new StepOptions { UniqueKey = "k" }).SubmitAsync(Ct));
    }

    [Fact]
    public async Task User_transaction_keeps_working_after_workflow_exceptions()   // §12, Review Focus 1
    {
        await Client.EnqueueAsync(A(0), new EnqueueOptions { UniqueKey = "taken" }, Ct);
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        var mine = await Client.EnqueueAsync(A(1), new EnqueueOptions { Transaction = tx }, Ct);
        var options = new WorkflowOptions { Transaction = tx };

        await Assert.ThrowsAsync<UniqueKeyConflictException>(async () =>
            await Client.Workflow(options).StartBatch(b => b.Enqueue(A(2)))
                .ContinueWith(A(3), options: new StepOptions { UniqueKey = "taken" }).SubmitAsync(Ct));
        var wf = Client.Workflow(options);
        wf.Enqueue(A(4));                               // вставлена до подписки, откатывается вместе с графом
        wf.After(JobId.New()).ContinueWith(A(5));
        await Assert.ThrowsAsync<ParentNotFoundException>(async () => await wf.SubmitAsync(Ct));
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(mine.Id!.Value.Value));
        Assert.Equal(2L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
    }

    [Fact]
    public async Task Workflow_in_user_transaction_is_written_on_commit_without_wakeups()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);

        var r = await Client.Workflow(new WorkflowOptions { Transaction = tx }).Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        Assert.False(await InQueueAsync(r.Jobs[0].Value));
        await tx.CommitAsync(Ct);

        Assert.True(await InQueueAsync(r.Jobs[0].Value));
        Assert.Empty(Wakeup.Signals);
    }

    [Fact]
    public async Task Repeatable_read_transaction_is_rejected()   // §5.2: требования core §5.1
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead, Ct);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Client.Workflow(new WorkflowOptions { Transaction = tx }).Enqueue(A(1)).SubmitAsync(Ct));
    }

    [Fact]
    public async Task Awaiting_job_is_not_dequeued_promoted_or_triggered_and_cancels()   // §12 SQL-контракт
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        var child = r.Jobs[1];

        Assert.Equal(r.Jobs[0].Value, Assert.Single(await Flow.DequeueAsync("client", 10, ServerA, Lease, Ct)).Id);
        Assert.Equal(0, (await Flow.PromoteDueAsync(100, Ct)).Promoted);
        Assert.False(await Client.TriggerAsync(child, Ct));
        Assert.Equal(JobState.Awaiting, (await Client.GetAsync(child, Ct))!.State);
        Assert.Equal(CancelResult.Cancelled, await Client.CancelAsync(child, ct: Ct));
    }

    [Fact]
    public async Task Root_job_with_delay_is_scheduled_without_wakeup()
    {
        var r = await Client.Workflow().Enqueue(A(1), new StepOptions { Delay = TimeSpan.FromMinutes(5) }).SubmitAsync(Ct);

        Assert.Equal(JobState.Scheduled, (await Client.GetAsync(r.Jobs[0], Ct))!.State);
        Assert.Empty(Wakeup.Signals);
    }
}

public sealed class WorkflowSubmitTests_Direct(PostgresFixture pg) : WorkflowSubmitTests(pg, ConnectionMode.Direct);
public sealed class WorkflowSubmitTests_Bouncer(PostgresFixture pg) : WorkflowSubmitTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 4: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowBuilderTests|FullyQualifiedName~WorkflowSubmitTests"`
Expected: FAIL — нет `IJobClient.Workflow`.

- [x] **Step 5: Граф**

`src/PgBolt.Postgres/Client/Workflows/WorkflowGraph.cs`:

```csharp
namespace PgBolt.Postgres.Client.Workflows;

/// <summary>Область построителя (§5.2): корень workflow или тело batch. Тело открыто только на время лямбды batch.</summary>
internal sealed class WorkflowScope(BatchNode? batch)
{
    public BatchNode? Batch { get; } = batch;
    public bool Open { get; set; } = batch is null;
}

/// <summary>Узел графа: задача, batch или существующий родитель (After).</summary>
internal abstract class WorkflowNode(Guid id, WorkflowScope scope)
{
    public Guid Id { get; } = id;

    /// <summary>Область, в которой узел создан; продолжения узла добавляются в неё же.</summary>
    public WorkflowScope Scope { get; } = scope;

    /// <summary>У узла есть дети в графе: у задачи — notify = true (§6.1 п. 2).</summary>
    public bool HasChildren { get; set; }

    public abstract bool IsBatch { get; }
}

internal sealed class JobNode(Guid id, WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition,
    JobDescriptor? descriptor, string? jobName, string argsJson, StepOptions? options) : WorkflowNode(id, scope)
{
    public WorkflowNode? Parent { get; } = parent;
    public ContinuationCondition Condition { get; } = condition;

    /// <summary>Типизированный шаг; null — динамический (JobName, умолчания из job_types при SubmitAsync).</summary>
    public JobDescriptor? Descriptor { get; } = descriptor;

    public string? JobName { get; } = jobName;
    public string ArgsJson { get; } = argsJson;
    public StepOptions? Options { get; } = options;
    public override bool IsBatch => false;
}

internal sealed class BatchNode : WorkflowNode
{
    public BatchNode(Guid id, WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition) : base(id, scope)
    {
        Parent = parent;
        Condition = condition;
        Awaiting = parent is not null || scope.Batch is { Awaiting: true };
        Body = new WorkflowScope(this);
    }

    public WorkflowNode? Parent { get; }
    public ContinuationCondition Condition { get; }

    /// <summary>§6.1 п. 1: state = 1 у batch-продолжений и у вложенных в них на любой глубине.</summary>
    public bool Awaiting { get; }

    public WorkflowScope Body { get; }

    /// <summary>Прямые члены: задачи и вложенные batches (total).</summary>
    public int Members { get; set; }

    public override bool IsBatch => true;
}

internal sealed class ExternalNode(Guid id, WorkflowScope scope, bool isBatch) : WorkflowNode(id, scope)
{
    public override bool IsBatch { get; } = isBatch;
}
```

- [x] **Step 6: Построитель**

`src/PgBolt.Postgres/Client/Workflows/WorkflowBuilder.cs`:

```csharp
using System.Text.Json;
using Npgsql;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client.Workflows;

/// <summary>
/// Fluent-построитель workflow (часть 5 §5.1–§5.2): синхронный, не потокобезопасный граф узлов; id — при создании шага;
/// запись — SubmitAsync одной транзакцией (PgBoltStore.SubmitWorkflowAsync).
/// </summary>
internal sealed class WorkflowBuilder(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup, NpgsqlTransaction? transaction)
    : IWorkflow
{
    private readonly List<JobNode> _jobs = [];
    private readonly List<BatchNode> _batches = [];
    private readonly List<ExternalNode> _external = [];
    private readonly HashSet<string> _uniqueKeys = new(StringComparer.Ordinal);
    private readonly WorkflowScope _root = new(null);
    private bool _submitted;
    private bool _broken;

    public IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null) => EnqueueIn(_root, args, options, null, default);

    public IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null) =>
        EnqueueIn(_root, jobName, args, options, null, default);

    public IBatchStep StartBatch(Action<IBatchBuilder> build) => StartBatchIn(_root, null, default, build);

    public INodeStep After(JobId existing) => AddExternal(existing.Value, isBatch: false);

    public INodeStep After(BatchId existing) => AddExternal(existing.Value, isBatch: true);

    public ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default) => new(SubmitCoreAsync(ct));

    internal IJobStep EnqueueIn<TArgs>(WorkflowScope scope, TArgs args, StepOptions? options, WorkflowNode? parent,
        ContinuationCondition condition)
    {
        EnsureUsable(scope);
        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        return AddJob(scope, parent, condition, descriptor, null, JsonSerializer.Serialize(args, typeInfo), options);
    }

    internal IJobStep EnqueueIn(WorkflowScope scope, string jobName, JsonElement args, StepOptions? options, WorkflowNode? parent,
        ContinuationCondition condition)
    {
        EnsureUsable(scope);
        ArgumentException.ThrowIfNullOrEmpty(jobName);
        return AddJob(scope, parent, condition, null, jobName, args.GetRawText(), options);
    }

    internal IReadOnlyList<IJobStep> EnqueueManyIn<TArgs>(WorkflowScope scope, IReadOnlyList<TArgs> args, StepOptions? options)
    {
        EnsureUsable(scope);
        ArgumentNullException.ThrowIfNull(args);
        var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
        var steps = new IJobStep[args.Count];
        for (var i = 0; i < args.Count; i++)
            steps[i] = AddJob(scope, null, default, descriptor, null, JsonSerializer.Serialize(args[i], typeInfo), options);
        return steps;
    }

    internal IBatchStep StartBatchIn(WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition, Action<IBatchBuilder> build)
    {
        EnsureUsable(scope);
        ArgumentNullException.ThrowIfNull(build);
        EnsureCondition(condition);
        var node = new BatchNode(Guid.CreateVersion7(), scope, parent, condition);
        Attach(node, parent);
        _batches.Add(node);
        node.Body.Open = true;
        try
        {
            build(new BatchBuilder(this, node));
        }
        catch
        {
            _broken = true;   // недостроенный batch уже в графе (решение плана 5)
            throw;
        }
        finally
        {
            node.Body.Open = false;
        }
        if (node.Members == 0)
        {
            _broken = true;
            throw new ArgumentException("A workflow batch must have at least one member.", nameof(build));
        }
        return new BatchStep(this, node);
    }

    private ExternalStep AddExternal(Guid id, bool isBatch)
    {
        EnsureUsable(_root);
        var node = new ExternalNode(id, _root, isBatch);
        _external.Add(node);
        return new ExternalStep(this, node);
    }

    private JobStep AddJob(WorkflowScope scope, WorkflowNode? parent, ContinuationCondition condition, JobDescriptor? descriptor,
        string? jobName, string argsJson, StepOptions? options)
    {
        EnsureCondition(condition);
        ValidateOptions(scope, parent, options);
        var node = new JobNode(Guid.CreateVersion7(), scope, parent, condition, descriptor, jobName, argsJson, options);
        Attach(node, parent);
        _jobs.Add(node);
        return new JobStep(this, node);
    }

    private static void Attach(WorkflowNode node, WorkflowNode? parent)
    {
        if (parent is not null)
            parent.HasChildren = true;
        if (node.Scope.Batch is { } batch)
            batch.Members++;
    }

    private void EnsureUsable(WorkflowScope scope)
    {
        if (_submitted)
            throw new InvalidOperationException("The workflow has already been submitted; create a new one with IJobClient.Workflow().");
        if (_broken)
            throw new InvalidOperationException("The workflow is unusable after a failed batch definition; create a new one.");
        if (!scope.Open)
            throw new InvalidOperationException("The batch is closed: its members can be added only inside its build lambda.");
    }

    private static void EnsureCondition(ContinuationCondition condition)
    {
        if (!Enum.IsDefined(condition))
            throw new ArgumentOutOfRangeException(nameof(condition), condition, "Unknown continuation condition.");
    }

    /// <summary>§5.2: значения опций, RunAt/Delay только без родителя и вне ждущих batch, UniqueKey не у членов и без повторов в графе.</summary>
    private void ValidateOptions(WorkflowScope scope, WorkflowNode? parent, StepOptions? options)
    {
        if (options is null)
            return;
        if (options.RunAt is not null && options.Delay is not null)
            throw new ArgumentException("StepOptions.RunAt and StepOptions.Delay are mutually exclusive.", nameof(options));
        if (options.Delay < TimeSpan.Zero)
            throw new ArgumentException("StepOptions.Delay must be non-negative.", nameof(options));
        if (options.MaxAttempts < 1)
            throw new ArgumentException("StepOptions.MaxAttempts must be >= 1.", nameof(options));
        if (options.Queue is { Length: 0 })
            throw new ArgumentException("StepOptions.Queue must not be empty.", nameof(options));
        if (options.RetryDelays is { } delays && delays.Any(d => d < TimeSpan.Zero))
            throw new ArgumentException("StepOptions.RetryDelays must be non-negative.", nameof(options));
        if ((options.RunAt is not null || options.Delay is not null) && (parent is not null || scope.Batch is { Awaiting: true }))
            throw new ArgumentException(
                "StepOptions.RunAt and StepOptions.Delay are allowed only for root jobs and members of batches without a parent: " +
                "continuations start when their condition resolves.", nameof(options));
        if (options.UniqueKey is not { } key)
            return;
        if (scope.Batch is not null)
            throw new ArgumentException("StepOptions.UniqueKey is not allowed for batch members: a conflict would change the batch total.",
                nameof(options));
        ReservedUniqueKeys.EnsureUserKeys([key]);
        if (!_uniqueKeys.Add(key))
            throw new ArgumentException($"Unique key '{key}' is used by more than one step of the workflow.", nameof(options));
    }

    private async Task<WorkflowResult> SubmitCoreAsync(CancellationToken ct)
    {
        EnsureUsable(_root);
        _submitted = true;
        if (_jobs.Count == 0 && _batches.Count == 0)
            throw new ArgumentException("The workflow has no steps.");
        foreach (var e in _external)
            if (!e.HasChildren)
                throw new ArgumentException($"After({e.Id}) has no continuations.");

        var dynamic = new Dictionary<string, JobDescriptor>(StringComparer.Ordinal);
        foreach (var job in _jobs)
        {
            if (job.Descriptor is not null || dynamic.ContainsKey(job.JobName!))
                continue;
            var type = await jobTypes.GetAsync(job.JobName!, ct) ?? throw new UnknownJobException(job.JobName!);
            dynamic[job.JobName!] = new JobDescriptor(type.Name, type.DefaultQueue, type.DefaultMaxAttempts, type.DefaultPriority,
                type.DefaultRetryDelaysSeconds);
        }
        if (transaction is not null)
            await PgBoltJobClient.EnsureReadCommittedAsync(transaction, ct);

        var (plan, readyQueues) = WorkflowPlanner.Build(_batches, _jobs, _external, dynamic, PgBoltJobClient.CurrentTraceParent());
        await store.SubmitWorkflowAsync(plan, transaction, ct);
        if (transaction is null)
            foreach (var queue in readyQueues)
                wakeup.Signal(queue);   // core §5.10: только после своего коммита; resolver будит store (синтетические сигналы)

        return new WorkflowResult([.. _jobs.Select(j => new JobId(j.Id))], [.. _batches.Select(b => new BatchId(b.Id))]);
    }
}

/// <summary>Методы узла (§5.1): продолжения добавляются в область узла, где бы их ни вызвали.</summary>
internal abstract class NodeStep(WorkflowBuilder workflow, WorkflowNode node) : INodeStep
{
    public IJobStep ContinueWith<TArgs>(TArgs args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
        StepOptions? options = null) =>
        workflow.EnqueueIn(node.Scope, args, options, node, condition);

    public IJobStep ContinueWith(string jobName, JsonElement args, ContinuationCondition condition = ContinuationCondition.OnSuccess,
        StepOptions? options = null) =>
        workflow.EnqueueIn(node.Scope, jobName, args, options, node, condition);

    public IBatchStep ContinueWithBatch(Action<IBatchBuilder> build, ContinuationCondition condition = ContinuationCondition.OnSuccess) =>
        workflow.StartBatchIn(node.Scope, node, condition, build);

    public ValueTask<WorkflowResult> SubmitAsync(CancellationToken ct = default) => workflow.SubmitAsync(ct);
}

internal sealed class JobStep(WorkflowBuilder workflow, JobNode node) : NodeStep(workflow, node), IJobStep
{
    public JobId Id { get; } = new(node.Id);
}

internal sealed class BatchStep(WorkflowBuilder workflow, BatchNode node) : NodeStep(workflow, node), IBatchStep
{
    public BatchId Id { get; } = new(node.Id);
}

internal sealed class ExternalStep(WorkflowBuilder workflow, ExternalNode node) : NodeStep(workflow, node);

/// <summary>Тело batch: всё добавляется в его область (члены batch).</summary>
internal sealed class BatchBuilder(WorkflowBuilder workflow, BatchNode batch) : IBatchBuilder
{
    public IJobStep Enqueue<TArgs>(TArgs args, StepOptions? options = null) => workflow.EnqueueIn(batch.Body, args, options, null, default);

    public IJobStep Enqueue(string jobName, JsonElement args, StepOptions? options = null) =>
        workflow.EnqueueIn(batch.Body, jobName, args, options, null, default);

    public IBatchStep StartBatch(Action<IBatchBuilder> build) => workflow.StartBatchIn(batch.Body, null, default, build);

    public IReadOnlyList<IJobStep> EnqueueMany<TArgs>(IReadOnlyList<TArgs> args, StepOptions? options = null) =>
        workflow.EnqueueManyIn(batch.Body, args, options);
}
```

- [x] **Step 7: Планировщик записи**

`src/PgBolt.Postgres/Client/Workflows/WorkflowPlanner.cs`:

```csharp
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client.Workflows;

/// <summary>
/// Граф → план записи (§6.1): batches с состоянием и total; задачи с кодом continuation (5 — член ждущего batch без родителя,
/// parent_id = его batch), notify (член batch или есть дети), группы enqueue по (type, queue, priority, max_attempts,
/// retry_delays, delay) по 10 000 строк; внешние родители — различные, в порядке id (Guid.CompareTo совпадает с порядком uuid).
/// </summary>
internal static class WorkflowPlanner
{
    internal const int MaxRowsPerCommand = 10_000;

    private sealed record JobRow(Guid Id, string Type, string Queue, short Priority, int MaxAttempts, int[]? RetryDelays,
        string ArgsJson, DateTime? RunAtUtc, TimeSpan Delay, string? UniqueKey, Guid? ParentId, short? Continuation, bool Notify,
        Guid? BatchId);

    public static (WorkflowPlan Plan, IReadOnlyCollection<string> ReadyQueues) Build(IReadOnlyList<BatchNode> batches,
        IReadOnlyList<JobNode> jobs, IReadOnlyList<ExternalNode> external, IReadOnlyDictionary<string, JobDescriptor> dynamic,
        string? traceParent)
    {
        var batchRows = batches
            .Select(b => new WorkflowBatchRow(b.Id, b.Scope.Batch?.Id, b.Parent?.Id,
                b.Parent is { } p ? WorkflowCodes.Code(b.Condition, p.IsBatch) : null,
                b.Awaiting ? WorkflowCodes.BatchAwaiting : WorkflowCodes.BatchActive, b.Members))
            .ToList();

        var rows = jobs.Select(j => Row(j, dynamic)).ToList();
        var groups = new List<EnqueueBatch>();
        foreach (var g in rows.GroupBy(r => (r.Type, r.Queue, r.Priority, r.MaxAttempts,
                     Retry: r.RetryDelays is { } d ? string.Join(',', d) : "-", r.Delay)))
        {
            foreach (var chunk in g.Chunk(MaxRowsPerCommand))
            {
                groups.Add(new EnqueueBatch(g.Key.Type, g.Key.Queue, g.Key.Priority, g.Key.MaxAttempts, chunk[0].RetryDelays, traceParent,
                    g.Key.Delay, [.. chunk.Select(r => r.Id)], [.. chunk.Select(r => r.ArgsJson)], [.. chunk.Select(r => r.RunAtUtc)],
                    [.. chunk.Select(r => r.UniqueKey)])
                {
                    ParentIds = [.. chunk.Select(r => r.ParentId)],
                    Continuations = [.. chunk.Select(r => r.Continuation)],
                    Notify = [.. chunk.Select(r => r.Notify)],
                    BatchIds = [.. chunk.Select(r => r.BatchId)],
                });
            }
        }

        var parents = external.Select(e => new WorkflowParent(e.Id, e.IsBatch)).Distinct().OrderBy(p => p.Id).ToList();
        var ready = rows.Where(r => r.Continuation is null && r.RunAtUtc is null && r.Delay == TimeSpan.Zero)
            .Select(r => r.Queue).ToHashSet(StringComparer.Ordinal);
        return (new WorkflowPlan(batchRows, groups, parents), ready);
    }

    private static JobRow Row(JobNode job, IReadOnlyDictionary<string, JobDescriptor> dynamic)
    {
        var d = job.Descriptor ?? dynamic[job.JobName!];
        var o = job.Options;
        var activation = job.Parent is null && job.Scope.Batch is { Awaiting: true };
        short? continuation = job.Parent is { } parent
            ? WorkflowCodes.Code(job.Condition, parent.IsBatch)
            : activation ? WorkflowCodes.Activation : (short?)null;
        var parentId = job.Parent?.Id ?? (activation ? job.Scope.Batch!.Id : (Guid?)null);
        return new JobRow(job.Id, d.Name, o?.Queue ?? d.Queue, o?.Priority ?? d.Priority, o?.MaxAttempts ?? d.MaxAttempts,
            o?.RetryDelays is { } delays ? [.. delays.Select(ToSeconds)] : d.RetryDelaysSeconds, job.ArgsJson,
            o?.RunAt?.UtcDateTime, o?.Delay ?? TimeSpan.Zero, o?.UniqueKey, parentId, continuation,
            Notify: job.Scope.Batch is not null || job.HasChildren, job.Scope.Batch?.Id);
    }

    private static int ToSeconds(TimeSpan delay) => checked((int)Math.Ceiling(delay.TotalSeconds));
}
```

- [x] **Step 8: Клиент**

`src/PgBolt.Abstractions/IJobClient.cs` — добавить член:

```csharp
    /// <summary>Новый граф продолжений и batches (часть 5 §5); пишется SubmitAsync одной транзакцией.</summary>
    IWorkflow Workflow(WorkflowOptions? options = null);
```

`src/PgBolt.Postgres/Client/PgBoltJobClient.cs`: добавить `using PgBolt.Postgres.Client.Workflows;`; методы `CurrentTraceParent` и `EnsureReadCommittedAsync` сделать `internal static` (тела не менять); добавить:

```csharp
    public IWorkflow Workflow(WorkflowOptions? options = null)
    {
        var transaction = options?.Transaction switch
        {
            null => null,
            NpgsqlTransaction t => t,
            _ => throw new ArgumentException("WorkflowOptions.Transaction must be an NpgsqlTransaction.", nameof(options)),
        };
        return new WorkflowBuilder(store, jobTypes, wakeup, transaction);
    }
```

- [x] **Step 9: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowBuilderTests|FullyQualifiedName~WorkflowSubmitTests|FullyQualifiedName~WorkflowWriteTests|FullyQualifiedName~ClientEnqueueTests"`
Expected: PASS (Direct и Bouncer).

- [x] **Step 10: Commit**

```bash
git add src/PgBolt.Abstractions/IJobClient.cs src/PgBolt.Postgres/Client tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): fluent workflow builder and IJobClient.Workflow"
```

---
### Task 6: Resolver — учёт (Т1) и обход детей (Т2) (§6.4)

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/WorkflowRecords.cs` (`CompletionCountResult`, `ResolveResult`, `ResolverStep`)
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Resolver.cs`
- Modify (тесты): `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` (`WorkflowOutcome`, `FinishAsync`, `FateAsync`, `ResolveOnceAsync`, `ResolveAllAsync`)
- Test: `tests/PgBolt.Postgres.Tests/WorkflowResolverTests.cs`, `tests/PgBolt.Postgres.Tests/WorkflowResolverPlanTests.cs`

**Interfaces:**
- Consumes: схема (Task 1); сигналы финализации (Task 3); граф через клиента (Task 5).
- Produces: `CountCompletionsAsync(int batchSize, CancellationToken)` — один statement: захват `kind <> 6 AND NOT counted` (`completions_count`), счётчики `batches` `ORDER BY id FOR NO KEY UPDATE`, переход в терминальное состояние с сигналом исхода (`batch_id = parent_batch_id`), удаление сигналов без детей, `counted = true` остальным; `ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken)` — транзакция Т2; `OldestCompletionAgeAsync`; `ResolverHook` (только тесты, `ResolverStep.Walked` — перед `COMMIT` Т2); `CountSql`, `WalkCaptureSql`, `WalkJobsSql` — `internal` для `EXPLAIN`-тестов.

- [x] **Step 1: Тестовая поддержка**

В `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` добавить на уровне namespace:

```csharp
public enum WorkflowOutcome { Succeeded, Failed, Cancelled }
```

и в `WorkflowTestBase`:

```csharp
    protected Task FinishAsync(Guid id, WorkflowOutcome outcome) => outcome switch
    {
        WorkflowOutcome.Succeeded => SucceedAsync(id),
        WorkflowOutcome.Failed => FailFinallyAsync(id),
        _ => CancelJobAsync(id),
    };

    /// <summary>awaiting — ждёт; launched — запущена (готова к dequeue); иначе по jobs.state: succeeded / failed / cancelled.</summary>
    protected async Task<string> FateAsync(Guid id)
    {
        if (await AwaitAsync(id) is not null)
            return "awaiting";
        if (await ReadyAsync(id))
            return "launched";
        return await StateAsync(id) switch
        {
            2 => "succeeded",
            3 => "failed",
            4 => "cancelled",
            var s => $"state {s}",
        };
    }

    /// <summary>Один проход resolver'а без CancelMembers: Т1, затем Т2.</summary>
    protected async Task<(CompletionCountResult Counted, ResolveResult Walked)> ResolveOnceAsync()
    {
        var counted = await Flow.CountCompletionsAsync(500, Ct);
        var walked = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);
        return (counted, walked);
    }

    /// <summary>Проходы, пока захватываются сигналы (отложенные retry_after не захватываются).</summary>
    protected async Task ResolveAllAsync()
    {
        for (var i = 0; i < 200; i++)
        {
            var (counted, walked) = await ResolveOnceAsync();
            if (counted.Captured == 0 && walked.Captured == 0)
                return;
        }
        throw new InvalidOperationException("The resolver did not settle within 200 passes.");
    }
```

- [x] **Step 2: Тесты правил и протокола**

`tests/PgBolt.Postgres.Tests/WorkflowResolverTests.cs`:

```csharp
using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §2.2, §2.3, §6.4: таблица исходов, каскады, исход batch, активация, протокол Т1/Т2.</summary>
public abstract class WorkflowResolverTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    private Task<string> EventMessageAsync(Guid id) =>
        Db.ScalarAsync<string>("SELECT message FROM {schema}.job_events WHERE job_id = @id ORDER BY at DESC LIMIT 1", ("id", id));

    [Theory]
    [InlineData(WorkflowOutcome.Succeeded, ContinuationCondition.OnSuccess, "launched")]
    [InlineData(WorkflowOutcome.Succeeded, ContinuationCondition.OnAnyFinished, "launched")]
    [InlineData(WorkflowOutcome.Failed, ContinuationCondition.OnSuccess, "awaiting")]
    [InlineData(WorkflowOutcome.Failed, ContinuationCondition.OnAnyFinished, "launched")]
    [InlineData(WorkflowOutcome.Cancelled, ContinuationCondition.OnSuccess, "cancelled")]
    [InlineData(WorkflowOutcome.Cancelled, ContinuationCondition.OnAnyFinished, "launched")]
    public async Task Parent_outcome_table(WorkflowOutcome outcome, ContinuationCondition condition, string expected)   // §2.2
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2), condition).SubmitAsync(Ct);
        var (parent, child) = (r.Jobs[0].Value, r.Jobs[1].Value);
        await FinishAsync(parent, outcome);

        await ResolveAllAsync();

        Assert.Equal(expected, await FateAsync(child));
        if (expected == "cancelled")
            Assert.Equal($"parent {parent} cancelled", await EventMessageAsync(child));
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Cancel_cascades_through_chain_of_three()   // §12 SQL-контракт
    {
        var root = Client.Workflow().Enqueue(A(0));
        var c1 = root.ContinueWith(A(1));
        var c2 = c1.ContinueWith(A(2));
        var c3 = c2.ContinueWith(A(3));
        await c3.SubmitAsync(Ct);
        await CancelJobAsync(root.Id.Value);

        await ResolveAllAsync();

        foreach (var c in new[] { c1, c2, c3 })
            Assert.Equal("cancelled", await FateAsync(c.Id.Value));
        Assert.Equal($"parent {c2.Id} cancelled", await EventMessageAsync(c3.Id.Value));
    }

    [Fact]
    public async Task Cancelling_awaiting_job_cascades_to_its_continuations()   // §12: CancelAsync → Cancelled и каскад
    {
        var root = Client.Workflow().Enqueue(A(0));
        var c1 = root.ContinueWith(A(1));
        var any = c1.ContinueWith(A(2), ContinuationCondition.OnAnyFinished);
        var success = c1.ContinueWith(A(3));
        await root.SubmitAsync(Ct);

        Assert.Equal(CancelResult.Cancelled, await Client.CancelAsync(c1.Id, ct: Ct));
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(any.Id.Value));
        Assert.Equal("cancelled", await FateAsync(success.Id.Value));
        Assert.Equal("launched", await FateAsync(root.Id.Value));   // родитель не затронут
    }

    [Theory]
    [InlineData(new[] { WorkflowOutcome.Succeeded, WorkflowOutcome.Succeeded }, (short)2)]
    [InlineData(new[] { WorkflowOutcome.Succeeded, WorkflowOutcome.Failed }, (short)3)]
    [InlineData(new[] { WorkflowOutcome.Failed, WorkflowOutcome.Cancelled }, (short)4)]
    [InlineData(new[] { WorkflowOutcome.Succeeded, WorkflowOutcome.Cancelled }, (short)4)]
    public async Task Batch_outcome_prefers_cancelled_then_failed(WorkflowOutcome[] members, short state)   // §2.3
    {
        IReadOnlyList<IJobStep> items = [];
        var batch = Client.Workflow().StartBatch(b => items = b.EnqueueMany(new[] { A(1), A(2) }));
        var any = batch.ContinueWith(A(3), ContinuationCondition.OnAnyFinished);
        var success = batch.ContinueWith(A(4));
        await batch.SubmitAsync(Ct);
        for (var i = 0; i < members.Length; i++)
            await FinishAsync(items[i].Id.Value, members[i]);
        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));   // до учёта resolver'ом

        await ResolveAllAsync();

        Assert.Equal((short?)state, await BatchStateAsync(batch.Id.Value));
        Assert.True(await Db.ScalarAsync<bool>(
            "SELECT finished_at IS NOT NULL AND succeeded + failed + cancelled = total FROM {schema}.batches WHERE id = @id",
            ("id", batch.Id.Value)));
        Assert.Equal("launched", await FateAsync(any.Id.Value));
        Assert.Equal(state switch { 2 => "launched", 3 => "awaiting", _ => "cancelled" }, await FateAsync(success.Id.Value));
    }

    [Fact]
    public async Task Member_waiting_for_failed_parent_keeps_batch_active()   // §2.3
    {
        IJobStep? x = null, y = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(1));
            y = x.ContinueWith(A(2));
        });
        await batch.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);

        await ResolveAllAsync();

        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));
        Assert.Equal("awaiting", await FateAsync(y!.Id.Value));
    }

    [Fact]
    public async Task Outcome_rises_through_three_levels_of_nesting()   // §12 Batches
    {
        IJobStep? x = null;
        IBatchStep? mid = null, inner = null;
        var outer = Client.Workflow().StartBatch(o => mid = o.StartBatch(m => inner = m.StartBatch(i => x = i.Enqueue(A(1)))));
        var next = outer.ContinueWith(A(2));
        await outer.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);

        await ResolveAllAsync();

        foreach (var b in new[] { inner!, mid!, outer })
            Assert.True(await Db.ScalarAsync<bool>("SELECT state = 3 AND failed = 1 FROM {schema}.batches WHERE id = @id", ("id", b.Id.Value)));
        Assert.Equal("awaiting", await FateAsync(next.Id.Value));
    }

    [Fact]
    public async Task Batch_continuation_activates_members_and_nested_batches()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null, n1 = null;
        IBatchStep? nested = null;
        var bc = p.ContinueWithBatch(b =>
        {
            m = b.Enqueue(A(1));
            nested = b.StartBatch(n => n1 = n.Enqueue(A(2)));
        });
        await p.SubmitAsync(Ct);

        await SucceedAsync(p.Id.Value);
        await ResolveAllAsync();

        Assert.Equal((short?)0, await BatchStateAsync(bc.Id.Value));
        Assert.Equal((short?)0, await BatchStateAsync(nested!.Id.Value));
        Assert.Equal("launched", await FateAsync(m!.Id.Value));
        Assert.Equal("launched", await FateAsync(n1!.Id.Value));
    }

    [Fact]
    public async Task Nested_batch_continuation_of_member_waits_for_the_member()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? x = null, y = null;
        IBatchStep? chained = null;
        var bc = p.ContinueWithBatch(b =>
        {
            x = b.Enqueue(A(1));
            chained = x.ContinueWithBatch(c => y = c.Enqueue(A(2)));
        });
        await p.SubmitAsync(Ct);

        await SucceedAsync(p.Id.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(x!.Id.Value));
        Assert.Equal((short?)1, await BatchStateAsync(chained!.Id.Value));   // не активирован вместе с внешним
        Assert.Equal("awaiting", await FateAsync(y!.Id.Value));

        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(y.Id.Value));

        await SucceedAsync(y.Id.Value);
        await ResolveAllAsync();
        Assert.Equal((short?)2, await BatchStateAsync(chained.Id.Value));
        Assert.Equal((short?)2, await BatchStateAsync(bc.Id.Value));
    }

    [Fact]
    public async Task Branch_cancel_by_outer_batch_names_nearest_cancelled_batch()   // §6.4 Т2.3: отмена ветки по внешнему batch
    {
        IJobStep? x = null, y = null;
        var outer = Client.Workflow().StartBatch(o => o.StartBatch(n =>
        {
            x = n.Enqueue(A(0));
            y = x.ContinueWith(A(1), ContinuationCondition.OnAnyFinished);
        }));
        await outer.SubmitAsync(Ct);
        // Отмена внешнего запрошена, а CancelMembers (Task 7) до вложенного ещё не дошёл: флаг есть только у внешнего.
        await Db.ExecAsync("UPDATE {schema}.batches SET cancel_requested = true WHERE id = @id", ("id", outer.Id.Value));

        await SucceedAsync(x!.Id.Value);
        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(y!.Id.Value));
        Assert.Equal($"batch {outer.Id} cancelled", await EventMessageAsync(y.Id.Value));   // не id собственного (вложенного) batch
    }

    [Fact]
    public async Task Subscribed_signal_applies_terminal_batch_outcome_or_is_dropped()   // §6.2, §6.4 Т2.2
    {
        IJobStep? m = null;
        var batch = (await Client.Workflow().StartBatch(b => m = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        var early = (await Client.Workflow().After(batch).ContinueWith(A(2)).SubmitAsync(Ct)).Jobs[0];
        await ResolveAllAsync();                                       // batch Active: Subscribed удалён, ребёнок ждёт
        Assert.Equal("awaiting", await FateAsync(early.Value));
        Assert.Equal(0L, await SignalsAsync());

        await SucceedAsync(m!.Id.Value);
        await ResolveAllAsync();
        var late = (await Client.Workflow().After(batch).ContinueWith(A(3)).SubmitAsync(Ct)).Jobs[0];
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(early.Value));
        Assert.Equal("launched", await FateAsync(late.Value));
    }

    [Fact]
    public async Task Count_deletes_500_leaf_member_signals_in_one_statement()   // §12 Batches
    {
        var batch = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, 1000, now());
            INSERT INTO {schema}.completions (node_id, kind, batch_id) SELECT gen_random_uuid(), 2, @b FROM generate_series(1, 500);
            """, ("b", batch));

        var counted = await Flow.CountCompletionsAsync(500, Ct);

        Assert.Equal((500, 0, 0), (counted.Captured, counted.Finished, counted.Walking));
        Assert.Equal(0L, await SignalsAsync());
        Assert.Equal(500, await Db.ScalarAsync<int>("SELECT succeeded FROM {schema}.batches WHERE id = @b", ("b", batch)));
    }

    [Fact]
    public async Task Counted_fan_out_signals_are_not_recaptured_by_count()   // §12 Batches
    {
        await Db.ExecAsync("""
            WITH p AS (SELECT gen_random_uuid() AS id FROM generate_series(1, 500)),
            c AS (INSERT INTO {schema}.completions (node_id, kind) SELECT id, 2 FROM p RETURNING node_id)
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            SELECT gen_random_uuid(), 'client', 0, 'infinity', false, 3, node_id, 1 FROM c;
            INSERT INTO {schema}.completions (node_id, kind) SELECT gen_random_uuid(), 2 FROM generate_series(1, 10);
            """);

        var first = await Flow.CountCompletionsAsync(500, Ct);
        var second = await Flow.CountCompletionsAsync(500, Ct);

        Assert.Equal((500, 500), (first.Captured, first.Walking));
        Assert.Equal((10, 0), (second.Captured, second.Walking));
        Assert.Equal(500L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE counted"));
    }

    /// <summary>Терминальные (Succeeded) родители в jobs, у каждого — ждущий OnSuccess-ребёнок; сигналы учтены Т1.</summary>
    private async Task<Guid[]> CountedParentsAsync(Guid[] parents, Guid[] children)
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, notify)
            SELECT id, 'client.test', 'client', 0, '{}', 2, 3, now(), true FROM unnest(@p) id;
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            SELECT c, 'client', 0, 'infinity', false, 3, p, 1 FROM unnest(@c, @p) AS t(c, p);
            INSERT INTO {schema}.completions (node_id, kind) SELECT id, 2 FROM unnest(@p) WITH ORDINALITY AS t(id, n) ORDER BY n;
            """, ("p", parents), ("c", children));
        var walking = 0;
        for (var i = 0; i < 10 && walking < parents.Length; i++)
            walking += (await Flow.CountCompletionsAsync(500, Ct)).Walking;
        Assert.Equal(parents.Length, walking);
        return children;
    }

    private static Guid[] NewIds(int n) => [.. Enumerable.Range(0, n).Select(_ => Guid.CreateVersion7())];

    [Theory]
    [InlineData(5)]
    [InlineData(500)]
    public async Task Walk_statement_count_does_not_depend_on_node_count(int nodes)   // §12 Batches: счётчик команд
    {
        await CountedParentsAsync(NewIds(nodes), NewIds(nodes));

        var walked = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal(6, walked.Statements);   // захват, этап 0, остаток этапа 0, этап 1, остаток этапа 1, завершение
        Assert.Equal(nodes, walked.Rows);
        Assert.Equal((long)nodes, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE ready AND await_condition IS NULL"));
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Signals_with_locked_children_are_deferred_and_do_not_block_next()   // §12 Batches, Review Focus 3
    {
        var children = await CountedParentsAsync(NewIds(501), NewIds(501));
        await using var holder = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var hold = new NpgsqlCommand(Db.Sql("BEGIN; SELECT 1 FROM {schema}.queue WHERE job_id = ANY(@ids) FOR UPDATE"), holder))
        {
            hold.Parameters.AddWithValue("ids", children[..500]);
            await hold.ExecuteNonQueryAsync(Ct);
        }

        var blocked = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);
        var next = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal((500, 500, 0), (blocked.Captured, blocked.Deferred, blocked.Rows));
        Assert.Equal((1, 0, 1), (next.Captured, next.Deferred, next.Rows));
        Assert.True(await ReadyAsync(children[500]));
        Assert.Equal(500L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE retry_after > now()"));

        await holder.CloseAsync();                                                   // откат держателя
        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");    // прошёл ResolverInterval
        Assert.Equal(500, (await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct)).Rows);
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Oldest_completion_age_reads_head_of_queue()
    {
        Assert.Null(await Flow.OldestCompletionAgeAsync(Ct));
        await Db.ExecAsync("INSERT INTO {schema}.completions (node_id, kind, created_at) VALUES (gen_random_uuid(), 2, now() - interval '1 minute')");

        Assert.InRange((await Flow.OldestCompletionAgeAsync(Ct))!.Value, TimeSpan.FromSeconds(59), TimeSpan.FromMinutes(5));
    }
}

public sealed class WorkflowResolverTests_Direct(PostgresFixture pg) : WorkflowResolverTests(pg, ConnectionMode.Direct);
public sealed class WorkflowResolverTests_Bouncer(PostgresFixture pg) : WorkflowResolverTests(pg, ConnectionMode.Bouncer);
```

`tests/PgBolt.Postgres.Tests/WorkflowResolverPlanTests.cs`:

```csharp
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>§12 Batches: fan-out 100 000, стоимость порции, частичные индексы захватов. Только Direct (EXPLAIN), Slow.</summary>
[Trait("Category", "Slow")]
public sealed class WorkflowResolverPlanTests(PostgresFixture pg) : WorkflowTestBase(pg, ConnectionMode.Direct)
{
    /// <summary>Активированный batch (state 0) с count ждущими членами (код 5) и учтённым сигналом Activated.</summary>
    private async Task<Guid> ActivatedFanOutAsync(int count)
    {
        var batch = Guid.CreateVersion7();
        var members = Enumerable.Range(0, count).Select(_ => Guid.CreateVersion7()).ToArray();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@b, 0, cardinality(@ids), now());
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, parent_id, batch_id, continuation, notify)
            SELECT id, 'client.test', 'fan', 0, '{}', 0, 3, now(), @b, @b, 5, true FROM unnest(@ids) id;
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition, batch_id, notify)
            SELECT id, 'fan', 0, 'infinity', false, 3, @b, 5, @b, true FROM unnest(@ids) id;
            INSERT INTO {schema}.completions (node_id, kind) VALUES (@b, 5);
            ANALYZE {schema}.queue;
            """, ("b", batch), ("ids", members));
        Assert.Equal(1, (await Flow.CountCompletionsAsync(500, Ct)).Walking);
        return batch;
    }

    [Fact]
    public async Task Fan_out_of_100000_members_is_walked_in_bounded_portions()
    {
        var batch = await ActivatedFanOutAsync(100_000);
        var passes = 0;
        PgBolt.Postgres.Storage.ResolveResult r;
        do
        {
            r = await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);
            passes++;
            Assert.True(r.Rows <= 5000, $"pass {passes} walked {r.Rows} rows");
            Assert.True(r.MaxPortion <= 1000, $"pass {passes} portion {r.MaxPortion}");
        }
        while (r.Captured > 0 && passes < 100);

        Assert.Equal(22, passes);   // 20 проходов по 5000, 21-й находит конец и удаляет сигнал, 22-й пуст
        Assert.Equal(100_000L, await Db.ScalarAsync<long>(
            "SELECT count(*) FROM {schema}.queue WHERE batch_id = @b AND ready AND await_condition IS NULL", ("b", batch)));
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Walk_portion_reads_do_not_grow_with_cursor_position()
    {
        var batch = await ActivatedFanOutAsync(20_000);
        var early = await IndexScanBuffersAsync(batch, cursor: null);
        for (var i = 0; i < 3; i++)
            await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);   // 15 000 запущено: мёртвые версии позади курсора
        var cursor = await Db.ScalarAsync<Guid>("SELECT cursor FROM {schema}.completions WHERE node_id = @b", ("b", batch));

        var late = await IndexScanBuffersAsync(batch, cursor);

        Assert.True(late <= early * 2 + 20, $"queue_awaiting scan: {early} buffers at start, {late} after 15 000 launched rows");
    }

    /// <summary>Буферы узла Index Scan по queue_awaiting в EXPLAIN (ANALYZE, BUFFERS) statement'а этапа 0; транзакция откатывается.</summary>
    private async Task<long> IndexScanBuffersAsync(Guid batch, Guid? cursor)
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + Flow.WalkJobsSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("nodes", [batch]));
        cmd.Parameters.Add(new NpgsqlParameter<short[]>("kinds", [5]));
        cmd.Parameters.Add(new NpgsqlParameter("cursors", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = new Guid?[] { cursor } });
        cmd.Parameters.Add(new NpgsqlParameter<int>("lim", 1000));
        cmd.Parameters.Add(new NpgsqlParameter<short>("kind_cancelled", (short)JobEventKind.Cancelled));
        var plan = (string)(await cmd.ExecuteScalarAsync(Ct))!;
        await tx.RollbackAsync(Ct);
        using var doc = JsonDocument.Parse(plan);
        var scan = Assert.Single(Flatten(doc.RootElement[0].GetProperty("Plan")),
            n => n.TryGetProperty("Index Name", out var name) && name.GetString() == "queue_awaiting");
        return scan.GetProperty("Shared Hit Blocks").GetInt64() + scan.GetProperty("Shared Read Blocks").GetInt64();
    }

    [Fact]
    public async Task Captures_use_their_partial_indexes()   // §12 Batches: EXPLAIN захватов Т1 и Т2
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.completions (node_id, kind, counted) SELECT gen_random_uuid(), 2, g % 2 = 0 FROM generate_series(1, 2000) g;
            INSERT INTO {schema}.completions (node_id, kind) SELECT gen_random_uuid(), 6 FROM generate_series(1, 200);
            ANALYZE {schema}.completions;
            """);

        Assert.Contains("completions_count", await IndexesOfPlanAsync(Flow.CountSql));
        Assert.Contains("completions_walk", await IndexesOfPlanAsync(Flow.WalkCaptureSql));
    }

    private async Task<List<string>> IndexesOfPlanAsync(string sql)
    {
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var off = new NpgsqlCommand("SET enable_seqscan = off", conn))
            await off.ExecuteNonQueryAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, conn);
        cmd.Parameters.Add(new NpgsqlParameter<int>("n", 500));
        using var doc = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(Ct))!);
        return [.. Flatten(doc.RootElement[0].GetProperty("Plan"))
            .Where(n => n.TryGetProperty("Index Name", out _))
            .Select(n => n.GetProperty("Index Name").GetString()!)];
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var n in Flatten(child))
                    yield return n;
    }
}
```

- [x] **Step 3: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowResolver"`
Expected: FAIL — нет `CountCompletionsAsync`, `ResolveAsync`, `ResolveResult`, `CompletionCountResult`.

- [x] **Step 4: Записи resolver'а**

В `src/PgBolt.Postgres/Storage/WorkflowRecords.cs` добавить:

```csharp
/// <summary>
/// Итог Т1 (§6.4): Captured — захвачено сигналов (= batchSize — пакет полный); Finished — batches перешли в терминальное
/// состояние (новые сигналы); Walking — сигналы с детьми переданы Т2; MissingBatches — дельты для удалённых строк batches (Debug).
/// </summary>
internal sealed record CompletionCountResult(int Captured, int Finished, int Walking, IReadOnlyList<Guid> MissingBatches)
{
    public static readonly CompletionCountResult Empty = new(0, 0, 0, []);
}

/// <summary>
/// Итог Т2 (§6.4): Rows — строк детей в порциях; Statements — команд транзакции (не зависит от числа узлов); MaxPortion — наибольшая
/// порция узла; BudgetExhausted — остались узлы без остатка из-за бюджета; NewSignals — Activated / CancelMembers / Cancelled-детей;
/// Deferred — сигналы с retry_after (остаток заблокирован другими); Queues — очереди запущенных детей.
/// </summary>
internal sealed record ResolveResult(int Captured, int Rows, int Statements, int MaxPortion, bool BudgetExhausted, int NewSignals,
    int Deferred, IReadOnlyCollection<string> Queues)
{
    public static readonly ResolveResult Empty = new(0, 0, 1, 0, false, 0, 0, []);
}

/// <summary>Точки тестового хука resolver'а: перед COMMIT транзакции Т2 и порции CancelMembers.</summary>
internal enum ResolverStep { Walked, CancelPortion }
```

- [x] **Step 5: Т1 и Т2**

`src/PgBolt.Postgres/Storage/PgBoltStore.Resolver.cs`:

```csharp
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Resolver части 5 (§6.4). Т1 — один statement: захват, счётчики batches, сигналы исходов batches, отсев сигналов без детей.
/// Т2 — одна транзакция с бюджетом строк: statement на этап (задачи, затем batches), а не на узел; исход узла перечитывается в
/// каждом statement'е выборки детей; строки queue / batches — SKIP LOCKED, остаток за чужими блокировками — retry_after.
/// </summary>
public sealed partial class PgBoltStore
{
    private const int MaxWalkPortion = 1000;

    private const string CountTemplate = """
        WITH c AS MATERIALIZED (
            SELECT id, node_id, kind, batch_id FROM {schema}.completions
            WHERE kind <> 6 AND NOT counted AND (retry_after IS NULL OR retry_after <= now())
            ORDER BY id
            LIMIT @n
            FOR UPDATE SKIP LOCKED
        ),
        dl AS (
            SELECT batch_id,
                   (count(*) FILTER (WHERE kind = 2))::int AS s,
                   (count(*) FILTER (WHERE kind = 3))::int AS f,
                   (count(*) FILTER (WHERE kind = 4))::int AS x
            FROM c
            WHERE batch_id IS NOT NULL AND kind IN (2, 3, 4)
            GROUP BY batch_id
        ),
        lb AS MATERIALIZED (
            SELECT b.id, b.state, b.total, b.succeeded, b.failed, b.cancelled, b.parent_batch_id
            FROM {schema}.batches b
            WHERE b.id IN (SELECT batch_id FROM dl)
            ORDER BY b.id
            FOR NO KEY UPDATE
        ),
        n AS (
            SELECT lb.id, lb.parent_batch_id, lb.state AS old_state,
                   lb.succeeded + dl.s AS s, lb.failed + dl.f AS f, lb.cancelled + dl.x AS x,
                   (CASE WHEN lb.state IN (0, 1) AND lb.succeeded + dl.s + lb.failed + dl.f + lb.cancelled + dl.x = lb.total
                         THEN CASE WHEN lb.cancelled + dl.x > 0 THEN 4 WHEN lb.failed + dl.f > 0 THEN 3 ELSE 2 END
                         ELSE lb.state END)::smallint AS new_state
            FROM lb JOIN dl ON dl.batch_id = lb.id
        ),
        ub AS (
            UPDATE {schema}.batches b
            SET succeeded = n.s, failed = n.f, cancelled = n.x, state = n.new_state,
                finished_at = CASE WHEN n.new_state <> n.old_state THEN now() ELSE b.finished_at END
            FROM n
            WHERE b.id = n.id
        ),
        fin AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, new_state, parent_batch_id FROM n WHERE new_state <> old_state
            RETURNING 1
        ),
        k AS (
            SELECT c.id,
                   EXISTS (SELECT 1 FROM {schema}.queue q
                           WHERE q.parent_id = c.node_id AND q.await_condition IS NOT NULL
                             AND q.await_condition = ANY (CASE c.kind WHEN 3 THEN '{2,4}'::smallint[] WHEN 5 THEN '{5}'::smallint[]
                                                                      ELSE '{1,2,3,4}'::smallint[] END))
                   OR (c.kind <> 5 AND EXISTS (
                           SELECT 1 FROM {schema}.batches b
                           WHERE b.parent_id = c.node_id AND b.parent_id IS NOT NULL AND b.state = 1 AND NOT b.cancel_requested
                             AND b.condition = ANY (CASE c.kind WHEN 3 THEN '{2,4}'::smallint[] ELSE '{1,2,3,4}'::smallint[] END)))
                   OR (c.kind = 5 AND EXISTS (
                           SELECT 1 FROM {schema}.batches b
                           WHERE b.parent_batch_id = c.node_id AND b.parent_batch_id IS NOT NULL AND b.parent_id IS NULL
                             AND b.state = 1 AND NOT b.cancel_requested)) AS has_children
            FROM c
        ),
        del AS (
            DELETE FROM {schema}.completions t USING k WHERE t.id = k.id AND NOT k.has_children
        ),
        walk AS (
            UPDATE {schema}.completions t SET counted = true FROM k WHERE t.id = k.id AND k.has_children
            RETURNING 1
        )
        SELECT (SELECT count(*) FROM c)::int AS captured,
               (SELECT count(*) FROM fin)::int AS finished,
               (SELECT count(*) FROM walk)::int AS walking,
               ARRAY(SELECT batch_id FROM dl WHERE batch_id NOT IN (SELECT id FROM lb)) AS missing
        """;

    private const string WalkCaptureTemplate = """
        SELECT id, node_id, kind, stage, cursor FROM {schema}.completions
        WHERE counted AND (retry_after IS NULL OR retry_after <= now())
        ORDER BY id
        LIMIT @n
        FOR UPDATE SKIP LOCKED
        """;

    /// <summary>Исход узла в снимке statement'а (§6.4 Т2.2): 2/3/4 — если состояние узла равно kind; 7 — терминальный batches.state; 5 — всегда.</summary>
    private const string WalkNodesCte = """
        n AS MATERIALIZED (
            SELECT u.ord, u.node, COALESCE(u.cur, '00000000-0000-0000-0000-000000000000'::uuid) AS cur, o.outcome,
                   CASE o.outcome WHEN 3 THEN '{2,4}'::smallint[] WHEN 5 THEN '{5}'::smallint[] ELSE '{1,2,3,4}'::smallint[] END AS conds
            FROM unnest(@nodes::uuid[], @kinds::smallint[], @cursors::uuid[]) WITH ORDINALITY AS u(node, kind, cur, ord)
            CROSS JOIN LATERAL (
                SELECT CASE
                    WHEN u.kind = 5 THEN 5::smallint
                    WHEN u.kind = 7 THEN (SELECT b.state FROM {schema}.batches b WHERE b.id = u.node AND b.state IN (2, 3, 4))
                    WHEN u.kind = COALESCE((SELECT j.state FROM {schema}.jobs j WHERE j.id = u.node AND j.id = ANY(@nodes)),
                                           (SELECT b.state FROM {schema}.batches b WHERE b.id = u.node)) THEN u.kind
                END AS outcome
            ) o
        )
        """;

    /// <summary>
    /// §6.4 Т2.3: дети-задачи всех узлов этапа. Запуск — UPDATE queue; отмена (Cancelled-родитель с OnSuccess или отмена ветки
    /// по cancel_requested batch ребёнка и внешних) — statement cancel core §5.7 с дополнением §6.3.
    /// </summary>
    private const string WalkJobsTemplate = "WITH RECURSIVE " + WalkNodesCte + """
        ,
        c AS MATERIALIZED (
            SELECT n.ord, n.node, n.outcome, q.job_id, q.await_condition, q.batch_id
            FROM n CROSS JOIN LATERAL (
                SELECT job_id, await_condition, batch_id FROM {schema}.queue
                WHERE parent_id = n.node AND await_condition IS NOT NULL AND await_condition = ANY(n.conds) AND job_id >= n.cur
                ORDER BY job_id
                LIMIT @lim
                FOR UPDATE SKIP LOCKED
            ) q
            WHERE n.outcome IS NOT NULL
        ),
        up(start, id, depth) AS (
            SELECT DISTINCT batch_id, batch_id, 0 FROM c WHERE batch_id IS NOT NULL
            UNION
            SELECT up.start, b.parent_batch_id, up.depth + 1
            FROM up JOIN {schema}.batches b ON b.id = up.id WHERE b.parent_batch_id IS NOT NULL
        ),
        br AS (
            -- Ближайший к ребёнку batch цепочки с запрошенной отменой (свой или внешний) — он и назван в сообщении.
            SELECT up.start AS batch_id,
                   (array_agg(up.id ORDER BY up.depth) FILTER (WHERE b.cancel_requested))[1] AS cancelled_by
            FROM up JOIN {schema}.batches b ON b.id = up.id
            GROUP BY up.start
        ),
        d AS MATERIALIZED (
            SELECT c.job_id,
                   CASE WHEN br.cancelled_by IS NOT NULL THEN 'batch ' || br.cancelled_by || ' cancelled'
                        WHEN c.outcome = 4 AND c.await_condition IN (1, 3) THEN 'parent ' || c.node || ' cancelled'
                   END AS cancel_reason
            FROM c LEFT JOIN br ON br.batch_id = c.batch_id
        ),
        l AS (
            UPDATE {schema}.queue q SET await_condition = NULL, run_at = now(), ready = true
            FROM d WHERE q.job_id = d.job_id AND d.cancel_reason IS NULL
            RETURNING q.queue
        ),
        x AS (
            DELETE FROM {schema}.queue q USING d
            WHERE q.job_id = d.job_id AND d.cancel_reason IS NOT NULL
            RETURNING q.job_id, q.attempt, q.notify, d.cancel_reason
        ),
        xj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = x.attempt, finished_at = now(), notify = j.notify OR x.notify
            FROM x WHERE j.id = x.job_id
            RETURNING j.id, j.batch_id, x.notify
        ),
        xe AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
            SELECT job_id, now(), @kind_cancelled, attempt, cancel_reason FROM x
        ),
        xs AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 4, batch_id FROM xj WHERE notify
            RETURNING 1
        )
        SELECT n.ord, n.outcome,
               (SELECT count(*) FROM c WHERE c.ord = n.ord)::int AS taken,
               (SELECT c.job_id FROM c WHERE c.ord = n.ord ORDER BY c.job_id DESC LIMIT 1) AS last_id,
               ARRAY(SELECT DISTINCT queue FROM l) AS queues,
               (SELECT count(*) FROM xs)::int AS signals
        FROM n
        ORDER BY n.ord
        """;

    /// <summary>
    /// §6.4 Т2.4: дети-batches. 2/3/4/7 — batch-продолжения узла (batches_awaiting); 5 — вложенные без родителя (batches_nested).
    /// Запуск — state = 0 и Activated; отмена (Cancelled-родитель с OnSuccess или отмена ветки от parent_batch_id) —
    /// cancel_requested и CancelMembers; batch с собственным cancel_requested не трогается.
    /// </summary>
    private const string WalkBatchesTemplate = "WITH RECURSIVE " + WalkNodesCte + """
        ,
        c AS MATERIALIZED (
            SELECT n.ord, n.outcome, b.id, b.condition, b.parent_batch_id
            FROM n CROSS JOIN LATERAL (
                SELECT id, condition, parent_batch_id FROM {schema}.batches
                WHERE parent_id = n.node AND parent_id IS NOT NULL AND state = 1 AND NOT cancel_requested
                  AND condition = ANY(n.conds) AND id >= n.cur
                ORDER BY id
                LIMIT @lim
                FOR NO KEY UPDATE SKIP LOCKED
            ) b
            WHERE n.outcome IS NOT NULL AND n.outcome <> 5
            UNION ALL
            SELECT n.ord, n.outcome, b.id, b.condition, b.parent_batch_id
            FROM n CROSS JOIN LATERAL (
                SELECT id, condition, parent_batch_id FROM {schema}.batches
                WHERE parent_batch_id = n.node AND parent_batch_id IS NOT NULL AND parent_id IS NULL AND state = 1
                  AND NOT cancel_requested AND id >= n.cur
                ORDER BY id
                LIMIT @lim
                FOR NO KEY UPDATE SKIP LOCKED
            ) b
            WHERE n.outcome = 5
        ),
        up(start, id) AS (
            SELECT DISTINCT parent_batch_id, parent_batch_id FROM c WHERE parent_batch_id IS NOT NULL
            UNION
            SELECT up.start, b.parent_batch_id FROM up JOIN {schema}.batches b ON b.id = up.id WHERE b.parent_batch_id IS NOT NULL
        ),
        br AS (
            SELECT up.start AS batch_id, bool_or(b.cancel_requested) AS cancelled
            FROM up JOIN {schema}.batches b ON b.id = up.id
            GROUP BY up.start
        ),
        d AS MATERIALIZED (
            SELECT c.id, COALESCE(br.cancelled, false) OR (c.outcome = 4 AND c.condition IN (1, 3)) AS cancel
            FROM c LEFT JOIN br ON br.batch_id = c.parent_batch_id
        ),
        u AS (
            UPDATE {schema}.batches b
            SET state = CASE WHEN d.cancel THEN b.state ELSE 0 END, cancel_requested = d.cancel
            FROM d WHERE b.id = d.id
            RETURNING b.id, d.cancel
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind)
            SELECT id, CASE WHEN cancel THEN 6 ELSE 5 END FROM u
            RETURNING 1
        )
        SELECT n.ord, n.outcome,
               (SELECT count(*) FROM c WHERE c.ord = n.ord)::int AS taken,
               (SELECT c.id FROM c WHERE c.ord = n.ord ORDER BY c.id DESC LIMIT 1) AS last_id,
               '{}'::text[] AS queues,
               (SELECT count(*) FROM s)::int AS signals
        FROM n
        ORDER BY n.ord
        """;

    /// <summary>§6.4 Т2.3: остаток узлов, дошедших до конца, — первая строка тем же предикатом без курсора и без блокировки.</summary>
    private const string WalkJobsTailTemplate = """
        SELECT u.ord,
               (SELECT q.job_id FROM {schema}.queue q
                WHERE q.parent_id = u.node AND q.await_condition IS NOT NULL
                  AND q.await_condition = ANY (CASE u.outcome WHEN 3 THEN '{2,4}'::smallint[] WHEN 5 THEN '{5}'::smallint[]
                                                              ELSE '{1,2,3,4}'::smallint[] END)
                ORDER BY q.job_id
                LIMIT 1) AS first_id
        FROM unnest(@nodes::uuid[], @outcomes::smallint[]) WITH ORDINALITY AS u(node, outcome, ord)
        """;

    private const string WalkBatchesTailTemplate = """
        SELECT u.ord,
               CASE WHEN u.outcome = 5 THEN
                   (SELECT b.id FROM {schema}.batches b
                    WHERE b.parent_batch_id = u.node AND b.parent_batch_id IS NOT NULL AND b.parent_id IS NULL AND b.state = 1
                      AND NOT b.cancel_requested
                    ORDER BY b.id LIMIT 1)
               ELSE
                   (SELECT b.id FROM {schema}.batches b
                    WHERE b.parent_id = u.node AND b.parent_id IS NOT NULL AND b.state = 1 AND NOT b.cancel_requested
                      AND b.condition = ANY (CASE u.outcome WHEN 3 THEN '{2,4}'::smallint[] ELSE '{1,2,3,4}'::smallint[] END)
                    ORDER BY b.id LIMIT 1)
               END AS first_id
        FROM unnest(@nodes::uuid[], @outcomes::smallint[]) WITH ORDINALITY AS u(node, outcome, ord)
        """;

    /// <summary>§6.4 Т2.5: прошедшие оба этапа без остатка удаляются; остальные — stage, cursor, retry_after.</summary>
    private const string WalkFinishTemplate = """
        WITH f AS (
            SELECT * FROM unnest(@ids::bigint[], @done::boolean[], @stages::smallint[], @cursors::uuid[], @defer::boolean[])
                     AS f(id, done, stage, cur, defer)
        ),
        del AS (
            DELETE FROM {schema}.completions c USING f WHERE c.id = f.id AND f.done
        )
        UPDATE {schema}.completions c
        SET stage = f.stage, cursor = f.cur, retry_after = CASE WHEN f.defer THEN now() + @retry ELSE NULL END
        FROM f
        WHERE c.id = f.id AND NOT f.done
        """;

    private const string OldestCompletionTemplate = "SELECT now() - created_at FROM {schema}.completions ORDER BY id LIMIT 1";

    internal string CountSql => field ??= Render(CountTemplate);
    internal string WalkCaptureSql => field ??= Render(WalkCaptureTemplate);
    internal string WalkJobsSql => field ??= Render(WalkJobsTemplate);
    private string WalkBatchesSql => field ??= Render(WalkBatchesTemplate);
    private string WalkJobsTailSql => field ??= Render(WalkJobsTailTemplate);
    private string WalkBatchesTailSql => field ??= Render(WalkBatchesTailTemplate);
    private string WalkFinishSql => field ??= Render(WalkFinishTemplate);
    private string OldestCompletionSql => field ??= Render(OldestCompletionTemplate);

    /// <summary>Только для тестов: вызывается перед COMMIT транзакции Т2 (Walked) и порции CancelMembers (CancelPortion).</summary>
    internal Func<ResolverStep, CancellationToken, Task>? ResolverHook { get; set; }

    /// <summary>Т1 (§6.4): один statement; повтор целиком при 40P01/40001.</summary>
    internal Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        return QueryAsync(CountSql, ps => P.Add(ps, "n", batchSize), async (r, c) =>
        {
            await r.ReadAsync(c);
            return new CompletionCountResult(r.GetInt32(0), r.GetInt32(1), r.GetInt32(2), r.GetFieldValue<Guid[]>(3));
        }, ct);
    }

    /// <summary>Возраст старейшего сигнала (§8: Warning при лаге resolver'а); null — сигналов нет.</summary>
    internal Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct) =>
        QueryAsync<TimeSpan?>(OldestCompletionSql, _ => { }, async (r, c) => await r.ReadAsync(c) ? r.GetFieldValue<TimeSpan>(0) : null, ct);

    private sealed class WalkNode(long id, Guid node, short kind, short stage, Guid? cursor)
    {
        public long Id { get; } = id;
        public Guid Node { get; } = node;
        public short Kind { get; } = kind;
        public short Stage { get; set; } = stage;
        public Guid? Cursor { get; set; } = cursor;
        public short? Outcome { get; set; }
        public bool Done { get; set; }        // сигнал удаляется: обход завершён или сигнал устарел
        public bool Ended { get; set; }       // порция меньше лимита — этап пройден, нужна проверка остатка
        public bool Deferred { get; set; }    // остаток заблокирован другими — retry_after
    }

    private sealed class WalkProgress(int budget)
    {
        public int Left { get; set; } = budget;
        public int Rows { get; set; }
        public int Statements { get; set; } = 1;   // захват
        public int MaxPortion { get; set; }
        public int NewSignals { get; set; }
        public HashSet<string> Queues { get; } = new(StringComparer.Ordinal);
    }

    /// <summary>Т2 (§6.4): одна транзакция; повтор целиком при 40P01/40001.</summary>
    internal Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowBudget);
        return RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);
            var nodes = await CaptureWalkAsync(conn, tx, batchSize, c);
            if (nodes.Count == 0)
            {
                await tx.CommitAsync(c);
                return ResolveResult.Empty;
            }

            var walk = new WalkProgress(rowBudget);
            foreach (var stage in new short[] { 0, 1 })
            {
                while (walk.Left > 0)
                {
                    var active = nodes.Where(n => n.Stage == stage && !n.Done && !n.Deferred && !n.Ended).ToList();
                    if (active.Count == 0)
                        break;
                    var lim = Math.Min(MaxWalkPortion, Math.Max(1, walk.Left / active.Count));
                    await WalkStageAsync(conn, tx, stage, active, lim, walk, c);
                }

                var ended = nodes.Where(n => n.Stage == stage && n.Ended && !n.Done).ToList();
                if (ended.Count == 0)
                    continue;
                var rest = await WalkTailAsync(conn, tx, stage, ended, c);
                walk.Statements++;
                foreach (var n in ended)
                {
                    n.Ended = false;
                    if (rest.TryGetValue(n.Id, out var first))
                    {
                        n.Cursor = first;
                        n.Deferred = true;
                    }
                    else if (stage == 0)
                    {
                        n.Stage = 1;
                        n.Cursor = null;
                    }
                    else
                    {
                        n.Done = true;
                    }
                }
            }

            await FinishWalkAsync(conn, tx, nodes, retryDelay, c);
            walk.Statements++;
            if (ResolverHook is { } hook)
                await hook(ResolverStep.Walked, c);
            await tx.CommitAsync(c);
            return new ResolveResult(nodes.Count, walk.Rows, walk.Statements, walk.MaxPortion,
                BudgetExhausted: nodes.Any(n => !n.Done && !n.Deferred), walk.NewSignals, nodes.Count(n => n.Deferred), walk.Queues);
        }, ct);
    }

    private async Task<List<WalkNode>> CaptureWalkAsync(NpgsqlConnection conn, NpgsqlTransaction tx, int batchSize, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(WalkCaptureSql, conn, tx);
        P.Add(cmd.Parameters, "n", batchSize);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var nodes = new List<WalkNode>();
        while (await r.ReadAsync(ct))
            nodes.Add(new WalkNode(r.GetInt64(0), r.GetGuid(1), r.GetInt16(2), r.GetInt16(3), r.GetNullableValue<Guid>(4)));
        return nodes;
    }

    private async Task WalkStageAsync(NpgsqlConnection conn, NpgsqlTransaction tx, short stage, List<WalkNode> active, int lim,
        WalkProgress walk, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(stage == 0 ? WalkJobsSql : WalkBatchesSql, conn, tx);
        var ps = cmd.Parameters;
        P.Add(ps, "nodes", active.Select(n => n.Node).ToArray());
        P.Add(ps, "kinds", active.Select(n => n.Kind).ToArray());
        P.Add(ps, "cursors", active.Select(n => n.Cursor).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "lim", lim);
        if (stage == 0)
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
        walk.Statements++;
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var first = true;
        while (await r.ReadAsync(ct))
        {
            if (first)
            {
                foreach (var queue in r.GetFieldValue<string[]>(4))
                    walk.Queues.Add(queue);
                walk.NewSignals += r.GetInt32(5);
                first = false;
            }
            var n = active[(int)r.GetInt64(0) - 1];
            if (r.IsDBNull(1))
            {
                n.Done = true;   // §6.4 Т2.2: устаревший сигнал (requeue, строки нет, batch не терминален) — к детям не применяется
                continue;
            }
            n.Outcome = r.GetInt16(1);
            var taken = r.GetInt32(2);
            walk.Rows += taken;
            walk.Left -= taken;
            walk.MaxPortion = Math.Max(walk.MaxPortion, taken);
            if (taken < lim)
                n.Ended = true;
            else
                n.Cursor = r.GetGuid(3);   // курсор включающий: обработанные строки предикату больше не удовлетворяют
        }
    }

    private async Task<Dictionary<long, Guid>> WalkTailAsync(NpgsqlConnection conn, NpgsqlTransaction tx, short stage, List<WalkNode> ended,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(stage == 0 ? WalkJobsTailSql : WalkBatchesTailSql, conn, tx);
        P.Add(cmd.Parameters, "nodes", ended.Select(n => n.Node).ToArray());
        P.Add(cmd.Parameters, "outcomes", ended.Select(n => n.Outcome!.Value).ToArray());
        var rest = new Dictionary<long, Guid>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            if (!r.IsDBNull(1))
                rest[ended[(int)r.GetInt64(0) - 1].Id] = r.GetGuid(1);
        return rest;
    }

    private async Task FinishWalkAsync(NpgsqlConnection conn, NpgsqlTransaction tx, List<WalkNode> nodes, TimeSpan retryDelay,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(WalkFinishSql, conn, tx);
        var ps = cmd.Parameters;
        P.Add(ps, "ids", nodes.Select(n => n.Id).ToArray());
        P.Add(ps, "done", nodes.Select(n => n.Done).ToArray());
        P.Add(ps, "stages", nodes.Select(n => n.Stage).ToArray());
        P.Add(ps, "cursors", nodes.Select(n => n.Cursor).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        P.Add(ps, "defer", nodes.Select(n => n.Deferred).ToArray());
        P.Add(ps, "retry", retryDelay);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowResolver"`
Expected: PASS (`WorkflowResolverTests` — Direct и Bouncer; `WorkflowResolverPlanTests` — Direct, ~1 мин). Если `EXPLAIN`-тест показывает Seq Scan или другой индекс — `BLOCKED` с планом (правило исполнителя), не ослаблять assert.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): resolver counting and child walk passes"
```

---
### Task 7: Отмена batch: `CancelBatchAsync`, `GetBatchAsync`, `CancelMembers` (§6.5)

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/WorkflowRecords.cs` (`CancelMembersResult`)
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Batches.cs`
- Modify: `src/PgBolt.Abstractions/IJobClient.cs`, `src/PgBolt.Postgres/Client/PgBoltJobClient.cs`
- Modify (тесты): `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` (`ResolveAllAsync` с CancelMembers, `ResolveWithoutCancelMembersAsync`, `NextMillisecond`, `EventMessageAsync`)
- Test: `tests/PgBolt.Postgres.Tests/WorkflowCancelTests.cs`

**Interfaces:**
- Consumes: Т1/Т2, `ResolverHook`, `ResolverStep` (Task 6); `WorkflowCodes`, `Completions` (Task 3).
- Produces: `IJobClient.CancelBatchAsync(BatchId, CancellationToken)`, `IJobClient.GetBatchAsync(BatchId, CancellationToken)`; `PgBoltStore.CancelBatchAsync(Guid, CancellationToken) : Task<BatchCancelResult>` (новый сигнал → `Completions.Signal()`), `GetBatchAsync(Guid, CancellationToken) : Task<BatchInfo?>` (счётчики `greatest(0, …)`), `CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay, CancellationToken) : Task<CancelMembersResult>`.

- [x] **Step 1: Тестовая поддержка**

В `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs`, класс `WorkflowTestBase`: заменить `ResolveAllAsync` и добавить помощники:

```csharp
    /// <summary>Проходы Т1 + Т2 без CancelMembers, пока захватываются сигналы: идущая отмена batch не продвигается.</summary>
    protected async Task ResolveWithoutCancelMembersAsync()
    {
        for (var i = 0; i < 200; i++)
        {
            var (counted, walked) = await ResolveOnceAsync();
            if (counted.Captured == 0 && walked.Captured == 0)
                return;
        }
        throw new InvalidOperationException("The resolver did not settle within 200 passes.");
    }

    /// <summary>Итерации resolver'а (Т1, Т2, CancelMembers), пока захватываются сигналы и выполняются порции отмены.</summary>
    protected async Task ResolveAllAsync()
    {
        for (var i = 0; i < 200; i++)
        {
            var (counted, walked) = await ResolveOnceAsync();
            var cancelled = await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);
            if (counted.Captured == 0 && walked.Captured == 0 && cancelled.Portions == 0)
                return;
        }
        throw new InvalidOperationException("The resolver did not settle within 200 passes.");
    }

    /// <summary>
    /// UUIDv7 упорядочены только с точностью до миллисекунды (Guid.CreateVersion7: остальное — случайно). Ждёт следующую
    /// миллисекунду, чтобы id шагов, созданных после вызова, были больше id созданных до него (порядок обхода CancelMembers).
    /// </summary>
    protected static void NextMillisecond()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        SpinWait.SpinUntil(() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > now);
    }

    protected Task<string> EventMessageAsync(Guid id) =>
        Db.ScalarAsync<string>("SELECT message FROM {schema}.job_events WHERE job_id = @id ORDER BY at DESC LIMIT 1", ("id", id));
```

В `tests/PgBolt.Postgres.Tests/WorkflowResolverTests.cs` удалить локальный `EventMessageAsync` (теперь в базе).

- [x] **Step 2: Тесты**

`tests/PgBolt.Postgres.Tests/WorkflowCancelTests.cs`:

```csharp
using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.5, §2.3: CancelBatchAsync, CancelMembers, отмена ветки, GetBatchAsync.</summary>
public abstract class WorkflowCancelTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    [Fact]
    public async Task Cancel_batch_reports_not_found_requested_and_already_finished()   // §12 CancelBatchAsync
    {
        Assert.Equal(BatchCancelResult.NotFound, await Client.CancelBatchAsync(BatchId.New(), Ct));
        var batch = (await Client.Workflow().StartBatch(b => b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));   // повтор — без нового сигнала
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 6"));
        Assert.Equal(1, Signal.Count);

        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(batch.Value));
        Assert.Equal(BatchCancelResult.AlreadyFinished, await Client.CancelBatchAsync(batch, Ct));
    }

    [Fact]
    public async Task Cancel_cancels_waiting_members_now_running_through_heartbeat_and_nested_recursively()   // §12 CancelBatchAsync
    {
        IJobStep? pending = null, running = null, chained = null, inner = null;
        IBatchStep? nested = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            pending = b.Enqueue(A(1));
            running = b.Enqueue(A(2));
            chained = running.ContinueWith(A(3));
            nested = b.StartBatch(n => inner = n.Enqueue(A(4)));
        });
        var after = batch.ContinueWith(A(5), ContinuationCondition.OnAnyFinished);
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(running!.Id.Value);

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch.Id, Ct));
        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(pending!.Id.Value));
        Assert.Equal($"batch {batch.Id} cancelled", await EventMessageAsync(pending.Id.Value));
        Assert.Equal("cancelled", await FateAsync(chained!.Id.Value));
        Assert.Equal("cancelled", await FateAsync(inner!.Id.Value));
        Assert.Equal($"batch {nested!.Id} cancelled", await EventMessageAsync(inner.Id.Value));
        Assert.True(Assert.Single((await Flow.HeartbeatAsync(ServerA, [new LeaseRef(running.Id.Value, lease)], Lease, Ct)).Leases).CancelRequested);
        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));   // выполняющийся член ещё не завершён

        Assert.Equal(FailOutcome.Cancelled, await Flow.ConfirmCancelAsync(running.Id.Value, lease, ServerA, "stopped", Ct));
        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(batch.Id.Value));
        Assert.Equal((short?)4, await BatchStateAsync(nested.Id.Value));
        Assert.Equal("launched", await FateAsync(after.Id.Value));
    }

    [Fact]
    public async Task Member_finishing_successfully_after_cancel_request_counts_as_succeeded()   // §2.3
    {
        IJobStep? running = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            running = b.Enqueue(A(1));
            b.Enqueue(A(2));
        });
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(running!.Id.Value);
        await Client.CancelBatchAsync(batch.Id, Ct);
        await ResolveAllAsync();

        Assert.Equal(1, await Flow.AckAsync(ServerA, [new LeaseRef(running.Id.Value, lease)], Ct));
        await ResolveAllAsync();

        var info = (await Client.GetBatchAsync(batch.Id, Ct))!;
        Assert.Equal((BatchState.Cancelled, 1, 0, 1), (info.State, info.Succeeded, info.Failed, info.Cancelled));
        Assert.True(info.CancellationRequested);
        Assert.NotNull(info.FinishedAt);
    }

    [Fact]
    public async Task Get_batch_reports_links_and_conditions()   // §12 API: BatchInfo
    {
        var job = Client.Workflow().Enqueue(A(0));
        IBatchStep? nested = null;
        var ofJob = job.ContinueWithBatch(b => nested = b.StartBatch(n => n.Enqueue(A(1))), ContinuationCondition.OnAnyFinished);
        var ofBatch = ofJob.ContinueWithBatch(b => b.Enqueue(A(2)));
        await job.SubmitAsync(Ct);

        var a = (await Client.GetBatchAsync(ofJob.Id, Ct))!;
        var b2 = (await Client.GetBatchAsync(ofBatch.Id, Ct))!;
        var n = (await Client.GetBatchAsync(nested!.Id, Ct))!;

        Assert.Equal((BatchState.Awaiting, 1, (JobId?)job.Id, (BatchId?)null, (ContinuationCondition?)ContinuationCondition.OnAnyFinished),
            (a.State, a.Total, a.ParentId, a.ParentBatchId, a.Condition));
        Assert.Null(a.OuterBatchId);
        Assert.Equal(((JobId?)null, (BatchId?)ofJob.Id, (ContinuationCondition?)ContinuationCondition.OnSuccess),
            (b2.ParentId, b2.ParentBatchId, b2.Condition));
        Assert.Equal(((BatchId?)ofJob.Id, (JobId?)null, (ContinuationCondition?)null, BatchState.Awaiting),
            (n.OuterBatchId, n.ParentId, n.Condition, n.State));
        Assert.False(a.CancellationRequested);
        Assert.Null(a.FinishedAt);
        Assert.Null(await Client.GetBatchAsync(BatchId.New(), Ct));
    }

    [Fact]
    public async Task Cancelled_parent_cancels_batch_continuation_entirely()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null, x = null, y = null, n1 = null;
        var bc = p.ContinueWithBatch(b =>
        {
            m = b.Enqueue(A(1));
            b.StartBatch(n => n1 = n.Enqueue(A(2)));
            x = b.Enqueue(A(3));
            y = x.ContinueWith(A(4), ContinuationCondition.OnAnyFinished);
        });
        await p.SubmitAsync(Ct);
        await CancelJobAsync(p.Id.Value);

        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(bc.Id.Value));
        foreach (var s in new[] { m!, n1!, x!, y! })
            Assert.Equal("cancelled", await FateAsync(s.Id.Value));
    }

    [Fact]
    public async Task Batch_continuation_with_running_cancel_is_not_activated()   // §12 Batches
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null;
        var bc = p.ContinueWithBatch(b => m = b.Enqueue(A(1)));
        await p.SubmitAsync(Ct);
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(bc.Id, Ct));   // Awaiting — не терминален

        await SucceedAsync(p.Id.Value);
        await ResolveOnceAsync();                                         // batch с cancel_requested Т2 не трогает
        Assert.Equal((short?)1, await BatchStateAsync(bc.Id.Value));
        await ResolveAllAsync();

        Assert.Equal((short?)4, await BatchStateAsync(bc.Id.Value));
        Assert.Equal("cancelled", await FateAsync(m!.Id.Value));
    }

    [Theory]
    [InlineData(ContinuationCondition.OnAnyFinished, false)]
    [InlineData(ContinuationCondition.OnSuccess, false)]
    [InlineData(ContinuationCondition.OnAnyFinished, true)]
    [InlineData(ContinuationCondition.OnSuccess, true)]
    public async Task Branch_cancel_beats_continuation_of_running_member(ContinuationCondition condition, bool samePortion)   // §12 «Отмена ветки»
    {
        IJobStep? x = null, y = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            NextMillisecond();
            b.EnqueueMany(Enumerable.Range(1, samePortion ? 10 : 1200).Select(A).ToArray());
            NextMillisecond();
            y = x.ContinueWith(A(9999), condition);                       // id больше всех членов: при 1200 — за первой порцией
        });
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(x!.Id.Value);
        await Client.CancelBatchAsync(batch.Id, Ct);

        Assert.Equal(1, (await Flow.CancelMembersAsync(1000, 1000, 1, TimeSpan.FromSeconds(30), Ct)).Portions);   // одна порция: X выполняется
        Assert.Equal(samePortion ? "cancelled" : "awaiting", await FateAsync(y!.Id.Value));
        Assert.Equal(1, await Flow.AckAsync(ServerA, [new LeaseRef(x.Id.Value, lease)], Ct));                    // X успел завершиться
        await ResolveWithoutCancelMembersAsync();                         // Y решает Т2 по сигналу X, а не следующая порция CancelMembers

        Assert.Equal("cancelled", await FateAsync(y.Id.Value));
        Assert.Equal($"batch {batch.Id} cancelled", await EventMessageAsync(y.Id.Value));
    }

    [Fact]
    public async Task Nested_batch_continuation_of_member_in_cancelled_batch_is_not_activated()   // §12 «Отмена ветки»
    {
        IJobStep? x = null, z = null;
        IBatchStep? chained = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            chained = x.ContinueWithBatch(c => z = c.Enqueue(A(1)));
        });
        await batch.SubmitAsync(Ct);
        var lease = await LeaseAsync(x!.Id.Value);
        await Client.CancelBatchAsync(batch.Id, Ct);
        Assert.Equal(1, (await Flow.CancelMembersAsync(1, 1, 1, TimeSpan.FromSeconds(30), Ct)).Rows);   // порция из одной строки: X

        Assert.Equal(1, await Flow.AckAsync(ServerA, [new LeaseRef(x.Id.Value, lease)], Ct));
        await ResolveOnceAsync();

        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 1 AND cancel_requested FROM {schema}.batches WHERE id = @id",
            ("id", chained!.Id.Value)));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 5"));
        Assert.Equal("awaiting", await FateAsync(z!.Id.Value));

        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(z.Id.Value));
        Assert.Equal((short?)4, await BatchStateAsync(batch.Id.Value));
    }

    [Fact]
    public async Task Activated_batch_with_cancel_requested_does_not_launch_members()   // §12 «Отмена ветки»
    {
        var p = Client.Workflow().Enqueue(A(0));
        IJobStep? m = null;
        var bc = p.ContinueWithBatch(b => m = b.Enqueue(A(1)));
        await p.SubmitAsync(Ct);
        await SucceedAsync(p.Id.Value);
        await ResolveOnceAsync();                                                 // активация: state 0 и сигнал Activated
        Assert.Equal((short?)0, await BatchStateAsync(bc.Id.Value));
        Assert.Equal("awaiting", await FateAsync(m!.Id.Value));

        await Client.CancelBatchAsync(bc.Id, Ct);
        await ResolveOnceAsync();                                                 // Т2 по Activated: отмена вместо запуска

        Assert.Equal("cancelled", await FateAsync(m.Id.Value));
        Assert.Equal($"batch {bc.Id} cancelled", await EventMessageAsync(m.Id.Value));
    }

    [Fact]
    public async Task Member_row_held_longer_than_lock_timeout_defers_only_its_batch()   // §12 CancelBatchAsync, §6.5
    {
        IJobStep? held = null, other = null, elsewhere = null;
        var first = (await Client.Workflow().StartBatch(b =>
        {
            held = b.Enqueue(A(1));
            other = b.Enqueue(A(2));
        }).SubmitAsync(Ct)).Batches[0];
        var second = (await Client.Workflow().StartBatch(b => elsewhere = b.Enqueue(A(3))).SubmitAsync(Ct)).Batches[0];
        await using var holder = await Db.Admin.OpenConnectionAsync(Ct);
        await using (var hold = new NpgsqlCommand(Db.Sql("BEGIN; SELECT 1 FROM {schema}.queue WHERE job_id = @id FOR UPDATE"), holder))
        {
            hold.Parameters.AddWithValue("id", held!.Id.Value);
            await hold.ExecuteNonQueryAsync(Ct);
        }
        await Client.CancelBatchAsync(first, Ct);
        await Client.CancelBatchAsync(second, Ct);

        var result = await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal(1, result.TimedOut);
        Assert.Equal("cancelled", await FateAsync(elsewhere!.Id.Value));      // отмена другого batch идёт
        Assert.Equal("launched", await FateAsync(other!.Id.Value));           // порция первого откатана целиком
        Assert.True(await Db.ScalarAsync<bool>("SELECT retry_after > now() FROM {schema}.completions WHERE node_id = @b AND kind = 6",
            ("b", first.Value)));

        await holder.CloseAsync();                                             // строка освобождена
        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");
        await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal("cancelled", await FateAsync(held.Id.Value));
        Assert.Equal("cancelled", await FateAsync(other.Id.Value));
    }
}

public sealed class WorkflowCancelTests_Direct(PostgresFixture pg) : WorkflowCancelTests(pg, ConnectionMode.Direct);
public sealed class WorkflowCancelTests_Bouncer(PostgresFixture pg) : WorkflowCancelTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 3: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowCancelTests"`
Expected: FAIL — нет `IJobClient.CancelBatchAsync`, `GetBatchAsync`, `PgBoltStore.CancelMembersAsync`.

- [x] **Step 4: Запись результата**

В `src/PgBolt.Postgres/Storage/WorkflowRecords.cs` добавить:

```csharp
/// <summary>
/// Итог CancelMembers за итерацию (§6.5): Portions — выполненные транзакции-порции (≥ 1 — итерация повторяется сразу);
/// Rows — строк обхода; NewSignals — сигналы Cancelled-членов и CancelMembers вложенных; TimedOut — порции, откатанные по lock_timeout.
/// </summary>
internal sealed record CancelMembersResult(int Portions, int Rows, int NewSignals, int TimedOut)
{
    public static readonly CancelMembersResult Empty = new(0, 0, 0, 0);
}
```

- [x] **Step 5: SQL отмены и чтения batch**

`src/PgBolt.Postgres/Storage/PgBoltStore.Batches.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres.Storage;

/// <summary>
/// Отмена batch (часть 5 §6.5): CancelBatchAsync — один statement; CancelMembers — короткие транзакции с lock_timeout 1 с, сигнал
/// за сигналом: этап 0 — члены-задачи (jobs_batch, семантика cancel core §5.7 набором), этап 1 — вложенные batches. Каждый
/// statement порции перечитывает cancel_requested своего узла: сброшен requeue'ом (§6.6) — порция ничего не меняет, сигнал удаляется.
/// </summary>
public sealed partial class PgBoltStore
{
    private const string CancelBatchTemplate = """
        WITH b AS MATERIALIZED (
            SELECT id, state, cancel_requested FROM {schema}.batches WHERE id = @id FOR NO KEY UPDATE
        ),
        u AS (
            UPDATE {schema}.batches t SET cancel_requested = true FROM b
            WHERE t.id = b.id AND b.state IN (0, 1) AND NOT b.cancel_requested
            RETURNING t.id
        ),
        s AS (INSERT INTO {schema}.completions (node_id, kind) SELECT id, 6 FROM u RETURNING 1)
        SELECT b.state, EXISTS (SELECT 1 FROM s) AS signalled FROM b
        """;

    private const string GetBatchTemplate = """
        SELECT id, parent_batch_id, parent_id, condition, state, total,
               greatest(0, succeeded), greatest(0, failed), greatest(0, cancelled), cancel_requested, created_at, finished_at
        FROM {schema}.batches WHERE id = @id
        """;

    private const string CancelCaptureTemplate = """
        SELECT id, node_id, stage, cursor FROM {schema}.completions
        WHERE kind = 6 AND (retry_after IS NULL OR retry_after <= now())
        ORDER BY id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        """;

    /// <summary>§6.5 stage = 0: члены по jobs_batch без блокировки jobs; их строки queue — ORDER BY job_id FOR UPDATE (без SKIP LOCKED).</summary>
    private const string CancelJobsTemplate = """
        WITH b AS (
            SELECT cancel_requested FROM {schema}.batches WHERE id = @node
        ),
        m AS MATERIALIZED (
            SELECT id FROM {schema}.jobs
            WHERE batch_id = @node AND batch_id IS NOT NULL AND id > @cursor AND (SELECT cancel_requested FROM b)
            ORDER BY id
            LIMIT @lim
        ),
        l AS MATERIALIZED (
            SELECT q.job_id, q.lease_owner FROM {schema}.queue q
            WHERE q.job_id IN (SELECT id FROM m)
            ORDER BY q.job_id
            FOR UPDATE
        ),
        mark AS (
            UPDATE {schema}.queue q SET cancel_requested = true
            FROM l WHERE q.job_id = l.job_id AND l.lease_owner IS NOT NULL
        ),
        x AS (
            DELETE FROM {schema}.queue q USING l
            WHERE q.job_id = l.job_id AND l.lease_owner IS NULL
            RETURNING q.job_id, q.attempt, q.notify
        ),
        xj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = x.attempt, finished_at = now(), notify = j.notify OR x.notify
            FROM x WHERE j.id = x.job_id
            RETURNING j.id, j.batch_id, x.notify
        ),
        xe AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
            SELECT job_id, now(), @kind_cancelled, attempt, 'batch ' || @node || ' cancelled' FROM x
        ),
        xs AS (
            INSERT INTO {schema}.completions (node_id, kind, batch_id)
            SELECT id, 4, batch_id FROM xj WHERE notify
            RETURNING 1
        )
        SELECT COALESCE((SELECT cancel_requested FROM b), false) AS requested,
               (SELECT count(*) FROM m)::int AS taken,
               (SELECT id FROM m ORDER BY id DESC LIMIT 1) AS last_id,
               (SELECT count(*) FROM xs)::int AS signals
        """;

    /// <summary>§6.5 stage = 1: вложенные batches (batches_nested) в порядке id под FOR NO KEY UPDATE; Active/Awaiting без флага — флаг и свой CancelMembers.</summary>
    private const string CancelNestedTemplate = """
        WITH b AS (
            SELECT cancel_requested FROM {schema}.batches WHERE id = @node
        ),
        c AS MATERIALIZED (
            SELECT id FROM {schema}.batches
            WHERE parent_batch_id = @node AND parent_batch_id IS NOT NULL AND id > @cursor AND (SELECT cancel_requested FROM b)
            ORDER BY id
            LIMIT @lim
        ),
        lk AS MATERIALIZED (
            SELECT t.id, t.state, t.cancel_requested FROM {schema}.batches t
            WHERE t.id IN (SELECT id FROM c)
            ORDER BY t.id
            FOR NO KEY UPDATE
        ),
        u AS (
            UPDATE {schema}.batches t SET cancel_requested = true
            FROM lk WHERE t.id = lk.id AND lk.state IN (0, 1) AND NOT lk.cancel_requested
            RETURNING t.id
        ),
        s AS (
            INSERT INTO {schema}.completions (node_id, kind) SELECT id, 6 FROM u RETURNING 1
        )
        SELECT COALESCE((SELECT cancel_requested FROM b), false) AS requested,
               (SELECT count(*) FROM c)::int AS taken,
               (SELECT id FROM c ORDER BY id DESC LIMIT 1) AS last_id,
               (SELECT count(*) FROM s)::int AS signals
        """;

    private const string CancelAdvanceTemplate =
        "UPDATE {schema}.completions SET stage = @stage, cursor = @cursor, retry_after = NULL WHERE id = @id";

    private const string CancelDoneTemplate = "DELETE FROM {schema}.completions WHERE id = @id";

    private const string CancelDeferTemplate = "UPDATE {schema}.completions SET retry_after = now() + @delay WHERE id = @id";

    private string CancelBatchSql => field ??= Render(CancelBatchTemplate);
    private string GetBatchSql => field ??= Render(GetBatchTemplate);
    private string CancelCaptureSql => field ??= Render(CancelCaptureTemplate);
    private string CancelJobsSql => field ??= Render(CancelJobsTemplate);
    private string CancelNestedSql => field ??= Render(CancelNestedTemplate);
    private string CancelAdvanceSql => field ??= Render(CancelAdvanceTemplate);
    private string CancelDoneSql => field ??= Render(CancelDoneTemplate);
    private string CancelDeferSql => field ??= Render(CancelDeferTemplate);

    /// <summary>§6.5: нет строки — NotFound; терминальный — AlreadyFinished; иначе CancellationRequested (повтор — без нового сигнала).</summary>
    internal Task<BatchCancelResult> CancelBatchAsync(Guid batchId, CancellationToken ct) =>
        QueryAndSignalAsync<BatchCancelResult>(CancelBatchSql, ps => P.Add(ps, "id", batchId), async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return (BatchCancelResult.NotFound, 0L);
            var state = r.GetInt16(0);
            var result = state is 2 or 3 or 4 ? BatchCancelResult.AlreadyFinished : BatchCancelResult.CancellationRequested;
            return (result, r.GetBoolean(1) ? 1L : 0L);   // сигнал — после завершения statement'а (коммит на Sync)
        }, ct);

    /// <summary>§10: счётчики ограничены снизу нулём — между requeue члена и учётом его прежнего сигнала они бывают ниже факта (§6.6).</summary>
    internal Task<BatchInfo?> GetBatchAsync(Guid batchId, CancellationToken ct) =>
        QueryAsync<BatchInfo?>(GetBatchSql, ps => P.Add(ps, "id", batchId), async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return null;
            var parent = r.GetNullableValue<Guid>(2);
            var condition = r.GetNullableValue<short>(3);
            return new BatchInfo(new BatchId(r.GetGuid(0)), (BatchState)r.GetInt16(4), r.GetInt32(5), r.GetInt32(6), r.GetInt32(7),
                r.GetInt32(8), r.GetNullableValue<Guid>(1) is { } outer ? new BatchId(outer) : null,
                WorkflowCodes.ParentJob(condition, parent), WorkflowCodes.ParentBatch(condition, parent), WorkflowCodes.Condition(condition),
                r.GetBoolean(9), r.GetFieldValue<DateTimeOffset>(10), r.GetNullableValue<DateTimeOffset>(11));
        }, ct);

    private enum PortionKind { Empty, Done, LockTimeout }

    private readonly record struct CancelPortion(PortionKind Kind, long SignalId, int Rows, int Signals);

    private readonly record struct CancelStage(bool Requested, int Taken, Guid? Last, int Signals);

    /// <summary>
    /// §6.5: транзакции подряд, сигнал за сигналом, пока есть готовые сигналы, строк обхода меньше rowBudget и транзакций меньше
    /// maxTransactions. lock_timeout — откат порции и retry_after этого сигнала отдельным statement'ом, итерация продолжается.
    /// </summary>
    internal async Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay,
        CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(portion);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowBudget);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxTransactions);
        int portions = 0, rows = 0, signals = 0, timedOut = 0;
        for (var transactions = 0; transactions < maxTransactions && rows < rowBudget; transactions++)
        {
            var lim = Math.Min(portion, rowBudget - rows);
            var step = await RunAsync(c => CancelMembersOnceAsync(lim, c), ct);
            if (step.Kind == PortionKind.Empty)
                break;
            if (step.Kind == PortionKind.LockTimeout)
            {
                timedOut++;
                await NonQueryAsync(CancelDeferSql, ps =>
                {
                    P.Add(ps, "id", step.SignalId);
                    P.Add(ps, "delay", retryDelay);
                }, ct);
                continue;
            }
            portions++;
            rows += step.Rows;
            signals += step.Signals;
        }
        return new CancelMembersResult(portions, rows, signals, timedOut);
    }

    private async Task<CancelPortion> CancelMembersOnceAsync(int lim, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var timeout = new NpgsqlCommand("SET LOCAL lock_timeout = '1s'", conn, tx))
            await timeout.ExecuteNonQueryAsync(ct);

        (long Id, Guid Node, short Stage, Guid? Cursor)? captured = null;
        await using (var capture = new NpgsqlCommand(CancelCaptureSql, conn, tx))
        {
            await using var r = await capture.ExecuteReaderAsync(ct);
            if (await r.ReadAsync(ct))
                captured = (r.GetInt64(0), r.GetGuid(1), r.GetInt16(2), r.GetNullableValue<Guid>(3));
        }
        if (captured is not { } signal)
        {
            await tx.CommitAsync(ct);
            return new CancelPortion(PortionKind.Empty, 0, 0, 0);
        }

        try
        {
            var (stage, cursor) = (signal.Stage, signal.Cursor);
            var rows = 0;
            var signals = 0;
            var done = false;
            if (stage == 0)
            {
                var jobs = await CancelStageAsync(conn, tx, CancelJobsSql, signal.Node, cursor, lim, jobsStage: true, ct);
                rows += jobs.Taken;
                signals += jobs.Signals;
                if (!jobs.Requested)
                    done = true;                       // флаг сброшен requeue'ом или строки нет: устаревший сигнал
                else if (jobs.Taken == lim)
                    cursor = jobs.Last;                // порция исчерпана
                else
                    (stage, cursor) = ((short)1, null);
            }
            if (!done && stage == 1 && rows < lim)
            {
                var left = lim - rows;
                var nested = await CancelStageAsync(conn, tx, CancelNestedSql, signal.Node, cursor, left, jobsStage: false, ct);
                rows += nested.Taken;
                signals += nested.Signals;
                if (!nested.Requested || nested.Taken < left)
                    done = true;
                else
                    cursor = nested.Last;
            }

            await using (var finish = new NpgsqlCommand(done ? CancelDoneSql : CancelAdvanceSql, conn, tx))
            {
                P.Add(finish.Parameters, "id", signal.Id);
                if (!done)
                {
                    P.Add(finish.Parameters, "stage", stage);
                    P.AddValue(finish.Parameters, "cursor", cursor, NpgsqlTypes.NpgsqlDbType.Uuid);
                }
                await finish.ExecuteNonQueryAsync(ct);
            }
            if (ResolverHook is { } hook)
                await hook(ResolverStep.CancelPortion, ct);
            await tx.CommitAsync(ct);
            return new CancelPortion(PortionKind.Done, signal.Id, rows, signals);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            return new CancelPortion(PortionKind.LockTimeout, signal.Id, 0, 0);   // dispose транзакции — откат порции
        }
    }

    private static async Task<CancelStage> CancelStageAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, Guid node, Guid? cursor,
        int lim, bool jobsStage, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        P.Add(cmd.Parameters, "node", node);
        P.Add(cmd.Parameters, "cursor", cursor ?? Guid.Empty);
        P.Add(cmd.Parameters, "lim", lim);
        if (jobsStage)
            P.Add(cmd.Parameters, "kind_cancelled", (short)JobEventKind.Cancelled);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        return new CancelStage(r.GetBoolean(0), r.GetInt32(1), r.GetNullableValue<Guid>(2), r.GetInt32(3));
    }
}
```

- [x] **Step 6: Клиент**

`src/PgBolt.Abstractions/IJobClient.cs` — добавить:

```csharp
    /// <summary>
    /// Отмена batch (часть 5 §6.5): невзятые члены отменяются сразу, выполняющимся отмена доставляется heartbeat'ом, вложенные
    /// batches — рекурсивно; член, успевший завершиться успехом, учитывается как Succeeded. Ждущие члены ветки не запускаются.
    /// Повторная отмена — CancellationRequested без нового сигнала. Требует resolver (сервер с RunMaintenance = true).
    /// </summary>
    ValueTask<BatchCancelResult> CancelBatchAsync(BatchId id, CancellationToken ct = default);

    /// <summary>Строка batch; счётчики ≥ 0 и временно могут быть ниже факта после requeue члена (§6.6).</summary>
    ValueTask<BatchInfo?> GetBatchAsync(BatchId id, CancellationToken ct = default);
```

`src/PgBolt.Postgres/Client/PgBoltJobClient.cs` — добавить:

```csharp
    public async ValueTask<BatchCancelResult> CancelBatchAsync(BatchId id, CancellationToken ct = default) =>
        await store.CancelBatchAsync(id.Value, ct);

    public async ValueTask<BatchInfo?> GetBatchAsync(BatchId id, CancellationToken ct = default) =>
        await store.GetBatchAsync(id.Value, ct);
```

- [x] **Step 7: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowCancelTests|FullyQualifiedName~WorkflowResolverTests"`
Expected: PASS (Direct и Bouncer).

- [x] **Step 8: Commit**

```bash
git add src/PgBolt.Abstractions/IJobClient.cs src/PgBolt.Postgres tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): batch cancellation and member cancel walk"
```

---
### Task 8: Requeue: `notify`, `batch_id` и синхронный учёт в `batches` (§6.6)

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.Requeue.cs`, `src/PgBolt.Abstractions/IJobClient.cs` (XML-doc `RequeueAsync`)
- Modify (тесты): `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` (`SucceedManyAsync`)
- Test: `tests/PgBolt.Postgres.Tests/WorkflowRequeueTests.cs`

**Interfaces:**
- Consumes: resolver (Task 6), отмена batch (Task 7).
- Produces: `RequeueAsync` (сигнатура и результат прежние): вставка в `queue` пишет `notify = jobs.notify OR jobs.batch_id IS NOT NULL`, `batch_id = jobs.batch_id`; у члена batch в той же транзакции — блокировка цепочки `batches` члена и внешних `ORDER BY id FOR NO KEY UPDATE` и подъём снизу вверх (§6.6 п. 2); `WorkflowTestBase.SucceedManyAsync(Guid[])`.

- [x] **Step 1: Тестовая поддержка**

В `WorkflowTestBase` (`tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs`) добавить:

```csharp
    /// <summary>Ack множества готовых задач одним statement'ом (lease в обход очереди).</summary>
    protected async Task SucceedManyAsync(Guid[] ids)
    {
        var leases = new List<LeaseRef>();
        await using (var cmd = Db.Admin.CreateCommand(Db.Sql("""
            UPDATE {schema}.queue
            SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '5 minutes', leased_at = now(),
                attempt = attempt + 1
            WHERE job_id = ANY(@ids) AND lease_owner IS NULL AND ready AND await_condition IS NULL
            RETURNING job_id, lease_id
            """)))
        {
            cmd.Parameters.AddWithValue("ids", ids);
            cmd.Parameters.AddWithValue("s", ServerA);
            await using var r = await cmd.ExecuteReaderAsync(Ct);
            while (await r.ReadAsync(Ct))
                leases.Add(new LeaseRef(r.GetGuid(0), r.GetGuid(1)));
        }
        Assert.Equal(ids.Length, await Flow.AckAsync(ServerA, leases, Ct));
    }
```

- [x] **Step 2: Тесты**

`tests/PgBolt.Postgres.Tests/WorkflowRequeueTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §6.6: requeue родителей и членов, синхронный учёт, requeue при идущей отмене, устаревший CancelMembers.</summary>
public abstract class WorkflowRequeueTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    private Task<bool> BatchIsAsync(Guid id, string condition) =>
        Db.ScalarAsync<bool>($"SELECT {condition} FROM {{schema}}.batches WHERE id = @id", ("id", id));

    private Task<long> CancelledEventsAsync(Guid id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.job_events WHERE job_id = @id AND kind = 4", ("id", id));

    [Fact]
    public async Task Requeue_writes_notify_and_batch_id_into_queue()
    {
        IJobStep? member = null;
        var batch = (await Client.Workflow().StartBatch(b => member = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        var lone = (await Client.Workflow().Enqueue(A(2)).SubmitAsync(Ct)).Jobs[0].Value;
        await FailFinallyAsync(member!.Id.Value);
        await FailFinallyAsync(lone);
        await Db.ExecAsync("UPDATE {schema}.jobs SET notify = true WHERE id = @id", ("id", lone));   // подписка §6.2 п. 2 после финализации

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(member.Id, Ct));
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(new JobId(lone), Ct));

        Assert.True(await Db.ScalarAsync<bool>("SELECT notify AND batch_id = @b FROM {schema}.queue WHERE job_id = @id",
            ("id", member.Id.Value), ("b", batch.Value)));
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify AND batch_id IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", lone)));
    }

    [Fact]
    public async Task Requeued_failed_parent_runs_its_waiting_OnSuccess_child_after_success()   // §12 SQL-контракт
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        var (parent, child) = (r.Jobs[0].Value, r.Jobs[1].Value);
        await FailFinallyAsync(parent);
        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(r.Jobs[0], Ct));
        await SucceedAsync(parent);
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Requeue_of_failed_member_returns_batch_and_outer_to_active()   // §12 Batches
    {
        IJobStep? x = null;
        IBatchStep? inner = null;
        var outer = Client.Workflow().StartBatch(o => inner = o.StartBatch(b => x = b.Enqueue(A(1))));
        var next = outer.ContinueWith(A(2));
        await outer.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);
        await ResolveAllAsync();
        Assert.Equal((short?)3, await BatchStateAsync(outer.Id.Value));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));

        foreach (var b in new[] { inner!.Id.Value, outer.Id.Value })
            Assert.True(await BatchIsAsync(b, "state = 0 AND failed = 0 AND finished_at IS NULL AND NOT cancel_requested"));
        Assert.Equal("awaiting", await FateAsync(next.Id.Value));
        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();

        Assert.Equal((short?)2, await BatchStateAsync(inner.Id.Value));
        Assert.Equal((short?)2, await BatchStateAsync(outer.Id.Value));
        Assert.Equal("launched", await FateAsync(next.Id.Value));
    }

    [Fact]
    public async Task Requeue_of_member_of_cancelled_batch_allows_cancelling_again()   // §12 Batches
    {
        IReadOnlyList<IJobStep> members = [];
        var batch = (await Client.Workflow().StartBatch(b => members = b.EnqueueMany(new[] { A(1), A(2) })).SubmitAsync(Ct)).Batches[0];
        await Client.CancelBatchAsync(batch, Ct);
        await ResolveAllAsync();
        Assert.Equal((short?)4, await BatchStateAsync(batch.Value));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(members[0].Id, Ct));
        Assert.True(await BatchIsAsync(batch.Value, "state = 0 AND cancelled = 1 AND NOT cancel_requested"));

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 6"));
        await ResolveAllAsync();

        Assert.Equal("cancelled", await FateAsync(members[0].Id.Value));
        Assert.Equal((short?)4, await BatchStateAsync(batch.Value));
    }

    [Fact]
    public async Task Requeue_during_cancel_behind_and_ahead_of_cursor()   // §12 Batches, Review Focus 4
    {
        IJobStep? x = null, y = null;
        IReadOnlyList<IJobStep> others = [];
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            NextMillisecond();
            others = b.EnqueueMany(Enumerable.Range(1, 2998).Select(A).ToArray());
            NextMillisecond();
            y = x.ContinueWith(A(9999), ContinuationCondition.OnAnyFinished);   // ждущее продолжение X, последнее по id
        });
        await batch.SubmitAsync(Ct);
        var ahead = others.Select(s => s.Id.Value).Order().ElementAt(1500);   // за первой порцией (X + 999)
        await Client.CancelBatchAsync(batch.Id, Ct);
        Assert.Equal(1000, (await Flow.CancelMembersAsync(1000, 1000, 1, TimeSpan.FromSeconds(30), Ct)).Rows);
        Assert.Equal("cancelled", await FateAsync(x!.Id.Value));
        await CancelJobAsync(ahead);

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(new JobId(ahead), Ct));

        Assert.True(await BatchIsAsync(batch.Id.Value, "state = 0 AND cancel_requested"));   // флаг не сброшен
        await SucceedAsync(x.Id.Value);                                                          // позади курсора — выполняется
        await ResolveWithoutCancelMembersAsync();
        Assert.Equal("cancelled", await FateAsync(y!.Id.Value));                                // продолжение ветки отменено, не запущено
        Assert.Equal($"batch {batch.Id} cancelled", await EventMessageAsync(y.Id.Value));

        await ResolveAllAsync();

        Assert.Equal("succeeded", await FateAsync(x.Id.Value));
        Assert.Equal("cancelled", await FateAsync(ahead));                                      // впереди курсора — отменён снова
        Assert.Equal(2L, await CancelledEventsAsync(ahead));
        Assert.True(await BatchIsAsync(batch.Id.Value, "state = 4 AND succeeded = 1 AND cancelled = 2999 AND failed = 0"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Nested_batch_requeued_during_outer_cancel(bool beforeStageOne)   // §12 Batches, §6.6
    {
        IJobStep? x = null, nm = null;
        IBatchStep? nested = null;
        var outer = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(0));
            nested = b.StartBatch(n => nm = n.Enqueue(A(1)));
        });
        await outer.SubmitAsync(Ct);
        var lease = await LeaseAsync(x!.Id.Value);                       // X выполняется: внешний не станет терминальным
        await CancelJobAsync(nm!.Id.Value);
        await ResolveWithoutCancelMembersAsync();
        Assert.Equal((short?)4, await BatchStateAsync(nested!.Id.Value));
        await Client.CancelBatchAsync(outer.Id, Ct);
        if (beforeStageOne)
            Assert.Equal(1, (await Flow.CancelMembersAsync(1, 1, 1, TimeSpan.FromSeconds(30), Ct)).Rows);   // этап 0: только X
        else
            await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);                   // этап 1 прошёл терминальный вложенный

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(nm.Id, Ct));
        Assert.True(await BatchIsAsync(nested.Id.Value, "state = 0 AND NOT cancel_requested AND cancelled = 0"));
        Assert.True(await BatchIsAsync(outer.Id.Value, "state = 0 AND cancel_requested"));

        await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);
        if (beforeStageOne)
        {
            Assert.Equal("cancelled", await FateAsync(nm.Id.Value));              // этап 1 внешнего снова пометил вложенный
            Assert.Equal(2L, await CancelledEventsAsync(nm.Id.Value));
        }
        else
        {
            Assert.True(await BatchIsAsync(nested.Id.Value, "state = 0 AND NOT cancel_requested"));   // остаётся Active
            await SucceedAsync(nm.Id.Value);                                                          // и выполняется
            await ResolveWithoutCancelMembersAsync();
            Assert.Equal((short?)2, await BatchStateAsync(nested.Id.Value));
        }
        Assert.Equal(FailOutcome.Cancelled, await Flow.ConfirmCancelAsync(x.Id.Value, lease, ServerA, "stop", Ct));
        await ResolveAllAsync();
        Assert.Equal((short?)4, await BatchStateAsync(outer.Id.Value));
    }

    [Fact]
    public async Task Stale_cancel_members_after_batch_finished_is_dropped()   // §12 «Устаревший CancelMembers»
    {
        IReadOnlyList<IJobStep> first = [], last = [];
        IJobStep? nm = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            first = b.EnqueueMany(Enumerable.Range(0, 1000).Select(A).ToArray());
            NextMillisecond();
            b.StartBatch(n => nm = n.Enqueue(A(1000)));
            NextMillisecond();
            last = b.EnqueueMany(Enumerable.Range(1001, 1999).Select(A).ToArray());
        });
        await batch.SubmitAsync(Ct);
        await SucceedManyAsync([.. last.Select(s => s.Id.Value)]);
        await Client.CancelBatchAsync(batch.Id, Ct);
        Assert.Equal(1000, (await Flow.CancelMembersAsync(1000, 1000, 1, TimeSpan.FromSeconds(30), Ct)).Rows);   // курсор — после first
        await CancelJobAsync(nm!.Id.Value);
        await ResolveWithoutCancelMembersAsync();
        Assert.Equal((short?)4, await BatchStateAsync(batch.Id.Value));                    // Cancelled до конца обхода

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(first[0].Id, Ct));   // за курсором
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(nm.Id, Ct));         // член вложенного
        Assert.True(await BatchIsAsync(batch.Id.Value, "state = 0 AND NOT cancel_requested"));

        var next = await Flow.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal((1, 0), (next.Portions, next.Rows));                                    // порция удалила сигнал, ничего не изменив
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions WHERE kind = 6"));
        Assert.Equal("launched", await FateAsync(first[0].Id.Value));
        Assert.Equal("launched", await FateAsync(nm.Id.Value));
    }

    [Fact]
    public async Task Batch_counters_read_as_non_negative_between_requeue_and_count()   // §12 Batches
    {
        IJobStep? x = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            x = b.Enqueue(A(1));
            b.Enqueue(A(2));
        });
        await batch.SubmitAsync(Ct);
        await FailFinallyAsync(x!.Id.Value);                                       // сигнал Failed ещё не учтён
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));

        Assert.Equal(-1, await Db.ScalarAsync<int>("SELECT failed FROM {schema}.batches WHERE id = @id", ("id", batch.Id.Value)));
        var info = (await Client.GetBatchAsync(batch.Id, Ct))!;
        Assert.Equal((0, 0, 0, BatchState.Active), (info.Succeeded, info.Failed, info.Cancelled, info.State));

        await ResolveAllAsync();
        Assert.Equal(0, (await Client.GetBatchAsync(batch.Id, Ct))!.Failed);
    }

    [Fact]
    public async Task Requeue_of_member_of_deleted_batch_succeeds()   // Review Focus 5, §6.6
    {
        IJobStep? x = null;
        var batch = (await Client.Workflow().StartBatch(b => x = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        await FailFinallyAsync(x!.Id.Value);
        await ResolveAllAsync();
        await Db.ExecAsync("DELETE FROM {schema}.batches WHERE id = @id", ("id", batch.Value));   // удалён retention'ом (§7.1)

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        await SucceedAsync(x.Id.Value);
        var counted = await Flow.CountCompletionsAsync(500, Ct);

        Assert.Equal([batch.Value], counted.MissingBatches);
        Assert.Equal(0L, await SignalsAsync());
    }

    [Fact]
    public async Task Requeued_cancelled_continuation_becomes_pending()   // §6.6
    {
        var r = await Client.Workflow().Enqueue(A(1)).ContinueWith(A(2)).SubmitAsync(Ct);
        await CancelJobAsync(r.Jobs[0].Value);
        await ResolveAllAsync();
        Assert.Equal("cancelled", await FateAsync(r.Jobs[1].Value));

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(r.Jobs[1], Ct));

        Assert.Equal("launched", await FateAsync(r.Jobs[1].Value));
    }
}

public sealed class WorkflowRequeueTests_Direct(PostgresFixture pg) : WorkflowRequeueTests(pg, ConnectionMode.Direct);
public sealed class WorkflowRequeueTests_Bouncer(PostgresFixture pg) : WorkflowRequeueTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 3: Запустить — падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowRequeueTests"`
Expected: FAIL — requeue не пишет `notify`/`batch_id` и не уменьшает счётчики `batches` (`Requeue_writes_notify_and_batch_id_into_queue`, `Requeue_of_failed_member_returns_batch_and_outer_to_active` и др.).

- [x] **Step 4: Requeue (§6.6)**

`src/PgBolt.Postgres/Storage/PgBoltStore.Requeue.cs` — заменить шаблон и summary метода (метод не меняется):

```csharp
    private const string RequeueTemplate = """
        WITH RECURSIVE s AS MATERIALIZED (
            SELECT id, state, queue, priority, max_attempts, unique_key, notify, batch_id
            FROM {schema}.jobs WHERE id = @id
            FOR UPDATE
        ),
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key, notify, batch_id)
            SELECT id, queue, priority, now(), true, max_attempts, unique_key, notify OR batch_id IS NOT NULL, batch_id
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
        ),
        chain(id, depth) AS (
            SELECT s.batch_id, 0 FROM s WHERE s.batch_id IS NOT NULL AND EXISTS (SELECT 1 FROM q)
            UNION ALL
            SELECT b.parent_batch_id, chain.depth + 1
            FROM chain JOIN {schema}.batches b ON b.id = chain.id
            WHERE b.parent_batch_id IS NOT NULL
        ),
        lk AS MATERIALIZED (
            SELECT b.id, b.state, chain.depth
            FROM {schema}.batches b JOIN chain ON chain.id = b.id
            ORDER BY b.id
            FOR NO KEY UPDATE OF b
        ),
        lv AS (
            SELECT id, depth,
                   lag(state) OVER w AS below_state,
                   bool_and(state IN (2, 3, 4)) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS below_terminal,
                   row_number() OVER w - 1 = depth AS contiguous
            FROM lk
            WINDOW w AS (ORDER BY depth)
        ),
        u AS (
            UPDATE {schema}.batches b
            SET failed = b.failed - (k.k = 3)::int, cancelled = b.cancelled - (k.k = 4)::int,
                state = CASE WHEN b.state IN (2, 3, 4) THEN 0 ELSE b.state END,
                cancel_requested = b.cancel_requested AND b.state NOT IN (2, 3, 4),
                finished_at = NULL
            FROM (SELECT lv.id, CASE WHEN lv.depth = 0 THEN (SELECT state FROM s) ELSE lv.below_state END AS k
                  FROM lv
                  WHERE lv.contiguous AND (lv.depth = 0 OR lv.below_terminal)) k
            WHERE b.id = k.id
        )
        SELECT (SELECT state FROM s) AS state, EXISTS (SELECT 1 FROM q) AS requeued, (SELECT queue FROM s) AS queue
        """;
```

```csharp
    /// <summary>
    /// Failed/Cancelled → Pending (core §5.9). Часть 5 §6.6: queue.notify = jobs.notify OR член batch, queue.batch_id = jobs.batch_id;
    /// у члена batch в той же транзакции счётчики batch и терминальных внешних уменьшаются синхронно (цепочка блокируется
    /// ORDER BY id FOR NO KEY UPDATE, подъём снизу вверх до первого нетерминального или отсутствующего). 40P01/40001 — повтор целиком.
    /// </summary>
```

В `src/PgBolt.Abstractions/IJobClient.cs` над `RequeueAsync` добавить:

```csharp
    /// <summary>
    /// Failed/Cancelled → Pending (core §5.9). Член batch (часть 5 §6.6): batch и терминальные внешние синхронно возвращаются в
    /// Active, исход вычисляется заново при следующем завершении; уже запущенные или отменённые продолжения не пересматриваются.
    /// Requeue Cancelled-члена при идущей отмене batch: флаг отмены не сбрасывается, член выполняется или отменяется снова в
    /// зависимости от позиции обхода отмены, ждущие члены его ветки не запускаются, а отменяются; детерминированно — requeue после
    /// того, как batch стал терминальным. Requeue отменённого продолжения ставит его сразу в Pending.
    /// </summary>
```

- [x] **Step 5: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowRequeueTests|FullyQualifiedName~TriggerRequeueTests|FullyQualifiedName~ClientManagementTests"`
Expected: PASS (Direct и Bouncer).

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Storage/PgBoltStore.Requeue.cs src/PgBolt.Abstractions/IJobClient.cs tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): requeue with synchronous batch counters"
```

Конец фазы 2: `dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный.

---
### Task 9: Детерминированные гонки и падения (§12 «Гонки», «Падения»)

**Files:**
- Modify (тесты): `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` (`OpenTransaction`, `BeginAsync`, `HoldRowsAsync`, `WaitForLockWaitersAsync`)
- Test: `tests/PgBolt.Postgres.Tests/WorkflowRaceTests.cs`
- Modify (только если тест нашёл ошибку): файлы продукта задач 3–8 (`src/PgBolt.Postgres`, `src/PgBolt.Abstractions`, `src/PgBolt.Server/Runtime/JobRunner.cs`) — входят в коммит Step 4

**Interfaces:**
- Consumes: всё из Task 3–8; `ResolverHook`, `ResolverStep.Walked` (Task 6).
- Produces: тестовые помощники `WorkflowTestBase.BeginAsync() : Task<OpenTransaction>`, `HoldRowsAsync(string sql, params (string, object)[]) : Task<NpgsqlConnection>`, `WaitForLockWaitersAsync(int count = 1)`; `record OpenTransaction(NpgsqlConnection, NpgsqlTransaction) : IAsyncDisposable` с `CommitAsync()`.

Задача только с тестами: продукт уже реализован в Task 3–8. Гонка «п. 2 подписки дождался requeue, затем CancelAsync / CancelMembers / ack той же задачи против п. 1» детерминированно воспроизводится для CancelAsync; для всех трёх цикл исключает одно и то же свойство — после отката к `pgbolt_after` строка `jobs` не заблокирована, — его тест проверяет `FOR UPDATE NOWAIT`. Падающий тест здесь — найденная ошибка: исправлять код продукта задачи-владельца в этом же коммите задачи, с описанием в `DEVIATIONS` (тест не менять). Порядок событий задаётся держателями блокировок и ожиданием сессий в `pg_stat_activity` (очередь ожидающих блокировку строки в PostgreSQL — FIFO через блокировку кортежа), а не задержками.

- [x] **Step 1: Помощники**

В `tests/PgBolt.Postgres.Tests/WorkflowTestSupport.cs` добавить `using System.Diagnostics; using Npgsql;`, на уровне namespace:

```csharp
/// <summary>Открытая пользовательская транзакция (соединение режима теста: напрямую или через PgBouncer).</summary>
public sealed record OpenTransaction(NpgsqlConnection Connection, NpgsqlTransaction Transaction) : IAsyncDisposable
{
    public Task CommitAsync() => Transaction.CommitAsync();

    public async ValueTask DisposeAsync()
    {
        await Transaction.DisposeAsync();
        await Connection.DisposeAsync();
    }
}
```

и в `WorkflowTestBase`:

```csharp
    protected async Task<OpenTransaction> BeginAsync()
    {
        var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        return new OpenTransaction(conn, await conn.BeginTransactionAsync(Ct));
    }

    /// <summary>Держит блокировки sql в открытой транзакции прямого соединения; Dispose / Close откатывает её.</summary>
    protected async Task<NpgsqlConnection> HoldRowsAsync(string sql, params (string Name, object Value)[] ps)
    {
        var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var cmd = new NpgsqlCommand(Db.Sql("BEGIN; " + sql), conn);
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(Ct);
        return conn;
    }

    /// <summary>Ждёт, пока не меньше count сессий схемы теста встанут в ожидание блокировки (pg_stat_activity).</summary>
    protected async Task WaitForLockWaitersAsync(int count = 1)
    {
        var started = Stopwatch.GetTimestamp();
        while (await Db.ScalarAsync<long>("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p",
                   ("p", $"%{Db.Schema}.%")) < count)
        {
            if (Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException($"Expected {count} sessions waiting for a lock in {Db.Schema}.");
            await Task.Delay(20, Ct);
        }
    }
```

- [x] **Step 2: Тесты**

`tests/PgBolt.Postgres.Tests/WorkflowRaceTests.cs`:

```csharp
using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §12 «Гонки» и «Падения»: два-три соединения, ручные транзакции, держатели блокировок и хуки.</summary>
public abstract class WorkflowRaceTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    public enum Finalizer { Ack, Fail, Cancel }

    private static readonly TimeSpan NoWait = TimeSpan.FromSeconds(10);

    private Task<WorkflowResult> SubscribeAsync(Guid parent, NpgsqlTransaction? tx = null,
        ContinuationCondition condition = ContinuationCondition.OnAnyFinished) =>
        Client.Workflow(new WorkflowOptions { Transaction = tx }).After(new JobId(parent)).ContinueWith(A(1), condition).SubmitAsync(Ct).AsTask();

    private async Task<Func<Task>> FinalizerAsync(Finalizer kind, Guid parent)
    {
        switch (kind)
        {
            case Finalizer.Ack:
            {
                var lease = await LeaseAsync(parent);
                return async () => Assert.Equal(1, await Flow.AckAsync(ServerA, [new LeaseRef(parent, lease)], Ct));
            }
            case Finalizer.Fail:
            {
                var lease = await LeaseAsync(parent);
                return async () => Assert.Equal(FailOutcome.Failed,
                    await Flow.FailAsync(parent, lease, ServerA, JobEventKind.Failed, "x", null, Ct));
            }
            default:
                return async () => Assert.Equal(CancelResult.Cancelled, await Flow.CancelAsync(parent, "x", Ct));
        }
    }

    [Theory]
    [InlineData(Finalizer.Ack, true)]
    [InlineData(Finalizer.Ack, false)]
    [InlineData(Finalizer.Fail, true)]
    [InlineData(Finalizer.Fail, false)]
    [InlineData(Finalizer.Cancel, true)]
    [InlineData(Finalizer.Cancel, false)]
    public async Task Subscription_versus_finalization(Finalizer kind, bool subscribeFirst)   // §12 Гонки, §6.2 «Почему гонки нет»
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        var finalize = await FinalizerAsync(kind, parent);
        Guid child;
        if (subscribeFirst)
        {
            await using var user = await BeginAsync();
            child = (await SubscribeAsync(parent, user.Transaction)).Jobs[0].Value;   // п. 1 держит строку queue родителя
            var final = finalize();
            await WaitForLockWaitersAsync();
            Assert.False(final.IsCompleted);
            await user.CommitAsync();
            await final;                                                           // DELETE … RETURNING видит notify = true
        }
        else
        {
            var holder = await HoldRowsAsync("SELECT 1 FROM {schema}.queue WHERE job_id = @id FOR UPDATE", ("id", parent));
            var final = finalize();
            await WaitForLockWaitersAsync(1);                                      // финализация первой в очереди строки
            var subscribe = SubscribeAsync(parent);
            await WaitForLockWaitersAsync(2);                                      // п. 1 подписки — второй
            await holder.DisposeAsync();
            await final;
            child = (await subscribe).Jobs[0].Value;                               // п. 1 не нашёл строку → п. 2 видит терминальное
        }

        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(child));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Subscription_versus_sweeper(bool subscribeFirst)   // §12 Гонки
    {
        var parent = (await Client.Workflow().Enqueue(A(0), new StepOptions { MaxAttempts = 1 }).SubmitAsync(Ct)).Jobs[0].Value;
        await LeaseAsync(parent);
        await ExpireLeaseAsync(parent);
        Guid child;
        if (subscribeFirst)
        {
            await using var user = await BeginAsync();
            child = (await SubscribeAsync(parent, user.Transaction)).Jobs[0].Value;
            Assert.Equal(0, await Flow.SweepExpiredLeasesAsync(100, Ct));            // SKIP LOCKED: строку держит подписка
            await user.CommitAsync();
            Assert.Equal(1, await Flow.SweepExpiredLeasesAsync(100, Ct));
        }
        else
        {
            Assert.Equal(1, await Flow.SweepExpiredLeasesAsync(100, Ct));
            child = (await SubscribeAsync(parent)).Jobs[0].Value;
        }

        await ResolveAllAsync();

        Assert.Equal((short?)3, await StateAsync(parent));
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Subscription_to_failed_parent_waits_for_requeue_and_releases_jobs_row()   // §12 Гонки, §6.2 пп. 2–3
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        await FailFinallyAsync(parent);
        var holder = await HoldRowsAsync("SELECT 1 FROM {schema}.jobs WHERE id = @id FOR UPDATE", ("id", parent));
        var requeue = Client.RequeueAsync(new JobId(parent), Ct).AsTask();
        await WaitForLockWaitersAsync(1);                                            // requeue ждёт держателя строки jobs
        await using var user = await BeginAsync();
        var subscribe = SubscribeAsync(parent, user.Transaction, ContinuationCondition.OnSuccess);
        await WaitForLockWaitersAsync(2);                                            // п. 2 подписки ждёт за requeue
        await holder.DisposeAsync();

        Assert.Equal(RequeueResult.Requeued, await requeue);
        var child = (await subscribe).Jobs[0].Value;                                 // 0 строк → откат к точке сохранения → п. 1
        // notify = true п. 1 видна только в незакоммиченной транзакции подписки: закоммиченную строку queue requeue создал из
        // jobs.notify = false (UPDATE jobs п. 2 откатан к pgbolt_after) — читать через Db (Admin) до коммита нельзя.
        await using (var notify = new NpgsqlCommand(Db.Sql("SELECT notify FROM {schema}.queue WHERE job_id = @id"),
                         user.Connection, user.Transaction))
        {
            notify.Parameters.AddWithValue("id", parent);
            Assert.True((bool)(await notify.ExecuteScalarAsync(Ct))!);
        }
        await using (var probe = await Db.Admin.OpenConnectionAsync(Ct))             // строка jobs не заблокирована подпиской
        await using (var nowait = new NpgsqlCommand(Db.Sql("SELECT 1 FROM {schema}.jobs WHERE id = @id FOR UPDATE NOWAIT"), probe))
        {
            await using var probeTx = await probe.BeginTransactionAsync(Ct);
            nowait.Transaction = probeTx;
            nowait.Parameters.AddWithValue("id", parent);
            await nowait.ExecuteScalarAsync(Ct);
        }

        var cancel = Flow.CancelAsync(parent, "x", Ct);                              // держит queue и ждёт jobs — против п. 1
        await WaitForLockWaitersAsync(1);
        await user.CommitAsync();
        Assert.Equal(CancelResult.Cancelled, await cancel.WaitAsync(NoWait, Ct));    // без 40P01

        await ResolveAllAsync();
        Assert.Equal("cancelled", await FateAsync(child));
    }

    [Fact]
    public async Task Requeue_waits_for_user_transaction_subscribed_to_terminal_parent()   // §12 Гонки
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        await FailFinallyAsync(parent);
        await using var user = await BeginAsync();
        var child = (await SubscribeAsync(parent, user.Transaction, ContinuationCondition.OnSuccess)).Jobs[0].Value;   // п. 2 держит jobs

        var requeue = Client.RequeueAsync(new JobId(parent), Ct).AsTask();
        await WaitForLockWaitersAsync(1);
        Assert.False(requeue.IsCompleted);
        await user.CommitAsync();

        Assert.Equal(RequeueResult.Requeued, await requeue);
        Assert.True(await Db.ScalarAsync<bool>("SELECT notify FROM {schema}.queue WHERE job_id = @id", ("id", parent)));   // из jobs.notify
        await SucceedAsync(parent);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Batch_finished_before_subscription_commit_resolves_children_by_subscribed()   // §12 Гонки
    {
        IJobStep? m = null;
        var batch = (await Client.Workflow().StartBatch(b => m = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        await using var user = await BeginAsync();
        var child = (await Client.Workflow(new WorkflowOptions { Transaction = user.Transaction })
            .After(batch).ContinueWith(A(2)).SubmitAsync(Ct)).Jobs[0].Value;

        await SucceedAsync(m!.Id.Value);
        await ResolveAllAsync().WaitAsync(NoWait, Ct);                               // учёт не ждёт KEY SHARE подписки
        Assert.Equal((short?)2, await BatchStateAsync(batch.Value));                 // сигнал batch обработан до коммита детей
        await user.CommitAsync();
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Open_batch_subscription_does_not_block_counting_requeue_or_cancel()   // §12 Гонки
    {
        IReadOnlyList<IJobStep> members = [];
        var batch = (await Client.Workflow().StartBatch(b => members = b.EnqueueMany(new[] { A(1), A(2), A(3) })).SubmitAsync(Ct)).Batches[0];
        await FailFinallyAsync(members[0].Id.Value);
        await using var user = await BeginAsync();
        await Client.Workflow(new WorkflowOptions { Transaction = user.Transaction }).After(batch).ContinueWith(A(9)).SubmitAsync(Ct);

        await SucceedAsync(members[1].Id.Value);
        await ResolveWithoutCancelMembersAsync().WaitAsync(NoWait, Ct);
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(members[0].Id, Ct).AsTask().WaitAsync(NoWait, Ct));
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct).AsTask().WaitAsync(NoWait, Ct));

        await user.CommitAsync();
    }

    [Fact]
    public async Task Stale_cancelled_signal_does_not_cancel_new_OnSuccess_child()   // §12 Гонки «Устаревший сигнал»
    {
        var r = await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1), ContinuationCondition.OnAnyFinished).SubmitAsync(Ct);
        var parent = r.Jobs[0];
        await CancelJobAsync(parent.Value);                                          // сигнал Cancelled не обработан
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(parent, Ct));
        var child = (await Client.Workflow().After(parent).ContinueWith(A(2)).SubmitAsync(Ct)).Jobs[0].Value;

        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        await SucceedAsync(parent.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Stale_failed_signal_does_not_launch_OnAnyFinished_child_while_parent_runs()   // §12 Гонки
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1)).SubmitAsync(Ct)).Jobs[0];
        await FailFinallyAsync(parent.Value);
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(parent, Ct));
        var lease = await LeaseAsync(parent.Value);                                  // родитель снова выполняется
        var child = (await Client.Workflow().After(parent).ContinueWith(A(2), ContinuationCondition.OnAnyFinished).SubmitAsync(Ct)).Jobs[0].Value;

        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        Assert.Equal(1, await Flow.AckAsync(ServerA, [new LeaseRef(parent.Value, lease)], Ct));
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Stale_batch_outcome_after_member_requeue_is_dropped()   // §12 Гонки
    {
        IJobStep? x = null;
        var batch = (await Client.Workflow().StartBatch(b => x = b.Enqueue(A(1))).SubmitAsync(Ct)).Batches[0];
        await FailFinallyAsync(x!.Id.Value);
        await Flow.CountCompletionsAsync(500, Ct);                                   // batch Failed, его сигнал не обработан
        Assert.Equal((short?)3, await BatchStateAsync(batch.Value));
        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        var child = (await Client.Workflow().After(batch).ContinueWith(A(2), ContinuationCondition.OnAnyFinished).SubmitAsync(Ct)).Jobs[0].Value;

        await ResolveAllAsync();
        Assert.Equal("awaiting", await FateAsync(child));

        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(child));
    }

    [Fact]
    public async Task Two_resolvers_count_one_batch_exactly_without_deadlock()   // §12 Гонки
    {
        IReadOnlyList<IJobStep> members = [];
        var batch = (await Client.Workflow().StartBatch(b => members = b.EnqueueMany(Enumerable.Range(0, 2000).Select(A).ToArray()))
            .SubmitAsync(Ct)).Batches[0];
        await SucceedManyAsync([.. members.Select(m => m.Id.Value)]);
        // retryOnConflict: false — 40P01 не гасится повтором и проявился бы исключением
        var resolvers = Enumerable.Range(0, 2)
            .Select(_ => new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false)).ToArray();

        await Task.WhenAll(resolvers.Select(s => Task.Run(async () =>
        {
            while ((await s.CountCompletionsAsync(100, Ct)).Captured > 0)
            {
            }
        }, Ct)));
        await ResolveAllAsync();

        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 2 AND succeeded = 2000 AND failed = 0 FROM {schema}.batches WHERE id = @id",
            ("id", batch.Value)));
    }

    [Theory]
    [InlineData(WorkflowOutcome.Cancelled, true, false)]
    [InlineData(WorkflowOutcome.Cancelled, false, false)]
    [InlineData(WorkflowOutcome.Failed, true, false)]
    [InlineData(WorkflowOutcome.Failed, false, false)]
    [InlineData(WorkflowOutcome.Cancelled, true, true)]
    [InlineData(WorkflowOutcome.Failed, false, true)]
    public async Task Requeue_versus_counting_keeps_batch_active(WorkflowOutcome first, bool countedBeforeRequeue, bool nested)   // §12 Гонки
    {
        IJobStep? x = null, y = null;
        IBatchStep? inner = null;
        var wf = Client.Workflow();
        var batch = nested
            ? wf.StartBatch(o =>
            {
                inner = o.StartBatch(b => x = b.Enqueue(A(1)));
                y = o.Enqueue(A(2));
            })
            : wf.StartBatch(b =>
            {
                x = b.Enqueue(A(1));
                y = b.Enqueue(A(2));
            });
        var onSuccess = batch.ContinueWith(A(3));
        var onAny = batch.ContinueWith(A(4), ContinuationCondition.OnAnyFinished);
        await wf.SubmitAsync(Ct);
        var yLease = await LeaseAsync(y!.Id.Value);                                  // Y выполняется
        await FinishAsync(x!.Id.Value, first);
        if (countedBeforeRequeue)
            await ResolveAllAsync();                                                 // сигнал X учтён до requeue

        Assert.Equal(RequeueResult.Requeued, await Client.RequeueAsync(x.Id, Ct));
        if (nested)
            Assert.Equal((short?)0, await BatchStateAsync(inner!.Id.Value));         // вложенный вернулся в Active
        await SucceedAsync(x.Id.Value);
        await ResolveAllAsync();

        Assert.Equal((short?)0, await BatchStateAsync(batch.Id.Value));
        Assert.Equal("awaiting", await FateAsync(onSuccess.Id.Value));               // не отменено
        Assert.Equal("awaiting", await FateAsync(onAny.Id.Value));                   // не запущено раньше времени

        Assert.Equal(1, await Flow.AckAsync(ServerA, [new LeaseRef(y.Id.Value, yLease)], Ct));
        await ResolveAllAsync();

        Assert.Equal((short?)2, await BatchStateAsync(batch.Id.Value));
        Assert.Equal("launched", await FateAsync(onSuccess.Id.Value));
        Assert.Equal("launched", await FateAsync(onAny.Id.Value));
    }

    [Fact]
    public async Task Subscription_holding_awaiting_child_does_not_stall_resolver()   // §12 Гонки, Review Focus 3
    {
        var first = await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1)).SubmitAsync(Ct);
        var second = await Client.Workflow().Enqueue(A(2)).ContinueWith(A(3)).SubmitAsync(Ct);
        var held = first.Jobs[1];
        await using var user = await BeginAsync();
        await Client.Workflow(new WorkflowOptions { Transaction = user.Transaction }).After(held).ContinueWith(A(4)).SubmitAsync(Ct);
        await SucceedAsync(first.Jobs[0].Value);
        await SucceedAsync(second.Jobs[0].Value);

        await ResolveAllAsync().WaitAsync(NoWait, Ct);

        Assert.Equal("launched", await FateAsync(second.Jobs[1].Value));
        Assert.Equal("awaiting", await FateAsync(held.Value));
        Assert.True(await Db.ScalarAsync<bool>("SELECT retry_after > now() FROM {schema}.completions WHERE node_id = @id",
            ("id", first.Jobs[0].Value)));

        await user.CommitAsync();
        await Db.ExecAsync("UPDATE {schema}.completions SET retry_after = NULL");   // прошёл ResolverInterval
        await ResolveAllAsync();
        Assert.Equal("launched", await FateAsync(held.Value));
    }

    [Fact]
    public async Task Open_subscription_blocks_heartbeat_of_running_parent_until_commit()   // §12 Гонки (документированное поведение)
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        var lease = await LeaseAsync(parent);
        await using var user = await BeginAsync();
        await SubscribeAsync(parent, user.Transaction);

        var heartbeat = Flow.HeartbeatAsync(ServerA, [new LeaseRef(parent, lease)], Lease, Ct);
        await WaitForLockWaitersAsync();
        Assert.False(heartbeat.IsCompleted);
        await user.CommitAsync();

        Assert.Single((await heartbeat).Leases);
    }

    [Fact]
    public async Task Open_subscription_hides_pending_parent_from_dequeue_until_commit()   // §12 Гонки (документированное поведение)
    {
        var parent = (await Client.Workflow().Enqueue(A(0)).SubmitAsync(Ct)).Jobs[0].Value;
        await using var user = await BeginAsync();
        await SubscribeAsync(parent, user.Transaction);

        Assert.Empty(await Flow.DequeueAsync("client", 10, ServerA, Lease, Ct));      // SKIP LOCKED
        await user.CommitAsync();

        Assert.Equal(parent, Assert.Single(await Flow.DequeueAsync("client", 10, ServerA, Lease, Ct)).Id);
    }

    [Fact]
    public async Task Signal_written_by_ack_is_resolved_by_another_process()   // §12 Падения
    {
        var r = await Client.Workflow().Enqueue(A(0)).ContinueWith(A(1)).SubmitAsync(Ct);
        await SucceedAsync(r.Jobs[0].Value);                                         // «процесс» умер, не разбудив resolver
        var other = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true);

        await other.CountCompletionsAsync(500, Ct);
        await other.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);

        Assert.Equal("launched", await FateAsync(r.Jobs[1].Value));
    }

    [Fact]
    public async Task Failure_inside_walk_rolls_back_and_retry_launches_once_and_counts_once()   // §12 Падения
    {
        IJobStep? m = null;
        var batch = Client.Workflow().StartBatch(b => m = b.Enqueue(A(1)));
        var next = batch.ContinueWith(A(2));
        await batch.SubmitAsync(Ct);
        await SucceedAsync(m!.Id.Value);
        await Flow.CountCompletionsAsync(500, Ct);                                   // счётчик учтён, сигнал batch создан
        await Flow.CountCompletionsAsync(500, Ct);                                   // сигнал batch передан Т2
        Flow.ResolverHook = (step, _) => step == ResolverStep.Walked ? throw new InvalidOperationException("crash") : Task.CompletedTask;

        await Assert.ThrowsAsync<InvalidOperationException>(() => Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct));
        Assert.Equal("awaiting", await FateAsync(next.Id.Value));                    // откат: запуск не закоммичен
        Flow.ResolverHook = null;
        await ResolveAllAsync();

        Assert.Equal("launched", await FateAsync(next.Id.Value));
        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 2 AND succeeded = 1 FROM {schema}.batches WHERE id = @id", ("id", batch.Id.Value)));
        Assert.Equal(0L, await SignalsAsync());
    }
}

public sealed class WorkflowRaceTests_Direct(PostgresFixture pg) : WorkflowRaceTests(pg, ConnectionMode.Direct);
public sealed class WorkflowRaceTests_Bouncer(PostgresFixture pg) : WorkflowRaceTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 3: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowRaceTests"`
Expected: PASS (Direct и Bouncer), повторный прогон 3 раза подряд — PASS (детерминированность): `(for i in 1 2 3; do dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WorkflowRaceTests" || exit 1; done)` — код возврата 0 и три вывода PASS; первое падение прерывает цикл, и подоболочка возвращает 1 (не `break`: после него цикл вернул бы 0).

- [x] **Step 4: Commit**

```bash
git add src/PgBolt.Postgres src/PgBolt.Abstractions src/PgBolt.Server/Runtime/JobRunner.cs tests/PgBolt.Postgres.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(workflows): deterministic races and crash recovery"
```

---
### Task 10: Retention: живые строки части 5 и удаление строк `batches` (§7.1)

**Files:**
- Create: `src/PgBolt.Postgres/Schema/RetentionRunner.Batches.cs`
- Modify: `src/PgBolt.Postgres/Schema/RetentionRunner.cs` (`ApplyAsync`, `DetachGroupAsync`, `Accumulator.ToResult`), `src/PgBolt.Postgres/Schema/RetentionRunner.Jobs.cs` (`PrecheckAsync`, `CompactAsync`), `src/PgBolt.Postgres/Schema/RetentionTuning.cs` (`RetentionStep.BatchesLocked`), `src/PgBolt.Postgres/Schema/RetentionResult.cs` (`DeletedBatches`)
- Test: `tests/PgBolt.Postgres.Tests/RetentionWorkflowTests.cs`

**Interfaces:**
- Consumes: схема (Task 1), клиент и resolver (Task 5–8), `RetentionTestBase`, `HookRecorder` (часть 2).
- Produces: `RetentionResult(…, int AttachedJobsPartitions, int DeletedBatches = 0)`; `RetentionStep.BatchesLocked` (хук после statement'а 1 удаления, `Parent = "batches"`); `RetentionRunner.AwaitingParentsSql(bool batches, string lo, string hi, string limit)`, `BatchMembersSql`, `BatchCandidatesSql(bool withCursor)` — `internal` (тесты `EXPLAIN`).

- [x] **Step 1: Тесты**

`tests/PgBolt.Postgres.Tests/RetentionWorkflowTests.cs`:

```csharp
using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §7.1: живые строки (а)/(б), повторная проверка под блокировкой, удаление строк batches; гонки подписки с удалением.</summary>
public abstract class RetentionWorkflowTests(PostgresFixture pg, ConnectionMode mode) : RetentionTestBase(pg, mode)
{
    private static readonly TimeSpan Days3 = TimeSpan.FromDays(3);
    private IJobClient _client = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        _client = new PgBoltJobClient(Store, new JobTypesCache(Store, TimeSpan.FromSeconds(30), TimeProvider.System), new RecordingWakeup());
    }

    /// <summary>Терминальная задача суток day (notify — у неё были подписчики или она член batch).</summary>
    private async Task<Guid> TerminalAsync(DateOnly day, short state, Guid? batch = null)
    {
        var id = IdOn(day);
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at, batch_id, notify)
            VALUES (@id, 'test.job', 'q', 0, '{}', @s, 1, 3, now(), now(), @b, true)
            """, ("id", id), ("s", state), ("b", batch));
        return id;
    }

    /// <summary>Ждущий ребёнок (сегодняшний) родителя parent с кодом continuation code.</summary>
    private async Task<Guid> AwaitingChildAsync(Guid parent, short code)
    {
        var id = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            VALUES (@id, 'q', 0, 'infinity', false, 3, @p, @c);
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, parent_id, continuation)
            VALUES (@id, 'test.job', 'q', 0, '{}', 0, 3, now(), @p, @c);
            """, ("id", id), ("p", parent), ("c", code));
        return id;
    }

    /// <summary>Строка batches; finishedDaysAgo — finished_at = now() − N суток (терминальный), null — не завершён.</summary>
    private async Task<Guid> BatchAsync(short state, int? finishedDaysAgo = null, Guid? outer = null, Guid? parent = null,
        short? condition = null, Guid? id = null)
    {
        var batch = id ?? Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, parent_batch_id, parent_id, condition, state, total, created_at, finished_at)
            VALUES (@id, @o, @p, @c, @s, 1, now(), now() - make_interval(days => @d))
            """, ("id", batch), ("o", outer), ("p", parent), ("c", condition), ("s", state), ("d", finishedDaysAgo ?? 0));
        if (finishedDaysAgo is null)
            await Db.ExecAsync("UPDATE {schema}.batches SET finished_at = NULL WHERE id = @id", ("id", batch));
        return batch;
    }

    private Task<bool> JobExistsAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.jobs WHERE id = @id)", ("id", id));

    private Task<bool> BatchExistsAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.batches WHERE id = @id)", ("id", id));

    private async Task ResolveAllAsync()
    {
        for (var i = 0; i < 100; i++)
        {
            var counted = await Store.CountCompletionsAsync(500, Ct);
            var walked = await Store.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);
            var cancelled = await Store.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), Ct);
            if (counted.Captured == 0 && walked.Captured == 0 && cancelled.Portions == 0)
                return;
        }
    }

    private async Task WaitForRowLockWaiterAsync()
    {
        var started = Stopwatch.GetTimestamp();
        while (await Db.ScalarAsync<long>("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p",
                   ("p", $"%{Db.Schema}.batches%")) == 0)
        {
            if (Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10))
                throw new TimeoutException("No session waits for a batches row lock.");
            await Task.Delay(20, Ct);
        }
    }

    [Fact]
    public async Task Partition_with_workflow_live_rows_is_compacted_and_keeps_them()   // §12 Retention
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var failedParent = await TerminalAsync(day, 3);                       // (а): ждущий OnSuccess-ребёнок
        await AwaitingChildAsync(failedParent, 1);
        var batchParent = await TerminalAsync(day, 3);                        // (а): единственный ребёнок — batch-продолжение
        await BatchAsync(state: 1, parent: batchParent, condition: 1);
        var failedBatch = await BatchAsync(state: 3, finishedDaysAgo: 0);
        var failedMember = await TerminalAsync(day, 3, failedBatch);          // (б)
        var cancelledMember = await TerminalAsync(day, 4, failedBatch);       // (б)
        var cancelledBatch = await BatchAsync(state: 4, finishedDaysAgo: 0);
        var memberOfCancelled = await TerminalAsync(day, 4, cancelledBatch);  // (б): Cancelled-член Cancelled-batch
        await InsertTerminalAsync(day, 150);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        var pinned = Assert.Single(r.Pinned);
        Assert.Equal((day, 5, true), (pinned.Day, pinned.LiveJobs, pinned.Compacted));
        Assert.Equal(5L, await CountAsync(J(day)));
        foreach (var id in new[] { failedParent, batchParent, failedMember, cancelledMember, memberOfCancelled })
            Assert.True(await JobExistsAsync(id));
    }

    [Fact]
    public async Task Batch_ids_with_waiting_children_are_not_live_jobs()   // §12 Retention: id batch не попадают в LiveJobs
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        await InsertTerminalAsync(day, 3);
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 0, id: IdOn(day));   // id batch в диапазоне суток
        await AwaitingChildAsync(batch, 3);
        await BatchAsync(state: 1, parent: batch, condition: 3);

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Contains(day, r.DroppedJobsPartitions);
    }

    [Fact]
    public async Task Subscription_to_old_terminal_job_between_precheck_and_lock_keeps_parent()   // §12 Retention (хук)
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var parent = await TerminalAsync(day, 2);
        await InsertTerminalAsync(day, 3);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.LocksChecked && c.Parent == "jobs")
                    await _client.Workflow().After(new JobId(parent)).ContinueWith(new ClientArgs(1)).SubmitAsync(ct);
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.DoesNotContain(day, r.DroppedJobsPartitions);
        Assert.True(await AttachedAsync(J(day)));
        Assert.True(await JobExistsAsync(parent));
    }

    [Fact]
    public async Task Requeue_and_final_fail_of_cancelled_member_between_precheck_and_lock_keeps_member()   // §12 Retention (хук)
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 4, finishedDaysAgo: 0);
        var member = await TerminalAsync(day, 4, batch);
        await InsertTerminalAsync(day, 150);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step != RetentionStep.WarmedUp)
                    return;
                Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, ct)).Result);
                var leased = Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, ct));
                Assert.Equal(FailOutcome.Failed, await Store.FailAsync(member, leased.LeaseId, ServerA, JobEventKind.Failed, "x", null, ct));
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.True(Assert.Single(r.Pinned).Compacted);
        Assert.True(await JobExistsAsync(member));                            // объединение с набором предпроверки
        Assert.Equal((short?)3, await JobStateAsync(member));
    }

    [Fact]
    public async Task Cancel_of_pending_member_between_precheck_and_lock_keeps_member()   // §12 Retention (хук), §7.1
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 0);                                // существующий незавершённый batch
        var member = IdOn(day);                                                // Pending-член: в предпроверке — строка queue, не (б)
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, batch_id, notify)
            VALUES (@id, 'q', 0, now(), true, 3, @b, true);
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at, batch_id, notify)
            VALUES (@id, 'test.job', 'q', 0, '{}', 0, 3, now(), @b, true);
            """, ("id", member), ("b", batch));
        await InsertTerminalAsync(day, 150);
        var hooks = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step == RetentionStep.WarmedUp)                         // член стал живым по (б); queue и (а) под блокировкой пусты
                    Assert.Equal(CancelResult.Cancelled, await Store.CancelAsync(member, null, ct));
            },
        };

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.DoesNotContain(day, r.DroppedJobsPartitions);                  // не отсоединена: объединение с набором предпроверки
        var pinned = Assert.Single(r.Pinned);
        Assert.Equal((day, 1, true), (pinned.Day, pinned.LiveJobs, pinned.Compacted));
        Assert.Equal(1L, await CountAsync(J(day)));
        Assert.Equal((short?)4, await JobStateAsync(member));
    }

    [Fact]
    public async Task Batch_rows_are_deleted_top_down_by_jobs_retention()   // §12 Retention
    {
        var outer = await BatchAsync(state: 2, finishedDaysAgo: 30);
        var nested = await BatchAsync(state: 2, finishedDaysAgo: 31, outer: outer);   // завершён раньше внешнего
        var recent = await BatchAsync(state: 3, finishedDaysAgo: 0);
        var active = await BatchAsync(state: 0);

        Assert.Equal(0, (await Runner(o => o.JobsRetention = null).ApplyAsync(Ct)).DeletedBatches);
        var first = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);
        Assert.Equal(1, first.DeletedBatches);
        Assert.False(await BatchExistsAsync(outer));
        Assert.True(await BatchExistsAsync(nested));                          // внешний существовал при проверке
        var second = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(1, second.DeletedBatches);
        Assert.False(await BatchExistsAsync(nested));
        Assert.True(await BatchExistsAsync(recent));
        Assert.True(await BatchExistsAsync(active));
    }

    [Fact]
    public async Task Held_batches_are_read_once_per_run()   // §12 Retention
    {
        var outer = await BatchAsync(state: 0);
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, parent_batch_id, state, total, created_at, finished_at)
            SELECT gen_random_uuid(), CASE WHEN g % 2 = 0 THEN @o END, 2, 1, now(), now() - interval '30 days' - g * interval '1 second'
            FROM generate_series(1, 10000) g
            """, ("o", outer));
        var hooks = new HookRecorder();

        var r = await Runner(o => o.JobsRetention = Days3, hooks.Tuning()).ApplyAsync(Ct);

        Assert.Equal(5000, r.DeletedBatches);                                 // удерживаемые внешним 5000 не удалены
        Assert.Equal(6, hooks.Count(RetentionStep.BatchesLocked, "batches")); // 5 полных пакетов и пустой: курсор не возвращается назад
        Assert.Equal(5001L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
        // Прямое измерение «число прочитанных строк не растёт с числом пакетов» — EXPLAIN (ANALYZE) statement'а 1 в начале и в
        // конце диапазона: RetentionWorkflowPlanTests.Held_batches_scan_does_not_grow_with_cursor_position (Direct, Slow).
    }

    [Fact]
    public async Task Failed_member_of_deleted_batch_is_removed_by_next_run()   // §12 Retention
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 30);
        await TerminalAsync(day, 3, batch);
        await InsertTerminalAsync(day, 3);

        var first = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);
        Assert.DoesNotContain(day, first.DroppedJobsPartitions);              // член жив по (б), пока есть строка batch
        Assert.Equal(1, first.DeletedBatches);
        var second = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Contains(day, second.DroppedJobsPartitions);
    }

    [Fact]
    public async Task Continuation_of_old_failed_batch_waits_for_member_requeue()   // §12 Retention
    {
        var day = Ago(10);
        await CreatePartitionsAsync(day, events: false);
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 30);
        await Db.ExecAsync("UPDATE {schema}.batches SET failed = 1 WHERE id = @id", ("id", batch));
        var member = await TerminalAsync(day, 3, batch);
        await InsertTerminalAsync(day, 150);
        var child = (await _client.Workflow().After(new BatchId(batch)).ContinueWith(new ClientArgs(1)).SubmitAsync(Ct)).Jobs[0].Value;
        await ResolveAllAsync();

        var r = await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct);

        Assert.Equal(0, r.DeletedBatches);                                     // ждущий ребёнок держит строку batch
        Assert.True(await JobExistsAsync(member));
        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, Ct)).Result);
        var leased = Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
        Assert.Equal(1, await Store.AckAsync(ServerA, [new LeaseRef(member, leased.LeaseId)], Ct));
        await ResolveAllAsync();

        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND await_condition IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", child)));
    }

    [Fact]
    public async Task Retention_deleted_batch_references_are_harmless()   // Review Focus 5, §7.1
    {
        var batch = await BatchAsync(state: 3, finishedDaysAgo: 30);
        var member = Guid.CreateVersion7();                                    // сегодняшний Failed-член
        await Db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, attempt, max_attempts, created_at, finished_at, batch_id, notify)
            VALUES (@id, 'test.job', 'q', 0, '{}', 3, 1, 3, now(), now(), @b, true)
            """, ("id", member), ("b", batch));
        Assert.Equal(1, (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DeletedBatches);

        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(member, Ct)).Result);
        var leased = Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
        Assert.Equal(1, await Store.AckAsync(ServerA, [new LeaseRef(member, leased.LeaseId)], Ct));

        Assert.Equal([batch], (await Store.CountCompletionsAsync(500, Ct)).MissingBatches);
    }

    [Fact]
    public async Task Batch_subscription_versus_batch_deletion()   // §12 Гонки: подписка на batch против удаления retention'ом
    {
        // (а) подписка первой: удаление пропускает строку (SKIP LOCKED) и не ждёт; ребёнок разрешается.
        var first = await BatchAsync(state: 2, finishedDaysAgo: 30);
        Guid child;
        await using (var conn = await Db.DataSource.OpenConnectionAsync(Ct))
        await using (var tx = await conn.BeginTransactionAsync(Ct))
        {
            child = (await _client.Workflow(new WorkflowOptions { Transaction = tx }).After(new BatchId(first))
                .ContinueWith(new ClientArgs(1)).SubmitAsync(Ct)).Jobs[0].Value;
            Assert.Equal(0, (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct)).DeletedBatches);
            await tx.CommitAsync(Ct);
        }
        await ResolveAllAsync();
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND await_condition IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", child)));
        Assert.Equal(1, (await Runner(o => o.JobsRetention = Days3).ApplyAsync(Ct)).DeletedBatches);   // ребёнок запущен — строка не держится

        // (б) удаление первым: подписка ждёт FOR UPDATE и не находит строку.
        var second = await BatchAsync(state: 2, finishedDaysAgo: 30);
        Task<WorkflowResult>? subscribe = null;
        var deleting = new HookRecorder
        {
            Then = async (c, ct) =>
            {
                if (c.Step != RetentionStep.BatchesLocked || subscribe is not null)
                    return;
                subscribe = _client.Workflow().After(new BatchId(second)).ContinueWith(new ClientArgs(2)).SubmitAsync(ct).AsTask();
                await WaitForRowLockWaiterAsync();
            },
        };
        Assert.Equal(1, (await Runner(o => o.JobsRetention = Days3, deleting.Tuning()).ApplyAsync(Ct)).DeletedBatches);
        Assert.Equal(second, (await Assert.ThrowsAsync<ParentNotFoundException>(() => subscribe!)).ParentId);

        // (в) ребёнок закоммичен между снимком выбора кандидатов и проверкой удаления (решение плана 4): строка не удаляется.
        var third = await BatchAsync(state: 2, finishedDaysAgo: 30);
        var committed = false;
        var racing = new HookRecorder
        {
            Then = async (c, _) =>
            {
                if (c.Step != RetentionStep.BatchesLocked || committed)
                    return;
                committed = true;
                await AwaitingChildAsync(third, 3);
            },
        };
        Assert.Equal(0, (await Runner(o => o.JobsRetention = Days3, racing.Tuning()).ApplyAsync(Ct)).DeletedBatches);
        Assert.True(await BatchExistsAsync(third));
    }
}

public sealed class RetentionWorkflowTests_Direct(PostgresFixture pg) : RetentionWorkflowTests(pg, ConnectionMode.Direct);
public sealed class RetentionWorkflowTests_Bouncer(PostgresFixture pg) : RetentionWorkflowTests(pg, ConnectionMode.Bouncer);

/// <summary>§12 Retention: стоимость под блокировкой и план удаления batches. Только Direct (EXPLAIN), Slow.</summary>
[Trait("Category", "Slow")]
public sealed class RetentionWorkflowPlanTests(PostgresFixture pg) : RetentionTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Awaiting_parents_scan_does_not_grow_with_children_of_one_parent()
    {
        var day = Ago(10);
        var parent = IdOn(day);
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, parent_id, await_condition)
            SELECT gen_random_uuid(), 'q', 0, 'infinity', false, 3, @p, 1 FROM generate_series(1, 1000000);
            ANALYZE {schema}.queue;
            """, ("p", parent));
        var sql = Runner().AwaitingParentsSql(batches: false, "$1", "$2", "$3");
        await using var cmd = Db.Admin.CreateCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = UuidV7.LowerBound(day) });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = UuidV7.LowerBound(day.AddDays(1)) });
        cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1001 });

        using var doc = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(Ct))!);
        var root = doc.RootElement[0].GetProperty("Plan");

        Assert.Equal(1L, root.GetProperty("Actual Rows").GetInt64());
        var blocks = root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64();
        Assert.True(blocks < 100, $"(a) scan touched {blocks} buffers for one parent with a million children");
    }

    [Fact]
    public async Task Batch_candidates_are_a_range_scan_without_sort()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at, finished_at)
            SELECT gen_random_uuid(), 2, 1, now(), now() - interval '30 days' - g * interval '1 second' FROM generate_series(1, 20000) g;
            ANALYZE {schema}.batches;
            """);
        await using var cmd = Db.Admin.CreateCommand("EXPLAIN " + Runner().BatchCandidatesSql(withCursor: true));
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow.AddDays(-7) });
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow.AddDays(-60) });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = Guid.Empty });
        var lines = new List<string>();
        await using (var r = await cmd.ExecuteReaderAsync(Ct))
            while (await r.ReadAsync(Ct))
                lines.Add(r.GetString(0));
        var plan = string.Join('\n', lines);

        Assert.Contains("batches_finished", plan);
        Assert.DoesNotContain("Sort", plan);
    }

    [Fact]
    public async Task Held_batches_scan_does_not_grow_with_cursor_position()   // §12 Retention: строки читаются запуском один раз
    {
        var outer = Guid.CreateVersion7();
        await Db.ExecAsync("""
            INSERT INTO {schema}.batches (id, state, total, created_at) VALUES (@o, 0, 5000, now());
            INSERT INTO {schema}.batches (id, parent_batch_id, state, total, created_at, finished_at)
            SELECT gen_random_uuid(), CASE WHEN g % 2 = 0 THEN @o END, 2, 1, now(), now() - interval '30 days' - g * interval '1 second'
            FROM generate_series(1, 10000) g;
            ANALYZE {schema}.batches;
            """, ("o", outer));
        // Удерживаемые внешним (чётные g) чередуются с кандидатами: пакет из 1000 кандидатов читает ~2000 строк в любом месте
        // диапазона. Курсор «в конце» — 7000-я строка порядка (finished_at, id): за ним 3000 строк, кандидатов больше пакета.
        DateTime lateAt;
        Guid lateId;
        await using (var position = Db.Admin.CreateCommand(Db.Sql(
                         "SELECT finished_at, id FROM {schema}.batches WHERE finished_at IS NOT NULL ORDER BY finished_at, id OFFSET 6999 LIMIT 1")))
        await using (var r = await position.ExecuteReaderAsync(Ct))
        {
            Assert.True(await r.ReadAsync(Ct));
            (lateAt, lateId) = (r.GetDateTime(0), r.GetGuid(1));
        }

        var early = await CandidateRowsReadAsync(DateTime.UtcNow.AddYears(-1), Guid.Empty);   // курсор до начала диапазона
        var late = await CandidateRowsReadAsync(lateAt, lateId);

        Assert.InRange(early, RetentionRunner.BatchDeleteChunk, 2 * RetentionRunner.BatchDeleteChunk + 2);
        Assert.True(late <= early + 2, $"batches_finished scan read {early} rows at the start of the range and {late} near its end");
    }

    /// <summary>
    /// Строки, прочитанные узлом Index Scan по batches_finished в EXPLAIN (ANALYZE, BUFFERS) statement'а 1 с курсором:
    /// Actual Rows × Loops + Rows Removed by Filter. Транзакция откатывается (блокировки FOR UPDATE).
    /// </summary>
    private async Task<long> CandidateRowsReadAsync(DateTime cursorAt, Guid cursorId)
    {
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + Runner().BatchCandidatesSql(withCursor: true),
            conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow.AddDays(-7) });
        cmd.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cursorAt, DateTimeKind.Utc) });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = cursorId });
        var plan = (string)(await cmd.ExecuteScalarAsync(Ct))!;
        await tx.RollbackAsync(Ct);
        using var doc = JsonDocument.Parse(plan);
        var scan = Assert.Single(Flatten(doc.RootElement[0].GetProperty("Plan")),
            n => n.TryGetProperty("Index Name", out var name) && name.GetString() == "batches_finished");
        var removed = scan.TryGetProperty("Rows Removed by Filter", out var f) ? f.GetInt64() : 0;
        return scan.GetProperty("Actual Rows").GetInt64() * scan.GetProperty("Actual Loops").GetInt64() + removed;
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var n in Flatten(child))
                    yield return n;
    }
}
```

- [x] **Step 2: Запустить — не компилируется**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~RetentionWorkflow"`
Expected: FAIL — нет `RetentionStep.BatchesLocked`, `RetentionResult.DeletedBatches`, `AwaitingParentsSql`, `BatchCandidatesSql`.

- [x] **Step 3: Шаг хука и результат**

`src/PgBolt.Postgres/Schema/RetentionTuning.cs`: в `RetentionStep` добавить `BatchesLocked` последним членом; summary перечисления дополнить «… и после выбора и блокировки кандидатов удаления строк batches (часть 5 §7.1)».

`src/PgBolt.Postgres/Schema/RetentionResult.cs`: в `RetentionResult` после `int AttachedJobsPartitions` добавить параметр `int DeletedBatches = 0` и строку XML-doc `/// <param name="DeletedBatches">Удалено строк batches старше JobsRetention (часть 5 §7.1).</param>`.

`src/PgBolt.Postgres/Schema/RetentionRunner.cs`:
- в `ApplyAsync` заменить `return acc.ToResult(await CountAttachedJobsAsync(ct));` на:

```csharp
        // Часть 5 §7.1: строки batches — по JobsRetention, сверху вниз; при null не удаляются.
        var deletedBatches = _options.JobsRetention is { } retention ? await DeleteBatchesAsync(retention, ct) : 0;
        return acc.ToResult(await CountAttachedJobsAsync(ct), deletedBatches);
```

- в `Accumulator.ToResult` добавить параметр: `public RetentionResult ToResult(int attachedJobs, int deletedBatches) => new([.. DroppedJobs], [.. DroppedEvents], RemnantsDropped, [.. Pinned], [.. Deferred], attachedJobs, deletedBatches);`
- в `DetachGroupAsync` третий столбец проверки `jobs` заменить на объединение трёх диапазонов (§7.1 «при отсоединении — EXISTS по тем же трём диапазонам»):

```csharp
                       EXISTS (SELECT 1 FROM {_schema}.queue q WHERE q.job_id >= p.lo AND q.job_id < p.hi)
                       OR EXISTS (SELECT 1 FROM {_schema}.queue q
                                  WHERE q.await_condition IN (1, 2) AND q.parent_id >= p.lo AND q.parent_id < p.hi)
                       OR EXISTS (SELECT 1 FROM {_schema}.batches b
                                  WHERE b.state = 1 AND b.condition IN (1, 2) AND b.parent_id >= p.lo AND b.parent_id < p.hi)
```

- [x] **Step 4: Источники живых строк и удаление batches**

`src/PgBolt.Postgres/Schema/RetentionRunner.Batches.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres.Schema;

/// <summary>
/// Часть 5 §7.1: источники живых строк партиции jobs [lo, hi) — (а) родители-задачи с ждущими детьми (loose index scan по
/// queue_await_jobs / batches_await_jobs, не больше T + 1 спусков), (б) Failed- и Cancelled-члены существующих незавершённых
/// успехом batches (только в предпроверке); удаление строк batches по JobsRetention — блокировка, затем проверка новым снимком.
/// </summary>
internal sealed partial class RetentionRunner
{
    internal const int BatchDeleteChunk = 1000;

    /// <summary>(а): различные parent_id ожиданий родителей-задач (коды 1, 2) в [lo, hi); lo/hi/limit — текст параметров ($N).</summary>
    internal string AwaitingParentsSql(bool batches, string lo, string hi, string limit)
    {
        var (table, predicate) = batches
            ? ("batches", "state = 1 AND condition IN (1, 2)")
            : ("queue", "await_condition IN (1, 2)");
        return $"""
            WITH RECURSIVE p(id) AS (
                (SELECT parent_id FROM {_schema}.{table}
                 WHERE {predicate} AND parent_id >= {lo} AND parent_id < {hi} ORDER BY parent_id LIMIT 1)
                UNION ALL
                SELECT (SELECT parent_id FROM {_schema}.{table}
                        WHERE {predicate} AND parent_id > p.id AND parent_id < {hi} ORDER BY parent_id LIMIT 1)
                FROM p WHERE p.id IS NOT NULL
            )
            SELECT id FROM p WHERE id IS NOT NULL LIMIT {limit}
            """;
    }

    /// <summary>(б): у Succeeded-batch Failed/Cancelled-членов нет (§6.6), поэтому обходятся batches_open; $1/$2 — диапазон, $3 — лимит.</summary>
    internal string BatchMembersSql => $"""
        SELECT j.id FROM {_schema}.batches b
        CROSS JOIN LATERAL (SELECT id FROM {_schema}.jobs
                            WHERE batch_id = b.id AND batch_id IS NOT NULL AND id >= $1 AND id < $2 AND state IN (3, 4)) j
        WHERE b.state <> 2
        LIMIT $3
        """;

    /// <summary>
    /// §7.1, statement 1: кандидаты удаления в порядке (finished_at, id) с проверками statement'а 2 (только отсев заведомо живых),
    /// FOR UPDATE SKIP LOCKED. $1 — порог, $2/$3 — курсор последнего заблокированного кандидата пакета (в пределах запуска).
    /// </summary>
    internal string BatchCandidatesSql(bool withCursor) => $"""
        SELECT b.id, b.finished_at FROM {_schema}.batches b
        WHERE b.finished_at < $1 {(withCursor ? "AND (b.finished_at, b.id) > ($2, $3)" : "")}
          AND b.state IN (2, 3, 4)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.queue q WHERE q.parent_id = b.id AND q.await_condition IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches c WHERE c.parent_id = b.id AND c.parent_id IS NOT NULL AND c.state = 1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches o WHERE o.id = b.parent_batch_id)
        ORDER BY b.finished_at, b.id
        LIMIT {BatchDeleteChunk}
        FOR UPDATE OF b SKIP LOCKED
        """;

    /// <summary>§7.1, statement 2: решение — новым снимком после блокировок (подписка, закоммиченная до блокировки, видна).</summary>
    private string BatchDeleteSql => $"""
        DELETE FROM {_schema}.batches b
        WHERE b.id = ANY($1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.queue WHERE parent_id = b.id AND await_condition IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches WHERE parent_id = b.id AND state = 1)
          AND NOT EXISTS (SELECT 1 FROM {_schema}.batches o WHERE o.id = b.parent_batch_id)
        RETURNING b.id
        """;

    /// <summary>§7.1: пакеты по 1000; порог now() − JobsRetention фиксируется на запуск; неполный пакет завершает запуск.</summary>
    private async Task<int> DeleteBatchesAsync(TimeSpan retention, CancellationToken ct)
    {
        DateTime threshold;
        await using (var now = _dataSource.CreateCommand("SELECT now() - $1"))
        {
            now.Parameters.Add(Arg(retention));
            threshold = (DateTime)(await now.ExecuteScalarAsync(ct))!;
        }

        var deleted = 0;
        (DateTime At, Guid Id)? cursor = null;
        while (true)
        {
            await using var conn = await _dataSource.OpenConnectionAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);
            var locked = new List<(Guid Id, DateTime At)>();
            await using (var select = new NpgsqlCommand(BatchCandidatesSql(cursor is not null), conn, tx))
            {
                select.Parameters.Add(Arg(threshold));
                if (cursor is { } c)
                {
                    select.Parameters.Add(Arg(c.At));
                    select.Parameters.Add(Arg(c.Id));
                }
                await using var r = await select.ExecuteReaderAsync(ct);
                while (await r.ReadAsync(ct))
                    locked.Add((r.GetGuid(0), r.GetDateTime(1)));
            }
            await HookAsync(RetentionStep.BatchesLocked, "batches", [], ct);
            if (locked.Count > 0)
            {
                await using var delete = new NpgsqlCommand(BatchDeleteSql, conn, tx);
                delete.Parameters.Add(Arg(locked.Select(l => l.Id).ToArray()));
                await using var r = await delete.ExecuteReaderAsync(ct);
                while (await r.ReadAsync(ct))
                    deleted++;
            }
            await tx.CommitAsync(ct);
            if (locked.Count < BatchDeleteChunk)
                return deleted;
            cursor = (locked[^1].At, locked[^1].Id);
        }
    }
}
```

- [x] **Step 5: Предпроверка и компактирование**

`src/PgBolt.Postgres/Schema/RetentionRunner.Jobs.cs`:
- записи `Precheck(PrecheckKind Kind, Guid[] Live)` и `Compaction(Candidate Candidate, Guid[] Live)`, `ApplyJobsAsync` и сигнатура `CompactAsync(Candidate c, Guid[] live, Accumulator acc, CancellationToken ct)` не меняются: `Live` — объединение `queue`, (а) и (б) предпроверки;
- в `PrecheckAsync` объявить `var live = new HashSet<Guid>();` вместо `var live = new List<Guid>();`; в первый пакет после команды `SELECT job_id FROM … queue …` добавить:

```csharp
                batch.BatchCommands.Add(Cmd(AwaitingParentsSql(batches: false, "$1", "$2", "$3"), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1)));
                batch.BatchCommands.Add(Cmd(AwaitingParentsSql(batches: true, "$1", "$2", "$3"), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1)));
                batch.BatchCommands.Add(Cmd(BatchMembersSql, Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1)));
```

  и чтение после строк `queue` (порядок команд важен: в READ COMMITTED у каждой свой снимок, и задача, ушедшая из `queue` в Failed / Cancelled между командой `queue` и (б), видна (б)):

```csharp
                for (var source = 0; source < 3; source++)       // (а): queue и batches; (б): члены batches
                {
                    await reader.NextResultAsync(ct);
                    while (await reader.ReadAsync(ct))
                        live.Add(reader.GetGuid(0));
                }
```

  итог `return new Precheck(kind, [.. live]);`, порог `live.Count > MaxLive` и расчёт `need` — без изменений (по объединению; каждый источник — `LIMIT T + 1`);
- в `CompactAsync` проверку под блокировкой и решение пакета Б заменить на:

```csharp
        var attached = false;
        var recheck = new HashSet<Guid>();
        var check = Cmd($"""
            SELECT to_regclass($1) = $2::oid
                     AND EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = $2::oid AND inhparent = '{_schema}.jobs'::regclass),
                   ARRAY(SELECT job_id FROM {_schema}.queue WHERE job_id >= $3 AND job_id < $4 LIMIT $5),
                   ARRAY({AwaitingParentsSql(batches: false, "$3", "$4", "$5")}),
                   ARRAY({AwaitingParentsSql(batches: true, "$3", "$4", "$5")})
            """, Arg(Qualified(name)), Arg(c.Table.Oid), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1));

        var outcome = await RunLockedAsync("jobs", check, days, async (reader, t) =>
        {
            if (!await reader.ReadAsync(t))
                return;
            attached = !reader.IsDBNull(0) && reader.GetBoolean(0);
            for (var i = 1; i <= 3; i++)
                recheck.UnionWith(reader.GetFieldValue<Guid[]>(i));
        }, () =>
        {
            if (!attached)
                return [];
            // §7.1: @ids — объединение предпроверки и повторной проверки. Набор предпроверки на этом пути непуст, поэтому
            // отсоединения нет даже при пустой повторной проверке: (б) под блокировкой не повторяется, а член, ставший живым
            // по (б) после предпроверки (отмена, окончательный fail), был в ней строкой queue и переносится через объединение.
            Guid[] ids = [.. live.Union(recheck)];
            if (ids.Length > MaxLive)
                return [];
            return CompactionCommands(c, remnant, ids);
        }, ct);
```

  и после `await HookAsync(RetentionStep.Committed, "jobs", days, ct);` удалить блок `if (ids.Length == 0) { acc.RecordDrop(Detached(c.Table), …); return true; }`: ветка отсоединения в пакете Б компактирования (часть 2 §5.3 «`ids` пуст → отсоединение») после части 5 недостижима и удаляется. Строки, переставшие быть живыми, копируются лишними и уходят следующим запуском (предпроверка пуста → отсоединение группой с `EXISTS` по трём диапазонам). Внешняя переменная `Guid[] ids = [];` больше не нужна (`ids` — локальная в делегате пакета Б). В summary `CompactAsync` уточнить: под блокировкой повторяются живые `queue` и (а), `@ids` — объединение с набором предпроверки (§7.1).

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~Retention"`
Expected: PASS — новые `RetentionWorkflowTests` (Direct и Bouncer), `RetentionWorkflowPlanTests` (Direct) и все прежние тесты retention (регрессия: без строк части 5 решения прежние; убранное отсоединение в транзакции компактирования, когда живые `queue` завершились между предпроверкой и блокировкой, прежними тестами не проверяется — такая партиция теперь компактируется и отсоединяется следующим запуском).

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Schema tests/PgBolt.Postgres.Tests/RetentionWorkflowTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(workflows): retention keeps workflow live rows and deletes old batches"
```

---
### Task 11: Генератор: метаданные шагов workflow и BOLT009 (§5.4)

**Files:**
- Modify: `src/PgBolt.Generators/KnownNames.cs`, `src/PgBolt.Generators/JobGenerator.cs`, `src/PgBolt.Generators/Models/EnqueueCallModel.cs`, `src/PgBolt.Generators/Analysis/AnalysisState.cs`, `src/PgBolt.Generators/Analysis/Diagnostics.cs`, `src/PgBolt.Generators/Analysis/PgBoltAnalyzer.cs`, `src/PgBolt.Generators/AnalyzerReleases.Unshipped.md`
- Create: `src/PgBolt.Generators/Analysis/WorkflowSubmitAnalysis.cs`
- Test: `tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs` (+ snapshot `MetadataGenerationTests.Workflow_call_sites_register_type_info_from_context#PgBolt.Metadata.g.verified.cs`), `tests/PgBolt.Generators.Tests/JsonCoverageTests.cs`, `tests/PgBolt.Generators.Tests/AnalyzerTests.cs`

**Interfaces:**
- Consumes: публичные интерфейсы `IWorkflowScope`, `IWorkflow`, `INodeStep`, `IBatchBuilder` (Task 2), `IJobClient.Workflow` (Task 5).
- Produces: `KnownNames.WorkflowScopeMetadata / NodeStepMetadata / BatchBuilderMetadata / WorkflowMetadata`, `KnownNames.IsWorkflowMethod`, `KnownNames.Implements`; `Diagnostics.WorkflowNotSubmitted` (BOLT009, Warning); `WorkflowSubmitAnalysis`.

- [x] **Step 1: Тесты генератора**

В `tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs` добавить:

```csharp
    /// <summary>Типы шагов из referenced-сборки: регистрируются только по местам типизированных вызовов workflow (часть 5 §5.4).</summary>
    private const string WorkflowContracts = """
        namespace Contracts;

        [PgBolt.Job("flow.fetch")] public sealed record FetchArgs(int Id);
        [PgBolt.Job("flow.parse")] public sealed record ParseArgs(int Id);
        [PgBolt.Job("flow.item")] public sealed record ItemArgs(int Id);
        [PgBolt.Job("flow.raw")] public sealed record RawArgs(int Id);
        """;

    private const string WorkflowProducer = """
        using System.Text.Json;
        using System.Threading.Tasks;
        using Contracts;
        using PgBolt;

        [assembly: PgBoltJsonContext(typeof(AppJson))]

        public static class Producer
        {
            public static async Task RunAsync(IJobClient client, JsonElement raw) =>
                await client.Workflow()
                    .Enqueue(new FetchArgs(1))
                    .ContinueWith(new ParseArgs(1))
                    .ContinueWithBatch(b => b.EnqueueMany(new[] { new ItemArgs(1), new ItemArgs(2) }))
                    .ContinueWith("flow.raw", raw)                 // нетипизированный шаг — метаданные не нужны
                    .SubmitAsync();
        }

        [System.Text.Json.Serialization.JsonSerializable(typeof(FetchArgs))]
        [System.Text.Json.Serialization.JsonSerializable(typeof(ParseArgs))]
        [System.Text.Json.Serialization.JsonSerializable(typeof(ItemArgs))]
        """;

    [Fact]
    public void Workflow_call_sites_register_types_from_referenced_assemblies()
    {
        var (driver, output) = GeneratorHarness.Run(WorkflowProducer + HandWrittenContext,
            library: GeneratorHarness.Library(WorkflowContracts, "Contracts"));

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("Register<global::Contracts.FetchArgs>", code);
        Assert.Contains("Register<global::Contracts.ParseArgs>", code);
        Assert.Contains("Register<global::Contracts.ItemArgs>", code);
        Assert.DoesNotContain("RawArgs", code);
        GeneratorHarness.AssertClean(output);
    }

    [Fact]
    public Task Workflow_call_sites_register_type_info_from_context()   // §12: snapshot метаданных Enqueue / ContinueWith / EnqueueMany
    {
        var (driver, output) = GeneratorHarness.Run(WorkflowProducer + HandWrittenContext,
            library: GeneratorHarness.Library(WorkflowContracts, "Contracts"));

        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public void Unrelated_generic_Enqueue_and_ContinueWith_are_ignored()
    {
        var (driver, _) = GeneratorHarness.Run("""
            using System.Collections.Generic;
            using System.Threading.Tasks;

            public static class Other
            {
                public static void Go(Queue<int> q, Task t)
                {
                    q.Enqueue(1);
                    t.ContinueWith<int>(_ => 1);
                }
            }
            """);

        Assert.Empty(GeneratorHarness.HintNames(driver));
    }
```

В `tests/PgBolt.Generators.Tests/JsonCoverageTests.cs` добавить:

```csharp
    [Fact]
    public Task Workflow_step_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public static class S
        {
            public static async Task Go(IJobClient c) =>
                await {|#0:c.Workflow().Enqueue(new A(1)).ContinueWith(new B(1))|}.SubmitAsync();

            public static async Task Many(IJobClient c) =>
                await c.Workflow().StartBatch(b => {|#1:b.EnqueueMany(new[] { new B(2) })|}).SubmitAsync();
        }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"),
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(1).WithArguments("B", "AppJson"));
```

В `tests/PgBolt.Generators.Tests/AnalyzerTests.cs` добавить:

```csharp
    private const string Workflows = """
        using System.Text.Json;
        using System.Threading.Tasks;
        using PgBolt;

        """;

    [Fact]
    public Task Workflow_typed_step_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public static class S { public static async Task Go(IJobClient c) => await c.Workflow().Enqueue(new A(1)).SubmitAsync(); }
        """,
        Diagnostic(Diagnostics.MissingJsonContext));

    [Fact]
    public Task Workflow_in_local_without_submit_BOLT009() => VerifyAsync(Workflows + """
        public static class S
        {
            public static void Go(IJobClient c, JsonElement a)
            {
                var wf = {|#0:c.Workflow()|};
                var fetch = wf.Enqueue("a.job", a);
                fetch.ContinueWith("b.job", a);
            }
        }
        """,
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(0));

    [Fact]
    public Task Workflow_chain_without_submit_BOLT009() => VerifyAsync(Workflows + """
        public static class S
        {
            public static void Go(IJobClient c, JsonElement a) => {|#0:c.Workflow()|}.Enqueue("a.job", a).ContinueWith("b.job", a);
        }
        """,
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(0));

    [Fact]
    public Task Workflow_submitted_through_any_step_has_no_diagnostics() => VerifyAsync(Workflows + """
        public static class S
        {
            public static async Task Chain(IJobClient c, JsonElement a) =>
                await c.Workflow().Enqueue("a.job", a).ContinueWith("b.job", a).SubmitAsync();

            public static async Task Local(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                var fetch = wf.Enqueue("a.job", a);
                fetch.ContinueWith("b.job", a);
                await fetch.SubmitAsync();
            }

            public static async Task Batches(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                wf.StartBatch(b => b.Enqueue("a.job", a)).ContinueWith("b.job", a);
                await wf.SubmitAsync();
            }
        }
        """);

    [Fact]
    public Task Workflow_escaping_the_method_suppresses_BOLT009() => VerifyAsync(Workflows + """
        public sealed class S
        {
            private IWorkflow? _field;
            public INodeStep? Property { get; set; }

            public IWorkflow Returned(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                wf.Enqueue("a.job", a);
                return wf;
            }

            public void Passed(IJobClient c, JsonElement a) => Consume(c.Workflow().Enqueue("a.job", a));

            public void Field(IJobClient c)
            {
                var wf = c.Workflow();
                _field = wf;
            }

            public void Prop(IJobClient c, JsonElement a) => Property = c.Workflow().Enqueue("a.job", a);

            private static void Consume(INodeStep step) { }
        }
        """);
```

- [x] **Step 2: Запустить — падают**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: FAIL — метаданные шагов workflow не регистрируются (`DoesNotContain`/`Contains`), нет `Diagnostics.WorkflowNotSubmitted`, BOLT001/BOLT006 для шагов не выдаются; snapshot-тест создаёт `.received.cs`.

- [x] **Step 3: Распознавание вызовов**

`src/PgBolt.Generators/KnownNames.cs` — добавить константы и методы:

```csharp
        public const string WorkflowScopeMetadata = "PgBolt.IWorkflowScope";
        public const string NodeStepMetadata = "PgBolt.INodeStep";
        public const string BatchBuilderMetadata = "PgBolt.IBatchBuilder";
        public const string WorkflowMetadata = "PgBolt.IWorkflow";

        /// <summary>
        /// Часть 5 §5.4: типизированные шаги workflow (Enqueue&lt;TArgs&gt;, ContinueWith&lt;TArgs&gt;, EnqueueMany&lt;TArgs&gt;) берут
        /// JsonTypeInfo из JobMetadata&lt;TArgs&gt;, как EnqueueAsync, — места их вызова тоже регистрируют метаданные.
        /// Методы, унаследованные IWorkflow от IWorkflowScope, распознаются по объявляющему интерфейсу (ContainingType = IWorkflowScope), поэтому отдельная проверка IWorkflow не нужна.
        /// </summary>
        public static bool IsWorkflowMethod(IMethodSymbol method, INamedTypeSymbol? scope, INamedTypeSymbol? node, INamedTypeSymbol? batchBuilder) =>
            method.IsGenericMethod
            && method.Name switch
            {
                "Enqueue" => Implements(method.ContainingType, scope),
                "ContinueWith" => Implements(method.ContainingType, node),
                "EnqueueMany" => Implements(method.ContainingType, batchBuilder),
                _ => false,
            };

        public static bool Implements(INamedTypeSymbol type, INamedTypeSymbol? contract) =>
            contract is not null
            && (SymbolEqualityComparer.Default.Equals(type, contract) || type.AllInterfaces.Contains(contract, SymbolEqualityComparer.Default));
```

`src/PgBolt.Generators/JobGenerator.cs` — в предикате `calls` оба списка имён дополнить: `or "Enqueue" or "ContinueWith" or "EnqueueMany"`.

`src/PgBolt.Generators/Models/EnqueueCallModel.cs` — условие `callSite` дополнить:

```csharp
                || KnownNames.IsWorkflowMethod(method,
                    compilation.GetTypeByMetadataName(KnownNames.WorkflowScopeMetadata),
                    compilation.GetTypeByMetadataName(KnownNames.NodeStepMetadata),
                    compilation.GetTypeByMetadataName(KnownNames.BatchBuilderMetadata));
```

(summary класса: «… и типизированные шаги workflow (часть 5)»).

`src/PgBolt.Generators/Analysis/AnalysisState.cs`: добавить поля `private readonly INamedTypeSymbol? _workflowScope, _nodeStep, _batchBuilder;`, параметры приватного конструктора и их заполнение в `TryCreate` (`compilation.GetTypeByMetadataName(KnownNames.WorkflowScopeMetadata)` и т. д.); в `AnalyzeInvocation` условие заменить на:

```csharp
            if (!KnownNames.IsClientMethod(method, _jobClient)
                && !KnownNames.IsRecurringMethod(method, _recurringManager, _options)
                && !KnownNames.IsWorkflowMethod(method, _workflowScope, _nodeStep, _batchBuilder))
                return;
```

- [x] **Step 4: BOLT009**

`src/PgBolt.Generators/Analysis/Diagnostics.cs` — добавить дескриптор и включить его в `All` последним:

```csharp
        public static readonly DiagnosticDescriptor WorkflowNotSubmitted = new(
            "BOLT009", "Workflow is never submitted",
            "The workflow created here is never submitted: none of its steps reaches SubmitAsync in this method, is returned, passed as an argument or stored in a field or property",
            Category, DiagnosticSeverity.Warning, isEnabledByDefault: true);
```

`src/PgBolt.Generators/AnalyzerReleases.Unshipped.md` — строка `BOLT009 | PgBolt | Warning | Diagnostics` после BOLT008.

`src/PgBolt.Generators/Analysis/WorkflowSubmitAnalysis.cs`:

```csharp
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace PgBolt.Generators.Analysis
{
    /// <summary>
    /// BOLT009 (часть 5 §5.4): результат IJobClient.Workflow(), сохранённый в локальную переменную или использованный цепочкой,
    /// ни одно значение потока которого (сам workflow, шаги от вызовов над значениями потока, локальные с ними) не дошло до
    /// SubmitAsync в том же методе. Возврат из метода, передача аргументом и присваивание полю/свойству подавляют диагностику.
    /// Анализ нечувствителен к порядку и ветвлениям — эвристика, а не поток управления.
    /// </summary>
    internal sealed class WorkflowSubmitAnalysis
    {
        private readonly INamedTypeSymbol _client;
        private readonly INamedTypeSymbol _workflow;
        private readonly INamedTypeSymbol _node;

        private WorkflowSubmitAnalysis(INamedTypeSymbol client, INamedTypeSymbol workflow, INamedTypeSymbol node)
        {
            _client = client;
            _workflow = workflow;
            _node = node;
        }

        public static WorkflowSubmitAnalysis? TryCreate(Compilation compilation)
        {
            var client = compilation.GetTypeByMetadataName(KnownNames.JobClientMetadata);
            var workflow = compilation.GetTypeByMetadataName(KnownNames.WorkflowMetadata);
            var node = compilation.GetTypeByMetadataName(KnownNames.NodeStepMetadata);
            return client is null || workflow is null || node is null ? null : new WorkflowSubmitAnalysis(client, workflow, node);
        }

        public void AnalyzeBlock(OperationBlockAnalysisContext ctx)
        {
            foreach (var block in ctx.OperationBlocks)
            {
                var all = block.Descendants().ToList();
                foreach (var root in all.OfType<IInvocationOperation>().Where(IsWorkflowCall))
                {
                    if ((StoredLocal(root) is not null || IsReceiver(root)) && !Escapes(root, all))
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.WorkflowNotSubmitted, root.Syntax.GetLocation()));
                }
            }
        }

        private bool IsWorkflowCall(IInvocationOperation invocation) =>
            invocation.TargetMethod.Name == "Workflow" && KnownNames.Implements(invocation.TargetMethod.ContainingType, _client);

        private bool IsFlowType(ITypeSymbol? type) =>
            type is INamedTypeSymbol named
            && (SymbolEqualityComparer.Default.Equals(named, _workflow) || KnownNames.Implements(named, _node));

        /// <summary>Замыкание потока от root; true — хотя бы одно значение потока отправлено или ушло из метода.</summary>
        private bool Escapes(IInvocationOperation root, List<IOperation> all)
        {
            var flow = new HashSet<IOperation> { root };
            var locals = new HashSet<ILocalSymbol>(SymbolEqualityComparer.Default);
            for (var changed = true; changed;)
            {
                changed = false;
                foreach (var op in all)
                {
                    if (flow.Contains(op))
                        continue;
                    var joins = op switch
                    {
                        IInvocationOperation call => call.Instance is { } receiver && flow.Contains(Unwrap(receiver))
                                                     && IsFlowType(call.TargetMethod.ReturnType),
                        ILocalReferenceOperation local => locals.Contains(local.Local),
                        _ => false,
                    };
                    if (joins)
                    {
                        flow.Add(op);
                        changed = true;
                    }
                }
                foreach (var op in flow.ToList())
                {
                    if (StoredLocal(op) is { } local && locals.Add(local))
                        changed = true;
                }
            }

            return flow.Any(op => Parent(op) switch
            {
                IReturnOperation => true,
                IArgumentOperation => true,
                ISimpleAssignmentOperation assignment => Unwrap(assignment.Value) == op
                                                         && assignment.Target is IFieldReferenceOperation or IPropertyReferenceOperation,
                IInvocationOperation call => call.TargetMethod.Name == "SubmitAsync" && call.Instance is { } r && Unwrap(r) == op,
                _ => false,
            });
        }

        private static bool IsReceiver(IOperation op) =>
            Parent(op) is IInvocationOperation call && call.Instance is { } receiver && Unwrap(receiver) == op;

        /// <summary>Локальная, в которую сохранено значение: инициализатор объявления или присваивание локальной.</summary>
        private static ILocalSymbol? StoredLocal(IOperation op) => Parent(op) switch
        {
            IVariableInitializerOperation { Parent: IVariableDeclaratorOperation declarator } => declarator.Symbol,
            ISimpleAssignmentOperation { Target: ILocalReferenceOperation local } assignment when Unwrap(assignment.Value) == op => local.Local,
            _ => null,
        };

        private static IOperation Unwrap(IOperation op)
        {
            while (op is IConversionOperation conversion)
                op = conversion.Operand;
            return op;
        }

        private static IOperation? Parent(IOperation op)
        {
            var parent = op.Parent;
            while (parent is IConversionOperation)
                parent = parent.Parent;
            return parent;
        }
    }
}
```

`src/PgBolt.Generators/Analysis/PgBoltAnalyzer.cs` — после `start.RegisterCompilationEndAction(state.OnCompilationEnd);`:

```csharp
                if (WorkflowSubmitAnalysis.TryCreate(start.Compilation) is { } workflows)
                    start.RegisterOperationBlockAction(workflows.AnalyzeBlock);
```

- [x] **Step 5: Snapshot**

Run: `dotnet test tests/PgBolt.Generators.Tests --filter "FullyQualifiedName~Workflow_call_sites_register_type_info_from_context"` — первый запуск падает и пишет `tests/PgBolt.Generators.Tests/MetadataGenerationTests.Workflow_call_sites_register_type_info_from_context#PgBolt.Metadata.g.received.cs`. Сверить его содержимое с эталоном ниже (кроме BOM в начале файла) и переименовать в `….verified.cs`:

```csharp
//HintName: PgBolt.Metadata.g.cs
// <auto-generated/>
#nullable enable

namespace PgBolt.Generated
{
    internal static class PgBoltMetadataInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        internal static void Initialize()
        {
            global::PgBolt.JobMetadataRegistry.Register<global::Contracts.FetchArgs>(new global::PgBolt.JobDescriptor("flow.fetch", "default", 10, (short)0, null), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::Contracts.FetchArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::Contracts.FetchArgs))!);
            global::PgBolt.JobMetadataRegistry.Register<global::Contracts.ItemArgs>(new global::PgBolt.JobDescriptor("flow.item", "default", 10, (short)0, null), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::Contracts.ItemArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::Contracts.ItemArgs))!);
            global::PgBolt.JobMetadataRegistry.Register<global::Contracts.ParseArgs>(new global::PgBolt.JobDescriptor("flow.parse", "default", 10, (short)0, null), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::Contracts.ParseArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::Contracts.ParseArgs))!);
        }
    }
}
```

Расхождение с эталоном — не править эталон под вывод, а `BLOCKED` с diff.

- [x] **Step 6: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Generators.Tests` → PASS. Затем `dotnet build PgBolt.slnx` → без ошибок и предупреждений (E2E-сборки идут через анализатор: их workflow-цепочки, появятся в Task 13, должны заканчиваться `SubmitAsync`).

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Generators tests/PgBolt.Generators.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(generators): workflow step metadata and BOLT009"
```

Конец фазы 3: `dotnet test PgBolt.slnx --filter "Category!=Load"` — зелёный.

---
### Task 12: Цикл `resolver` в `PgBolt.Server` (§6.4, §8, worker §6.3)

**Files:**
- Modify: `src/PgBolt.Server/PgBoltProcessingOptions.cs`, `src/PgBolt.Server/Runtime/IServerStore.cs`, `src/PgBolt.Server/Runtime/PgBoltServerStore.cs`, `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs`, `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`, `src/PgBolt.Server/Runtime/RetentionReport.cs`
- Modify (тесты): `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`, `tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs`, `tests/PgBolt.Server.Tests/Unit/HousekeepingTests.cs`, `tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs`, `tests/PgBolt.Server.Tests/Unit/RetentionReportTests.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/ResolverLoopTests.cs`, `tests/PgBolt.Server.Tests/Integration/WorkflowResolverLoopTests.cs`

**Interfaces:**
- Consumes: `CountCompletionsAsync`, `ResolveAsync`, `OldestCompletionAgeAsync`, `CompletionCountResult`, `ResolveResult` (Task 6); `CancelMembersAsync`, `CancelMembersResult` (Task 7); `ICompletionSignal`, `InProcessCompletionSignal` (Task 3); `RetentionResult.DeletedBatches` (Task 10); `AsyncAutoResetEvent`, `MonoClock` (worker).
- Produces: `PgBoltProcessingOptions.ResolverInterval` (1 с, проверка как у интервалов); члены `IServerStore` из «Контракта»; `ProcessingMaintenance(…, RecurringDeclarations? recurring = null, ICompletionSignal? completions = null)` с константами и `ResolveOnceAsync`; цикл `resolver` при `RunMaintenance`, пауза `ResolverInterval` ±20 % или до `ICompletionSignal`; лог удалённых `batches`.

- [x] **Step 1: Фейк хранилища**

В `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs` добавить:

```csharp
    public Func<int, CancellationToken, Task<CompletionCountResult>> OnCount { get; set; } =
        (_, _) => Task.FromResult(CompletionCountResult.Empty);

    public Func<int, int, TimeSpan, CancellationToken, Task<ResolveResult>> OnResolve { get; set; } =
        (_, _, _, _) => Task.FromResult(ResolveResult.Empty);

    public Func<int, int, int, TimeSpan, CancellationToken, Task<CancelMembersResult>> OnCancelMembers { get; set; } =
        (_, _, _, _, _) => Task.FromResult(CancelMembersResult.Empty);

    public Func<CancellationToken, Task<TimeSpan?>> OnOldest { get; set; } = _ => Task.FromResult<TimeSpan?>(null);

    public Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Count", Detail: batchSize));
        return OnCount(batchSize, ct);
    }

    public Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Resolve", Detail: (batchSize, rowBudget, retryDelay)));
        return OnResolve(batchSize, rowBudget, retryDelay, ct);
    }

    public Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay,
        CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("CancelMembers", Detail: (portion, rowBudget, maxTransactions, retryDelay)));
        return OnCancelMembers(portion, rowBudget, maxTransactions, retryDelay, ct);
    }

    public Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Oldest"));
        return OnOldest(ct);
    }
```

- [x] **Step 2: Тесты**

`tests/PgBolt.Server.Tests/Unit/ResolverLoopTests.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class ResolverLoopTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly InProcessCompletionSignal _completions = new();
    private readonly ListLoggerFactory _logs = new();
    private readonly PgBoltProcessingOptions _options = new() { RunScheduler = false };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create() =>
        new(_store, _wakeup, _options, new FakeMaintenance(), new PgBoltOptions(), new MonoClock(_time), _logs.CreateLogger("test"),
            recurring: null, completions: _completions)
        {
            NextRandom = () => 0.5,
        };

    [Fact]
    public async Task Iteration_repeats_while_work_remains_and_signals_launched_queues()   // §6.4
    {
        var signals = new ConcurrentQueue<string>();
        _wakeup.Subscribe("a", () => signals.Enqueue("a"));
        var counts = new Queue<CompletionCountResult>(
        [
            CompletionCountResult.Empty with { Captured = 500 },
            CompletionCountResult.Empty with { Captured = 3, Finished = 1 },
        ]);
        _store.OnCount = (_, _) => Task.FromResult(counts.Count > 0 ? counts.Dequeue() : CompletionCountResult.Empty);
        _store.OnResolve = (_, _, _, _) => Task.FromResult(ResolveResult.Empty with { Captured = 1, Queues = ["a"] });

        Assert.Equal(506, await Create().ResolveOnceAsync(Ct));

        Assert.Equal(3, _store.CallsOf("Count").Count());
        Assert.Equal(["a", "a", "a"], signals);
        Assert.All(_store.CallsOf("Count"), c => Assert.Equal(500, (int)c.Detail!));
        Assert.All(_store.CallsOf("Resolve"), c => Assert.Equal((500, 5000, TimeSpan.FromSeconds(1)), ((int, int, TimeSpan))c.Detail!));
        Assert.All(_store.CallsOf("CancelMembers"),
            c => Assert.Equal((1000, 5000, 100, TimeSpan.FromSeconds(1)), ((int, int, int, TimeSpan))c.Detail!));
    }

    [Theory]
    [InlineData("walk-full")]
    [InlineData("new-signals")]
    [InlineData("budget")]
    [InlineData("cancel-portion")]
    public async Task Each_kind_of_remaining_work_repeats_the_iteration(string kind)   // §6.4: следующая итерация сразу
    {
        var first = true;
        _store.OnResolve = (_, _, _, _) => Task.FromResult(!first ? ResolveResult.Empty : kind switch
        {
            "walk-full" => ResolveResult.Empty with { Captured = 500 },
            "new-signals" => ResolveResult.Empty with { NewSignals = 1 },
            "budget" => ResolveResult.Empty with { BudgetExhausted = true },
            _ => ResolveResult.Empty,
        });
        _store.OnCancelMembers = (_, _, _, _, _) =>
        {
            var result = first && kind == "cancel-portion" ? CancelMembersResult.Empty with { Portions = 1 } : CancelMembersResult.Empty;
            first = false;
            return Task.FromResult(result);
        };

        await Create().ResolveOnceAsync(Ct);

        Assert.Equal(2, _store.CallsOf("Count").Count());
    }

    [Fact]
    public async Task Completion_signal_wakes_resolver_before_its_interval()   // §6.4: ICompletionSignal
    {
        _options.ResolverInterval = TimeSpan.FromMinutes(1);
        var maintenance = Create();
        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Count").Count() == 1 && maintenance.IdleWaits >= 4,
            "promoter, sweeper, housekeeping and resolver are waiting");

        _completions.Signal();

        await Eventually.True(() => _store.CallsOf("Count").Count() == 2, "the signal wakes the resolver without advancing time");
        await maintenance.StopAsync();
    }

    [Fact]
    public async Task Lag_warning_is_checked_at_most_once_a_minute()   // §8
    {
        _store.OnOldest = _ => Task.FromResult<TimeSpan?>(TimeSpan.FromSeconds(30));   // > 10 × ResolverInterval
        var maintenance = Create();

        await maintenance.ResolveOnceAsync(Ct);
        await maintenance.ResolveOnceAsync(Ct);
        Assert.Single(_store.CallsOf("Oldest"));
        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("oldest workflow completion signal"));

        _time.Advance(TimeSpan.FromMinutes(1));
        await maintenance.ResolveOnceAsync(Ct);

        Assert.Equal(2, _store.CallsOf("Oldest").Count());
        Assert.Equal(2, _logs.At(LogLevel.Warning).Count(e => e.Message.Contains("oldest workflow completion signal")));
    }

    [Fact]
    public async Task Young_signals_do_not_warn()
    {
        _store.OnOldest = _ => Task.FromResult<TimeSpan?>(TimeSpan.FromSeconds(5));

        await Create().ResolveOnceAsync(Ct);

        Assert.DoesNotContain(_logs.At(LogLevel.Warning), e => e.Message.Contains("oldest workflow completion signal"));
    }

    [Fact]
    public async Task Missing_batches_are_logged_at_debug()   // §6.4 Т1
    {
        var batch = Guid.NewGuid();
        var first = true;
        _store.OnCount = (_, _) =>
        {
            var result = first ? CompletionCountResult.Empty with { Captured = 1, MissingBatches = [batch] } : CompletionCountResult.Empty;
            first = false;
            return Task.FromResult(result);
        };

        await Create().ResolveOnceAsync(Ct);

        Assert.Contains(_logs.At(LogLevel.Debug), e => e.Message.Contains(batch.ToString()));
    }

    [Fact]
    public async Task Resolver_runs_only_with_maintenance()   // §8: только RunMaintenance = true
    {
        _options.RunMaintenance = false;
        _options.RunScheduler = true;
        var maintenance = Create();

        maintenance.Start();
        await Eventually.True(() => _store.CallsOf("Tick").Any(), "scheduler ticks");
        await maintenance.StopAsync();

        Assert.Empty(_store.CallsOf("Count"));
    }
}
```

`tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs` — в `Defaults_match_spec` после проверок части 4 добавить `Assert.Equal(TimeSpan.FromSeconds(1), p.ResolverInterval);`, и тест:

```csharp
    [Fact]
    public void Resolver_interval_is_validated_like_other_intervals()
    {
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { ResolverInterval = TimeSpan.Zero }.Validate());
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { ResolverInterval = TimeSpan.FromDays(2) }.Validate());
        new PgBoltProcessingOptions { ResolverInterval = TimeSpan.FromSeconds(10) }.Validate();
    }
```

**Существующие тесты циклов обслуживания.** С `RunMaintenance = true` `ProcessingMaintenance.Start()` запускает четвёртый цикл обслуживания — `resolver`; он тоже встаёт на паузу (фейк возвращает пустые результаты), поэтому ожидания «все циклы ждут» считают четыре паузы:
- `tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs`: в `Loop_error_is_logged_and_retried_after_interval` и `Stop_ends_both_loops` заменить `maintenance.IdleWaits == 3, "promoter, sweeper and housekeeping are waiting"` на `maintenance.IdleWaits == 4, "promoter, sweeper, housekeeping and resolver are waiting"`; комментарий у `_options` — «IdleWaits == 4 считает их паузы и паузу resolver'а (часть 5)»;
- `tests/PgBolt.Server.Tests/Unit/HousekeepingTests.cs`: в `Claim_failure_is_logged_by_the_loop_and_retried_after_poll_interval` — `maintenance.IdleWaits == 4, "promoter, sweeper, housekeeping and resolver wait for their intervals"`; тот же комментарий у `_options`.

`tests/PgBolt.Server.Tests/Unit/RetentionReportTests.cs` — добавить:

```csharp
    [Fact]
    public void Deleted_batches_are_reported()   // часть 5 §7.1
    {
        var logger = new TestLogger();

        RetentionReport.Write(logger, new RetentionResult([], [], 0, [], [], 3, DeletedBatches: 7), new PgBoltOptions());

        Assert.Contains(logger.At(LogLevel.Information), e => e.Message.Contains("deleted 7 finished batches rows"));
    }
```

(если в файле нет `using PgBolt.Postgres.Schema;` / `using PgBolt.Postgres;` / `using Microsoft.Extensions.Logging;` — добавить).

`tests/PgBolt.Server.Tests/Integration/WorkflowResolverLoopTests.cs`:

```csharp
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Resolver с ResolverInterval = 10 с над хранилищем без ICompletionSignal: ack в нём resolver не будит.</summary>
internal static class WorkflowResolverHarness
{
    public static (ProcessingMaintenance Maintenance, PgBoltStore Store) Create(TestDb db)
    {
        var store = new PgBoltStore(db.DataSource, db.Schema, usePreparedStatements: false);
        var maintenance = new ProcessingMaintenance(new PgBoltServerStore(store), new InProcessWakeupSignal(),
            new PgBoltProcessingOptions { ResolverInterval = TimeSpan.FromSeconds(10) }, new FakeMaintenance(), new PgBoltOptions(),
            new MonoClock(TimeProvider.System), NullLogger.Instance);
        return (maintenance, store);
    }
}

/// <summary>§12 Batches, §1 «Задержка»: многошаговый путь и отмена — повторами итерации, без пауз и без in-process сигнала.</summary>
public abstract class WorkflowResolverLoopTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private (ProcessingMaintenance Maintenance, PgBoltStore Store) Resolver() => WorkflowResolverHarness.Create(Db);

    [Fact]
    public async Task Member_batch_continuation_and_its_nested_batch_resolve_in_one_iteration()
    {
        IJobStep? x = null, y = null;
        var outer = Client.Workflow().StartBatch(b => x = b.Enqueue(new SuccessArgs(1)));
        outer.ContinueWithBatch(c => c.StartBatch(n => y = n.Enqueue(new SuccessArgs(2))));
        await outer.SubmitAsync(Ct);
        var (maintenance, store) = Resolver();
        var server = Guid.NewGuid();
        var job = Assert.Single(await store.DequeueAsync(TestJobs.Queue, 10, server, TimeSpan.FromMinutes(1), Ct));
        Assert.Equal(x!.Id.Value, job.Id);
        Assert.Equal(1, await store.AckAsync(server, [new LeaseRef(job.Id, job.LeaseId)], Ct));

        await maintenance.ResolveOnceAsync(Ct);   // член → batch → OnSuccess batch-продолжение → его вложенный batch → запуск

        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND await_condition IS NULL FROM {schema}.queue WHERE job_id = @id",
            ("id", y!.Id.Value)));
    }

    [Fact]
    public async Task Small_batch_cancel_finishes_in_one_resolver_call()   // §12 CancelBatchAsync: без пауз
    {
        var batch = (await Client.Workflow().StartBatch(b => b.EnqueueMany(new[] { new SuccessArgs(1), new SuccessArgs(2) })).SubmitAsync(Ct)).Batches[0];
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));

        await Resolver().Maintenance.ResolveOnceAsync(Ct);

        Assert.Equal(BatchState.Cancelled, (await Client.GetBatchAsync(batch, Ct))!.State);
    }
}

public sealed class WorkflowResolverLoopTests_Direct(PostgresFixture pg) : WorkflowResolverLoopTests(pg, ConnectionMode.Direct);
public sealed class WorkflowResolverLoopTests_Bouncer(PostgresFixture pg) : WorkflowResolverLoopTests(pg, ConnectionMode.Bouncer);

/// <summary>§12 CancelBatchAsync: batch с 5000 вложенными отменяется итерациями без пауз. Только Direct, Slow.</summary>
[Trait("Category", "Slow")]
public sealed class WorkflowResolverLoadTests(PostgresFixture pg) : ServerIntegrationTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Batch_with_5000_nested_batches_is_cancelled_in_one_resolver_call()
    {
        var batch = (await Client.Workflow().StartBatch(b =>
        {
            for (var i = 0; i < 5000; i++)
            {
                var n = i;
                b.StartBatch(nested => nested.Enqueue(new SuccessArgs(n)));
            }
        }).SubmitAsync(Ct)).Batches[0];
        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch, Ct));

        await WorkflowResolverHarness.Create(Db).Maintenance.ResolveOnceAsync(Ct);

        var info = (await Client.GetBatchAsync(batch, Ct))!;
        Assert.Equal((BatchState.Cancelled, 5000), (info.State, info.Cancelled));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions"));
    }
}
```

- [x] **Step 3: Запустить — не компилируется**

Run: `dotnet build PgBolt.slnx`
Expected: FAIL — `FakeServerStore` реализует методы, которых нет в `IServerStore`; нет `ResolverInterval`, `ResolveOnceAsync`, параметра `completions`.

- [x] **Step 4: Опции**

`src/PgBolt.Server/PgBoltProcessingOptions.cs`:
- XML-doc `RunMaintenance` дополнить: «Workflows (часть 5): продолжения и batches разрешает resolver, который работает только в процессах с AddPgBoltServer и RunMaintenance = true; если таких нет, сигналы копятся и продолжения не стартуют (§8).»;
- свойство:

```csharp
    /// <summary>
    /// Пауза resolver'а workflow (±20 %, часть 5 §6.4); финализация в процессе с resolver'ом будит его сразу (in-process сигнал).
    /// Также задержка повтора для сигналов, чьих детей держат чужие транзакции (retry_after).
    /// </summary>
    public TimeSpan ResolverInterval { get; set; } = TimeSpan.FromSeconds(1);
```

- в `Validate()`: `Check(ResolverInterval, nameof(ResolverInterval));`.

- [x] **Step 5: `IServerStore`**

`IServerStore.cs` — добавить члены из «Контракта» (`CountCompletionsAsync`, `ResolveAsync`, `CancelMembersAsync`, `OldestCompletionAgeAsync`). `PgBoltServerStore.cs` — пробросы:

```csharp
    public Task<CompletionCountResult> CountCompletionsAsync(int batchSize, CancellationToken ct) =>
        _store.CountCompletionsAsync(batchSize, ct);

    public Task<ResolveResult> ResolveAsync(int batchSize, int rowBudget, TimeSpan retryDelay, CancellationToken ct) =>
        _store.ResolveAsync(batchSize, rowBudget, retryDelay, ct);

    public Task<CancelMembersResult> CancelMembersAsync(int portion, int rowBudget, int maxTransactions, TimeSpan retryDelay,
        CancellationToken ct) =>
        _store.CancelMembersAsync(portion, rowBudget, maxTransactions, retryDelay, ct);

    public Task<TimeSpan?> OldestCompletionAgeAsync(CancellationToken ct) => _store.OldestCompletionAgeAsync(ct);
```

- [x] **Step 6: `ProcessingMaintenance`**

Первичный конструктор — последний параметр `ICompletionSignal? completions = null` (после `RecurringDeclarations? recurring = null`); summary класса дополнить: «resolver workflow (часть 5 §6.4)». Добавить члены:

```csharp
    internal const int ResolverBatchSize = 500;
    internal const int ResolverRowBudget = 5000;
    internal const int CancelMembersPortion = 1000;
    internal const int CancelMembersMaxTransactions = 100;
    internal static readonly TimeSpan ResolverLagCheckInterval = TimeSpan.FromMinutes(1);

    private readonly AsyncAutoResetEvent _resolverWake = new();
    private IDisposable? _completionSubscription;
    private long? _lastLagCheck;
```

В `Start()` внутри блока `if (options.RunMaintenance)` после housekeeping:

```csharp
            _completionSubscription = completions?.Subscribe(_resolverWake.Set);
            loops.Add(Task.Run(() => LoopAsync("resolver", ResolveOnceAsync, options.ResolverInterval, _resolverWake)));
```

`StopAsync()`:

```csharp
    public Task StopAsync()
    {
        _completionSubscription?.Dispose();
        _stop.Cancel();
        return _loops;
    }
```

Методы:

```csharp
    /// <summary>
    /// Часть 5 §6.4: итерация — учёт (Т1), обход (Т2), CancelMembers; следующая — сразу, если захвачен полный пакет, созданы новые
    /// сигналы, исчерпан бюджет строк или выполнена порция отмены: многошаговый путь не добавляет интервалов.
    /// </summary>
    internal async Task<int> ResolveOnceAsync(CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var counted = await store.CountCompletionsAsync(ResolverBatchSize, ct);
            foreach (var batch in counted.MissingBatches)
                logger.LogDebug("PgBolt: completion of a member of batch {BatchId} is not counted: the batch row was removed by retention.", batch);
            var walked = await store.ResolveAsync(ResolverBatchSize, ResolverRowBudget, options.ResolverInterval, ct);
            foreach (var queue in walked.Queues)
                wakeup.Signal(queue);
            var cancelled = await store.CancelMembersAsync(CancelMembersPortion, ResolverRowBudget, CancelMembersMaxTransactions,
                options.ResolverInterval, ct);
            total += counted.Captured + walked.Captured + cancelled.Portions;
            await CheckResolverLagAsync(ct);

            var again = counted.Captured == ResolverBatchSize || counted.Finished > 0
                        || walked.Captured == ResolverBatchSize || walked.NewSignals > 0 || walked.BudgetExhausted
                        || cancelled.Portions > 0;
            if (!again)
                return total;
        }
    }

    /// <summary>§8: Warning, если старейший сигнал старше 10 × ResolverInterval; проверка — не чаще раза в минуту на под.</summary>
    private async Task CheckResolverLagAsync(CancellationToken ct)
    {
        if (_lastLagCheck is { } last && clock.Elapsed(last) < ResolverLagCheckInterval)
            return;
        _lastLagCheck = clock.Now;
        if (await store.OldestCompletionAgeAsync(ct) is { } age && age > 10 * options.ResolverInterval)
            logger.LogWarning(
                "PgBolt: the oldest workflow completion signal is {Age} old (ResolverInterval {Interval}); continuations are delayed. " +
                "Check that a process with RunMaintenance = true is running and that no long user transaction holds workflow rows (After).",
                age, options.ResolverInterval);
    }
```

`LoopAsync` — добавить параметр `AsyncAutoResetEvent? wake = null` и заменить создание паузы:

```csharp
                var delay = interval * (1 + IntervalJitter * (2 * NextRandom() - 1));
                var pause = wake is null ? Task.Delay(delay, clock.Time, ct) : wake.WaitAsync(delay, clock.Time, ct);
                Interlocked.Increment(ref _idleWaits);
                await pause;
```

`src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`: в создании `ProcessingMaintenance` добавить последний аргумент `_services.GetService<ICompletionSignal>()`.

`src/PgBolt.Server/Runtime/RetentionReport.cs` — после лога удалённых партиций:

```csharp
        if (result.DeletedBatches > 0)
            logger.LogInformation("PgBolt retention: deleted {Count} finished batches rows older than JobsRetention.", result.DeletedBatches);
```

- [x] **Step 7: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Unit"` → PASS (включая `ProcessingMaintenanceTests` и `HousekeepingTests` с четырьмя паузами).
Затем: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~WorkflowResolverLoop|FullyQualifiedName~WorkflowResolverLoad|FullyQualifiedName~MaintenanceTests"` → PASS.

- [x] **Step 8: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): workflow resolver loop"
```

---
### Task 13: Интеграция с сервером и E2E

**Files:**
- Modify (тесты): `tests/PgBolt.Server.Tests/Integration/TestJobs.cs` (`JobRun`: ссылки workflow)
- Test: `tests/PgBolt.Server.Tests/Integration/WorkflowTests.cs`, `tests/E2E/PgBolt.E2E.Tests/WorkflowEndToEndTests.cs`

**Interfaces:**
- Consumes: всё из Task 1–12; `ServerIntegrationTestBase.StartHostAsync`, `WaitForStateAsync`, `WaitUntilAsync`, `JobStateAsync`, `JobProbe.Runs(JobId)`, `DbState`; E2E — `PgBoltJobRegistry.Register` трёх сборок, `PingProbe`, `PongProbe`, `LocalProbe`.
- Produces: `JobRun.ParentId : JobId?`, `JobRun.ParentBatchId : BatchId?`, `JobRun.BatchId : BatchId?` (тестовая инфраструктура).

- [x] **Step 1: `JobRun` фиксирует ссылки workflow**

В `TestJobs.cs` в класс `JobRun` добавить:

```csharp
    public JobId? ParentId { get; init; }
    public BatchId? ParentBatchId { get; init; }
    public BatchId? BatchId { get; init; }
```

и в `JobProbe.RunAsync` инициализатор прогона дополнить:

```csharp
            ParentId = context.ParentId,
            ParentBatchId = context.ParentBatchId,
            BatchId = context.BatchId,
```

- [x] **Step 2: Интеграционные тесты сервера**

`tests/PgBolt.Server.Tests/Integration/WorkflowTests.cs`:

```csharp
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Часть 5 через сервер: resolver процесса будится ack'ом своего сервера; JobContext / JobInfo / BatchInfo.</summary>
public abstract class WorkflowTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task Chain_with_fan_out_runs_through_server_and_exposes_links()   // §5.3, §12 API: JobContext
    {
        await StartHostAsync();
        var first = Client.Workflow().Enqueue(new SuccessArgs(1));
        var second = first.ContinueWith(new SuccessArgs(2));
        IReadOnlyList<IJobStep> members = [];
        var fan = second.ContinueWithBatch(b => members = b.EnqueueMany(new[] { new SuccessArgs(3), new SuccessArgs(4), new SuccessArgs(5) }));
        var report = fan.ContinueWith(new SuccessArgs(6), ContinuationCondition.OnAnyFinished);
        await first.SubmitAsync(Ct);

        await WaitForStateAsync(report.Id, DbState.Succeeded, Timeout);

        var secondRun = Assert.Single(Probe.Runs(second.Id));
        Assert.Equal(first.Id, secondRun.ParentId);
        Assert.Null(secondRun.ParentBatchId);
        Assert.Null(secondRun.BatchId);
        foreach (var member in members)
        {
            var run = Assert.Single(Probe.Runs(member.Id));
            Assert.Equal(((JobId?)null, (BatchId?)null, (BatchId?)fan.Id), (run.ParentId, run.ParentBatchId, run.BatchId));
        }
        var reportRun = Assert.Single(Probe.Runs(report.Id));
        Assert.Equal(((JobId?)null, (BatchId?)fan.Id, (BatchId?)null), (reportRun.ParentId, reportRun.ParentBatchId, reportRun.BatchId));

        var batch = (await Client.GetBatchAsync(fan.Id, Ct))!;
        Assert.Equal((BatchState.Succeeded, 3, 3), (batch.State, batch.Total, batch.Succeeded));
        Assert.Equal((second.Id, ContinuationCondition.OnSuccess), (batch.ParentId!.Value, batch.Condition!.Value));
        var info = (await Client.GetAsync(report.Id, Ct))!;
        Assert.Equal((fan.Id, ContinuationCondition.OnAnyFinished), (info.ParentBatchId!.Value, info.Continuation!.Value));
    }

    [Fact]
    public async Task Failed_member_runs_OnAnyFinished_and_holds_OnSuccess()   // §2.2, §2.3
    {
        await StartHostAsync();
        var batch = Client.Workflow().StartBatch(b =>
        {
            b.Enqueue(new AbortArgs("no retry"));
            b.Enqueue(new SuccessArgs(1));
        });
        var any = batch.ContinueWith(new SuccessArgs(2), ContinuationCondition.OnAnyFinished);
        var success = batch.ContinueWith(new SuccessArgs(3));
        await batch.SubmitAsync(Ct);

        await WaitForStateAsync(any.Id, DbState.Succeeded, Timeout);

        Assert.Equal(BatchState.Failed, (await Client.GetBatchAsync(batch.Id, Ct))!.State);
        Assert.Equal(JobState.Awaiting, (await Client.GetAsync(success.Id, Ct))!.State);
    }

    [Fact]
    public async Task Cancel_batch_stops_running_member_through_heartbeat()   // §6.5, §12 CancelBatchAsync
    {
        await StartHostAsync();
        IJobStep? blocked = null, waiting = null;
        var batch = Client.Workflow().StartBatch(b =>
        {
            blocked = b.Enqueue(new BlockArgs(1));
            waiting = blocked.ContinueWith(new SuccessArgs(1));
        });
        var any = batch.ContinueWith(new SuccessArgs(2), ContinuationCondition.OnAnyFinished);
        var success = batch.ContinueWith(new SuccessArgs(3));
        await batch.SubmitAsync(Ct);
        await WaitUntilAsync(() => Probe.RunCount(blocked!.Id) == 1, Timeout, "the member runs");

        Assert.Equal(BatchCancelResult.CancellationRequested, await Client.CancelBatchAsync(batch.Id, Ct));

        await WaitForStateAsync(any.Id, DbState.Succeeded, Timeout);
        Assert.True(Assert.Single(Probe.Runs(blocked!.Id)).WasCancelled);
        Assert.Equal(DbState.Cancelled, await JobStateAsync(blocked.Id));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(waiting!.Id));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(success.Id));
        Assert.Equal(BatchState.Cancelled, (await Client.GetBatchAsync(batch.Id, Ct))!.State);
    }
}

public sealed class WorkflowTests_Direct(PostgresFixture pg) : WorkflowTests(pg, ConnectionMode.Direct);
public sealed class WorkflowTests_Bouncer(PostgresFixture pg) : WorkflowTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 3: E2E со сгенерированными метаданными**

`tests/E2E/PgBolt.E2E.Tests/WorkflowEndToEndTests.cs`:

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.E2E.Contracts;
using PgBolt.E2E.JobsA;
using PgBolt.E2E.JobsB;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

/// <summary>Часть 5 E2E: типизированные шаги workflow (Enqueue / ContinueWith / EnqueueMany) с метаданными генератора.</summary>
public abstract class WorkflowEndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private static readonly Guid Server = Guid.NewGuid();
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Typed_workflow_runs_end_to_end_with_generated_metadata()
    {
        var run = Guid.NewGuid().ToString("N");
        var (ping, pong) = ($"flow-{run}", Random.Shared.Next());
        int[] locals = [Random.Shared.Next(), Random.Shared.Next()];
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register);
            o.AddJobs(PgBolt.E2E.JobsB.PgBoltJobRegistry.Register);
            o.AddJobs(PgBoltJobRegistry.Register);
        });
        await using var sp = services.BuildServiceProvider();
        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);
        var client = sp.GetRequiredService<IJobClient>();
        var store = sp.GetRequiredService<PgBoltStore>();
        var registry = sp.GetRequiredService<JobRegistry>();
        await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), "e2e-server", Ct);

        var first = client.Workflow().Enqueue(new PingArgs(ping));
        var second = first.ContinueWith(new PongArgs(pong));
        var fan = second.ContinueWithBatch(b => b.EnqueueMany(locals.Select(n => new LocalArgs(n)).ToArray()));
        var result = await fan.SubmitAsync(Ct);

        var processed = 0;
        for (var round = 0; round < 50 && processed < result.Jobs.Count; round++)
        {
            foreach (var job in await store.DequeueAsync("e2e", 10, Server, TimeSpan.FromMinutes(1), Ct))
            {
                Assert.True(registry.TryGet(job.Type, out var executor), job.Type);
                await using var scope = sp.CreateAsyncScope();
                var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts,
                    WorkflowCodes.ParentJob(job.Continuation, job.ParentId), job.BatchId is { } b ? new BatchId(b) : null,
                    NullLogger.Instance, parentBatchId: WorkflowCodes.ParentBatch(job.Continuation, job.ParentId));
                await executor.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes(job.ArgsJson), context, Ct);
                processed += await store.AckAsync(Server, [new LeaseRef(job.Id, job.LeaseId)], Ct);
            }
            await store.CountCompletionsAsync(500, Ct);                      // процесс без PgBolt.Server: проход resolver'а вручную
            await store.ResolveAsync(500, 5000, TimeSpan.FromSeconds(1), Ct);
        }

        Assert.Equal(result.Jobs.Count, processed);
        Assert.Contains(ping, PingProbe.Messages);
        Assert.Contains(pong, PongProbe.Values);
        Assert.All(locals, n => Assert.Contains(n, LocalProbe.Values));
        Assert.Equal(first.Id, (await client.GetAsync(second.Id, Ct))!.ParentId);
        Assert.Equal(BatchState.Succeeded, (await client.GetBatchAsync(fan.Id, Ct))!.State);
    }
}

public sealed class WorkflowEndToEndTests_Direct(PostgresFixture pg) : WorkflowEndToEndTests(pg, ConnectionMode.Direct);
public sealed class WorkflowEndToEndTests_Bouncer(PostgresFixture pg) : WorkflowEndToEndTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 4: Запустить — проходят**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~WorkflowTests"` и `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~WorkflowEndToEndTests"`
Expected: PASS (Direct и Bouncer); сборка E2E без BOLT009 (цепочки доходят до `SubmitAsync`).

- [x] **Step 5: Commit**

```bash
git add tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(workflows): server integration and end-to-end scenarios"
```

---
### Task 14: Документация и полный прогон

**Files:**
- Modify: `CHANGELOG.md`, `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md`, `docs/superpowers/specs/2026-10-02-pgbolt-workflows-design.md` (строка статуса; лог Debug в §6.6 и §7.1 — отклонение 3), `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md`, `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md`, `docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md`, `docs/superpowers/specs/2026-09-30-pgbolt-scheduler-design.md` (§9 спеки части 5: изменения других частей)

- [x] **Step 1: CHANGELOG**

В `CHANGELOG.md`, раздел `## Unreleased`:
- в `### Breaking changes` добавить:

```markdown
- Workflows (часть 5): в начальную миграцию `V001` добавлены `batches`, `completions` и колонки `queue` / `jobs`; схемы сборок
  части 4 не обновляются — старт падает с просьбой пересоздать схему (`DROP SCHEMA … CASCADE`).
- `JobContext.BatchId`: `Guid?` → `BatchId?`; новый необязательный параметр конструктора `parentBatchId`. `JobContext.ParentId`
  заполняется только для родителя-задачи продолжения.
- `IJobClient`: новые члены `Workflow`, `CancelBatchAsync`, `GetBatchAsync` — пользовательские реализации и моки должны их реализовать.
- `jobs.state = 1` больше не используется: `JobState.Awaiting` выводится из строки `queue` с `await_condition`.
- `PgBoltProcessingOptions.RunMaintenance = true` запускает и resolver workflow: продолжения и batches разрешаются только в процессах
  с `AddPgBoltServer` и `RunMaintenance = true`.
```

- в `### Added`:

```markdown
- Workflows (часть 5): fluent API `IJobClient.Workflow()` — `Enqueue`, `StartBatch`, `After(JobId | BatchId)`, `ContinueWith`,
  `ContinueWithBatch` (`ContinuationCondition.OnSuccess` / `OnAnyFinished`), `EnqueueMany` в теле batch, `SubmitAsync` одной
  транзакцией (`WorkflowOptions.Transaction` — точка сохранения); вложенные batches и batch-продолжения; `CancelBatchAsync`,
  `GetBatchAsync` (`BatchInfo`, `BatchState`, `BatchCancelResult`); `ParentNotFoundException`, `UniqueKeyConflictException`;
  `JobInfo.ParentId`, `ParentBatchId`, `BatchId`, `Continuation`, `JobState.Awaiting` для ждущих задач; `JobContext.ParentBatchId`.
- `PgBolt.Postgres.Client.ICompletionSignal` / `InProcessCompletionSignal` (пробуждение resolver'а), `PgBoltProcessingOptions.ResolverInterval`
  (1 с); resolver (учёт сигналов, обход детей, отмена членов) в цикле обслуживания `PgBolt.Server`; Warning при лаге сигналов.
- Retention: родители с ждущими продолжениями и Failed/Cancelled-члены существующих batches не удаляются; строки `batches` старше
  `JobsRetention` удаляются сверху вниз (`RetentionResult.DeletedBatches`).
- Генератор регистрирует метаданные для типизированных шагов workflow (`Enqueue<TArgs>`, `ContinueWith<TArgs>`, `EnqueueMany<TArgs>`);
  BOLT001 / BOLT006 для них; новая диагностика BOLT009 (Warning) — workflow не отправлен (`SubmitAsync`) в методе.
```

- [x] **Step 2: Roadmap и статус спеки**

В `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md` в части 5 заменить `Спека: \`2026-10-02-pgbolt-workflows-design.md\`. **Статус: спека.**` на ``Спека: `2026-10-02-pgbolt-workflows-design.md`, план: `docs/superpowers/plans/2026-10-02-pgbolt-workflows.md`. **Статус: реализовано.**``.

В строке статуса `docs/superpowers/specs/2026-10-02-pgbolt-workflows-design.md` заменить `Статус: одобрено ревью (4 итерации)` на `Статус: одобрено ревью (4 итерации), реализовано (план docs/superpowers/plans/2026-10-02-pgbolt-workflows.md; диагностика §5.4 выпущена как BOLT009 — BOLT007 занят частью 3)`.

В той же спеке — лог Debug при отсутствующей строке `batches` (отклонение 3 плана):
- §6.6 п. 2: `Нетерминальный batch или отсутствие строки (удалена retention'ом, §7.1; лог Debug) завершают подъём.` заменить на `Нетерминальный batch или отсутствие строки (удалена retention'ом, §7.1) завершают подъём; лога нет — requeue вызывается клиентом без логгера, лог Debug об удалённой строке пишет resolver при учёте сигнала (§6.4 Т1).`
- §7.1: `пропускают учёт с логом Debug.` заменить на `пропускают учёт (лог Debug — только у resolver'а при учёте сигнала, §6.4 Т1).`

- [x] **Step 3: Изменения других частей (§9 спеки части 5)**

В конец каждой спеки добавить раздел (текст — дословно):

`docs/superpowers/specs/2026-09-26-pgbolt-core-design.md`:

```markdown
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
```

`docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md`:

```markdown
## Изменения части 5 (Workflows)

Подробности — `2026-10-02-pgbolt-workflows-design.md` (§9).
- §7.1: освобождение при shutdown с подтверждённой отменой пишет сигнал завершения, как остальные statement'ы финализации.
- §6.3: цикл `resolver` (при `RunMaintenance`), `PgBoltProcessingOptions.ResolverInterval` (1 с), будится `ICompletionSignal`;
  `IServerStore.CountCompletionsAsync` / `ResolveAsync` / `CancelMembersAsync` / `OldestCompletionAgeAsync`; store сам подаёт
  in-process сигнал после финализации с подписчиками. Workflows требуют хотя бы одного процесса с сервером и `RunMaintenance = true`.
```

`docs/superpowers/specs/2026-09-29-pgbolt-storage-maintenance-design.md`:

```markdown
## Изменения части 5 (Workflows)

Подробности — `2026-10-02-pgbolt-workflows-design.md` (§7, §9).
- §5.3: живые строки партиции — также (а) родители-задачи с ждущими детьми (loose index scan по `queue_await_jobs` /
  `batches_await_jobs`) и (б) Failed- и Cancelled-члены существующих batches (только предпроверка); под блокировкой повторяются
  `queue` и (а); `@ids` компактирования — объединение набора предпроверки и повторной проверки (не пусто — отсоединение
  в транзакции компактирования, «`ids` пуст → отсоединение», больше не выполняется).
- §6.2: задача `retention` удаляет строки `batches` старше `JobsRetention` сверху вниз — блокировка кандидатов, затем проверка новым
  снимком; при `null` не удаляет.
- §11: к `jobs` через родителя обращаются statement'ы части 5, создающие `Awaiting`-задачи или делающие их терминальными; запуск
  `Awaiting`-строки (`UPDATE queue`) и активация batch (`UPDATE batches`) к `jobs` не обращаются — безвредно.
```

`docs/superpowers/specs/2026-09-30-pgbolt-scheduler-design.md`:

```markdown
## Изменения части 5 (Workflows)

Подробности — `2026-10-02-pgbolt-workflows-design.md` (§9).
- §3: детектор пре-релизной схемы проверяет и `batches`.
- §4.4: префикс `pgbolt:` отклоняется и в `StepOptions.UniqueKey`.
```

- [ ] **Step 4: Полный прогон**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: все тесты проходят. Падение — остановиться и доложить с выводом.

- [ ] **Step 5: Commit**

```bash
git add CHANGELOG.md docs/superpowers/specs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "docs: workflows (part 5) changelog and status"
```
