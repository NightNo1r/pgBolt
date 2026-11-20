# PgBolt — часть 4: Scheduler (recurring)

Статус: одобрено ревью (5 итераций); ред. 2 — миграции объединены, обратная совместимость схемы не поддерживается; реализовано (план docs/superpowers/plans/2026-09-30-pgbolt-scheduler.md) · Дата: 2026-09-30 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»), `2026-09-28-pgbolt-worker-design.md` (далее «worker»), `2026-09-29-pgbolt-storage-maintenance-design.md` (далее «часть 2»)

## 1. Цель и рамки

Recurring-задачи: типизированная (или динамическая по имени) задача с фиксированными аргументами ставится в очередь по cron-расписанию. Срабатывание — обычный enqueue core §5.1; отдельного пути выполнения нет. Работает при нескольких подах, через PgBouncer в transaction mode, без лидера, `LISTEN` и session state; горячий путь не затрагивается.

**Входит:**
- таблица `recurring_jobs` и колонки `jobs.recurring_id`, `jobs.scheduled_at`; миграции V001 и V002 объединяются в одну начальную V001 вместе с ними (§3);
- runtime API `IRecurringJobManager` (создание, изменение, удаление, пауза, «запустить сейчас», чтение);
- code-first объявления в `AddPgBolt` с синхронизацией набора владельца;
- тикер в `PgBolt.Server`: cron (Cronos) с секундами и таймзонами, misfire-политики `FireOnce` / `Skip` / `CatchUp`, политики наложения `Skip` / `Allow`;
- время срабатывания и id расписания в `JobContext`;
- изменения core и worker (§8).

**Не входит:**
- экраны recurring в dashboard — часть 6 (контракт чтения — §9);
- метрики — часть 7 (сейчас логи, как в части 2);
- интервальные расписания без cron («каждые 90 минут от последнего завершения»), зависимости между расписаниями, календари исключений.

### Критерии успеха
- На каждое срабатывание расписания в кластере создаётся не больше одной задачи при любом числе подов, рестартах и повторах после ошибок; при `Overlap = Allow` и отсутствии ошибок — ровно одна на каждое срабатывание, выбранное misfire-политикой (§5.3).
- Задержка срабатывания при работающем тикере — не больше `SchedulerInterval × 1.2` плюс время транзакции тика (штатно ~1 с), пока тикеры кластера успевают за потоком срабатываний. Догоняющие после простоя строки (`CatchUp`) встают в общую очередь по времени и остальные строки не вытесняют (§5.3).
- Пустой тик — один index scan по пустому диапазону частичного индекса на под в секунду.
- После простоя кластера пропущенные срабатывания обрабатываются по misfire-политике расписания; расчёт не зависит от часов подов (время — `now()` БД) и от часового пояса сессии PostgreSQL.
- Одна некорректная строка (неизвестный тип задачи, неразбираемый cron, таймзона, которой нет на поде, любое исключение планирования) не мешает срабатыванию остальных.
- Code-first определения сходятся к набору последней версии сервиса после завершения rolling deploy, включая пустой набор, объявленный `ClearRecurring()` (§4.3). Каждая запись синхронизации видна в логе (§7).

## 2. Принятые решения

| Вопрос | Решение | Причина |
|---|---|---|
| Источник определений | Runtime API над таблицей — основа; code-first объявления синхронизируются в ту же таблицу с привязкой к владельцу (§6) | Динамические расписания (per-tenant) и dashboard требуют API; code-first даёт удаление расписаний, убранных из кода (в Hangfire они остаются навсегда) |
| Координация подов | Без лидера: тик захватывает строки `FOR UPDATE SKIP LOCKED`, ставит задачи и сдвигает курсор в одной транзакции | Атомарность захвата, enqueue и сдвига курсора даёт одну задачу на срабатывание без `unique_key`; нагрузка распределяется по подам; масштабируется на сотни тысяч расписаний. Лидер через `maintenance_tasks` (часть 2) — единая точка с задержкой при смене, а атомарность всё равно нужна: перекрытие захватов, безвредное в части 2, здесь дало бы дубли. Предматериализация следующего запуска Scheduled-задачей ломает смену cron (отмена созданной задачи), CatchUp и `Overlap = Skip` (будущая задача занимает ключ). Часть 2 (§4, §11) предполагала, что часть 4 возьмёт `maintenance_tasks`; это предположение отменено — часть 4 `maintenance_tasks` не использует (§9) |
| Misfire | На расписание: `FireOnce` (по умолчанию), `Skip`, `CatchUp` с окном | `CatchUp` полезен только вместе со временем срабатывания в задаче (`JobContext.ScheduledAt`), поэтому оно хранится в `jobs` |
| Окно `CatchUp` | По времени (`CatchUpWindow`, 1 сутки), а не по числу | Cronos не перечисляет срабатывания назад: начало «последних N» нельзя получить прямым перебором без прохода всего пропущенного диапазона (месяц простоя посекундного cron — 2,6 млн итераций), а бинарный поиск §5.3 находит только последнее. С окном перебор начинается с `max(next_fire_at, now − window)` и ограничен лимитом на тик |
| Наложение | На расписание: `Skip` (по умолчанию) — через зарезервированный `unique_key`, `Allow` — без ключа. `CatchUp + Skip` не теряет срабатывания: курсор стоит, пока жив предыдущий запуск | `unique_key` core уже гарантирует «не больше одной активной задачи с ключом»; для `CatchUp` пропуск противоречил бы смыслу политики |
| Курсор и пробуждение | `next_fire_at` (логический курсор) отдельно от `wake_at` (когда смотреть на строку); индекс — по `wake_at`; `wake_at` не бывает раньше `db_now` тика, записавшего его (§5.3) | Строки, ждущие завершения предыдущего запуска, отложенные после ошибки или догоняющие простой, иначе стояли бы первыми в `ORDER BY next_fire_at` и каждую секунду на каждом поде вытесняли бы остальные из `LIMIT` |
| Умолчания задачи | `NULL` в `queue` / `priority` / `max_attempts` / `retry_delays` — значение из `job_types` на момент срабатывания, `LEFT JOIN` в запросе захвата (§5.2) | Изменение `[Job]` в сервисе-исполнителе подхватывается без пересохранения; тикер ставит задачу по имени и не требует исполнителя в своём процессе. `JobTypesCache` в транзакции тика ходил бы в БД вторым соединением при промахе и держал бы негативный кэш на TTL |
| Ошибка строки | Любое исключение разбора и планирования одной строки (неизвестный тип, cron, таймзона, Cronos) — ошибка этой строки: `last_error`, `wake_at = now() + 1 мин`, курсор стоит; пакет не откатывается | Битая строка с самым старым `wake_at` попадала бы в каждый пакет и откатывала бы все тики пода раз в секунду |
| Редактирование code-first строк через API | Запрещено, кроме Pause / Resume / Trigger / Get | Иначе правку молча откатила бы следующая синхронизация |
| Синхронизация code-first | На старте и периодически (`RecurringSyncInterval`, 1 мин) в `PgBolt.Server` | Только на старте: рестарт старого пода после выкатки новых откатил бы определения до следующего деплоя. Периодическая синхронизация сходится, когда старых подов не остаётся |
| Кто синхронизирует | Процесс с хотя бы одним `AddRecurring` или с вызовом `ClearRecurring()` — явно объявленным пустым набором (строки владельца удаляются). `RecurringOwner` — только имя владельца, синхронизацию не включает | Без `ClearRecurring` удаление последнего `AddRecurring` не удаляло бы строки никогда. Включатель в имени владельца — молчаливая ловушка: `RecurringOwner` задан в общем для api и worker коде, `AddRecurring` — только в worker, и каждый старт api удалял бы строки, а worker через минуту вставлял заново, теряя `paused`, курсор и срабатывания. Безусловная синхронизация — та же ловушка для api- и worker-деплойментов одной сборки с одинаковым `ApplicationName` |
| Миграции | V001 и V002 объединяются с изменениями части 4 в одну начальную миграцию V001; совместимость со схемами предыдущих сборок не поддерживается | Сервисом ещё никто не пользуется. `ALTER TABLE jobs ADD COLUMN` отдельной миграцией на живой схеме берёт `ACCESS EXCLUSIVE` на родителе и всех партициях и потребовал бы защиты горячего пути (`lock_timeout`, повторы, диагностика держателей) — без живых схем это лишняя сложность. Защита миграций, меняющих `jobs` под нагрузкой, проектируется, когда понадобится первая такая миграция |
| Пауза | Флаг `paused` отдельно от определения | Синхронизация его не трогает: пауза из dashboard переживает рестарт и деплой |
| Trigger | Не обновляет строку `recurring_jobs` | Обновление давало бы порядок блокировок «ключ в `queue` → строка расписания», обратный тику, и deadlock. История запусков — по `jobs.recurring_id` |
| Cron | Cronos: 5 полей (`CronFormat.Standard`), 6 (`IncludeSeconds`) или макрос `@…` (`@daily`, `@every_second` и т. п.; разбирается одинаково в обоих форматах); семантика DST Cronos; ответ Cronos `after(t) ≤ t` на исторических сменах базового смещения исправляет `after` (§5.4) | Библиотека без reflection (AOT), корректно обрабатывает переходы DST |

## 3. Схема: объединённая миграция V001

Скрипты `V001__initial.sql` и `V002__maintenance_tasks.sql` объединяются в один `Schema/Migrations/V001__initial.sql`, в него же добавляются объекты ниже; `V002__maintenance_tasks.sql` удаляется, `PgBoltMigrations.LatestVersion = 1`. Схемы, созданные предыдущими сборками, не обновляются: у них в `schema_version` версия 1 (старая V001 без V002) или 2, а объектов части 4 нет, и миграция считала бы их актуальными или «новее кода» (часть 2 §9.3 — Warning и продолжение). Поэтому `PgBoltInitializer` после миграций (и при `AutoMigrate = false` — после проверки версии) проверяет `SELECT to_regclass('{schema}.recurring_jobs') IS NULL` и при отсутствии таблицы падает: `InvalidOperationException` «schema '{schema}' was created by a pre-release PgBolt build and cannot be upgraded; drop it (`DROP SCHEMA … CASCADE`) and restart». Правило части 2 «версия новее кода — Warning» для остальных случаев не меняется.

```sql
CREATE TABLE {schema}.recurring_jobs (
    id              text        PRIMARY KEY,          -- пользовательский, 1..200 символов
    type            text        NOT NULL,             -- имя задачи
    args            jsonb       NOT NULL,
    cron            text        NOT NULL,             -- Cronos: 5 полей или 6 (с секундами)
    time_zone       text        NOT NULL,             -- IANA id; по умолчанию 'UTC'
    misfire         smallint    NOT NULL,             -- 0 FireOnce, 1 Skip, 2 CatchUp
    catch_up_window interval    NOT NULL,
    overlap         smallint    NOT NULL,             -- 0 Skip, 1 Allow
    queue           text,                             -- NULL → умолчание из job_types на момент срабатывания
    priority        smallint,
    max_attempts    int,
    retry_delays    int[],                            -- секунды
    owner           text,                             -- NULL = создано через API; иначе code-first владелец (§6)
    paused          boolean     NOT NULL DEFAULT false,
    next_fire_at    timestamptz,                      -- логический курсор: следующее срабатывание по cron
    wake_at         timestamptz,                      -- когда тикеру смотреть на строку (§5.2)
    last_fire_at    timestamptz,                      -- ScheduledAt последнего поставленного срабатывания
    last_job_id     uuid,
    last_error      text,                             -- только ошибки; очищается успешным срабатыванием
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);
CREATE INDEX recurring_due   ON {schema}.recurring_jobs (wake_at) WHERE NOT paused;
CREATE INDEX recurring_owner ON {schema}.recurring_jobs (owner)   WHERE owner IS NOT NULL;

-- в CREATE TABLE {schema}.jobs (core §4.2) добавляются колонки:
--     recurring_id  text,          -- id расписания (часть 4)
--     scheduled_at  timestamptz,   -- момент срабатывания cron (часть 4)
```

- `jobs.recurring_id` и `jobs.scheduled_at` записываются один раз при создании и не меняются — контракт HOT (core §4.2) не нарушается. Партиции получают их через `LIKE` (core §4.4).
- Таблица маленькая по сравнению с `queue`; обновление строки — раз на срабатывание. Настройки autovacuum по умолчанию.
- Инвариант: `next_fire_at IS NULL` ⇔ `wake_at IS NULL` ⇔ у cron нет будущих срабатываний. `AddOrUpdate` и синхронизация такие выражения отклоняют (§4.2), поэтому штатно курсор не пуст. Все записи `wake_at` (тик, `ResumeAsync`, API) его сохраняют.

## 4. Публичный API

### 4.1 Контракты (`PgBolt.Abstractions`)

```csharp
public enum MisfirePolicy { FireOnce, Skip, CatchUp }
public enum OverlapPolicy { Skip, Allow }

public sealed record RecurringJobOptions
{
    public TimeZoneInfo? TimeZone { get; init; }                  // null = UTC; хранится как IANA id
    public MisfirePolicy Misfire { get; init; } = MisfirePolicy.FireOnce;
    public TimeSpan CatchUpWindow { get; init; } = TimeSpan.FromDays(1);   // (0, 366 суток]
    public OverlapPolicy Overlap { get; init; } = OverlapPolicy.Skip;
    public string? Queue { get; init; }
    public short? Priority { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}

public sealed record RecurringJobInfo(
    string Id, string JobName, JsonElement Args, string Cron, string TimeZone,
    MisfirePolicy Misfire, TimeSpan CatchUpWindow, OverlapPolicy Overlap,
    string? Queue, short? Priority, int? MaxAttempts, IReadOnlyList<TimeSpan>? RetryDelays,
    string? Owner, bool Paused,
    DateTimeOffset? NextFireAt, DateTimeOffset? LastFireAt, JobId? LastJobId, string? LastError,
    DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt);

public interface IRecurringJobManager
{
    ValueTask AddOrUpdateAsync<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null, CancellationToken ct = default);
    ValueTask AddOrUpdateAsync(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null, CancellationToken ct = default);
    ValueTask<bool> RemoveAsync(string id, CancellationToken ct = default);
    ValueTask<bool> PauseAsync(string id, CancellationToken ct = default);
    ValueTask<bool> ResumeAsync(string id, CancellationToken ct = default);
    ValueTask<EnqueueResult?> TriggerAsync(string id, CancellationToken ct = default);   // null — расписания нет
    ValueTask<RecurringJobInfo?> GetAsync(string id, CancellationToken ct = default);
}
```

`JobContext` получает свойства `DateTimeOffset? ScheduledAt` и `string? RecurringJobId` (необязательные параметры конструктора в конце); `JobInfo` — те же поля в конце списка. Для задач не из расписания оба `null`.

Реализация `IRecurringJobManager` — в `PgBolt.Postgres`, регистрируется `AddPgBolt` рядом с `IJobClient`.

### 4.2 Правила runtime API

- **Валидация** до обращения к БД, ошибка — `ArgumentException`: `id` непустой, не длиннее 200 символов; cron разбирается Cronos и имеет хотя бы одно будущее срабатывание (`0 0 30 2 *` отклоняется). Формат выбирается одинаково при валидации и в тике: строка, начинающаяся с `@`, — макрос Cronos (`CronFormat.Standard`; макросы, включая `@every_second`, разбираются одинаково в обоих форматах), иначе по числу полей через пробелы: 5 — `Standard`, 6 — `IncludeSeconds`, другое — `ArgumentException`. `CatchUpWindow` в `(0, 366 суток]`; `Queue` непустой, если задан (как в `EnqueueOptionsResolver` core: пустая очередь попала бы в строку, и тик ставил бы задачи в очередь, которую никто не слушает); `MaxAttempts ≥ 1`; `RetryDelays` неотрицательны. Таймзона сохраняется как IANA id (`TimeZoneInfo.HasIanaId` или `TryConvertWindowsIdToIanaId`; неконвертируемая — `ArgumentException`).
- **Нормализация** — объявленные значения приводятся к хранимому виду до записи и до сравнения в синхронизации (§6.2): `RetryDelays` — секунды с округлением вверх (`Math.Ceiling`, как `EnqueueOptionsResolver` core), `CatchUpWindow` — усечение до микросекунд (точность `interval`), таймзона — IANA id. `GetAsync` возвращает нормализованные значения.
- **Аргументы:** типизированная перегрузка сериализует через `JobMetadata<TArgs>` (как `EnqueueAsync<TArgs>`, core §6.2, включая ошибку для незарегистрированного типа); динамическая проверяет имя по `job_types` (кэш core) — неизвестное имя → `UnknownJobException`. Проверка по `job_types` — только в runtime API; code-first синхронизация её не делает (§4.3).
- **`AddOrUpdateAsync`** — транзакция в два round-trip'а; курсор пересчитывается от строки, прочитанной под блокировкой (обозначения — §5.3):
  ```sql
  -- пакет 1
  BEGIN;
  SELECT now() AS db_now;
  SELECT owner, cron, time_zone, last_fire_at FROM {schema}.recurring_jobs WHERE id = @id FOR UPDATE;
  -- пакет 2, строки нет:
  INSERT INTO {schema}.recurring_jobs (id, type, args, cron, time_zone, misfire, catch_up_window, overlap,
         queue, priority, max_attempts, retry_delays, owner, next_fire_at, wake_at, created_at, updated_at)
  VALUES (@id, …, NULL, @c, @c, now(), now())
  ON CONFLICT (id) DO NOTHING
  RETURNING 1;
  -- пакет 2, строка с owner IS NULL:
  UPDATE {schema}.recurring_jobs SET type = @type, args = @args, …, updated_at = now()
         [, next_fire_at = @c, wake_at = @c]          -- только при смене cron или time_zone
  WHERE id = @id;
  COMMIT;
  ```
  `c = after(max(db_now, last_fire_at))`, вычисляется в .NET. Строка под блокировкой: если тик держал её и успел поставить срабатывание по старому cron, его `last_fire_at` уже виден, и совпадающее время нового cron второй раз не ставится. Поэтому `db_now`, взятый до ожидания блокировки, безвреден; `statement_timestamp()` запроса `FOR UPDATE` тоже отсчитывается от его начала, а не от получения блокировки. Срабатывание нового cron в `[db_now, COMMIT]` обработает следующий тик как просроченное по misfire-политике. Новая строка — `owner = NULL`; пустой `RETURNING` вставки — строку создали параллельно (API или синхронизация), операция повторяется с начала и видит её под блокировкой. У существующей строки обновляются поля определения; курсор пересчитывается только при смене `cron` или `time_zone`; `paused` и `last_*` не меняются. Строка с `owner IS NOT NULL` принадлежит code-first синхронизации (§6) → `ROLLBACK`, `InvalidOperationException`.
- **`RemoveAsync`** — `DELETE` строки с `owner IS NULL`; для code-first строки — `InvalidOperationException` (строки выведенного из эксплуатации владельца — §6.2); `false` — строки нет. Уже поставленные задачи не затрагиваются.
- **`PauseAsync`** — `paused = true`. **`ResumeAsync`** — `paused = false, wake_at = CASE WHEN wake_at IS NULL THEN NULL ELSE LEAST(wake_at, now()) END` (`LEAST` игнорирует `NULL` и без `CASE` оживил бы строку без будущих срабатываний, нарушив инвариант §3). Курсор на паузе стоит, поэтому после Resume пропущенное обрабатывается misfire-политикой: `FireOnce` — одно срабатывание сразу, `Skip` — ничего, `CatchUp` — догоняет в пределах окна. Разрешены и для code-first строк. `false` — строки нет.
- **`TriggerAsync`** — читает строку без блокировки вместе с умолчаниями `job_types` (тот же `LEFT JOIN`, что захват §5.2) и ставит задачу обычным путём клиента (core §5.1, включая запрос разрешения конфликта) с `scheduled_at = NULL` (в SQL — `COALESCE(scheduled_at, now())`, время БД, §8), `recurring_id = id` и ключом §4.4 при `Overlap = Skip`. Тип, которого нет в `job_types`, — `UnknownJobException`, как у динамического `EnqueueAsync`; cron, таймзона и `catch_up_window` Trigger'у не нужны (нечитаемое окно строки, записанной в обход API, Trigger не останавливает); нечитаемый `retry_delays` уходит в задачу, поэтому — `InvalidOperationException` с id и полем. Строку `recurring_jobs` не меняет (§5.5), курсор не сдвигает, `paused` не учитывает. При живом запуске и `Overlap = Skip` — `Created = false` с id живого запуска. Разрешён для code-first строк.
- **`GetAsync`** — строка целиком; `null` — нет строки; нечитаемое значение строки, записанной в обход API (`catch_up_window` с месяцами, NULL в `retry_delays`), — `InvalidOperationException` с id, полем и способом исправления.

### 4.3 Code-first

```csharp
services.AddPgBolt(o =>
{
    o.RecurringOwner = "orders";   // только имя; по умолчанию IHostEnvironment.ApplicationName
    o.AddRecurring("orders.cleanup", "0 3 * * *", new CleanupArgs(Days: 30),
        new() { TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow") });
    o.AddRecurring("reports.daily", "reports.build", "0 6 * * *", JsonDocument.Parse("""{"kind":"daily"}""").RootElement);
    // o.ClearRecurring();         // вместо AddRecurring: владелец больше не объявляет расписаний
});
```

- `AddRecurring` сразу валидирует всё, кроме аргументов и существования типа (правила §4.2), и уникальность `id` в наборе процесса; ошибка — исключение при построении опций, до старта хоста.
- Аргументы сериализуются при первой синхронизации (регистрация `JobMetadata<TArgs>` может ещё не произойти в момент вызова `AddRecurring`); ошибка — падение старта (§6.1).
- **Тип задачи по `job_types` не проверяется** ни при `AddRecurring`, ни синхронизацией: при первом деплое сервис с расписаниями может стартовать раньше, чем исполнитель опубликовал `job_types`, и проверка дала бы падение старта и зависимость от порядка деплоя. Неизвестный тип — ошибка строки в тике (§5.2 п. 2), `last_error` виден в `GetAsync`; как только исполнитель опубликует тип, строка сработает.
- `RecurringOwner`, если задан, непустой, не длиннее 200 символов; если не задан и `IHostEnvironment` недоступен или `ApplicationName` пуст или из одних пробелов, а синхронизация есть, — исключение при старте.
- **Владелец** — единица синхронизации: все процессы одного владельца с синхронизацией обязаны объявлять одинаковый набор (все поды одного сервиса). Разные сервисы на одной схеме — разные владельцы.
- **Участие в синхронизации:** процесс синхронизирует набор владельца, если объявляет хотя бы одно расписание **или** вызывает `o.ClearRecurring()` — явно объявляет пустой набор: строки владельца удаляются, так убирают последнее `AddRecurring`. `ClearRecurring()` вместе с `AddRecurring` — исключение при построении опций. `RecurringOwner` — только имя и синхронизацию не включает: процесс без `AddRecurring` и `ClearRecurring` (api, когда расписания объявлены в worker) строк не трогает при любом владельце, явном или по умолчанию. Документируется: удаление последнего `AddRecurring` без `ClearRecurring()` строки не удаляет; когда выкатка с `ClearRecurring()` завершена и строк владельца нет, вызов можно убрать следующей версией.

### 4.4 Зарезервированный префикс `unique_key`

`Overlap = Skip` использует ключ `pgbolt:recurring:{id}`. Префикс `pgbolt:` резервируется: клиент отклоняет его в `EnqueueOptions.UniqueKey` и в результатах `uniqueKeySelector` (`ArgumentException`). Внутренние пути (тик, Trigger) проверку обходят.

## 5. Тикер (`PgBolt.Server`)

### 5.1 Цикл

Цикл `scheduler` в `ProcessingMaintenance` (worker §6.3) рядом с promoter, sweeper и housekeeping, на всех подах; `SKIP LOCKED` делает параллельные тики безопасными. Итерация — транзакции тика (§5.2) подряд, пока пакет полный (захвачено `@n = 100` строк или достигнут лимит срабатываний пакета); затем пауза `SchedulerInterval × (1 ± 0.2)`, по умолчанию 1 с. Адаптивного сна до ближайшего `wake_at` нет: при 1 с выигрыша нет, а изменения через API другого процесса он бы не видел.

Ошибка итерации — лог Error и следующий интервал (как у остальных циклов). Остановка — общий токен `ProcessingMaintenance`; прерванная транзакция откатывается целиком.

### 5.2 Транзакция тика

```sql
-- пакет 1: BEGIN + захват
SELECT r.id, r.type, r.args, r.cron, r.time_zone, r.misfire, r.catch_up_window, r.overlap, r.next_fire_at,
       COALESCE(r.queue,        t.default_queue)        AS queue,
       COALESCE(r.priority,     t.default_priority)     AS priority,
       COALESCE(r.max_attempts, t.default_max_attempts) AS max_attempts,
       COALESCE(r.retry_delays, t.default_retry_delays) AS retry_delays,
       t.name IS NULL AS unknown_type,
       now() AS db_now
FROM {schema}.recurring_jobs r
LEFT JOIN {schema}.job_types t ON t.name = r.type
WHERE r.wake_at <= now() AND NOT r.paused
ORDER BY r.wake_at
LIMIT @n
FOR UPDATE OF r SKIP LOCKED;
```

1. **Захват.** Строки, заблокированные другим тиком, синхронизацией или API, пропускаются. `OF r` блокирует только строки расписаний (`job_types` не блокируется). Умолчания `NULL`-полей приходят в том же round-trip'е; `JobTypesCache` тик не использует (при промахе он ходил бы в БД вторым соединением при открытой транзакции и держал бы негативный кэш на TTL). Пустой тик по-прежнему — index scan `recurring_due` по пустому диапазону.
2. **Планирование в .NET** (чистая функция `RecurringPlanner`, §5.3) по каждой строке: список срабатываний (`ScheduledAt`) и новые `next_fire_at` / `wake_at`. Все расчёты — от `db_now`. **Ошибка строки** — `unknown_type` или любое исключение разбора и планирования этой строки, кроме отмены токена остановки: cron не разбирается (строка записана в обход API или более новой версией), таймзоны нет на поде (`TimeZoneNotFoundException`: образ без tzdata — chiseled, `InvariantTimezone`), исключение Cronos или `RecurringPlanner`. Строка не ставится: `last_error` = тип и сообщение исключения (`unknown job type …`), `wake_at = now() + 1 мин`, курсор стоит (§7); пакет не откатывается, остальные строки обрабатываются. На неоднородном кластере (tzdata есть не на всех подах) строка сработает на исправном поде после backoff — по своей misfire-политике: для исправного пода это обычный простой длиной ≥ 1 мин. `FireOnce` и `CatchUp` ставят пропущенное; `Skip` при `MisfireThreshold ≤ 1 мин` (умолчание при `SchedulerInterval ≤ 12 с`, §5.3) срабатывание, на котором строка упала, отбрасывает (Warning §7) — кому оно важно, выбирает `FireOnce` или `MisfireThreshold` больше backoff с запасом на интервал тика (например, 2 мин). Лимит пакета — 1000 срабатываний (достижим только при `CatchUp + Allow`: до 100 срабатываний на строку): строка, срабатывания которой не помещаются в остаток лимита, и все следующие остаются нетронутыми (снимаются с блокировки на `COMMIT`) и берутся следующей транзакцией.
3. **Пакет 2: enqueue.** Срабатывания группируются по `(type, queue, priority, max_attempts, retry_delays)`; на группу — один statement core §5.1 с дополнительными массивами `scheduled_at` и `recurring_id` в `unnest` (§8), `run_at = now()`, `unique_key = 'pgbolt:recurring:' || id` при `Overlap = Skip`, `trace_parent = NULL`. Все группы отправляются одним `NpgsqlBatch`. Запрос разрешения конфликта core §5.1 не нужен: id свежие, поэтому строка без `created` — конфликт по ключу, то есть жив предыдущий запуск.
4. **Пакет 3: обновление курсоров** одним statement'ом:
   ```sql
   UPDATE {schema}.recurring_jobs r
   SET next_fire_at = u.next_fire_at, wake_at = u.wake_at,
       last_fire_at = COALESCE(u.last_fire_at, r.last_fire_at),
       last_job_id  = COALESCE(u.last_job_id,  r.last_job_id),
       last_error   = CASE WHEN u.last_error IS NOT NULL OR u.last_fire_at IS NOT NULL THEN u.last_error ELSE r.last_error END
   FROM unnest(@ids::text[], @next::timestamptz[], @wake::timestamptz[],
               @fired::timestamptz[], @job_ids::uuid[], @errors::text[])
        AS u(id, next_fire_at, wake_at, last_fire_at, last_job_id, last_error)
   WHERE r.id = u.id;
   ```
   Выполняется после enqueue: при `CatchUp + Skip` курсор зависит от конфликта по ключу (§5.3). `updated_at` не меняется — это время изменения определения.
5. `COMMIT`; затем `IWakeupSignal.Signal` по очередям поставленных задач (core §5.10).

Штатно — 3 round-trip'а до `COMMIT` плюс `COMMIT` на пакет до 100 расписаний. Ровно одна задача на срабатывание: захват, enqueue и сдвиг курсора атомарны. Системная ошибка (нет партиции `jobs`, обрыв соединения) откатывает всё; пакет повторяется следующим тиком с новыми id — дублей нет. Обрыв на `COMMIT` с неизвестным исходом безопасен: если транзакция закоммичена, курсор уже сдвинут.

### 5.3 Правила срабатывания (`RecurringPlanner`)

Обозначения: `after(t)` — первое срабатывание cron строго после `t` в таймзоне расписания; `due` — срабатывания в `[next_fire_at, db_now]` (первое из них — сам `next_fire_at`); `window_start = db_now − catch_up_window`; `T = MisfireThreshold`.

| Политика | Что ставится | Новый `next_fire_at` |
|---|---|---|
| `FireOnce` | одно срабатывание, `ScheduledAt` = последнее из `due` | `after(db_now)` |
| `Skip` | то же, если последнее из `due` ≥ `db_now − T`; иначе ничего | `after(db_now)` |
| `CatchUp`, `Overlap = Allow` | до 100 первых срабатываний `due`, не раньше `window_start` | `after(последнее поставленное)` — остаток догоняют следующие тики; если в окне срабатываний нет — `after(db_now)` |
| `CatchUp`, `Overlap = Skip` | одно: первое из `due`, не раньше `window_start`; если в окне срабатываний нет — ничего | `after(него)`; при конфликте по ключу — курсор не меняется; если в окне срабатываний нет — `after(db_now)` |

- `wake_at = max(next_fire_at, db_now)` (`NULL` при `next_fire_at IS NULL`, инвариант §3), кроме: конфликт при `CatchUp + Skip` → `wake_at = now() + 5 с` (константа `OverlapRecheckDelay`); ошибка строки (§5.2 п. 2) → `wake_at = now() + 1 мин`, курсор не меняется. `max` важен для догона (`CatchUp` после частичного догона оставляет `next_fire_at` в прошлом): иначе строка с `wake_at` в далёком прошлом стояла бы первой в `ORDER BY wake_at`, и несколько посекундных `CatchUp + Allow` строк после суток простоя (сотни тысяч срабатываний) занимали бы все пакеты кластера, задерживая остальные строки. С `wake_at = db_now` догоняющая строка встаёт в общую очередь за строками, проснувшимися раньше, и продолжает догон в свою очередь.
- **`Overlap = Skip` при `FireOnce` / `Skip`:** конфликт по ключу — срабатывание пропущено, курсор сдвигается как обычно, `last_fire_at` / `last_job_id` не меняются, лог Debug (частый cron с долгой задачей давал бы запись в секунду на расписание; счётчик пропусков — метрика части 7).
- **Поиск последнего из `due`** (`FireOnce`, `Skip`) — бинарным поиском, потому что Cronos не ищет назад. Множество срабатываний фиксировано, поэтому `after(t)` не убывает, и предикат `after(t) ≤ db_now` истинен для `t` раньше последнего из `due` и ложен начиная с него. При `next_fire_at > db_now` `due` пуст. Если предикат ложен уже при `t = next_fire_at`, ответ — `next_fire_at` (штатный случай: один вызов). Иначе поиск по `t ∈ [next_fire_at, db_now]` с инвариантом «предикат истинен в `lo`, ложен в `hi`» до `hi − lo ≤ 1 с`; ответ — `after(lo)`: срабатывания Cronos — на целых секундах, в `(lo, hi]` их не больше одного, после `hi` до `db_now` — ни одного. Вызовов `GetNextOccurrence` — `⌈log₂((db_now − next_fire_at) / 1 с)⌉ + 2`, не больше ~42 при любом простое и любом cron, включая пачечные (`* * * * 1 *` — каждую секунду в январе, простой 10 января – 1 марта: ответ 31 января 23:59:59), под блокировкой строки — микросекунды. `Skip` сравнивает найденное с `db_now − T`. `CatchUp` перебирает вперёд от `max(next_fire_at, window_start)`, не больше 100 срабатываний.
- **Почему порог `Skip`:** тик раз в секунду делает любое срабатывание «опоздавшим» на миллисекунды; без порога `Skip` не отличал бы простой от штатной задержки. Порог обязан перекрывать штатную задержку (`SchedulerInterval × 1.2` плюс транзакции полной итерации, §5.1) с запасом: при `T < SchedulerInterval × 1.2` `Skip` отбрасывал бы почти всё. Поэтому `MisfireThreshold` (`TimeSpan?`) по умолчанию — `max(1 мин, 5 × SchedulerInterval)`, а явно заданное значение валидация требует `≥ 5 × SchedulerInterval` (ошибка — исключение при построении опций): смена одного `SchedulerInterval` не ломает опцию, которую пользователь не задавал. `MisfireThreshold` — опция процесса: при разных значениях на подах действует значение пода, захватившего строку.
- **Warning о пропуске** (`Skip` отбросил просроченное, `CatchUp` пропустил срабатывания `[next_fire_at, window_start)`, §7) пишется только вместе с фактическим сдвигом курсора в этой транзакции. При конфликте `CatchUp + Skip` курсор стоит, и перепроверка раз в 5 с иначе повторяла бы Warning на каждом тике (~17 000 в сутки), пока предыдущий запуск живёт дольше `CatchUpWindow`; пропуск логируется один раз — когда запуск закончился и курсор сдвинут, диапазоном от старого `next_fire_at`.
- **`FireOnce`** ставит последнее пропущенное срабатывание: задача вроде «отчёт за сутки» получает самую свежую дату.

### 5.4 Время и DST

- Все моменты — `timestamptz` (UTC в .NET); `db_now` — `now()` БД из запроса захвата. Часы подов в расчётах не участвуют: `scheduled_at` тика — момент cron, посчитанный от `db_now`, у Trigger — `now()` БД (§4.2). Правила сравнения — как в части 2 §3.
- `after(t)` = `CronExpression.GetNextOccurrence(t, tz, inclusive: false)` (`null` — срабатываний нет, предикат бинарного поиска ложен) с исправлением ниже. Перебор `CatchUp` (§5.3) — цепочка `after`: первое срабатывание `≥ from` — `after(from − 1 тик)` (срабатывания Cronos — на целых секундах), следующее — `after` предыдущего, пока `≤ db_now`. `GetOccurrences` Cronos не используется: на ошибке Cronos ниже он бесконечно повторяет одно срабатывание. `due` — замкнутый интервал `[next_fire_at, db_now]`: предикат поиска `after(t) ≤ db_now` нестрогий, и перебор включает срабатывание ровно в `db_now` — иначе оно выпало бы при уже сдвинутом за него курсоре `after(db_now)`.
- Переходы DST — семантика Cronos: несуществующее локальное время (переход вперёд) срабатывает в момент перехода; неоднозначное (переход назад) — один раз для выражений с фиксированными часом и минутой, дважды для интервальных (`*/15` в часах и минутах). Документируется.
- **Исторические смены базового смещения** (не DST: Europe/Moscow 2011 и 2014, Pacific/Apia 2011 и т. п.). Cronos 0.13 рядом с такой сменой может вернуть `after(t) ≤ t` (Europe/Moscow 2011-03-27, 02:00 MSK+3 → 03:00 MSK+4: для `0 * * * *` — 22:00Z при любом `t` из `[22:00Z, 23:00Z)`); без исправления тик ставил бы одно срабатывание до ~3600 раз подряд, а бинарный поиск §5.3 потерял бы монотонность. Поэтому `after(t)`, получив от Cronos значение `≤ t`, ищет исправную точку `s > t` — целую секунду, от которой Cronos отвечает срабатыванием `≥ s` или `null` (удвоением шага до 2²⁰ с ≈ 12 сут, затем бинарным поиском — ближайшую после последней неисправной), — и возвращает срабатывание от неё. Так `after(t) > t` и не убывает по `t`; на неисправном участке вызовов Cronos больше оценки §5.3 (до ~40 на `after`). Семантика DST на таких участках не гарантируется: повторяющийся час срабатывает один раз (Europe/Moscow 2014-10-26, 02:00 MSK+4 → 01:00 MSK+3: `*/15 1 * * *` — 4 срабатывания, а не 8), несуществующее время — не обязательно в момент перехода (Europe/Moscow 2011-03-27: `30 2 * * *` — в 22:30Z, за полчаса до перехода). Исправная точка не найдена — `InvalidOperationException`, в тике — ошибка строки (§5.2 п. 2). Документируется.
- Смена правил таймзоны в tzdata пода влияет на расчёт следующего курсора; поды с разными версиями tzdata могут разойтись на один курсор — допустимо.

### 5.5 Блокировки и конкурентность

- Тик: строки `recurring_jobs` (`FOR UPDATE`) → вставки в `queue` / `jobs`. Ключи `pgbolt:recurring:{id}` разных строк различны, строки захвачены разными тиками без пересечения — тики друг с другом не конфликтуют.
- Trigger: только вставка в `queue` / `jobs` (строку расписания не блокирует) — цикла с тиком нет. При совпадении ключа вставка Trigger'а ждёт транзакцию тика (или наоборот) и получает конфликт — задача одна.
- `Remove` / `Pause` / `Resume`: один statement на строку; ждут транзакцию тика, держащего строку, — миллисекунды. `AddOrUpdate`: `FOR UPDATE` строки на один round-trip (§4.2), других блокировок не берёт; тик в это время строку пропускает (`SKIP LOCKED`) и берёт следующим тиком. После `Remove` тик, уже захвативший строку, завершается штатно: удаление применяется после его `COMMIT`; после `Pause` — так же, следующие тики строку не видят.
- Синхронизация (§6): advisory lock владельца → строки `recurring_jobs` (`FOR UPDATE` только строк с пересчётом курсора, §6.2). Тик advisory lock не берёт и синхронизацию не ждёт — цикла нет. Синхронизации разных владельцев пересекаются только вставками одного id (коллизия); вставки идут в порядке id (§6.2), поэтому цикла ожидания между ними нет.
- Уровень изоляции — READ COMMITTED (core §1), как у всего горячего пути.

## 6. Code-first синхронизация

### 6.1 Где выполняется

Процесс участвует в синхронизации, если объявляет расписания (`AddRecurring`) или пустой набор (`ClearRecurring()`, §4.3); `RecurringOwner` на участие не влияет. Далее — «есть синхронизация».

- **На старте** — в `PgBoltInitializer` после миграций и партиций, до `Ready`, если есть синхронизация. Работает и в процессах без сервера. Ошибка (включая сериализацию аргументов) — падение старта, как при ошибке миграций.
- **Периодически** — цикл `recurring-sync` в `ProcessingMaintenance`, раз в `RecurringSyncInterval × (1 ± 0.2)` (1 мин), если есть синхронизация и сервер — независимо от `RunMaintenance` и `RunScheduler` (§8). В процессе без сервера периодической синхронизации нет, и защиты от отката при rolling deploy нет — документируется.

### 6.2 Транзакция

```sql
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('{schema}:recurring:' || @owner));
SELECT r.id, r.owner, r.type, r.args = d.args AS args_equal, r.cron, r.time_zone, r.misfire,
       r.catch_up_window, r.overlap, r.queue, r.priority, r.max_attempts, r.retry_delays, now() AS db_now
FROM {schema}.recurring_jobs r
LEFT JOIN unnest(@ids::text[], @args::jsonb[]) AS d(id, args) ON d.id = r.id
WHERE r.owner = @owner OR r.id = ANY(@ids);
-- только если есть смена cron / time_zone:
SELECT id, last_fire_at FROM {schema}.recurring_jobs WHERE id = ANY(@recalc) AND owner = @owner FOR UPDATE;
-- дифф в .NET → INSERT … SELECT … FROM unnest(…) AS d(id, …) ORDER BY d.id
--               ON CONFLICT (id) DO NOTHING RETURNING id / UPDATE / DELETE
-- одним NpgsqlBatch, только при разнице
COMMIT;
```

**Порядок вставок.** Синхронизации разных владельцев с пересекающимися id (коллизия) вставляют одни и те же ключи: вставка, наткнувшаяся на незакоммиченную строку другой транзакции, ждёт её исхода. В произвольном порядке (A — `X, Y`, B — `Y, X`) это deadlock `40P01` и падение старта вместо «коллизия, лог Error». Поэтому вставка упорядочена явно — `ORDER BY d.id` в SQL, как `ORDER BY unique_key, id` в enqueue core §5.1: порядок строк `INSERT … SELECT` без `ORDER BY` не гарантирован, а collation сравнения одна для всех процессов (collation базы), и ожидания идут в одном порядке; сортировка массивов в .NET не нужна. `FOR UPDATE`, `UPDATE` и `DELETE` касаются только строк своего владельца (`AND owner = @owner`); их одновременно трогают лишь синхронизации того же владельца, сериализованные advisory lock'ом, и тик, который синхронизацию не ждёт (§5.5), — сортировка внутри них ничего не даёт. API вставляет и блокирует по одной строке и цикла не замыкает. Остаётся случай двойной коллизии: A вставляет `j`, затем ждёт `k`, который владелец B в ту же секунду меняет или удаляет, а B следующей вставкой ждёт `j`. Сортировка его не устраняет (ожидания идут через разные statement'ы), поэтому `40P01` синхронизации — повтор транзакции (до 3 раз, и на старте); по исчерпании — как любая ошибка синхронизации (§7).

`args` сравниваются в SQL как `jsonb`, чтобы разное форматирование JSON не давало записей; остальные поля — в .NET, объявленные значения предварительно нормализуются так же, как при записи (§4.2: `RetryDelays` — секунды с округлением вверх, `CatchUpWindow` — до микросекунд, таймзона — IANA id). Без нормализации `RetryDelays = 1,5 с` или `CatchUpWindow` с долями микросекунды давали бы `UPDATE` каждую минуту на каждом поде.

Advisory lock сериализует синхронизации одного владельца; транзакционный — работает через PgBouncer. Параллельную вставку того же id через API или синхронизацией другого владельца он не исключает — её разрешает `ON CONFLICT`. Дифф:

- **Объявлено, строки нет:** `INSERT … ON CONFLICT (id) DO NOTHING` с `owner`, `next_fire_at = wake_at = after(db_now)`. id, не вернувшийся из `RETURNING`, — строку параллельно создал API или другой владелец: коллизия, как ниже (лог Error, старт не падает).
- **Объявлено, строка этого владельца отличается:** `UPDATE … WHERE id = @id AND owner = @owner` полей определения и `updated_at`; при смене `cron` / `time_zone` — пересчёт курсора `c = after(max(db_now, last_fire_at))` с `last_fire_at`, перечитанным под `FOR UPDATE` (как в `AddOrUpdate`, §4.2: срабатывание, поставленное тиком по старому cron, не повторится). Блокируются только такие строки — штатный периодический проход строки не блокирует и тик не задерживает. `paused` и `last_*` не меняются.
- **Строка владельца, не объявленная:** `DELETE`. Уже поставленные задачи живут. При пустом наборе (`ClearRecurring()`, §4.3) — все строки владельца.
- **Владелец выведен из эксплуатации** (сервиса больше нет, синхронизировать некому): его строки продолжают срабатывать или раз в минуту пишут ошибку строки, а API их не удаляет (правило «API не правит code-first строки», §2). Штатный путь — последний деплой сервиса с `ClearRecurring()`; если деплоить нечего — документированный SQL `DELETE FROM {schema}.recurring_jobs WHERE owner = '<owner>';` (тик, держащий строку, задерживает его на миллисекунды, §5.5). Если процесс этого владельца всё же жив, следующая синхронизация вставит строки заново — это видно по логу Information (§7). Отдельный API (`force`, удаление по владельцу) не вводится: он был бы исключением из того же правила ради разовой операции.
- **Объявлено, id занят другим владельцем или создан через API:** строка не трогается, лог Error. Перевод расписания из API в code-first — через `RemoveAsync`, затем синхронизация.
- **Разницы нет:** только чтение. Штатный случай для периодического прохода.
- **Каждая запись** — вставка, изменение, удаление — лог Information с id, владельцем и видом записи (для `UPDATE` — изменённые поля) после `COMMIT`; вставка, не вернувшаяся из `RETURNING`, в лог Information не идёт (она — коллизия). Штатный проход без разницы не пишет ничего; повторяющиеся каждую минуту вставки и удаления одного id — признак двух разных наборов у одного владельца.

Rolling deploy: старый и новый наборы могут чередоваться до ухода старых подов, затем сходятся к новому. Удалённая и снова вставленная строка теряет курсор и `paused`, поэтому рядом со сменой версии срабатывание может выпасть или повториться, а пауза удаляемого в новой версии расписания — сброситься при возврате старым подом. Окно ограничено временем выкатки; документируется.

## 7. Ошибки и наблюдаемость

Логи категории `PgBolt.Processing` (тикер, периодическая синхронизация) и `PgBolt.Postgres` (синхронизация на старте). Метрики (срабатывания, пропуски по наложению и misfire, ошибки, отставание `now() − min(wake_at)`, длительность тика) — часть 7.

| Ситуация | Поведение | Лог |
|---|---|---|
| Системная ошибка тика | откат пакета, повтор следующим тиком | Error |
| Ошибка строки (§5.2 п. 2): неизвестный тип (в том числе code-first до публикации `job_types` исполнителем), cron не разбирается, таймзоны нет на поде, исключение Cronos / планировщика | строка не ставится, `last_error`, `wake_at = now() + 1 мин`, курсор стоит; остальные строки пакета обрабатываются; после исправления пропущенное — по misfire-политике | Error на строку с исключением (не чаще раза в минуту — по `wake_at`) |
| Пропуск по наложению (`FireOnce` / `Skip`) | курсор сдвигается | Debug (счётчик — метрика части 7) |
| `Skip` отбросил просроченные срабатывания; `CatchUp` пропустил срабатывания старше окна | курсор сдвигается | Warning: id и с какого момента пропущено; только при фактическом сдвиге курсора — при конфликте `CatchUp + Skip` не повторяется на каждой перепроверке (§5.3) |
| `CatchUp + Skip` ждёт живой запуск | `wake_at = now() + 5 с` | Debug |
| `next_fire_at` стал `NULL` (у cron кончились срабатывания) | строка больше не срабатывает | Warning |
| Запись синхронизации: вставка, изменение, удаление строки (§6.2) | применена | Information: id, владелец, вид записи (для изменения — поля) |
| Коллизия id при синхронизации (включая параллельную вставку через API — `ON CONFLICT DO NOTHING`) | строка пропущена | Error |
| `40P01` синхронизации (§6.2) | повтор транзакции, до 3 раз | Warning |
| Ошибка синхронизации на старте | старт падает | — (исключение) |
| Ошибка периодической синхронизации | повтор через интервал | Error |
| `TriggerAsync` для типа, которого нет в `job_types` | `UnknownJobException` | — (исключение) |
| `GetAsync` / `TriggerAsync` на строке с нечитаемым значением, записанной в обход API (§4.2) | `InvalidOperationException` с id, полем и способом исправления | — (исключение) |

`last_error` хранит только ошибки строки и очищается успешным срабатыванием. Пропуски в него не пишутся.

## 8. Изменения core и worker

- **Миграции:** V001 и V002 объединяются в одну V001 с объектами части 4 (§3); `V002__maintenance_tasks.sql` удаляется. Тесты, ссылающиеся на версию 2 или `GetScript(fromVersion: 1)`, обновляются. `PgBoltInitializer`: проверка схемы предыдущих сборок (нет `recurring_jobs`) — падение старта (§3).
- **Enqueue (core §5.1):** шаблон параметризуется двумя дополнительными массивами `scheduled_at` и `recurring_id` в `src` и колонками в `INSERT INTO jobs`; `scheduled_at` пишется как `COALESCE(i.scheduled_at, now())`: тик передаёт момент cron, Trigger — `NULL` (время БД, §5.4). Внешний enqueue клиента использует прежний SQL; вариант с колонками — внутренний (тик, Trigger).
- **Dequeue (core §5.2):** `SELECT` возвращает `j.recurring_id, j.scheduled_at`; расширяются `LeasedJob` и создание `JobContext` в `JobRunner`.
- **`GetJob`:** `JobInfo.RecurringJobId`, `JobInfo.ScheduledAt`.
- **`JobContext`, `JobInfo`:** новые свойства в конце (§4.1).
- **Префикс `pgbolt:`** в `UniqueKey` — `ArgumentException` в клиенте (§4.4).
- **Requeue (core §5.9)** не меняется: ключ восстанавливается из `jobs.unique_key`; если жив новый запуск — `RequeueResult.Conflict`.
- **`PgBoltOptions`:** `RecurringOwner` (только имя), `AddRecurring(...)` (две перегрузки), `ClearRecurring()` (несовместим с `AddRecurring`).
- **`PgBoltProcessingOptions`:** `RunScheduler` (true), `SchedulerInterval` (1 с), `MisfireThreshold` (`TimeSpan?`; не задан — `max(1 мин, 5 × SchedulerInterval)`; явно заданный — `≥ 5 × SchedulerInterval`, §5.3), `RecurringSyncInterval` (1 мин); проверка интервалов — как у существующих.
- **`ProcessingMaintenance`:** запускается, если в процессе есть сервер и `RunMaintenance || RunScheduler || есть синхронизация` (§6.1); циклы promoter / sweeper / housekeeping — при `RunMaintenance`, `scheduler` — при `RunScheduler`, `recurring-sync` — при наличии синхронизации (`AddRecurring` или `ClearRecurring()`, §4.3).
- **`IServerStore`:** `TickRecurringAsync(int batchSize, …)` и `SyncRecurringAsync(owner, definitions, …)` — реализация в `PgBoltStore`, внутренний API.
- **Cronos** — зависимость `PgBolt.Postgres` (`Directory.Packages.props`).
- **Retention (часть 2)** `recurring_jobs` не касается; удаление партиций `jobs` на расписания не влияет (`last_job_id` может указывать на удалённую задачу — `GetAsync` клиента вернёт `null`).

## 9. Контракты для других частей

- **Часть 6 (Dashboard):** читает `recurring_jobs` целиком (список, `next_fire_at`, `last_*`, `paused`, `owner`); действия — через `IRecurringJobManager` (для code-first строк — только Pause / Resume / Trigger). История запусков расписания — по `jobs.recurring_id`: индекс по этой неизменяемой колонке (разрешён контрактом HOT) или иной путь выборки проектируется в части 6.
- **Часть 2 (оговорка):** часть 2 (§4, §11) указывает, что часть 4 использует `maintenance_tasks` через `TryClaim` / `Complete`. Это предположение отменено: тикер и синхронизация `maintenance_tasks` не используют и строк в ней не создают (§2, «Координация подов»); спека части 2 не меняется, действует эта оговорка. `TryClaim` / `Complete` остаются доступны будущим частям.
- **Часть 5 (Workflows):** recurring ставит только одиночные задачи; запуск batch по расписанию — задача-обёртка, которая создаёт batch.
- **Часть 7:** метрики §7; trace для срабатывания — новый корневой span (сейчас `trace_parent = NULL`).

## 10. Тестирование

**Unit (`RecurringPlanner`, без БД)** — табличные тесты по входу (строка, `db_now`, `T`) и выходу (срабатывания, курсор, `wake_at`):
- каждая misfire-политика × `Overlap`, включая конфликт при `CatchUp + Skip`;
- пустой `due`; ровно одно срабатывание; граница порога `Skip`; граница окна `CatchUp`; `CatchUp` × `Overlap` без срабатываний в окне → ничего, `after(db_now)`;
- `CatchUp + Allow`: лимит 100 на тик и продолжение со следующего срабатывания;
- поиск последнего из `due` (бинарный, §5.3): `FireOnce` после простоя больше суток для посекундного, почасового, еженедельного cron и раз в год; пачечный `* * * * 1 *` при простое 10 января – 1 марта → `ScheduledAt` = 31 января 23:59:59; число вызовов `GetNextOccurrence` ≤ 42 при простое 100 лет и 1 в штатном случае; срабатывание ровно в `db_now` входит в `due`; `Skip` сравнивает найденное с `db_now − T`;
- `CatchUp` с пропуском старше окна: Warning о пропуске — только в результате, сдвигающем курсор; при конфликте `CatchUp + Skip` его нет, после завершения запуска — один, диапазоном от старого `next_fire_at`;
- `wake_at = max(next_fire_at, db_now)`: `CatchUp` после частичного догона → `wake_at = db_now`; `next_fire_at = NULL` → `wake_at = NULL`;
- DST в `America/New_York`: переход вперёд и назад, фиксированное выражение и `*/15`;
- смена базового смещения: Europe/Moscow 2011-03-27 — `after(t) > t` и не убывает посекундно вокруг перехода, перебор и догон `CatchUp` без повторов одного срабатывания, поиск последнего из `due` верен; Europe/Moscow 2014-10-26 — повторяющийся час срабатывает один раз;
- cron с секундами; макросы `@daily`, `@every_second`; 4 и 7 полей → `ArgumentException`; `Queue = ""` → `ArgumentException`; таймзона Windows id → IANA;
- ошибка строки: неизвестная таймзона, неразбираемый cron → результат «ошибка строки», а не исключение;
- нормализация: `RetryDelays = 1,5 с` → 2 с, `CatchUpWindow` с тиками меньше микросекунды → усечён; повторная нормализация идемпотентна;
- валидация опций: явный `MisfireThreshold < 5 × SchedulerInterval` → исключение; не заданный при `SchedulerInterval = 15 с` → 75 с без исключения, при 1 с → 1 мин; `ClearRecurring()` вместе с `AddRecurring` → исключение;
- пересчёт курсора при смене cron: `c = after(max(db_now, last_fire_at))`, `last_fire_at` новее `db_now` (тик поставил срабатывание, совпадающее с новым cron) → совпадающее время не повторяется.

**Интеграция (PostgreSQL, как `PgBolt.Postgres.Tests` / `PgBolt.Server.Tests`):**
- 8 параллельных тикеров, посекундный cron `CatchUp + Allow` (при умолчании `Overlap = Skip` без воркера первая задача держала бы ключ, и следующие срабатывания законно пропускались бы; `FireOnce` при задержке тика > 1 с законно ставит только последнее), 10 с: ровно одна задача на каждую секунду интервала, без пропусков и дублей;
- `Overlap = Skip` с долгим запуском: следующее срабатывание пропущено, курсор сдвинут;
- `CatchUp + Skip`: срабатывания строго последовательны после завершения предыдущего, без пропусков;
- `CatchUp + Allow`: 250 пропущенных срабатываний догоняются за 3 тика; справедливость: 5 посекундных строк `CatchUp + Allow` после суток простоя и обычная строка, наступившая позже их `wake_at`, — обычная срабатывает в пределах критерия задержки §1, догон продолжается;
- ошибки строки: неизвестный тип, `time_zone = 'Mars/Olympus'` и неразбираемый cron (строки вставлены SQL'ем в обход API) — у каждой `last_error`, `wake_at ≈ now() + 1 мин`, курсор не сдвинут; остальные строки того же пакета сработали; строка с самым старым `wake_at` не откатывает следующие тики;
- умолчания: `NULL`-поля берутся из `job_types` запросом захвата, смена `job_types` видна следующему срабатыванию без ожидания TTL кэша;
- 1000 расписаний одного типа на одно время, `Overlap = Allow`: все поставлены, по 100 строк на транзакцию (проверяется одно срабатывание каждого);
- лимит пакета 1000 срабатываний: 15 строк `CatchUp + Allow` по 100+ пропущенных срабатываний — первая транзакция ставит ровно 1000 (10 строк), 5 строк не тронуты и срабатывают следующей транзакцией той же итерации;
- Pause / Resume для каждой политики; `paused` переживает `AddOrUpdate` и синхронизацию; `ResumeAsync` строки с `wake_at IS NULL` оставляет `NULL`;
- Trigger при живом запуске (`Created = false`, id живого); стресс Trigger + тик на одном id — без deadlock и дублей; `scheduled_at` Trigger'а — время БД (часы клиента сдвинуты); Trigger строки с неизвестным типом → `UnknownJobException`;
- `AddOrUpdate`: смена cron пересчитывает курсор, смена args — нет; code-first строка → `InvalidOperationException`; два параллельных `AddOrUpdate` нового id — одна вставка, второй повторяется и обновляет;
- конкуренция API и тика по одной строке (тик удерживается хуком между захватом и `COMMIT`): `AddOrUpdate` со сменой cron, совпадающего по времени со срабатыванием, которое тик ставит, — одна задача на это время; `Remove` — тик завершается, задача создана, строка удалена; `Pause` — тик завершается, следующие тики строку не берут;
- синхронизация: add / update / remove, отсутствие записей без разницы (`updated_at` не меняется при `RetryDelays = 1,5 с` и `CatchUpWindow` с долями микросекунды), коллизии, две параллельные синхронизации одного владельца, сходимость при чередовании двух наборов; параллельный `AddOrUpdate` того же id через API во время синхронизации на старте — старт не падает, коллизия в логе; смена cron при удерживаемом тиком захвате — без дубля; две синхронизации разных владельцев с пересекающимися id, объявленными в обратном порядке, параллельно — без `40P01`, коллизии в логе; каждая вставка, изменение и удаление — лог Information с id и владельцем, проход без разницы — без логов; code-first расписание с типом, которого ещё нет в `job_types`, — старт не падает, ошибка строки в тике, после публикации типа исполнителем строка срабатывает;
- участие: `ClearRecurring()` удаляет строки владельца (лог Information на каждую); явный `RecurringOwner` без `AddRecurring` и `ClearRecurring` строк не трогает (api-процесс рядом с worker того же владельца: строки worker'а не удаляются, `paused` и курсор сохраняются); владелец по умолчанию без объявлений строк не трогает; при `RunMaintenance = false`, `RunScheduler = false` и объявлениях цикл `recurring-sync` работает;
- зарезервированный префикс отклоняется в `EnqueueAsync` и `uniqueKeySelector`;
- Requeue упавшей recurring-задачи при живом новом запуске того же расписания (`Overlap = Skip`) → `RequeueResult.Conflict`; после его завершения — успешный Requeue;
- схема: чистая установка создаёт `recurring_jobs` с индексами и колонки `recurring_id`, `scheduled_at` во всех партициях `jobs` (сегодня и будущие); `LatestVersion = 1`; схема предыдущей сборки (версия 1 или 2 в `schema_version`, `recurring_jobs` нет) — старт падает с сообщением о пересоздании, при `AutoMigrate = true` и `false`;
- через PgBouncer в transaction mode: тик, синхронизация, API.

**E2E:** recurring-задача из `E2E.Contracts`, объявленная code-first, исполняется в JobsA; `JobContext.ScheduledAt` и `RecurringJobId` доходят до исполнителя; `GetAsync` показывает `last_fire_at` / `last_job_id`.

## Изменения части 5 (Workflows)

Подробности — `2026-10-02-pgbolt-workflows-design.md` (§9).
- §3: детектор пре-релизной схемы проверяет и `batches`.
- §4.4: префикс `pgbolt:` отклоняется и в `StepOptions.UniqueKey`.

## Изменения части 6 (Dashboard)

Подробности — `2026-10-03-pgbolt-dashboard-design.md` (§3.2, §6.1).
- §9: история запусков расписания — по индексу `jobs_recurring (recurring_id, id)`.
