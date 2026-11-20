# PgBolt — часть 3b: кластерный лимит и веса очередей

Статус: реализовано, план `docs/superpowers/plans/2026-10-12-pgbolt-queue-limits.md` · Дата: 2026-10-10 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»), `2026-09-28-pgbolt-worker-design.md` (далее «worker»), `2026-10-03-pgbolt-dashboard-design.md` (далее «часть 6»)

## 1. Цель и рамки

Ограничить число одновременно выполняемых задач очереди на весь кластер (защита внешнего ресурса: API партнёра, чужая БД), дать оператору менять этот лимит и ставить очередь на паузу без деплоя, а общему пулу сервера — делиться между очередями по весам вместо строгого порядка; вес очереди оператор тоже меняет без деплоя.

**Входит:**
- таблица `queue_settings`: значения лимита и веса по умолчанию из кода и независимые переопределения из БД, приоритетные над ними (§3, §4, §5);
- `QueueOptions.ClusterMaxConcurrency` и `QueueOptions.Weight`, синхронизация значений по умолчанию при старте процесса (§4);
- `IQueueSettingsManager`: чтение настроек, переопределение лимита (число, «без лимита», 0 = пауза) и веса (число, «без веса»), сброс каждого к значению из кода; необязательный кэш в памяти (§5);
- dequeue лимитированной очереди под `pg_advisory_xact_lock` с подсчётом взятых строк (§6);
- стратегия общего пула smooth weighted round-robin по эффективным весам очередей сервера, выбор стратегии и смена весов на лету (§7);
- анализатор: BOLT010 (разные константные лимиты или веса одной очереди), BOLT011 (недопустимые константные значения опций) (§4.3);
- dashboard: колонки и действия лимита и веса на экране очередей, сброс кэша настроек (§8).

**Не входит:**
- межпроцессная доставка изменений настроек (шина; `LISTEN/NOTIFY` исключён core §5.10 и roadmap): изменения подхватываются по интервалу обновления (§6.1) и TTL кэша (§5.2);
- история переопределений: хранится только текущее;
- распределение очереди между серверами по весу: вес делит общий пул каждого сервера отдельно (§7.1);
- лимит по типу задачи или по аргументам — только по очереди.

### Критерии успеха
- Число строк очереди с `lease_owner IS NOT NULL` никогда не превышает эффективный лимит из-за dequeue, если лимит действовал на момент dequeue и очередь была в списке лимитированных всех процессов, выполнявших dequeue (§6.1); проверяется конкурентным интеграционным тестом.
- Dequeue очередей без лимита не меняется: тот же один statement core §5.2, без новых обращений к БД; это относится и к очередям со строкой `queue_settings` только с весом (§6.1).
- Число индексов `queue` не растёт (замена `queue_owner` на `queue_leased`, §3.2); подсчёт взятых строк — index-only scan.
- Dequeue лимитированной очереди — один round trip и одна транзакция; работает через PgBouncer в transaction mode.
- Сервер, у очередей которого нет эффективного веса, распределяет общий пул как до части 3b — в строгом порядке объявления (worker §4.2); внутри плана общий пул ограничен бюджетом плана (§7.2).

## 2. Принятые решения

| Решение | Значение | Почему |
|---|---|---|
| Источник лимита и веса | Значение по умолчанию в коде, переопределение в БД приоритетнее; у лимита и веса — независимо | Код — воспроизводимая конфигурация, не теряется при пересоздании схемы; переопределение — срочная реакция на инцидент без деплоя |
| Имя таблицы | `queue_settings` | `queues` неотличимо от таблицы задач `queue` |
| Механизм лимита | `pg_advisory_xact_lock` отдельным statement'ом + `count(*)` взятых строк в dequeue | Меняет только dequeue лимитированных очередей. Таблица токенов-слотов дала бы точный учёт, но затронула бы все пути финализации (ack, fail, sweeper, cancel, release); счётчик в строке — горячая строка и дрейф при падениях |
| Lock отдельным statement'ом | Batch из двух statement'ов в неявной транзакции | В READ COMMITTED snapshot statement'а берётся до захвата lock: подсчёт в одном statement'е с lock не увидел бы строки, закоммиченные конкурентом, пока шло ожидание, и лимит превышался бы. Следующий statement берёт новый snapshot уже после коммита предыдущего держателя |
| Тип advisory lock | Только транзакционный (`_xact_`) | Снимается на COMMIT/ROLLBACK; сессионный остаётся на серверном соединении PgBouncer после возврата его в пул |
| Какие очереди лимитированы, веса очередей | Кэш процесса, обновление раз в `QueueSettingsRefreshInterval` (10 с) | Очереди без лимита не платят ни statement'ом. Значение лимита читается в SQL на каждом dequeue — изменение числа действует сразу; с задержкой — только появление/снятие лимита у очереди и смена веса |
| Пауза | Переопределение `0` | Тот же механизм (`LIMIT 0`), без отдельного флага и протокола |
| Подсчёт | `count(*)` по `queue_leased` | Строки с истёкшим lease упавшего пода считаются занятыми до sweeper'а: лимит недобирается, но не превышается |
| Индекс | `queue_owner (lease_owner)` → `queue_leased (queue)` с тем же предикатом | `queue_owner` использует только список Processing dashboard (часть 6 §6.1), heartbeat идёт по PK, sweeper — по `queue_expiry`; новый индекс служит и ему (с фильтром по очереди), и подсчёту |
| Веса | Опция поверх строгого порядка; есть эффективный вес хотя бы у одной очереди сервера — гладкий взвешенный round-robin (SWRR) на уровне слотов в форме stride scheduling (§7.3) | Текущее поведение не меняется по умолчанию; учёт по факту взятия слота даёт точные доли при любом числе свободных слотов и любом разбиении плана на записи, пропорциональная делёжка — перекос округления при 1–2 слотах |
| Вес — чья настройка | Очереди на весь кластер (`queue_settings`), а не пула одного сервера; каждый сервер делит по нему свой общий пул | Переопределение одной записью действует на всех серверах очереди; веса, опубликованные сервером при регистрации, устаревали бы после переопределения |
| Смена весов на лету | На границе плана под lock `SlotAllocator`; изменился вектор весов очередей сервера — новый экземпляр стратегии (`pass` = 0) | Прошлые `pass` посчитаны в шагах старых весов, пересчёт не имеет точного смысла; сброс забывает лишь отставание и опережение в пределах двух кругов, а старт всех очередей с 0 залпа не даёт (§7.3) |
| Проверки конфигурации | Анализатор для константных значений + валидация при старте | Анализатор не видит неконстантных значений и других сборок — гарантию даёт старт |
| Кэш менеджера | Свой, на `TimeProvider`, выключен по умолчанию | Без зависимости `Microsoft.Extensions.Caching.Memory`; dequeue и сервер его не используют, чтобы TTL не задерживал паузу |
| Миграции | Изменения вносятся в `V001__initial.sql` | Как в частях 4–6: пользователей нет, схемы прошлых сборок отвергаются при старте |

## 3. Схема (добавляется в `V001__initial.sql`)

### 3.1 `queue_settings`

```sql
CREATE TABLE {schema}.queue_settings (
    queue                           text PRIMARY KEY,
    default_max_concurrency         int,            -- из кода (§4.2); NULL — без лимита
    default_weight                  int,            -- из кода (§4.2); NULL — без веса
    default_published_by            text,           -- оба значения по умолчанию пишутся вместе
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
```

Эффективные лимит и вес:

```sql
CASE WHEN override_concurrency_set THEN override_max_concurrency ELSE default_max_concurrency END
CASE WHEN override_weight_set THEN override_weight ELSE default_weight END
```

- Переопределения лимита и веса независимы: у каждого свои признак, значение, автор и время; запись и сброс одного не трогают другое (§5.1). Значения по умолчанию пишутся только синхронизацией и всегда вместе (§4.2), поэтому `default_published_by` и `default_updated_at` у них общие.
- Строка существует, только если у очереди когда-либо было значение по умолчанию не `NULL` (лимита или веса) или переопределение. Отсутствие строки и эффективный `NULL` значат одно: лимита (веса) нет.
- Строки не удаляются ни синхронизацией (§4.2), ни сбросом переопределений (§5.1). Таблица мала (по строке на очередь с лимитом или весом), retention не нужен. Строка очереди, переименованной или убранной из кода, остаётся: на dequeue других очередей не влияет, dashboard её не показывает, если у неё нет переопределения, задач и серверов (§8.1); удалить — вручную SQL (§9).

### 3.2 Индекс взятых строк

```sql
-- вместо queue_owner ON {schema}.queue (lease_owner) WHERE lease_owner IS NOT NULL
CREATE INDEX queue_leased ON {schema}.queue (queue) WHERE lease_owner IS NOT NULL;
```

Предикат тот же, поэтому стоимость поддержки на горячем пути не меняется: строка входит в индекс при dequeue и выходит при ack / fail / release / sweeper, как раньше. Ключ `queue` не меняется у строки никогда.

### 3.3 `servers`

Не меняется (DDL — core §4.6): веса не публикуются при регистрации сервера — опубликованные значения устаревали бы после переопределения (§5.1). `ServerRegistration` и `PgBoltStore.RegisterServerAsync` не меняются; эффективный вес dashboard берёт из `queue_settings` (§8.1).

## 4. Конфигурация в коде

### 4.1 `QueueOptions`

| Свойство | По умолчанию | Смысл | Валидация |
|---|---|---|---|
| `ClusterMaxConcurrency` | `null` (нет) | Значение по умолчанию кластерного лимита очереди (§6) | `null` или ≥ 1 |
| `Weight` | `null` (нет) | Значение по умолчанию веса очереди — настройки очереди на весь кластер: каждый сервер очереди делит по нему свой общий пул (§7) | `null` или 1…1000 |

Остальные свойства worker §3.3 не меняются.

### 4.2 Синхронизация значений по умолчанию

Новый шаг старта `PgBoltProcessingService.StartAsync` (worker §4.4) — после публикации `job_types` (п. 2), до регистрации серверов (п. 3), один раз на процесс:

1. Собрать по всем серверам процесса тройки `(queue, ClusterMaxConcurrency, Weight)` — по одной на очередь (ordinal): очередь на нескольких серверах с одинаковыми значениями даёт одну тройку. Одна очередь с разным `ClusterMaxConcurrency` или разным `Weight` на разных серверах процесса — ошибка старта (`InvalidOperationException` с именами серверов, опцией и значениями — как вся валидация опций, `PgBoltServerOptions.Invalid`). `null` на одном сервере и число на другом — тоже различие.
2. Upsert в `queue_settings` одним statement'ом (`unnest` массивов, упорядоченных по `queue`; дублей нет — иначе `ON CONFLICT DO UPDATE` упал бы на повторной строке; 40P01 при одновременном старте процессов повторяет `DbRetry`), `published_by` — как у `job_types` (`{MachineName}/{ProcessId}`):
   - хотя бы одно значение не `NULL` → `INSERT … ON CONFLICT (queue) DO UPDATE SET default_max_concurrency, default_weight, default_published_by, default_updated_at`;
   - оба `NULL` → только `UPDATE` существующей строки: `default_max_concurrency = NULL`, `default_weight = NULL`, `default_published_by`, `default_updated_at` — как в первой ветке; строка не создаётся;
   - колонки `override_*` statement не трогает никогда;
   - statement возвращает только строки для предупреждения п. 3 (`QueueDefaultConflict`: очередь, прежние лимит, вес и `default_published_by`): условие п. 3 считается в SQL по CTE со строками до upsert.
3. Warning в лог на очередь (last writer wins при расхождении процессов): прежнее значение лимита или веса отличалось (в сообщении — какое), прежний `default_published_by` не `NULL` и не равен своему, а прежний `default_updated_at` моложе 5 минут — то же окно свежести, что у расхождения схем `job_types`. Смена значения деплоем после давно стартовавших подов предупреждения не даёт; даёт — старт старого и нового кода вперемешку (rolling deploy, перезапуск старого пода). Строка, созданная переопределением (`default_published_by IS NULL`: код значения ещё не публиковал), предупреждения не даёт.
4. Загрузить настройки очередей: множество лимитированных очередей и эффективные веса (§6.1).

Ошибка шагов — ошибка старта хоста с понятным сообщением (как п. 1–3 worker §4.4): без списка лимитированных очередей сервер нарушил бы лимит с первого dequeue. Загрузка завершается до создания серверов, поэтому первый план каждого диспетчера уже видит веса (§7.1).

Процесс без `AddPgBoltServer` (только клиент или dashboard) синхронизацию не выполняет.

### 4.3 Анализатор

Добавляются в `PgBoltAnalyzer` (`PgBolt.Generators`), записываются в `AnalyzerReleases.Unshipped.md`. Типы опций ищутся по metadata name — `PgBolt.Server.QueueOptions`, `PgBolt.Server.PgBoltServerOptions`, `PgBolt.Postgres.PgBoltOptions`; отдельный объект анализа создаётся, только если нужные типы найдены (как `WorkflowSubmitAnalysis`), без этих сборок в компиляции правила молчат. BOLT011 по `PgBoltOptions` не требует `PgBolt.Server`.

**BOLT010 (Error) — разные кластерные лимиты или веса одной очереди.** Дескриптор с тегом `WellKnownDiagnosticTags.CompilationEnd`; на уровне компиляции (`RegisterCompilationEndAction`, как BOLT002/BOLT004/BOLT006 в `AnalysisState.OnCompilationEnd`) собираются вызовы `PgBoltServerOptions.Queue(name, configure)` с константным `name`. Опции `ClusterMaxConcurrency` и `Weight` проверяются независимо, по одним правилам; значение опции вызова:
- **`null`** — `configure` не передан (параметр по умолчанию) или передан литерал `null`/`default`; либо `configure` — лямбда «из простых присваиваний» без присваивания этой опции;
- **значение** — лямбда «из простых присваиваний» с присваиванием опции константы (включая `null`); при нескольких — последнее;
- **неизвестно** — всё остальное: method group, переменная, поле, параметр или другое выражение-делегат; лямбда с иными операторами (условие, вызов метода отдельным оператором, цикл, локальная функция, `return`); неконстантное значение опции. Вызов с неизвестным значением опции в сравнении по ней не участвует.

Лямбда «из простых присваиваний» — тело-выражение или блок, каждый оператор которого — присваивание свойству параметра лямбды (`q.X = …`), а правая часть не упоминает параметр. Если для одного имени среди известных значений опции есть разные — ошибка на каждом вызове с известным значением (на присваивании опции, без него — на аргументе имени) с именем опции и указанием места другого значения; расхождение по обеим опциям — две диагностики.

Не ловятся (их проверяет старт, §4.2): неконстантное имя, неизвестное значение, вызовы в разных сборках. Диагностика срабатывает и для серверов, которые в рантайме окажутся в разных процессах (один проект — разные хосты по конфигурации): такая конфигурация — last writer wins с предупреждением (§4.2), поэтому ошибка компиляции оправдана и для неё.

**BOLT011 (Error) — недопустимое константное значение опции.** На присваивании константы:

| Опция | Ошибка, если |
|---|---|
| `QueueOptions.Reserved` | < 0 |
| `QueueOptions.MaxConcurrency` | ≤ 0; или < `Reserved`, если оба присвоены константами в одной лямбде |
| `QueueOptions.ClusterMaxConcurrency` | ≤ 0 |
| `QueueOptions.Weight` | ≤ 0 или > 1000 |
| `PgBoltOptions.QueueSettingsCacheDuration` | ≤ 0: `TimeSpan.Zero`, `TimeSpan.From*(<константа ≤ 0>)`, `new TimeSpan(<константы>)` с результатом ≤ 0 |

Ноль у `QueueSettingsCacheDuration` — ошибка: кэш отключается значением `null`, «кэш на 0 с» двусмыслен. Валидация при старте проверяет то же для любых значений.

## 5. Переопределение: `IQueueSettingsManager`

### 5.1 API (`PgBolt.Abstractions`, реализация — `PgBolt.Postgres`)

```csharp
public interface IQueueSettingsManager
{
    ValueTask<IReadOnlyList<QueueSettings>> ListAsync(CancellationToken ct = default);
    ValueTask<QueueSettings?> GetAsync(string queue, CancellationToken ct = default);

    /// <param name="maxConcurrency">null — без лимита, 0 — пауза, ≥ 1 — лимит.</param>
    ValueTask SetConcurrencyOverrideAsync(string queue, int? maxConcurrency, string? updatedBy = null, CancellationToken ct = default);

    /// <summary>Снять переопределение лимита: действует значение из кода.</summary>
    ValueTask ResetConcurrencyOverrideAsync(string queue, CancellationToken ct = default);

    /// <param name="weight">null — без веса, иначе 1…1000.</param>
    ValueTask SetWeightOverrideAsync(string queue, int? weight, string? updatedBy = null, CancellationToken ct = default);

    /// <summary>Снять переопределение веса: действует значение из кода.</summary>
    ValueTask ResetWeightOverrideAsync(string queue, CancellationToken ct = default);

    /// <summary>Сбросить кэш этого процесса (§5.2); без кэша — ничего не делает.</summary>
    void InvalidateCache();
}

public sealed record QueueSettings(
    string Queue,
    int? DefaultMaxConcurrency,
    int? DefaultWeight,
    string? DefaultPublishedBy,
    bool OverrideConcurrencySet,
    int? OverrideMaxConcurrency,
    string? OverrideConcurrencyUpdatedBy,
    DateTimeOffset? OverrideConcurrencyUpdatedAt,
    bool OverrideWeightSet,
    int? OverrideWeight,
    string? OverrideWeightUpdatedBy,
    DateTimeOffset? OverrideWeightUpdatedAt)
{
    public int? EffectiveMaxConcurrency => OverrideConcurrencySet ? OverrideMaxConcurrency : DefaultMaxConcurrency;
    public bool Paused => EffectiveMaxConcurrency == 0;
    public int? EffectiveWeight => OverrideWeightSet ? OverrideWeight : DefaultWeight;
}
```

- Регистрируется `AddPgBolt` (singleton), доступен в любом процессе: клиент, сервер, dashboard.
- `SetConcurrencyOverrideAsync` / `SetWeightOverrideAsync` — upsert: для очереди без строки создаётся строка с `default_* = NULL`. Так ставится на паузу, ограничивается или получает вес очередь, которую код не объявлял лимитированной или взвешенной. Пишет `override_concurrency_set = true` и значение (соответственно `override_weight_set`, `override_weight`), автора `updatedBy` и время `now()` в колонки своей настройки; колонки другой настройки не трогает.
- `ResetConcurrencyOverrideAsync` — `override_concurrency_set = false, override_max_concurrency = NULL`, автор и время лимита сбрасываются; `ResetWeightOverrideAsync` — то же для `override_weight_*`. Строка не удаляется; нет строки — ничего не делает.
- Валидация: `queue` — не `null` и не пустая/пробельная строка (как `s.Queue(…)`), `maxConcurrency` — `null` или ≥ 0, `weight` — `null` или 1…1000; иначе `ArgumentException` / `ArgumentOutOfRangeException`.
- Пауза затирает предыдущее ручное переопределение лимита: возобновление (`ResetConcurrencyOverrideAsync` или новый `SetConcurrencyOverrideAsync`) не восстанавливает прежнее число.
- Действие переопределения на серверах — §6.1: число и пауза для уже лимитированной очереди — со следующего dequeue; появление/снятие лимита у очереди и смена веса — в пределах `QueueSettingsRefreshInterval` (вес — с ближайшей границы плана после обновления, §7.1), а на серверах процесса, выполнившего запись, — сразу: после записи менеджер вызывает `IQueueSettingsRefresher.RefreshNow()` (§6.1), если он зарегистрирован (менеджер берёт его из `IServiceProvider` при вызове: `AddPgBoltServer` может идти после `AddPgBolt`).

### 5.2 Кэш

`PgBoltOptions.QueueSettingsCacheDuration` (`TimeSpan?`, по умолчанию `null` — без кэша; явное значение > 0 и ≤ 1 сут).

- Кэшируется снимок `ListAsync` целиком; `GetAsync` отвечает из снимка. Время — `TimeProvider`.
- Промах: один запрос к БД; параллельные промахи ждут его же (`Task` в полёте), ошибка не кэшируется. Общий `Task` запускается без токена вызывающего (отмена одного не обрывает загрузку для остальных); каждый вызывающий ждёт его через `WaitAsync(ct)`.
- **Поколение.** Кэш хранит счётчик поколений; `InvalidateCache()` и запись увеличивают его и забывают `Task` в полёте. Загрузка запоминает поколение при старте; результат кладётся в кэш, только если поколение не изменилось, — иначе снимок, прочитанный до записи, жил бы до конца TTL. Вызывающий, пришедший после инвалидации, начинает новую загрузку, а не ждёт старую.
- Методы записи (`Set…OverrideAsync`, `Reset…OverrideAsync`) этого процесса сбрасывают кэш и вызывают `RefreshNow()` (§5.1) в `finally` — и после ошибки: запись могла закоммититься (таймаут или обрыв после коммита). Изменения из других процессов видны через TTL или после `InvalidateCache()`.
- Dequeue и настройки очередей сервера (§6.1) этот кэш **не используют**: иначе TTL задержал бы паузу и смену веса.

## 6. Кластерный лимит в dequeue

### 6.1 Настройки очередей процесса

`QueueSettingsCache` (internal, на процесс, `PgBolt.Server`, singleton из `AddPgBoltServer`) держит неизменяемый снимок `QueueSettingsSnapshot(FrozenSet<string> Limited, FrozenDictionary<string, int> Weights)`: множество очередей с эффективным лимитом не `NULL` (включая паузу 0) и эффективные веса очередей, у которых он не `NULL`. Снимок читается одним запросом из `queue_settings` в обход кэша менеджера (`PgBoltStore.GetActiveQueueSettingsAsync`, §10). Обновление строит новый снимок и публикует ссылку на него (`Volatile.Write`); читатели берут `Current` без lock'а и работают с одним снимком целиком. Правило построения снимка: `Limited` — очереди с эффективным лимитом не `NULL`, вес на членство не влияет (строка `queue_settings` только с весом — очередь не лимитирована: «строка есть ≠ лимитирована»); `Weights` — очереди с эффективным весом не `NULL`, лимит не влияет. Эффективное значение — переопределение, если оно задано (`override_*_set`), в том числе «без лимита» / «без веса» поверх значения из кода, иначе значение из кода. Читают: стратегия выборки, которую создаёт `PgBoltServer` (сейчас `new DequeueFetchStrategy(…)` в его конструкторе), — `Limited` на каждом dequeue; `SlotAllocator` каждого сервера — `Weights` на границе плана (§7.1). Поэтому кэш создаётся и загружается до создания серверов.

- Первая загрузка — шаг старта (§4.2 п. 4); ошибка — ошибка старта.
- Обновление — фоновый цикл раз в `PgBoltProcessingOptions.QueueSettingsRefreshInterval` (по умолчанию 10 с, ±10 %; > 0 и ≤ 1 сут). Ошибка — Warning, остаётся последний известный снимок, повтор через интервал.
- **Жизненный цикл цикла.** Стартует первым в п. 6 worker §4.4 — до `ProcessingMaintenance` и серверов. Останавливается в `StopAsync` (worker §6.4) после `FlushAndStopAsync` серверов, то есть после завершения диспетчеров и их in-flight dequeue; локальный цикл останавливается и при отменённом токене хоста. Ветка ошибки п. 6 вызывает тот же `StopAsync` и останавливает цикл вместе с остальным запущенным.
- `RefreshNow()` — внеочередное обновление (запись менеджера этого процесса, §5.1; кнопка dashboard, §8.1); не ждёт его. Если обновление уже в полёте, ставит ещё одно сразу после него (не больше одного отложенного): обновление в полёте могло прочитать состояние до изменения, ради которого вызван метод. До старта цикла и после его остановки — no-op без исключений (первую загрузку делает старт, §4.2 п. 4).
- `RefreshNow()` доступен dashboard через internal-интерфейс `IQueueSettingsRefresher { void RefreshNow(); }` в `PgBolt.Postgres` (у сборки есть `InternalsVisibleTo` для `PgBolt.Server` и `PgBolt.Dashboard`; `QueueSettingsCache` internal в `PgBolt.Server`, а dashboard ссылается только на `PgBolt.Postgres`). `AddPgBoltServer` регистрирует `QueueSettingsCache` и как `IQueueSettingsRefresher`; dashboard берёт его через `GetService` — отсутствие (dashboard в процессе без сервера) допустимо.
- Цикл работает при любом `RunMaintenance`: это часть выборки, а не обслуживания.

Свойства (лимит; веса — §7.1):
- очередь в множестве → лимитированный dequeue (§6.2); вне множества → dequeue core §5.2;
- лимит снят, а очередь ещё в множестве → лимитированный dequeue с `lim = NULL` берёт `@n`: корректно, лишний lock до следующего обновления;
- лимит появился, а очереди ещё нет в множестве → до `QueueSettingsRefreshInterval` dequeue этого процесса лимит не соблюдает. Лимит действует со следующей выборки после обновления снимка; dequeue без лимита, начатый по старому снимку, ещё может вернуть задачи (не позже своего таймаута `HeartbeatInterval`, worker §4.3). Осознанное допущение, документируется в XML-doc `ClusterMaxConcurrency` и `SetConcurrencyOverrideAsync`.

### 6.2 SQL

`IFetchStrategy` (worker §4.1) выбирает вариант по `QueueSettingsCache.Current.Limited`; лимитированный — новый публичный метод `PgBoltStore.DequeueLimitedAsync(queue, max, serverId, lease, ct)` → `Task<LimitedDequeueResult>`, `public sealed record LimitedDequeueResult(IReadOnlyList<LeasedJob> Jobs, bool LimitReached)` (`LimitReached` — §6.3), и `IServerStore.DequeueLimitedAsync` с реализацией в `PgBoltServerStore`. Один `NpgsqlBatch` из двух команд с одним `Sync` — неявная транзакция, один round trip. Отдельный хелпер не нужен: образец — батч `PgBoltStore.GetJob` (`NpgsqlBatch` без `BeginTransaction`, подготовка по `_prepare`, как у остальных команд; `NextResultAsync` — мимо результата `pg_advisory_xact_lock`); `RecurringSync` образцом не служит — у него явная транзакция, копия добавила бы COMMIT отдельным round trip'ом. Повтор — `DbRetry`, как у `DequeueAsync`. XML-doc класса («каждая операция — один statement») уже не точен для батчей частей 4–5 — уточняется: «один round trip».

```sql
-- 1
SELECT pg_advisory_xact_lock(hashtextextended('pgbolt:{schema}:' || @queue, 0));

-- 2
WITH s AS (
    SELECT CASE WHEN override_concurrency_set THEN override_max_concurrency ELSE default_max_concurrency END AS lim
    FROM {schema}.queue_settings WHERE queue = @queue
),
k AS (
    SELECT count(*) AS leased FROM {schema}.queue
    WHERE queue = @queue AND lease_owner IS NOT NULL               -- queue_leased, index-only
),
r AS (
    SELECT (SELECT lim FROM s) - (SELECT leased FROM k) AS room  -- NULL — без лимита
),
c AS (
    SELECT job_id, row_number() OVER (ORDER BY priority DESC, run_at) AS rn
    FROM (
        SELECT job_id, priority, run_at FROM {schema}.queue
        WHERE queue = @queue AND lease_owner IS NULL AND ready
          AND coalesce((SELECT room FROM r) > 0, true)          -- мест нет — кандидаты не читаются
        ORDER BY priority DESC, run_at
        LIMIT @n
        FOR UPDATE SKIP LOCKED
    ) t
),
l AS (
    UPDATE {schema}.queue q
    SET …                                                       -- как core §5.2
    FROM c, r
    WHERE q.job_id = c.job_id AND (r.room IS NULL OR c.rn <= r.room)
    RETURNING q.job_id, q.lease_id, q.attempt
)
SELECT r.room, x.* FROM r LEFT JOIN (
    SELECT … FROM l JOIN {schema}.jobs j ON j.id = l.job_id     -- столбцы core §5.2
) x ON true;
```

- `LIMIT @n` — параметр, как в core §5.2, а отсечение по `room` — в `l`: `c` берёт и блокирует до `@n` кандидатов, `l` обновляет первые `room` из них; нет строки или эффективный `NULL` → `room IS NULL`, берутся все `@n`. `LIMIT` с подзапросом (`least(@n, room)`) не годится: для него планировщик закладывает 10 % строк очереди, и `l` с финальным join уходят в Hash Join с Seq Scan по всей `queue` и всем партициям `jobs`. Проверено `EXPLAIN ANALYZE` на PG 16 (схема `V001__initial.sql` с `queue_leased`, 300 тыс. готовых строк очереди, `jobs` — 10 партиций, 1,4 млн строк): `LIMIT` с подзапросом — 420–555 мс, форма выше — 0,4 мс при `@n` = 3 и 16 мс при `@n` = 1000; везде Nested Loop по PK `queue` и партициям `jobs` (runtime pruning), подсчёт — Index Only Scan по `queue_leased`, `c` — Index Scan по `queue_fetch`. С подготовленными statement'ами (`UsePreparedStatements`) остаются custom-планы, как у core §5.2: generic-план с `LIMIT` параметром оценивается на порядки дороже (11 исполнений — 11 custom).
- Лишние кандидаты (не больше `@n − room`) остаются заблокированы `FOR UPDATE` до конца неявной транзакции — коммита батча сразу после statement'а 2 (проверено: после коммита все они снова доступны `SKIP LOCKED`); конкурентный dequeue без лимита их пропускает, лимитированный и так ждёт advisory lock. При `room ≤ 0` (лимит исчерпан, пауза) однократный фильтр `coalesce(room > 0, true)` отключает чтение `queue_fetch` — ничего не блокируется.
- `LEFT JOIN` от одной строки `r`: `room` приходит и когда ничего не взято (одна строка с `NULL` вместо задачи, reader её пропускает); `room` — `bigint` (`int` лимита минус `count(*)`). `LimitReached = room IS NOT NULL AND взято ≥ room` — после этого dequeue мест в лимите нет (включая паузу: `room ≤ 0`).
- **Почему лимит не превышается.** Все лимитированные dequeue очереди сериализованы lock'ом. Держатель отпускает его на COMMIT, после того как коммит виден; statement 2 следующего держателя берёт snapshot после захвата lock и видит все строки, взятые предыдущими. Строки, освобождённые конкурентно (ack, fail, sweeper), только уменьшают реальное число — подсчёт может лишь переоценить занятость.
- **Почему не в одном statement'е.** Snapshot statement'а берётся до выполнения его функций; `count` после `pg_advisory_xact_lock` в том же statement'е не видел бы коммит держателя, ожидание которого и было причиной блокировки. Тест §11 фиксирует это поведение на PG 16.
- Ключ lock — `bigint` от схемы и имени очереди. Коллизия хэшей двух очередей лишь сериализует их dequeue; коллизия с advisory lock приложения — то же.
- Сессионный `pg_advisory_lock` в проекте запрещён (снимается только с сессией: в PgBouncer transaction mode остаётся на чужом серверном соединении). Транзакционный снимается на `Sync`-коммите и на откате при ошибке statement'а 2; соединение занято столько же, сколько обычным dequeue.
- Таймаут — общий таймаут dequeue `HeartbeatInterval` (worker §4.3), покрывает и ожидание lock. Npgsql при отмене батча отменяет всю неявную транзакцию.
- Пропускная способность лимитированной очереди ограничена сериализацией: ≈ 1 / (время удержания lock) dequeue в секунду на кластер, каждый до `@n` задач. Для очередей с лимитом (единицы–десятки одновременных задач) это не ограничение; бенчмарк — часть 7.

### 6.3 Поведение диспетчера

`IFetchStrategy.FetchAsync` возвращает `LimitedDequeueResult(Jobs, LimitReached)` (отдельного типа результата нет; у нелимитированного варианта `LimitReached = false`); диспетчер хранит признак в состоянии очереди и обновляет его каждым dequeue очереди. Без признака всё как в worker §4.3. Пока признак установлен:
- **Результат dequeue — как пустой**, сколько бы строк ни взято: `interval = min(interval × 2, Max)`, `nextPoll = now + interval × (1 ± 0,1)`. В том числе при `room = @n` (`jobs.Count ≥ n`): мест больше нет, немедленный повтор взял бы 0 под lock.
- **`signaled[q]` поглощается** без изменения `nextPoll` и `interval`. Раньше очередь с занятыми локальными слотами не попадала в план (`Available = 0`); у очереди с исчерпанным кластерным лимитом локальные слоты свободны, и сигнал (enqueue этого процесса, `TriggerAsync`, promoter и resolver `ProcessingMaintenance`) ставил бы `nextPoll = now`, `interval = Min` — при потоке enqueue сервер делал бы dequeue под lock каждую итерацию, сериализуя dequeue очереди на кластере, а таймаут ожидания lock ставил бы на паузу 1 → 30 с весь сервер (§9).
- **`slotFreed[q]` поглощается.** Слот освобождается до коммита исхода: `PgBoltServer.RunExecutionAsync` вызывает `SlotAllocator.Release` и `OnSlotFreed`, а затем сдаёт исход в `CompletionSink`, ack которого уходит пакетом через `AckFlushInterval`. Немедленный dequeue по `slotFreed` видел бы строку ещё взятой: 0 строк, удвоение `interval` — за несколько таких опросов сервер у лимита уходил бы к `MaxPollInterval`, а освободившиеся места ждали бы планового опроса.
- **Новый сигнал `leaseFreed[q]` ставит `nextPoll = now`** (`interval` не меняется). `CompletionSink` получает колбэк `Action<string> onLeaseFreed` и вызывает его с очередью выполнения, когда statement его исхода вернулся (ack-пакет — для каждой записи пакета; retry, fail, confirm-cancel, release; успех или `LeaseLost` — там же, где `Finish`), то есть после коммита, когда место в лимите видно следующему dequeue. `PgBoltServer` передаёт `_dispatcher.OnLeaseFreed`: флаг очереди и `wake.Set()`, как у `slotFreed`. Без признака `leaseFreed[q]` сбрасывается без действия: место уже учтено dequeue по `slotFreed`. Флаги latched и применяются после обновления признака по результату dequeue, поэтому сигнал, пришедший во время dequeue, не теряется.

Следствия:
- сервер, держащий задачи очереди, перезапрашивает её после коммита каждого своего исхода, поэтому при backlog места лимита используются почти полностью: место простаивает не дольше `AckFlushInterval` и одного dequeue (тест пропускной способности §11). Места, освобождённые другими серверами и sweeper'ом, сигнала не дают — их берут плановые опросы, не реже `MaxPollInterval`. Места лимита достаются в основном серверам, уже держащим задачи очереди; справедливость между серверами не цель части;
- увеличение лимита подхватывается следующим плановым опросом (не позже `MaxPollInterval`) или после ближайшего исхода задачи очереди на этом сервере;
- пауза → все серверы получают 0 строк с `LimitReached` и уходят в backoff, сигналы enqueue его не сбрасывают; после снятия паузы выборка возобновляется в пределах `MaxPollInterval`. Выполняющиеся задачи дорабатывают.

Лимит консервативен: строка с истёкшим lease упавшего пода занимает место, пока sweeper не снимет её (до `LeaseDuration` + `SweeperInterval`).

## 7. Веса общего пула

### 7.1 Выбор стратегии и смена весов на лету

- Вес очереди — её эффективный вес из `queue_settings` (§3.1), один на кластер; каждый сервер, обслуживающий очередь, делит по нему свой общий пул. Доля очереди на кластере — сумма её долей на серверах; согласования между серверами нет.
- Вектор весов сервера — `w[q]` по его очередям в порядке объявления из `QueueSettingsCache.Current.Weights` (§6.1); очередь без записи — без веса. Ни у одной очереди нет веса → `StrictOrderDistribution` (worker §4.2), поведение без изменений; есть хотя бы у одной → `WeightedDistribution`, очереди без веса — вес 1.
- **Применение — на границе плана.** `SlotAllocator` получает в конструкторе `Func<FrozenDictionary<string, int>> weights` (`PgBoltServer` передаёт `() => cache.Current.Weights`) и хранит применённый вектор, ссылку на снимок, из которого он построен, и текущую стратегию — поля, которые читаются и меняются только под его lock. В первом `NextEntry` плана, под lock и до `BeginPlan` (§7.2): берёт снимок; ссылка та же, что при прошлом применении, → ничего; иначе строит вектор и сравнивает его по значениям с применённым: совпал (обновление кэша без изменений для этого сервера) → запоминает только ссылку, состояние стратегии сохраняется; отличается → новая стратегия: `StrictOrderDistribution` или новый `WeightedDistribution` (`pass = 0`, `short = false`, пустые счётчики плана). Затем `BeginPlan` текущей стратегии. Внутри плана стратегия не меняется: все `Next` и `OnTaken` плана приходят в один экземпляр — `Take` по записям выполняет тот же диспетчер до следующего плана (worker §4.3), `Release` стратегию не вызывает.
- **Сброс состояния.** Итог прошлого плана старой стратегией не подводится; отставание и опережение очередей (не больше круга и двух кругов, §7.3) забываются. Все очереди начинают с `pass = 0`, поэтому залпа нет: доли с момента смены — как у сервера, запущенного с новыми весами (моделирование — §7.3). Сброс происходит только при изменении вектора сервера, а не на каждом обновлении кэша.
- До первого плана стратегия — строгий порядок (пустой вектор); первый план применяет снимок, загруженный при старте до создания серверов (§4.2 п. 4).
- **`Reserved`-слоты весами не распределяются:** очередь получает свои свободные `Reserved` в обход весов (`freeReserved`, §7.3), вес управляет только общим пулом. `MaxConcurrency` и кластерный лимит ограничивают сверху как раньше.

### 7.2 Изменение контракта `ISharedPoolDistribution`

```csharp
internal interface ISharedPoolDistribution
{
    // Граница плана: SlotAllocator.PlanIterator — в первом NextEntry, под lock allocator'а, после применения снимка весов
    // (§7.1), перед первым Next и с тем же набором кандидаток (isDue вычислен один раз, ни одна очередь ещё не выдана).
    // Вызывается и для плана без записей.
    void BeginPlan(int queueCount, Func<int, bool> candidate);

    // freeShared — бюджет общего пула плана: свободные общие слоты на границе плана минус общие, взятые в этом плане
    // (не больше фактически свободных); available — min(свободные Reserved + freeShared, MaxConcurrency − занято);
    // freeReserved — свободные Reserved-слоты очереди
    (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available,
                             Func<int, int> freeReserved, int freeShared);

    // SlotAllocator.Take занял слот очереди queue (под lock allocator'а); fromReserved — то, что вернёт Take
    void OnTaken(int queue, bool fromReserved);
}
```

**Бюджет плана** — правило `SlotAllocator`, общее для любой стратегии, включая строгий порядок: внутри плана общий пул ограничен остатком «свободные общие слоты на границе плана минус общие, взятые в этом плане». Слоты, освободившиеся посреди плана, в этом плане не раздаются и достаются следующему, который диспетчер начинает сразу по `slotFreed`. Без бюджета при пачечном освобождении (несколько задач закончились одновременно) план, начатый по первому освобождению, отдавал бы остальные слоты следующим записям мимо порядка и весов — не держались ни веса, ни строгий порядок. `Reserved` бюджетом не ограничены; недобор очереди по-прежнему переходит к следующим записям плана (он остаётся в бюджете); `Available(queue)` вне плана — фактический.

`StrictOrderDistribution` игнорирует `BeginPlan`, `freeReserved`, `freeShared` и `OnTaken`. Проверки ответа стратегии в `SlotAllocator` (worker §4.2) не меняются. Порядок вызовов за итерацию диспетчера: `BeginPlan` → `Next` → dequeue → `Take` по слоту на задачу (каждый — `OnTaken`) → `Next` → … → `null`. План может оборваться без финального `Next` (остановка, ошибка dequeue) — итог такого плана подводит следующий `BeginPlan`.

### 7.3 `WeightedDistribution` — взвешенный round-robin на уровне слотов (stride scheduling)

**Состояние.** Между планами: `pass[q]` (`double`, начальный 0) — виртуальное время очереди в «кругах»: каждый взятый общий слот прибавляет `1 / w[q]`, поэтому за круг, в котором очереди с backlog взяли слоты по весам, `pass` каждой растёт на 1; признак `short[q]` — недобор в последней записи очереди с общей долей; признак `wasCandidate[q]` — очередь была кандидаткой подводимого плана. Внутри плана по очередям: `N[q]` — размер записи (0 — записи не было), `share[q]` — общая доля в ней, `took[q]` — взято по записи (все `OnTaken`, Reserved и общие), `capped[q]` — доля упёрлась в `MaxConcurrency` (`share = available − freeReserved < freeShared` при выдаче).

**`OnTaken(q, fromReserved)`:** `took[q]++`; общий слот — `pass[q] += 1 / w[q]`. Учёт сразу по факту: Reserved на `pass` не влияет; если во время dequeue освободился Reserved-слот очереди и `Take` занял его вместо общего, запись взята целиком (недобора нет), а общим зачтено ровно то, что занято.

**`BeginPlan`** — итог прошлого плана:
1. `short[q] = took[q] < N[q]` для очередей с записью и `share ≥ 1`.
2. `F` — очереди с записью, `share ≥ 1`, `took = N` и без `capped`: они взяли бы ещё, их `pass` — текущее виртуальное время. `F` пусто → шаги 3–4 пропускаются.
3. `V = min pass` по `F`. Вне `F`: недобравшая (`took < N`), `capped` или неготовая очередь — не кандидатка текущего плана или не бывшая кандидаткой подводимого (`!candidate || !wasCandidate`) → `pass = max(pass, V)`; остальные (готовы, но доли не получили) → `pass = max(pass, V − 1)`. Всем → `pass = min(pass, V + 2)`.
4. `pass −= V` у всех.
5. Всегда, в том числе при пустом `F`: `wasCandidate = candidate` текущего плана; счётчики плана `N`, `share`, `took`, `capped` обнулить — итог подводится только по прошлому плану, без накопления по планам.

Признак `wasCandidate` нужен в шаге 3: по одному `!candidate` текущего плана очередь, вышедшая из backoff на этой границе, считалась бы готовой без доли (`V − 1`) и отставала бы на круг — залп до веса слотов после каждого backoff.

**`Next`:**
1. `A` ← кандидатки с `available ≥ 1`; пусто → `null`.
2. На копии `pass` разыграть `freeShared` общих слотов; `s[q]` — счётчик розыгрыша, локальный: в каждом `Next` начинается с 0. Шаг — участники `q ∈ A` с `s[q] < available(q) − freeReserved(q)`; участников нет → стоп; победитель — минимум `copy` (равенство — порядок объявления); `copy[победитель] += 1 / w`, `s[победитель]++`.
3. Есть очереди с `s ≥ 1` → из них `p`: сначала с `short`, затем с меньшим `pass`, затем по порядку объявления; запомнить `share[p] = s[p]`, `N[p] = freeReserved(p) + s[p]`, `capped[p]` и вернуть `(p, N[p])` (≥ 1, ≤ `available(p)`).
4. Иначе — первая в порядке объявления очередь `p ∈ A` с `freeReserved ≥ 1` (такая есть: без розыгрыша `available ≤ freeReserved`); запомнить `N[p] = freeReserved(p)` (`share[p] = 0`) и вернуть `(p, N[p])`.

Свойства:
- **Точные доли.** Взятый слот меняет `pass` только взявшей очереди, поэтому розыгрыш в каждом `Next` по оставшимся слотам — хвост розыгрыша всего пула: доли не зависят от числа свободных слотов и разбиения плана на записи. При backlog во всех очередях — последовательность stride scheduling. Моделирование на реальном порядке вызовов (`BeginPlan` → `Next` → dequeue → `Take` по слоту): веса 1:1:1, 3:1, 5:2:1, 7:1, 1:2:3:4, 2:3:6 при 1–8 свободных слотах, 20 000 итераций — число взятых очередью общих слотов на границе плана (после каждой итерации диспетчера) отличается от точной доли не больше чем на 1,25 слота и не растёт со временем; внутри плана отклонение больше, так как запись выдаётся блоком (до 2 слотов при 8 свободных). `double` достаточно: при непустом `F` нормировка (шаг 4) держит значения малыми; пока все очереди недобирают, шаги 3–4 пропускаются и `pass` растёт, но на поведение это не влияет, а округление сказывается только на почти равных `pass` — в пределах слота.
- **Недобор и простой не копят преимущества, долг ограничен.** Недобравшая, неготовая или упёршаяся в `MaxConcurrency` очередь подтягивается к `V` и после появления backlog начинает наравне с самой отстающей из очередей с backlog — без залпа (моделирование: очередь, пустая 3000 итераций, в первые 20 итераций с backlog берёт ровно свою долю; пока она пуста, остальные делят пул точно по весам; то же с backoff пустой очереди и с соседом, упёршимся в `MaxConcurrency`). Готовая очередь без доли отстаёт не больше чем на круг; очередь, набравшая слотов сверх доли (остатки плана), опережает не больше чем на два круга.
- **Освобождение слотов посреди плана.** Reserved-слот той же очереди — см. `OnTaken`; общий слот другой очереди в этом плане не раздаётся (бюджет плана, §7.2) и уходит в следующий план, который диспетчер начинает сразу по `slotFreed`. Недобранные слоты в той же итерации разыгрываются заново среди ещё не выданных очередей (план ленивый) и зачитываются взявшим; порядок «сначала недобравшие в прошлый раз» ставит вероятный недобор раньше, чтобы остаток достался очередям с backlog.
- Очередь, ставшая готовой посреди плана, участвует в следующих `Next` со своим `pass` (на границе она была неготовой и подтянута к `V`): получает остаток, контракт «план не кончается при готовой очереди со свободным слотом» (worker §4.2) соблюдается.
- **Смена весов** (§7.1) — новый экземпляр с `pass = 0`. Моделирование на том же порядке вызовов: смена 3:1 → 1:3, 5:2:1 → 1:2:5, 1:2:3:4 → 4:3:2:1, строгий порядок → 1:2:3 на разных итерациях при 1–8 свободных слотах, в том числе с задачами длительностью 1–3 итерации, — отклонение накопленных после смены слотов от долей новых весов то же, что у запуска с нуля (не больше 1,25 слота); очередь, пустая 3000 итераций, при смене весов посреди её простоя или в момент появления backlog (в том числе с соседом, упёршимся в `MaxConcurrency`) в первые 20 итераций берёт свою новую долю — залпа нет.
- Пример: `critical: 3, default: 1`, обе с backlog, один свободный общий слот на итерацию → `c d c c | c d c c …`.

## 8. Dashboard

### 8.1 Экран очередей

- Список очередей: к объединению снимка и очередей живых серверов (`QueuesTemplate`, часть 6 §6.1) добавляются строки `queue_settings` хотя бы с одним переопределением (`override_concurrency_set OR override_weight_set`) — иначе очередь с переопределением без задач и без живых серверов не видна, и снять переопределение из UI нельзя. Строки только со значениями из кода в объединение не входят: строка очереди, убранной из кода (§3.1), не висит в списке вечно.
- Колонки: кластерный лимит (эффективный; пометка «из кода» или «переопределён: кто, когда»; «—» без лимита) с живым числом взятых строк в виде `N / лимит`, бейдж «Пауза». Живое число — `SELECT queue, count(*) FROM {schema}.queue WHERE lease_owner IS NOT NULL AND queue = ANY(@queues) GROUP BY queue` (index-only по `queue_leased`) только по очередям со строкой в `queue_settings`. Колонка Processing снимка (`queue_stats`) не меняется: она согласована с остальными колонками снимка и его временем. Колонка «Вес»: эффективный вес, пометка «из кода» или «переопределён: кто, когда» (из `override_weight_updated_by/at`), «—» без веса; вес — настройка очереди на кластер, общая для всех её серверов (§7.1).
- Действия (POST `/actions/…`, проверки части 6 §5.3: скрыты и запрещены при `ReadOnly` или отказе `ActionsPolicy`; `ActionsPolicy = null` — действия разрешены, как у остальных действий): для лимита — «Задать лимит» (число ≥ 1), «Без лимита», «Пауза», «Сбросить к значению из кода»; для веса — «Задать вес» (1…1000), «Без веса», «Сбросить к значению из кода»; каждое трогает только свою настройку (§5.1) — через `IQueueSettingsManager` с `updatedBy = $"dashboard: {User.Identity.Name ?? "anonymous"}"`; лог Information; ответ — htmx-фрагмент строки + `HX-Trigger` уведомление (часть 6 §7).
- Кнопка «Сбросить кэш настроек» — для изменений из других процессов: `IQueueSettingsManager.InvalidateCache()` и, если в процессе зарегистрирован `IQueueSettingsRefresher` (§6.1), `RefreshNow()`. Это POST-действие с теми же проверками: в read-only режиме скрыта, как все действия (там изменения подхватываются по TTL и интервалу). Подсказка рядом: другие поды подхватят изменения в пределах `QueueSettingsRefreshInterval` и TTL их кэша. После действий лимита и веса этого dashboard кнопка не нужна: запись менеджера сама сбрасывает кэш и вызывает `RefreshNow()` (§5.1, §5.2).
- Чтение настроек — `IQueueSettingsManager.ListAsync` (через кэш, если включён); после действия строка перечитывается через `GetAsync` — кэш уже сброшен записью (§5.2), поэтому чтение идёт в БД.

### 8.2 Список серверов

Не меняется: веса в `servers` нет (§3.3), `ServersTemplate` (`DashboardStore.Overview`) прежний. Вес — настройка очереди, показывается на экране очередей (§8.1).

### 8.3 Список Processing

Запрос части 6 §6.1 использует `queue_leased` вместо `queue_owner`; с фильтром по очереди — range scan по ключу.

## 9. Ошибки

| Ситуация | Поведение |
|---|---|
| Таймаут / ошибка лимитированного dequeue, включая ожидание lock | Обычная ошибка dequeue: лог, пауза сервера 1 → 30 с (worker §4.3) |
| Ошибка обновления `QueueSettingsCache` | Warning, последний известный снимок (лимиты и веса), повтор через интервал |
| Разные `ClusterMaxConcurrency` или `Weight` одной очереди в процессе | Ошибка старта (§4.2); BOLT010 при компиляции для константного случая |
| Недопустимое значение опции | Ошибка старта (`InvalidOperationException`, как остальная валидация опций); BOLT011 для константы |
| `Set…OverrideAsync` с пустым именем, отрицательным лимитом или весом вне 1…1000 | `ArgumentException` / `ArgumentOutOfRangeException` |
| Строка `queue_settings` удалена вручную | Для dequeue — «без лимита»; при следующем обновлении очередь выпадает из множества и теряет вес (смена вектора весов, §7.1) |
| Ошибка БД в `IQueueSettingsManager` | Исключение вызывающему; кэш не портится (ошибка не кэшируется) |

## 10. Изменения других частей

- **core:** `queue_owner` → `queue_leased` (§3.2); `queue_settings` в списке таблиц.
- **worker:** `QueueOptions` (+`ClusterMaxConcurrency`, `Weight`, §3.3); `PgBoltProcessingOptions` (+`QueueSettingsRefreshInterval`, §3.1); шаг старта синхронизации и загрузки настроек очередей, старт и остановка цикла `QueueSettingsCache` (§4.4, §6.4); `IFetchStrategy` с лимитированным вариантом и результатом `LimitedDequeueResult(Jobs, LimitReached)`, `QueueSettingsCache` передаётся в стратегию, создаваемую `PgBoltServer` (§4.1); диспетчер при `LimitReached`: результат как пустой, `signaled[q]` и `slotFreed[q]` поглощаются, новый сигнал `leaseFreed[q]` (§4.3); колбэк `onLeaseFreed` в `CompletionSink` после statement'а исхода (§6.1); `ISharedPoolDistribution` (+`BeginPlan`, `freeReserved`, `freeShared`, `OnTaken`; `SlotAllocator.PlanIterator` вызывает `BeginPlan`, каждый `Take` — `OnTaken`) и `WeightedDistribution`; `SlotAllocator` получает источник весов и на границе плана применяет снимок, меняя стратегию, и ограничивает общий пул плана бюджетом (§7.2) (worker §4.2); регистрация сервера (§7.2) не меняется; `IServerStore`: +`DequeueLimitedAsync`, `SyncQueueDefaultsAsync`, `GetActiveQueueSettingsAsync`.
- **`PgBolt.Postgres`:** публичные методы `PgBoltStore` (изменение публичного API): `DequeueLimitedAsync` → `LimitedDequeueResult` (батч по образцу `GetJob`, уточнение XML-doc класса, §6.2); `SyncQueueDefaultsAsync(IReadOnlyList<QueueDefault> defaults, string publishedBy, ct)` → `IReadOnlyList<QueueDefaultConflict>` — только строки для предупреждения, условие §4.2 п. 3 считается в SQL (§4.2); `GetActiveQueueSettingsAsync(ct)` → `IReadOnlyList<ActiveQueueSetting>` (`Queue`, `Limited`, `Weight`) очередей с эффективным лимитом или весом не `NULL` (§6.1); записи `QueueDefault(Queue, MaxConcurrency, Weight)`, `QueueDefaultConflict`, `ActiveQueueSetting` — в `PgBolt.Postgres.Storage`; `RegisterServerAsync` не меняется (§3.3); реализация `IQueueSettingsManager`; internal `IQueueSettingsRefresher` (§6.1).
- **`PgBoltOptions`:** +`QueueSettingsCacheDuration`; `AddPgBolt` регистрирует `IQueueSettingsManager`.
- **часть 6:** экран очередей (`queue_settings` с переопределением в списке очередей, колонки лимита и веса, действия, §8.1), индекс Processing.
- **`PgBoltInitializer`:** `EnsureNotPreReleaseSchemaAsync` дополнительно требует `queue_settings` (как часть 6 §11 для своих таблиц): схема сборки части 6 отвергается при старте, а не падает позже с 42P01/42703 на синхронизации или в dashboard. `MigrationTests` — точные списки таблиц и индексов (`queue_leased` вместо `queue_owner`, `queue_settings`).
- **roadmap:** 3b — ссылка на спеку и статус.

## 11. Тестирование

**Unit (`PgBolt.Server.Tests`, без PostgreSQL):**
- `WeightedDistribution` через настоящий `SlotAllocator` и порядок вызовов диспетчера (план → dequeue → `Take` по слоту): доли 1:1:1, 3:1, 5:2:1 по весам с отклонением не больше 1,25 слота на каждой границе плана при 1, 2, 3, 4, 8 свободных общих слотах; очередь, вернувшая 0, неготовая (backoff) или упёршаяся в `MaxConcurrency` не получает залпа после появления backlog, пока пуста — остальные делят пул по весам; недобранные слоты переходят к следующим записям и зачитываются взявшим; освобождение посреди плана (между `Next` и `Take`) Reserved-слота той же очереди — запись не считается недобором, зачтены только занятые общие — и общего слота другой очереди — в этом плане он не раздаётся (бюджет плана, §7.2) и достаётся следующему; очередь, ставшая готовой посреди плана, получает остаток; план, оборванный без финального `Next`, подводится следующим `BeginPlan`; `Reserved` не меняет `pass`; `MaxConcurrency` ограничивает; равенство `pass` — порядок объявления; на границе плана с непустым `F` `min pass` по `F` = 0, остальные в `[−1, 2]`.
- `WeightedDistribution`, граница плана при пустом `F`: несколько планов подряд, в которых все очереди недобирают, затем backlog. `BeginPlan` при пустом `F` не меняет `pass`, но обнуляет `N`, `share`, `took`, `capped`: очередь, взявшая половину записи в двух планах подряд, на следующей границе не попадает в `F` по сумме `took` прошлых планов (`F` пусто, `pass` не меняется); после планов, в которых все очереди вернули 0, доли с начала backlog — по весам с отклонением не больше 1,25 слота (веса и число слотов — как выше). Розыгрыш `Next` не переносится между записями: 1:1:1, 4 свободных общих слота, все очереди берут записи целиком → записи 2, 1, 1.
- Выбор стратегии и смена на лету (`SlotAllocator` с подменяемым источником весов): нет эффективного веса ни у одной очереди сервера — строгий порядок; появился — `WeightedDistribution`, очереди без веса — 1; веса очередей чужих серверов не влияют; снимок применяется только на границе плана — смена источника посреди плана не меняет стратегию до следующего плана; при изменении вектора `pass` сброшен (strict → weighted → другие веса → strict); после смены — на наборах §7.3 (3:1 → 1:3, 5:2:1 → 1:2:5, 1:2:3:4 → 4:3:2:1, строгий порядок → 1:2:3) при 1, 2, 3, 4, 8 свободных общих слотах — доли по новым весам с отклонением не больше 1,25 слота на каждой границе плана, то есть не больше, чем у запуска с нуля с новыми весами (граница 1,25 верна только для этих наборов, для других весов — критерий «как у запуска с нуля»); очередь, пустая до смены, не получает залпа; новый снимок с тем же вектором сервера состояние не сбрасывает.
- `SlotAllocator` вызывает `BeginPlan` ровно один раз на план, до первого `Next` и с теми же кандидатками; `OnTaken` — на каждый `Take` с его результатом.
- `Dispatcher`: при `LimitReached` результат dequeue ведёт к backoff и при `jobs.Count = n`; `signaled` и `slotFreed` не меняют `nextPoll`/`interval`; `leaseFreed` ставит `nextPoll = now`, без `LimitReached` — игнорируется; сигнал, пришедший во время dequeue, не теряется; dequeue без `LimitReached` возвращает обычное применение сигналов.
- Стратегия выборки (`IFetchStrategy` с подменяемым снимком `QueueSettingsCache`): очередь вне `Limited` — `DequeueAsync` (core §5.2), `DequeueLimitedAsync` не вызывается; в `Limited` — `DequeueLimitedAsync`; очередь только с весом в `Weights` и вне `Limited` — `DequeueAsync`; новый снимок подхватывается следующим dequeue.
- `CompletionSink`: `onLeaseFreed` вызывается для каждой записи после statement'а исхода (ack-пакет, retry, fail, confirm-cancel, release; успех и `LeaseLost`), не раньше; не вызывается для брошенных по бюджету и при отменённой остановке.
- Валидация опций: все правила §4.1, §5.2, §6.1 (`QueueSettingsRefreshInterval`), конфликт `ClusterMaxConcurrency` и конфликт `Weight` между серверами процесса (§4.2).
- Кэш менеджера: TTL по `FakeTimeProvider`, сброс записью и `InvalidateCache`, схлопывание параллельных промахов, ошибка не кэшируется; загрузка, начатая до инвалидации, в кэш не попадает; отмена одного ожидающего не отменяет общую загрузку.
- `QueueSettingsCache`: ошибка обновления сохраняет прежний снимок (лимиты и веса); `RefreshNow` во время обновления в полёте ставит одно повторное после него; `RefreshNow` до старта цикла и после остановки — no-op; цикл останавливается в `StopAsync` и в ветке ошибки старта.
- `IQueueSettingsManager`: каждый метод записи сбрасывает кэш и вызывает `RefreshNow` и при исключении записи; валидация `weight` (`null`, 1…1000).

**Анализатор (`PgBolt.Generators.Tests`):** BOLT010 — для `ClusterMaxConcurrency` и для `Weight` по отдельности: разные константы; константа против: лямбды без присваивания, вызова без `configure`, литерала `null`, присваивания `null`; последнее из нескольких присваиваний; одинаковые значения молчат; молчат при неизвестном значении: method group с присваиванием 5 против лямбды с 5 и против лямбды с 3, переменная-делегат, лямбда с `if` или вызовом, неконстантное значение, правая часть с параметром лямбды; диагностика на аргументе имени у вызова без присваивания; расхождение только по `Weight` при одинаковом `ClusterMaxConcurrency` — на каждом вызове одна диагностика с именем опции, при расхождении по обеим опциям — две на каждом вызове; BOLT011 — каждая строка таблицы §4.3 и граничные значения.

**Интеграционные (Testcontainers, PG 16):**
- схема сборки части 6 (без `queue_settings`) отвергается `PgBoltInitializer` при старте с понятным сообщением; `MigrationTests` сверяют точные списки таблиц и индексов;
- конкурентные лимитированные dequeue из N параллельных соединений с разными `@n` под нагрузкой, вперемешку с конкурентными ack и release взятых строк: максимум одновременно взятых строк ≤ лимита на всём прогоне. Измеряет отдельное соединение периодическим `pg_advisory_xact_lock(<ключ очереди>)` и `SELECT count(*) …` — отдельными statement'ами в одной транзакции, тем же протоколом, что dequeue, поэтому замер видит согласованное состояние между dequeue; клиентский счёт отстаёт от БД и мерилом не служит;
- контрольный тест (детерминированный): соединение A в явной транзакции берёт lock и строки до лимита; B запускает вариант «lock и подсчёт в одном statement'е» и блокируется на lock (ожидание — по `pg_locks`); A коммитит; B берёт строки сверх лимита. Фиксирует причину решения §6.2;
- `LimitReached`: истина при исчерпанном лимите и паузе, ложь при недоборе из-за пустой очереди и без лимита;
- override приоритетнее значения из кода; `ResetConcurrencyOverrideAsync` возвращает его; «без лимита» снимает лимит из кода;
- переопределение веса: приоритетнее значения из кода, «без веса» снимает вес из кода, `ResetWeightOverrideAsync` возвращает его; запись и сброс веса не трогают колонки лимита и наоборот; `SetWeightOverrideAsync` для очереди без строки создаёт строку с `default_* = NULL`; CHECK-и отвергают вес вне 1…1000;
- пауза: dequeue возвращает 0 при backlog; после снятия задачи выбираются;
- снимок `GetActiveQueueSettingsAsync` / `QueueSettingsCache` (правило §6.1): строка только с весом из кода или из переопределения → в `Weights`, не в `Limited`; переопределение «без лимита» поверх лимита из кода → не в `Limited` (вес, если есть, — в `Weights`); «без веса» поверх веса из кода → не в `Weights` (лимит, если есть, — в `Limited`); пауза 0 → в `Limited`; строка с обоими эффективными `NULL` — ни в одном;
- синхронизация: upsert не трогает `override_*`; лимит и вес пишутся вместе; строка создаётся, если не `NULL` хотя бы одно из значений (в том числе только вес); оба `NULL` из кода не создают строку и обнуляют существующую; одна очередь на двух серверах процесса — одна строка, без ошибки; Warning при расхождении лимита или веса с чужим `published_by` и свежим `default_updated_at`, без Warning — при давнем и при первой публикации в строку, созданную переопределением (`default_published_by IS NULL`);
- появление и снятие лимита, смена веса (сервер переключает стратегию на следующей границе плана) подхватываются после обновления `QueueSettingsCache`, в процессе, выполнившем запись, — после `RefreshNow`;
- строка с истёкшим lease занимает место до прохода sweeper'а;
- `EXPLAIN` — план целиком, при backlog ≥ 300 тыс. готовых строк очереди и `jobs` из ≥ 10 партиций, для `@n` 1 и 100: в лимитированном dequeue нет ни Seq Scan по `queue` и партициям `jobs`, ни Hash Join; подсчёт — Index Only Scan по `queue_leased`, `c` — Index Scan по `queue_fetch`, при `room ≤ 0` чтение `queue_fetch` не выполняется (`never executed`); dequeue без лимита — прежний план;
- лишние кандидаты (`@n > room`) после dequeue не заблокированы: `SELECT … FOR UPDATE NOWAIT` по ним из другого соединения проходит;
- пропускная способность: один сервер, лимит L = 4, `Workers` 16, backlog 200 задач длительностью d = 200 мс, `AckFlushInterval` по умолчанию — за 10 с выполнено не меньше 0,8 · L · 10 с / d = 160 задач (≈ L/d), одновременно взятых строк не больше L (§6.3);
- dashboard: очередь только со строкой `queue_settings` с переопределением лимита или веса (без задач и серверов) видна в списке очередей, со строкой только значений из кода — не видна;
- PgBouncer (transaction mode, контейнер): лимитированный dequeue проходит, advisory lock не остаётся на соединении (`pg_locks` после возврата соединения пуст);
- тест HOT-контракта `jobs` (core §11) проходит.

**E2E:** сервер с лимитированной очередью и backlog — одновременно выполняющихся задач на двух серверах не больше лимита; dashboard — действия лимита, паузы и веса, колонки, сброс кэша.

## 12. Риски

- **Сериализация на lock** ограничивает пропускную способность лимитированной очереди — приемлемо для её назначения; бенчмарк — часть 7.
- **Окно до обновления списка** (§6.1): лимит, впервые заданный очереди, не соблюдается до `QueueSettingsRefreshInterval`. Если окажется критичным — уменьшить интервал; переход на проверку в каждом dequeue (без кэша) ударил бы по горячему пути всех очередей.
- **Доли при недоборе** (§7.3): при backlog во всех очередях доли точны (учёт по факту), но остаток слотов от очереди, недобравшей неожиданно, достаётся очередям, идущим после неё в плане, — порядок записей «сначала недобравшие в прошлый раз» эвристика. Моделирование на реальном порядке вызовов (400 наборов: веса 1–6, 1–8 слотов, 2–4 очереди, часть очередей с притоком 0,05–1,5 от справедливой доли, 3000 итераций; 156 наборов с ≥ 2 очередями с backlog), отклонение долей очередей с backlog от их весов: медиана 0 %, p90 0,1 %, больше 6 % — 2 из 156, максимум 24 %; при притоке 0,05–0,5 доли — 0 из 156, максимум 0,2 %; при задачах длительностью 1–3 итерации (пул занят частично) — 0 из 156, максимум 1 %. Оба выброса — очередь с притоком чуть ниже своей доли (0,67 при доле 0,75, 2 слота; 1,82 при 1,88, 8 слотов): она то берёт запись целиком, то недобирает, не будучи `short`, и остаток уходит последней записи — веса очередей с backlog 4:1 дают 3:1, 6:1 — 4,8:1. Для сравнения, кредиты SWRR с пересчётом на границе плана в тех же сценариях — 16, 20 и 13 наборов из 156 с отклонением больше 6 %, максимум 42 %. Крайний случай Reserved: если Reserved-слот очереди освобождается во время каждого её dequeue, общая доля 3:1 становится 1,5:1 (при 30 % dequeue — 2,9:1): размер записи `N` фиксирован до dequeue, и освободившийся общий слот уходит следующим записям. Контракт при уточнении эвристики не меняется.
- **Несправедливость между серверами** (§6.3): места лимита липнут к серверам, уже держащим задачи очереди. Не влияет на соблюдение лимита и его использование.
- **Вес кластерный, деление — по серверам** (§7.1): доля очереди на кластере — сумма долей на серверах её обслуживающих; при разных наборах очередей и `Workers` у серверов она не равна весу относительно всего кластера. `Reserved` идёт в обход весов. Смена весов сбрасывает `pass` сервера: накопленное отставание (до круга) и опережение (до двух кругов) забываются — разовая неточность без залпа.
- **Visibility map `queue`:** при высоком churn index-only scan подсчёта делает heap fetches; число строк ограничено числом взятых задач очереди — дёшево. Настройка autovacuum `queue` — часть 7.
