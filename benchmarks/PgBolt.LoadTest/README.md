# PgBolt.LoadTest

Нагрузочный стенд PgBolt: измеряет, выдерживает ли PgBolt поток 10 000 enqueue/с на PostgreSQL 17 (напрямую и через PgBouncer), с какого числа партиций `jobs` деградируют горячие запросы и как ведёт себя autovacuum таблицы `queue` под высоким churn. Результат прогона — отчёт `.md` с вердиктами SLO и `.csv` с посекундными срезами.

Стенд — консольное приложение .NET 10 в одном процессе: продюсер, два хоста PgBolt (`AddPgBolt` + `AddPgBoltServer`, по 32 воркера) и сборщик срезов. Это не BenchmarkDotNet: микробенчмарки узлов горячего пути лежат в `benchmarks/PgBolt.Benchmarks` и `benchmarks/PgBolt.Server.Benchmarks`.

## Требования

- .NET SDK из `global.json`.
- Docker: без `--connection` стенд на время вызова поднимает контейнеры `postgres:17-alpine` и `edoburu/pgbouncer:v1.24.1-p1` в общей сети и удаляет их по окончании. Свои контейнеры включают `fsync`, `full_page_writes` и `synchronous_commit` (сборщик Testcontainers по умолчанию их выключает).
- В образе диска Docker не меньше 30 ГБ свободного места: прогон `vacuum` даёт около 22 млн строк `jobs` (10–15 ГБ данных) плюс WAL до `max_wal_size` (4 ГБ). Схема прогона удаляется после каждого режима.
- Для полного набора — машина, на которой стенд не упирается в CPU: загрузка процесса стенда выше 80 % ядер больше чем в 10 % окон замера даёт код возврата 2 (см. «Коды возврата»). Docker for Mac (виртуализация диска) может стать узким местом раньше PgBolt; окружение пишется в отчёт, а `--connection` позволяет повторить прогон на выделенном PostgreSQL.

## Запуск

Из корня репозитория:

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- <sustained|ramp|partitions|vacuum> [флаги]
```

Первый аргумент — сценарий, дальше пары «флаг значение». Всё, что не задано, берётся по умолчанию (таблица флагов ниже). Прерывание Ctrl+C останавливает прогон с кодом 2 и частичным отчётом.

## Профиль нагрузки B

Все сценарии крутят один профиль:

- 10 000 enqueue/с (`--rate`); очереди `default` — 80 %, `critical` — 20 %, оба хоста обслуживают обе.
- 90 % задач немедленные, 10 % отложенные на целое число секунд 1–30.
- 1 % задач падает на первой попытке и повторяется по политике по умолчанию (15 с ± 20 %).
- Работа задачи — 1 мс, args около 200 байт.

Продюсер отдаёт нагрузку пачками через `EnqueueManyAsync` (до 100 задач или 10 мс возраста пачки), поэтому вызовов около 400 в секунду при 10 000 задач/с.

## Сценарии

В каждом сценарии ниже сначала команда по умолчанию (полный прогон), затем короткий режим для проверки стенда за пару минут. Короткий режим пишет отчёты в `/tmp/pgbolt-load` — подставьте свой каталог.

### sustained

Стационарная нагрузка B: прогрев 1 мин, окно замера 10 мин, затем дренаж (до 2 мин) и сверка корректности. Оцениваются SLO 1–6.

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- sustained
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- sustained --mode both --rate 500 --warmup 10s --duration 20s --out /tmp/pgbolt-load
```

Режимы `direct` и `bouncer` по умолчанию (`both`) идут по очереди на одном наборе контейнеров, у каждого свой отчёт.

Для измерения со штатными параметрами памяти и WAL запустите `sustained --mode direct --pg-profile stock`. Для парного сравнения через PgBouncer используйте `sustained --mode bouncer --pg-profile stock`, затем тот же вызов с `--pg-setting autovacuum_naptime=10s`. Каждый вызов поднимает свежий контейнер. Прежний профиль воспроизводится с `--pg-profile bench` или без флага.

### ramp

Поиск максимального устойчивого потока: поток растёт ступенями от `--ramp-start` на `--ramp-step` каждые `--ramp-step-duration` до `--ramp-max`, без прогрева и дренажа. Рост останавливается на первой ступени с нарушением. Ступень оценивается по окнам 10 с, начинающимся не раньше трети ступени (при 60 с — последние 40 с): SLO 1 (с недогрузом) в каждом окне и p99 SLO 2 по ним вместе. Результат — максимальный устойчивый rate и то, что нарушено первым: SLO 1, SLO 2, «enqueue» (недогруз) или «стенд» (упор в CPU стенда; тогда rate помечается «ограничено стендом»).

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- ramp
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- ramp --mode direct --ramp-start 500 --ramp-step 500 --ramp-step-duration 60s --ramp-max 1000 --out /tmp/pgbolt-load
```

`--ramp-start` не может быть больше `--ramp-max`, ступень не короче 30 с.

### partitions

Матрица «режим × набор индексов × число партиций `jobs`» (P): с какого P деградируют dequeue, ack и enqueue и как на это влияют prepared statements. На каждую комбинацию — новая схема, измерение блокировок statement'ов до нагрузки, прогрев 30 с и окно замера 2 мин нагрузки B без дренажа. По умолчанию это 4 режима (`direct`, `bouncer`, `direct-prepared`, `bouncer-prepared`) × 2 набора индексов × P = 15, 60, 200, 400 — 32 комбинации.

Наборы индексов: `current` — индексы продукта (PK, `jobs_batch`, `jobs_parent`, `jobs_recurring`); `extra` — плюс btree `(type, id)` и GIN `(args jsonb_path_ops)`, имитация индексов, которые пока не входят в продукт.

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- partitions
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- partitions --mode all --partitions 15,20 --indexes current --rate 500 --warmup 5s --duration 20s --out /tmp/pgbolt-load
```

Сбой одной комбинации не обрывает матрицу: она получает строку «Сбой» в сводке, прогон продолжается и в конце возвращает код 2. Режимы `*-prepared` определяют тип плана каждого statement'а (custom или generic); блокировки generic plan вычисляются как нижняя оценка. `bouncer-prepared` требует `max_prepared_statements > 0` у PgBouncer. У своего контейнера он всегда 200. Внешний PgBouncer стенд не проверяет (канала к админ-консоли нет) и только пишет в отчёт предупреждение «не проверен»; если там значение 0, комбинации `bouncer-prepared` завершатся сбоем `prepared statement "…" does not exist` и прогон вернёт код 2.

### vacuum

Autovacuum `queue` под нагрузкой B: прогрев 2 мин, фаза 1 (стационарная, 20 мин), фаза 2 (удержание xmin открытой транзакцией `REPEATABLE READ`, 5 мин), фаза 3 (восстановление, 10 мин), без дренажа. Оцениваются SLO 6–9. Если сервер оборвал сессию с удерживающей транзакцией, фаза 2 не считается состоявшейся: прогон прерывается с ошибкой, а не выдаёт результат.

`--queue-reloptions` до прогона выполняет `ALTER TABLE queue SET (...)` — так сравниваются параметры без правки кода; `--pg-setting` задаёт глобальные параметры своего контейнера PostgreSQL (пары `-c k=v` при старте).

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- vacuum
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- vacuum --mode both --rate 500 --warmup 5s --phase1 20s --hold-xmin 10s --phase3 20s --queue-reloptions "autovacuum_vacuum_cost_delay=0" --pg-setting "autovacuum_naptime=10s" --out /tmp/pgbolt-load
```

При фазе 1 короче 3 минут SLO 7 даёт «нет данных».

### Внешняя БД

```
dotnet run -c Release --project benchmarks/PgBolt.LoadTest -- sustained --connection "Host=db;Database=bench;Username=u;Password=p" --rate 500 --warmup 10s --duration 20s --out /tmp/pgbolt-load
```

Со `--connection` стенд создаёт в базе уникальную схему и удаляет её после прогона. `pg_stat_statements` используется, если расширение доступно, иначе соответствующие колонки отчёта — «нет данных». Режимы через PgBouncer требуют `--bouncer-connection`; канала к админ-консоли внешнего PgBouncer у стенда нет, поэтому данные `SHOW POOLS` и SLO 6 для `bouncer` — «нет данных».

## Флаги

| Флаг | Сценарии | По умолчанию | Смысл |
|---|---|---|---|
| `--mode direct\|bouncer\|both\|direct-prepared\|bouncer-prepared\|all` | все | без `--connection` — `both` (`direct` + `bouncer`), у `partitions` — `all`; с `--connection` без `--bouncer-connection` — `direct`, у `partitions` — `direct` и `direct-prepared`; с обоими — `both`, у `partitions` — `direct`, `bouncer`, `direct-prepared` | Режимы подключения. `*-prepared` и `all` допустимы только в `partitions`; с `--connection` режимы `bouncer*` требуют `--bouncer-connection`; `bouncer-prepared` с внешним PgBouncer — только при явном `--mode bouncer-prepared` или `all` |
| `--connection "<cs>"` | все | свои контейнеры | Внешняя БД |
| `--bouncer-connection "<cs>"` | все | свой PgBouncer | Внешний PgBouncer; только вместе с `--connection` |
| `--out <dir>` | все | `benchmarks/PgBolt.LoadTest/results` | Каталог отчётов (путь от текущего каталога) |
| `--rate <n>` | `sustained`, `partitions`, `vacuum` | 10 000 | Целевой поток enqueue/с |
| `--warmup <t>` | `sustained`, `partitions`, `vacuum` | 1 мин; `partitions` — 30 с; `vacuum` — 2 мин | Прогрев до окна замера (в `vacuum` — до фазы 1) |
| `--duration <t>` | `sustained`, `partitions` | 10 мин; `partitions` — 2 мин на комбинацию | Окно замера |
| `--ramp-start <n>` | `ramp` | 2 000 | Поток первой ступени |
| `--ramp-step <n>` | `ramp` | 2 000 | Прирост потока на ступень |
| `--ramp-step-duration <t>` | `ramp` | 60 с | Длительность ступени, не короче 30 с |
| `--ramp-max <n>` | `ramp` | 40 000 | Поток последней ступени; не меньше `--ramp-start` |
| `--partitions <P,...>` | `partitions` | 15,60,200,400 | Число партиций `jobs` |
| `--indexes current,extra` | `partitions` | `current,extra` | Наборы индексов |
| `--phase1 <t>` | `vacuum` | 20 мин | Стационарная фаза |
| `--hold-xmin <t>` | `vacuum` | 5 мин | Фаза удержания xmin |
| `--phase3 <t>` | `vacuum` | 10 мин | Фаза восстановления |
| `--queue-reloptions "k=v,..."` | `vacuum` | — | `ALTER TABLE queue SET (...)` до прогона |
| `--pg-profile <профиль>` | `sustained`, `partitions`, `vacuum` | `bench` | Выберите `stock` для штатных настроек PostgreSQL или `bench` для `shared_buffers=1GB` и `max_wal_size=4GB`. Флаг применяется к контейнеру стенда. |
| `--pg-setting "k=v,..."` | `sustained`, `vacuum` | — | Явные параметры контейнера PostgreSQL перекрывают параметры профиля. Вместе с `--connection` — ошибка. |

Длительности — число с единицей `s`, `m` или `h` (`30s`, `2m`, `1h`). Длительность окна замера, фаз и ступени кратна 10 с (по окнам 10 с считаются SLO), прогрев — любая. Флаг вне своих сценариев, неизвестный флаг, флаг без значения и недопустимое сочетание — ошибка разбора (код 2, текст ошибки в stderr).

## Коды возврата

| Код | Значение |
|---|---|
| 0 | Все оцениваемые SLO выполнены. Для `ramp` — прогон без сбоев стенда: остановка роста на нарушении SLO или с пометкой «стенд» — результат, а не сбой; для `partitions` — выполнены SLO у `P = 15, current` во всех режимах |
| 1 | Нарушен хотя бы один SLO, в том числе SLO 1 с пометкой «enqueue» (недогруз). `ramp` код 1 не возвращает. Для `partitions` — только SLO у `P = 15, current` хотя бы в одном режиме: нарушения при других P и у `extra` на код не влияют, они дают порог P в сводке; если `P = 15, current` нет в матрице, код 1 невозможен |
| 2 | Сбой самого стенда: исключение стенда или сбой комбинации `partitions`, недоступность БД или контейнеров (30 подряд неудачных попыток среза), неверные аргументы, `max_prepared_statements = 0` у своего PgBouncer в режиме `bouncer-prepared` (у внешнего значение не проверяется, при 0 комбинации падают сбоем), прерывание Ctrl+C, упор в CPU процесса стенда (в `ramp` он код 2 не даёт, в `partitions` — только у `P = 15, current`) |

Вердикт «нет данных» (источник недоступен, отрезок для статистики короче нужного) нарушением не считается и на код не влияет. При нескольких режимах возвращается наибольший код.

## SLO

Оцениваются на окне замера отдельно для каждого режима.

| # | SLO | Порог |
|---|---|---|
| 1 | Пропускная способность | каждое окно 10 с: выполнено ≥ 95 % rate; окно недогруза (продюсер поставил меньше 98 % rate × 10 с) — нарушение с пометкой «enqueue» |
| 2 | Задержка старта немедленных задач | p50 ≤ 50 мс, p99 ≤ 500 мс, p99.9 ≤ 2 с |
| 3 | Опоздание отложенных задач (от `run_at`) | p99 ≤ 1,5 с |
| 4 | Backlog готовых не взятых задач | в каждом срезе ≤ rate × 1 с; среднее последней минуты окна ≤ среднее первой минуты + rate × 1 с |
| 5 | Корректность (после дренажа ≤ 2 мин) | все строки `jobs` в Succeeded, `queue` пуста, `outcome.dropped` = 0, `storage.errors` = 0, счётчик выполнений = 2 у задач с первым падением и = 1 у остальных, `jobs.attempt` соответствует |
| 6 | Соединения | `bouncer`: максимальный `maxwait` из `SHOW POOLS` ≤ 100 мс в каждом срезе; `direct`: соединений хостов и продюсера ≤ 48 |
| 7 | Размер `queue` в фазе 1 (`vacuum`) | среднее последней минуты ≤ 2 × среднее за 60–120 с от начала фазы 1 |
| 8 | Стабильность в фазе 1 (`vacuum`) | SLO 1–4 выполняются во всех окнах 10 с фазы 1 |
| 9 | Восстановление (`vacuum`) | SLO 1, 2 и первая часть SLO 4 восстанавливаются не позже чем через 5 мин после начала фазы 3 |

В `sustained` оцениваются SLO 1–6, в `ramp` — SLO 1–2 по ступеням, в `partitions` — SLO 1–2 по комбинациям, в `vacuum` — SLO 6–9 (SLO 5 не оценивается: дренажа нет; `outcome.dropped`, `storage.errors` и повторные выполнения пишутся без порога).

## Отчёт

Каталог отчётов по умолчанию исключён из Git. Выводы опубликованных замеров собраны в [docs/performance.md](../../docs/performance.md).

В `--out` на каждый режим пишутся два файла: `<yyyyMMdd-HHmm>-<сценарий>-<режим>.md` и рядом `.csv` с посекундными срезами. У комбинации `partitions` к имени добавляется `-p<P>-<индексы>`, а в конце матрицы пишется сводка `<yyyyMMdd-HHmm>-partitions-summary.md`. Существующие файлы не перезаписываются: к имени добавляется `-2`, `-3`…

Отчёт прогона (`.md`):

- параметры прогона и окружение: ресурсы хоста и Docker, версии PostgreSQL и PgBouncer, `--pg-profile` (`external` для внешней БД), `--pg-setting`, фактические настройки памяти, WAL, autovacuum, диагностики и гарантий записи, reloptions `queue` и `completions`, версия .NET, коммит;
- таблица SLO: порог, значение, PASS/FAIL/«нет данных», пометки «enqueue» и «ограничено стендом»;
- результат `ramp` (максимальный устойчивый rate и что нарушено первым) или входы выбора параметров autovacuum (`vacuum`);
- перцентили задержки старта немедленных и отложенных задач (p50, p90, p99, p99.9, max) и число переполнений гистограммы;
- пропускная способность по окнам 10 с: выполнено, поставлено, вызовы `EnqueueManyAsync` и средний размер пачки, ошибки и p99 enqueue, загрузка CPU, ожидание генератора и отброшенные токены, пометка недогруза;
- загрузка CPU стенда, сводка `pg_stat_statements` по классам (dequeue, ack, outcome, heartbeat, enqueue, promoter, а если найден — `dequeue_limited`: calls, mean и total времени планирования и исполнения);
- блокировки statement'ов (`partitions`): тип плана, блокировки вне fast-path и всего, измерение или нижняя оценка;
- bloat и autovacuum (с размером БД в конце прогона), checkpoint'ы, ожидания `LockManager`;
- корректность и счётчики PgBolt (пропущенные выполнения — только в `sustained`, с дренажом; повторные выполнения, `outcome.dropped`, `storage.errors`);
- нагрузка самого среза (время запросов, пропущенные тики, неудачные запросы) и предупреждения окружения.

Сводка `partitions`: ёмкость общей таблицы блокировок, SLO 1–2, p99, ожидания `LockManager` и `storage.errors` каждой комбинации (с причиной сбоя, если он был), statement'ы горячего пути с типом плана, блокировками и `mean_plan_time`/`mean_exec_time`, порог P (наименьшее P матрицы, с которого режим и набор индексов нарушают SLO).

Колонки `.csv`: `at_s`, `ready_backlog`, `queue_total_bytes`, `max_wait_us`, `cl_waiting`, `sv_active`, `sv_idle`, `connections`, `completed`, `delayed`, `leased`, `lock_manager_waits`, `autovacuum_workers`, `queue_live_tup`, `queue_dead_tup`, `completions_live_tup`, `completions_dead_tup`, `wal_bytes`, `vacuums`, `query_ms`, `counts_ms`. Пустая ячейка — отсчёт пропущен или источник недоступен.

Окружение, шум и пороги: однократные прогоны на ноутбуке шумные. Сравнивайте режимы, P и reloptions внутри одного прогона; разницу меньше 10 % считайте равенством. Всплески checkpoint'ов видны в отчёте, чтобы не приписывать их PgBolt.

## Длительность

| Набор | Время |
|---|---|
| `sustained`, 2 режима | ≈ 0,5 ч (на режим: прогрев 1 + замер 10 + дренаж ≤ 2 мин + подготовка) |
| `ramp`, 2 режима | ≈ 0,7 ч (до 20 ступеней по 1 мин на режим) |
| `partitions`, 32 комбинации | ≈ 1,8 ч (≈ 3,5 мин на комбинацию) |
| `vacuum`, 2 режима × 2 варианта (текущие reloptions и `autovacuum_vacuum_cost_delay=0`) | ≈ 2,5 ч (≈ 38 мин на прогон) |
| Всё вместе | ≈ 5,5 ч; с глобальным кандидатом `autovacuum_naptime=10s` (ещё 2 × 38 мин) — до ≈ 6,8 ч |

Полный набор запускайте в фоне и по одному сценарию: прогоны не делят контейнеры и не должны идти одновременно на одной машине.

## Smoke

Интеграционные тесты стенда (нужен Docker), в том числе короткий прогон каждого сценария внутри процесса: отчёт создан, код возврата 0 или 1 (у `ramp` — 0):

```
dotnet test tests/PgBolt.LoadTest.Tests --filter "Category=Load"
```

Эти тесты исключены из обычного прогона `dotnet test PgBolt.slnx --filter "Category!=Load"`.
