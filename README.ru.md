# PgBolt

[English](README.md) · [Русский](README.ru.md)

PgBolt — библиотека фоновых задач для .NET 10 с очередью в PostgreSQL. Например, API может сохранить заказ и в той же транзакции поставить задачу на подготовку отчёта. После коммита воркер заберёт задачу, вызовет C#-обработчик и запишет результат в PostgreSQL. Клиент и воркер могут работать в одном приложении или в разных процессах.

## Содержание

- [Подключение и выбор пакетов](#packages)
- [Быстрый старт с PostgreSQL](#quickstart)
- [Отдельные приложения для клиента и воркера](#services)
- [Постановка, дедупликация и транзакции](#enqueue)
- [Выполнение, повторы и отмена](#execution)
- [Очереди и параллелизм](#queues)
- [Расписания cron](#cron)
- [Цепочки и группы задач](#workflows)
- [Панель управления и наблюдаемость](#dashboard)
- [PostgreSQL, PgBouncer и ограничения](#operations)
- [Сборка, тесты и нагрузочный стенд](#development)

<a id="packages"></a>

## Подключение и выбор пакетов

| Что делает приложение | Что подключить |
| --- | --- |
| Объявляет типы аргументов или вызывает типизированный API | `PgBolt.Abstractions` напрямую. Пакет включает атрибуты, интерфейсы, генератор кода и анализаторы. |
| Только ставит задачи из API, CLI или сервиса | `PgBolt.Postgres` для `AddPgBolt` и реализации `IJobClient`. |
| Выполняет задачи | `PgBolt.Server` для `AddPgBoltServer`. Пакет также подключает `PgBolt.Postgres`. |
| Показывает задачи в ASP.NET Core | `PgBolt.Dashboard` в веб-приложении. |
| Экспортирует метрики и трассировки через OpenTelemetry | `PgBolt.OpenTelemetry` вместе с провайдером и экспортёром приложения. |

Для проекта с типизированными задачами и воркером команды установки версии 0.1.0 из NuGet выглядят так:

```sh
dotnet add package PgBolt.Abstractions --version 0.1.0
dotnet add package PgBolt.Server --version 0.1.0
```

Используйте пакеты одной версии.

<a id="quickstart"></a>

## Быстрый старт с PostgreSQL

Пример создаёт консольное приложение `PgBoltDemo`. При каждом запуске оно ставит одну задачу в очередь `reports`. Воркер печатает строку с номером отчёта. Нужны .NET 10 SDK, Docker и оболочка Bash или Zsh.

**1. Создайте приложение и установите пакеты из NuGet.** Выполните команды в каталоге, где хотите создать проект.

```sh
dotnet new console -n PgBoltDemo -f net10.0
cd PgBoltDemo
dotnet add package PgBolt.Abstractions --version 0.1.0
dotnet add package PgBolt.Server --version 0.1.0
```

**2. Запустите базу для примера.** Контейнер слушает только локальный интерфейс. Уже есть своя тестовая база? Задайте её строку в `PGBOLT_CONNECTION` и переходите к шагу 3. Пользователю нужны права на создание схемы, таблиц и партиций.

```sh
docker run --rm -d --name pgbolt-guide-db -p 127.0.0.1:55432:5432 \
  -e POSTGRES_DB=pgbolt_demo -e POSTGRES_PASSWORD=pgbolt_demo postgres:17-alpine
export PGBOLT_CONNECTION='Host=127.0.0.1;Port=55432;Database=pgbolt_demo;Username=postgres;Password=pgbolt_demo'
```

**3. Создайте `ReportJob.cs` рядом с `Program.cs`.** Атрибут `[Job]` задаёт имя задачи, которое хранится в БД, и очередь по умолчанию. `JsonSerializerContext` описывает сериализацию аргументов. Класс обработчика получает зависимости через DI.

```csharp
using System.Text.Json.Serialization;
using PgBolt;

[assembly: PgBoltJsonContext(typeof(PgBoltDemo.ReportJson))]

namespace PgBoltDemo;

[Job("reports.generate", Queue = "reports")]
public sealed record ReportArgs(int ReportId);

public sealed class ReportJob : IJob<ReportArgs>
{
    public Task ExecuteAsync(ReportArgs args, JobContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Обработка отчёта {args.ReportId}, попытка {context.Attempt}");
        return Task.CompletedTask;
    }
}

[JsonSerializable(typeof(ReportArgs))]
internal sealed partial class ReportJson : JsonSerializerContext;
```

**4. Замените `Program.cs`.** `AddPgBolt` регистрирует клиент и инициализацию схемы. `AddJobs` подключает обработчики этой сборки через сгенерированный реестр. `AddPgBoltServer` запускает воркеры для выбранных очередей.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgBolt;
using PgBoltDemo;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddPgBolt(options =>
{
    options.ConnectionString = Environment.GetEnvironmentVariable("PGBOLT_CONNECTION")
        ?? throw new InvalidOperationException("Задайте PGBOLT_CONNECTION");
    options.AddJobs(PgBoltJobRegistry.Register);
});
builder.Services.AddPgBoltServer("reports-worker", server => server.Queue("reports"));

using var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();
var result = await client.EnqueueAsync(new ReportArgs(42));
Console.WriteLine($"Задача {result.Id} поставлена в очередь");
await host.WaitForShutdownAsync();
```

**5. Выполните задачу.** Дождитесь готовности PostgreSQL и запустите приложение в том же терминале.

```sh
dotnet run --no-restore
```

В выводе появятся идентификатор задачи и строка `Обработка отчёта 42, попытка 1`. Хост продолжит слушать очередь. Остановите его с помощью Ctrl+C. По окончании локального примера команда `docker stop pgbolt-guide-db` удалит временный контейнер вместе с демонстрационной БД.

<a id="services"></a>

## Отдельные приложения для клиента и воркера

| Сборка | Что находится в ней |
| --- | --- |
| Контракты | Публичный тип `ReportArgs` с `[Job("reports.generate", Queue = "reports")]`. На эту сборку ссылаются API и воркер. |
| API | `AddPgBolt` с общей БД и схемой. Внедрите `IJobClient` в endpoint или сервис и вызовите `EnqueueAsync`. Для типизированного вызова подключите генератор и JSON-контекст, охватывающий аргументы. |
| Воркер | `ReportJob`, JSON-контекст и регистрация `AddJobs(...Register)` для каждой сборки обработчиков. Подключите `AddPgBoltServer` и перечислите очереди, которые процесс обслуживает. |

`PgBoltJobRegistry` генерируется в корневом пространстве имён сборки обработчиков. Если таких сборок несколько, передайте каждый реестр с полным именем. При проблеме с типизированной постановкой проверьте прямую ссылку на `PgBolt.Abstractions`, атрибут сборки `PgBoltJsonContext` и `[JsonSerializable(typeof(ReportArgs))]` в проекте, где компилируется вызов. [Контракты примера](tests/E2E/PgBolt.E2E.Contracts/Contracts.cs) · [Обработчик и JSON-контекст](tests/E2E/PgBolt.E2E.JobsA/PingJob.cs) · [Регистрация нескольких сборок](tests/E2E/PgBolt.E2E.Tests/EndToEndTests.cs)

Клиент также может передать строковое имя и `JsonElement` в `EnqueueAsync`. Такому клиенту достаточно знать имя и форму JSON. Сначала запустите воркер хотя бы раз, чтобы он опубликовал описание типа в `job_types`. Для неизвестного имени клиент вернёт `UnknownJobException`. При изменении контракта учитывайте задачи со старыми аргументами, которые уже лежат в очереди.

<a id="enqueue"></a>

## Постановка, дедупликация и транзакции

По умолчанию задача доступна сразу. Среди готовых задач одной очереди воркер сначала выбирает больший приоритет, затем более ранний срок запуска. При параллельной обработке порядок завершения может отличаться. Значения из `EnqueueOptions` переопределяют очередь, приоритет и настройки попыток отдельного вызова. `Delay` отсчитывается по часам PostgreSQL. Для конкретного момента задайте `RunAt` вместо `Delay`.

```csharp
var result = await client.EnqueueAsync(new ReportArgs(42), new EnqueueOptions
{
    Delay = TimeSpan.FromMinutes(5),
    UniqueKey = "reports.generate:42"
});
```

- **Дедупликация.** `UniqueKey` уникален среди активных задач схемы, включая другие очереди. При совпадении `result.Created` равен `false`. Если прежняя задача успела завершиться, `Id` может быть пустым. После удаления строки из очереди ключ снова свободен. Префикс `pgbolt:` зарезервирован.
- **Пакетная постановка.** `EnqueueManyAsync` принимает список аргументов одного типа и возвращает результат для каждого элемента. Для ключей передайте `uniqueKeySelector`. Если задачи нужны сейчас, пакетная постановка не требует создания workflow.
- **Управление по идентификатору.** `GetAsync(id)` возвращает состояние или `null`. `TriggerAsync(id)` позволяет приблизить запуск отложенной задачи. `RequeueAsync(id)` возвращает в очередь задачу в состоянии `Failed` или `Cancelled`. [Контракт клиента](src/PgBolt.Abstractions/IJobClient.cs)

**Поставить задачу вместе с изменением бизнес-данных.** Откройте `NpgsqlTransaction` с уровнем `ReadCommitted` в той же базе. Выполните прикладной SQL в этой транзакции и передайте её клиенту до коммита. Тогда откат отменит обе записи, а воркер увидит задачу только после коммита. В примере `connection` — уже открытое соединение Npgsql.

```csharp
await using var tx = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted);
// Здесь приложение сохраняет изменения через connection и tx.
await client.EnqueueAsync(new ReportArgs(42), new EnqueueOptions { Transaction = tx });
await tx.CommitAsync();
```

<a id="execution"></a>

## Выполнение, повторы и отмена

PgBolt выполняет задачи с семантикой **at-least-once**. Воркер берёт задачу в аренду и периодически продлевает её. Если процесс выполнил внешний вызов и упал до записи результата, другой воркер сможет получить ту же задачу. Для списания денег или отправки сообщения передайте внешней системе стабильный ключ операции либо сохраните отметку об обработке вместе с бизнес-изменением. `UniqueKey` защищает только от повторной постановки активной задачи.

| Событие | Как ведёт себя PgBolt |
| --- | --- |
| Обработчик успешно вернул `Task` | Воркер подтверждает выполнение. В `GetAsync` задача переходит в `Succeeded`. |
| Обработчик выбросил исключение | Пока остались попытки, PgBolt планирует повтор. По умолчанию `MaxAttempts = 10`, включая первую попытку. Задержка растёт от 15 секунд до базового предела в час, со случайным отклонением ±20 %. |
| Вызван `CancelAsync` | Задача в ожидании может быть отменена сразу. Для уже выполняющейся возвращается `CancellationRequested`. Обработчик получает сигнал через `CancellationToken`. Передавайте токен в запросы и долгие операции. |
| Для задачи нужен таймаут | Укажите `TimeoutSeconds` в `[Job]` или `DefaultJobTimeout` на сервере. Обработчик должен реагировать на отмену. |

Доступны настройки `MaxAttempts` и `RetryDelaysSeconds` в контракте, `MaxAttempts` и `RetryDelays` при постановке, а также `RetryPolicy` сервера. Номер текущей попытки, идентификатор задачи и логгер доступны через `JobContext`. [Атрибут задачи](src/PgBolt.Abstractions/JobAttribute.cs) · [Политика повторов](src/PgBolt.Server/RetryPolicy.cs)

<a id="queues"></a>

## Очереди и параллелизм

`Workers` задаёт размер общего пула виртуального сервера. В примере у сервера восемь общих слотов и ещё два только для отчётов — десять всего. Все серверы вместе могут держать в работе до четырёх задач очереди `reports`.

```csharp
builder.Services.AddPgBoltServer("background", server =>
{
    server.Workers = 8;
    server.Queue("reports", queue =>
    {
        queue.Reserved = 2;
        queue.ClusterMaxConcurrency = 4;
    });
    server.Queue("default");
});
```

`MaxConcurrency` ограничивает очередь внутри одного виртуального сервера. `Weight` задаёт относительную долю общего пула для очередей с весом. Через `IQueueSettingsManager` можно менять кластерный лимит и вес во время работы. Лимит `0` в runtime-переопределении приостанавливает выдачу новых задач. Уже взятые задачи продолжают выполняться. Впервые установленный лимит другие процессы подхватывают при обновлении настроек, по умолчанию в пределах 10 секунд. [Настройки очередей](src/PgBolt.Server/QueueOptions.cs) · [Управление в runtime](src/PgBolt.Abstractions/IQueueSettingsManager.cs)

<a id="cron"></a>

## Расписания cron

Для расписания, которое меняется вместе с приложением, добавьте объявление внутри `AddPgBolt`. Пример ставит задачу по будням в 08:00 UTC. `RecurringOwner` — стабильное имя приложения, общее для его реплик.

```csharp
options.RecurringOwner = "reporting";
options.AddRecurring("weekday-report", "0 8 * * 1-5", new ReportArgs(42));
```

Cron поддерживает пять полей или шесть с секундами. По умолчанию используется UTC. Для местного времени передайте `RecurringJobOptions.TimeZone`. После простоя политика `FireOnce` создаёт одну задачу за пропущенные срабатывания. `CatchUp` догоняет срабатывания внутри заданного окна. По умолчанию `OverlapPolicy.Skip` пропускает новый запуск, пока предыдущий активен.

Расписания, которыми управляет пользователь, создавайте через `IRecurringJobManager.AddOrUpdateAsync`. Менеджер умеет приостановить, возобновить и запустить расписание вручную. Для расписания из кода состав и параметры задаёт приложение-владелец. Запуск по времени требует хотя бы одного сервера с `RunScheduler = true` (значение по умолчанию). [API расписаний](src/PgBolt.Abstractions/IRecurringJobManager.cs) · [Часовые пояса и политики](src/PgBolt.Abstractions/RecurringJobOptions.cs)

<a id="workflows"></a>

## Цепочки и группы задач

`Workflow()` описывает зависимость между задачами, а `SubmitAsync()` записывает весь граф одной транзакцией. Здесь задача для отчёта 43 станет доступна после успешного завершения задачи для отчёта 42.

```csharp
var first = client.Workflow().Enqueue(new ReportArgs(42));
var next = first.ContinueWith(new ReportArgs(43));
await next.SubmitAsync();
```

- `OnSuccess` используется по умолчанию. После ошибки родителя продолжение ждёт его повторной постановки, а отмена родителя отменяет зависимое продолжение. `OnAnyFinished` запускает продолжение после любого терминального исхода.
- `Batch` объединяет задачи с общим состоянием и счётчиками. `ContinueWithBatch` создаёт группу после предыдущего шага. Такой сценарий подходит, например, для параллельной подготовки файлов с последующей сборкой архива.
- Продолжения и группы обрабатывает сервер с `RunMaintenance = true`. При выключенном обслуживании на всех серверах сигналы завершения копятся, а продолжения остаются в ожидании. [Контракт workflow](src/PgBolt.Abstractions/Workflow.cs) · [Исполняемый пример цепочки с группой](tests/E2E/PgBolt.E2E.Tests/WorkflowEndToEndTests.cs)

<a id="dashboard"></a>

## Панель управления и наблюдаемость

В ASP.NET Core зарегистрируйте `AddPgBolt` и `AddPgBoltDashboard` до `Build()`, затем добавьте маршрут. В примере приложение уже настроило аутентификацию и политику `PgBoltReaders`. Панель доступна по адресу `/pgbolt` в режиме просмотра.

```csharp
builder.Services.AddPgBoltDashboard(options => options.ReadOnly = true);
var app = builder.Build();
app.MapPgBoltDashboard().RequireAuthorization("PgBoltReaders");
```

Панель показывает очереди, задачи, группы и расписания. Для операторов можно включить действия и задать отдельную `ActionsPolicy`. При выключенном `ReadOnly` и пустой `ActionsPolicy` действия разрешены всем, кто видит панель. Для удалённого доступа задайте политику просмотра на группе маршрутов. [Параметры панели](src/PgBolt.Dashboard/PgBoltDashboardOptions.cs)

PgBolt связывает постановку и обработку задачи в одном трейсе. Для приложения с OpenTelemetry добавьте `AddPgBoltInstrumentation()` в настройки трассировки и метрик. Примеры настройки экспортёра, запросы PromQL и различия между метриками процесса и кластера собраны в [руководстве по наблюдаемости](docs/observability.md).

<a id="operations"></a>

## PostgreSQL, PgBouncer и ограничения

| Область | Что учитывать |
| --- | --- |
| База и схема | Все участники одной очереди должны указывать одну БД и `Schema` (по умолчанию `pgbolt`). На старте PgBolt проверяет, что уровень изоляции по умолчанию — `READ COMMITTED`. |
| Миграции | По умолчанию `AutoMigrate = true`. Если миграции применяет отдельный процесс, получите SQL через `PgBoltMigrations.GetScript(schema)`, примените его до старта и задайте `AutoMigrate = false`. Инициализация всё равно проверяет схему и готовит партиции. Для ранних экспериментальных схем автоматического обновления нет. [История изменений](CHANGELOG.md) |
| Партиции и обслуживание | По умолчанию PgBolt готовит партиции на семь дней вперёд. Оставьте `RunMaintenance` включённым хотя бы на одном сервере. Если партиции перестанут создаваться, по исчерпании запаса постановка задач начнёт завершаться ошибкой. [Настройки обслуживания](src/PgBolt.Server/PgBoltProcessingOptions.cs) |
| Хранение истории | `JobsRetention` и `EventsRetention` по умолчанию равны семи суткам. Для завершённых задач срок считается от суток **создания** по UTC, для событий — от суток события. Незавершённые задачи сохраняются. Задавайте одинаковые сроки во всех процессах одной схемы. Значение `null` в коде включает бессрочное хранение. |
| PgBouncer | Проверенная конфигурация — PostgreSQL 17 и PgBouncer 1.24 в режиме `pool_mode=transaction`. Передайте адрес пула в `ConnectionString`. По умолчанию `UsePreparedStatements = false`. При включении проверьте поддержку подготовленных запросов и `max_prepared_statements` в пуле. |
| Задержка и пропускная способность | В разных процессах воркеры обнаруживают новые задачи опросом БД. Ожидание зависит от интервала опроса, свободных слотов и нагрузки. На стенде 10 000 постановок/с выдержаны в течение десяти минут напрямую и через PgBouncer. Для длительной нагрузки и восстановления после удержания `xmin` показатели SLO 8 и 9 остаются открытыми. [Методика и результаты](docs/performance.md) |

<a id="development"></a>

## Сборка, тесты и нагрузочный стенд

Из корня репозитория с запущенным Docker можно проверить постановку и выполнение напрямую и через PgBouncer. Ожидаемый результат команды — два прошедших теста.

```sh
dotnet test tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj --filter FullyQualifiedName~Jobs_from_three_executor_assemblies_run_end_to_end
```

Собрать решение и запустить полный набор тестов можно из того же каталога.

```sh
dotnet build PgBolt.slnx
dotnet test PgBolt.slnx
```

Для измерений на своей конфигурации используйте [инструкцию нагрузочного стенда](benchmarks/PgBolt.LoadTest/README.md). Параметры стенда и уже полученные результаты приведены в [отчёте о производительности](docs/performance.md).
