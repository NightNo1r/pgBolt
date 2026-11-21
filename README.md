# PgBolt

PgBolt — библиотека фоновых задач для .NET 10 с очередью в PostgreSQL. Клиент ставит задачу через `IJobClient`, а воркер вызывает обработчик `IJob<TArgs>`. Клиент и воркеры могут работать в разных процессах. PostgreSQL хранит аргументы, расписание, состояние и историю попыток.

PgBolt поддерживает отложенный запуск, расписания cron, повторные попытки и отмену. Задачи можно объединять в группы (batch) и задавать условия запуска продолжений. Для общей очереди можно ограничить число одновременно выполняемых задач на всех серверах.

## Запустить пример

Понадобятся .NET SDK из [global.json](global.json) и запущенный Docker. Из корня репозитория выполните:

```sh
dotnet test tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj --filter FullyQualifiedName~Jobs_from_three_executor_assemblies_run_end_to_end
```

Тест поднимает PostgreSQL и PgBouncer в контейнерах, ставит задачи из трёх сборок в очередь и проверяет, что каждая завершилась со статусом `Succeeded`. Ожидаемый результат — два прошедших теста, один с прямым подключением к PostgreSQL и один через PgBouncer.

## Подключить к приложению

1. Объявите тип аргументов с атрибутом `[Job]`. Имя связывает клиент и обработчик, а `Queue` выбирает очередь. [Пример контракта](tests/E2E/PgBolt.E2E.Contracts/Contracts.cs)
2. Реализуйте `IJob<TArgs>.ExecuteAsync`. В сборке обработчика добавьте тип аргументов в `JsonSerializerContext` и укажите контекст через `[assembly: PgBoltJsonContext(...)]`. Генератор PgBolt создаст для сборки `PgBoltJobRegistry`. [Пример обработчика и контекста](tests/E2E/PgBolt.E2E.JobsA/PingJob.cs) · [Подключение генератора](eng/UsePgBoltGenerator.props)
3. В хосте воркера вызовите `AddPgBolt`, задайте `ConnectionString` и зарегистрируйте обработчики через `AddJobs(PgBoltJobRegistry.Register)`. Затем вызовите `AddPgBoltServer` и укажите ту же очередь. При запуске хоста PgBolt по умолчанию создаёт схему `pgbolt` и применяет миграции. [Пример настройки хоста](tests/E2E/PgBolt.E2E.Tests/EndToEndTests.cs)
4. Получите `IJobClient` из DI и вызовите `await client.EnqueueAsync(new PingArgs("hello"))`. Результат содержит идентификатор задачи для запроса состояния через `GetAsync`. [Клиентский API](src/PgBolt.Abstractions/IJobClient.cs)

## Разработка и эксплуатация

Сборка решения и полный набор тестов:

```sh
dotnet build PgBolt.slnx
dotnet test PgBolt.slnx
```

- [Панель задач](src/PgBolt.Dashboard) — HTTP-интерфейс для просмотра очередей и задач.
- [Наблюдаемость](docs/observability.md) — подключение OpenTelemetry, метрики, трассировка и примеры PromQL.
- [Нагрузочный стенд](benchmarks/PgBolt.LoadTest/README.md) и [результаты измерений](docs/performance.md) — сценарии для PostgreSQL и PgBouncer.
- [История изменений](CHANGELOG.md) — изменения API, миграций и сроков хранения данных.
