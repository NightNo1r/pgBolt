# PgBolt

PgBolt — библиотека фоновых задач для .NET. Клиент ставит задачи в PostgreSQL, а воркеры забирают их из очереди и выполняют. PostgreSQL хранит расписание, состояние задач и сведения о попытках выполнения.

## Где искать код

- `src/PgBolt.Abstractions` — контракты задач и клиентский API. Генератор кода и анализаторы находятся в `src/PgBolt.Generators`.
- `src/PgBolt.Postgres` — клиент, SQL и миграции схемы. Запросы выборки и подтверждения задач находятся в `Storage`, миграции — в `Schema/Migrations`.
- `src/PgBolt.Server/Runtime` — диспетчер очередей, воркеры, продление аренды задач и запись результатов. Здесь меняется поведение опроса очереди.
- `src/PgBolt.Dashboard` — HTTP-интерфейс панели задач. Инструментация OpenTelemetry описана в [руководстве по наблюдаемости](docs/observability.md).

## Сборка и проверка

Используй .NET SDK из [global.json](global.json). Из корня репозитория решение собирается командой `dotnet build PgBolt.slnx`. Тесты сгруппированы в `tests` по компонентам. Для изменений диспетчера запусти его тесты.

```sh
dotnet test tests/PgBolt.Server.Tests/PgBolt.Server.Tests.csproj --filter FullyQualifiedName~DispatcherTests
```

Для полного прогона выполни `dotnet test PgBolt.slnx`. Интеграционные тесты с PostgreSQL используют Docker. Сценарии под нагрузкой, включая подключение через PgBouncer, описаны в [инструкции стенда](benchmarks/PgBolt.LoadTest/README.md), измерения и выводы — в [отчёте о производительности](docs/performance.md).

[Дорожная карта](docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md) связывает части системы с их спеками. Планы реализации находятся в [docs/superpowers/plans](docs/superpowers/plans).
