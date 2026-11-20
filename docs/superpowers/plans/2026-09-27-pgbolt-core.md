# PgBolt Core (часть 1) — план реализации

> **Для исполнителя-агента.** План самодостаточен и не требует внешних скиллов: протокол исполнения, выбор моделей и требования к окружению описаны ниже, в разделах «Протокол исполнения» и «Требования к окружению». Шаги задач размечены чекбоксами (`- [ ]`).

**Goal:** Реализовать ядро PgBolt: схему PostgreSQL с миграциями и партициями, SQL горячего пути (enqueue / dequeue / ack / fail / heartbeat / sweeper / promoter / cancel / trigger / requeue), клиент `IJobClient`, DI-регистрацию, source generator и анализаторы.

**Architecture:** Три сборки: `PgBolt.Abstractions` (публичные контракты без зависимости от Npgsql, в пакет упакованы генератор и анализаторы), `PgBolt.Generators` (+ `PgBolt.Generators.CodeFixes`) и `PgBolt.Postgres` (схема, `PgBoltStore` с одним SQL-statement на операцию, клиент, DI). Каждая операция хранилища — отдельный partial-файл `PgBoltStore.<Op>.cs` со своим SQL и своим набором интеграционных тестов, которые прогоняются дважды: напрямую к PostgreSQL и через PgBouncer в transaction mode.

**Tech Stack:** .NET 10 (C# 14), Npgsql 10, System.Text.Json source generation, Roslyn incremental generators 4.14, xUnit v3, Testcontainers (PostgreSQL 17 + PgBouncer 1.24), Verify.SourceGenerators, Microsoft.CodeAnalysis.Testing.

**Spec:** `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` (карта проекта: `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md`). Номера разделов вида «§5.3» ниже ссылаются на спеку.

## Global Constraints

- Только .NET 10 (`net10.0`); генератор и анализаторы — `netstandard2.0` (требование Roslyn).
- Npgsql, без EF Core. Без reflection на горячем пути; `PgBolt.Abstractions` и `PgBolt.Postgres` собираются с `IsAotCompatible=true` и `TreatWarningsAsErrors=true`.
- PostgreSQL 13+ (тесты — на 17).
- Подключение напрямую или через PgBouncer в transaction mode. Запрещено: `LISTEN`, session-level advisory locks, `SET` (кроме `SET LOCAL` внутри транзакции), temp tables, опора на session state.
- Prepared statements выключены по умолчанию; опция `UsePreparedStatements` включает явный `PrepareAsync`.
- Каждая операция горячего пути — один SQL-statement. Многошаговые транзакции — только cancel, миграции, создание партиций, транзакционный enqueue пользователя.
- `{schema}` по умолчанию `pgbolt`; имя схемы валидируется регуляркой `^[a-z_][a-z0-9_]{0,62}$` и подставляется в SQL один раз.
- Коды `jobs.state`: 0 Pending, 1 Awaiting, 2 Succeeded, 3 Failed, 4 Cancelled.
- Коды `job_events.kind`: 1 Retry, 2 Failed, 3 LeaseExpired, 4 Cancelled, 5 Requeued, 6 Triggered, 7 NoExecutor.
- Контракт HOT (§4.2): партиции `jobs` создаются с `fillfactor` (по умолчанию 80); ни один индекс `jobs` не включает `state`, `attempt`, `started_at`, `finished_at`, `server_id`.
- `PgBolt.Abstractions` зависит только от BCL и `Microsoft.Extensions.*.Abstractions`, не от Npgsql.
- `jsonb` передаётся и читается как текст (`string`) с явным `NpgsqlDbType.Jsonb`.
- Имя задачи: `^[a-z0-9._-]{1,128}$`.
- Сообщения коммитов — Conventional Commits, в конце строка `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Требования к окружению

Проверить **до Task 1**. Если любой пункт не выполняется — остановиться и доложить, не начинать реализацию.

| Требование | Проверка | Ожидаемо |
|---|---|---|
| .NET SDK 10 | `dotnet --version` | `10.0.*` |
| Docker (нужен Testcontainers: PostgreSQL и PgBouncer поднимаются в контейнерах) | `docker info` | выполняется без ошибок |
| Доступ к Docker Hub | `docker pull postgres:17-alpine && docker pull edoburu/pgbouncer:v1.24.1-p1` | оба образа скачаны |
| Доступ к nuget.org (restore; тесты анализаторов скачивают `Microsoft.NETCore.App.Ref` во время запуска) | `curl -sSfI https://api.nuget.org/v3/index.json` | `HTTP/2 200` |
| Git с настроенным автором, push в ветку разрешён | `git config user.name && git config user.email` | обе строки непусты |

- Если Docker недоступен, **не заменять** базу моками, in-memory реализациями или пропуском тестов: интеграционные тесты — основная проверка корректности SQL. Остановиться и доложить.
- Если Docker Hub отвечает `429 Too Many Requests`, настроить зеркало реестра: в `/etc/docker/daemon.json` добавить `"registry-mirrors": ["https://mirror.gcr.io"]`, перезапустить Docker и повторить `docker pull`.
- Тестовые проекты работают в режиме VSTest (`Microsoft.NET.Test.Sdk` + `xunit.runner.visualstudio`), поэтому путь к проекту передаётся позиционно: `dotnet test tests/<Проект> --filter "..."`. Флаг `--project` существует только в режиме Microsoft.Testing.Platform и здесь падает с `MSB1001: Unknown switch`.
- Если `dotnet restore` не находит указанную в `Directory.Packages.props` версию пакета, взять последнюю стабильную версию той же major и зафиксировать её в этом же файле (отметить в отчёте задачи).

## Протокол исполнения

### Роли

- **Координатор** — основная сессия. Ведёт задачи строго по порядку (Task 1 → Task 32), запускает исполнителя и ревьюера, принимает решения по отчётам, отмечает чекбоксы.
- **Исполнитель** — отдельный субагент на каждую задачу, модель по колонке «Исполнитель» таблицы ниже. Получает свежий контекст без истории предыдущих задач.
- **Ревьюер** — отдельный субагент на каждую задачу, всегда **Opus 5.5** (`claude-opus-5-5`).

Если среда не поддерживает субагентов, координатор выполняет роли исполнителя и ревьюера сам, последовательно, с тем же чек-листом ревью.

### Модели

| Задачи | Исполнитель | Почему |
|---|---|---|
| 1–3, 5–8, 11, 12, 14–16, 19–22, 24, 30 | Sonnet 5 (`claude-sonnet-5`) | Код дан в плане почти целиком: перенести, собрать, прогнать |
| 4, 9, 10, 13, 17, 18, 23, 25–29, 31, 32 | Opus 5.5 (`claude-opus-5-5`) | Контейнеры, конкурентность, тонкая семантика SQL, Roslyn и code fix: чаще всего нужна отладка сверх написанного |
| Ревью каждой задачи | Opus 5.5 (`claude-opus-5-5`) | Сверка со спекой, поиск гонок и отклонений |

Модель задачи продублирована строкой **Model** под заголовком каждой задачи.

### Цикл одной задачи

1. **Координатор → исполнитель.** Передать дословно: полный текст задачи, раздел «Global Constraints», путь к спеке и номера разделов спеки, на которые ссылается задача, текущий SHA ветки. Потребовать отчёт в формате ниже.
2. **Исполнитель** выполняет шаги задачи по порядку: падающий тест → проверка, что падает → реализация → проверка, что проходит → коммит (одна задача = один или несколько коммитов с сообщениями из задачи). Отчёт:
   - `STATUS: DONE` или `STATUS: BLOCKED`;
   - SHA коммитов;
   - вывод последнего прогона тестов задачи (итоговые строки `passed/failed`);
   - отклонения от кода плана и причина каждого (например, другая сигнатура API пакета).
3. **Координатор → ревьюер.** Передать текст задачи, разделы спеки, `git diff <SHA до задачи>..HEAD`, отчёт исполнителя. Ревьюер отвечает `APPROVE` или `CHANGES` со списком конкретных замечаний (файл:строка, что не так, чем подтверждается).
4. При `CHANGES` — новый субагент-исполнитель (та же модель) получает задачу, замечания и diff, исправляет новым коммитом; затем повторное ревью. **Не более двух раундов исправлений**; после третьего `CHANGES` — остановиться и доложить.
5. При `APPROVE` координатор отмечает чекбоксы задачи в этом файле и коммитит `chore(plan): mark task N done`.
6. В конце каждой фазы координатор прогоняет `dotnet test PgBolt.slnx`; падение — остановиться и доложить.

### Чек-лист ревьюера

- Реализовано всё из шагов задачи; тесты задачи есть и проходят в обоих режимах (Direct и Bouncer), где задача их предусматривает.
- SQL совпадает со спекой дословно, кроме отличий, явно перечисленных в задаче.
- Операция горячего пути — один statement; нет `LISTEN`, session-level `SET`, session advisory locks, temp tables.
- Нет reflection на горячем пути; сборка без предупреждений (`TreatWarningsAsErrors`, AOT/trim-анализ).
- Тесты не ослаблены: нет удалённых/закомментированных assert'ов, `Skip`, пустых `catch`, подгонки ожидаемых значений под фактические без обоснования.
- Отклонения из отчёта исполнителя обоснованы и не противоречат спеке.
- Сообщения коммитов — Conventional Commits со строкой `Co-Authored-By`.

### Правила для исполнителя

- Не переходить к следующей задаче и не делать работу будущих задач.
- Не менять тесты, чтобы они прошли, если задача прямо этого не предписывает (исключение — принятие снапшотов Verify по инструкции Task 26–27). Упавший тест — сигнал исправить продакшен-код.
- Код из плана — эталон. Отклоняться можно только чтобы устранить ошибку компиляции или несовместимость API версии пакета; каждое отклонение — в отчёт.
- Спеку менять только там, где задача это предписывает (Task 32).
- Две безуспешные попытки исправить один и тот же падающий тест → `STATUS: BLOCKED` с выводом ошибки и гипотезами; не обходить проблему.
- Не использовать `--no-verify`, не переписывать историю, не пушить с `--force`.

## Структура файлов

```
PgBolt.slnx
global.json
Directory.Build.props                  — общие свойства (net10.0, nullable, warnings as errors)
Directory.Packages.props               — центральное управление версиями
eng/UsePgBoltGenerator.props           — подключение генератора к проектам репозитория

src/PgBolt.Abstractions/
  PgBolt.Abstractions.csproj           — пакует генератор/анализаторы в analyzers/dotnet/cs
  build/PgBolt.Abstractions.props      — CompilerVisibleProperty RootNamespace (buildTransitive)
  JobId.cs  JobState.cs  JobEventKind.cs  Results.cs
  JobAttribute.cs  JobDescriptor.cs  IJob.cs  JobContext.cs
  EnqueueOptions.cs  IJobClient.cs  JobInfo.cs  UnknownJobException.cs
  PgBoltJsonContextAttribute.cs  IPgBoltJobsBuilder.cs
  JobMetadata.cs                       — JobMetadata<TArgs> + JobMetadataRegistry

src/PgBolt.Generators/                 — netstandard2.0
  KnownNames.cs  EquatableArray.cs  IsExternalInit.cs
  Models/JobTypeModel.cs  Models/ExecutorModel.cs  Models/EnqueueCallModel.cs  Models/JsonContextModel.cs
  JobGenerator.cs  Emitter.cs
  Analysis/Diagnostics.cs  Analysis/JobNames.cs  Analysis/PgBoltAnalyzer.cs  Analysis/AnalysisState.cs
  AnalyzerReleases.Shipped.md  AnalyzerReleases.Unshipped.md   — release tracking диагностик (RS2008)
src/PgBolt.Generators.CodeFixes/       — netstandard2.0
  AddJsonSerializableCodeFix.cs

src/PgBolt.Postgres/
  PgBoltOptions.cs  SchemaName.cs  UuidV7.cs
  PgBoltDataSourceFactory.cs           — фабрика + класс-владелец PgBoltDataSource (dispose только своего источника)
  Schema/Migrations/V001__initial.sql  Schema/MigrationRunner.cs  Schema/PgBoltMigrations.cs
  Schema/PartitionManager.cs  Schema/PartitionEnsureResult.cs
  Storage/P.cs  Storage/DbRetry.cs  Storage/ReaderExtensions.cs  Storage/StoreRecords.cs
  Storage/PgBoltStore.cs
  Storage/PgBoltStore.Enqueue.cs  .Dequeue.cs  .Ack.cs  .Fail.cs  .Heartbeat.cs  .Sweeper.cs
  Storage/PgBoltStore.Promoter.cs  .Cancel.cs  .Trigger.cs  .Requeue.cs  .GetJob.cs  .JobTypes.cs
  Jobs/JobRegistry.cs  Jobs/JobsBuilder.cs
  Client/IWakeupSignal.cs  Client/InProcessWakeupSignal.cs  Client/JobTypesCache.cs
  Client/EnqueueOptionsResolver.cs  Client/PgBoltJobClient.cs
  Hosting/IPgBoltMaintenance.cs  Hosting/PgBoltInitializer.cs  Hosting/PgBoltServiceCollectionExtensions.cs

tests/PgBolt.Testing/                  — общая инфраструктура: контейнеры, TestDb, JobSeeder
tests/PgBolt.Abstractions.Tests/
tests/PgBolt.Postgres.Tests/           — интеграционные тесты хранилища, клиента, DI
  StoreTestBase.cs                     — база тестов хранилища (TestDb + PgBoltStore + хелперы)
  ClientTestSupport.cs                 — аргументы, JSON-контекст, регистрация метаданных, RecordingWakeup, FakeTransaction
tests/PgBolt.Generators.Tests/         — snapshot-тесты генератора, тесты анализаторов и code fix
tests/E2E/PgBolt.E2E.Contracts/        — только [Job]-типы
tests/E2E/PgBolt.E2E.JobsA/            — исполнитель + JSON-контекст
tests/E2E/PgBolt.E2E.JobsB/            — исполнитель + JSON-контекст
tests/E2E/PgBolt.E2E.Tests/            — свой исполнитель, InternalsVisibleTo от JobsA, сквозные тесты
```

Интеграционные тестовые классы пишутся как `abstract class XxxTests(PostgresFixture pg, ConnectionMode mode)` с парой наследников `XxxTests_Direct` / `XxxTests_Bouncer` в конце того же файла: xUnit исполняет унаследованные `[Fact]` в каждом наследнике. Тесты, которым нужна статистика PG, `EXPLAIN` или `pg_stat_database`, живут в классах только с `Direct`.

---

## Фаза 0. Каркас

### Task 1: Решение, общие свойства, пустые проекты

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `global.json`, `Directory.Build.props`, `Directory.Packages.props`, `PgBolt.slnx`, `.editorconfig`
- Create: `src/PgBolt.Abstractions/PgBolt.Abstractions.csproj`, `src/PgBolt.Postgres/PgBolt.Postgres.csproj`, `src/PgBolt.Generators/PgBolt.Generators.csproj`, `src/PgBolt.Generators.CodeFixes/PgBolt.Generators.CodeFixes.csproj`
- Create: `tests/PgBolt.Testing/PgBolt.Testing.csproj`, `tests/PgBolt.Abstractions.Tests/PgBolt.Abstractions.Tests.csproj`, `tests/PgBolt.Postgres.Tests/PgBolt.Postgres.Tests.csproj`, `tests/PgBolt.Generators.Tests/PgBolt.Generators.Tests.csproj`
- Test: `tests/PgBolt.Abstractions.Tests/SmokeTests.cs`

**Interfaces:**
- Produces: структура решения и CPM, на которые опираются все следующие задачи.

- [x] **Step 1: Создать `global.json`**

```json
{
  "sdk": { "version": "10.0.100", "rollForward": "latestFeature" }
}
```

- [x] **Step 2: Создать `Directory.Build.props`**

```xml
<Project>
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <RepoRoot>$(MSBuildThisFileDirectory)</RepoRoot>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
</Project>
```

- [x] **Step 3: Создать `Directory.Packages.props`**

```xml
<Project>
  <ItemGroup>
    <PackageVersion Include="Npgsql" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Hosting.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.TimeProvider.Testing" Version="10.0.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp" Version="4.14.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp.Workspaces" Version="4.14.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.Analyzers" Version="3.11.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp.CodeFix.Testing" Version="1.1.2" />
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp.Analyzer.Testing" Version="1.1.2" />
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="18.0.0" />
    <PackageVersion Include="xunit.v3" Version="3.1.0" />
    <PackageVersion Include="xunit.v3.extensibility.core" Version="3.1.0" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="3.1.5" />
    <PackageVersion Include="Testcontainers" Version="4.15.0" />
    <PackageVersion Include="Testcontainers.PostgreSql" Version="4.15.0" />
    <PackageVersion Include="Verify.XunitV3" Version="31.0.0" />
    <PackageVersion Include="Verify.SourceGenerators" Version="2.5.0" />
  </ItemGroup>
</Project>
```

- [x] **Step 4: Создать `.editorconfig`**

```ini
root = true

[*.cs]
indent_style = space
indent_size = 4
csharp_style_namespace_declarations = file_scoped:warning

[*.{csproj,props,json,sql}]
indent_style = space
indent_size = 2
```

- [x] **Step 5: Создать csproj библиотек**

`src/PgBolt.Abstractions/PgBolt.Abstractions.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <RootNamespace>PgBolt</RootNamespace>
    <IsAotCompatible>true</IsAotCompatible>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
  </ItemGroup>
  <ItemGroup>
    <InternalsVisibleTo Include="PgBolt.Abstractions.Tests" />
  </ItemGroup>
</Project>
```

`src/PgBolt.Postgres/PgBolt.Postgres.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <RootNamespace>PgBolt.Postgres</RootNamespace>
    <IsAotCompatible>true</IsAotCompatible>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Npgsql" />
    <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\PgBolt.Abstractions\PgBolt.Abstractions.csproj" />
  </ItemGroup>
  <ItemGroup>
    <EmbeddedResource Include="Schema\Migrations\*.sql" />
  </ItemGroup>
  <ItemGroup>
    <InternalsVisibleTo Include="PgBolt.Postgres.Tests" />
    <InternalsVisibleTo Include="PgBolt.Testing" />
    <InternalsVisibleTo Include="PgBolt.E2E.Tests" />
  </ItemGroup>
</Project>
```

`src/PgBolt.Generators/PgBolt.Generators.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <IsRoslynComponent>true</IsRoslynComponent>
    <EnforceExtendedAnalyzerRules>true</EnforceExtendedAnalyzerRules>
    <RootNamespace>PgBolt.Generators</RootNamespace>
    <ImplicitUsings>disable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" PrivateAssets="all" />
    <PackageReference Include="Microsoft.CodeAnalysis.Analyzers" PrivateAssets="all" />
  </ItemGroup>
  <ItemGroup>
    <InternalsVisibleTo Include="PgBolt.Generators.Tests" />
    <InternalsVisibleTo Include="PgBolt.Generators.CodeFixes" />
  </ItemGroup>
</Project>
```

`src/PgBolt.Generators.CodeFixes/PgBolt.Generators.CodeFixes.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <IsRoslynComponent>true</IsRoslynComponent>
    <RootNamespace>PgBolt.Generators.CodeFixes</RootNamespace>
    <ImplicitUsings>disable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp.Workspaces" PrivateAssets="all" />
    <PackageReference Include="Microsoft.CodeAnalysis.Analyzers" PrivateAssets="all" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\PgBolt.Generators\PgBolt.Generators.csproj" />
  </ItemGroup>
</Project>
```

- [x] **Step 6: Создать csproj тестовых проектов**

`tests/PgBolt.Testing/PgBolt.Testing.csproj` (библиотека, не тесты):

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <ItemGroup>
    <PackageReference Include="Testcontainers" />
    <PackageReference Include="Testcontainers.PostgreSql" />
    <PackageReference Include="xunit.v3.extensibility.core" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\PgBolt.Postgres\PgBolt.Postgres.csproj" />
  </ItemGroup>
</Project>
```

`tests/PgBolt.Abstractions.Tests/PgBolt.Abstractions.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <IsPackable>false</IsPackable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" />
    <PackageReference Include="xunit.v3" />
    <PackageReference Include="xunit.runner.visualstudio" />
  </ItemGroup>
  <ItemGroup>
    <Using Include="Xunit" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\PgBolt.Abstractions\PgBolt.Abstractions.csproj" />
  </ItemGroup>
</Project>
```

`tests/PgBolt.Postgres.Tests/PgBolt.Postgres.Tests.csproj` — то же, что выше, плюс:

```xml
  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" />
    <PackageReference Include="Microsoft.Extensions.TimeProvider.Testing" />
    <ProjectReference Include="..\PgBolt.Testing\PgBolt.Testing.csproj" />
  </ItemGroup>
```

(ссылку на `PgBolt.Abstractions` заменить ссылкой на `..\..\src\PgBolt.Postgres\PgBolt.Postgres.csproj`).

`tests/PgBolt.Generators.Tests/PgBolt.Generators.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <IsPackable>false</IsPackable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" />
    <PackageReference Include="xunit.v3" />
    <PackageReference Include="xunit.runner.visualstudio" />
    <PackageReference Include="Verify.XunitV3" />
    <PackageReference Include="Verify.SourceGenerators" />
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" />
    <!-- Явно: иначе CodeFix.Testing 1.1.2 тянет Workspaces 1.0.1 (NU1701 как ошибка, несовпадение Roslyn в рантайме). -->
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp.Workspaces" />
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp.CodeFix.Testing" />
    <!-- CSharpAnalyzerTest<,> (AnalyzerTests) — в отдельном пакете, CodeFix.Testing его не тянет. -->
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp.Analyzer.Testing" />
  </ItemGroup>
  <ItemGroup>
    <Using Include="Xunit" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\PgBolt.Abstractions\PgBolt.Abstractions.csproj" />
    <ProjectReference Include="..\..\src\PgBolt.Generators\PgBolt.Generators.csproj" />
    <ProjectReference Include="..\..\src\PgBolt.Generators.CodeFixes\PgBolt.Generators.CodeFixes.csproj" />
  </ItemGroup>
</Project>
```

- [x] **Step 7: Создать `PgBolt.slnx`**

```xml
<Solution>
  <Folder Name="/src/">
    <Project Path="src/PgBolt.Abstractions/PgBolt.Abstractions.csproj" />
    <Project Path="src/PgBolt.Generators/PgBolt.Generators.csproj" />
    <Project Path="src/PgBolt.Generators.CodeFixes/PgBolt.Generators.CodeFixes.csproj" />
    <Project Path="src/PgBolt.Postgres/PgBolt.Postgres.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/PgBolt.Testing/PgBolt.Testing.csproj" />
    <Project Path="tests/PgBolt.Abstractions.Tests/PgBolt.Abstractions.Tests.csproj" />
    <Project Path="tests/PgBolt.Postgres.Tests/PgBolt.Postgres.Tests.csproj" />
    <Project Path="tests/PgBolt.Generators.Tests/PgBolt.Generators.Tests.csproj" />
  </Folder>
</Solution>
```

- [x] **Step 8: Написать smoke-тест**

`tests/PgBolt.Abstractions.Tests/SmokeTests.cs`:

```csharp
namespace PgBolt.Abstractions.Tests;

public sealed class SmokeTests
{
    [Fact]
    public void Solution_builds_and_tests_run() => Assert.True(true);
}
```

- [x] **Step 9: Собрать и прогнать**

Run: `dotnet build PgBolt.slnx && dotnet test tests/PgBolt.Abstractions.Tests`
Expected: сборка без предупреждений, `Passed!  - Failed: 0, Passed: 1`.

- [x] **Step 10: Commit**

```bash
git add global.json Directory.Build.props Directory.Packages.props .editorconfig PgBolt.slnx src tests
git commit -m "build: solution skeleton with central package management"
```

---

## Фаза 1. Abstractions

### Task 2: Публичные контракты

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Abstractions/JobId.cs`, `JobState.cs`, `JobEventKind.cs`, `Results.cs`, `JobAttribute.cs`, `JobDescriptor.cs`, `IJob.cs`, `JobContext.cs`, `EnqueueOptions.cs`, `IJobClient.cs`, `JobInfo.cs`, `UnknownJobException.cs`, `PgBoltJsonContextAttribute.cs`, `IPgBoltJobsBuilder.cs`
- Test: `tests/PgBolt.Abstractions.Tests/ContractsTests.cs`
- Delete: `tests/PgBolt.Abstractions.Tests/SmokeTests.cs`

**Interfaces:**
- Produces (все в namespace `PgBolt`):
  - `readonly record struct JobId(Guid Value)` с `static JobId New()`
  - `enum JobState { Scheduled, Enqueued, Processing, Awaiting, Succeeded, Failed, Cancelled }`
  - `enum JobEventKind : short { Retry = 1, Failed = 2, LeaseExpired = 3, Cancelled = 4, Requeued = 5, Triggered = 6, NoExecutor = 7 }`
  - `readonly record struct EnqueueResult(JobId? Id, bool Created)`, `enum CancelResult { NotFound, AlreadyFinished, Cancelled, CancellationRequested }`, `enum RequeueResult { NotFound, NotTerminal, Conflict, Requeued }`
  - `JobAttribute(string name)` со свойствами `Name`, `Queue = "default"`, `MaxAttempts = 10`, `Priority`, `RetryDelaysSeconds`
  - `sealed record JobDescriptor(string Name, string Queue, int MaxAttempts, short Priority, int[]? RetryDelaysSeconds)` с методом `bool SameAs(JobDescriptor other)`
  - `interface IJob<in TArgs> { Task ExecuteAsync(TArgs args, JobContext context, CancellationToken cancellationToken); }`
  - `sealed class JobContext(JobId jobId, string jobName, int attempt, int maxAttempts, JobId? parentId, Guid? batchId, ILogger logger)`
  - `sealed record EnqueueOptions` (поля §6.2), `interface IJobClient` (§6.2)
  - `sealed record JobInfo(...)`, `sealed record JobEventInfo(...)`
  - `sealed class UnknownJobException(string jobName) : Exception`
  - `[AttributeUsage(Assembly)] sealed class PgBoltJsonContextAttribute(Type contextType)`
  - `interface IPgBoltJobsBuilder { IPgBoltJobsBuilder AddExecutor<TArgs, TJob>(JobDescriptor descriptor, JsonTypeInfo<TArgs> argsTypeInfo) where TJob : class, IJob<TArgs>; }`

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Abstractions.Tests/ContractsTests.cs`:

```csharp
using Microsoft.Extensions.Logging.Abstractions;

namespace PgBolt.Abstractions.Tests;

public sealed class ContractsTests
{
    [Fact]
    public void JobId_New_is_uuid_v7()
    {
        var id = JobId.New();
        Assert.Equal(7, id.Value.Version);
        Assert.Equal(id.Value.ToString(), id.ToString());
    }

    [Fact]
    public void JobAttribute_has_spec_defaults()
    {
        var a = new JobAttribute("orders.send-receipt");
        Assert.Equal("orders.send-receipt", a.Name);
        Assert.Equal("default", a.Queue);
        Assert.Equal(10, a.MaxAttempts);
        Assert.Equal(0, a.Priority);
        Assert.Null(a.RetryDelaysSeconds);
    }

    [Fact]
    public void JobEventKind_codes_match_spec()
    {
        Assert.Equal(1, (short)JobEventKind.Retry);
        Assert.Equal(2, (short)JobEventKind.Failed);
        Assert.Equal(3, (short)JobEventKind.LeaseExpired);
        Assert.Equal(4, (short)JobEventKind.Cancelled);
        Assert.Equal(5, (short)JobEventKind.Requeued);
        Assert.Equal(6, (short)JobEventKind.Triggered);
        Assert.Equal(7, (short)JobEventKind.NoExecutor);
    }

    [Fact]
    public void JobDescriptor_SameAs_compares_retry_delays_by_value()
    {
        var a = new JobDescriptor("x", "q", 3, 1, [1, 2]);
        var b = new JobDescriptor("x", "q", 3, 1, [1, 2]);
        var c = new JobDescriptor("x", "q", 3, 1, null);
        Assert.True(a.SameAs(b));
        Assert.False(a.SameAs(c));
    }

    [Fact]
    public void JobContext_exposes_constructor_values()
    {
        var id = JobId.New();
        var parent = JobId.New();
        var batch = Guid.NewGuid();
        var ctx = new JobContext(id, "n", 2, 5, parent, batch, NullLogger.Instance);
        Assert.Equal((id, "n", 2, 5, parent, batch), (ctx.JobId, ctx.JobName, ctx.Attempt, ctx.MaxAttempts, ctx.ParentId, ctx.BatchId));
        Assert.Same(NullLogger.Instance, ctx.Logger);
    }

    [Fact]
    public void UnknownJobException_carries_name()
    {
        var e = new UnknownJobException("a.b");
        Assert.Equal("a.b", e.JobName);
        Assert.Contains("a.b", e.Message);
    }
}
```

- [x] **Step 2: Убедиться, что тесты не компилируются**

Run: `dotnet test tests/PgBolt.Abstractions.Tests`
Expected: FAIL — ошибки компиляции `JobId`, `JobAttribute` … не найдены.

- [x] **Step 3: Реализовать контракты**

`src/PgBolt.Abstractions/JobId.cs`:

```csharp
namespace PgBolt;

/// <summary>Идентификатор задачи (UUIDv7, генерируется на клиенте).</summary>
public readonly record struct JobId(Guid Value)
{
    public static JobId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString();
}
```

`src/PgBolt.Abstractions/JobState.cs`:

```csharp
namespace PgBolt;

/// <summary>Состояние задачи для клиента: Scheduled/Enqueued/Processing выводятся из строки queue (§2).</summary>
public enum JobState
{
    Scheduled,
    Enqueued,
    Processing,
    Awaiting,
    Succeeded,
    Failed,
    Cancelled,
}
```

`src/PgBolt.Abstractions/JobEventKind.cs`:

```csharp
namespace PgBolt;

/// <summary>Коды job_events.kind (§4.3). Новые коды только добавляются.</summary>
public enum JobEventKind : short
{
    Retry = 1,
    Failed = 2,
    LeaseExpired = 3,
    Cancelled = 4,
    Requeued = 5,
    Triggered = 6,
    NoExecutor = 7,
}
```

`src/PgBolt.Abstractions/Results.cs`:

```csharp
namespace PgBolt;

public readonly record struct EnqueueResult(JobId? Id, bool Created);

public enum CancelResult { NotFound, AlreadyFinished, Cancelled, CancellationRequested }

public enum RequeueResult { NotFound, NotTerminal, Conflict, Requeued }
```

`src/PgBolt.Abstractions/JobAttribute.cs`:

```csharp
namespace PgBolt;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class JobAttribute(string name) : Attribute
{
    public string Name { get; } = name;
    public string Queue { get; init; } = "default";
    public int MaxAttempts { get; init; } = 10;
    public short Priority { get; init; }
    public int[]? RetryDelaysSeconds { get; init; }
}
```

`src/PgBolt.Abstractions/JobDescriptor.cs`:

```csharp
namespace PgBolt;

/// <summary>Умолчания задачи из [Job] (или из job_types для динамического enqueue).</summary>
public sealed record JobDescriptor(string Name, string Queue, int MaxAttempts, short Priority, int[]? RetryDelaysSeconds)
{
    /// <summary>Сравнение по значению, включая содержимое RetryDelaysSeconds.</summary>
    public bool SameAs(JobDescriptor other) =>
        Name == other.Name && Queue == other.Queue && MaxAttempts == other.MaxAttempts && Priority == other.Priority
        && (RetryDelaysSeconds ?? []).AsSpan().SequenceEqual(other.RetryDelaysSeconds ?? [])
        && (RetryDelaysSeconds is null) == (other.RetryDelaysSeconds is null);
}
```

`src/PgBolt.Abstractions/IJob.cs`:

```csharp
namespace PgBolt;

public interface IJob<in TArgs>
{
    Task ExecuteAsync(TArgs args, JobContext context, CancellationToken cancellationToken);
}
```

`src/PgBolt.Abstractions/JobContext.cs`:

```csharp
using Microsoft.Extensions.Logging;

namespace PgBolt;

public sealed class JobContext(JobId jobId, string jobName, int attempt, int maxAttempts, JobId? parentId, Guid? batchId, ILogger logger)
{
    public JobId JobId { get; } = jobId;
    public string JobName { get; } = jobName;
    public int Attempt { get; } = attempt;
    public int MaxAttempts { get; } = maxAttempts;
    public JobId? ParentId { get; } = parentId;
    public Guid? BatchId { get; } = batchId;
    public ILogger Logger { get; } = logger;
}
```

`src/PgBolt.Abstractions/EnqueueOptions.cs`:

```csharp
using System.Data.Common;

namespace PgBolt;

public sealed record EnqueueOptions
{
    public string? Queue { get; init; }
    public short? Priority { get; init; }
    public DateTimeOffset? RunAt { get; init; }
    /// <summary>Взаимоисключимо с RunAt. Отсчитывается от now() сервера БД.</summary>
    public TimeSpan? Delay { get; init; }
    public string? UniqueKey { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
    /// <summary>Транзакционный enqueue: только NpgsqlTransaction с уровнем READ COMMITTED.</summary>
    public DbTransaction? Transaction { get; init; }
}
```

`src/PgBolt.Abstractions/IJobClient.cs`:

```csharp
using System.Text.Json;

namespace PgBolt;

public interface IJobClient
{
    ValueTask<EnqueueResult> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<IReadOnlyList<EnqueueResult>> EnqueueManyAsync<TArgs>(
        IReadOnlyList<TArgs> args, EnqueueOptions? options = null,
        Func<TArgs, string?>? uniqueKeySelector = null, CancellationToken ct = default);

    ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null, CancellationToken ct = default);

    ValueTask<CancelResult> CancelAsync(JobId id, string? reason = null, CancellationToken ct = default);
    ValueTask<bool> TriggerAsync(JobId id, CancellationToken ct = default);
    ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default);
    ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default);
}
```

`src/PgBolt.Abstractions/JobInfo.cs`:

```csharp
using System.Text.Json;

namespace PgBolt;

public sealed record JobInfo(
    JobId Id,
    string Name,
    string Queue,
    short Priority,
    JobState State,
    int Attempt,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset? RunAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? FinishedAt,
    Guid? ServerId,
    string? UniqueKey,
    bool CancellationRequested,
    JsonElement Args,
    IReadOnlyList<JobEventInfo> Events);

public sealed record JobEventInfo(JobEventKind Kind, DateTimeOffset At, Guid? ServerId, int? Attempt, string? Message, string? Exception);
```

`src/PgBolt.Abstractions/UnknownJobException.cs`:

```csharp
namespace PgBolt;

public sealed class UnknownJobException(string jobName)
    : Exception($"Job '{jobName}' is not published in job_types.")
{
    public string JobName { get; } = jobName;
}
```

`src/PgBolt.Abstractions/PgBoltJsonContextAttribute.cs`:

```csharp
namespace PgBolt;

/// <summary>Указывает JsonSerializerContext сборки, в котором объявлены args-типы задач (§7).</summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class PgBoltJsonContextAttribute(Type contextType) : Attribute
{
    public Type ContextType { get; } = contextType;
}
```

`src/PgBolt.Abstractions/IPgBoltJobsBuilder.cs`:

```csharp
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization.Metadata;

namespace PgBolt;

/// <summary>Приёмник регистраций из сгенерированного PgBoltJobRegistry.Register.</summary>
public interface IPgBoltJobsBuilder
{
    IPgBoltJobsBuilder AddExecutor<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TJob>(
        JobDescriptor descriptor, JsonTypeInfo<TArgs> argsTypeInfo)
        where TJob : class, IJob<TArgs>;
}
```

Удалить `SmokeTests.cs`.

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Abstractions.Tests`
Expected: PASS (6 тестов).

- [x] **Step 5: Commit**

```bash
git add -A src/PgBolt.Abstractions tests/PgBolt.Abstractions.Tests
git commit -m "feat(abstractions): public job contracts, client API and result types"
```

### Task 3: Реестр метаданных `JobMetadata<TArgs>`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Abstractions/JobMetadata.cs`
- Test: `tests/PgBolt.Abstractions.Tests/JobMetadataTests.cs`

**Interfaces:**
- Consumes: `JobDescriptor` (Task 2).
- Produces:
  - `public static class JobMetadata<TArgs> { public static JobDescriptor? Descriptor { get; } public static JsonTypeInfo<TArgs>? TypeInfo { get; } }`
  - `public static class JobMetadataRegistry { public static void Register<TArgs>(JobDescriptor descriptor, JsonTypeInfo<TArgs>? typeInfo); }` — вызывается сгенерированным module initializer'ом (Task 26) и `JobsBuilder` (Task 21). Семантика слияния: дескриптор задаётся первым вызовом, повторный с тем же значением — no-op, с другим — `InvalidOperationException`; `TypeInfo` заполняется первым не-null значением и никогда не затирается null'ом (сборка контрактов регистрирует дескриптор без контекста).

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Abstractions.Tests/JobMetadataTests.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace PgBolt.Abstractions.Tests;

public sealed record MetaArgsA(int X);
public sealed record MetaArgsB(int X);
public sealed record MetaArgsC(int X);

[JsonSerializable(typeof(MetaArgsA))]
[JsonSerializable(typeof(MetaArgsB))]
[JsonSerializable(typeof(MetaArgsC))]
internal sealed partial class MetaJson : JsonSerializerContext;

public sealed class JobMetadataTests
{
    [Fact]
    public void Register_sets_descriptor_and_type_info()
    {
        var d = new JobDescriptor("meta.a", "q", 3, 0, null);
        JobMetadataRegistry.Register(d, MetaJson.Default.MetaArgsA);
        Assert.True(JobMetadata<MetaArgsA>.Descriptor!.SameAs(d));
        Assert.Same(MetaJson.Default.MetaArgsA, JobMetadata<MetaArgsA>.TypeInfo);
    }

    [Fact]
    public void Null_type_info_never_overwrites_existing()
    {
        var d = new JobDescriptor("meta.b", "q", 3, 0, [5]);
        JobMetadataRegistry.Register(d, MetaJson.Default.MetaArgsB);
        JobMetadataRegistry.Register<MetaArgsB>(d with { RetryDelaysSeconds = [5] }, null);
        Assert.Same(MetaJson.Default.MetaArgsB, JobMetadata<MetaArgsB>.TypeInfo);
    }

    [Fact]
    public void Type_info_is_filled_by_later_registration()
    {
        var d = new JobDescriptor("meta.c", "q", 3, 0, null);
        JobMetadataRegistry.Register<MetaArgsC>(d, null);
        Assert.Null(JobMetadata<MetaArgsC>.TypeInfo);
        JobMetadataRegistry.Register(d, MetaJson.Default.MetaArgsC);
        Assert.Same(MetaJson.Default.MetaArgsC, JobMetadata<MetaArgsC>.TypeInfo);
    }

    [Fact]
    public void Conflicting_descriptor_throws()
    {
        JobMetadataRegistry.Register<ConflictArgs>(new JobDescriptor("meta.d", "q", 3, 0, null), null);
        var e = Assert.Throws<InvalidOperationException>(() =>
            JobMetadataRegistry.Register<ConflictArgs>(new JobDescriptor("meta.d", "other", 3, 0, null), null));
        Assert.Contains(nameof(ConflictArgs), e.Message);
    }

    private sealed record ConflictArgs(int X);
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Abstractions.Tests`
Expected: FAIL — `JobMetadataRegistry` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Abstractions/JobMetadata.cs`:

```csharp
using System.Text.Json.Serialization.Metadata;

namespace PgBolt;

/// <summary>Метаданные [Job]-типа, заполняемые сгенерированным кодом (без reflection).</summary>
public static class JobMetadata<TArgs>
{
    public static JobDescriptor? Descriptor { get; private set; }
    public static JsonTypeInfo<TArgs>? TypeInfo { get; private set; }

    internal static void Merge(JobDescriptor descriptor, JsonTypeInfo<TArgs>? typeInfo)
    {
        if (Descriptor is null)
            Descriptor = descriptor;
        else if (!Descriptor.SameAs(descriptor))
            throw new InvalidOperationException(
                $"Conflicting [Job] metadata for {typeof(TArgs).FullName}: '{Descriptor}' vs '{descriptor}'.");

        if (typeInfo is not null && TypeInfo is null)
            TypeInfo = typeInfo;
    }
}

public static class JobMetadataRegistry
{
    private static readonly Lock Gate = new();

    public static void Register<TArgs>(JobDescriptor descriptor, JsonTypeInfo<TArgs>? typeInfo)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        lock (Gate)
            JobMetadata<TArgs>.Merge(descriptor, typeInfo);
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Abstractions.Tests`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Abstractions/JobMetadata.cs tests/PgBolt.Abstractions.Tests/JobMetadataTests.cs
git commit -m "feat(abstractions): JobMetadata registry with merge semantics"
```

---
## Фаза 2. Тестовая инфраструктура, схема, миграции, партиции

### Task 4: Контейнеры PostgreSQL + PgBouncer и `TestDb`

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `tests/PgBolt.Testing/PostgresFixture.cs`, `tests/PgBolt.Testing/TestDb.cs`
- Create: `tests/PgBolt.Postgres.Tests/AssemblyInfo.cs`
- Test: `tests/PgBolt.Postgres.Tests/InfrastructureTests.cs`

**Interfaces:**
- Produces (namespace `PgBolt.Testing`):
  - `enum ConnectionMode { Direct, Bouncer }`
  - `sealed class PostgresFixture : IAsyncLifetime` — `string DirectConnectionString`, `string BouncerConnectionString`, `string ConnectionString(ConnectionMode mode)`
  - `sealed class TestDb : IAsyncDisposable` — `static Task<TestDb> CreateAsync(PostgresFixture pg, ConnectionMode mode)`, свойства `Schema`, `Mode`, `DataSource` (через выбранный режим), `Admin` (всегда напрямую), методы `string Sql(string text)` (подставляет `{schema}`), `Task ExecAsync(string sql, params (string Name, object? Value)[] ps)`, `Task<T> ScalarAsync<T>(string sql, params (string Name, object? Value)[] ps)`. В Task 6 и Task 7 `CreateAsync` расширяется миграциями и партициями.

- [x] **Step 1: Написать фикстуру контейнеров**

`tests/PgBolt.Testing/PostgresFixture.cs`:

```csharp
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.PostgreSql;
using Xunit;

namespace PgBolt.Testing;

public enum ConnectionMode { Direct, Bouncer }

/// <summary>Один PostgreSQL и один PgBouncer (transaction mode) на тестовую сборку.</summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private const string Database = "pgbolt";
    private const string User = "postgres";
    private const string Password = "postgres";

    private const string BouncerIni = """
        [databases]
        pgbolt = host=pg port=5432 dbname=pgbolt user=postgres password=postgres

        [pgbouncer]
        listen_addr = 0.0.0.0
        listen_port = 6432
        auth_type = plain
        auth_file = /etc/pgbouncer/userlist.txt
        pool_mode = transaction
        max_prepared_statements = 0
        max_client_conn = 1000
        default_pool_size = 80
        ignore_startup_parameters = extra_float_digits
        """;

    private INetwork _network = null!;
    private PostgreSqlContainer _postgres = null!;
    private IContainer _bouncer = null!;

    public string DirectConnectionString { get; private set; } = "";
    public string BouncerConnectionString { get; private set; } = "";

    public string ConnectionString(ConnectionMode mode) =>
        mode == ConnectionMode.Direct ? DirectConnectionString : BouncerConnectionString;

    public async ValueTask InitializeAsync()
    {
        _network = new NetworkBuilder().Build();
        await _network.CreateAsync();

        _postgres = new PostgreSqlBuilder("postgres:17-alpine")
            .WithDatabase(Database)
            .WithUsername(User)
            .WithPassword(Password)
            .WithNetwork(_network)
            .WithNetworkAliases("pg")
            .WithCommand("-c", "max_connections=400", "-c", "max_locks_per_transaction=256")
            .Build();
        await _postgres.StartAsync();
        DirectConnectionString = _postgres.GetConnectionString();

        _bouncer = new ContainerBuilder("edoburu/pgbouncer:v1.24.1-p1")
            .WithNetwork(_network)
            .WithResourceMapping(Encoding.UTF8.GetBytes(BouncerIni), "/etc/pgbouncer/pgbouncer.ini")
            .WithResourceMapping(Encoding.UTF8.GetBytes($"\"{User}\" \"{Password}\"\n"), "/etc/pgbouncer/userlist.txt")
            .WithPortBinding(6432, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(6432))
            .Build();
        await _bouncer.StartAsync();
        BouncerConnectionString =
            $"Host={_bouncer.Hostname};Port={_bouncer.GetMappedPublicPort(6432)};Database={Database};" +
            $"Username={User};Password={Password};No Reset On Close=true;Max Auto Prepare=0";
    }

    public async ValueTask DisposeAsync()
    {
        await _bouncer.DisposeAsync();
        await _postgres.DisposeAsync();
        await _network.DisposeAsync();
    }
}
```

- [x] **Step 2: Написать `TestDb` (без миграций)**

`tests/PgBolt.Testing/TestDb.cs`:

```csharp
using Npgsql;

namespace PgBolt.Testing;

/// <summary>Изолированная схема на тест. DataSource идёт через выбранный режим, Admin — всегда напрямую.</summary>
public sealed class TestDb : IAsyncDisposable
{
    private TestDb(string schema, ConnectionMode mode, string connectionString, NpgsqlDataSource dataSource, NpgsqlDataSource admin)
    {
        Schema = schema;
        Mode = mode;
        ConnectionString = connectionString;
        DataSource = dataSource;
        Admin = admin;
    }

    public string Schema { get; }
    public ConnectionMode Mode { get; }
    public string ConnectionString { get; }
    public NpgsqlDataSource DataSource { get; }
    public NpgsqlDataSource Admin { get; }

    public static async Task<TestDb> CreateAsync(PostgresFixture pg, ConnectionMode mode)
    {
        var schema = "t_" + Guid.NewGuid().ToString("N")[..16];
        var cs = pg.ConnectionString(mode);
        var db = new TestDb(schema, mode, cs, NpgsqlDataSource.Create(cs), NpgsqlDataSource.Create(pg.DirectConnectionString));
        await db.ExecAsync("CREATE SCHEMA {schema}");
        return db;
    }

    public string Sql(string text) => text.Replace("{schema}", Schema, StringComparison.Ordinal);

    public async Task ExecAsync(string sql, params (string Name, object? Value)[] ps)
    {
        await using var cmd = Admin.CreateCommand(Sql(sql));
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<T> ScalarAsync<T>(string sql, params (string Name, object? Value)[] ps)
    {
        await using var cmd = Admin.CreateCommand(Sql(sql));
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        var result = await cmd.ExecuteScalarAsync();
        return result is DBNull or null ? default! : (T)result;
    }

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await ExecAsync("DROP SCHEMA IF EXISTS {schema} CASCADE");
        await Admin.DisposeAsync();
    }
}
```

- [x] **Step 3: Подключить фикстуру к тестовой сборке и написать тест**

`tests/PgBolt.Postgres.Tests/AssemblyInfo.cs`:

```csharp
using PgBolt.Testing;

[assembly: AssemblyFixture(typeof(PostgresFixture))]
```

`tests/PgBolt.Postgres.Tests/InfrastructureTests.cs`:

```csharp
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class InfrastructureTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Connection_through_mode_executes_queries()
    {
        await using var cmd = _db.DataSource.CreateCommand("SELECT 41 + 1");
        Assert.Equal(42, (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!);
    }

    [Fact]
    public async Task Schema_is_created_and_isolated()
    {
        var exists = await _db.ScalarAsync<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = @s)", ("s", _db.Schema));
        Assert.True(exists);
    }
}

public sealed class InfrastructureTests_Direct(PostgresFixture pg) : InfrastructureTests(pg, ConnectionMode.Direct);
public sealed class InfrastructureTests_Bouncer(PostgresFixture pg) : InfrastructureTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests`
Expected: PASS (4 теста). Если контейнер PgBouncer не стартует — посмотреть `docker logs` контейнера `edoburu/pgbouncer`: чаще всего причина в формате `userlist.txt` или правах на файлы; правка — в `PostgresFixture`, не в тестах.

- [x] **Step 5: Commit**

```bash
git add tests/PgBolt.Testing tests/PgBolt.Postgres.Tests
git commit -m "test: PostgreSQL + PgBouncer fixture and per-test schema"
```

### Task 5: Опции, имя схемы, UUIDv7

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/PgBoltOptions.cs`, `src/PgBolt.Postgres/SchemaName.cs`, `src/PgBolt.Postgres/UuidV7.cs`
- Test: `tests/PgBolt.Postgres.Tests/OptionsTests.cs`, `tests/PgBolt.Postgres.Tests/UuidV7Tests.cs`

**Interfaces:**
- Produces:
  - `public sealed class PgBoltOptions` (namespace `PgBolt.Postgres`): `NpgsqlDataSource? DataSource`, `string? ConnectionString`, `string Schema = "pgbolt"`, `bool AutoMigrate = true`, `bool UsePreparedStatements`, `int JobsFillFactor = 80`, `int PartitionDaysAhead = 7`, `TimeSpan PartitionLockTimeout = 2s`, `TimeSpan JobTypesCacheTtl = 30s`, `PgBoltOptions AddJobs(Action<IPgBoltJobsBuilder> register)`, `internal IReadOnlyList<Action<IPgBoltJobsBuilder>> JobRegistrations`, `internal void Validate()`
  - `internal static class SchemaName { string Validate(string schema); }`
  - `internal static class UuidV7 { Guid LowerBound(DateOnly day); DateTimeOffset GetTimestamp(Guid id); DateTimeOffset EventsLowerBound(Guid id); }`

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/OptionsTests.cs`:

```csharp
namespace PgBolt.Postgres.Tests;

public sealed class OptionsTests
{
    [Theory]
    [InlineData("pgbolt")]
    [InlineData("_x1")]
    [InlineData("a23456789012345678901234567890123456789012345678901234567890123")]
    public void Valid_schema_names_pass(string name) => Assert.Equal(name, SchemaName.Validate(name));

    [Theory]
    [InlineData("")]
    [InlineData("Upper")]
    [InlineData("1abc")]
    [InlineData("a-b")]
    [InlineData("a;drop")]
    [InlineData("a234567890123456789012345678901234567890123456789012345678901234")]
    public void Invalid_schema_names_throw(string name) =>
        Assert.Throws<ArgumentException>(() => SchemaName.Validate(name));

    [Fact]
    public void Defaults_match_spec()
    {
        var o = new PgBoltOptions();
        Assert.Equal("pgbolt", o.Schema);
        Assert.True(o.AutoMigrate);
        Assert.False(o.UsePreparedStatements);
        Assert.Equal(80, o.JobsFillFactor);
        Assert.Equal(7, o.PartitionDaysAhead);
        Assert.Equal(TimeSpan.FromSeconds(2), o.PartitionLockTimeout);
    }

    [Fact]
    public void Validate_requires_exactly_one_connection_source()
    {
        Assert.Throws<InvalidOperationException>(() => new PgBoltOptions().Validate());
        new PgBoltOptions { ConnectionString = "Host=x" }.Validate();
    }

    [Theory]
    [InlineData(9)]
    [InlineData(101)]
    public void Validate_rejects_fillfactor_out_of_range(int ff) =>
        Assert.Throws<InvalidOperationException>(() => new PgBoltOptions { ConnectionString = "Host=x", JobsFillFactor = ff }.Validate());

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_rejects_non_positive_job_types_cache_ttl(int seconds) =>
        Assert.Throws<InvalidOperationException>(() => new PgBoltOptions { ConnectionString = "Host=x", JobTypesCacheTtl = TimeSpan.FromSeconds(seconds) }.Validate());

    [Fact]
    public void AddJobs_collects_registrations()
    {
        var o = new PgBoltOptions();
        o.AddJobs(_ => { }).AddJobs(_ => { });
        Assert.Equal(2, o.JobRegistrations.Count);
    }
}
```

`tests/PgBolt.Postgres.Tests/UuidV7Tests.cs`:

```csharp
namespace PgBolt.Postgres.Tests;

public sealed class UuidV7Tests
{
    private static byte[] Be(Guid g) => g.ToByteArray(bigEndian: true);

    [Fact]
    public void GetTimestamp_roundtrips_CreateVersion7()
    {
        var ts = new DateTimeOffset(2026, 9, 27, 13, 45, 12, 345, TimeSpan.Zero);
        Assert.Equal(ts, UuidV7.GetTimestamp(Guid.CreateVersion7(ts)));
    }

    [Fact]
    public void LowerBound_brackets_ids_of_the_day_in_postgres_byte_order()
    {
        var day = new DateOnly(2026, 9, 27);
        var lo = Be(UuidV7.LowerBound(day));
        var hi = Be(UuidV7.LowerBound(day.AddDays(1)));
        foreach (var hour in new[] { 0, 12, 23 })
        {
            var id = Be(Guid.CreateVersion7(new DateTimeOffset(2026, 9, 27, hour, 59, 59, 999, TimeSpan.Zero)));
            Assert.True(lo.AsSpan().SequenceCompareTo(id) <= 0);
            Assert.True(id.AsSpan().SequenceCompareTo(hi) < 0);
        }
    }

    [Fact]
    public void LowerBound_is_timestamp_prefix_with_zero_tail()
    {
        var g = UuidV7.LowerBound(new DateOnly(1970, 1, 2));
        Assert.Equal("00000526-5c00-0000-0000-000000000000", g.ToString());
    }

    [Fact]
    public void EventsLowerBound_is_start_of_previous_utc_day()
    {
        var id = Guid.CreateVersion7(new DateTimeOffset(2026, 9, 27, 0, 30, 0, TimeSpan.Zero));
        Assert.Equal(new DateTimeOffset(2026, 9, 26, 0, 0, 0, TimeSpan.Zero), UuidV7.EventsLowerBound(id));
    }
}
```

(Проверка константы: 1970-01-02 = 86 400 000 мс = `0x0000_0526_5C00`.)

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~OptionsTests|FullyQualifiedName~UuidV7Tests"`
Expected: FAIL — ошибки компиляции.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/SchemaName.cs`:

```csharp
using System.Text.RegularExpressions;

namespace PgBolt.Postgres;

internal static partial class SchemaName
{
    [GeneratedRegex("^[a-z_][a-z0-9_]{0,62}$", RegexOptions.CultureInvariant)]
    private static partial Regex Pattern();

    public static string Validate(string schema) =>
        schema is not null && Pattern().IsMatch(schema)
            ? schema
            : throw new ArgumentException($"Invalid PgBolt schema name '{schema}'. Allowed: ^[a-z_][a-z0-9_]{{0,62}}$.", nameof(schema));
}
```

`src/PgBolt.Postgres/UuidV7.cs`:

```csharp
namespace PgBolt.Postgres;

/// <summary>Работа с 48-битным unix-ms префиксом UUIDv7 в порядке байтов PostgreSQL (big-endian).</summary>
internal static class UuidV7
{
    public static Guid LowerBound(DateOnly day)
    {
        var ms = new DateTimeOffset(day.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero).ToUnixTimeMilliseconds();
        Span<byte> bytes = stackalloc byte[16];
        bytes.Clear();
        for (var i = 0; i < 6; i++)
            bytes[i] = (byte)(ms >> (8 * (5 - i)));
        return new Guid(bytes, bigEndian: true);
    }

    public static DateTimeOffset GetTimestamp(Guid id)
    {
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        long ms = 0;
        for (var i = 0; i < 6; i++)
            ms = (ms << 8) | bytes[i];
        return DateTimeOffset.FromUnixTimeMilliseconds(ms);
    }

    /// <summary>Нижняя граница поиска job_events (§4.3): начало суток UTC минус одни сутки.</summary>
    public static DateTimeOffset EventsLowerBound(Guid id) =>
        new DateTimeOffset(GetTimestamp(id).UtcDateTime.Date, TimeSpan.Zero).AddDays(-1);
}
```

`src/PgBolt.Postgres/PgBoltOptions.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres;

public sealed class PgBoltOptions
{
    private readonly List<Action<IPgBoltJobsBuilder>> _jobRegistrations = [];

    public NpgsqlDataSource? DataSource { get; set; }
    public string? ConnectionString { get; set; }
    public string Schema { get; set; } = "pgbolt";
    public bool AutoMigrate { get; set; } = true;
    public bool UsePreparedStatements { get; set; }
    /// <summary>fillfactor партиций jobs (контракт HOT, §4.2).</summary>
    public int JobsFillFactor { get; set; } = 80;
    public int PartitionDaysAhead { get; set; } = 7;
    public TimeSpan PartitionLockTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan JobTypesCacheTtl { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<Action<IPgBoltJobsBuilder>> JobRegistrations => _jobRegistrations;

    /// <summary>Регистрирует исполнителей сборки: o.AddJobs(My.Jobs.PgBoltJobRegistry.Register).</summary>
    public PgBoltOptions AddJobs(Action<IPgBoltJobsBuilder> register)
    {
        ArgumentNullException.ThrowIfNull(register);
        _jobRegistrations.Add(register);
        return this;
    }

    internal void Validate()
    {
        if ((DataSource is null) == string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException("PgBolt: set exactly one of DataSource or ConnectionString.");
        SchemaName.Validate(Schema);
        if (JobsFillFactor is < 10 or > 100)
            throw new InvalidOperationException("PgBolt: JobsFillFactor must be in [10, 100].");
        if (PartitionDaysAhead < 1)
            throw new InvalidOperationException("PgBolt: PartitionDaysAhead must be >= 1.");
        if (PartitionLockTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException("PgBolt: PartitionLockTimeout must be positive.");
        if (JobTypesCacheTtl <= TimeSpan.Zero)
            throw new InvalidOperationException("PgBolt: JobTypesCacheTtl must be positive.");
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~OptionsTests|FullyQualifiedName~UuidV7Tests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres tests/PgBolt.Postgres.Tests
git commit -m "feat(postgres): options, schema name validation, UUIDv7 helpers"
```

### Task 6: Миграции

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql`, `src/PgBolt.Postgres/Schema/PgBoltMigrations.cs`, `src/PgBolt.Postgres/Schema/MigrationRunner.cs`
- Modify: `tests/PgBolt.Testing/TestDb.cs` (метод `CreateAsync`)
- Test: `tests/PgBolt.Postgres.Tests/MigrationTests.cs`

**Interfaces:**
- Consumes: `SchemaName` (Task 5).
- Produces (namespace `PgBolt.Postgres.Schema`):
  - `public static class PgBoltMigrations { IReadOnlyList<Migration> All; int LatestVersion; string GetScript(string schema, int fromVersion = 0); }`, `public sealed record Migration(int Version, string Name, string Sql)`
  - `public sealed class MigrationRunner(NpgsqlDataSource dataSource, string schema) { Task<int> MigrateAsync(CancellationToken ct = default); Task<int?> GetCurrentVersionAsync(CancellationToken ct = default); }`
  - `internal const string LockKeySql = "SELECT pg_advisory_xact_lock(hashtext(@lock_key))"` в `PgBoltMigrations` — общий с Task 7.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/MigrationTests.cs`:

```csharp
using System.Text.RegularExpressions;
using PgBolt.Postgres.Schema;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class MigrationTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private MigrationRunner Runner() => new(_db.DataSource, _db.Schema);

    [Fact]
    public async Task Migrate_creates_all_tables_once()
    {
        Assert.Equal(PgBoltMigrations.All.Count, await Runner().MigrateAsync(Ct));
        Assert.Equal(0, await Runner().MigrateAsync(Ct));
        foreach (var table in new[] { "queue", "jobs", "job_events", "job_types", "servers", "schema_version" })
            Assert.True(await _db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{_db.Schema}.{table}")), table);
        Assert.Equal(PgBoltMigrations.LatestVersion, await Runner().GetCurrentVersionAsync(Ct));
    }

    [Fact]
    public async Task Parallel_migrations_apply_each_version_exactly_once()
    {
        var applied = await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => Runner().MigrateAsync(Ct)));
        Assert.Equal(PgBoltMigrations.All.Count, applied.Sum());
        Assert.Equal((long)PgBoltMigrations.All.Count, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.schema_version"));
    }

    [Fact]
    public async Task Version_is_null_before_migration()
    {
        Assert.Null(await Runner().GetCurrentVersionAsync(Ct));
    }

    [Fact]
    public async Task Parents_are_partitioned_and_have_no_partitions()
    {
        await Runner().MigrateAsync(Ct);
        Assert.Equal("p", await _db.ScalarAsync<string>("SELECT relkind::text FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
        Assert.Equal("p", await _db.ScalarAsync<string>("SELECT relkind::text FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.job_events")));
        Assert.Equal(0L, await _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
    }

    [Fact]
    public async Task Hot_contract_no_jobs_index_uses_mutable_columns()
    {
        await Runner().MigrateAsync(Ct);
        await using var cmd = _db.Admin.CreateCommand("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = to_regclass(@t)");
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.jobs");
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var mutable = new Regex(@"\b(state|attempt|started_at|finished_at|server_id)\b");
        var count = 0;
        while (await r.ReadAsync(Ct))
        {
            count++;
            Assert.DoesNotMatch(mutable, r.GetString(0));
        }
        Assert.True(count >= 1); // PK
    }

    [Fact]
    public async Task Queue_indexes_match_spec_predicates()
    {
        await Runner().MigrateAsync(Ct);
        var fetch = await _db.ScalarAsync<string>("SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.queue_fetch"));
        Assert.Contains("(queue, priority DESC, run_at)", fetch);
        Assert.Contains("WHERE ((lease_owner IS NULL) AND ready)", fetch);
        var delayed = await _db.ScalarAsync<string>("SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.queue_delayed"));
        Assert.Contains("WHERE (NOT ready)", delayed);
    }

    [Fact]
    public void Exported_script_substitutes_schema()
    {
        var script = PgBoltMigrations.GetScript("my_schema");
        Assert.DoesNotContain("{schema}", script);
        Assert.Contains("CREATE TABLE my_schema.queue", script);
        Assert.Contains("INSERT INTO my_schema.schema_version", script);
        Assert.StartsWith("BEGIN;", script);
        Assert.EndsWith("COMMIT;\n", script);
    }
}

public sealed class MigrationTests_Direct(PostgresFixture pg) : MigrationTests(pg, ConnectionMode.Direct);
public sealed class MigrationTests_Bouncer(PostgresFixture pg) : MigrationTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Добавить параметр `migrate` в `TestDb.CreateAsync`**

Заменить метод `CreateAsync` в `tests/PgBolt.Testing/TestDb.cs`:

```csharp
    public static async Task<TestDb> CreateAsync(PostgresFixture pg, ConnectionMode mode, bool migrate = true)
    {
        var schema = "t_" + Guid.NewGuid().ToString("N")[..16];
        var cs = pg.ConnectionString(mode);
        var db = new TestDb(schema, mode, cs, NpgsqlDataSource.Create(cs), NpgsqlDataSource.Create(pg.DirectConnectionString));
        if (migrate)
            await new PgBolt.Postgres.Schema.MigrationRunner(db.Admin, schema).MigrateAsync();
        else
            await db.ExecAsync("CREATE SCHEMA {schema}");
        return db;
    }
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests"`
Expected: FAIL — ошибки компиляции (`MigrationRunner` не найден).

- [x] **Step 4: Написать миграцию V001**

`src/PgBolt.Postgres/Schema/Migrations/V001__initial.sql` (DDL из §4.1–§4.6; партиции не создаются — §4.4):

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
    cancel_requested boolean     NOT NULL DEFAULT false
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 5000,
        autovacuum_vacuum_cost_limit = 2000);

CREATE INDEX queue_fetch   ON {schema}.queue (queue, priority DESC, run_at) WHERE lease_owner IS NULL AND ready;
CREATE INDEX queue_delayed ON {schema}.queue (run_at)                       WHERE NOT ready;
CREATE INDEX queue_owner   ON {schema}.queue (lease_owner)                  WHERE lease_owner IS NOT NULL;
CREATE INDEX queue_expiry  ON {schema}.queue (lease_until)                  WHERE lease_owner IS NOT NULL;
CREATE UNIQUE INDEX queue_unique ON {schema}.queue (unique_key)             WHERE unique_key IS NOT NULL;

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
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);

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
```

- [x] **Step 5: Реализовать загрузку и экспорт миграций**

`src/PgBolt.Postgres/Schema/PgBoltMigrations.cs`:

```csharp
using System.Text;
using System.Text.RegularExpressions;

namespace PgBolt.Postgres.Schema;

public sealed record Migration(int Version, string Name, string Sql);

public static partial class PgBoltMigrations
{
    internal const string LockKeySql = "SELECT pg_advisory_xact_lock(hashtext(@lock_key))";

    internal const string BootstrapSql = """
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.schema_version (version int PRIMARY KEY, applied_at timestamptz NOT NULL);
        """;

    [GeneratedRegex(@"\.V(\d{3})__([a-z0-9_]+)\.sql$")]
    private static partial Regex ResourceName();

    public static IReadOnlyList<Migration> All { get; } = Load();

    public static int LatestVersion => All[^1].Version;

    /// <summary>Скрипт для ручного применения (AutoMigrate = false): миграции с версией больше fromVersion.</summary>
    public static string GetScript(string schema, int fromVersion = 0)
    {
        SchemaName.Validate(schema);
        var sb = new StringBuilder();
        sb.Append("BEGIN;\n");
        sb.Append($"SELECT pg_advisory_xact_lock(hashtext('{schema}'));\n");
        sb.Append(Render(BootstrapSql, schema)).Append('\n');
        foreach (var m in All.Where(m => m.Version > fromVersion))
        {
            sb.Append($"-- V{m.Version:D3} {m.Name}\n");
            sb.Append(Render(m.Sql, schema)).Append('\n');
            sb.Append($"INSERT INTO {schema}.schema_version (version, applied_at) VALUES ({m.Version}, now());\n");
        }
        sb.Append("COMMIT;\n");
        return sb.ToString();
    }

    internal static string Render(string sql, string schema) => sql.Replace("{schema}", schema, StringComparison.Ordinal);

    private static List<Migration> Load()
    {
        var assembly = typeof(PgBoltMigrations).Assembly;
        var list = new List<Migration>();
        foreach (var name in assembly.GetManifestResourceNames())
        {
            var match = ResourceName().Match(name);
            if (!match.Success)
                continue;
            using var stream = assembly.GetManifestResourceStream(name)!;
            using var reader = new StreamReader(stream);
            list.Add(new Migration(int.Parse(match.Groups[1].Value), match.Groups[2].Value, reader.ReadToEnd()));
        }
        list.Sort((a, b) => a.Version.CompareTo(b.Version));
        if (list.Count == 0 || list.Select((m, i) => m.Version != i + 1).Any(x => x))
            throw new InvalidOperationException("PgBolt migrations must be numbered V001, V002, ... without gaps.");
        return list;
    }
}
```

- [x] **Step 6: Реализовать `MigrationRunner`**

`src/PgBolt.Postgres/Schema/MigrationRunner.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres.Schema;

public sealed class MigrationRunner(NpgsqlDataSource dataSource, string schema)
{
    private readonly string _schema = SchemaName.Validate(schema);

    /// <summary>Применяет недостающие миграции в одной транзакции под advisory xact lock. Возвращает число применённых.</summary>
    public async Task<int> MigrateAsync(CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await ExecAsync(conn, tx, PgBoltMigrations.LockKeySql, ct, ("lock_key", _schema));
        await ExecAsync(conn, tx, PgBoltMigrations.Render(PgBoltMigrations.BootstrapSql, _schema), ct);

        var applied = new HashSet<int>();
        await using (var cmd = new NpgsqlCommand(PgBoltMigrations.Render("SELECT version FROM {schema}.schema_version", _schema), conn, tx))
        await using (var r = await cmd.ExecuteReaderAsync(ct))
            while (await r.ReadAsync(ct))
                applied.Add(r.GetInt32(0));

        var count = 0;
        foreach (var m in PgBoltMigrations.All.Where(m => !applied.Contains(m.Version)))
        {
            try
            {
                await ExecAsync(conn, tx, PgBoltMigrations.Render(m.Sql, _schema), ct);
            }
            catch (PostgresException e)
            {
                throw new InvalidOperationException($"PgBolt migration V{m.Version:D3} ({m.Name}) failed in schema '{_schema}': {e.MessageText}", e);
            }
            await ExecAsync(conn, tx, PgBoltMigrations.Render("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (@v, now())", _schema), ct, ("v", m.Version));
            count++;
        }

        await tx.CommitAsync(ct);
        return count;
    }

    public async Task<int?> GetCurrentVersionAsync(CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using (var check = new NpgsqlCommand("SELECT to_regclass(@t) IS NOT NULL", conn))
        {
            check.Parameters.AddWithValue("t", $"{_schema}.schema_version");
            if (!(bool)(await check.ExecuteScalarAsync(ct))!)
                return null;
        }
        await using var cmd = new NpgsqlCommand(PgBoltMigrations.Render("SELECT max(version) FROM {schema}.schema_version", _schema), conn);
        return await cmd.ExecuteScalarAsync(ct) is int v ? v : null;
    }

    private static async Task ExecAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, CancellationToken ct, params (string Name, object Value)[] ps)
    {
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

`AddWithValue` здесь допустим: код не на горячем пути, типы параметров (`string`, `int`) однозначны и не требуют reflection.

- [x] **Step 7: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~MigrationTests|FullyQualifiedName~InfrastructureTests"`
Expected: PASS (18 тестов: 7 × 2 MigrationTests + 2 × 2 InfrastructureTests).

- [x] **Step 8: Commit**

```bash
git add src/PgBolt.Postgres tests/PgBolt.Testing tests/PgBolt.Postgres.Tests
git commit -m "feat(postgres): versioned migrations under advisory xact lock, script export"
```

### Task 7: Идемпотентное создание партиций (`EnsurePartitionsAsync`)

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Schema/PartitionManager.cs`, `src/PgBolt.Postgres/Schema/PartitionEnsureResult.cs`
- Modify: `tests/PgBolt.Testing/TestDb.cs` (метод `CreateAsync` — финальная версия)
- Test: `tests/PgBolt.Postgres.Tests/PartitionTests.cs`

**Interfaces:**
- Consumes: `PgBoltMigrations.LockKeySql` (Task 6), `UuidV7.LowerBound` (Task 5).
- Produces:
  - `public sealed record PartitionEnsureResult(int Created, bool TimedOut, bool TodayPresent)`
  - `public sealed class PartitionManager(NpgsqlDataSource dataSource, string schema, int jobsFillFactor, TimeSpan lockTimeout) { Task<PartitionEnsureResult> EnsureAsync(int daysAhead, CancellationToken ct = default); }` — создаёт партиции на сутки UTC сервера БД `today .. today + daysAhead` включительно.
  - `TestDb.CreateAsync(PostgresFixture pg, ConnectionMode mode, bool migrate = true, int partitionDaysAhead = 2)`; `partitionDaysAhead < 0` — партиции не создаются.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/PartitionTests.cs`:

```csharp
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class PartitionTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, partitionDaysAhead: -1);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PartitionManager Manager(TimeSpan? lockTimeout = null) =>
        new(_db.DataSource, _db.Schema, 80, lockTimeout ?? TimeSpan.FromSeconds(2));

    private Task<DateOnly> TodayAsync() => _db.ScalarAsync<DateOnly>("SELECT (now() AT TIME ZONE 'UTC')::date");

    private Task<long> PartitionCountAsync(string parent) =>
        _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.{parent}"));

    [Fact]
    public async Task Creates_partitions_for_today_and_ahead_idempotently()
    {
        var first = await Manager().EnsureAsync(3, Ct);
        Assert.Equal(new PartitionEnsureResult(8, false, true), first);
        Assert.Equal(4L, await PartitionCountAsync("jobs"));
        Assert.Equal(4L, await PartitionCountAsync("job_events"));
        Assert.Equal(new PartitionEnsureResult(0, false, true), await Manager().EnsureAsync(3, Ct));
    }

    [Fact]
    public async Task Parallel_calls_are_safe()
    {
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Manager().EnsureAsync(2, Ct)));
        Assert.Equal(3L, await PartitionCountAsync("jobs"));
    }

    [Fact]
    public async Task Jobs_partitions_have_fillfactor_and_events_do_not()
    {
        await Manager().EnsureAsync(1, Ct); // запас в день: полночь UTC между EnsureAsync и TodayAsync не ломает тест
        var today = await TodayAsync();
        var jobsOptions = await _db.ScalarAsync<string[]>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.jobs_{today:yyyyMMdd}"));
        Assert.Contains("fillfactor=80", jobsOptions);
        var eventsOptions = await _db.ScalarAsync<string[]?>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.job_events_{today:yyyyMMdd}"));
        Assert.Null(eventsOptions);
    }

    [Fact]
    public async Task Row_with_uuid_of_future_day_is_accepted_after_ensure()
    {
        await Manager().EnsureAsync(9, Ct); // запас в день на случай полуночи UTC между EnsureAsync и TodayAsync
        var today = await TodayAsync();
        var id = Guid.CreateVersion7(new DateTimeOffset(today.AddDays(8).ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero));
        await _db.ExecAsync("""
            INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at)
            VALUES (@id, 't', 'q', 0, '{}', 0, 1, now())
            """, ("id", id));
        Assert.Equal(1L, await _db.ScalarAsync<long>($"SELECT count(*) FROM {_db.Schema}.jobs_{today.AddDays(8):yyyyMMdd}"));
    }

    [Fact]
    public async Task Long_reader_on_jobs_blocks_neither_attach_nor_inserts()
    {
        await Manager().EnsureAsync(0, Ct);
        await using var reader = await _db.Admin.OpenConnectionAsync(Ct);
        await using var tx = await reader.BeginTransactionAsync(Ct);
        await using (var cmd = new NpgsqlCommand(_db.Sql("SELECT count(*) FROM {schema}.jobs"), reader, tx))
            await cmd.ExecuteScalarAsync(Ct);

        var result = await Manager().EnsureAsync(4, Ct).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.False(result.TimedOut);
        Assert.Equal(5L, await PartitionCountAsync("jobs"));

        // Вставка в jobs, как у enqueue (§5.1), при всё ещё открытом читателе: в сегодняшнюю и в только что присоединённую партицию.
        var today = await TodayAsync();
        var ids = new[] { Guid.CreateVersion7(), Guid.CreateVersion7(new DateTimeOffset(today.AddDays(4).ToDateTime(new TimeOnly(12, 0)), TimeSpan.Zero)) };
        foreach (var id in ids)
            await _db.ExecAsync("""
                INSERT INTO {schema}.jobs (id, type, queue, priority, args, state, max_attempts, created_at)
                VALUES (@id, 't', 'q', 0, '{}', 0, 1, now())
                """, ("id", id)).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.Equal(2L, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Lock_timeout_reports_timed_out_with_today_present()
    {
        await Manager().EnsureAsync(0, Ct);
        await using var holder = await HoldShareUpdateExclusiveAsync();
        var result = await Manager(TimeSpan.FromMilliseconds(200)).EnsureAsync(3, Ct);
        Assert.True(result.TimedOut);
        Assert.True(result.TodayPresent);
    }

    [Fact]
    public async Task Lock_timeout_reports_today_missing()
    {
        await using var holder = await HoldShareUpdateExclusiveAsync();
        var result = await Manager(TimeSpan.FromMilliseconds(200)).EnsureAsync(1, Ct);
        Assert.Equal(new PartitionEnsureResult(0, true, false), result);
    }

    private async Task<NpgsqlConnection> HoldShareUpdateExclusiveAsync()
    {
        var conn = await _db.Admin.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(_db.Sql("BEGIN; LOCK TABLE {schema}.jobs IN SHARE UPDATE EXCLUSIVE MODE"), conn);
        await cmd.ExecuteNonQueryAsync();
        return conn; // Dispose закрывает соединение и откатывает транзакцию
    }
}

public sealed class PartitionTests_Direct(PostgresFixture pg) : PartitionTests(pg, ConnectionMode.Direct);
public sealed class PartitionTests_Bouncer(PostgresFixture pg) : PartitionTests(pg, ConnectionMode.Bouncer);
```

В `Long_reader_on_jobs_blocks_neither_attach_nor_inserts` запись идёт обычным `INSERT` в `jobs` с колонками statement'а enqueue, а не через `PgBoltStore.EnqueueAsync`: хранилище появляется только в Task 8–9, а для блокировок существенна именно вставка в партиционированный `jobs` (`ROW EXCLUSIVE` на родителе и партиции); `queue` не партиционирована и `ATTACH` её не затрагивает.

- [x] **Step 2: Финальная версия `TestDb.CreateAsync`**

Заменить метод `CreateAsync` в `tests/PgBolt.Testing/TestDb.cs`:

```csharp
    public static async Task<TestDb> CreateAsync(PostgresFixture pg, ConnectionMode mode, bool migrate = true, int partitionDaysAhead = 2)
    {
        var schema = "t_" + Guid.NewGuid().ToString("N")[..16];
        var cs = pg.ConnectionString(mode);
        var db = new TestDb(schema, mode, cs, NpgsqlDataSource.Create(cs), NpgsqlDataSource.Create(pg.DirectConnectionString));
        if (!migrate)
        {
            await db.ExecAsync("CREATE SCHEMA {schema}");
            return db;
        }
        await new PgBolt.Postgres.Schema.MigrationRunner(db.Admin, schema).MigrateAsync();
        if (partitionDaysAhead >= 0)
            await new PgBolt.Postgres.Schema.PartitionManager(db.Admin, schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(partitionDaysAhead);
        return db;
    }
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~PartitionTests"`
Expected: FAIL — ошибки компиляции.

- [x] **Step 4: Реализовать**

`src/PgBolt.Postgres/Schema/PartitionEnsureResult.cs`:

```csharp
namespace PgBolt.Postgres.Schema;

/// <param name="Created">Сколько партиций создано (jobs + job_events).</param>
/// <param name="TimedOut">Не удалось взять блокировку за lock_timeout — повторить позже (§4.4).</param>
/// <param name="TodayPresent">Партиции jobs и job_events на текущие сутки UTC сервера существуют.</param>
public sealed record PartitionEnsureResult(int Created, bool TimedOut, bool TodayPresent);
```

`src/PgBolt.Postgres/Schema/PartitionManager.cs`:

```csharp
using System.Globalization;
using Npgsql;

namespace PgBolt.Postgres.Schema;

/// <summary>Создание суточных партиций jobs/job_events через CREATE TABLE (LIKE) + ATTACH (§4.4).</summary>
public sealed class PartitionManager(NpgsqlDataSource dataSource, string schema, int jobsFillFactor, TimeSpan lockTimeout)
{
    private readonly string _schema = SchemaName.Validate(schema);

    public async Task<PartitionEnsureResult> EnsureAsync(int daysAhead, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(daysAhead);
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            var ms = (int)Math.Ceiling(lockTimeout.TotalMilliseconds);
            // Сначала advisory lock, затем lock_timeout: ожидание параллельного EnsureAsync/миграции (§4.4) не таймаутится.
            await ExecAsync(conn, tx, PgBoltMigrations.LockKeySql, ct, ("lock_key", _schema));
            await ExecAsync(conn, tx, $"SET LOCAL lock_timeout = '{ms}ms'", ct);

            var today = await TodayAsync(conn, tx, ct);
            var existing = await ExistingAsync(conn, tx, ct);
            var created = 0;
            for (var day = today; day <= today.AddDays(daysAhead); day = day.AddDays(1))
            {
                var suffix = day.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
                if (!existing.Contains($"jobs_{suffix}"))
                {
                    await ExecAsync(conn, tx, JobsDdl(day, suffix), ct);
                    created++;
                }
                if (!existing.Contains($"job_events_{suffix}"))
                {
                    await ExecAsync(conn, tx, EventsDdl(day, suffix), ct);
                    created++;
                }
            }
            await tx.CommitAsync(ct);
            return new PartitionEnsureResult(created, TimedOut: false, TodayPresent: true);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await tx.RollbackAsync(ct);
            return new PartitionEnsureResult(0, TimedOut: true, TodayPresent: await TodayPresentAsync(ct));
        }
    }

    private string JobsDdl(DateOnly day, string suffix) => $"""
        CREATE TABLE {_schema}.jobs_{suffix} (LIKE {_schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
            WITH (fillfactor = {jobsFillFactor});
        ALTER TABLE {_schema}.jobs ATTACH PARTITION {_schema}.jobs_{suffix}
            FOR VALUES FROM ('{UuidV7.LowerBound(day)}') TO ('{UuidV7.LowerBound(day.AddDays(1))}');
        """;

    private string EventsDdl(DateOnly day, string suffix) => $"""
        CREATE TABLE {_schema}.job_events_{suffix} (LIKE {_schema}.job_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
        ALTER TABLE {_schema}.job_events ATTACH PARTITION {_schema}.job_events_{suffix}
            FOR VALUES FROM ('{Iso(day)} 00:00:00+00') TO ('{Iso(day.AddDays(1))} 00:00:00+00');
        """;

    private static string Iso(DateOnly day) => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private async Task<bool> TodayPresentAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var today = await TodayAsync(conn, null, ct);
        var existing = await ExistingAsync(conn, null, ct);
        var suffix = today.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        return existing.Contains($"jobs_{suffix}") && existing.Contains($"job_events_{suffix}");
    }

    private static async Task<DateOnly> TodayAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT (now() AT TIME ZONE 'UTC')::date", conn, tx);
        return (DateOnly)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private async Task<HashSet<string>> ExistingAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct)
    {
        const string sql = """
            SELECT c.relname
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = p.relnamespace
            WHERE n.nspname = @schema AND p.relname IN ('jobs', 'job_events')
            """;
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        cmd.Parameters.AddWithValue("schema", _schema);
        var set = new HashSet<string>(StringComparer.Ordinal);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            set.Add(r.GetString(0));
        return set;
    }

    private static async Task ExecAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, CancellationToken ct, params (string Name, object Value)[] ps)
    {
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

- [x] **Step 5: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests`
Expected: PASS (все тесты фаз 2). `Lock_timeout_*` завершаются менее чем за секунду.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres tests/PgBolt.Testing tests/PgBolt.Postgres.Tests
git commit -m "feat(postgres): idempotent daily partitions via LIKE + ATTACH under lock_timeout"
```

---
## Фаза 3. Горячий путь (`PgBoltStore`)

Все операции — методы `public sealed partial class PgBoltStore` (namespace `PgBolt.Postgres.Storage`), по одному partial-файлу на операцию. SQL-шаблоны — `const string` с `{schema}`, рендерятся лениво через C# 14 `field`: `private string XSql => field ??= Render(XTemplate);`. SQL берётся из спеки дословно; отличия от спеки перечислены в задаче явно (это только добавленные `RETURNING`/итоговые `SELECT`, нужные, чтобы метод вернул результат).

### Task 8: Каркас `PgBoltStore`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/StoreRecords.cs`, `P.cs`, `DbRetry.cs`, `ReaderExtensions.cs`, `PgBoltStore.cs`
- Create: `tests/PgBolt.Postgres.Tests/StoreTestBase.cs`
- Test: `tests/PgBolt.Postgres.Tests/DbRetryTests.cs`

**Interfaces:**
- Produces (namespace `PgBolt.Postgres.Storage`):
  - `public sealed record EnqueueBatch(string Type, string Queue, short Priority, int MaxAttempts, int[]? RetryDelaysSeconds, string? TraceParent, TimeSpan Delay, Guid[] Ids, string[] ArgsJson, DateTime?[] RunAtUtc, string?[] UniqueKeys)` с `internal void Validate()`
  - `public readonly record struct EnqueueRowResult(Guid? Id, bool Created)`
  - `public sealed record LeasedJob(Guid Id, Guid LeaseId, string Type, string ArgsJson, int Attempt, int MaxAttempts, int[]? RetryDelaysSeconds, Guid? ParentId, Guid? BatchId, string? TraceParent)`
  - `public readonly record struct LeaseRef(Guid JobId, Guid LeaseId)`
  - `public readonly record struct HeartbeatLease(Guid JobId, Guid LeaseId, bool CancelRequested)`
  - `public enum FailOutcome { LeaseLost, Retried, Cancelled, Failed }`
  - `public sealed record PromoteResult(int Promoted, IReadOnlyCollection<string> Queues)`
  - `public sealed record JobTypeRecord(string Name, string ArgsSchemaJson, string DefaultQueue, int DefaultMaxAttempts, short DefaultPriority, int[]? DefaultRetryDelaysSeconds)`
  - `public PgBoltStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements)`; `internal PgBoltStore(..., bool retryOnConflict)`; `public string Schema`
  - private-хелперы: `Render`, `PrepareAsync`, `RunAsync<T>`, `QueryAsync<T>`, `NonQueryAsync`, константа `MaxStatementAttempts = 3`
  - `internal static class DbRetry { Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> op, CancellationToken ct); bool IsRetryable(PostgresException e); }`
  - Тестовая база `StoreTestBase` (см. ниже) — используется всеми задачами фазы 3.

- [x] **Step 1: Написать падающий тест `DbRetry`**

`tests/PgBolt.Postgres.Tests/DbRetryTests.cs`:

```csharp
using Npgsql;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Tests;

public sealed class DbRetryTests
{
    private static PostgresException Pg(string sqlState) => new("boom", "ERROR", "ERROR", sqlState);

    [Theory]
    [InlineData(PostgresErrorCodes.DeadlockDetected)]
    [InlineData(PostgresErrorCodes.SerializationFailure)]
    public async Task Retries_deadlock_and_serialization_failure(string state)
    {
        var calls = 0;
        var result = await DbRetry.RunAsync(_ => ++calls < 3 ? throw Pg(state) : Task.FromResult(42), CancellationToken.None);
        Assert.Equal(42, result);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Gives_up_after_three_attempts()
    {
        var calls = 0;
        await Assert.ThrowsAsync<PostgresException>(() =>
            DbRetry.RunAsync<int>(_ => { calls++; throw Pg(PostgresErrorCodes.DeadlockDetected); }, CancellationToken.None));
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Does_not_retry_other_errors()
    {
        var calls = 0;
        await Assert.ThrowsAsync<PostgresException>(() =>
            DbRetry.RunAsync<int>(_ => { calls++; throw Pg(PostgresErrorCodes.UniqueViolation); }, CancellationToken.None));
        Assert.Equal(1, calls);
    }
}
```

- [x] **Step 2: Убедиться, что тест падает**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~DbRetryTests"`
Expected: FAIL — `DbRetry` не найден.

- [x] **Step 3: Реализовать записи и хелперы**

`src/PgBolt.Postgres/Storage/StoreRecords.cs`:

```csharp
namespace PgBolt.Postgres.Storage;

/// <summary>Пакет задач одного типа (§5.1): поэлементные массивы + общие скаляры.</summary>
public sealed record EnqueueBatch(
    string Type,
    string Queue,
    short Priority,
    int MaxAttempts,
    int[]? RetryDelaysSeconds,
    string? TraceParent,
    TimeSpan Delay,
    Guid[] Ids,
    string[] ArgsJson,
    DateTime?[] RunAtUtc,
    string?[] UniqueKeys)
{
    internal void Validate()
    {
        if (Ids.Length == 0)
            throw new ArgumentException("Batch is empty.");
        if (ArgsJson.Length != Ids.Length || RunAtUtc.Length != Ids.Length || UniqueKeys.Length != Ids.Length)
            throw new ArgumentException("Batch arrays must have equal length.");
        if (Delay < TimeSpan.Zero)
            throw new ArgumentException("Delay must be non-negative.");
        if (MaxAttempts < 1)
            throw new ArgumentException("MaxAttempts must be >= 1.");
        foreach (var runAt in RunAtUtc)
            if (runAt is { Kind: not DateTimeKind.Utc })
                throw new ArgumentException("RunAtUtc values must have DateTimeKind.Utc.");
    }
}

public readonly record struct EnqueueRowResult(Guid? Id, bool Created);

public sealed record LeasedJob(
    Guid Id,
    Guid LeaseId,
    string Type,
    string ArgsJson,
    int Attempt,
    int MaxAttempts,
    int[]? RetryDelaysSeconds,
    Guid? ParentId,
    Guid? BatchId,
    string? TraceParent);

public readonly record struct LeaseRef(Guid JobId, Guid LeaseId);

public readonly record struct HeartbeatLease(Guid JobId, Guid LeaseId, bool CancelRequested);

public enum FailOutcome { LeaseLost, Retried, Cancelled, Failed }

public sealed record PromoteResult(int Promoted, IReadOnlyCollection<string> Queues);

public sealed record JobTypeRecord(
    string Name,
    string ArgsSchemaJson,
    string DefaultQueue,
    int DefaultMaxAttempts,
    short DefaultPriority,
    int[]? DefaultRetryDelaysSeconds);
```

`src/PgBolt.Postgres/Storage/P.cs`:

```csharp
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

/// <summary>Типизированные параметры (NpgsqlParameter&lt;T&gt;) без boxing.</summary>
internal static class P
{
    public static void Add<T>(NpgsqlParameterCollection ps, string name, T value) =>
        ps.Add(new NpgsqlParameter<T>(name, value));

    public static void Add<T>(NpgsqlParameterCollection ps, string name, T value, NpgsqlDbType type) =>
        ps.Add(new NpgsqlParameter<T>(name, type) { TypedValue = value });

    public static void AddNullable<T>(NpgsqlParameterCollection ps, string name, T? value, NpgsqlDbType type) where T : class =>
        ps.Add(value is null
            ? new NpgsqlParameter(name, type) { Value = DBNull.Value }
            : new NpgsqlParameter<T>(name, type) { TypedValue = value });
}
```

`src/PgBolt.Postgres/Storage/DbRetry.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres.Storage;

/// <summary>Повтор statement'а при 40P01/40001 вне пользовательской транзакции (§9).</summary>
internal static class DbRetry
{
    public const int MaxAttempts = 3;

    public static bool IsRetryable(PostgresException e) =>
        e.SqlState is PostgresErrorCodes.DeadlockDetected or PostgresErrorCodes.SerializationFailure;

    public static async Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> op, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await op(ct);
            }
            catch (PostgresException e) when (attempt < MaxAttempts && IsRetryable(e))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(5 * attempt + Random.Shared.Next(10)), ct);
            }
        }
    }
}
```

`src/PgBolt.Postgres/Storage/ReaderExtensions.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres.Storage;

internal static class ReaderExtensions
{
    public static async Task<List<T>> ReadAllAsync<T>(this NpgsqlDataReader reader, Func<NpgsqlDataReader, T> map, CancellationToken ct)
    {
        var list = new List<T>();
        while (await reader.ReadAsync(ct))
            list.Add(map(reader));
        return list;
    }

    public static T? GetNullable<T>(this NpgsqlDataReader reader, int ordinal) where T : class =>
        reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);

    public static T? GetNullableValue<T>(this NpgsqlDataReader reader, int ordinal) where T : struct =>
        reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
}
```

`src/PgBolt.Postgres/Storage/PgBoltStore.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres.Storage;

/// <summary>SQL горячего пути (§5). Каждая операция — один statement; один partial-файл на операцию.</summary>
public sealed partial class PgBoltStore
{
    private const int MaxStatementAttempts = DbRetry.MaxAttempts;

    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _prepare;
    private readonly bool _retryOnConflict;

    public PgBoltStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements)
        : this(dataSource, schema, usePreparedStatements, retryOnConflict: true)
    {
    }

    /// <param name="retryOnConflict">false — только для тестов, проверяющих отсутствие deadlock.</param>
    internal PgBoltStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements, bool retryOnConflict)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        Schema = SchemaName.Validate(schema);
        _prepare = usePreparedStatements;
        _retryOnConflict = retryOnConflict;
    }

    public string Schema { get; }

    private string Render(string template) => template.Replace("{schema}", Schema, StringComparison.Ordinal);

    private async Task PrepareAsync(NpgsqlCommand cmd, CancellationToken ct)
    {
        if (_prepare)
            await cmd.PrepareAsync(ct);
    }

    private Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> op, CancellationToken ct) =>
        _retryOnConflict ? DbRetry.RunAsync(op, ct) : op(ct);

    private Task<T> QueryAsync<T>(string sql, Action<NpgsqlParameterCollection> bind,
        Func<NpgsqlDataReader, CancellationToken, Task<T>> read, CancellationToken ct) =>
        RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var cmd = new NpgsqlCommand(sql, conn);
            bind(cmd.Parameters);
            await PrepareAsync(cmd, c);
            await using var reader = await cmd.ExecuteReaderAsync(c);
            return await read(reader, c);
        }, ct);

    private Task<int> NonQueryAsync(string sql, Action<NpgsqlParameterCollection> bind, CancellationToken ct) =>
        RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var cmd = new NpgsqlCommand(sql, conn);
            bind(cmd.Parameters);
            await PrepareAsync(cmd, c);
            return await cmd.ExecuteNonQueryAsync(c);
        }, ct);
}
```

- [x] **Step 4: Написать тестовую базу для хранилища**

`tests/PgBolt.Postgres.Tests/StoreTestBase.cs` (хелперы `EnqueueAsync` и `LeaseOneAsync` добавляются в Task 9 и Task 11, когда появятся соответствующие методы хранилища):

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class StoreTestBase(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    protected static readonly Guid ServerA = Guid.Parse("00000000-0000-0000-0000-0000000000a1");
    protected static readonly Guid ServerB = Guid.Parse("00000000-0000-0000-0000-0000000000b2");
    protected static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    protected PostgresFixture Pg { get; } = pg;
    protected ConnectionMode Mode { get; } = mode;
    protected TestDb Db { get; private set; } = null!;
    protected PgBoltStore Store { get; private set; } = null!;
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    public virtual async ValueTask InitializeAsync()
    {
        Db = await TestDb.CreateAsync(Pg, Mode);
        Store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true);
    }

    public virtual async ValueTask DisposeAsync() => await Db.DisposeAsync();

    protected static EnqueueBatch Batch(Guid id, string queue = "q", short priority = 0, TimeSpan? delay = null,
        DateTime? runAtUtc = null, string? uniqueKey = null, int maxAttempts = 3, string args = """{"n":1}""",
        string type = "test.job", int[]? retryDelays = null, string? traceParent = null) =>
        new(type, queue, priority, maxAttempts, retryDelays, traceParent, delay ?? TimeSpan.Zero,
            [id], [args], [runAtUtc], [uniqueKey]);

    protected Task<short?> JobStateAsync(Guid id) =>
        Db.ScalarAsync<short?>("SELECT state FROM {schema}.jobs WHERE id = @id", ("id", id));

    protected Task<bool> InQueueAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.queue WHERE job_id = @id)", ("id", id));

    protected async Task<List<short>> EventKindsAsync(Guid id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql("SELECT kind FROM {schema}.job_events WHERE job_id = @id ORDER BY at, kind"));
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var list = new List<short>();
        while (await r.ReadAsync(Ct))
            list.Add(r.GetInt16(0));
        return list;
    }

    protected Task ExpireLeaseAsync(Guid id) =>
        Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() - interval '1 second' WHERE job_id = @id", ("id", id));
}
```

- [x] **Step 5: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~DbRetryTests"`
Expected: PASS (4 теста).

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(postgres): store skeleton, typed parameters, deadlock retry"
```

### Task 9: Enqueue — создание, время на сервере, `ready`, `unique_key`

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs`
- Modify: `tests/PgBolt.Postgres.Tests/StoreTestBase.cs` (добавить хелпер `EnqueueAsync`)
- Test: `tests/PgBolt.Postgres.Tests/EnqueueTests.cs`

**Interfaces:**
- Consumes: `EnqueueBatch`, `EnqueueRowResult`, `P`, `DbRetry` (Task 8).
- Produces: `public Task<IReadOnlyList<EnqueueRowResult>> EnqueueAsync(EnqueueBatch batch, NpgsqlTransaction? transaction, CancellationToken ct)` — результат в порядке `batch.Ids`.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/EnqueueTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class EnqueueTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Enqueue_creates_ready_queue_row_and_pending_job()
    {
        var id = Guid.CreateVersion7();
        var result = await Store.EnqueueAsync(Batch(id, queue: "emails", priority: 5, maxAttempts: 4,
            retryDelays: [1, 30], traceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, result);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT ready AND lease_owner IS NULL AND attempt = 0 AND max_attempts = 4 AND queue = 'emails' AND priority = 5
                   AND abs(extract(epoch FROM run_at - now())) < 5
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id)));
        Assert.Equal((short)0, await JobStateAsync(id));
        Assert.Equal(new[] { 1, 30 }, await Db.ScalarAsync<int[]>("SELECT retry_delays FROM {schema}.jobs WHERE id = @id", ("id", id)));
        Assert.Equal("""{"n": 1}""", await Db.ScalarAsync<string>("SELECT args::text FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    // Покрывает §11 «тест со сдвигом часов клиента»: в SQL уходит только интервал @delay, а run_at
    // считается от now() сервера, поэтому часы процесса не участвуют. Сдвинуть системные часы
    // процесса из теста нельзя, а TimeProvider клиентом для run_at не используется — отдельного теста нет.
    [Fact]
    public async Task Delay_is_counted_from_database_now_and_row_is_not_ready()
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, delay: TimeSpan.FromHours(1)), null, Ct);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT NOT ready AND abs(extract(epoch FROM run_at - (now() + interval '1 hour'))) < 5
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id)));
    }

    [Fact]
    public async Task Absolute_run_at_is_stored_as_is()
    {
        var past = Guid.CreateVersion7();
        var future = Guid.CreateVersion7();
        var pastAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var futureAt = DateTime.UtcNow.AddDays(1);
        await Store.EnqueueAsync(Batch(past, runAtUtc: pastAt), null, Ct);
        await Store.EnqueueAsync(Batch(future, runAtUtc: futureAt), null, Ct);

        Assert.True(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", past)));
        Assert.False(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", future)));
        Assert.Equal(pastAt, await Db.ScalarAsync<DateTime>("SELECT run_at FROM {schema}.queue WHERE job_id = @id", ("id", past)));
    }

    [Fact]
    public async Task Unique_key_conflict_returns_existing_active_job()
    {
        var first = Guid.CreateVersion7();
        var second = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(first, uniqueKey: "user:42"), null, Ct);
        var r = await Store.EnqueueAsync(Batch(second, uniqueKey: "user:42"), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(first, false) }, r);
        Assert.Null(await JobStateAsync(second));
    }

    [Fact]
    public async Task Unique_key_is_free_after_job_leaves_queue()
    {
        var first = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(first, uniqueKey: "k"), null, Ct);
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id", ("id", first));
        var second = Guid.CreateVersion7();
        Assert.Equal(new[] { new EnqueueRowResult(second, true) }, await Store.EnqueueAsync(Batch(second, uniqueKey: "k"), null, Ct));
    }

    [Fact]
    public async Task Duplicate_keys_inside_batch_resolve_to_first_inserted()
    {
        var a = Guid.CreateVersion7();
        var b = Guid.CreateVersion7();
        var batch = new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
            [a, b], ["{}", "{}"], [null, null], ["dup", "dup"]);
        var r = await Store.EnqueueAsync(batch, null, Ct);

        Assert.Single(r, x => x.Created);
        Assert.All(r, x => Assert.Equal(r.Single(y => y.Created).Id, x.Id));
    }

    [Fact]
    public async Task Batch_preserves_input_order()
    {
        var ids = Enumerable.Range(0, 100).Select(_ => Guid.CreateVersion7()).ToArray();
        var batch = new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
            ids, ids.Select(i => $$"""{"id":"{{i}}"}""").ToArray(), new DateTime?[100], new string?[100]);
        var r = await Store.EnqueueAsync(batch, null, Ct);

        Assert.Equal(ids.Select(i => new EnqueueRowResult(i, true)), r);
        Assert.Equal(100L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue"));
    }

    [Fact]
    public async Task Mismatched_arrays_are_rejected()
    {
        var batch = new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
            [Guid.CreateVersion7()], [], [null], [null]);
        await Assert.ThrowsAsync<ArgumentException>(() => Store.EnqueueAsync(batch, null, Ct));
    }
}

public sealed class EnqueueTests_Direct(PostgresFixture pg) : EnqueueTests(pg, ConnectionMode.Direct);
public sealed class EnqueueTests_Bouncer(PostgresFixture pg) : EnqueueTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~EnqueueTests"`
Expected: FAIL — `EnqueueAsync` не найден.

- [x] **Step 3: Реализовать enqueue**

`src/PgBolt.Postgres/Storage/PgBoltStore.Enqueue.cs` (SQL — §5.1 дословно):

```csharp
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string EnqueueTemplate = """
        WITH src AS (
            SELECT * FROM unnest(@ids::uuid[], @args::jsonb[], @run_at::timestamptz[], @unique_keys::text[])
                     AS t(id, args, run_at, unique_key)
        ),
        existing AS (
            SELECT id FROM {schema}.jobs WHERE id = ANY(@ids)
        ),
        input AS (
            SELECT * FROM src WHERE id NOT IN (SELECT id FROM existing)
        ),
        q AS (
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts, unique_key)
            SELECT id, @queue, @priority, r.run_at, r.run_at <= now(), @max_attempts, unique_key
            FROM input CROSS JOIN LATERAL (SELECT COALESCE(input.run_at, now() + @delay) AS run_at) r
            ORDER BY unique_key, id
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
        LEFT JOIN existing e ON e.id = s.id
        """;

    private const string ResolveTemplate = """
        SELECT s.id, j.id IS NOT NULL AS exists_by_id, q.job_id AS existing_id
        FROM unnest(@ids::uuid[], @unique_keys::text[]) AS s(id, unique_key)
        LEFT JOIN {schema}.jobs  j ON j.id = s.id AND j.id = ANY(@ids)
        LEFT JOIN {schema}.queue q ON q.unique_key = s.unique_key
        """;

    private string EnqueueSql => field ??= Render(EnqueueTemplate);
    private string ResolveSql => field ??= Render(ResolveTemplate);

    public async Task<IReadOnlyList<EnqueueRowResult>> EnqueueAsync(EnqueueBatch batch, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        batch.Validate();

        if (transaction is not null)
        {
            // Внутри пользовательской транзакции повторов нет (§9).
            var conn = transaction.Connection ?? throw new InvalidOperationException("The transaction is already completed.");
            return await EnqueueOnceAsync(conn, transaction, batch, ct);
        }

        var transientRetried = false;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var conn = await _dataSource.OpenConnectionAsync(ct);
                return await EnqueueOnceAsync(conn, null, batch, ct);
            }
            catch (PostgresException e) when (attempt < MaxStatementAttempts
                                              && (IsJobsPrimaryKeyViolation(e) || (_retryOnConflict && DbRetry.IsRetryable(e))))
            {
                // 23505 по PK jobs: исходная попытка закоммитилась между снимком existing и вставкой — повтор увидит её (§5.1).
            }
            catch (NpgsqlException e) when (e is not PostgresException && e.IsTransient && !transientRetried)
            {
                transientRetried = true;
            }
        }
    }

    private static bool IsJobsPrimaryKeyViolation(PostgresException e) =>
        e.SqlState == PostgresErrorCodes.UniqueViolation
        && e.TableName is { } table
        && (table == "jobs" || table.StartsWith("jobs_", StringComparison.Ordinal));

    private async Task<IReadOnlyList<EnqueueRowResult>> EnqueueOnceAsync(
        NpgsqlConnection conn, NpgsqlTransaction? tx, EnqueueBatch b, CancellationToken ct)
    {
        var index = new Dictionary<Guid, int>(b.Ids.Length);
        for (var i = 0; i < b.Ids.Length; i++)
            index[b.Ids[i]] = i;

        var created = new bool[b.Ids.Length];
        var existed = new bool[b.Ids.Length];
        await using (var cmd = new NpgsqlCommand(EnqueueSql, conn, tx))
        {
            var ps = cmd.Parameters;
            P.Add(ps, "ids", b.Ids);
            P.Add(ps, "args", b.ArgsJson, NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
            P.Add(ps, "run_at", b.RunAtUtc, NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
            P.Add(ps, "unique_keys", b.UniqueKeys, NpgsqlDbType.Array | NpgsqlDbType.Text);
            P.Add(ps, "type", b.Type);
            P.Add(ps, "queue", b.Queue);
            P.Add(ps, "priority", b.Priority);
            P.Add(ps, "max_attempts", b.MaxAttempts);
            P.AddNullable(ps, "retry_delays", b.RetryDelaysSeconds, NpgsqlDbType.Array | NpgsqlDbType.Integer);
            P.AddNullable(ps, "trace_parent", b.TraceParent, NpgsqlDbType.Text);
            P.Add(ps, "delay", b.Delay);
            await PrepareAsync(cmd, ct);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var i = index[r.GetGuid(0)];
                created[i] = r.GetBoolean(1);
                existed[i] = r.GetBoolean(2);
            }
        }

        var results = new EnqueueRowResult[b.Ids.Length];
        var unresolved = new List<int>();
        for (var i = 0; i < results.Length; i++)
        {
            if (created[i] || existed[i])
                results[i] = new EnqueueRowResult(b.Ids[i], true);
            else
                unresolved.Add(i);
        }
        if (unresolved.Count == 0)
            return results;

        // Конфликт в queue: по unique_key или по PK с ещё не видимой исходной транзакцией. Новый снимок (§5.1).
        await using (var cmd = new NpgsqlCommand(ResolveSql, conn, tx))
        {
            P.Add(cmd.Parameters, "ids", unresolved.Select(i => b.Ids[i]).ToArray());
            P.Add(cmd.Parameters, "unique_keys", unresolved.Select(i => b.UniqueKeys[i]).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Text);
            await PrepareAsync(cmd, ct);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var i = index[r.GetGuid(0)];
                var existingId = r.GetNullableValue<Guid>(2);
                results[i] = r.GetBoolean(1)
                    ? new EnqueueRowResult(b.Ids[i], true)
                    : new EnqueueRowResult(existingId, false);
            }
        }
        return results;
    }
}
```

Добавить в `StoreTestBase` хелпер:

```csharp
    protected async Task<Guid> EnqueueAsync(string queue = "q", short priority = 0, TimeSpan? delay = null,
        string? uniqueKey = null, int maxAttempts = 3, string args = """{"n":1}""")
    {
        var id = Guid.CreateVersion7();
        var r = await Store.EnqueueAsync(Batch(id, queue, priority, delay, null, uniqueKey, maxAttempts, args), null, Ct);
        Assert.True(r[0].Created);
        return id;
    }
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~EnqueueTests"`
Expected: PASS (16 тестов: 8 × 2 режима).

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): enqueue with server-side run_at, ready flag and unique_key resolution"
```

### Task 10: Enqueue — идемпотентный повтор, транзакция, отсутствие deadlock

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Test: `tests/PgBolt.Postgres.Tests/EnqueueIdempotencyTests.cs`

**Interfaces:**
- Consumes: `PgBoltStore.EnqueueAsync` (Task 9). Новый продакшен-код не нужен, если тесты проходят; если какой-то тест падает, исправление вносится в `PgBoltStore.Enqueue.cs` в рамках спеки §5.1/§9.

- [x] **Step 1: Написать тесты**

`tests/PgBolt.Postgres.Tests/EnqueueIdempotencyTests.cs`:

```csharp
using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class EnqueueIdempotencyTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Theory]
    [InlineData(null)]
    [InlineData("key-1")]
    public async Task Repeat_while_job_is_in_queue_returns_created(string? key)
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);
        var again = await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, again);
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("key-2")]
    public async Task Repeat_after_job_finished_does_not_requeue(string? key)
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);
        await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id", ("id", id));
        await Db.ExecAsync("UPDATE {schema}.jobs SET state = 2, finished_at = now() WHERE id = @id", ("id", id));

        var again = await Store.EnqueueAsync(Batch(id, uniqueKey: key), null, Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, again);
        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)2, await JobStateAsync(id));
    }

    [Fact]
    public async Task Repeat_while_original_transaction_is_uncommitted_waits_and_returns_created()
    {
        var id = Guid.CreateVersion7();
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await Store.EnqueueAsync(Batch(id), tx, Ct);

        var retry = Store.EnqueueAsync(Batch(id), null, Ct);
        await Task.Delay(300, Ct);
        Assert.False(retry.IsCompleted);
        await tx.CommitAsync(Ct);

        Assert.Equal(new[] { new EnqueueRowResult(id, true) }, await retry.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Rolled_back_transaction_leaves_nothing()
    {
        var id = Guid.CreateVersion7();
        await using (var conn = await Db.DataSource.OpenConnectionAsync(Ct))
        await using (var tx = await conn.BeginTransactionAsync(Ct))
        {
            await Store.EnqueueAsync(Batch(id), tx, Ct);
            await tx.RollbackAsync(Ct);
        }
        Assert.False(await InQueueAsync(id));
        Assert.Null(await JobStateAsync(id));
    }

    [Fact]
    public async Task Completed_transaction_argument_is_rejected()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        var tx = await conn.BeginTransactionAsync(Ct);
        await tx.CommitAsync(Ct);
        // Npgsql 10 оставляет tx.Connection ненулевым после Commit и сам бросает InvalidOperationException при
        // выполнении команды на завершённой транзакции; store даёт то же исключение, если Connection == null.
        await Assert.ThrowsAsync<InvalidOperationException>(() => Store.EnqueueAsync(Batch(Guid.CreateVersion7()), tx, Ct));
    }
}

public sealed class EnqueueIdempotencyTests_Direct(PostgresFixture pg) : EnqueueIdempotencyTests(pg, ConnectionMode.Direct);
public sealed class EnqueueIdempotencyTests_Bouncer(PostgresFixture pg) : EnqueueIdempotencyTests(pg, ConnectionMode.Bouncer);

/// <summary>Без повторов при 40P01: deadlock должен всплыть исключением, если порядок вставки неединый (§5.1).</summary>
public sealed class EnqueueDeadlockTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Parallel_batches_with_overlapping_keys_in_different_order_do_not_deadlock()
    {
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false);
        for (var round = 0; round < 10; round++)
        {
            var keys = Enumerable.Range(0, 40).Select(i => $"r{round}-k{i}").ToArray();
            var tasks = Enumerable.Range(0, 8).Select(w =>
            {
                var shuffled = keys.ToArray();
                new Random(round * 100 + w).Shuffle(shuffled);
                var ids = shuffled.Select(_ => Guid.CreateVersion7()).ToArray();
                return store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                    ids, ids.Select(_ => "{}").ToArray(), new DateTime?[ids.Length], shuffled), null, Ct);
            });
            var results = await Task.WhenAll(tasks); // PostgresException 40P01 провалит тест
            Assert.Equal(40, results.SelectMany(r => r).Count(r => r.Created));
        }
    }
}
```

- [x] **Step 2: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~EnqueueIdempotencyTests|FullyQualifiedName~EnqueueDeadlockTests"`
Expected: PASS. Если `Repeat_after_job_finished_does_not_requeue` падает с 23505 — проверить, что CTE `existing` фильтрует `input` (строка `WHERE id NOT IN (SELECT id FROM existing)`).

- [x] **Step 3: Commit**

```bash
git add tests/PgBolt.Postgres.Tests/EnqueueIdempotencyTests.cs
git commit -m "test(store): idempotent enqueue retry, user transactions, no deadlock between batches"
```

### Task 11: Dequeue

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Dequeue.cs`
- Modify: `tests/PgBolt.Postgres.Tests/StoreTestBase.cs` (добавить хелпер `LeaseOneAsync`)
- Test: `tests/PgBolt.Postgres.Tests/DequeueTests.cs`

**Interfaces:**
- Produces: `public Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)`.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/DequeueTests.cs`:

```csharp
using System.Collections.Concurrent;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class DequeueTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Dequeue_leases_job_and_returns_definition()
    {
        var id = Guid.CreateVersion7();
        await Store.EnqueueAsync(Batch(id, args: """{"a":"b"}""", maxAttempts: 7, retryDelays: [2],
            traceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"), null, Ct);

        var job = await LeaseOneAsync();

        Assert.Equal(id, job.Id);
        Assert.Equal("test.job", job.Type);
        Assert.Equal("""{"a": "b"}""", job.ArgsJson);
        Assert.Equal((1, 7), (job.Attempt, job.MaxAttempts));
        Assert.Equal(new[] { 2 }, job.RetryDelaysSeconds);
        Assert.Equal("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", job.TraceParent);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT lease_owner = @s AND lease_id = @l AND attempt = 1 AND leased_at IS NOT NULL
                   AND abs(extract(epoch FROM lease_until - (now() + interval '5 minutes'))) < 5
            FROM {schema}.queue WHERE job_id = @id
            """, ("s", ServerA), ("l", job.LeaseId), ("id", id)));
        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 0 AND started_at IS NULL FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Not_ready_jobs_are_not_dequeued()
    {
        await EnqueueAsync(delay: TimeSpan.FromHours(1));
        Assert.Empty(await Store.DequeueAsync("q", 10, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Order_is_priority_desc_then_run_at()
    {
        var lowEarly = await EnqueueAsync(priority: 0);
        var high = await EnqueueAsync(priority: 5);
        var lowLate = await EnqueueAsync(priority: 0);

        var jobs = await Store.DequeueAsync("q", 10, ServerA, Lease, Ct);

        Assert.Equal(new[] { high, lowEarly, lowLate }, jobs.Select(j => j.Id));
    }

    [Fact]
    public async Task Respects_limit_queue_and_existing_leases()
    {
        for (var i = 0; i < 5; i++)
            await EnqueueAsync();
        await EnqueueAsync(queue: "other");

        Assert.Equal(3, (await Store.DequeueAsync("q", 3, ServerA, Lease, Ct)).Count);
        Assert.Equal(2, (await Store.DequeueAsync("q", 10, ServerB, Lease, Ct)).Count);
        Assert.Empty(await Store.DequeueAsync("q", 10, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Concurrent_dequeuers_get_disjoint_jobs()
    {
        var ids = new HashSet<Guid>();
        for (var i = 0; i < 200; i++)
            ids.Add(await EnqueueAsync());

        var seen = new ConcurrentBag<Guid>();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(async w =>
        {
            var server = Guid.NewGuid();
            while (true)
            {
                var batch = await Store.DequeueAsync("q", 10, server, Lease, Ct);
                if (batch.Count == 0)
                    return;
                foreach (var j in batch)
                    seen.Add(j.Id);
            }
        }));

        Assert.Equal(200, seen.Count);
        Assert.True(ids.SetEquals(seen));
    }
}

public sealed class DequeueTests_Direct(PostgresFixture pg) : DequeueTests(pg, ConnectionMode.Direct);
public sealed class DequeueTests_Bouncer(PostgresFixture pg) : DequeueTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Добавить в `StoreTestBase` хелпер:

```csharp
    protected async Task<LeasedJob> LeaseOneAsync(string queue = "q", Guid? server = null)
    {
        var leased = await Store.DequeueAsync(queue, 1, server ?? ServerA, Lease, Ct);
        return Assert.Single(leased);
    }
```

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~DequeueTests"`
Expected: FAIL — `DequeueAsync` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Dequeue.cs` (SQL — §5.2 дословно; условие `ANY(ARRAY(SELECT …))` не добавлять — оно отключает pruning):

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string DequeueTemplate = """
        WITH c AS (
            SELECT job_id FROM {schema}.queue
            WHERE queue = @queue AND lease_owner IS NULL AND ready
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
        FROM l JOIN {schema}.jobs j ON j.id = l.job_id
        """;

    private string DequeueSql => field ??= Render(DequeueTemplate);

    /// <summary>Берёт до <paramref name="max"/> готовых задач очереди в lease. max = число свободных слотов (без prefetch).</summary>
    public Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(max);
        return QueryAsync<IReadOnlyList<LeasedJob>>(DequeueSql, ps =>
        {
            P.Add(ps, "queue", queue);
            P.Add(ps, "n", max);
            P.Add(ps, "server", serverId);
            P.Add(ps, "lease", lease);
        }, async (r, c) => await r.ReadAllAsync(static r => new LeasedJob(
            r.GetGuid(0), r.GetGuid(1), r.GetString(2), r.GetString(3), r.GetInt32(4), r.GetInt32(5),
            r.GetNullable<int[]>(6), r.GetNullableValue<Guid>(7), r.GetNullableValue<Guid>(8), r.GetNullable<string>(9)), c), ct);
    }
}
```

`QueryAsync` возвращает результат только после полного чтения reader'а и его закрытия — это требование §5.5 («dequeue завершился = ReadyForQuery»).

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~DequeueTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): dequeue ready jobs with SKIP LOCKED and fencing lease_id"
```

### Task 12: Ack и контракт HOT

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Ack.cs`
- Test: `tests/PgBolt.Postgres.Tests/AckTests.cs` (+ смоук `UsePreparedStatements = true`), `tests/PgBolt.Postgres.Tests/HotUpdateTests.cs`

**Interfaces:**
- Produces: `public Task<int> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct)` — число подтверждённых задач.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/AckTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class AckTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Ack_deletes_queue_row_and_finalizes_job()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        var leasedAt = await Db.ScalarAsync<DateTime>("SELECT leased_at FROM {schema}.queue WHERE job_id = @id", ("id", id));

        Assert.Equal(1, await Store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct));

        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 2 AND attempt = 1 AND started_at = @leased AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("leased", leasedAt), ("s", ServerA), ("id", id)));
        Assert.Empty(await EventKindsAsync(id));
    }

    [Fact]
    public async Task Ack_with_foreign_lease_id_is_ignored()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Equal(0, await Store.AckAsync(ServerA, [new LeaseRef(id, Guid.NewGuid())], Ct));
        Assert.True(await InQueueAsync(id));
    }

    [Fact]
    public async Task Stale_lease_of_same_server_cannot_ack_new_attempt()
    {
        var id = await EnqueueAsync();
        var first = await LeaseOneAsync();
        await Db.ExecAsync("UPDATE {schema}.queue SET lease_owner = NULL, lease_id = NULL, lease_until = NULL, leased_at = NULL WHERE job_id = @id", ("id", id));
        var second = await LeaseOneAsync();

        Assert.Equal(0, await Store.AckAsync(ServerA, [new LeaseRef(id, first.LeaseId)], Ct));
        Assert.Equal(1, await Store.AckAsync(ServerA, [new LeaseRef(id, second.LeaseId)], Ct));
        Assert.Equal((short)2, await JobStateAsync(id));
        Assert.Equal(2, await Db.ScalarAsync<int>("SELECT attempt FROM {schema}.jobs WHERE id = @id", ("id", id)));
    }

    [Fact]
    public async Task Batch_ack()
    {
        for (var i = 0; i < 50; i++)
            await EnqueueAsync();
        var jobs = await Store.DequeueAsync("q", 50, ServerA, Lease, Ct);
        Assert.Equal(50, await Store.AckAsync(ServerA, jobs.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToList(), Ct));
        Assert.Equal(50L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE state = 2"));
    }

    [Fact]
    public async Task Empty_ack_is_noop()
    {
        Assert.Equal(0, await Store.AckAsync(ServerA, [], Ct));
    }
}

public sealed class AckTests_Direct(PostgresFixture pg) : AckTests(pg, ConnectionMode.Direct);
public sealed class AckTests_Bouncer(PostgresFixture pg) : AckTests(pg, ConnectionMode.Bouncer);

/// <summary>§1 `UsePreparedStatements = true`: смоук цикла enqueue → dequeue → ack с явным PrepareAsync. Только Direct: PgBouncer в тестах — с max_prepared_statements = 0.</summary>
public sealed class PreparedStatementsTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Enqueue_dequeue_ack_with_prepared_statements()
    {
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: true, retryOnConflict: true);
        for (var round = 0; round < 2; round++) // второй круг выполняет уже подготовленные statement'ы
        {
            var id = Guid.CreateVersion7();
            Assert.True((await store.EnqueueAsync(Batch(id), null, Ct))[0].Created);
            var job = Assert.Single(await store.DequeueAsync("q", 10, ServerA, Lease, Ct));
            Assert.Equal(id, job.Id);
            Assert.Equal(1, await store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct));
            Assert.Equal((short)2, await JobStateAsync(id));
        }
    }
}
```

`tests/PgBolt.Postgres.Tests/HotUpdateTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>
/// Несвязанная с другими тестами коллекция без параллелизма: xUnit запускает её после всех параллельных коллекций.
/// PostgresFixture — assembly fixture (AssemblyInfo.cs), поэтому коллекции не нужен свой ICollectionFixture.
/// </summary>
[CollectionDefinition("Serial", DisableParallelization = true)]
public sealed class SerialCollection;

/// <summary>
/// Контракт HOT (§4.2, §11): терминальное обновление jobs остаётся HOT. Только Direct — нужна статистика pg_stat.
/// Serial: открытые транзакции параллельных тестов удерживают xmin-горизонт, старые версии строк не вычищаются
/// page pruning'ом, страницы переполняются, и доля HOT падает ниже 95 % (на полном прогоне — ~87 %).
/// </summary>
[Collection("Serial")]
public sealed class HotUpdateTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Terminal_update_of_jobs_is_hot()
    {
        const int count = 2000;
        var args = $$"""{"payload":"{{new string('x', 180)}}"}""";
        var ids = Enumerable.Range(0, count).Select(_ => Guid.CreateVersion7()).ToArray();
        for (var i = 0; i < count; i += 200)
        {
            var chunk = ids[i..(i + 200)];
            await Store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                chunk, chunk.Select(_ => args).ToArray(), new DateTime?[chunk.Length], new string?[chunk.Length]), null, Ct);
        }
        var leases = new Dictionary<Guid, LeaseRef>(count);
        while (true)
        {
            var jobs = await Store.DequeueAsync("q", 200, ServerA, Lease, Ct);
            if (jobs.Count == 0)
                break;
            foreach (var j in jobs)
                leases.Add(j.Id, new LeaseRef(j.Id, j.LeaseId));
        }
        Assert.Equal(count, leases.Count);

        // Строки одного enqueue-чанка лежат на соседних страницах (~18 строк на страницу), а запас
        // fillfactor=80 вмещает ~5 новых версий; старые версии той же транзакции не вычищаются.
        // Ack 200 соседних строк одним statement дал бы ~30 % HOT (проверено на PG 17), поэтому
        // пакеты берутся с шагом 10 в порядке вставки — ~2 строки на страницу за statement (§11).
        for (var k = 0; k < 10; k++)
            await Store.AckAsync(ServerA, ids.Where((_, i) => i % 10 == k).Select(id => leases[id]).ToList(), Ct);

        await Db.DataSource.DisposeAsync(); // backend'ы завершаются и сбрасывают статистику
        var deadline = DateTime.UtcNow.AddSeconds(20);
        long updates = 0, hot = 0;
        while (DateTime.UtcNow < deadline)
        {
            updates = await Db.ScalarAsync<long>("SELECT coalesce(sum(n_tup_upd), 0)::bigint FROM pg_stat_user_tables WHERE schemaname = @s AND relname LIKE 'jobs\\_%'", ("s", Db.Schema));
            hot = await Db.ScalarAsync<long>("SELECT coalesce(sum(n_tup_hot_upd), 0)::bigint FROM pg_stat_user_tables WHERE schemaname = @s AND relname LIKE 'jobs\\_%'", ("s", Db.Schema));
            if (updates >= count)
                break;
            await Task.Delay(500, Ct);
        }

        Assert.True(updates >= count, $"n_tup_upd = {updates}");
        Assert.True(hot >= updates * 0.95, $"HOT {hot} of {updates}");
    }
}
```

`TestDb.DisposeAsync` повторно вызовет `DataSource.DisposeAsync()` — `NpgsqlDataSource` допускает повторный dispose.

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~AckTests|FullyQualifiedName~HotUpdateTests|FullyQualifiedName~PreparedStatementsTests"`
Expected: FAIL — `AckAsync` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Ack.cs` (SQL — §5.3 дословно):

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
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
            RETURNING q.job_id, q.attempt, q.leased_at
        )
        UPDATE {schema}.jobs j
        SET state = 2, attempt = d.attempt, started_at = d.leased_at, finished_at = now(), server_id = @server
        FROM d
        WHERE j.id = d.job_id AND j.id = ANY(@ids)
        """;

    private string AckSql => field ??= Render(AckTemplate);

    /// <summary>Пакетный ack (§5.3). Задачи с потерянным lease игнорируются. Возвращает число подтверждённых.</summary>
    public Task<int> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct)
    {
        if (leases.Count == 0)
            return Task.FromResult(0);
        var ids = new Guid[leases.Count];
        var leaseIds = new Guid[leases.Count];
        for (var i = 0; i < leases.Count; i++)
            (ids[i], leaseIds[i]) = (leases[i].JobId, leases[i].LeaseId);

        return NonQueryAsync(AckSql, ps =>
        {
            P.Add(ps, "ids", ids);
            P.Add(ps, "lease_ids", leaseIds);
            P.Add(ps, "server", serverId);
        }, ct);
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~AckTests|FullyQualifiedName~HotUpdateTests|FullyQualifiedName~PreparedStatementsTests"`
Expected: PASS. Если HOT-доля ниже 95 % — проверить `fillfactor` партиций (Task 7) и отсутствие индексов по изменяемым колонкам (Task 6).

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): batch ack with lease fencing; HOT contract test"
```

### Task 13: Fail — retry, NoExecutor, окончательный fail, подтверждение отмены

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Fail.cs`
- Test: `tests/PgBolt.Postgres.Tests/FailTests.cs`

**Interfaces:**
- Produces:
  - `public Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind, string? message, string? exception, CancellationToken ct)` — `kind` ∈ {`Retry`, `NoExecutor`}; `Retried` / `Cancelled` (запрошена отмена) / `LeaseLost`.
  - `public Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind, string? message, string? exception, CancellationToken ct)` — `kind` ∈ {`Failed`, `NoExecutor`}; `Failed` / `Cancelled` / `LeaseLost`.
  - `public Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct)` — окончательный fail с `@state = 4`, `@kind = Cancelled` (§5.7).
- Отличие SQL от спеки: итоговый `INSERT INTO job_events` дополнен `RETURNING kind`, чтобы метод различал исходы; при отсутствии строк — `LeaseLost`.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/FailTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class FailTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private Task RequestCancelAsync(Guid id) =>
        Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

    [Fact]
    public async Task Retry_without_delay_returns_job_ready_with_event()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();

        var outcome = await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "boom", "stack", Ct);

        Assert.Equal(FailOutcome.Retried, outcome);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT ready AND lease_owner IS NULL AND lease_id IS NULL AND attempt = 1 FROM {schema}.queue WHERE job_id = @id
            """, ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Retry }, await EventKindsAsync(id));
        Assert.Equal("stack", await Db.ScalarAsync<string>("SELECT exception FROM {schema}.job_events WHERE job_id = @id", ("id", id)));
        Assert.Equal((short)0, await JobStateAsync(id));
    }

    [Fact]
    public async Task Retry_with_delay_resets_ready()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.FromMinutes(10), JobEventKind.Retry, null, null, Ct);
        Assert.False(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.Empty(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Retry_with_lost_lease_does_nothing()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Equal(FailOutcome.LeaseLost, await Store.RetryAsync(id, Guid.NewGuid(), ServerA, TimeSpan.Zero, JobEventKind.Retry, null, null, Ct));
        Assert.Empty(await EventKindsAsync(id));
    }

    [Fact]
    public async Task Retry_when_cancel_requested_finalizes_as_cancelled()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await RequestCancelAsync(id);

        Assert.Equal(FailOutcome.Cancelled, await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "x", null, Ct));
        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task NoExecutor_retry_writes_its_kind()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Store.RetryAsync(id, job.LeaseId, ServerA, TimeSpan.FromSeconds(30), JobEventKind.NoExecutor, null, null, Ct);
        Assert.Equal(new[] { (short)JobEventKind.NoExecutor }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Final_fail_marks_failed()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();

        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.Failed, "bad", "trace", Ct));
        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 3 AND attempt = 1 AND started_at IS NOT NULL AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("s", ServerA), ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Failed }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Final_fail_when_cancel_requested_marks_cancelled()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await RequestCancelAsync(id);
        Assert.Equal(FailOutcome.Cancelled, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.Failed, null, null, Ct));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Final_NoExecutor_marks_failed_with_its_kind()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.NoExecutor, null, null, Ct));
        Assert.Equal((short)3, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.NoExecutor }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task ConfirmCancel_marks_cancelled()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Cancelled, await Store.ConfirmCancelAsync(id, job.LeaseId, ServerA, "stopped", Ct));
        Assert.Equal((short)4, await JobStateAsync(id));
    }

    [Fact]
    public async Task Invalid_kinds_are_rejected()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.RetryAsync(Guid.NewGuid(), Guid.NewGuid(), ServerA, TimeSpan.Zero, JobEventKind.Failed, null, null, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.FailAsync(Guid.NewGuid(), Guid.NewGuid(), ServerA, JobEventKind.Retry, null, null, Ct));
    }
}

public sealed class FailTests_Direct(PostgresFixture pg) : FailTests(pg, ConnectionMode.Direct);
public sealed class FailTests_Bouncer(PostgresFixture pg) : FailTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~FailTests"`
Expected: FAIL — методы не найдены.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Fail.cs` (SQL — §5.4 + `RETURNING kind`):

```csharp
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
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
            RETURNING q.job_id, q.attempt, q.leased_at
        ),
        cj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = c.attempt, started_at = c.leased_at,
                                       finished_at = now(), server_id = @server
            FROM c WHERE j.id = c.job_id AND j.id = @id
        )
        INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message, exception)
        SELECT job_id, now(), @kind, @server, attempt, @message, @exception FROM r
        UNION ALL
        SELECT job_id, now(), @kind_cancelled, @server, attempt, @message, @exception FROM c
        RETURNING kind
        """;

    private const string FinalFailTemplate = """
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
        FROM d
        RETURNING kind
        """;

    private string RetrySql => field ??= Render(RetryTemplate);
    private string FinalFailSql => field ??= Render(FinalFailTemplate);

    public Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        if (kind is not (JobEventKind.Retry or JobEventKind.NoExecutor))
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "Retry accepts Retry or NoExecutor.");
        return FailStatementAsync(RetrySql, jobId, leaseId, serverId, kind, message, exception, ps =>
        {
            P.Add(ps, "delay", delay);
        }, FailOutcome.Retried, ct);
    }

    public Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        if (kind is not (JobEventKind.Failed or JobEventKind.NoExecutor))
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "Fail accepts Failed or NoExecutor.");
        return FailStatementAsync(FinalFailSql, jobId, leaseId, serverId, kind, message, exception,
            ps => P.Add(ps, "state", (short)3), FailOutcome.Failed, ct);
    }

    public Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct) =>
        FailStatementAsync(FinalFailSql, jobId, leaseId, serverId, JobEventKind.Cancelled, message, null,
            ps => P.Add(ps, "state", (short)4), FailOutcome.Cancelled, ct);

    private Task<FailOutcome> FailStatementAsync(string sql, Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, Action<Npgsql.NpgsqlParameterCollection> bindExtra, FailOutcome normalOutcome,
        CancellationToken ct) =>
        QueryAsync(sql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "lease_id", leaseId);
            P.Add(ps, "server", serverId);
            P.Add(ps, "kind", (short)kind);
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
            P.AddNullable(ps, "message", message, NpgsqlDbType.Text);
            P.AddNullable(ps, "exception", exception, NpgsqlDbType.Text);
            bindExtra(ps);
        }, async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                return FailOutcome.LeaseLost;
            return r.GetInt16(0) == (short)JobEventKind.Cancelled && kind != JobEventKind.Cancelled
                ? FailOutcome.Cancelled
                : normalOutcome;
        }, ct);
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~FailTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): retry, final fail and cancel confirmation with unified cancel rule"
```

### Task 14: Heartbeat и порядок захвата строк

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Heartbeat.cs`
- Test: `tests/PgBolt.Postgres.Tests/HeartbeatTests.cs`

**Interfaces:**
- Produces: `public Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, TimeSpan lease, CancellationToken ct)`.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/HeartbeatTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class HeartbeatTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Extends_only_own_leases_and_returns_them()
    {
        await EnqueueAsync();
        await EnqueueAsync();
        var b1 = await EnqueueAsync();
        var leasedA = await Store.DequeueAsync("q", 2, ServerA, Lease, Ct);
        await Store.DequeueAsync("q", 1, ServerB, Lease, Ct);
        await Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() + interval '1 second'");

        var result = await Store.HeartbeatAsync(ServerA, TimeSpan.FromMinutes(10), Ct);

        Assert.Equal(leasedA.Select(j => (j.Id, j.LeaseId)).OrderBy(x => x.Id), result.Select(h => (h.JobId, h.LeaseId)).OrderBy(x => x.JobId));
        Assert.All(result, h => Assert.False(h.CancelRequested));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT bool_and(lease_until > now() + interval '9 minutes') FROM {schema}.queue WHERE lease_owner = @s
            """, ("s", ServerA)));
        Assert.True(await Db.ScalarAsync<bool>("SELECT lease_until < now() + interval '1 minute' FROM {schema}.queue WHERE job_id = @id", ("id", b1)));
    }

    [Fact]
    public async Task Reports_cancel_requests()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

        var hb = Assert.Single(await Store.HeartbeatAsync(ServerA, Lease, Ct));
        Assert.True(hb.CancelRequested);
    }

    [Fact]
    public async Task Updates_server_heartbeat_when_registered()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
            VALUES (@id, 's', 'h', ARRAY['q'], 1, now(), now() - interval '1 hour')
            """, ("id", ServerA));
        await Store.HeartbeatAsync(ServerA, Lease, Ct);
        Assert.True(await Db.ScalarAsync<bool>("SELECT heartbeat_at > now() - interval '1 minute' FROM {schema}.servers WHERE id = @id", ("id", ServerA)));
    }

    [Fact]
    public async Task No_leases_returns_empty()
    {
        Assert.Empty(await Store.HeartbeatAsync(ServerA, Lease, Ct));
    }
}

public sealed class HeartbeatTests_Direct(PostgresFixture pg) : HeartbeatTests(pg, ConnectionMode.Direct);
public sealed class HeartbeatTests_Bouncer(PostgresFixture pg) : HeartbeatTests(pg, ConnectionMode.Bouncer);

/// <summary>Без повторов при 40P01: heartbeat и пакетный ack одного сервера не должны давать deadlock (§5.3).</summary>
public sealed class HeartbeatAckDeadlockTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Concurrent_heartbeat_and_shuffled_acks_do_not_deadlock()
    {
        var store = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: false);
        for (var i = 0; i < 300; i += 100)
        {
            var ids = Enumerable.Range(0, 100).Select(_ => Guid.CreateVersion7()).ToArray();
            await store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                ids, ids.Select(_ => "{}").ToArray(), new DateTime?[100], new string?[100]), null, Ct);
        }
        var leased = (await store.DequeueAsync("q", 300, ServerA, Lease, Ct)).Select(j => new LeaseRef(j.Id, j.LeaseId)).ToArray();
        Random.Shared.Shuffle(leased);

        using var stop = new CancellationTokenSource();
        var heartbeats = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
                await store.HeartbeatAsync(ServerA, Lease, Ct);
        }, Ct);

        var acks = leased.Chunk(10).Select(chunk => store.AckAsync(ServerA, chunk, Ct));
        var acked = await Task.WhenAll(acks);   // PostgresException 40P01 провалит тест
        stop.Cancel();
        await heartbeats;

        Assert.Equal(300, acked.Sum());
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~HeartbeatTests|FullyQualifiedName~HeartbeatAckDeadlockTests"`
Expected: FAIL — `HeartbeatAsync` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Heartbeat.cs` (SQL — §5.5 дословно):

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string HeartbeatTemplate = """
        WITH s AS (
            UPDATE {schema}.servers SET heartbeat_at = now() WHERE id = @server
        ),
        l AS MATERIALIZED (
            SELECT job_id FROM {schema}.queue
            WHERE lease_owner = @server
            ORDER BY job_id
            FOR UPDATE
        )
        UPDATE {schema}.queue q SET lease_until = now() + @lease
        FROM l WHERE q.job_id = l.job_id
        RETURNING q.job_id, q.lease_id, q.cancel_requested
        """;

    private string HeartbeatSql => field ??= Render(HeartbeatTemplate);

    /// <summary>Продлевает все lease сервера и доставляет запросы отмены (§5.5).</summary>
    public Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, TimeSpan lease, CancellationToken ct) =>
        QueryAsync<IReadOnlyList<HeartbeatLease>>(HeartbeatSql, ps =>
        {
            P.Add(ps, "server", serverId);
            P.Add(ps, "lease", lease);
        }, async (r, c) => await r.ReadAllAsync(static r => new HeartbeatLease(r.GetGuid(0), r.GetGuid(1), r.GetBoolean(2)), c), ct);
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~HeartbeatTests|FullyQualifiedName~HeartbeatAckDeadlockTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): server heartbeat with ordered row locking"
```

### Task 15: Sweeper истёкших lease и fencing

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Sweeper.cs`
- Test: `tests/PgBolt.Postgres.Tests/SweeperTests.cs`

**Interfaces:**
- Produces: `public Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct)` — число обработанных истёкших lease (событий `LeaseExpired`).
- Отличия SQL от спеки: `LIMIT 1000` → `LIMIT @batch`; итоговый `INSERT` дополнен `RETURNING kind`.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/SweeperTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class SweeperTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Expired_lease_with_attempts_left_returns_job_to_queue()
    {
        var id = await EnqueueAsync(maxAttempts: 3);
        await LeaseOneAsync();
        await ExpireLeaseAsync(id);

        Assert.Equal(1, await Store.SweepExpiredLeasesAsync(1000, Ct));

        Assert.True(await Db.ScalarAsync<bool>("SELECT lease_owner IS NULL AND lease_id IS NULL AND ready AND attempt = 1 FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.LeaseExpired }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Expired_lease_with_exhausted_attempts_fails_job()
    {
        var id = await EnqueueAsync(maxAttempts: 1);
        await LeaseOneAsync();
        await ExpireLeaseAsync(id);

        await Store.SweepExpiredLeasesAsync(1000, Ct);

        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)3, await JobStateAsync(id));
    }

    [Fact]
    public async Task Expired_lease_with_cancel_request_cancels_job()
    {
        var id = await EnqueueAsync(maxAttempts: 5);
        await LeaseOneAsync();
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));
        await ExpireLeaseAsync(id);

        await Store.SweepExpiredLeasesAsync(1000, Ct);

        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.LeaseExpired, (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Live_leases_are_untouched()
    {
        await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Equal(0, await Store.SweepExpiredLeasesAsync(1000, Ct));
    }

    [Fact]
    public async Task Fencing_after_sweep_and_re_lease_by_same_server()
    {
        var id = await EnqueueAsync(maxAttempts: 5);
        var first = await LeaseOneAsync();
        await ExpireLeaseAsync(id);
        await Store.SweepExpiredLeasesAsync(1000, Ct);
        var second = await LeaseOneAsync();

        var hb = Assert.Single(await Store.HeartbeatAsync(ServerA, Lease, Ct));
        Assert.Equal(second.LeaseId, hb.LeaseId);
        Assert.Equal(FailOutcome.LeaseLost, await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, null, null, Ct));
        Assert.Equal(0, await Store.AckAsync(ServerA, [new LeaseRef(id, first.LeaseId)], Ct));
        Assert.Equal(1, await Store.AckAsync(ServerA, [new LeaseRef(id, second.LeaseId)], Ct));
    }
}

public sealed class SweeperTests_Direct(PostgresFixture pg) : SweeperTests(pg, ConnectionMode.Direct);
public sealed class SweeperTests_Bouncer(PostgresFixture pg) : SweeperTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~SweeperTests"`
Expected: FAIL — `SweepExpiredLeasesAsync` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Sweeper.cs`:

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
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
            RETURNING q.job_id, q.attempt, q.cancel_requested
        ),
        f AS (
            UPDATE {schema}.jobs j
            SET state = CASE WHEN fin.cancel_requested THEN 4 ELSE 3 END, attempt = fin.attempt, finished_at = now()
            FROM fin
            WHERE j.id = fin.job_id
        )
        INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt)
        SELECT job_id, now(), @kind_lease_expired, lease_owner, attempt FROM e
        UNION ALL
        SELECT job_id, now(), @kind_cancelled, lease_owner, attempt FROM e WHERE cancel_requested
        RETURNING kind
        """;

    private string SweepSql => field ??= Render(SweepTemplate);

    /// <summary>Сбрасывает истёкшие lease (§5.6). Можно запускать на всех серверах одновременно.</summary>
    public Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        return QueryAsync(SweepSql, ps =>
        {
            P.Add(ps, "batch", batchSize);
            P.Add(ps, "kind_lease_expired", (short)JobEventKind.LeaseExpired);
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
        }, async (r, c) =>
        {
            var expired = 0;
            while (await r.ReadAsync(c))
                if (r.GetInt16(0) == (short)JobEventKind.LeaseExpired)
                    expired++;
            return expired;
        }, ct);
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~SweeperTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): expired lease sweeper honoring attempts and cancel requests"
```

### Task 16: Promoter и индекс dequeue без отложенных строк

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Promoter.cs`
- Test: `tests/PgBolt.Postgres.Tests/PromoterTests.cs`

**Interfaces:**
- Produces: `public Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct)` — повторяет statement §5.11, пока он возвращает полный пакет; `Queues` — множество очередей для wakeup.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/PromoterTests.cs`:

```csharp
using System.Text.Json;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class PromoterTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Promotes_only_due_rows_and_reports_queues()
    {
        var due = await EnqueueAsync(queue: "a", delay: TimeSpan.FromHours(1));
        var later = await EnqueueAsync(queue: "b", delay: TimeSpan.FromHours(1));
        await Db.ExecAsync("UPDATE {schema}.queue SET run_at = now() - interval '1 second' WHERE job_id = @id", ("id", due));

        var result = await Store.PromoteDueAsync(1000, Ct);

        Assert.Equal(1, result.Promoted);
        Assert.Equal(new[] { "a" }, result.Queues);
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", due)));
        Assert.False(await Db.ScalarAsync<bool>("SELECT ready FROM {schema}.queue WHERE job_id = @id", ("id", later)));
    }

    [Fact]
    public async Task Loops_until_partial_batch()
    {
        for (var i = 0; i < 25; i++)
            await EnqueueAsync(delay: TimeSpan.FromHours(1));
        await Db.ExecAsync("UPDATE {schema}.queue SET run_at = now() - interval '1 second'");

        Assert.Equal(25, (await Store.PromoteDueAsync(10, Ct)).Promoted);
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue WHERE NOT ready"));
    }

    [Fact]
    public async Task Delayed_job_is_invisible_to_dequeue_until_promoted()
    {
        await EnqueueAsync(delay: TimeSpan.FromMilliseconds(200));
        await Task.Delay(500, Ct);
        Assert.Empty(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
        await Store.PromoteDueAsync(1000, Ct);
        Assert.Single(await Store.DequeueAsync("q", 1, ServerA, Lease, Ct));
    }
}

public sealed class PromoterTests_Direct(PostgresFixture pg) : PromoterTests(pg, ConnectionMode.Direct);
public sealed class PromoterTests_Bouncer(PostgresFixture pg) : PromoterTests(pg, ConnectionMode.Bouncer);

/// <summary>§10/§11: стоимость dequeue не зависит от числа отложенных строк. Только Direct (EXPLAIN).</summary>
[Trait("Category", "Slow")]
public sealed class DequeuePlanTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Dequeue_reads_only_ready_entries_with_million_delayed_rows()
    {
        await Db.ExecAsync("""
            INSERT INTO {schema}.queue (job_id, queue, priority, run_at, ready, max_attempts)
            SELECT gen_random_uuid(), 'q', (g % 3)::smallint, now() + interval '1 day', false, 3
            FROM generate_series(1, 1000000) g
            """);
        for (var i = 0; i < 5; i++)
            await EnqueueAsync();
        await Db.ExecAsync("ANALYZE {schema}.queue");

        var plan = await Db.ScalarAsync<string>("""
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT job_id FROM {schema}.queue
            WHERE queue = 'q' AND lease_owner IS NULL AND ready
            ORDER BY priority DESC, run_at
            LIMIT 10
            FOR UPDATE SKIP LOCKED
            """);

        using var doc = JsonDocument.Parse(plan);
        var root = doc.RootElement[0].GetProperty("Plan");
        Assert.Contains(Flatten(root), n => n.TryGetProperty("Index Name", out var name) && name.GetString() == "queue_fetch");
        var blocks = root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64();
        Assert.True(blocks < 100, $"dequeue touched {blocks} buffers");
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

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~PromoterTests|FullyQualifiedName~DequeuePlanTests"`
Expected: FAIL — `PromoteDueAsync` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Promoter.cs` (SQL — §5.11 дословно):

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string PromoteTemplate = """
        WITH p AS (
            SELECT job_id FROM {schema}.queue
            WHERE NOT ready AND run_at <= now()
            ORDER BY run_at
            LIMIT @batch
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {schema}.queue q SET ready = true
        FROM p WHERE q.job_id = p.job_id
        RETURNING q.queue
        """;

    private string PromoteSql => field ??= Render(PromoteTemplate);

    /// <summary>Scheduled → Enqueued для наступивших run_at (§5.11). Повторяет пакеты, пока пакет полный.</summary>
    public async Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        var queues = new HashSet<string>(StringComparer.Ordinal);
        var total = 0;
        while (true)
        {
            var batch = await QueryAsync(PromoteSql, ps => P.Add(ps, "batch", batchSize),
                async (r, c) => await r.ReadAllAsync(static r => r.GetString(0), c), ct);
            total += batch.Count;
            queues.UnionWith(batch);
            if (batch.Count < batchSize)
                return new PromoteResult(total, queues);
        }
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~PromoterTests|FullyQualifiedName~DequeuePlanTests"`
Expected: PASS (`DequeuePlanTests` — до ~30 с из-за вставки миллиона строк).

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): promoter for delayed jobs; dequeue plan independent of delayed rows"
```

### Task 17: Cancel

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Cancel.cs`
- Test: `tests/PgBolt.Postgres.Tests/CancelTests.cs`

**Interfaces:**
- Produces: `public Task<CancelResult> CancelAsync(Guid jobId, string? reason, CancellationToken ct)` — транзакция из двух statement'ов (§5.7); при 40P01/40001 повторяется вся транзакция.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/CancelTests.cs`:

```csharp
using System.Collections.Concurrent;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class CancelTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Pending_job_is_cancelled_immediately()
    {
        var id = await EnqueueAsync();
        Assert.Equal(CancelResult.Cancelled, await Store.CancelAsync(id, "no longer needed", Ct));
        Assert.False(await InQueueAsync(id));
        Assert.Equal((short)4, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
        Assert.Equal("no longer needed", await Db.ScalarAsync<string>("SELECT message FROM {schema}.job_events WHERE job_id = @id", ("id", id)));
    }

    [Fact]
    public async Task Delayed_job_is_cancelled_immediately()
    {
        var id = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        Assert.Equal(CancelResult.Cancelled, await Store.CancelAsync(id, null, Ct));
        Assert.Equal((short)4, await JobStateAsync(id));
    }

    [Fact]
    public async Task Leased_job_gets_cancellation_request()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Equal(CancelResult.CancellationRequested, await Store.CancelAsync(id, null, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT cancel_requested FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.True(Assert.Single(await Store.HeartbeatAsync(ServerA, Lease, Ct)).CancelRequested);
    }

    [Fact]
    public async Task Finished_job_reports_already_finished()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct);
        Assert.Equal(CancelResult.AlreadyFinished, await Store.CancelAsync(id, null, Ct));
    }

    [Fact]
    public async Task Unknown_job_reports_not_found()
    {
        Assert.Equal(CancelResult.NotFound, await Store.CancelAsync(Guid.CreateVersion7(), null, Ct));
    }

    [Fact]
    public async Task Cancel_racing_dequeue_never_loses_a_leased_job()
    {
        var ids = new List<Guid>();
        for (var i = 0; i < 100; i++)
            ids.Add(await EnqueueAsync());

        var leased = new ConcurrentDictionary<Guid, bool>();
        var dequeuer = Task.Run(async () =>
        {
            for (var empty = 0; empty < 3;)
            {
                var batch = await Store.DequeueAsync("q", 5, ServerA, Lease, Ct);
                if (batch.Count == 0) { empty++; continue; }
                foreach (var j in batch)
                    leased[j.Id] = true;
            }
        }, Ct);
        var results = await Task.WhenAll(ids.Select(id => Store.CancelAsync(id, null, Ct)));
        await dequeuer;

        for (var i = 0; i < ids.Count; i++)
        {
            if (leased.ContainsKey(ids[i]))
            {
                Assert.Equal(CancelResult.CancellationRequested, results[i]);
                Assert.True(await Db.ScalarAsync<bool>("SELECT cancel_requested FROM {schema}.queue WHERE job_id = @id", ("id", ids[i])));
            }
            else
            {
                Assert.Equal(CancelResult.Cancelled, results[i]);
                Assert.Equal((short)4, await JobStateAsync(ids[i]));
            }
        }
    }
}

public sealed class CancelTests_Direct(PostgresFixture pg) : CancelTests(pg, ConnectionMode.Direct);
public sealed class CancelTests_Bouncer(PostgresFixture pg) : CancelTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~CancelTests"`
Expected: FAIL — `CancelAsync` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Cancel.cs` (SQL — §5.7 дословно, плюс чтение `jobs.state`, когда задачи нет в `queue`):

```csharp
using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string CancelMarkTemplate = """
        UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id RETURNING lease_owner
        """;

    private const string CancelNowTemplate = """
        WITH d AS (DELETE FROM {schema}.queue WHERE job_id = @id RETURNING job_id, attempt),
        j AS (UPDATE {schema}.jobs SET state = 4, attempt = d.attempt, finished_at = now()
              FROM d WHERE jobs.id = d.job_id AND jobs.id = @id)
        INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
        SELECT job_id, now(), @kind_cancelled, attempt, @reason FROM d
        """;

    private const string JobStateTemplate = "SELECT state FROM {schema}.jobs WHERE id = @id";

    private string CancelMarkSql => field ??= Render(CancelMarkTemplate);
    private string CancelNowSql => field ??= Render(CancelNowTemplate);
    private string JobStateSql => field ??= Render(JobStateTemplate);

    public Task<CancelResult> CancelAsync(Guid jobId, string? reason, CancellationToken ct) =>
        RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, c);

            bool found;
            Guid? owner = null;
            await using (var mark = new NpgsqlCommand(CancelMarkSql, conn, tx))
            {
                P.Add(mark.Parameters, "id", jobId);
                await PrepareAsync(mark, c);
                await using var r = await mark.ExecuteReaderAsync(c);
                found = await r.ReadAsync(c);
                if (found)
                    owner = r.GetNullableValue<Guid>(0);
            }

            CancelResult result;
            if (!found)
            {
                await using var state = new NpgsqlCommand(JobStateSql, conn, tx);
                P.Add(state.Parameters, "id", jobId);
                result = await state.ExecuteScalarAsync(c) switch
                {
                    short s when s is 2 or 3 or 4 => CancelResult.AlreadyFinished,
                    _ => CancelResult.NotFound, // нет задачи; Awaiting (1) — часть 5
                };
            }
            else if (owner is null)
            {
                await using var now = new NpgsqlCommand(CancelNowSql, conn, tx);
                P.Add(now.Parameters, "id", jobId);
                P.Add(now.Parameters, "kind_cancelled", (short)JobEventKind.Cancelled);
                P.AddNullable(now.Parameters, "reason", reason, NpgsqlDbType.Text);
                await PrepareAsync(now, c);
                await now.ExecuteNonQueryAsync(c);
                result = CancelResult.Cancelled;
            }
            else
            {
                result = CancelResult.CancellationRequested;
            }

            await tx.CommitAsync(c);
            return result;
        }, ct);
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~CancelTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): two-statement cancel transaction without dequeue race"
```

### Task 18: Trigger и Requeue

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Trigger.cs`, `src/PgBolt.Postgres/Storage/PgBoltStore.Requeue.cs`
- Test: `tests/PgBolt.Postgres.Tests/TriggerRequeueTests.cs`

**Interfaces:**
- Produces:
  - `public Task<string?> TriggerAsync(Guid jobId, CancellationToken ct)` — очередь задачи, если она стала готовой, иначе `null`.
  - `public Task<(RequeueResult Result, string? Queue)> RequeueAsync(Guid jobId, CancellationToken ct)`.
- Отличия SQL от спеки: trigger возвращает `queue` итоговым `SELECT` из CTE (вместо `RETURNING job_id` у вставки события); requeue дополнительно возвращает `(SELECT queue FROM s)` — обе очереди нужны клиенту для wakeup.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/TriggerRequeueTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class TriggerRequeueTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private async Task<Guid> FailedJobAsync(string? uniqueKey = null)
    {
        var id = await EnqueueAsync(maxAttempts: 1, uniqueKey: uniqueKey);
        var job = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Failed, await Store.FailAsync(id, job.LeaseId, ServerA, JobEventKind.Failed, null, null, Ct));
        return id;
    }

    [Fact]
    public async Task Trigger_makes_scheduled_job_ready_now()
    {
        var id = await EnqueueAsync(queue: "mail", delay: TimeSpan.FromHours(1));
        Assert.Equal("mail", await Store.TriggerAsync(id, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND abs(extract(epoch FROM run_at - now())) < 5 FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Triggered }, await EventKindsAsync(id));
        Assert.Single(await Store.DequeueAsync("mail", 1, ServerA, Lease, Ct));
    }

    [Fact]
    public async Task Trigger_on_ready_leased_or_missing_job_returns_null()
    {
        var ready = await EnqueueAsync();
        Assert.Null(await Store.TriggerAsync(ready, Ct));
        await LeaseOneAsync();
        Assert.Null(await Store.TriggerAsync(ready, Ct));
        Assert.Null(await Store.TriggerAsync(Guid.CreateVersion7(), Ct));
        Assert.Empty(await EventKindsAsync(ready));
    }

    [Fact]
    public async Task Requeue_failed_job()
    {
        var id = await FailedJobAsync();
        Assert.Equal((RequeueResult.Requeued, "q"), await Store.RequeueAsync(id, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT ready AND attempt = 0 AND lease_owner IS NULL FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.True(await Db.ScalarAsync<bool>("SELECT state = 0 AND finished_at IS NULL FROM {schema}.jobs WHERE id = @id", ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Failed, (short)JobEventKind.Requeued }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Requeue_cancelled_job()
    {
        var id = await EnqueueAsync();
        await Store.CancelAsync(id, null, Ct);
        Assert.Equal(RequeueResult.Requeued, (await Store.RequeueAsync(id, Ct)).Result);
    }

    [Fact]
    public async Task Requeue_non_terminal_or_unknown()
    {
        var pending = await EnqueueAsync();
        Assert.Equal(RequeueResult.NotTerminal, (await Store.RequeueAsync(pending, Ct)).Result);
        Assert.Equal(RequeueResult.NotFound, (await Store.RequeueAsync(Guid.CreateVersion7(), Ct)).Result);
    }

    [Fact]
    public async Task Requeue_conflicts_with_active_unique_key()
    {
        var failed = await FailedJobAsync(uniqueKey: "k");
        await EnqueueAsync(uniqueKey: "k");
        Assert.Equal(RequeueResult.Conflict, (await Store.RequeueAsync(failed, Ct)).Result);
        Assert.Equal((short)3, await JobStateAsync(failed));
    }

    [Fact]
    public async Task Succeeded_after_requeue_is_not_requeued_again()
    {
        var id = await FailedJobAsync();
        await Store.RequeueAsync(id, Ct);
        var job = await LeaseOneAsync();
        await Store.AckAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Ct);
        Assert.Equal(RequeueResult.NotTerminal, (await Store.RequeueAsync(id, Ct)).Result);
        Assert.False(await InQueueAsync(id));
    }

    [Fact]
    public async Task Concurrent_requeues_requeue_exactly_once()
    {
        for (var i = 0; i < 20; i++)
        {
            var id = await FailedJobAsync();
            var results = await Task.WhenAll(Store.RequeueAsync(id, Ct), Store.RequeueAsync(id, Ct));
            Assert.Single(results, r => r.Result == RequeueResult.Requeued);
            Assert.Single(results, r => r.Result == RequeueResult.NotTerminal);
            await Db.ExecAsync("DELETE FROM {schema}.queue WHERE job_id = @id", ("id", id));
        }
    }
}

public sealed class TriggerRequeueTests_Direct(PostgresFixture pg) : TriggerRequeueTests(pg, ConnectionMode.Direct);
public sealed class TriggerRequeueTests_Bouncer(PostgresFixture pg) : TriggerRequeueTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~TriggerRequeueTests"`
Expected: FAIL — методы не найдены.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Trigger.cs`:

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string TriggerTemplate = """
        WITH t AS (
            UPDATE {schema}.queue SET run_at = now(), ready = true
            WHERE job_id = @id AND lease_owner IS NULL AND NOT ready
            RETURNING job_id, attempt, queue
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, attempt)
            SELECT job_id, now(), @kind_triggered, attempt FROM t
        )
        SELECT queue FROM t
        """;

    private string TriggerSql => field ??= Render(TriggerTemplate);

    /// <summary>Scheduled → Enqueued сейчас (§5.8). Возвращает очередь для wakeup или null.</summary>
    public Task<string?> TriggerAsync(Guid jobId, CancellationToken ct) =>
        QueryAsync(TriggerSql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "kind_triggered", (short)JobEventKind.Triggered);
        }, async (r, c) => await r.ReadAsync(c) ? r.GetString(0) : null, ct);
}
```

`src/PgBolt.Postgres/Storage/PgBoltStore.Requeue.cs`:

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string RequeueTemplate = """
        WITH s AS MATERIALIZED (
            SELECT id, state, queue, priority, max_attempts, unique_key
            FROM {schema}.jobs WHERE id = @id
            FOR UPDATE
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
        SELECT (SELECT state FROM s) AS state, EXISTS (SELECT 1 FROM q) AS requeued, (SELECT queue FROM s) AS queue
        """;

    private string RequeueSql => field ??= Render(RequeueTemplate);

    /// <summary>Failed/Cancelled → Pending (§5.9).</summary>
    public Task<(RequeueResult Result, string? Queue)> RequeueAsync(Guid jobId, CancellationToken ct) =>
        QueryAsync(RequeueSql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "kind_requeued", (short)JobEventKind.Requeued);
        }, async (r, c) =>
        {
            await r.ReadAsync(c);
            var state = r.GetNullableValue<short>(0);
            var requeued = r.GetBoolean(1);
            var queue = r.GetNullable<string>(2);
            var result = requeued ? RequeueResult.Requeued
                : state is null ? RequeueResult.NotFound
                : state is not (3 or 4) ? RequeueResult.NotTerminal
                : RequeueResult.Conflict;
            return (result, queue);
        }, ct);
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~TriggerRequeueTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): trigger scheduled jobs and serialized requeue"
```

### Task 19: Чтение задачи и `job_types`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.GetJob.cs`, `src/PgBolt.Postgres/Storage/PgBoltStore.JobTypes.cs`
- Test: `tests/PgBolt.Postgres.Tests/GetJobTests.cs`, `tests/PgBolt.Postgres.Tests/JobTypesTests.cs`

**Interfaces:**
- Consumes: `UuidV7.EventsLowerBound` (Task 5), `JobInfo`, `JobEventInfo`, `JobState` (Task 2).
- Produces:
  - `public Task<JobInfo?> GetJobAsync(Guid jobId, CancellationToken ct)` — производное состояние по §2; последние 50 событий, новые первыми.
  - `public Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct)` — upsert; возвращает имена, у которых схема аргументов отличается от опубликованной другим сервером, виденным за последние 5 минут (приближение «живого сервера» из §4.5).
  - `public Task<JobTypeRecord?> GetJobTypeAsync(string name, CancellationToken ct)`.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/GetJobTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class GetJobTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Unknown_job_is_null()
    {
        Assert.Null(await Store.GetJobAsync(Guid.CreateVersion7(), Ct));
    }

    [Fact]
    public async Task Derives_scheduled_enqueued_processing()
    {
        var scheduled = await EnqueueAsync(delay: TimeSpan.FromHours(1));
        var enqueued = await EnqueueAsync(args: """{"x":42}""");
        Assert.Equal(JobState.Scheduled, (await Store.GetJobAsync(scheduled, Ct))!.State);

        var info = (await Store.GetJobAsync(enqueued, Ct))!;
        Assert.Equal(JobState.Enqueued, info.State);
        Assert.Equal(42, info.Args.GetProperty("x").GetInt32());
        Assert.Equal("test.job", info.Name);
        Assert.NotNull(info.RunAt);

        var job = await LeaseOneAsync();
        var processing = (await Store.GetJobAsync(job.Id, Ct))!;
        Assert.Equal(JobState.Processing, processing.State);
        Assert.Equal(1, processing.Attempt);
        Assert.Equal(ServerA, processing.ServerId);
        Assert.NotNull(processing.StartedAt);
    }

    [Fact]
    public async Task Terminal_job_reads_jobs_columns_and_events()
    {
        var id = await EnqueueAsync(maxAttempts: 2);
        var first = await LeaseOneAsync();
        await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "oops", "trace", Ct);
        var second = await LeaseOneAsync();
        await Store.AckAsync(ServerA, [new LeaseRef(id, second.LeaseId)], Ct);

        var info = (await Store.GetJobAsync(id, Ct))!;
        Assert.Equal(JobState.Succeeded, info.State);
        Assert.Equal(2, info.Attempt);
        Assert.Null(info.RunAt);
        Assert.NotNull(info.FinishedAt);
        var ev = Assert.Single(info.Events);
        Assert.Equal((JobEventKind.Retry, "oops", "trace"), (ev.Kind, ev.Message, ev.Exception));
    }

    [Fact]
    public async Task Cancellation_request_is_visible()
    {
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        await Store.CancelAsync(id, null, Ct);
        Assert.True((await Store.GetJobAsync(id, Ct))!.CancellationRequested);
    }
}

public sealed class GetJobTests_Direct(PostgresFixture pg) : GetJobTests(pg, ConnectionMode.Direct);
public sealed class GetJobTests_Bouncer(PostgresFixture pg) : GetJobTests(pg, ConnectionMode.Bouncer);
```

`tests/PgBolt.Postgres.Tests/JobTypesTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class JobTypesTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static JobTypeRecord Type(string schema, string queue = "emails") =>
        new("orders.send-receipt", schema, queue, 5, 1, [10, 60]);

    [Fact]
    public async Task Publish_then_get()
    {
        Assert.Empty(await Store.PublishJobTypesAsync([Type("""{"type":"object"}""")], "server-a", Ct));
        var t = await Store.GetJobTypeAsync("orders.send-receipt", Ct);
        Assert.NotNull(t);
        Assert.Equal(("emails", 5, (short)1), (t.DefaultQueue, t.DefaultMaxAttempts, t.DefaultPriority));
        Assert.Equal(new[] { 10, 60 }, t.DefaultRetryDelaysSeconds);
        Assert.Null(await Store.GetJobTypeAsync("missing", Ct));
    }

    [Fact]
    public async Task Different_schema_from_other_live_server_is_reported_and_overwritten()
    {
        await Store.PublishJobTypesAsync([Type("""{"type":"object"}""")], "server-a", Ct);
        var mismatches = await Store.PublishJobTypesAsync([Type("""{"type":"string"}""", "other")], "server-b", Ct);
        Assert.Equal(new[] { "orders.send-receipt" }, mismatches);
        Assert.Equal("other", (await Store.GetJobTypeAsync("orders.send-receipt", Ct))!.DefaultQueue);
    }

    [Fact]
    public async Task Same_server_republish_is_not_a_mismatch()
    {
        await Store.PublishJobTypesAsync([Type("""{"type":"object"}""")], "server-a", Ct);
        Assert.Empty(await Store.PublishJobTypesAsync([Type("""{"type":"string"}""")], "server-a", Ct));
    }
}

public sealed class JobTypesTests_Direct(PostgresFixture pg) : JobTypesTests(pg, ConnectionMode.Direct);
public sealed class JobTypesTests_Bouncer(PostgresFixture pg) : JobTypesTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~GetJobTests|FullyQualifiedName~JobTypesTests"`
Expected: FAIL — методы не найдены.

- [x] **Step 3: Реализовать чтение задачи**

`src/PgBolt.Postgres/Storage/PgBoltStore.GetJob.cs`:

```csharp
using System.Text.Json;
using Npgsql;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string GetJobTemplate = """
        SELECT j.id, j.type, j.queue, j.priority, j.args, j.state, j.attempt, j.max_attempts, j.created_at,
               j.started_at, j.finished_at, j.server_id, j.unique_key,
               q.job_id IS NOT NULL, q.run_at, q.ready, q.lease_owner, q.attempt, q.leased_at, q.cancel_requested
        FROM {schema}.jobs j LEFT JOIN {schema}.queue q ON q.job_id = j.id
        WHERE j.id = @id
        """;

    private const string GetEventsTemplate = """
        SELECT kind, at, server_id, attempt, message, exception
        FROM {schema}.job_events
        WHERE job_id = @id AND at >= @created_from
        ORDER BY at DESC
        LIMIT 50
        """;

    private string GetJobSql => field ??= Render(GetJobTemplate);
    private string GetEventsSql => field ??= Render(GetEventsTemplate);

    public Task<JobInfo?> GetJobAsync(Guid jobId, CancellationToken ct) =>
        RunAsync<JobInfo?>(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands = { new NpgsqlBatchCommand(GetJobSql), new NpgsqlBatchCommand(GetEventsSql) },
            };
            P.Add(batch.BatchCommands[0].Parameters, "id", jobId);
            P.Add(batch.BatchCommands[1].Parameters, "id", jobId);
            P.Add(batch.BatchCommands[1].Parameters, "created_from", UuidV7.EventsLowerBound(jobId));
            if (_prepare)
                await batch.PrepareAsync(c);

            await using var r = await batch.ExecuteReaderAsync(c);
            if (!await r.ReadAsync(c))
                return null;

            var stored = r.GetInt16(5);
            var inQueue = r.GetBoolean(13);
            var ready = inQueue && r.GetBoolean(15);
            var leaseOwner = r.GetNullableValue<Guid>(16);
            var state = Derive(stored, inQueue, ready, leaseOwner);
            using var args = JsonDocument.Parse(r.GetString(4));
            var id = r.GetGuid(0);
            var type = r.GetString(1);
            var queue = r.GetString(2);
            var priority = r.GetInt16(3);
            var attempt = inQueue ? r.GetInt32(17) : r.GetInt32(6);
            var maxAttempts = r.GetInt32(7);
            var createdAt = r.GetFieldValue<DateTimeOffset>(8);
            var startedAt = state == JobState.Processing ? r.GetNullableValue<DateTimeOffset>(18) : r.GetNullableValue<DateTimeOffset>(9);
            var finishedAt = r.GetNullableValue<DateTimeOffset>(10);
            var serverId = state == JobState.Processing ? leaseOwner : r.GetNullableValue<Guid>(11);
            var uniqueKey = r.GetNullable<string>(12);
            var runAt = inQueue ? r.GetFieldValue<DateTimeOffset>(14) : (DateTimeOffset?)null;
            var cancelRequested = inQueue && r.GetBoolean(19);
            var argsElement = args.RootElement.Clone();

            await r.NextResultAsync(c);
            var events = await r.ReadAllAsync(static e => new JobEventInfo(
                (JobEventKind)e.GetInt16(0), e.GetFieldValue<DateTimeOffset>(1), e.GetNullableValue<Guid>(2),
                e.GetNullableValue<int>(3), e.GetNullable<string>(4), e.GetNullable<string>(5)), c);

            return new JobInfo(new JobId(id), type, queue, priority, state, attempt, maxAttempts, createdAt, runAt,
                startedAt, finishedAt, serverId, uniqueKey, cancelRequested, argsElement, events);
        }, ct);

    private static JobState Derive(short stored, bool inQueue, bool ready, Guid? leaseOwner) => stored switch
    {
        0 when leaseOwner is not null => JobState.Processing,
        0 when inQueue && !ready => JobState.Scheduled,
        0 => JobState.Enqueued,
        1 => JobState.Awaiting,
        2 => JobState.Succeeded,
        3 => JobState.Failed,
        4 => JobState.Cancelled,
        _ => throw new InvalidOperationException($"Unknown stored job state {stored}."),
    };
}
```

- [x] **Step 4: Реализовать `job_types`**

`src/PgBolt.Postgres/Storage/PgBoltStore.JobTypes.cs`:

```csharp
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string JobTypeMismatchTemplate = """
        SELECT t.name
        FROM {schema}.job_types t
        JOIN unnest(@names::text[], @schemas::jsonb[]) AS u(name, args_schema) ON u.name = t.name
        WHERE t.published_by <> @published_by
          AND t.last_seen_at > now() - interval '5 minutes'
          AND t.args_schema <> u.args_schema
        """;

    private const string JobTypeUpsertTemplate = """
        INSERT INTO {schema}.job_types (name, args_schema, default_queue, default_max_attempts, default_priority,
                                        default_retry_delays, published_by, last_seen_at)
        VALUES (@name, @args_schema, @default_queue, @default_max_attempts, @default_priority,
                @default_retry_delays, @published_by, now())
        ON CONFLICT (name) DO UPDATE SET
            args_schema = EXCLUDED.args_schema,
            default_queue = EXCLUDED.default_queue,
            default_max_attempts = EXCLUDED.default_max_attempts,
            default_priority = EXCLUDED.default_priority,
            default_retry_delays = EXCLUDED.default_retry_delays,
            published_by = EXCLUDED.published_by,
            last_seen_at = now()
        """;

    private const string GetJobTypeTemplate = """
        SELECT name, args_schema, default_queue, default_max_attempts, default_priority, default_retry_delays
        FROM {schema}.job_types WHERE name = @name
        """;

    private string JobTypeMismatchSql => field ??= Render(JobTypeMismatchTemplate);
    private string JobTypeUpsertSql => field ??= Render(JobTypeUpsertTemplate);
    private string GetJobTypeSql => field ??= Render(GetJobTypeTemplate);

    public Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct)
    {
        if (types.Count == 0)
            return Task.FromResult<IReadOnlyList<string>>([]);

        return RunAsync<IReadOnlyList<string>>(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);

            List<string> mismatches;
            await using (var check = new NpgsqlCommand(JobTypeMismatchSql, conn, tx))
            {
                P.Add(check.Parameters, "names", types.Select(t => t.Name).ToArray());
                P.Add(check.Parameters, "schemas", types.Select(t => t.ArgsSchemaJson).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
                P.Add(check.Parameters, "published_by", publishedBy);
                await using var r = await check.ExecuteReaderAsync(c);
                mismatches = await r.ReadAllAsync(static r => r.GetString(0), c);
            }

            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                foreach (var t in types)
                {
                    var cmd = new NpgsqlBatchCommand(JobTypeUpsertSql);
                    P.Add(cmd.Parameters, "name", t.Name);
                    P.Add(cmd.Parameters, "args_schema", t.ArgsSchemaJson, NpgsqlDbType.Jsonb);
                    P.Add(cmd.Parameters, "default_queue", t.DefaultQueue);
                    P.Add(cmd.Parameters, "default_max_attempts", t.DefaultMaxAttempts);
                    P.Add(cmd.Parameters, "default_priority", t.DefaultPriority);
                    P.AddNullable(cmd.Parameters, "default_retry_delays", t.DefaultRetryDelaysSeconds, NpgsqlDbType.Array | NpgsqlDbType.Integer);
                    P.Add(cmd.Parameters, "published_by", publishedBy);
                    batch.BatchCommands.Add(cmd);
                }
                await batch.ExecuteNonQueryAsync(c);
            }

            await tx.CommitAsync(c);
            return mismatches;
        }, ct);
    }

    public Task<JobTypeRecord?> GetJobTypeAsync(string name, CancellationToken ct) =>
        QueryAsync(GetJobTypeSql, ps => P.Add(ps, "name", name), async (r, c) =>
            await r.ReadAsync(c)
                ? new JobTypeRecord(r.GetString(0), r.GetString(1), r.GetString(2), r.GetInt32(3), r.GetInt16(4), r.GetNullable<int[]>(5))
                : null, ct);
}
```

- [x] **Step 5: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~GetJobTests|FullyQualifiedName~JobTypesTests"`
Expected: PASS.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests
git commit -m "feat(store): job snapshot with derived state; job_types publish and lookup"
```

### Task 20: Инварианты конкурентности горячего пути

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Test: `tests/PgBolt.Postgres.Tests/ConcurrencyInvariantTests.cs`

**Interfaces:**
- Consumes: enqueue / dequeue / ack (Task 9–12). Новый продакшен-код не нужен; падение теста — дефект в соответствующем statement.

- [x] **Step 1: Написать тесты**

`tests/PgBolt.Postgres.Tests/ConcurrencyInvariantTests.cs`:

```csharp
using System.Collections.Concurrent;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ConcurrencyInvariantTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Every_job_is_executed_exactly_once_without_failures()
    {
        const int total = 2000;
        for (var i = 0; i < total; i += 500)
        {
            var ids = Enumerable.Range(0, 500).Select(_ => Guid.CreateVersion7()).ToArray();
            await Store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                ids, ids.Select(_ => "{}").ToArray(), new DateTime?[500], new string?[500]), null, Ct);
        }

        var executions = new ConcurrentBag<Guid>();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
        {
            var server = Guid.NewGuid();
            for (var empty = 0; empty < 3;)
            {
                var jobs = await Store.DequeueAsync("q", 20, server, Lease, Ct);
                if (jobs.Count == 0) { empty++; await Task.Delay(20, Ct); continue; }
                foreach (var j in jobs)
                    executions.Add(j.Id);
                Assert.Equal(jobs.Count, await Store.AckAsync(server, jobs.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToList(), Ct));
            }
        }));

        Assert.Equal(total, executions.Count);
        Assert.Equal(total, executions.Distinct().Count());
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue"));
        Assert.Equal((long)total, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE state = 2"));
    }

    [Fact]
    public async Task Unique_key_race_creates_exactly_one_job()
    {
        var results = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ =>
            Store.EnqueueAsync(Batch(Guid.CreateVersion7(), uniqueKey: "race"), null, Ct)));

        var flat = results.Select(r => r[0]).ToList();
        var winner = Assert.Single(flat, r => r.Created);
        Assert.All(flat, r => Assert.Equal(winner.Id, r.Id));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }
}

public sealed class ConcurrencyInvariantTests_Direct(PostgresFixture pg) : ConcurrencyInvariantTests(pg, ConnectionMode.Direct);
public sealed class ConcurrencyInvariantTests_Bouncer(PostgresFixture pg) : ConcurrencyInvariantTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Прогнать весь набор хранилища**

Run: `dotnet test tests/PgBolt.Postgres.Tests`
Expected: PASS — все тесты фаз 2–3 в обоих режимах.

- [x] **Step 3: Commit**

```bash
git add tests/PgBolt.Postgres.Tests/ConcurrencyInvariantTests.cs
git commit -m "test(store): exactly-once execution and unique_key race invariants"
```

---
## Фаза 4. Registry исполнителей, клиент, DI

### Task 21: `JobRegistry` и `IPgBoltJobsBuilder`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Jobs/JobRegistry.cs`, `src/PgBolt.Postgres/Jobs/JobsBuilder.cs`
- Test: `tests/PgBolt.Postgres.Tests/JobRegistryTests.cs`

**Interfaces:**
- Consumes: `IPgBoltJobsBuilder`, `JobDescriptor`, `JobMetadataRegistry` (Task 2–3), `JobTypeRecord` (Task 8).
- Produces (namespace `PgBolt.Postgres.Jobs`):
  - `public delegate Task JobInvoker(IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, JobContext context, CancellationToken ct)` — сигнатура из §7. Воркер (часть 3) получает `LeasedJob.ArgsJson` строкой и передаёт `Encoding.UTF8.GetBytes(...)`.
  - `public sealed record JobExecutorRegistration(JobDescriptor Descriptor, Type ArgsType, Type JobType, JsonTypeInfo ArgsTypeInfo, JobInvoker Invoke)`
  - `public sealed class JobRegistry { IReadOnlyCollection<JobExecutorRegistration> All; bool TryGet(string name, out JobExecutorRegistration registration); IReadOnlyList<JobTypeRecord> ToJobTypeRecords(); internal bool TryAdd(JobExecutorRegistration r); }`
  - `internal sealed class JobsBuilder(IServiceCollection services, JobRegistry registry) : IPgBoltJobsBuilder`

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/JobRegistryTests.cs`:

```csharp
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres.Jobs;

namespace PgBolt.Postgres.Tests;

public sealed record RegistryArgs(string Text, int Count);

public sealed class RegistryProbe
{
    public List<RegistryArgs> Seen { get; } = [];
}

public sealed class RegistryJob(RegistryProbe probe) : IJob<RegistryArgs>
{
    public Task ExecuteAsync(RegistryArgs args, JobContext context, CancellationToken cancellationToken)
    {
        probe.Seen.Add(args);
        return Task.CompletedTask;
    }
}

public sealed class OtherRegistryJob : IJob<RegistryArgs>
{
    public Task ExecuteAsync(RegistryArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

[JsonSerializable(typeof(RegistryArgs))]
internal sealed partial class RegistryJson : JsonSerializerContext;

public sealed class JobRegistryTests
{
    private static readonly JobDescriptor Descriptor = new("registry.job", "reg", 4, 1, [3]);

    private static (ServiceCollection Services, JobRegistry Registry, JobsBuilder Builder) Create()
    {
        var services = new ServiceCollection();
        var registry = new JobRegistry();
        return (services, registry, new JobsBuilder(services, registry));
    }

    [Fact]
    public async Task Registered_invoker_deserializes_and_runs_scoped_job()
    {
        var (services, registry, builder) = Create();
        services.AddSingleton<RegistryProbe>();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        await using var sp = services.BuildServiceProvider();

        Assert.True(registry.TryGet("registry.job", out var reg));
        Assert.Equal((typeof(RegistryArgs), typeof(RegistryJob)), (reg.ArgsType, reg.JobType));
        await using var scope = sp.CreateAsyncScope();
        var ctx = new JobContext(JobId.New(), "registry.job", 1, 4, null, null, NullLogger.Instance);
        await reg.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes("""{"Text":"hi","Count":2}"""), ctx, CancellationToken.None);

        Assert.Equal(new[] { new RegistryArgs("hi", 2) }, sp.GetRequiredService<RegistryProbe>().Seen);
        Assert.Contains(services, d => d.ServiceType == typeof(RegistryJob) && d.Lifetime == ServiceLifetime.Scoped);
    }

    [Fact]
    public void Repeated_registration_is_idempotent()
    {
        var (services, registry, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        Assert.Single(registry.All);
        Assert.Single(services, d => d.ServiceType == typeof(RegistryJob));
    }

    [Fact]
    public void Conflicting_executor_for_same_name_throws()
    {
        var (_, _, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        Assert.Throws<InvalidOperationException>(() =>
            builder.AddExecutor<RegistryArgs, OtherRegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs));
    }

    [Fact]
    public void Registration_fills_job_metadata()
    {
        var (_, _, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        Assert.True(JobMetadata<RegistryArgs>.Descriptor!.SameAs(Descriptor));
        Assert.NotNull(JobMetadata<RegistryArgs>.TypeInfo);
    }

    [Fact]
    public void Job_type_records_carry_json_schema_and_defaults()
    {
        var (_, registry, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);

        var record = Assert.Single(registry.ToJobTypeRecords());
        Assert.Equal(("registry.job", "reg", 4, (short)1), (record.Name, record.DefaultQueue, record.DefaultMaxAttempts, record.DefaultPriority));
        Assert.Equal(new[] { 3 }, record.DefaultRetryDelaysSeconds);
        using var schema = JsonDocument.Parse(record.ArgsSchemaJson);
        Assert.True(schema.RootElement.GetProperty("properties").TryGetProperty("Text", out _));
    }

    [Fact]
    public async Task Null_arguments_throw_json_exception()
    {
        var (services, registry, builder) = Create();
        services.AddSingleton<RegistryProbe>();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        await using var sp = services.BuildServiceProvider();
        registry.TryGet("registry.job", out var reg);
        var ctx = new JobContext(JobId.New(), "registry.job", 1, 4, null, null, NullLogger.Instance);
        await Assert.ThrowsAsync<JsonException>(() => reg!.Invoke(sp, "null"u8.ToArray(), ctx, CancellationToken.None));
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~JobRegistryTests"`
Expected: FAIL — `JobRegistry` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Jobs/JobRegistry.cs`:

```csharp
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Schema;
using System.Text.Json.Serialization.Metadata;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Jobs;

public delegate Task JobInvoker(IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, JobContext context, CancellationToken ct);

public sealed record JobExecutorRegistration(
    JobDescriptor Descriptor, Type ArgsType, Type JobType, JsonTypeInfo ArgsTypeInfo, JobInvoker Invoke);

/// <summary>Исполнители, зарегистрированные сгенерированными PgBoltJobRegistry.Register (§7).</summary>
public sealed class JobRegistry
{
    private readonly Dictionary<string, JobExecutorRegistration> _byName = new(StringComparer.Ordinal);

    public IReadOnlyCollection<JobExecutorRegistration> All => _byName.Values;

    public bool TryGet(string name, [MaybeNullWhen(false)] out JobExecutorRegistration registration) =>
        _byName.TryGetValue(name, out registration);

    /// <summary>Записи для job_types: схема аргументов строится JsonSchemaExporter из JsonTypeInfo (§7).</summary>
    public IReadOnlyList<JobTypeRecord> ToJobTypeRecords() =>
        _byName.Values
            .OrderBy(r => r.Descriptor.Name, StringComparer.Ordinal)
            .Select(r => new JobTypeRecord(
                r.Descriptor.Name,
                JsonSchemaExporter.GetJsonSchemaAsNode(r.ArgsTypeInfo).ToJsonString(),
                r.Descriptor.Queue,
                r.Descriptor.MaxAttempts,
                r.Descriptor.Priority,
                r.Descriptor.RetryDelaysSeconds))
            .ToList();

    /// <returns>true — добавлено; false — такая же регистрация уже есть.</returns>
    internal bool TryAdd(JobExecutorRegistration registration)
    {
        if (!_byName.TryGetValue(registration.Descriptor.Name, out var existing))
        {
            _byName.Add(registration.Descriptor.Name, registration);
            return true;
        }
        if (existing.ArgsType == registration.ArgsType && existing.JobType == registration.JobType
            && existing.Descriptor.SameAs(registration.Descriptor))
            return false;
        throw new InvalidOperationException(
            $"Job '{registration.Descriptor.Name}' is already registered with executor {existing.JobType.FullName}; " +
            $"cannot register {registration.JobType.FullName}.");
    }
}
```

`src/PgBolt.Postgres/Jobs/JobsBuilder.cs`:

```csharp
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace PgBolt.Postgres.Jobs;

internal sealed class JobsBuilder(IServiceCollection services, JobRegistry registry) : IPgBoltJobsBuilder
{
    public IPgBoltJobsBuilder AddExecutor<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TJob>(
        JobDescriptor descriptor, JsonTypeInfo<TArgs> argsTypeInfo)
        where TJob : class, IJob<TArgs>
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(argsTypeInfo);

        if (registry.TryAdd(new JobExecutorRegistration(descriptor, typeof(TArgs), typeof(TJob), argsTypeInfo, Invoke)))
            services.TryAddScoped<TJob>();
        JobMetadataRegistry.Register(descriptor, argsTypeInfo);
        return this;

        Task Invoke(IServiceProvider sp, ReadOnlyMemory<byte> args, JobContext context, CancellationToken ct)
        {
            var value = JsonSerializer.Deserialize(args.Span, argsTypeInfo)
                        ?? throw new JsonException($"Arguments of job '{descriptor.Name}' deserialized to null.");
            return sp.GetRequiredService<TJob>().ExecuteAsync(value, context, ct);
        }
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~JobRegistryTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Jobs tests/PgBolt.Postgres.Tests/JobRegistryTests.cs
git commit -m "feat(postgres): executor registry with AOT-safe invokers and JSON schema export"
```

### Task 22: Wakeup-сигнал и кэш `job_types`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Client/IWakeupSignal.cs`, `src/PgBolt.Postgres/Client/InProcessWakeupSignal.cs`, `src/PgBolt.Postgres/Client/JobTypesCache.cs`
- Test: `tests/PgBolt.Postgres.Tests/WakeupSignalTests.cs`, `tests/PgBolt.Postgres.Tests/JobTypesCacheTests.cs`

**Interfaces:**
- Produces (namespace `PgBolt.Postgres.Client`):
  - `public interface IWakeupSignal { void Signal(string queue); Task<bool> WaitAsync(IReadOnlyCollection<string> queues, TimeSpan timeout, CancellationToken ct); }` — сигнал теряется, если в момент `Signal` никто не ждёт; воркер (часть 3) всегда страхуется polling'ом.
  - `public sealed class InProcessWakeupSignal : IWakeupSignal`
  - `internal sealed class JobTypesCache(PgBoltStore store, TimeSpan ttl, TimeProvider time) { ValueTask<JobTypeRecord?> GetAsync(string name, CancellationToken ct); }` — кэширует и отрицательный результат на тот же TTL.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/WakeupSignalTests.cs`:

```csharp
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Tests;

public sealed class WakeupSignalTests
{
    [Fact]
    public async Task Signal_wakes_waiter_of_that_queue()
    {
        var signal = new InProcessWakeupSignal();
        var wait = signal.WaitAsync(["a", "b"], TimeSpan.FromSeconds(10), CancellationToken.None);
        signal.Signal("b");
        Assert.True(await wait.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Other_queue_does_not_wake_and_timeout_returns_false()
    {
        var signal = new InProcessWakeupSignal();
        var wait = signal.WaitAsync(["a"], TimeSpan.FromMilliseconds(200), CancellationToken.None);
        signal.Signal("z");
        Assert.False(await wait);
    }

    [Fact]
    public async Task Cancellation_is_propagated()
    {
        var signal = new InProcessWakeupSignal();
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => signal.WaitAsync(["a"], TimeSpan.FromSeconds(10), cts.Token));
    }
}
```

`tests/PgBolt.Postgres.Tests/JobTypesCacheTests.cs`:

```csharp
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Только Direct: проверяется логика TTL-кэша; SQL job_types покрыт в обоих режимах в JobTypesTests.</summary>
public sealed class JobTypesCacheTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    [Fact]
    public async Task Caches_for_ttl_then_refreshes()
    {
        var time = new FakeTimeProvider();
        var cache = new JobTypesCache(Store, TimeSpan.FromSeconds(30), time);
        await Store.PublishJobTypesAsync([new JobTypeRecord("x.job", "{}", "q1", 3, 0, null)], "s", Ct);

        Assert.Equal("q1", (await cache.GetAsync("x.job", Ct))!.DefaultQueue);
        await Store.PublishJobTypesAsync([new JobTypeRecord("x.job", "{}", "q2", 3, 0, null)], "s", Ct);
        Assert.Equal("q1", (await cache.GetAsync("x.job", Ct))!.DefaultQueue);

        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal("q2", (await cache.GetAsync("x.job", Ct))!.DefaultQueue);
    }

    [Fact]
    public async Task Missing_type_is_null()
    {
        var cache = new JobTypesCache(Store, TimeSpan.FromSeconds(30), TimeProvider.System);
        Assert.Null(await cache.GetAsync("nope", Ct));
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WakeupSignalTests|FullyQualifiedName~JobTypesCacheTests"`
Expected: FAIL — типы не найдены.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Client/IWakeupSignal.cs`:

```csharp
namespace PgBolt.Postgres.Client;

/// <summary>Пробуждение воркеров после enqueue/trigger/promote (§5.10). Сигнал не гарантирован — воркер страхуется polling'ом.</summary>
public interface IWakeupSignal
{
    void Signal(string queue);

    /// <returns>true — пришёл сигнал по одной из очередей; false — истёк timeout.</returns>
    Task<bool> WaitAsync(IReadOnlyCollection<string> queues, TimeSpan timeout, CancellationToken ct);
}
```

`src/PgBolt.Postgres/Client/InProcessWakeupSignal.cs`:

```csharp
using System.Collections.Concurrent;

namespace PgBolt.Postgres.Client;

public sealed class InProcessWakeupSignal : IWakeupSignal
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _waiters = new(StringComparer.Ordinal);

    public void Signal(string queue)
    {
        if (_waiters.TryRemove(queue, out var tcs))
            tcs.TrySetResult();
    }

    public async Task<bool> WaitAsync(IReadOnlyCollection<string> queues, TimeSpan timeout, CancellationToken ct)
    {
        if (queues.Count == 0) // Task.WhenAny на пустом массиве бросает ArgumentException
        {
            await Task.Delay(timeout, ct);
            return false;
        }
        var tasks = queues
            .Select(q => _waiters.GetOrAdd(q, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task)
            .ToArray();
        try
        {
            await Task.WhenAny(tasks).WaitAsync(timeout, ct);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }
}
```

`src/PgBolt.Postgres/Client/JobTypesCache.cs`:

```csharp
using System.Collections.Concurrent;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client;

/// <summary>Умолчания динамического enqueue из job_types с TTL (§6.2).</summary>
internal sealed class JobTypesCache(PgBoltStore store, TimeSpan ttl, TimeProvider time)
{
    private readonly ConcurrentDictionary<string, (JobTypeRecord? Record, DateTimeOffset ExpiresAt)> _entries = new(StringComparer.Ordinal);

    public async ValueTask<JobTypeRecord?> GetAsync(string name, CancellationToken ct)
    {
        var now = time.GetUtcNow();
        if (_entries.TryGetValue(name, out var entry) && now < entry.ExpiresAt)
            return entry.Record;
        var record = await store.GetJobTypeAsync(name, ct);
        _entries[name] = (record, now + ttl);
        return record;
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WakeupSignalTests|FullyQualifiedName~JobTypesCacheTests"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Client tests/PgBolt.Postgres.Tests
git commit -m "feat(client): in-process wakeup signal and job_types cache"
```

### Task 23: `PgBoltJobClient` — enqueue

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/Client/EnqueueOptionsResolver.cs`, `src/PgBolt.Postgres/Client/PgBoltJobClient.cs`
- Test: `tests/PgBolt.Postgres.Tests/ClientEnqueueTests.cs`, `tests/PgBolt.Postgres.Tests/ClientTestSupport.cs`

**Interfaces:**
- Consumes: `PgBoltStore.EnqueueAsync` (Task 9), `JobTypesCache`, `IWakeupSignal` (Task 22), `JobMetadata<TArgs>` (Task 3).
- Produces:
  - `internal readonly record struct ResolvedEnqueue(string Queue, short Priority, int MaxAttempts, int[]? RetryDelaysSeconds, DateTime? RunAtUtc, TimeSpan Delay, NpgsqlTransaction? Transaction)`
  - `internal static class EnqueueOptionsResolver { ResolvedEnqueue Resolve(JobDescriptor descriptor, EnqueueOptions? options); }`
  - `internal sealed partial class PgBoltJobClient(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup) : IJobClient` — в этой задаче реализуются три метода enqueue; методы управления (Task 24) временно бросают `NotImplementedException` и заменяются в Task 24.

- [x] **Step 1: Написать общие тестовые типы**

`tests/PgBolt.Postgres.Tests/ClientTestSupport.cs`:

```csharp
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Text.Json.Serialization;
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Tests;

public sealed record ClientArgs(int Order);
public sealed record UnregisteredArgs(int X);

[JsonSerializable(typeof(ClientArgs))]
internal sealed partial class ClientJson : JsonSerializerContext;

internal static class ClientMetadata
{
    public static readonly JobDescriptor Descriptor = new("client.test", "client", 4, 2, [1, 5]);

    /// <summary>Имитирует сгенерированный module initializer (Task 26).</summary>
    public static void EnsureRegistered() => JobMetadataRegistry.Register(Descriptor, ClientJson.Default.ClientArgs);
}

public sealed class RecordingWakeup : IWakeupSignal
{
    public ConcurrentQueue<string> Signals { get; } = new();
    public void Signal(string queue) => Signals.Enqueue(queue);
    public Task<bool> WaitAsync(IReadOnlyCollection<string> queues, TimeSpan timeout, CancellationToken ct) => Task.FromResult(false);
}

public sealed class FakeTransaction : DbTransaction
{
    public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
    protected override DbConnection? DbConnection => null;
    public override void Commit() { }
    public override void Rollback() { }
}
```

- [x] **Step 2: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/ClientEnqueueTests.cs`:

```csharp
using System.Data;
using System.Text.Json;
using Npgsql;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ClientEnqueueTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    protected RecordingWakeup Wakeup { get; } = new();
    protected IJobClient Client { get; private set; } = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        Client = new PgBoltJobClient(Store, new JobTypesCache(Store, TimeSpan.FromSeconds(30), TimeProvider.System), Wakeup);
    }

    [Fact]
    public async Task Typed_enqueue_uses_job_attribute_defaults()
    {
        var r = await Client.EnqueueAsync(new ClientArgs(7), ct: Ct);

        Assert.True(r.Created);
        var id = r.Id!.Value.Value;
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.queue = 'client' AND q.priority = 2 AND q.max_attempts = 4 AND j.retry_delays = ARRAY[1,5]
                   AND j.type = 'client.test' AND j.args = '{"Order":7}'::jsonb
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", id)));
        Assert.Equal(new[] { "client" }, Wakeup.Signals);
    }

    [Fact]
    public async Task Options_override_defaults()
    {
        var r = await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions
        {
            Queue = "other", Priority = 9, MaxAttempts = 1, RetryDelays = [TimeSpan.FromSeconds(1.5)],
        }, Ct);
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.queue = 'other' AND q.priority = 9 AND q.max_attempts = 1 AND j.retry_delays = ARRAY[2]
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", r.Id!.Value.Value)));
    }

    [Fact]
    public async Task Delay_and_run_at_are_mutually_exclusive()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await Client.EnqueueAsync(new ClientArgs(1),
            new EnqueueOptions { Delay = TimeSpan.FromSeconds(1), RunAt = DateTimeOffset.UtcNow }, Ct));
    }

    [Fact]
    public async Task Delay_creates_scheduled_job()
    {
        var r = await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Delay = TimeSpan.FromMinutes(5) }, Ct);
        Assert.Equal(JobState.Scheduled, (await Store.GetJobAsync(r.Id!.Value.Value, Ct))!.State);
    }

    [Fact]
    public async Task Unique_key_returns_existing_job()
    {
        var first = await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { UniqueKey = "u" }, Ct);
        var second = await Client.EnqueueAsync(new ClientArgs(2), new EnqueueOptions { UniqueKey = "u" }, Ct);
        Assert.Equal(new EnqueueResult(first.Id, false), second);
    }

    [Fact]
    public async Task EnqueueMany_with_key_selector()
    {
        var results = await Client.EnqueueManyAsync([new ClientArgs(1), new ClientArgs(2), new ClientArgs(1)],
            uniqueKeySelector: a => $"order:{a.Order}", ct: Ct);
        Assert.Equal(2, results.Count(r => r.Created));
        Assert.Equal(results[0].Id, results[2].Id);
        Assert.Empty(await Client.EnqueueManyAsync(Array.Empty<ClientArgs>(), ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await Client.EnqueueManyAsync([new ClientArgs(1)], new EnqueueOptions { UniqueKey = "x" }, ct: Ct));
    }

    [Fact]
    public async Task Type_without_metadata_throws()
    {
        var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Client.EnqueueAsync(new UnregisteredArgs(1), ct: Ct));
        Assert.Contains(nameof(UnregisteredArgs), e.Message);
    }

    [Fact]
    public async Task Dynamic_enqueue_uses_published_defaults()
    {
        await Store.PublishJobTypesAsync([new JobTypeRecord("dyn.job", "{}", "dyn", 6, 3, [7])], "s", Ct);
        using var args = JsonDocument.Parse("""{"a":1}""");

        var r = await Client.EnqueueAsync("dyn.job", args.RootElement, ct: Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.queue = 'dyn' AND q.priority = 3 AND q.max_attempts = 6 AND j.retry_delays = ARRAY[7] AND j.args = '{"a":1}'::jsonb
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", r.Id!.Value.Value)));
        await Assert.ThrowsAsync<UnknownJobException>(async () => await Client.EnqueueAsync("missing.job", args.RootElement, ct: Ct));
    }

    [Fact]
    public async Task Transactional_enqueue_commits_with_user_transaction_and_skips_wakeup()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using (var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, Ct))
        {
            var r = await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Transaction = tx }, Ct);
            Assert.False(await InQueueAsync(r.Id!.Value.Value));
            await tx.CommitAsync(Ct);
            Assert.True(await InQueueAsync(r.Id!.Value.Value));
        }
        Assert.Empty(Wakeup.Signals);
    }

    [Fact]
    public async Task Transaction_without_explicit_level_is_accepted()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        Assert.True((await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Transaction = tx }, Ct)).Created);
        await tx.CommitAsync(Ct);
    }

    [Fact]
    public async Task Non_read_committed_or_foreign_transactions_are_rejected()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using (var tx = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead, Ct))
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Transaction = tx }, Ct));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Transaction = new FakeTransaction() }, Ct));
    }

    /// <summary>
    /// §11 «Изоляция»: уровень, поднятый SET TRANSACTION внутри транзакции. NpgsqlTransaction.IsolationLevel
    /// остаётся ReadCommitted, поэтому клиент обязан спросить сервер. Команда транзакционная — работает и через PgBouncer.
    /// </summary>
    [Fact]
    public async Task Isolation_raised_by_set_transaction_is_rejected()
    {
        await using var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await using (var set = new NpgsqlCommand("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", conn, tx))
            await set.ExecuteNonQueryAsync(Ct);
        Assert.Equal(IsolationLevel.ReadCommitted, tx.IsolationLevel);
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Transaction = tx }, Ct));
    }
}

public sealed class ClientEnqueueTests_Direct(PostgresFixture pg) : ClientEnqueueTests(pg, ConnectionMode.Direct);

public sealed class ClientEnqueueTests_Bouncer(PostgresFixture pg) : ClientEnqueueTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~ClientEnqueueTests"`
Expected: FAIL — `PgBoltJobClient` не найден.

- [x] **Step 4: Реализовать резолвер опций**

`src/PgBolt.Postgres/Client/EnqueueOptionsResolver.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres.Client;

internal readonly record struct ResolvedEnqueue(
    string Queue, short Priority, int MaxAttempts, int[]? RetryDelaysSeconds,
    DateTime? RunAtUtc, TimeSpan Delay, NpgsqlTransaction? Transaction);

/// <summary>[Job]-умолчания + EnqueueOptions (§6.2): поле опций, если задано, переопределяет умолчание.</summary>
internal static class EnqueueOptionsResolver
{
    public static ResolvedEnqueue Resolve(JobDescriptor descriptor, EnqueueOptions? options)
    {
        if (options is null)
            return new ResolvedEnqueue(descriptor.Queue, descriptor.Priority, descriptor.MaxAttempts,
                descriptor.RetryDelaysSeconds, null, TimeSpan.Zero, null);

        if (options.RunAt is not null && options.Delay is not null)
            throw new ArgumentException("EnqueueOptions.RunAt and EnqueueOptions.Delay are mutually exclusive.", nameof(options));
        if (options.Delay < TimeSpan.Zero)
            throw new ArgumentException("EnqueueOptions.Delay must be non-negative.", nameof(options));
        if (options.MaxAttempts < 1)
            throw new ArgumentException("EnqueueOptions.MaxAttempts must be >= 1.", nameof(options));
        if (options.Queue is { Length: 0 })
            throw new ArgumentException("EnqueueOptions.Queue must not be empty.", nameof(options));

        var transaction = options.Transaction switch
        {
            null => null,
            NpgsqlTransaction t => t,
            _ => throw new ArgumentException("EnqueueOptions.Transaction must be an NpgsqlTransaction.", nameof(options)),
        };

        return new ResolvedEnqueue(
            options.Queue ?? descriptor.Queue,
            options.Priority ?? descriptor.Priority,
            options.MaxAttempts ?? descriptor.MaxAttempts,
            options.RetryDelays is null ? descriptor.RetryDelaysSeconds : options.RetryDelays.Select(ToSeconds).ToArray(),
            options.RunAt?.UtcDateTime,
            options.Delay ?? TimeSpan.Zero,
            transaction);
    }

    private static int ToSeconds(TimeSpan delay) =>
        delay < TimeSpan.Zero
            ? throw new ArgumentException("Retry delays must be non-negative.")
            : checked((int)Math.Ceiling(delay.TotalSeconds));
}
```

- [x] **Step 5: Реализовать enqueue-часть клиента**

`src/PgBolt.Postgres/Client/PgBoltJobClient.cs`:

```csharp
using System.Data;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Npgsql;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Client;

internal sealed partial class PgBoltJobClient(PgBoltStore store, JobTypesCache jobTypes, IWakeupSignal wakeup) : IJobClient
{
    public async ValueTask<EnqueueResult> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, CancellationToken ct = default)
    {
        var (descriptor, typeInfo) = Metadata<TArgs>();
        var results = await EnqueueCoreAsync(descriptor, [JsonSerializer.Serialize(args, typeInfo)], [options?.UniqueKey], options, ct);
        return results[0];
    }

    public async ValueTask<IReadOnlyList<EnqueueResult>> EnqueueManyAsync<TArgs>(
        IReadOnlyList<TArgs> args, EnqueueOptions? options = null,
        Func<TArgs, string?>? uniqueKeySelector = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (options?.UniqueKey is not null)
            throw new ArgumentException("EnqueueOptions.UniqueKey is not allowed in EnqueueManyAsync; use uniqueKeySelector.", nameof(options));
        if (args.Count == 0)
            return [];

        var (descriptor, typeInfo) = Metadata<TArgs>();
        var json = new string[args.Count];
        var keys = new string?[args.Count];
        for (var i = 0; i < args.Count; i++)
        {
            json[i] = JsonSerializer.Serialize(args[i], typeInfo);
            keys[i] = uniqueKeySelector?.Invoke(args[i]);
        }
        return await EnqueueCoreAsync(descriptor, json, keys, options, ct);
    }

    public async ValueTask<EnqueueResult> EnqueueAsync(string jobName, JsonElement args, EnqueueOptions? options = null, CancellationToken ct = default)
    {
        var type = await jobTypes.GetAsync(jobName, ct) ?? throw new UnknownJobException(jobName);
        var descriptor = new JobDescriptor(type.Name, type.DefaultQueue, type.DefaultMaxAttempts, type.DefaultPriority, type.DefaultRetryDelaysSeconds);
        var results = await EnqueueCoreAsync(descriptor, [args.GetRawText()], [options?.UniqueKey], options, ct);
        return results[0];
    }

    private async ValueTask<IReadOnlyList<EnqueueResult>> EnqueueCoreAsync(
        JobDescriptor descriptor, string[] argsJson, string?[] uniqueKeys, EnqueueOptions? options, CancellationToken ct)
    {
        var o = EnqueueOptionsResolver.Resolve(descriptor, options);
        if (o.Transaction is not null)
            await EnsureReadCommittedAsync(o.Transaction, ct);

        var ids = new Guid[argsJson.Length];
        for (var i = 0; i < ids.Length; i++)
            ids[i] = Guid.CreateVersion7();
        var runAt = new DateTime?[argsJson.Length];
        if (o.RunAtUtc is { } at)
            Array.Fill(runAt, at);

        var batch = new EnqueueBatch(descriptor.Name, o.Queue, o.Priority, o.MaxAttempts, o.RetryDelaysSeconds,
            CurrentTraceParent(), o.Delay, ids, argsJson, runAt, uniqueKeys);
        var rows = await store.EnqueueAsync(batch, o.Transaction, ct);

        if (o.Transaction is null && rows.Any(r => r.Created))
            wakeup.Signal(o.Queue);

        var results = new EnqueueResult[rows.Count];
        for (var i = 0; i < rows.Count; i++)
            results[i] = new EnqueueResult(rows[i].Id is { } id ? new JobId(id) : null, rows[i].Created);
        return results;
    }

    private static (JobDescriptor Descriptor, JsonTypeInfo<TArgs> TypeInfo) Metadata<TArgs>() =>
        JobMetadata<TArgs>.Descriptor is { } d && JobMetadata<TArgs>.TypeInfo is { } ti
            ? (d, ti)
            : throw new InvalidOperationException(
                $"No PgBolt metadata for '{typeof(TArgs).FullName}'. Mark the type with [Job] and use it from an assembly " +
                "that references the PgBolt source generator and declares [assembly: PgBoltJsonContext(...)] including this type.");

    private static string? CurrentTraceParent() =>
        Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity ? activity.Id : null;

    /// <summary>
    /// §5.1: только READ COMMITTED. Явный другой уровень отклоняется сразу. Иначе уровень всегда читается с сервера
    /// без кэша: Npgsql сообщает ReadCommitted и для BeginTransaction() без уровня, а SET TRANSACTION ISOLATION LEVEL
    /// внутри транзакции меняет уровень незаметно для NpgsqlTransaction.IsolationLevel.
    /// </summary>
    private static async Task EnsureReadCommittedAsync(NpgsqlTransaction tx, CancellationToken ct)
    {
        if (tx.IsolationLevel is not (IsolationLevel.ReadCommitted or IsolationLevel.Unspecified))
            throw new NotSupportedException($"PgBolt transactional enqueue requires READ COMMITTED isolation, got {tx.IsolationLevel}.");

        await using var cmd = new NpgsqlCommand("SELECT current_setting('transaction_isolation')", tx.Connection, tx);
        if (await cmd.ExecuteScalarAsync(ct) is not "read committed")
            throw new NotSupportedException("PgBolt transactional enqueue requires READ COMMITTED isolation.");
    }

    public ValueTask<CancelResult> CancelAsync(JobId id, string? reason = null, CancellationToken ct = default) => throw new NotImplementedException();
    public ValueTask<bool> TriggerAsync(JobId id, CancellationToken ct = default) => throw new NotImplementedException();
    public ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default) => throw new NotImplementedException();
    public ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default) => throw new NotImplementedException();
}
```

- [x] **Step 6: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~ClientEnqueueTests"`
Expected: PASS.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Client tests/PgBolt.Postgres.Tests
git commit -m "feat(client): typed, bulk and dynamic enqueue with option overrides"
```

### Task 24: `PgBoltJobClient` — cancel, trigger, requeue, get

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Modify: `src/PgBolt.Postgres/Client/PgBoltJobClient.cs` (заменить четыре заглушки)
- Test: `tests/PgBolt.Postgres.Tests/ClientManagementTests.cs`

**Interfaces:**
- Consumes: `PgBoltStore.CancelAsync/TriggerAsync/RequeueAsync/GetJobAsync` (Task 17–19).
- Produces: полная реализация `IJobClient`. Trigger и успешный requeue подают wakeup-сигнал в очередь задачи.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/ClientManagementTests.cs`:

```csharp
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ClientManagementTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private readonly RecordingWakeup _wakeup = new();
    private IJobClient _client = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        _client = new PgBoltJobClient(Store, new JobTypesCache(Store, TimeSpan.FromSeconds(30), TimeProvider.System), _wakeup);
    }

    [Fact]
    public async Task Cancel_delegates_to_store()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(1), ct: Ct);
        Assert.Equal(CancelResult.Cancelled, await _client.CancelAsync(r.Id!.Value, "why", Ct));
        Assert.Equal(CancelResult.AlreadyFinished, await _client.CancelAsync(r.Id!.Value, null, Ct));
    }

    [Fact]
    public async Task Trigger_signals_wakeup()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Delay = TimeSpan.FromHours(1) }, Ct);
        _wakeup.Signals.Clear();
        Assert.True(await _client.TriggerAsync(r.Id!.Value, Ct));
        Assert.Equal(new[] { "client" }, _wakeup.Signals);
        Assert.False(await _client.TriggerAsync(r.Id!.Value, Ct));
    }

    [Fact]
    public async Task Requeue_signals_wakeup_on_success()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { MaxAttempts = 1 }, Ct);
        var job = Assert.Single(await Store.DequeueAsync("client", 1, ServerA, Lease, Ct));
        await Store.FailAsync(job.Id, job.LeaseId, ServerA, JobEventKind.Failed, null, null, Ct);
        _wakeup.Signals.Clear();

        Assert.Equal(RequeueResult.Requeued, await _client.RequeueAsync(r.Id!.Value, Ct));
        Assert.Equal(new[] { "client" }, _wakeup.Signals);
        Assert.Equal(RequeueResult.NotTerminal, await _client.RequeueAsync(r.Id!.Value, Ct));
    }

    [Fact]
    public async Task Get_returns_snapshot()
    {
        var r = await _client.EnqueueAsync(new ClientArgs(5), ct: Ct);
        var info = await _client.GetAsync(r.Id!.Value, Ct);
        Assert.NotNull(info);
        Assert.Equal((JobState.Enqueued, "client.test", 5), (info.State, info.Name, info.Args.GetProperty("Order").GetInt32()));
        Assert.Null(await _client.GetAsync(JobId.New(), Ct));
    }
}

public sealed class ClientManagementTests_Direct(PostgresFixture pg) : ClientManagementTests(pg, ConnectionMode.Direct);
public sealed class ClientManagementTests_Bouncer(PostgresFixture pg) : ClientManagementTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~ClientManagementTests"`
Expected: FAIL — `NotImplementedException`.

- [x] **Step 3: Заменить заглушки**

В `src/PgBolt.Postgres/Client/PgBoltJobClient.cs` заменить четыре метода с `NotImplementedException` на:

```csharp
    public async ValueTask<CancelResult> CancelAsync(JobId id, string? reason = null, CancellationToken ct = default) =>
        await store.CancelAsync(id.Value, reason, ct);

    public async ValueTask<bool> TriggerAsync(JobId id, CancellationToken ct = default)
    {
        var queue = await store.TriggerAsync(id.Value, ct);
        if (queue is null)
            return false;
        wakeup.Signal(queue);
        return true;
    }

    public async ValueTask<RequeueResult> RequeueAsync(JobId id, CancellationToken ct = default)
    {
        var (result, queue) = await store.RequeueAsync(id.Value, ct);
        if (result == RequeueResult.Requeued && queue is not null)
            wakeup.Signal(queue);
        return result;
    }

    public async ValueTask<JobInfo?> GetAsync(JobId id, CancellationToken ct = default) =>
        await store.GetJobAsync(id.Value, ct);
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~Client"`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Client tests/PgBolt.Postgres.Tests
git commit -m "feat(client): cancel, trigger, requeue and snapshot"
```

### Task 25: DI, инициализация на старте, `IPgBoltMaintenance`

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Postgres/PgBoltDataSourceFactory.cs`, `src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs`, `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs`, `src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`
- Test: `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Consumes: всё из фаз 2–4.
- Produces:
  - `public static IServiceCollection AddPgBolt(this IServiceCollection services, Action<PgBoltOptions> configure)` (namespace `Microsoft.Extensions.DependencyInjection`) — регистрирует `PgBoltOptions`, `JobRegistry`, `PgBoltStore`, `MigrationRunner`, `PartitionManager`, `IPgBoltMaintenance`, `IWakeupSignal` (`TryAdd`, можно заменить до вызова), `IJobClient`, `PgBoltInitializer` (+ как `IHostedService`). Повторный вызов — `InvalidOperationException`.
  - `public interface IPgBoltMaintenance { Task<PartitionEnsureResult> EnsurePartitionsAsync(int daysAhead = 7, CancellationToken ct = default); }` (namespace `PgBolt.Postgres.Hosting`)
  - `public sealed class PgBoltInitializer : IHostedService` — `StartAsync`: проверка `current_setting('transaction_isolation') = 'read committed'` на новом соединении (§1; иначе `InvalidOperationException` до миграций), миграции (или проверка версии при `AutoMigrate = false`), затем партиции; при таймауте и отсутствии партиции на сегодня — исключение, при отсутствии только будущих — фоновый повтор с backoff 1 с → 60 с; `StopAsync` отменяет повтор, дожидается его и освобождает `CancellationTokenSource`. `internal PgBoltInitializer(PgBoltOptions, NpgsqlDataSource, MigrationRunner, PartitionManager, ILogger<PgBoltInitializer>)`; `internal Task? BackgroundRetry` для тестов.
  - `internal static class PgBoltDataSourceFactory { NpgsqlDataSource Create(string connectionString, bool usePreparedStatements); }` — `NpgsqlSlimDataSourceBuilder` + `EnableArrays()` + `EnableTransportSecurity()` (TLS к продакшен-БД); при `usePreparedStatements == false` строка подключения дополняется `Max Auto Prepare=0` и `No Reset On Close=true` (§1). Минимальный набор проверяется в Task 32.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Postgres.Tests/HostingTests.cs`:

```csharp
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed record HostingArgs(int N);

public sealed class HostingJob : IJob<HostingArgs>
{
    public Task ExecuteAsync(HostingArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

[JsonSerializable(typeof(HostingArgs))]
internal sealed partial class HostingJson : JsonSerializerContext;

public abstract class HostingTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private ServiceProvider Build(Action<PgBoltOptions>? extra = null)
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            extra?.Invoke(o);
        });
        return services.BuildServiceProvider();
    }

    private Task<long> PartitionCountAsync() =>
        _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs"));

    [Fact]
    public async Task Start_migrates_creates_partitions_and_client_works()
    {
        await using var sp = Build(o => o.AddJobs(b => b.AddExecutor<HostingArgs, HostingJob>(
            new JobDescriptor("hosting.job", "h", 3, 0, null), HostingJson.Default.HostingArgs)));
        foreach (var hosted in sp.GetServices<IHostedService>())
            await hosted.StartAsync(Ct);

        Assert.Equal(8L, await PartitionCountAsync()); // сегодня + 7
        Assert.True(sp.GetRequiredService<JobRegistry>().TryGet("hosting.job", out _));
        var r = await sp.GetRequiredService<IJobClient>().EnqueueAsync(new HostingArgs(1), ct: Ct);
        Assert.True(r.Created);
    }

    [Fact]
    public async Task AutoMigrate_off_on_unmigrated_schema_fails_start()
    {
        await using var sp = Build(o => o.AutoMigrate = false);
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));
        Assert.Contains("schema version", e.Message);
    }

    [Fact]
    public async Task User_data_source_is_not_disposed_with_container()
    {
        var ds = NpgsqlDataSource.Create(pg.ConnectionString(mode));
        var services = new ServiceCollection();
        services.AddPgBolt(o => { o.DataSource = ds; o.Schema = _db.Schema; });
        await using (var sp = services.BuildServiceProvider())
            await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

        await using var cmd = ds.CreateCommand("SELECT 1");
        Assert.Equal(1, (int)(await cmd.ExecuteScalarAsync(Ct))!);
        await ds.DisposeAsync();
    }

    [Fact]
    public void Second_AddPgBolt_throws()
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o => o.ConnectionString = "Host=x");
        Assert.Throws<InvalidOperationException>(() => services.AddPgBolt(o => o.ConnectionString = "Host=x"));
    }

    [Fact]
    public async Task Missing_future_partitions_are_retried_in_background()
    {
        await using var sp = Build(o => { o.PartitionLockTimeout = TimeSpan.FromMilliseconds(200); o.PartitionDaysAhead = 3; });
        await sp.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(0, Ct); // миграция + сегодня
        var initializer = sp.GetRequiredService<PgBoltInitializer>();

        await using (var holder = await _db.Admin.OpenConnectionAsync(Ct))
        {
            await using (var lk = new NpgsqlCommand(_db.Sql("BEGIN; LOCK TABLE {schema}.jobs IN SHARE UPDATE EXCLUSIVE MODE"), holder))
                await lk.ExecuteNonQueryAsync(Ct);
            await initializer.StartAsync(Ct);
            Assert.NotNull(initializer.BackgroundRetry);
        }

        await initializer.BackgroundRetry!.WaitAsync(TimeSpan.FromSeconds(15), Ct);
        Assert.Equal(4L, await PartitionCountAsync());
        await initializer.StopAsync(Ct);
    }

    [Fact]
    public async Task Missing_today_partition_under_lock_fails_start()
    {
        await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).MigrateAsync(Ct);
        await using var sp = Build(o => o.PartitionLockTimeout = TimeSpan.FromMilliseconds(200));
        await using var holder = await _db.Admin.OpenConnectionAsync(Ct);
        await using (var lk = new NpgsqlCommand(_db.Sql("BEGIN; LOCK TABLE {schema}.jobs IN SHARE UPDATE EXCLUSIVE MODE"), holder))
            await lk.ExecuteNonQueryAsync(Ct);

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));
        Assert.Contains("partition", e.Message);
    }
}

public sealed class HostingTests_Direct(PostgresFixture pg) : HostingTests(pg, ConnectionMode.Direct);
public sealed class HostingTests_Bouncer(PostgresFixture pg) : HostingTests(pg, ConnectionMode.Bouncer);

/// <summary>Только Direct: PgBouncer не пропускает стартовый параметр options.</summary>
public sealed class HostingIsolationTests(PostgresFixture pg) : IAsyncLifetime
{
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, ConnectionMode.Direct, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Non_read_committed_default_isolation_fails_start_before_migrations()
    {
        var cs = new NpgsqlConnectionStringBuilder(pg.DirectConnectionString) { Options = "-c default_transaction_isolation=serializable" };
        var services = new ServiceCollection();
        services.AddPgBolt(o => { o.ConnectionString = cs.ConnectionString; o.Schema = _db.Schema; });
        await using var sp = services.BuildServiceProvider();

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct));
        Assert.Contains("default_transaction_isolation", e.Message);
        Assert.Contains("serializable", e.Message);
        Assert.Null(await new PgBolt.Postgres.Schema.MigrationRunner(_db.Admin, _db.Schema).GetCurrentVersionAsync(Ct));
    }
}
```

`IPgBoltMaintenance.EnsurePartitionsAsync` в `Missing_future_partitions_are_retried_in_background` вызывается до миграции схемы — поэтому реализация `PgBoltMaintenance` сначала вызывает `MigrationRunner.MigrateAsync`, если `AutoMigrate = true` и версия схемы не последняя. Это же поведение нужно core-only развёртываниям, вызывающим обслуживание по расписанию.

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~HostingTests|FullyQualifiedName~HostingIsolationTests"`
Expected: FAIL при компиляции — первая ошибка CS0234: пространство имён `PgBolt.Postgres.Hosting` не существует (`AddPgBolt` тоже ещё не определён).

- [x] **Step 3: Реализовать фабрику источника данных и обслуживание**

`src/PgBolt.Postgres/PgBoltDataSourceFactory.cs`:

```csharp
using Npgsql;

namespace PgBolt.Postgres;

/// <summary>
/// AOT-совместимый источник данных для ConnectionString (§1: slim builder + EnableArrays). Без UsePreparedStatements
/// выставляет Max Auto Prepare=0 и No Reset On Close=true (§1), чтобы строка подключения работала и через PgBouncer.
/// </summary>
internal static class PgBoltDataSourceFactory
{
    public static NpgsqlDataSource Create(string connectionString, bool usePreparedStatements)
    {
        var csb = new NpgsqlConnectionStringBuilder(connectionString);
        if (!usePreparedStatements)
        {
            csb.MaxAutoPrepare = 0;
            csb.NoResetOnClose = true;
        }
        var builder = new NpgsqlSlimDataSourceBuilder(csb.ConnectionString);
        builder.EnableArrays();
        builder.EnableTransportSecurity();
        return builder.Build();
    }
}

internal sealed class PgBoltDataSource(NpgsqlDataSource value, bool owned) : IAsyncDisposable
{
    public NpgsqlDataSource Value { get; } = value;

    public ValueTask DisposeAsync() => owned ? Value.DisposeAsync() : ValueTask.CompletedTask;
}
```

`src/PgBolt.Postgres/Hosting/IPgBoltMaintenance.cs`:

```csharp
using PgBolt.Postgres.Schema;

namespace PgBolt.Postgres.Hosting;

/// <summary>Обслуживание схемы (§4.4). Core-only развёртывание вызывает EnsurePartitionsAsync периодически (раз в сутки при daysAhead ≥ 2).</summary>
public interface IPgBoltMaintenance
{
    Task<PartitionEnsureResult> EnsurePartitionsAsync(int daysAhead = 7, CancellationToken ct = default);
}

internal sealed class PgBoltMaintenance(PgBoltOptions options, MigrationRunner migrations, PartitionManager partitions) : IPgBoltMaintenance
{
    public async Task<PartitionEnsureResult> EnsurePartitionsAsync(int daysAhead = 7, CancellationToken ct = default)
    {
        if (options.AutoMigrate && await migrations.GetCurrentVersionAsync(ct) != PgBoltMigrations.LatestVersion)
            await migrations.MigrateAsync(ct);
        return await partitions.EnsureAsync(daysAhead, ct);
    }
}
```

- [x] **Step 4: Реализовать инициализатор**

`src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs`:

```csharp
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Postgres.Schema;

namespace PgBolt.Postgres.Hosting;

/// <summary>Проверка изоляции, миграции и партиции на старте (§1, §4.4, §4.7).</summary>
public sealed class PgBoltInitializer : IHostedService
{
    private readonly PgBoltOptions _options;
    private readonly NpgsqlDataSource _dataSource;
    private readonly MigrationRunner _migrations;
    private readonly PartitionManager _partitions;
    private readonly ILogger<PgBoltInitializer> _logger;
    private CancellationTokenSource? _stop;

    internal PgBoltInitializer(PgBoltOptions options, NpgsqlDataSource dataSource, MigrationRunner migrations, PartitionManager partitions,
        ILogger<PgBoltInitializer> logger)
    {
        _options = options;
        _dataSource = dataSource;
        _migrations = migrations;
        _partitions = partitions;
        _logger = logger;
    }

    internal Task? BackgroundRetry { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await EnsureReadCommittedDefaultAsync(cancellationToken);

        if (_options.AutoMigrate)
        {
            await _migrations.MigrateAsync(cancellationToken);
        }
        else
        {
            var version = await _migrations.GetCurrentVersionAsync(cancellationToken);
            if (version != PgBoltMigrations.LatestVersion)
                throw new InvalidOperationException(
                    $"PgBolt schema version in '{_options.Schema}' is {version?.ToString() ?? "absent"}, expected {PgBoltMigrations.LatestVersion}. " +
                    "Apply PgBoltMigrations.GetScript(...) or enable AutoMigrate.");
        }

        var result = await _partitions.EnsureAsync(_options.PartitionDaysAhead, cancellationToken);
        if (!result.TimedOut)
            return;
        if (!result.TodayPresent)
            throw new InvalidOperationException(
                $"PgBolt could not create today's partitions in '{_options.Schema}' within {_options.PartitionLockTimeout} (lock_timeout). " +
                "Enqueue is impossible without them; retry the start when long-running DDL/locks on jobs are gone.");

        _logger.LogWarning("PgBolt: future partitions were not created (lock_timeout); retrying in background.");
        _stop = new CancellationTokenSource();
        BackgroundRetry = RetryAsync(_stop.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stop is null || BackgroundRetry is null)
            return;
        await _stop.CancelAsync();
        try
        {
            await BackgroundRetry.WaitAsync(cancellationToken);
        }
        finally
        {
            // RetryAsync не бросает, поэтому после ожидания задача завершена. Если отменили само ожидание, задача ещё
            // читает токен — CTS не освобождаем (без таймера и регистраций это безопасно).
            if (BackgroundRetry.IsCompleted)
            {
                _stop.Dispose();
                _stop = null;
            }
        }
    }

    /// <summary>
    /// §1: statement'ы горячего пути вне пользовательской транзакции выполняются на уровне изоляции по умолчанию,
    /// а их SQL рассчитан на READ COMMITTED. Новое соединение показывает default_transaction_isolation сервера/БД/роли.
    /// </summary>
    private async Task EnsureReadCommittedDefaultAsync(CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT current_setting('transaction_isolation')", conn);
        var level = (string?)await cmd.ExecuteScalarAsync(ct);
        if (level != "read committed")
            throw new InvalidOperationException(
                $"PgBolt requires default_transaction_isolation = 'read committed' for its database/role, but a new connection runs at '{level}'. " +
                "Reset it with ALTER DATABASE/ALTER ROLE ... SET default_transaction_isolation = 'read committed' (or remove it from the connection options).");
    }

    private async Task RetryAsync(CancellationToken ct)
    {
        var delay = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(delay, ct);
                if (!(await _partitions.EnsureAsync(_options.PartitionDaysAhead, ct)).TimedOut)
                    return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "PgBolt: background partition creation failed.");
            }
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromMinutes(1).Ticks));
        }
    }
}
```

- [x] **Step 5: Реализовать `AddPgBolt`**

`src/PgBolt.Postgres/Hosting/PgBoltServiceCollectionExtensions.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;

namespace Microsoft.Extensions.DependencyInjection;

public static class PgBoltServiceCollectionExtensions
{
    public static IServiceCollection AddPgBolt(this IServiceCollection services, Action<PgBoltOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(d => d.ServiceType == typeof(PgBoltOptions)))
            throw new InvalidOperationException("AddPgBolt has already been called for this service collection.");

        var options = new PgBoltOptions();
        configure(options);
        options.Validate();

        var registry = new JobRegistry();
        var builder = new JobsBuilder(services, registry);
        foreach (var register in options.JobRegistrations)
            register(builder);

        services.AddSingleton(options);
        services.AddSingleton(registry);
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton(_ => options.DataSource is { } ds
            ? new PgBoltDataSource(ds, owned: false)
            : new PgBoltDataSource(PgBoltDataSourceFactory.Create(options.ConnectionString!, options.UsePreparedStatements), owned: true));
        services.AddSingleton(sp => new PgBoltStore(sp.GetRequiredService<PgBoltDataSource>().Value, options.Schema, options.UsePreparedStatements));
        services.AddSingleton(sp => new MigrationRunner(sp.GetRequiredService<PgBoltDataSource>().Value, options.Schema));
        services.AddSingleton(sp => new PartitionManager(sp.GetRequiredService<PgBoltDataSource>().Value, options.Schema,
            options.JobsFillFactor, options.PartitionLockTimeout));
        services.AddSingleton<IPgBoltMaintenance>(sp => new PgBoltMaintenance(options,
            sp.GetRequiredService<MigrationRunner>(), sp.GetRequiredService<PartitionManager>()));
        services.TryAddSingleton<IWakeupSignal>(_ => new InProcessWakeupSignal());
        services.AddSingleton(sp => new JobTypesCache(sp.GetRequiredService<PgBoltStore>(), options.JobTypesCacheTtl, sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IJobClient>(sp => new PgBoltJobClient(sp.GetRequiredService<PgBoltStore>(),
            sp.GetRequiredService<JobTypesCache>(), sp.GetRequiredService<IWakeupSignal>()));
        services.AddSingleton(sp => new PgBoltInitializer(options, sp.GetRequiredService<PgBoltDataSource>().Value,
            sp.GetRequiredService<MigrationRunner>(), sp.GetRequiredService<PartitionManager>(),
            sp.GetService<ILogger<PgBoltInitializer>>() ?? NullLogger<PgBoltInitializer>.Instance));
        services.AddHostedService(sp => sp.GetRequiredService<PgBoltInitializer>());
        return services;
    }
}
```

- [x] **Step 6: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests`
Expected: PASS — весь набор. `dotnet build src/PgBolt.Postgres -warnaserror` без предупреждений AOT/trim.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres tests/PgBolt.Postgres.Tests
git commit -m "feat(hosting): AddPgBolt, startup migrations/partitions with background retry, maintenance API"
```

---
## Фаза 5. Source generator и анализаторы

### Task 26: Генератор — метаданные `[Job]`-типов (module initializer)

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Generators/IsExternalInit.cs`, `KnownNames.cs`, `EquatableArray.cs`, `Models/JobTypeModel.cs`, `Models/ExecutorModel.cs`, `Models/EnqueueCallModel.cs`, `Models/JsonContextModel.cs`, `JobGenerator.cs`, `Emitter.cs`
- Create: `tests/PgBolt.Generators.Tests/ModuleInit.cs`, `tests/PgBolt.Generators.Tests/GeneratorHarness.cs`
- Test: `tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs`

**Interfaces:**
- Consumes: имена типов из `PgBolt.Abstractions` (Task 2–3): `PgBolt.JobAttribute`, `PgBolt.IJob<TArgs>`, `PgBolt.IJobClient`, `PgBolt.PgBoltJsonContextAttribute`, `PgBolt.JobMetadataRegistry.Register<TArgs>(JobDescriptor, JsonTypeInfo<TArgs>?)`.
- Produces:
  - Файл `PgBolt.Metadata.g.cs`: `internal static class PgBolt.Generated.PgBoltMetadataInitializer` с `[ModuleInitializer] Initialize()`, вызывающим `JobMetadataRegistry.Register<T>(descriptor, typeInfo)` для каждого `[Job]`-типа, который объявлен в компиляции, является аргументом исполнителя или используется в `EnqueueAsync<T>`/`EnqueueManyAsync<T>` (включая типы из referenced-сборок). `typeInfo` = `(JsonTypeInfo<T>)Ctx.Default.GetTypeInfo(typeof(T))!` (`!` — иначе CS8600 в сгенерированном коде), если тип есть в `[assembly: PgBoltJsonContext(typeof(Ctx))]`, иначе `null`.
  - Модели генератора (используются Task 27–29): `JobTypeModel(string TypeFqn, string JobName, string Queue, int MaxAttempts, short Priority, EquatableArray<int>? RetryDelays)` с `static JobTypeModel? FromType(ITypeSymbol)`; `ExecutorModel(string JobFqn, JobTypeModel Args)`; `JsonContextModel(string? ContextFqn, EquatableArray<string> Types)` с `bool Contains(string fqn)`; `KnownNames.IsClientMethod(IMethodSymbol, INamedTypeSymbol)`.
  - Тестовый хелпер `GeneratorHarness.Run(string source, string? rootNamespace = "Test.Jobs", MetadataReference? library = null)` → `(GeneratorDriver Driver, Compilation Output)`; `GeneratorHarness.Library(string source, string assemblyName)` → `MetadataReference` (referenced-сборка без генератора); `GeneratorHarness.AssertClean(Compilation)` — в итоговой компиляции нет диагностик уровня `Warning` и выше.

- [x] **Step 1: Написать тестовую инфраструктуру и падающие тесты**

`tests/PgBolt.Generators.Tests/ModuleInit.cs`:

```csharp
using System.Runtime.CompilerServices;

namespace PgBolt.Generators.Tests;

public static class ModuleInit
{
    [ModuleInitializer]
    public static void Init() => VerifySourceGenerators.Initialize();
}
```

`tests/PgBolt.Generators.Tests/GeneratorHarness.cs`:

```csharp
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace PgBolt.Generators.Tests;

internal static class GeneratorHarness
{
    private static readonly ImmutableArray<MetadataReference> References =
        ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))
            .ToImmutableArray();

    private static readonly CSharpParseOptions Parse = new(LanguageVersion.Latest);

    /// <summary>Сборка-«библиотека» без генератора: её [Job]-типы видны тестируемой компиляции только как referenced.</summary>
    public static MetadataReference Library(string source, string assemblyName) =>
        CSharpCompilation.Create(assemblyName, [CSharpSyntaxTree.ParseText(source, Parse)], References,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)).ToMetadataReference();

    public static (GeneratorDriver Driver, Compilation Output) Run(string source, string? rootNamespace = "Test.Jobs", MetadataReference? library = null)
    {
        var compilation = CSharpCompilation.Create("Test.Jobs",
            [CSharpSyntaxTree.ParseText(source, Parse)], library is null ? References : References.Add(library),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new JobGenerator().AsSourceGenerator()], parseOptions: Parse, optionsProvider: new OptionsProvider(rootNamespace));
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);
        return (driver, output);
    }

    /// <summary>Итоговая компиляция (исходник + сгенерированный код) без ошибок и предупреждений.</summary>
    public static void AssertClean(Compilation output) =>
        Assert.Empty(output.GetDiagnostics(TestContext.Current.CancellationToken).Where(d => d.Severity >= DiagnosticSeverity.Warning));

    public static string Generated(GeneratorDriver driver, string hintName) =>
        driver.GetRunResult().Results.Single().GeneratedSources.Single(s => s.HintName == hintName).SourceText.ToString();

    public static IReadOnlyList<string> HintNames(GeneratorDriver driver) =>
        driver.GetRunResult().Results.Single().GeneratedSources.Select(s => s.HintName).ToList();

    private sealed class OptionsProvider(string? rootNamespace) : AnalyzerConfigOptionsProvider
    {
        public override AnalyzerConfigOptions GlobalOptions { get; } = new Options(rootNamespace is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string> { ["build_property.RootNamespace"] = rootNamespace });

        public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => Options.Empty;
        public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => Options.Empty;
    }

    private sealed class Options(Dictionary<string, string> values) : AnalyzerConfigOptions
    {
        public static readonly Options Empty = new([]);

        public override bool TryGetValue(string key, [NotNullWhen(true)] out string? value) => values.TryGetValue(key, out value);
    }
}
```

`tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs`:

```csharp
using static VerifyXunit.Verifier;

namespace PgBolt.Generators.Tests;

public sealed class MetadataGenerationTests
{
    /// <summary>Рукописный контекст вместо STJ-генератора: сгенерированный код должен компилироваться без ошибок.</summary>
    internal const string HandWrittenContext = """
        public sealed class AppJson : System.Text.Json.Serialization.JsonSerializerContext
        {
            public AppJson() : base(null) { }
            public static AppJson Default { get; } = new();
            protected override System.Text.Json.JsonSerializerOptions? GeneratedSerializerOptions => null;
            public override System.Text.Json.Serialization.Metadata.JsonTypeInfo? GetTypeInfo(System.Type type) => null;
        }
        """;

    [Fact]
    public Task Contracts_only_assembly_registers_descriptors_without_type_info()
    {
        var (driver, output) = GeneratorHarness.Run("""
            namespace Orders.Contracts;

            [PgBolt.Job("orders.send-receipt", Queue = "emails", MaxAttempts = 5, Priority = -1, RetryDelaysSeconds = new[] { 10, 60 })]
            public sealed record SendReceiptArgs(long OrderId, string Email);
            """);

        Assert.Equal(new[] { "PgBolt.Metadata.g.cs" }, GeneratorHarness.HintNames(driver));
        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.SendReceiptArgs>(new global::PgBolt.JobDescriptor("orders.send-receipt", "emails", 5, (short)-1, new int[] { 10, 60 }), null);""", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public Task Enqueue_call_site_registers_type_info_from_context()
    {
        var (driver, output) = GeneratorHarness.Run($$"""
            using System.Threading.Tasks;
            using PgBolt;

            [assembly: PgBoltJsonContext(typeof(AppJson))]

            [Job("app.ping")]
            public sealed record PingArgs(string Message);

            public static class Sender
            {
                public static async Task SendAsync(IJobClient client) => await client.EnqueueAsync(new PingArgs("hi"));
            }

            [System.Text.Json.Serialization.JsonSerializable(typeof(PingArgs))]
            {{HandWrittenContext}}
            """);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("(global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::PingArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::PingArgs))!", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public void Call_sites_register_types_from_referenced_assemblies()
    {
        var contracts = GeneratorHarness.Library("""
            namespace Contracts;

            [PgBolt.Job("a.job")] public sealed record AArgs(int X);
            [PgBolt.Job("b.job")] public sealed record BArgs(int X);
            [PgBolt.Job("c.job")] public sealed record CArgs(int X);
            """, "Contracts");

        var (driver, output) = GeneratorHarness.Run("""
            using System.Threading.Tasks;
            using Contracts;
            using PgBolt;

            public static class Sender
            {
                public static async Task SendAsync(IJobClient client, IJobClient? maybe)
                {
                    await client.EnqueueAsync(new AArgs(1));
                    if (maybe?.EnqueueAsync(new BArgs(1)) is { } pending) // x?.EnqueueAsync — MemberBindingExpressionSyntax
                        await pending;
                }
            }
            """, library: contracts);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("Register<global::Contracts.AArgs>", code);
        Assert.Contains("Register<global::Contracts.BArgs>", code);
        Assert.DoesNotContain("CArgs", code);
        GeneratorHarness.AssertClean(output);
    }

    [Fact]
    public void Type_parameter_call_sites_are_ignored()
    {
        var (driver, _) = GeneratorHarness.Run("""
            using System.Threading.Tasks;
            using PgBolt;

            public static class Sender
            {
                public static async Task SendAsync<T>(IJobClient client, T args) => await client.EnqueueAsync(args);
            }
            """);

        Assert.Empty(GeneratorHarness.HintNames(driver));
    }
}
```

- [x] **Step 2: Убедиться, что тесты не компилируются**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: FAIL — `JobGenerator` не найден.

- [x] **Step 3: Реализовать инфраструктуру генератора**

`src/PgBolt.Generators/IsExternalInit.cs`:

```csharp
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit
    {
    }
}
```

`src/PgBolt.Generators/KnownNames.cs`:

```csharp
using System.Linq;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal static class KnownNames
    {
        public const string JobAttribute = "PgBolt.JobAttribute";
        public const string JobInterfaceMetadata = "PgBolt.IJob`1";
        public const string JobClientMetadata = "PgBolt.IJobClient";
        public const string JsonContextAttribute = "PgBolt.PgBoltJsonContextAttribute";
        public const string JsonSerializableAttribute = "System.Text.Json.Serialization.JsonSerializableAttribute";

        public static readonly SymbolDisplayFormat Fq = SymbolDisplayFormat.FullyQualifiedFormat;

        public static bool IsClientMethod(IMethodSymbol method, INamedTypeSymbol client) =>
            method.Name is "EnqueueAsync" or "EnqueueManyAsync"
            && method.IsGenericMethod
            && (SymbolEqualityComparer.Default.Equals(method.ContainingType, client)
                || method.ContainingType.AllInterfaces.Contains(client, SymbolEqualityComparer.Default));

        /// <summary>Имя для Compilation.GetTypeByMetadataName: Ns.Outer+Inner`1.</summary>
        public static string MetadataName(INamedTypeSymbol type)
        {
            var name = type.MetadataName;
            for (var t = type.ContainingType; t is not null; t = t.ContainingType)
                name = t.MetadataName + "+" + name;
            var ns = type.ContainingNamespace;
            return ns is null || ns.IsGlobalNamespace ? name : ns.ToDisplayString() + "." + name;
        }
    }
}
```

`src/PgBolt.Generators/EquatableArray.cs`:

```csharp
using System;
using System.Collections;
using System.Collections.Generic;

namespace PgBolt.Generators
{
    /// <summary>Массив со структурным равенством — нужен для кэширования инкрементального генератора.</summary>
    internal readonly struct EquatableArray<T> : IEquatable<EquatableArray<T>>, IEnumerable<T> where T : IEquatable<T>
    {
        private readonly T[]? _items;

        public EquatableArray(T[] items) => _items = items;

        public int Count => _items?.Length ?? 0;

        public bool Equals(EquatableArray<T> other)
        {
            var a = _items ?? Array.Empty<T>();
            var b = other._items ?? Array.Empty<T>();
            if (a.Length != b.Length)
                return false;
            for (var i = 0; i < a.Length; i++)
                if (!a[i].Equals(b[i]))
                    return false;
            return true;
        }

        public override bool Equals(object? obj) => obj is EquatableArray<T> other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                foreach (var item in _items ?? Array.Empty<T>())
                    hash = hash * 31 + item.GetHashCode();
                return hash;
            }
        }

        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)(_items ?? Array.Empty<T>())).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
```

- [x] **Step 4: Реализовать модели**

`src/PgBolt.Generators/Models/JobTypeModel.cs`:

```csharp
using System.Linq;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal sealed record JobTypeModel(string TypeFqn, string JobName, string Queue, int MaxAttempts, short Priority, EquatableArray<int>? RetryDelays)
    {
        public static JobTypeModel? FromType(ITypeSymbol type)
        {
            foreach (var attr in type.GetAttributes())
            {
                if (attr.AttributeClass?.ToDisplayString() != KnownNames.JobAttribute)
                    continue;
                if (attr.ConstructorArguments.Length != 1 || attr.ConstructorArguments[0].Value is not string name)
                    return null;

                var queue = "default";
                var maxAttempts = 10;
                short priority = 0;
                EquatableArray<int>? delays = null;
                foreach (var arg in attr.NamedArguments)
                {
                    switch (arg.Key)
                    {
                        case "Queue" when arg.Value.Value is string q:
                            queue = q;
                            break;
                        case "MaxAttempts" when arg.Value.Value is int m:
                            maxAttempts = m;
                            break;
                        case "Priority" when arg.Value.Value is short p:
                            priority = p;
                            break;
                        case "RetryDelaysSeconds" when !arg.Value.IsNull:
                            delays = new EquatableArray<int>(arg.Value.Values.Select(v => (int)v.Value!).ToArray());
                            break;
                    }
                }
                return new JobTypeModel(type.ToDisplayString(KnownNames.Fq), name, queue, maxAttempts, priority, delays);
            }
            return null;
        }
    }
}
```

`src/PgBolt.Generators/Models/ExecutorModel.cs`:

```csharp
using System.Threading;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal sealed record ExecutorModel(string JobFqn, JobTypeModel Args)
    {
        public static ExecutorModel? From(GeneratorSyntaxContext ctx, CancellationToken ct)
        {
            if (ctx.SemanticModel.GetDeclaredSymbol(ctx.Node, ct) is not INamedTypeSymbol { TypeKind: TypeKind.Class, IsAbstract: false, IsGenericType: false } type)
                return null;
            var jobInterface = ctx.SemanticModel.Compilation.GetTypeByMetadataName(KnownNames.JobInterfaceMetadata);
            if (jobInterface is null)
                return null;
            foreach (var i in type.AllInterfaces)
                if (SymbolEqualityComparer.Default.Equals(i.OriginalDefinition, jobInterface) && JobTypeModel.FromType(i.TypeArguments[0]) is { } args)
                    return new ExecutorModel(type.ToDisplayString(KnownNames.Fq), args);
            return null;
        }
    }
}
```

`src/PgBolt.Generators/Models/EnqueueCallModel.cs`:

```csharp
using System.Threading;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal static class EnqueueCallModel
    {
        public static JobTypeModel? From(GeneratorSyntaxContext ctx, CancellationToken ct)
        {
            if (ctx.SemanticModel.GetSymbolInfo(ctx.Node, ct).Symbol is not IMethodSymbol method)
                return null;
            var client = ctx.SemanticModel.Compilation.GetTypeByMetadataName(KnownNames.JobClientMetadata);
            if (client is null || !KnownNames.IsClientMethod(method, client))
                return null;
            return method.TypeArguments[0] is INamedTypeSymbol args ? JobTypeModel.FromType(args) : null;
        }
    }
}
```

`src/PgBolt.Generators/Models/JsonContextModel.cs`:

```csharp
using System;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal sealed record JsonContextModel(string? ContextFqn, EquatableArray<string> Types)
    {
        public bool Contains(string typeFqn) => Types.Contains(typeFqn);

        public static JsonContextModel From(Compilation compilation)
        {
            foreach (var attr in compilation.Assembly.GetAttributes())
            {
                if (attr.AttributeClass?.ToDisplayString() != KnownNames.JsonContextAttribute
                    || attr.ConstructorArguments.Length != 1
                    || attr.ConstructorArguments[0].Value is not INamedTypeSymbol context)
                    continue;
                var types = context.GetAttributes()
                    .Where(a => a.AttributeClass?.ToDisplayString() == KnownNames.JsonSerializableAttribute && a.ConstructorArguments.Length > 0)
                    .Select(a => a.ConstructorArguments[0].Value)
                    .OfType<ITypeSymbol>()
                    .Select(t => t.ToDisplayString(KnownNames.Fq))
                    .OrderBy(t => t, StringComparer.Ordinal)
                    .ToArray();
                return new JsonContextModel(context.ToDisplayString(KnownNames.Fq), new EquatableArray<string>(types));
            }
            return new JsonContextModel(null, new EquatableArray<string>(Array.Empty<string>()));
        }
    }
}
```

- [x] **Step 5: Реализовать генератор и эмиттер метаданных**

`src/PgBolt.Generators/JobGenerator.cs`:

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace PgBolt.Generators
{
    [Generator(LanguageNames.CSharp)]
    public sealed class JobGenerator : IIncrementalGenerator
    {
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            var declared = context.SyntaxProvider.ForAttributeWithMetadataName(KnownNames.JobAttribute,
                    static (node, _) => node is TypeDeclarationSyntax,
                    static (ctx, _) => JobTypeModel.FromType((ITypeSymbol)ctx.TargetSymbol))
                .Where(static m => m is not null).Select(static (m, _) => m!).Collect();

            var executors = context.SyntaxProvider.CreateSyntaxProvider(
                    static (node, _) => node is ClassDeclarationSyntax { BaseList: not null } or RecordDeclarationSyntax { BaseList: not null },
                    static (ctx, ct) => ExecutorModel.From(ctx, ct))
                .Where(static m => m is not null).Select(static (m, _) => m!).Collect();

            var calls = context.SyntaxProvider.CreateSyntaxProvider(
                    static (node, _) => node is InvocationExpressionSyntax
                    {
                        Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "EnqueueAsync" or "EnqueueManyAsync" }
                            or MemberBindingExpressionSyntax { Name.Identifier.ValueText: "EnqueueAsync" or "EnqueueManyAsync" },
                    },
                    static (ctx, ct) => EnqueueCallModel.From(ctx, ct))
                .Where(static m => m is not null).Select(static (m, _) => m!).Collect();

            var json = context.CompilationProvider.Select(static (c, _) => JsonContextModel.From(c));

            var rootNamespace = context.AnalyzerConfigOptionsProvider.Combine(context.CompilationProvider)
                .Select(static (pair, _) =>
                    pair.Left.GlobalOptions.TryGetValue("build_property.RootNamespace", out var ns) && !string.IsNullOrWhiteSpace(ns)
                        ? ns!
                        : pair.Right.AssemblyName ?? "PgBoltGenerated");

            var all = declared.Combine(executors).Combine(calls).Combine(json).Combine(rootNamespace);
            context.RegisterSourceOutput(all, static (spc, data) =>
            {
                var ((((declaredTypes, executorModels), callTypes), jsonContext), ns) = data;
                Emitter.Emit(spc, declaredTypes, executorModels, callTypes, jsonContext, ns);
            });
        }
    }
}
```

`src/PgBolt.Generators/Emitter.cs` (в этой задаче — только метаданные; `Registry` добавляется в Task 27):

```csharp
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace PgBolt.Generators
{
    internal static class Emitter
    {
        public static void Emit(SourceProductionContext spc, ImmutableArray<JobTypeModel> declared,
            ImmutableArray<ExecutorModel> executors, ImmutableArray<JobTypeModel> calls, JsonContextModel json, string rootNamespace)
        {
            var types = declared.Concat(executors.Select(e => e.Args)).Concat(calls)
                .GroupBy(m => m.TypeFqn).Select(g => g.First())
                .OrderBy(m => m.TypeFqn, StringComparer.Ordinal)
                .ToList();
            if (types.Count > 0)
                spc.AddSource("PgBolt.Metadata.g.cs", Metadata(types, json));
        }

        private static string Metadata(List<JobTypeModel> types, JsonContextModel json)
        {
            var sb = new StringBuilder();
            sb.AppendLine("// <auto-generated/>");
            sb.AppendLine("#nullable enable");
            sb.AppendLine();
            sb.AppendLine("namespace PgBolt.Generated");
            sb.AppendLine("{");
            sb.AppendLine("    internal static class PgBoltMetadataInitializer");
            sb.AppendLine("    {");
            sb.AppendLine("        [global::System.Runtime.CompilerServices.ModuleInitializer]");
            sb.AppendLine("        internal static void Initialize()");
            sb.AppendLine("        {");
            foreach (var t in types)
                sb.AppendLine($"            global::PgBolt.JobMetadataRegistry.Register<{t.TypeFqn}>({Descriptor(t)}, {TypeInfo(t.TypeFqn, json, nullable: true)});");
            sb.AppendLine("        }");
            sb.AppendLine("    }");
            sb.AppendLine("}");
            return sb.ToString();
        }

        internal static string Descriptor(JobTypeModel t) =>
            $"new global::PgBolt.JobDescriptor({Literal(t.JobName)}, {Literal(t.Queue)}, {t.MaxAttempts}, (short){t.Priority}, {Delays(t.RetryDelays)})";

        internal static string TypeInfo(string typeFqn, JsonContextModel json, bool nullable) =>
            json.ContextFqn is { } ctx && json.Contains(typeFqn)
                ? $"(global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{typeFqn}>){ctx}.Default.GetTypeInfo(typeof({typeFqn}))!"
                : nullable ? "null" : "null!";

        private static string Delays(EquatableArray<int>? delays) =>
            delays is { } d ? $"new int[] {{ {string.Join(", ", d)} }}" : "null";

        private static string Literal(string value) => SymbolDisplay.FormatLiteral(value, quote: true);
    }
}
```

- [x] **Step 6: Прогнать тесты и принять снапшоты**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: assert-проверки проходят; Verify создаёт `*.received.*` и падает, потому что `*.verified.*` ещё нет. Файлы снапшотов называются `<Класс>.<Тест>#<HintName>.received.cs` и начинаются строкой `//HintName: <HintName>` (её добавляет Verify.SourceGenerators; в `.verified.cs` она остаётся). Открыть `.received.cs` для `Contracts_only_assembly_...` и сверить с ожидаемым:

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
            global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.SendReceiptArgs>(new global::PgBolt.JobDescriptor("orders.send-receipt", "emails", 5, (short)-1, new int[] { 10, 60 }), null);
        }
    }
}
```

В `.received.cs` для `Enqueue_call_site_...` строка регистрации заканчивается `...GetTypeInfo(typeof(global::PingArgs))!);` — `!` обязателен: без него каст `JsonTypeInfo?` → `JsonTypeInfo<T>` даёт CS8600 под `#nullable enable`, а `GeneratorHarness.AssertClean` отклоняет и предупреждения.

Если совпадает — переименовать `.received.` в `.verified.` (или `dotnet verify accept`), прогнать снова.
Expected: PASS.

- [x] **Step 7: Commit**

```bash
grep -qxF '*.received.*' .gitignore || echo '*.received.*' >> .gitignore
git add .gitignore src/PgBolt.Generators tests/PgBolt.Generators.Tests
git commit -m "feat(generator): module-initializer metadata for [Job] types"
```

### Task 27: Генератор — `PgBoltJobRegistry` в сборках с исполнителями

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Modify: `src/PgBolt.Generators/Emitter.cs`
- Test: `tests/PgBolt.Generators.Tests/RegistryGenerationTests.cs`

**Interfaces:**
- Consumes: модели и `Emitter.Descriptor/TypeInfo` (Task 26).
- Produces: файл `PgBolt.JobRegistry.g.cs` — только при наличии исполнителей: `namespace {RootNamespace} { public static class PgBoltJobRegistry { public static void Register(global::PgBolt.IPgBoltJobsBuilder builder) { builder.AddExecutor<TArgs, TJob>(descriptor, typeInfo); … } } }`. `RootNamespace` — MSBuild-свойство (через `CompilerVisibleProperty`, Task 30); если его нет — имя сборки.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Generators.Tests/RegistryGenerationTests.cs`:

```csharp
using static VerifyXunit.Verifier;

namespace PgBolt.Generators.Tests;

public sealed class RegistryGenerationTests
{
    private const string Source = """
        using System.Threading;
        using System.Threading.Tasks;
        using PgBolt;

        [assembly: PgBoltJsonContext(typeof(Test.Jobs.AppJson))]

        namespace Test.Jobs;

        [Job("a.job")] public sealed record AArgs(int X);
        [Job("b.job", Queue = "b")] public sealed record BArgs(int X);

        public sealed class AJob : IJob<AArgs>
        {
            public Task ExecuteAsync(AArgs args, JobContext context, CancellationToken ct) => Task.CompletedTask;
        }

        public sealed record BJob : IJob<BArgs> // record class — тоже исполнитель (RecordDeclarationSyntax)
        {
            public Task ExecuteAsync(BArgs args, JobContext context, CancellationToken ct) => Task.CompletedTask;
        }

        public abstract class AbstractJob : IJob<AArgs>
        {
            public abstract Task ExecuteAsync(AArgs args, JobContext context, CancellationToken ct);
        }

        [System.Text.Json.Serialization.JsonSerializable(typeof(AArgs))]
        [System.Text.Json.Serialization.JsonSerializable(typeof(BArgs))]
        public sealed class AppJson : System.Text.Json.Serialization.JsonSerializerContext
        {
            public AppJson() : base(null) { }
            public static AppJson Default { get; } = new();
            protected override System.Text.Json.JsonSerializerOptions? GeneratedSerializerOptions => null;
            public override System.Text.Json.Serialization.Metadata.JsonTypeInfo? GetTypeInfo(System.Type type) => null;
        }
        """;

    [Fact]
    public Task Registry_is_generated_in_root_namespace_and_compiles()
    {
        var (driver, output) = GeneratorHarness.Run(Source, rootNamespace: "Test.Jobs");

        var code = GeneratorHarness.Generated(driver, "PgBolt.JobRegistry.g.cs");
        Assert.Contains("namespace Test.Jobs", code);
        Assert.Contains("public static class PgBoltJobRegistry", code);
        Assert.Contains("builder.AddExecutor<global::Test.Jobs.AArgs, global::Test.Jobs.AJob>(", code);
        Assert.Contains("builder.AddExecutor<global::Test.Jobs.BArgs, global::Test.Jobs.BJob>(", code);
        Assert.DoesNotContain("AbstractJob", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public void Missing_root_namespace_falls_back_to_assembly_name()
    {
        var (driver, _) = GeneratorHarness.Run(Source, rootNamespace: null);
        Assert.Contains("namespace Test.Jobs", GeneratorHarness.Generated(driver, "PgBolt.JobRegistry.g.cs"));
    }

    [Fact]
    public void No_executors_no_registry()
    {
        var (driver, _) = GeneratorHarness.Run("""
            [PgBolt.Job("only.contract")] public sealed record OnlyArgs(int X);
            """);
        Assert.DoesNotContain("PgBolt.JobRegistry.g.cs", GeneratorHarness.HintNames(driver));
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Generators.Tests --filter "FullyQualifiedName~RegistryGenerationTests"`
Expected: FAIL — `PgBolt.JobRegistry.g.cs` не сгенерирован.

- [x] **Step 3: Добавить генерацию registry**

В `src/PgBolt.Generators/Emitter.cs` в конец метода `Emit` добавить:

```csharp
            var distinctExecutors = executors
                .GroupBy(e => e.JobFqn).Select(g => g.First())
                .OrderBy(e => e.JobFqn, StringComparer.Ordinal)
                .ToList();
            if (distinctExecutors.Count > 0)
                spc.AddSource("PgBolt.JobRegistry.g.cs", Registry(distinctExecutors, json, rootNamespace));
```

и метод:

```csharp
        private static string Registry(List<ExecutorModel> executors, JsonContextModel json, string rootNamespace)
        {
            var sb = new StringBuilder();
            sb.AppendLine("// <auto-generated/>");
            sb.AppendLine("#nullable enable");
            sb.AppendLine();
            sb.AppendLine($"namespace {rootNamespace}");
            sb.AppendLine("{");
            sb.AppendLine("    /// <summary>Исполнители PgBolt этой сборки: o.AddJobs(PgBoltJobRegistry.Register).</summary>");
            sb.AppendLine("    public static class PgBoltJobRegistry");
            sb.AppendLine("    {");
            sb.AppendLine("        public static void Register(global::PgBolt.IPgBoltJobsBuilder builder)");
            sb.AppendLine("        {");
            sb.AppendLine("            if (builder is null) throw new global::System.ArgumentNullException(nameof(builder));");
            foreach (var e in executors)
                sb.AppendLine($"            builder.AddExecutor<{e.Args.TypeFqn}, {e.JobFqn}>({Descriptor(e.Args)}, {TypeInfo(e.Args.TypeFqn, json, nullable: false)});");
            sb.AppendLine("        }");
            sb.AppendLine("    }");
            sb.AppendLine("}");
            return sb.ToString();
        }
```

- [x] **Step 4: Прогнать тесты и принять снапшоты**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: assert-проверки проходят; тест регистра создаёт два `.received.cs` — генератор выдаёт оба файла:
- `RegistryGenerationTests.Registry_is_generated_in_root_namespace_and_compiles#PgBolt.JobRegistry.g.received.cs` — должен содержать две строки `builder.AddExecutor<…>(new global::PgBolt.JobDescriptor(…), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<…>)global::Test.Jobs.AppJson.Default.GetTypeInfo(typeof(…))!);`;
- `RegistryGenerationTests.Registry_is_generated_in_root_namespace_and_compiles#PgBolt.Metadata.g.received.cs` — module initializer с двумя строками `global::PgBolt.JobMetadataRegistry.Register<…>(…, (…)global::Test.Jobs.AppJson.Default.GetTypeInfo(typeof(…))!);` (для `a.job` и `b.job`).

Принять оба (переименовать `.received.` в `.verified.`) и прогнать снова. Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Generators tests/PgBolt.Generators.Tests
git commit -m "feat(generator): PgBoltJobRegistry.Register in assemblies with executors"
```

### Task 28: Анализатор — BOLT002–BOLT006

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Generators/Analysis/Diagnostics.cs`, `Analysis/JobNames.cs`, `Analysis/PgBoltAnalyzer.cs`, `Analysis/AnalysisState.cs`
- Create: `src/PgBolt.Generators/AnalyzerReleases.Shipped.md`, `src/PgBolt.Generators/AnalyzerReleases.Unshipped.md`
- Modify: `src/PgBolt.Generators/PgBolt.Generators.csproj` (AdditionalFiles)
- Create: `tests/PgBolt.Generators.Tests/AnalyzerVerifier.cs`
- Test: `tests/PgBolt.Generators.Tests/AnalyzerTests.cs`

**Interfaces:**
- Produces:
  - `internal static class Diagnostics` с дескрипторами `MissingJsonType` (BOLT001), `DuplicateName` (BOLT002), `ArgsNotJob` (BOLT003), `MultipleExecutors` (BOLT004), `InvalidName` (BOLT005), `MissingJsonContext` (BOLT006); все — `Error`, категория `PgBolt`. BOLT002/004/006 помечены `WellKnownDiagnosticTags.CompilationEnd`.
  - `[DiagnosticAnalyzer(LanguageNames.CSharp)] public sealed class PgBoltAnalyzer`.
  - Свойства диагностики BOLT001 (для code fix, Task 29): `ArgsType` — FQN args-типа (`global::…`), `ContextType` — metadata name контекста.
  - Места: BOLT002/003/004 — идентификатор типа; BOLT005 — синтаксис атрибута `[Job(...)]`; BOLT006 — без локации (`Location.None`), один раз на компиляцию.

- [x] **Step 1: Написать тестовый верификатор и падающие тесты**

`tests/PgBolt.Generators.Tests/AnalyzerVerifier.cs`:

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using PgBolt.Generators.Analysis;

namespace PgBolt.Generators.Tests;

internal static class AnalyzerVerifier
{
    public static readonly ReferenceAssemblies Net10 = new("net10.0",
        new PackageIdentity("Microsoft.NETCore.App.Ref", "10.0.0"), Path.Combine("ref", "net10.0")); // PackageIdentity из Microsoft.CodeAnalysis.Testing

    public static void AddPgBoltReferences(SolutionState state)
    {
        state.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(PgBolt.JobAttribute).Assembly.Location));
        state.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(Microsoft.Extensions.Logging.ILogger).Assembly.Location));
    }

    public static async Task VerifyAsync(string source, params DiagnosticResult[] expected)
    {
        var test = new CSharpAnalyzerTest<PgBoltAnalyzer, DefaultVerifier>
        {
            TestCode = source,
            ReferenceAssemblies = Net10,
            CompilerDiagnostics = CompilerDiagnostics.None,
        };
        AddPgBoltReferences(test.TestState);
        test.ExpectedDiagnostics.AddRange(expected);
        await test.RunAsync(TestContext.Current.CancellationToken);
    }

    public static DiagnosticResult Diagnostic(DiagnosticDescriptor descriptor) => new(descriptor);
}
```

`tests/PgBolt.Generators.Tests/AnalyzerTests.cs`:

```csharp
using PgBolt.Generators.Analysis;
using static PgBolt.Generators.Tests.AnalyzerVerifier;

namespace PgBolt.Generators.Tests;

public sealed class AnalyzerTests
{
    private const string Usings = """
        using System.Threading;
        using System.Threading.Tasks;
        using System.Text.Json.Serialization;
        using PgBolt;

        """;

    private const string Context = """

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """;

    [Fact]
    public Task Valid_assembly_has_no_diagnostics() => VerifyAsync(Usings + """
        [assembly: PgBoltJsonContext(typeof(AppJson))]
        [Job("a.job")] public sealed record A(int X);
        public sealed class AJob : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """ + Context);

    [Fact]
    public Task Duplicate_names_BOLT002() => VerifyAsync(Usings + """
        [Job("dup.job")] public sealed record {|#0:A|}(int X);
        [Job("dup.job")] public sealed record {|#1:B|}(int X);
        """,
        Diagnostic(Diagnostics.DuplicateName).WithLocation(0).WithArguments("dup.job"),
        Diagnostic(Diagnostics.DuplicateName).WithLocation(1).WithArguments("dup.job"));

    [Fact]
    public Task Executor_of_non_job_args_BOLT003() => VerifyAsync(Usings + """
        [assembly: PgBoltJsonContext(typeof(AppJson))]
        public sealed record A(int X);
        public sealed class {|#0:AJob|} : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """ + Context,
        Diagnostic(Diagnostics.ArgsNotJob).WithLocation(0).WithArguments("AJob", "A"));

    [Fact]
    public Task Two_executors_for_same_args_BOLT004() => VerifyAsync(Usings + """
        [assembly: PgBoltJsonContext(typeof(AppJson))]
        [Job("a.job")] public sealed record A(int X);
        public sealed class {|#0:AJob|} : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        public sealed class {|#1:AJob2|} : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """ + Context,
        Diagnostic(Diagnostics.MultipleExecutors).WithLocation(0).WithArguments("A"),
        Diagnostic(Diagnostics.MultipleExecutors).WithLocation(1).WithArguments("A"));

    [Theory]
    [InlineData("Bad Name")]
    [InlineData("UPPER")]
    [InlineData("")]
    public Task Invalid_name_BOLT005(string name) => VerifyAsync(Usings + $$"""
        [{|#0:Job("{{name}}")|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.InvalidName).WithLocation(0).WithArguments(name));

    [Fact]
    public Task Too_long_name_BOLT005()
    {
        var name = new string('a', 129);
        return VerifyAsync(Usings + $$"""
            [{|#0:Job("{{name}}")|}] public sealed record A(int X);
            """,
            Diagnostic(Diagnostics.InvalidName).WithLocation(0).WithArguments(name));
    }

    [Fact]
    public Task Executor_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public sealed class AJob : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """,
        Diagnostic(Diagnostics.MissingJsonContext));

    [Fact]
    public Task Enqueue_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public static class S { public static async Task Go(IJobClient c) => await c.EnqueueAsync(new A(1)); }
        """,
        Diagnostic(Diagnostics.MissingJsonContext));

    [Fact]
    public Task Contracts_only_assembly_needs_no_context() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        """);
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Generators.Tests --filter "FullyQualifiedName~AnalyzerTests"`
Expected: FAIL при компиляции — первая ошибка CS0234: пространство имён `PgBolt.Generators.Analysis` не существует (`PgBoltAnalyzer`, `Diagnostics` ещё не определены).

- [x] **Step 3: Добавить файлы release tracking**

`src/PgBolt.Generators/AnalyzerReleases.Shipped.md`:

```markdown
; Shipped analyzer releases
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md
```

`src/PgBolt.Generators/AnalyzerReleases.Unshipped.md`:

```markdown
; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
BOLT001 | PgBolt | Error | Diagnostics
BOLT002 | PgBolt | Error | Diagnostics
BOLT003 | PgBolt | Error | Diagnostics
BOLT004 | PgBolt | Error | Diagnostics
BOLT005 | PgBolt | Error | Diagnostics
BOLT006 | PgBolt | Error | Diagnostics
```

В `src/PgBolt.Generators/PgBolt.Generators.csproj` добавить:

```xml
  <ItemGroup>
    <AdditionalFiles Include="AnalyzerReleases.Shipped.md" />
    <AdditionalFiles Include="AnalyzerReleases.Unshipped.md" />
  </ItemGroup>
```

- [x] **Step 4: Реализовать дескрипторы и проверку имени**

`src/PgBolt.Generators/Analysis/Diagnostics.cs`:

```csharp
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators.Analysis
{
    internal static class Diagnostics
    {
        private const string Category = "PgBolt";

        public static readonly DiagnosticDescriptor MissingJsonType = new(
            "BOLT001", "Job arguments type is missing in the JSON context",
            "Job arguments type '{0}' is not declared in JSON context '{1}'; add [JsonSerializable(typeof({0}))]",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor DuplicateName = new(
            "BOLT002", "Duplicate job name",
            "Job name '{0}' is declared by more than one type in this compilation",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true, customTags: WellKnownDiagnosticTags.CompilationEnd);

        public static readonly DiagnosticDescriptor ArgsNotJob = new(
            "BOLT003", "Job executor arguments type is not a [Job]",
            "'{0}' implements IJob<{1}>, but '{1}' is not marked with [Job]",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor MultipleExecutors = new(
            "BOLT004", "Multiple executors for one job",
            "Arguments type '{0}' has more than one executor in this assembly",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true, customTags: WellKnownDiagnosticTags.CompilationEnd);

        public static readonly DiagnosticDescriptor InvalidName = new(
            "BOLT005", "Invalid job name",
            "Invalid job name '{0}': allowed characters are [a-z0-9._-], length 1-128",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor MissingJsonContext = new(
            "BOLT006", "Missing PgBolt JSON context",
            "This assembly has job executors or enqueue calls but no [assembly: PgBoltJsonContext(typeof(...))]",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true, customTags: WellKnownDiagnosticTags.CompilationEnd);

        public static readonly ImmutableArray<DiagnosticDescriptor> All = ImmutableArray.Create(
            MissingJsonType, DuplicateName, ArgsNotJob, MultipleExecutors, InvalidName, MissingJsonContext);
    }
}
```

`src/PgBolt.Generators/Analysis/JobNames.cs`:

```csharp
namespace PgBolt.Generators.Analysis
{
    internal static class JobNames
    {
        public static bool IsValid(string? name)
        {
            if (name is null || name.Length is 0 or > 128)
                return false;
            foreach (var c in name)
                if (!(c is >= 'a' and <= 'z' || c is >= '0' and <= '9' || c is '.' or '_' or '-'))
                    return false;
            return true;
        }
    }
}
```

- [x] **Step 5: Реализовать анализатор**

`src/PgBolt.Generators/Analysis/PgBoltAnalyzer.cs`:

```csharp
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace PgBolt.Generators.Analysis
{
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class PgBoltAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => Diagnostics.All;

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterCompilationStartAction(static start =>
            {
                var state = AnalysisState.TryCreate(start.Compilation);
                if (state is null)
                    return; // PgBolt.Abstractions не подключён
                start.RegisterSymbolAction(state.AnalyzeNamedType, SymbolKind.NamedType);
                start.RegisterOperationAction(state.AnalyzeInvocation, OperationKind.Invocation);
                start.RegisterCompilationEndAction(state.OnCompilationEnd);
            });
        }
    }
}
```

`src/PgBolt.Generators/Analysis/AnalysisState.cs`:

```csharp
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace PgBolt.Generators.Analysis
{
    internal sealed class AnalysisState
    {
        private readonly INamedTypeSymbol _jobAttribute;
        private readonly INamedTypeSymbol _jobInterface;
        private readonly INamedTypeSymbol _jobClient;
        private readonly INamedTypeSymbol? _context;
        private readonly HashSet<ITypeSymbol> _contextTypes;
        private readonly ConcurrentDictionary<string, ConcurrentBag<INamedTypeSymbol>> _names = new();
        private readonly ConcurrentDictionary<ITypeSymbol, ConcurrentBag<INamedTypeSymbol>> _executors = new(SymbolEqualityComparer.Default);
        private int _usages;

        private AnalysisState(INamedTypeSymbol jobAttribute, INamedTypeSymbol jobInterface, INamedTypeSymbol jobClient,
            INamedTypeSymbol? context, HashSet<ITypeSymbol> contextTypes)
        {
            _jobAttribute = jobAttribute;
            _jobInterface = jobInterface;
            _jobClient = jobClient;
            _context = context;
            _contextTypes = contextTypes;
        }

        public static AnalysisState? TryCreate(Compilation compilation)
        {
            var jobAttribute = compilation.GetTypeByMetadataName(KnownNames.JobAttribute);
            var jobInterface = compilation.GetTypeByMetadataName(KnownNames.JobInterfaceMetadata);
            var jobClient = compilation.GetTypeByMetadataName(KnownNames.JobClientMetadata);
            if (jobAttribute is null || jobInterface is null || jobClient is null)
                return null;

            INamedTypeSymbol? context = null;
            var types = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
            foreach (var attr in compilation.Assembly.GetAttributes())
            {
                if (attr.AttributeClass?.ToDisplayString() != KnownNames.JsonContextAttribute
                    || attr.ConstructorArguments.Length != 1
                    || attr.ConstructorArguments[0].Value is not INamedTypeSymbol ctx)
                    continue;
                context = ctx;
                foreach (var a in ctx.GetAttributes())
                    if (a.AttributeClass?.ToDisplayString() == KnownNames.JsonSerializableAttribute
                        && a.ConstructorArguments.Length > 0 && a.ConstructorArguments[0].Value is ITypeSymbol t)
                        types.Add(t);
            }
            return new AnalysisState(jobAttribute, jobInterface, jobClient, context, types);
        }

        public void AnalyzeNamedType(SymbolAnalysisContext ctx)
        {
            var type = (INamedTypeSymbol)ctx.Symbol;

            var attr = type.GetAttributes().FirstOrDefault(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, _jobAttribute));
            if (attr is { ConstructorArguments.Length: 1 } && attr.ConstructorArguments[0].Value is string name)
            {
                if (!JobNames.IsValid(name))
                    ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidName,
                        attr.ApplicationSyntaxReference?.GetSyntax(ctx.CancellationToken).GetLocation() ?? type.Locations[0], name));
                _names.GetOrAdd(name, _ => new ConcurrentBag<INamedTypeSymbol>()).Add(type);
            }

            if (type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsGenericType)
                return;
            foreach (var i in type.AllInterfaces)
            {
                if (!SymbolEqualityComparer.Default.Equals(i.OriginalDefinition, _jobInterface))
                    continue;
                var args = i.TypeArguments[0];
                Interlocked.Increment(ref _usages);
                if (!HasJobAttribute(args))
                {
                    ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.ArgsNotJob, type.Locations[0], type.Name, args.ToDisplayString()));
                    continue;
                }
                _executors.GetOrAdd(args, _ => new ConcurrentBag<INamedTypeSymbol>()).Add(type);
                CheckCoverage(args, type.Locations[0], ctx.ReportDiagnostic);
            }
        }

        public void AnalyzeInvocation(OperationAnalysisContext ctx)
        {
            var method = ((IInvocationOperation)ctx.Operation).TargetMethod;
            if (!KnownNames.IsClientMethod(method, _jobClient))
                return;
            var args = method.TypeArguments[0];
            if (args is ITypeParameterSymbol || !HasJobAttribute(args))
                return;
            Interlocked.Increment(ref _usages);
            CheckCoverage(args, ctx.Operation.Syntax.GetLocation(), ctx.ReportDiagnostic);
        }

        public void OnCompilationEnd(CompilationAnalysisContext ctx)
        {
            foreach (var pair in _names)
            {
                var types = pair.Value.Distinct<INamedTypeSymbol>(SymbolEqualityComparer.Default).ToList();
                if (types.Count > 1)
                    foreach (var t in types)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.DuplicateName, t.Locations[0], pair.Key));
            }
            foreach (var pair in _executors)
            {
                var executors = pair.Value.Distinct<INamedTypeSymbol>(SymbolEqualityComparer.Default).ToList();
                if (executors.Count > 1)
                    foreach (var e in executors)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.MultipleExecutors, e.Locations[0], pair.Key.ToDisplayString()));
            }
            if (_context is null && Volatile.Read(ref _usages) > 0)
                ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.MissingJsonContext, Location.None));
        }

        private bool HasJobAttribute(ITypeSymbol type) =>
            type.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, _jobAttribute));

        private void CheckCoverage(ITypeSymbol args, Location location, System.Action<Diagnostic> report)
        {
            if (_context is null || _contextTypes.Contains(args))
                return;
            var properties = ImmutableDictionary<string, string?>.Empty
                .Add("ArgsType", args.ToDisplayString(KnownNames.Fq))
                .Add("ContextType", KnownNames.MetadataName(_context));
            report(Diagnostic.Create(Diagnostics.MissingJsonType, location, properties, args.ToDisplayString(), _context.Name));
        }
    }
}
```

- [x] **Step 6: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Generators.Tests --filter "FullyQualifiedName~AnalyzerTests"`
Expected: PASS. Сборка `src/PgBolt.Generators` — без предупреждений RS* (release tracking заполнен).

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Generators tests/PgBolt.Generators.Tests
git commit -m "feat(analyzers): BOLT002-BOLT006 job name, executor and context rules"
```

### Task 29: BOLT001 и code fix «добавить `[JsonSerializable]`»

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Generators.CodeFixes/AddJsonSerializableCodeFix.cs`
- Test: `tests/PgBolt.Generators.Tests/JsonCoverageTests.cs`

**Interfaces:**
- Consumes: `Diagnostics.MissingJsonType` и свойства `ArgsType`/`ContextType` (Task 28 — анализатор уже сообщает BOLT001 через `CheckCoverage`).
- Produces: `[ExportCodeFixProvider(LanguageNames.CSharp)] public sealed class AddJsonSerializableCodeFix : CodeFixProvider` — добавляет `[JsonSerializable(typeof(TArgs))]` к классу контекста (контекст может быть в другом документе проекта).

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Generators.Tests/JsonCoverageTests.cs`:

```csharp
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using PgBolt.Generators.Analysis;
using PgBolt.Generators.CodeFixes;
using static PgBolt.Generators.Tests.AnalyzerVerifier;

namespace PgBolt.Generators.Tests;

public sealed class JsonCoverageTests
{
    private const string Head = """
        using System.Threading;
        using System.Threading.Tasks;
        using System.Text.Json.Serialization;
        using PgBolt;

        [assembly: PgBoltJsonContext(typeof(AppJson))]
        [Job("a.job")] public sealed record A(int X);
        [Job("b.job")] public sealed record B(int X);

        """;

    [Fact]
    public Task Executor_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public sealed class {|#0:BJob|} : IJob<B> { public Task ExecuteAsync(B b, JobContext c, CancellationToken t) => Task.CompletedTask; }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));

    [Fact]
    public Task Enqueue_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public static class S { public static async Task Go(IJobClient c) => await {|#0:c.EnqueueAsync(new B(1))|}; }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));

    [Fact]
    public async Task Code_fix_adds_json_serializable_attribute()
    {
        var test = new CSharpCodeFixTest<PgBoltAnalyzer, AddJsonSerializableCodeFix, DefaultVerifier>
        {
            TestCode = Head + """
                public sealed class {|#0:BJob|} : IJob<B> { public Task ExecuteAsync(B b, JobContext c, CancellationToken t) => Task.CompletedTask; }

                [JsonSerializable(typeof(A))]
                public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
                """,
            FixedCode = Head + """
                public sealed class BJob : IJob<B> { public Task ExecuteAsync(B b, JobContext c, CancellationToken t) => Task.CompletedTask; }

                [JsonSerializable(typeof(A))]
                [JsonSerializable(typeof(B))]
                public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
                """,
            ReferenceAssemblies = Net10,
            CompilerDiagnostics = CompilerDiagnostics.None,
        };
        AddPgBoltReferences(test.TestState);
        AddPgBoltReferences(test.FixedState);
        test.ExpectedDiagnostics.Add(Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));
        await test.RunAsync(TestContext.Current.CancellationToken);
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Generators.Tests --filter "FullyQualifiedName~JsonCoverageTests"`
Expected: FAIL при компиляции тестового проекта — первая ошибка CS0234: пространство имён `PgBolt.Generators.CodeFixes` не существует (`AddJsonSerializableCodeFix` ещё не определён), поэтому не запускается ни один тест проекта, включая два теста BOLT001. Они проходят после Step 3 без изменений анализатора (Task 28 уже проверяет покрытие).

- [x] **Step 3: Реализовать code fix**

`src/PgBolt.Generators.CodeFixes/AddJsonSerializableCodeFix.cs`:

```csharp
using System.Collections.Immutable;
using System.Composition;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Formatting;
using Microsoft.CodeAnalysis.Simplification;

namespace PgBolt.Generators.CodeFixes
{
    [ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(AddJsonSerializableCodeFix)), Shared]
    public sealed class AddJsonSerializableCodeFix : CodeFixProvider
    {
        public override ImmutableArray<string> FixableDiagnosticIds => ImmutableArray.Create("BOLT001");

        public override FixAllProvider? GetFixAllProvider() => null;

        public override Task RegisterCodeFixesAsync(CodeFixContext context)
        {
            var diagnostic = context.Diagnostics[0];
            if (!diagnostic.Properties.TryGetValue("ArgsType", out var argsType) || argsType is null
                || !diagnostic.Properties.TryGetValue("ContextType", out var contextType) || contextType is null)
                return Task.CompletedTask;

            var project = context.Document.Project;
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Add [JsonSerializable(typeof({argsType.Replace("global::", "")}))] to the PgBolt JSON context",
                    ct => AddAttributeAsync(project, contextType, argsType, ct),
                    equivalenceKey: "BOLT001:" + argsType),
                diagnostic);
            return Task.CompletedTask;
        }

        private static async Task<Solution> AddAttributeAsync(Project project, string contextMetadataName, string argsType, CancellationToken ct)
        {
            var solution = project.Solution;
            var compilation = await project.GetCompilationAsync(ct).ConfigureAwait(false);
            var syntaxRef = compilation?.GetTypeByMetadataName(contextMetadataName)?.DeclaringSyntaxReferences.FirstOrDefault();
            if (syntaxRef is null)
                return solution;
            var document = solution.GetDocument(syntaxRef.SyntaxTree);
            if (document is null)
                return solution;
            var root = await document.GetSyntaxRootAsync(ct).ConfigureAwait(false);
            if (root is null || await syntaxRef.GetSyntaxAsync(ct).ConfigureAwait(false) is not ClassDeclarationSyntax declaration)
                return solution;

            var endOfLine = declaration.AttributeLists.LastOrDefault()?.GetTrailingTrivia()
                                .FirstOrDefault(t => t.IsKind(SyntaxKind.EndOfLineTrivia)) is { } eol && eol.IsKind(SyntaxKind.EndOfLineTrivia)
                ? eol
                : SyntaxFactory.ElasticCarriageReturnLineFeed;

            var attributeList = SyntaxFactory.AttributeList(SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Attribute(
                        SyntaxFactory.ParseName("global::System.Text.Json.Serialization.JsonSerializable"),
                        SyntaxFactory.ParseAttributeArgumentList($"(typeof({argsType}))"))))
                .WithAdditionalAnnotations(Simplifier.Annotation, Formatter.Annotation);

            ClassDeclarationSyntax updated;
            if (declaration.AttributeLists.Count == 0)
            {
                var leading = declaration.GetLeadingTrivia();
                updated = declaration.WithoutLeadingTrivia()
                    .AddAttributeLists(attributeList.WithLeadingTrivia(leading).WithTrailingTrivia(endOfLine));
            }
            else
            {
                updated = declaration.AddAttributeLists(attributeList.WithTrailingTrivia(endOfLine));
            }

            return solution.WithDocumentSyntaxRoot(document.Id, root.ReplaceNode(declaration, updated));
        }
    }
}
```

`System.Composition` (`[Shared]`) приходит транзитивно с `Microsoft.CodeAnalysis.CSharp.Workspaces`.

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: PASS. Если code fix отличается от `FixedCode` только пробелами/переводами строк — поправить обработку trivia в `AddAttributeAsync` (не `FixedCode`): ожидаемый результат — атрибут на отдельной строке сразу после существующего.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Generators.CodeFixes tests/PgBolt.Generators.Tests
git commit -m "feat(analyzers): BOLT001 JSON context coverage with code fix"
```

### Task 30: Упаковка генератора в `PgBolt.Abstractions`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Abstractions/build/PgBolt.Abstractions.props`, `eng/UsePgBoltGenerator.props`
- Modify: `src/PgBolt.Abstractions/PgBolt.Abstractions.csproj`

**Interfaces:**
- Produces:
  - NuGet-пакет `PgBolt.Abstractions` с `lib/net10.0/PgBolt.Abstractions.dll`, `analyzers/dotnet/cs/PgBolt.Generators.dll`, `analyzers/dotnet/cs/PgBolt.Generators.CodeFixes.dll`, `buildTransitive/PgBolt.Abstractions.props` (`CompilerVisibleProperty RootNamespace`).
  - `eng/UsePgBoltGenerator.props` — подключение генератора к проектам этого репозитория (E2E, Task 31) так же, как это делает пакет.

- [x] **Step 1: Создать props**

`src/PgBolt.Abstractions/build/PgBolt.Abstractions.props`:

```xml
<Project>
  <ItemGroup>
    <CompilerVisibleProperty Include="RootNamespace" />
  </ItemGroup>
</Project>
```

`eng/UsePgBoltGenerator.props`:

```xml
<Project>
  <ItemGroup>
    <ProjectReference Include="$(RepoRoot)src/PgBolt.Generators/PgBolt.Generators.csproj"
                      OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
  </ItemGroup>
  <Import Project="$(RepoRoot)src/PgBolt.Abstractions/build/PgBolt.Abstractions.props" />
</Project>
```

- [x] **Step 2: Настроить упаковку**

В `src/PgBolt.Abstractions/PgBolt.Abstractions.csproj` добавить:

```xml
  <PropertyGroup>
    <PackageId>PgBolt.Abstractions</PackageId>
    <Description>PgBolt job contracts, client API, source generator and analyzers.</Description>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\PgBolt.Generators\PgBolt.Generators.csproj" ReferenceOutputAssembly="false" PrivateAssets="all" />
    <ProjectReference Include="..\PgBolt.Generators.CodeFixes\PgBolt.Generators.CodeFixes.csproj" ReferenceOutputAssembly="false" PrivateAssets="all" />
    <None Include="$(RepoRoot)src/PgBolt.Generators/bin/$(Configuration)/netstandard2.0/PgBolt.Generators.dll"
          Pack="true" PackagePath="analyzers/dotnet/cs" Visible="false" />
    <None Include="$(RepoRoot)src/PgBolt.Generators.CodeFixes/bin/$(Configuration)/netstandard2.0/PgBolt.Generators.CodeFixes.dll"
          Pack="true" PackagePath="analyzers/dotnet/cs" Visible="false" />
    <None Include="build/PgBolt.Abstractions.props" Pack="true" PackagePath="buildTransitive/PgBolt.Abstractions.props" />
  </ItemGroup>
```

- [x] **Step 3: Собрать пакет и проверить содержимое**

Run:
```bash
dotnet pack src/PgBolt.Abstractions -c Release -o artifacts
unzip -l artifacts/PgBolt.Abstractions.*.nupkg | grep -E "lib/net10.0|analyzers/dotnet/cs|buildTransitive"
```
Если `unzip` не установлен — `python3 -m zipfile -l artifacts/PgBolt.Abstractions.*.nupkg | grep -E "..."` с тем же шаблоном.
Expected: четыре строки — `lib/net10.0/PgBolt.Abstractions.dll`, `analyzers/dotnet/cs/PgBolt.Generators.dll`, `analyzers/dotnet/cs/PgBolt.Generators.CodeFixes.dll`, `buildTransitive/PgBolt.Abstractions.props`.

- [x] **Step 4: Прогнать все тесты**

Run: `dotnet test PgBolt.slnx`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
echo "artifacts/" >> .gitignore
git add .gitignore eng src/PgBolt.Abstractions
git commit -m "build: ship generator, analyzers and RootNamespace props in PgBolt.Abstractions"
```

---

## Фаза 6. Сквозная проверка

### Task 31: Три сборки с исполнителями, `InternalsVisibleTo`, полный цикл

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `tests/E2E/PgBolt.E2E.Contracts/PgBolt.E2E.Contracts.csproj`, `Contracts.cs`
- Create: `tests/E2E/PgBolt.E2E.JobsA/PgBolt.E2E.JobsA.csproj`, `PingJob.cs`
- Create: `tests/E2E/PgBolt.E2E.JobsB/PgBolt.E2E.JobsB.csproj`, `PongJob.cs`
- Create: `tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj`, `AssemblyInfo.cs`, `LocalJob.cs`, `EndToEndTests.cs`
- Modify: `PgBolt.slnx`

**Interfaces:**
- Consumes: всё из фаз 1–5.
- Produces: доказательство §11 «Регистрация»: две сборки с исполнителями и тестовый проект с `InternalsVisibleTo` компилируются без CS0121 и проходят enqueue → dequeue → invoke → ack напрямую и через PgBouncer.

- [x] **Step 1: Создать сборку контрактов**

`tests/E2E/PgBolt.E2E.Contracts/PgBolt.E2E.Contracts.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <Import Project="$(RepoRoot)eng/UsePgBoltGenerator.props" />
  <ItemGroup>
    <ProjectReference Include="$(RepoRoot)src/PgBolt.Abstractions/PgBolt.Abstractions.csproj" />
  </ItemGroup>
</Project>
```

`tests/E2E/PgBolt.E2E.Contracts/Contracts.cs`:

```csharp
namespace PgBolt.E2E.Contracts;

[Job("e2e.ping", Queue = "e2e")]
public sealed record PingArgs(string Message);

[Job("e2e.pong", Queue = "e2e", MaxAttempts = 3)]
public sealed record PongArgs(int N);
```

- [x] **Step 2: Создать две сборки с исполнителями**

`tests/E2E/PgBolt.E2E.JobsA/PgBolt.E2E.JobsA.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <Import Project="$(RepoRoot)eng/UsePgBoltGenerator.props" />
  <ItemGroup>
    <ProjectReference Include="..\PgBolt.E2E.Contracts\PgBolt.E2E.Contracts.csproj" />
  </ItemGroup>
  <ItemGroup>
    <InternalsVisibleTo Include="PgBolt.E2E.Tests" />
  </ItemGroup>
</Project>
```

`tests/E2E/PgBolt.E2E.JobsA/PingJob.cs`:

```csharp
using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using PgBolt.E2E.Contracts;

[assembly: PgBolt.PgBoltJsonContext(typeof(PgBolt.E2E.JobsA.JobsAJson))]

namespace PgBolt.E2E.JobsA;

public static class PingProbe
{
    public static ConcurrentQueue<string> Messages { get; } = new();
}

public sealed class PingJob : IJob<PingArgs>
{
    public Task ExecuteAsync(PingArgs args, JobContext context, CancellationToken cancellationToken)
    {
        PingProbe.Messages.Enqueue(args.Message);
        return Task.CompletedTask;
    }
}

[JsonSerializable(typeof(PingArgs))]
internal sealed partial class JobsAJson : JsonSerializerContext;
```

`tests/E2E/PgBolt.E2E.JobsB/PgBolt.E2E.JobsB.csproj` — как JobsA, без `InternalsVisibleTo`.

`tests/E2E/PgBolt.E2E.JobsB/PongJob.cs`:

```csharp
using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using PgBolt.E2E.Contracts;

[assembly: PgBolt.PgBoltJsonContext(typeof(PgBolt.E2E.JobsB.JobsBJson))]

namespace PgBolt.E2E.JobsB;

public static class PongProbe
{
    public static ConcurrentQueue<int> Values { get; } = new();
}

public sealed class PongJob : IJob<PongArgs>
{
    public Task ExecuteAsync(PongArgs args, JobContext context, CancellationToken cancellationToken)
    {
        PongProbe.Values.Enqueue(args.N);
        return Task.CompletedTask;
    }
}

[JsonSerializable(typeof(PongArgs))]
internal sealed partial class JobsBJson : JsonSerializerContext;
```

- [x] **Step 3: Создать тестовый проект со своим исполнителем**

`tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <Import Project="$(RepoRoot)eng/UsePgBoltGenerator.props" />
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <IsPackable>false</IsPackable>
    <RootNamespace>PgBolt.E2E.Tests</RootNamespace>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" />
    <PackageReference Include="xunit.v3" />
    <PackageReference Include="xunit.runner.visualstudio" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" />
  </ItemGroup>
  <ItemGroup>
    <Using Include="Xunit" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\PgBolt.E2E.JobsA\PgBolt.E2E.JobsA.csproj" />
    <ProjectReference Include="..\PgBolt.E2E.JobsB\PgBolt.E2E.JobsB.csproj" />
    <ProjectReference Include="..\..\PgBolt.Testing\PgBolt.Testing.csproj" />
  </ItemGroup>
</Project>
```

`tests/E2E/PgBolt.E2E.Tests/AssemblyInfo.cs`:

```csharp
using PgBolt.Testing;

[assembly: AssemblyFixture(typeof(PostgresFixture))]
[assembly: PgBolt.PgBoltJsonContext(typeof(PgBolt.E2E.Tests.E2EJson))]
```

`tests/E2E/PgBolt.E2E.Tests/LocalJob.cs`:

```csharp
using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using PgBolt.E2E.Contracts;

namespace PgBolt.E2E.Tests;

[Job("e2e.local", Queue = "e2e")]
public sealed record LocalArgs(int N);

public static class LocalProbe
{
    public static ConcurrentQueue<int> Values { get; } = new();
}

public sealed class LocalJob : IJob<LocalArgs>
{
    public Task ExecuteAsync(LocalArgs args, JobContext context, CancellationToken cancellationToken)
    {
        LocalProbe.Values.Enqueue(args.N);
        return Task.CompletedTask;
    }
}

/// <summary>Сборка вызывает EnqueueAsync для PingArgs/PongArgs/LocalArgs — все они обязаны быть в её контексте (BOLT001).</summary>
[JsonSerializable(typeof(LocalArgs))]
[JsonSerializable(typeof(PingArgs))]
[JsonSerializable(typeof(PongArgs))]
internal sealed partial class E2EJson : JsonSerializerContext;
```

- [x] **Step 4: Написать сквозной тест**

`tests/E2E/PgBolt.E2E.Tests/EndToEndTests.cs`:

```csharp
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.E2E.Contracts;
using PgBolt.E2E.JobsA;   // оба namespace импортированы одновременно:
using PgBolt.E2E.JobsB;   // сгенерированные PgBoltJobRegistry не конфликтуют (нет CS0121/CS0104)
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

public abstract class EndToEndTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private static readonly Guid Server = Guid.NewGuid();
    private TestDb _db = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Jobs_from_three_executor_assemblies_run_end_to_end()
    {
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = pg.ConnectionString(mode);
            o.Schema = _db.Schema;
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register);
            o.AddJobs(PgBolt.E2E.JobsB.PgBoltJobRegistry.Register);
            o.AddJobs(PgBoltJobRegistry.Register);                  // своя сборка: PgBolt.E2E.Tests.PgBoltJobRegistry
            o.AddJobs(PgBolt.E2E.JobsA.PgBoltJobRegistry.Register); // повтор идемпотентен
        });
        await using var sp = services.BuildServiceProvider();
        await sp.GetRequiredService<PgBoltInitializer>().StartAsync(Ct);

        var client = sp.GetRequiredService<IJobClient>();
        var store = sp.GetRequiredService<PgBoltStore>();
        var registry = sp.GetRequiredService<JobRegistry>();
        Assert.Equal(3, registry.All.Count);
        await store.PublishJobTypesAsync(registry.ToJobTypeRecords(), "e2e-server", Ct);

        var ids = new List<JobId>
        {
            (await client.EnqueueAsync(new PingArgs("hello"), ct: Ct)).Id!.Value,
            (await client.EnqueueAsync(new PongArgs(42), ct: Ct)).Id!.Value,
            (await client.EnqueueAsync(new LocalArgs(7), ct: Ct)).Id!.Value,
        };
        using (var dynamicArgs = JsonDocument.Parse("""{"Message":"dynamic"}"""))
            ids.Add((await client.EnqueueAsync("e2e.ping", dynamicArgs.RootElement, ct: Ct)).Id!.Value);

        var processed = 0;
        while (processed < ids.Count)
        {
            var leased = await store.DequeueAsync("e2e", 10, Server, TimeSpan.FromMinutes(1), Ct);
            Assert.NotEmpty(leased);
            foreach (var job in leased)
            {
                Assert.True(registry.TryGet(job.Type, out var executor), job.Type);
                await using var scope = sp.CreateAsyncScope();
                var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts, null, null, NullLogger.Instance);
                await executor.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes(job.ArgsJson), context, Ct);
            }
            processed += await store.AckAsync(Server, leased.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToList(), Ct);
        }

        Assert.Contains("hello", PingProbe.Messages);
        Assert.Contains("dynamic", PingProbe.Messages);
        Assert.Contains(42, PongProbe.Values);
        Assert.Contains(7, LocalProbe.Values);
        foreach (var id in ids)
            Assert.Equal(JobState.Succeeded, (await client.GetAsync(id, Ct))!.State);
    }
}

public sealed class EndToEndTests_Direct(PostgresFixture pg) : EndToEndTests(pg, ConnectionMode.Direct);
public sealed class EndToEndTests_Bouncer(PostgresFixture pg) : EndToEndTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 5: Добавить проекты в решение**

В `PgBolt.slnx` в папку `/tests/` добавить:

```xml
    <Project Path="tests/E2E/PgBolt.E2E.Contracts/PgBolt.E2E.Contracts.csproj" />
    <Project Path="tests/E2E/PgBolt.E2E.JobsA/PgBolt.E2E.JobsA.csproj" />
    <Project Path="tests/E2E/PgBolt.E2E.JobsB/PgBolt.E2E.JobsB.csproj" />
    <Project Path="tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj" />
```

- [x] **Step 6: Собрать и прогнать**

Run: `dotnet build PgBolt.slnx && dotnet test tests/E2E/PgBolt.E2E.Tests`
Expected: сборка без ошибок и предупреждений (нет CS0121/CS0104/BOLT*); PASS (2 теста: `EndToEndTests` × 2 режима; `SlimDataSourceTests` — ещё 2 — добавляются в Task 32).

- [x] **Step 7: Commit**

```bash
git add PgBolt.slnx tests/E2E
git commit -m "test(e2e): three executor assemblies with InternalsVisibleTo, full enqueue-to-ack cycle"
```

### Task 32: Slim data source и AOT-совместимость

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Test: `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`
- Modify (только если тест выявит недостающий `Enable*`): `src/PgBolt.Postgres/PgBoltDataSourceFactory.cs`, `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` §1

**Interfaces:**
- Consumes: весь `PgBoltStore`, `MigrationRunner`, `PartitionManager`.
- Produces: зафиксированный минимальный набор `Enable*` для `NpgsqlSlimDataSourceBuilder` (спека §1, §11 «Slim data source»).

- [x] **Step 1: Написать тест со всем SQL горячего пути на slim-источнике**

`tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`:

```csharp
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.E2E.Tests;

public abstract class SlimDataSourceTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    /// <summary>Минимальный набор, который спека §1 требует от пользователя.</summary>
    internal static void MinimalSlim(NpgsqlSlimDataSourceBuilder builder) => builder.EnableArrays();

    private static readonly Guid Server = Guid.NewGuid();
    private TestDb _db = null!;
    private NpgsqlDataSource _slim = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        _db = await TestDb.CreateAsync(pg, mode, migrate: false);
        var builder = new NpgsqlSlimDataSourceBuilder(pg.ConnectionString(mode));
        MinimalSlim(builder);
        _slim = builder.Build();
    }

    public async ValueTask DisposeAsync()
    {
        await _slim.DisposeAsync();
        await _db.DisposeAsync();
    }

    [Fact]
    public async Task Every_store_operation_works_on_slim_data_source()
    {
        await new MigrationRunner(_slim, _db.Schema).MigrateAsync(Ct);
        Assert.False((await new PartitionManager(_slim, _db.Schema, 80, TimeSpan.FromSeconds(2)).EnsureAsync(1, Ct)).TimedOut);
        var store = new PgBoltStore(_slim, _db.Schema, usePreparedStatements: false);
        var lease = TimeSpan.FromMinutes(1);

        EnqueueBatch One(string? key = null, TimeSpan? delay = null, int max = 3) =>
            new("slim.job", "slim", 1, max, [1, 2], "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                delay ?? TimeSpan.Zero, [Guid.CreateVersion7()], ["""{"a":1}"""], [null], [key]);

        var a = (await store.EnqueueAsync(One("k"), null, Ct))[0].Id!.Value;
        Assert.False((await store.EnqueueAsync(One("k"), null, Ct))[0].Created);
        var delayed = (await store.EnqueueAsync(One(delay: TimeSpan.FromHours(1)), null, Ct))[0].Id!.Value;
        Assert.Equal("slim", await store.TriggerAsync(delayed, Ct));
        await store.PromoteDueAsync(100, Ct);

        var leased = await store.DequeueAsync("slim", 10, Server, lease, Ct);
        Assert.Equal(2, leased.Count);
        Assert.Equal(2, (await store.HeartbeatAsync(Server, lease, Ct)).Count);
        var first = leased.Single(j => j.Id == a);
        var second = leased.Single(j => j.Id == delayed);
        Assert.Equal(FailOutcome.Retried, await store.RetryAsync(a, first.LeaseId, Server, TimeSpan.Zero, JobEventKind.Retry, "m", "e", Ct));
        Assert.Equal(1, await store.AckAsync(Server, [new LeaseRef(delayed, second.LeaseId)], Ct));

        var again = Assert.Single(await store.DequeueAsync("slim", 10, Server, lease, Ct));
        Assert.Equal(FailOutcome.Failed, await store.FailAsync(a, again.LeaseId, Server, JobEventKind.Failed, null, null, Ct));
        Assert.Equal(RequeueResult.Requeued, (await store.RequeueAsync(a, Ct)).Result);
        Assert.Equal(CancelResult.Cancelled, await store.CancelAsync(a, "stop", Ct));
        Assert.Equal(0, await store.SweepExpiredLeasesAsync(100, Ct));

        var info = await store.GetJobAsync(a, Ct);
        Assert.Equal(JobState.Cancelled, info!.State);
        Assert.NotEmpty(info.Events);

        await store.PublishJobTypesAsync([new JobTypeRecord("slim.job", """{"type":"object"}""", "slim", 3, 1, [1])], "slim", Ct);
        Assert.NotNull(await store.GetJobTypeAsync("slim.job", Ct));
    }
}

public sealed class SlimDataSourceTests_Direct(PostgresFixture pg) : SlimDataSourceTests(pg, ConnectionMode.Direct);
public sealed class SlimDataSourceTests_Bouncer(PostgresFixture pg) : SlimDataSourceTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Прогнать**

Run: `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~SlimDataSourceTests"`
Expected: PASS — минимальный набор `EnableArrays()` достаточен, спека §1 подтверждена.

Правило на случай FAIL с `InvalidCastException`/`NotSupportedException` вида «Reading/Writing as '…' is not supported for fields having DataTypeName '…'»: определить по сообщению недостающий резолвер (например, `jsonb` → `EnableJsonTypes()`), добавить вызов в `MinimalSlim`, в `PgBoltDataSourceFactory.Create` и в текст спеки §1 («он обязан вызвать `EnableArrays()` и …»), прогнать снова до PASS.

- [x] **Step 3: Проверить AOT/trim-анализ**

Run: `dotnet build src/PgBolt.Postgres -c Release -warnaserror && dotnet build src/PgBolt.Abstractions -c Release -warnaserror`
Expected: без предупреждений `IL2xxx`/`IL3xxx` (проекты помечены `IsAotCompatible=true`, что включает trim- и AOT-анализаторы).

- [x] **Step 4: Полный прогон**

Run: `dotnet test PgBolt.slnx`
Expected: PASS — все наборы.

- [x] **Step 5: Commit**

```bash
git add tests/E2E src/PgBolt.Postgres docs/superpowers/specs
git commit -m "test: hot path on slim data source; AOT/trim analysis clean"
```

---

## Покрытие спеки

| Раздел спеки | Задачи |
|---|---|
| §1 ограничения (.NET 10, Npgsql, AOT, PgBouncer, один statement, slim builder, READ COMMITTED по умолчанию) | 1, 4, 8, 25, 32 |
| §2 машина состояний, производные состояния | 11, 13, 19 |
| §3 UUIDv7, партиционирование по id | 5, 7 |
| §4.1 `queue`, `ready`, `lease_id`, инвариант cancel | 6, 11, 13, 15, 16, 17 |
| §4.2 `jobs`, контракт HOT | 6, 7, 12 |
| §4.3 `job_events`, коды, `@created_from` | 2, 5, 6, 19 |
| §4.4 партиции: `EnsurePartitionsAsync`, `LIKE` + `ATTACH`, `lock_timeout`, старт, фоновый повтор | 7, 25 |
| §4.5 `job_types` | 6, 19, 21, 22 |
| §4.6 `servers` | 6, 14 |
| §4.7 миграции, advisory lock, `AutoMigrate`, экспорт скрипта | 6, 25 |
| §5.1 enqueue, идемпотентность, порядок вставки, транзакция, изоляция, wakeup | 9, 10, 23 |
| §5.2 dequeue | 11, 16 (план) |
| §5.3 ack, порядок захвата | 12, 14 |
| §5.4 retry / NoExecutor / окончательный fail / правило отмены | 13 |
| §5.5 heartbeat | 14 |
| §5.6 sweeper | 15 |
| §5.7 cancel | 17, 24 |
| §5.8 trigger | 18, 24 |
| §5.9 requeue | 18, 24 |
| §5.10 wakeup | 22, 23, 24 |
| §5.11 promoter | 16 |
| §6.1 контракт задачи | 2, 21 |
| §6.2 клиент, правила параметров | 23, 24 |
| §6.3 регистрация, `AddJobs` | 5, 25, 31 |
| §7 генератор, `JobMetadata`, `PgBoltJobRegistry`, JSON-контекст, BOLT001–006, code fix | 3, 26–30 |
| §8 пакеты | 1, 30 |
| §9 обработка ошибок и повторы | 8, 9, 10, 17 |
| §10 риски (проверяемые в core) | 12 (HOT), 16 (EXPLAIN) |
| §11 тестирование | 4 (двойной прогон), 6–32 |

Отступление от §11: «тест со сдвигом часов клиента» (§11 «Время») отдельным тестом не пишется — системные часы процесса из теста не сдвинуть, а клиент не использует часы процесса для `run_at`. Требование покрыто структурно: в SQL уходит только интервал `@delay`, `run_at` считается от `now()` сервера; это проверяет `Delay_is_counted_from_database_now_and_row_is_not_ready` (Task 9).

Вне рамок core (по спеке): runtime воркеров и расписание периодических операций (часть 3), retention и периодический `EnsurePartitionsAsync` (часть 2), отмена `Awaiting`-задач (часть 5), бенчмарки §10 (часть 7).
