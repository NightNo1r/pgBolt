# PgBolt Worker runtime (часть 3) — план реализации

> **Для исполнителя-агента.** План самодостаточен: протокол исполнения, выбор моделей и требования к окружению описаны ниже. Шаги задач размечены чекбоксами (`- [ ]`). REQUIRED SUB-SKILL (если доступен): superpowers:subagent-driven-development или superpowers:executing-plans.

**Goal:** Реализовать пакет `PgBolt.Server` — runtime воркеров поверх core: виртуальные серверы, слоты, fetch-цикл с адаптивным polling, выполнение с фильтрами и таймаутом, пакетный ack, retry/NoExecutor, heartbeat по известным lease с проверкой срока lease каждого выполнения, promoter/sweeper, graceful shutdown без траты попыток — и точечные изменения core из §8 спеки.

**Architecture:** Один координирующий `IHostedService` (`PgBoltProcessingService`) на процесс владеет виртуальными серверами (`PgBoltServer`) и циклами обслуживания (`ProcessingMaintenance`). Каждый сервер состоит из независимо тестируемых компонентов: `SlotAllocator` (слоты и план выборки `[(queue, n)]` с заменяемой стратегией общего пула), `Dispatcher` (fetch-цикл по плану), `JobRunner` (выполнение и классификация исхода), `CompletionSink` (отправка исходов), `LeaseKeeper` (таблица выполнений, heartbeat, сроки lease). Доступ к БД — через внутренний `IServerStore` над `PgBoltStore`, время — через `TimeProvider`, поэтому вся логика runtime покрыта unit-тестами на `FakeTimeProvider` и фейковом store, а SQL и сквозные сценарии — интеграционными тестами на Testcontainers (напрямую и через PgBouncer).

**Tech Stack:** .NET 10 (C# 14), Npgsql 10, Microsoft.Extensions.Hosting 10, xUnit v3, Testcontainers (PostgreSQL 17 + PgBouncer 1.24), Microsoft.Extensions.TimeProvider.Testing, Verify.SourceGenerators.

**Spec:** `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md` (далее «спека», ссылки вида «§6.2»). Основа: `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` (ссылки вида «core §5.5»). Реализованный core-план — `docs/superpowers/plans/2026-09-27-pgbolt-core.md` (образец стиля кода и тестов).

## Global Constraints

- Только .NET 10 (`net10.0`); генератор — `netstandard2.0`. `PgBolt.Server`, `PgBolt.Postgres`, `PgBolt.Abstractions` собираются с `IsAotCompatible=true` и `TreatWarningsAsErrors=true` (из `Directory.Build.props`). Без reflection на горячем пути; DI-активация пользовательских типов (`IJobFilter`) — только с `[DynamicallyAccessedMembers(PublicConstructors)]`.
- Подключение напрямую или через PgBouncer в transaction mode. Запрещено: `LISTEN`/`NOTIFY`, session-level advisory locks, `SET` (кроме `SET LOCAL`), temp tables, постоянно удерживаемые соединения. Воркер не держит транзакцию дольше одного statement'а.
- Каждая операция горячего пути — один SQL-statement; SQL из спеки переносится дословно.
- Коды `jobs.state`: 0 Pending, 1 Awaiting, 2 Succeeded, 3 Failed, 4 Cancelled. Коды `job_events.kind`: 1 Retry, 2 Failed, 3 LeaseExpired, 4 Cancelled, 5 Requeued, 6 Triggered, 7 NoExecutor.
- Значения по умолчанию (§3.2, §3.3, §6.3): `Workers = Environment.ProcessorCount * 2`; `LeaseDuration = 60 с`; `HeartbeatInterval = 15 с` (≤ `LeaseDuration / 3`); `ShutdownTimeout = 25 с`; `DefaultJobTimeout = null`; `RetryPolicy = Exponential(15 с, 1 ч, 0.2)`; `AckBatchSize = 100`; `AckFlushInterval = 20 мс`; `MinPollInterval = 50 мс`; `MaxPollInterval = 10 с`; `PromoterInterval = 1 с` (±20 %); `SweeperInterval = 15 с` (±20 %); пакет promoter/sweeper = 1000.
- Производные таймауты (не настраиваются): dequeue — `HeartbeatInterval`; вызов heartbeat — `HeartbeatInterval / 2`; бюджет повторов отправки исхода — `LeaseDuration`; backoff повтора исхода 100 мс → 5 с; backoff ошибки dequeue 1 → 30 с.
- Верхние границы длительностей (§3.1–§3.3 спеки, ред. 4; нужны таймерам .NET — `Timer`, `CancellationTokenSource(delay, time)`, `Task.Delay`, `Task.WaitAsync` бросают `ArgumentOutOfRangeException` при задержке больше 2³² − 2 мс ≈ 49,7 сут): `LeaseDuration`, `ShutdownTimeout`, `AckFlushInterval`, `QueueOptions.MaxPollInterval` (а с ним и `MinPollInterval`), `PromoterInterval`, `SweeperInterval` — не больше 1 сут (`PgBoltServerOptions.MaxInterval`, запас на jitter ×1,2); `HeartbeatInterval` ≤ `LeaseDuration / 3` ≤ 8 ч; `DefaultJobTimeout` ≤ `PgBoltServerOptions.MaxJobTimeout` (2³² − 2 мс). Проверяет `Validate` (Task 8). `[Job(TimeoutSeconds)]` больше `MaxJobTimeout` не отклоняется при регистрации, а урезается до него в `JobRunner` (§3.5, §5.1): генератор предупреждает BOLT008 при `TimeoutSeconds > 4294967` (Task 2), runner пишет `Warning` один раз на тип задачи (Task 14).
- Все интервалы — монотонно через `TimeProvider.GetTimestamp()` (`MonoClock`, Task 8); все задержки и таймеры — через `TimeProvider` (`Task.Delay(d, time, ct)`, `time.CreateTimer`, `new CancellationTokenSource(d, time)`). `DateTime.Now`/`Stopwatch`/`Task.Delay` без `TimeProvider` в runtime запрещены.
- `message`/`exception` в `job_events` обрезаются до 8 КБ — 8192 байт в UTF-8 (так строка уходит в PostgreSQL), без разрыва суррогатной пары; одиночные суррогаты (невалидный UTF-16, строгий UTF-8 энкодер Npgsql их не принимает) заменяются на U+FFFD.
- `published_by = $"{Environment.MachineName}/{Environment.ProcessId}"`; `servers.host = Environment.MachineName`.
- Коммиты: Conventional Commits; автор и коммиттер — `NightNo1r <57496218+NightNo1r@users.noreply.github.com>` (`git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit ...`); строк `Co-Authored-By` **нет**.

## Требования к окружению

Проверить **до Task 1**. Если любой пункт не выполняется — остановиться и доложить.

| Требование | Проверка | Ожидаемо |
|---|---|---|
| .NET SDK 10 | `dotnet --version` | `10.0.*` |
| Docker | `docker info` | без ошибок |
| Образы | `docker pull postgres:17-alpine && docker pull edoburu/pgbouncer:v1.24.1-p1` | оба скачаны |
| nuget.org | `curl -sSfI https://api.nuget.org/v3/index.json` | `HTTP/2 200` |
| Сборка core зелёная | `dotnet test PgBolt.slnx` | все тесты проходят |

- Docker недоступен — **не** заменять базу моками и не пропускать интеграционные тесты; остановиться и доложить.
- Тестовые проекты — VSTest: `dotnet test tests/<Проект> --filter "FullyQualifiedName~<Класс>"` (флага `--project` нет).
- Если `dotnet restore` не находит версию пакета из `Directory.Packages.props`, взять последнюю стабильную той же major и зафиксировать (отметить в отчёте).

## Протокол исполнения

### Роли
- **Координатор** — основная сессия. Ведёт задачи строго по порядку (Task 1 → Task 21), запускает исполнителя и ревьюера, отмечает чекбоксы.
- **Исполнитель** — свежий субагент на задачу, модель из таблицы.
- **Ревьюер** — свежий субагент на задачу, всегда Opus 5.5 (`claude-opus-5-5`).

Если среда не поддерживает субагентов, координатор выполняет обе роли сам, последовательно, с тем же чек-листом.

### Модели

| Задачи | Исполнитель | Почему |
|---|---|---|
| 1, 3, 6, 7, 8, 9, 10, 21 | Sonnet 5 (`claude-sonnet-5`) | Код дан почти целиком, логика локальная |
| 2, 4, 5, 11–20 | Opus 5.5 (`claude-opus-5-5`) | Конкурентность, таймеры, SQL, генератор, контейнеры и прокси — нужна отладка сверх написанного |
| Ревью каждой задачи | Opus 5.5 | Сверка со спекой, поиск гонок |

### Цикл одной задачи
1. **Координатор → исполнитель:** дословно текст задачи, «Global Constraints», «Контракт внутренних типов» (ниже), путь к спеке и номера разделов из задачи, текущий SHA.
2. **Исполнитель:** падающий тест → убедиться, что падает → реализация → тесты проходят → коммит. Отчёт: `STATUS: DONE | BLOCKED`, SHA, итог прогона (`passed/failed`), отклонения от кода плана с причинами.
3. **Координатор → ревьюер:** текст задачи, разделы спеки, `git diff <SHA до>..HEAD`, отчёт. Ответ: `APPROVE` или `CHANGES` (файл:строка, что не так, чем подтверждается).
4. `CHANGES` → новый исполнитель исправляет новым коммитом, повторное ревью. Не более двух раундов; после третьего `CHANGES` — остановиться и доложить.
5. `APPROVE` → отметить чекбоксы задачи, коммит `chore(plan): mark task N done`.
6. В конце каждой фазы — `dotnet test PgBolt.slnx`; падение — остановиться и доложить.

### Чек-лист ревьюера
- Реализовано всё из шагов; тесты задачи есть и проходят (интеграционные — в Direct и Bouncer, где предусмотрено).
- SQL совпадает со спекой дословно, кроме отличий, явно перечисленных в задаче.
- Имена и сигнатуры совпадают с «Контрактом внутренних типов» — соседние задачи на них опираются.
- Нет `Task.Delay`/таймеров/`DateTime.UtcNow` мимо `TimeProvider` в runtime; нет блокирующих ожиданий (`.Result`, `.Wait()`) в runtime.
- Тесты не ослаблены: нет `Skip`, удалённых assert'ов, пустых `catch`, подгонки ожидаемых значений; unit-тесты runtime не используют реальное время (`Thread.Sleep`, `Task.Delay` без `FakeTimeProvider`), кроме явно оговорённых мест.
- Сборка без предупреждений (`TreatWarningsAsErrors`, AOT/trim-анализ).
- Коммиты — Conventional Commits, автор NightNo1r, без `Co-Authored-By`.

### Правила для исполнителя
- Не делать работу будущих задач. Не менять тесты ради прохождения, если задача этого не предписывает.
- Код плана — эталон; отклонения — только для устранения ошибки компиляции, несовместимости API или явной ошибки плана; каждое — в отчёт.
- Две безуспешные попытки исправить один падающий тест → `STATUS: BLOCKED` с выводом и гипотезами.
- Не использовать `--no-verify`, не переписывать историю, не пушить с `--force`.

## Структура файлов

```
CHANGELOG.md                      — breaking changes IWakeupSignal/HeartbeatAsync (§8.2)  (Task 21)
src/PgBolt.Abstractions/
  JobAttribute.cs                 — + TimeoutSeconds                                   (Task 1)
  JobDescriptor.cs                — + TimeoutSeconds (init), SameAs                   (Task 1)
  JobAbortedException.cs  JobArgumentsException.cs                                    (Task 1)
src/PgBolt.Generators/
  Models/JobTypeModel.cs  Emitter.cs  Analysis/Diagnostics.cs  Analysis/AnalysisState.cs
  AnalyzerReleases.Unshipped.md   — TimeoutSeconds в дескрипторе, BOLT007, BOLT008    (Task 2)
src/PgBolt.Postgres/
  Jobs/JobsBuilder.cs             — invoker → JobArgumentsException                   (Task 3)
  Storage/PgBoltStore.Heartbeat.cs — heartbeat по списку lease                        (Task 4)
  Storage/PgBoltStore.Release.cs  Storage/PgBoltStore.Servers.cs  Storage/StoreRecords.cs (Task 5)
  Client/IWakeupSignal.cs  Client/InProcessWakeupSignal.cs                            (Task 6)
  Hosting/PgBoltInitializer.cs  PgBolt.Postgres.csproj (InternalsVisibleTo)          (Task 7)

src/PgBolt.Server/                                                                     (Task 8+)
  PgBolt.Server.csproj
  PgBoltServerOptions.cs  QueueOptions.cs  PgBoltProcessingOptions.cs                  (Task 8; FilterRegistrations — Task 16)
  IJobFilter.cs  JobExecutingContext.cs                                                (Task 8: нужны AddFilter<T>; XML-доки — Task 14)
  RetryPolicy.cs  JobExecutorNotFoundException.cs                                      (Task 9)
  PgBoltServerServiceCollectionExtensions.cs                                           (Task 16)
  Runtime/MonoClock.cs                                                                 (Task 8)
  Runtime/RetryDecider.cs                                                              (Task 9)
  Runtime/SlotAllocator.cs        — + ISharedPoolDistribution, StrictOrderDistribution (Task 10)
  Runtime/Execution.cs  Runtime/ExecutionTable.cs  Runtime/JobOutcome.cs               (Task 11)
  Runtime/IServerStore.cs  Runtime/PgBoltServerStore.cs                                (Task 11)
  Runtime/LeaseKeeper.cs          — + Unregister (откат Register, Task 15)              (Task 12)
  Runtime/CompletionSink.cs                                                            (Task 13)
  Runtime/FilterPipeline.cs  Runtime/JobRunner.cs  Runtime/OutcomeClassifier.cs        (Task 14)
  Runtime/AsyncAutoResetEvent.cs  Runtime/IFetchStrategy.cs  Runtime/Dispatcher.cs     (Task 15)
  Runtime/PgBoltServer.cs  Runtime/ProcessingMaintenance.cs
  Runtime/PgBoltProcessingService.cs  Runtime/ServerDefinition.cs                     (Task 16)

tests/E2E/PgBolt.E2E.Tests/
  SlimDataSourceTests.cs          — новые операции store на slim data source           (Tasks 4, 5)
tests/PgBolt.Testing/
  TcpProxy.cs                     — управляемый TCP-прокси (пауза/обрыв)               (Task 17)
tests/PgBolt.Server.Tests/                                                             (Task 8+)
  PgBolt.Server.Tests.csproj  AssemblyInfo.cs
  Fakes/TestLogger.cs             — ILogger, собирающий записи                          (Task 9)
  Fakes/FakeServerStore.cs  Fakes/Leased.cs  Fakes/Eventually.cs                       (Task 11)
  Fakes/ListLoggerFactory.cs      — ILoggerFactory, собирающий записи                   (Task 13)
  Unit/*Tests.cs                  — unit-тесты компонентов                             (Tasks 8–16)
  Integration/ServerTestHost.cs  Integration/TestJobs.cs                               (Task 17)
  Integration/*Tests.cs           — сквозные сценарии                                  (Tasks 17–20)
```

## Контракт внутренних типов

Эталон имён и сигнатур. Задача, создающая тип, реализует его ровно так; остальные задачи на него опираются. Изменение контракта — только через отклонение в отчёте с согласованием координатора.

### Core (`PgBolt.Postgres`, `PgBolt.Abstractions`)

```csharp
// PgBolt.Abstractions (Task 1)
public sealed class JobAttribute { /* ... */ public int TimeoutSeconds { get; init; } }          // 0 = нет таймаута
public sealed record JobDescriptor(string Name, string Queue, int MaxAttempts, short Priority, int[]? RetryDelaysSeconds)
{ public int TimeoutSeconds { get; init; } /* SameAs сравнивает и его */ }
public class JobAbortedException(string message, Exception? inner = null) : Exception(message, inner);
public sealed class JobArgumentsException(string message, Exception? inner = null) : Exception(message, inner);

// PgBolt.Postgres.Storage (Tasks 4, 5)
public enum ReleaseOutcome { LeaseLost, Released, Cancelled }
public sealed partial class PgBoltStore
{
    public Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct);
    public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct);
    public Task RegisterServerAsync(Guid serverId, string name, string host, string[] queues, int capacity, CancellationToken ct);
    public Task RemoveServerAsync(Guid serverId, CancellationToken ct);
    /// <summary>Имена job_types, у которых default_queue ∈ queues (для предупреждения §4.4 п. 4).</summary>
    public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct);
}

// PgBolt.Postgres.Client (Task 6)
public interface IWakeupSignal
{
    void Signal(string queue);
    IDisposable Subscribe(string queue, Action callback);
}

// PgBolt.Postgres.Hosting (Task 7)
public sealed class PgBoltInitializer { internal Task Ready { get; } }
```

### `PgBolt.Server` — публичное

```csharp
namespace PgBolt.Server;

public sealed class QueueOptions                                                   // Task 8
{
    public string Name { get; }                       // задаётся PgBoltServerOptions.Queue
    public int Reserved { get; set; }                 // 0
    public int? MaxConcurrency { get; set; }          // null
    public TimeSpan MinPollInterval { get; set; }     // 50 мс
    public TimeSpan MaxPollInterval { get; set; }     // 10 с
}

public sealed class PgBoltServerOptions                                            // Task 8
{
    public int Workers { get; set; }
    public TimeSpan LeaseDuration { get; set; }
    public TimeSpan HeartbeatInterval { get; set; }
    public TimeSpan ShutdownTimeout { get; set; }
    public TimeSpan? DefaultJobTimeout { get; set; }
    public RetryPolicy RetryPolicy { get; set; }
    public int AckBatchSize { get; set; }
    public TimeSpan AckFlushInterval { get; set; }
    public PgBoltServerOptions Queue(string name, Action<QueueOptions>? configure = null);
    public PgBoltServerOptions AddFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>() where T : class, IJobFilter;
    internal IReadOnlyList<QueueOptions> Queues { get; }
    internal IReadOnlyList<Type> FilterTypes { get; }
    internal int Capacity { get; }                    // Workers + Σ Reserved
    internal void Validate(string serverName);        // InvalidOperationException с именем сервера и поля
    internal static readonly TimeSpan MaxInterval;    // 1 сут: верхняя граница LeaseDuration, ShutdownTimeout, AckFlushInterval, MaxPollInterval, интервалов обслуживания
    internal static readonly TimeSpan MaxJobTimeout;  // 2³² − 2 мс ≈ 49,7 сут (предел таймеров .NET): DefaultJobTimeout, урезание [Job(TimeoutSeconds)]
}

public sealed class PgBoltProcessingOptions                                        // Task 8
{
    public bool RunMaintenance { get; set; }          // true
    public TimeSpan PromoterInterval { get; set; }    // 1 с
    public TimeSpan SweeperInterval { get; set; }     // 15 с
    internal void Validate();                         // интервалы > 0 и ≤ PgBoltServerOptions.MaxInterval
}

public abstract class RetryPolicy                                                  // Task 9
{
    public abstract TimeSpan? GetDelay(int attempt, Exception error);
    public static RetryPolicy Exponential(TimeSpan baseDelay, TimeSpan maxDelay, double jitter);
    public static RetryPolicy Custom(Func<int, Exception, TimeSpan?> getDelay);
    public static RetryPolicy Default { get; }        // Exponential(15 с, 1 ч, 0.2)
}

public sealed class JobExecutorNotFoundException(string jobName) : Exception       // Task 9
{ public string JobName { get; } }

public delegate ValueTask JobExecutionDelegate();                                  // Task 14
public interface IJobFilter { ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next); }
public sealed class JobExecutingContext
{
    public JobContext Job { get; }
    public IServiceProvider Services { get; }
    public ReadOnlyMemory<byte> ArgsUtf8 { get; }
    public CancellationToken CancellationToken { get; }
}

public static class PgBoltServerServiceCollectionExtensions                        // Task 16
{   // namespace Microsoft.Extensions.DependencyInjection
    public static IServiceCollection AddPgBoltServer(this IServiceCollection services, string name, Action<PgBoltServerOptions> configure);
    public static IServiceCollection AddPgBoltJobFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(this IServiceCollection services) where T : class, IJobFilter;
    public static IServiceCollection ConfigurePgBoltProcessing(this IServiceCollection services, Action<PgBoltProcessingOptions> configure);
}
```

### `PgBolt.Server.Runtime` — внутреннее

```csharp
namespace PgBolt.Server.Runtime;

internal sealed class MonoClock(TimeProvider time)                                  // Task 8
{
    public TimeProvider Time { get; }
    public long Now { get; }                                     // time.GetTimestamp()
    public long Add(long timestamp, TimeSpan duration);          // timestamp + duration в единицах TimestampFrequency
    public TimeSpan Until(long timestamp);                       // max(0, timestamp − Now) как TimeSpan
    public TimeSpan Elapsed(long since);                         // Now − since как TimeSpan (≥ 0)
}

internal sealed class RetryDecider(RetryPolicy policy, ILogger logger)             // Task 9
{
    /// retryDelaysSeconds задачи (элемент [attempt−1], при нехватке — последний; пустой = null) → иначе policy.
    /// Исключение из policy — лог, результат null. Возвращает null = не повторять.
    public TimeSpan? GetDelay(int attempt, int[]? retryDelaysSeconds, Exception error);
}

internal sealed class SlotAllocator                                                  // Task 10, потокобезопасен (lock)
{
    public SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues);                 // стратегия — StrictOrderDistribution
    internal SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, ISharedPoolDistribution distribution);
    /// План итерации [(queue, n)] (§4.2, §4.3): ленивый, следующая запись вычисляется после Take предыдущей
    /// (недобор общих слотов переходит к следующим очередям); очередь — не больше раза за план; n ≥ 1; isDue — nextPoll наступил.
    public IEnumerable<(string Queue, int N)> Plan(Func<string, bool> isDue);
    public int Available(string queue);      // min(свободные Reserved очереди + свободные общие, MaxConcurrency − занято), ≥ 0
    public bool Take(string queue);          // занимает один слот: true = из Reserved, false = общий; InvalidOperationException, если Available == 0
    public void Release(string queue, bool fromReserved);
    public int InUse(string queue);
}

/// Распределение общего пула между очередями (§4.2) — внутренняя стратегия SlotAllocator; веса части 3b заменяют её
/// без изменения Dispatcher. Вызывается под lock allocator'а.
internal interface ISharedPoolDistribution                                          // Task 10
{
    /// Следующая запись плана: индекс очереди (порядок объявления) и n ≥ 1, либо null — план исчерпан.
    /// candidate(i) — очередь не выдавалась в этом плане и её nextPoll наступил; available(i) — Available очереди i.
    /// null допустим, только когда ни у одной кандидатки нет available ≥ 1; иначе allocator бросает InvalidOperationException
    /// (пустой план при готовой очереди диспетчер не отличил бы от простоя: NextPollDelay = 0 → цикл без ожидания).
    (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available);
}
internal sealed class StrictOrderDistribution : ISharedPoolDistribution;           // Task 10: первая кандидатка по порядку с available ≥ 1

internal enum ExecutionState { Running, OutcomePending, OutcomeInFlight, Removed }  // Task 11
internal enum CancelReason { None, Shutdown, CancelRequested, HeartbeatLost, LeaseLost, Timeout }

internal abstract record JobOutcome                                                  // Task 11
{
    public sealed record Ack : JobOutcome;
    public sealed record Retry(TimeSpan Delay, JobEventKind Kind, string? Message, string? Exception) : JobOutcome;
    public sealed record Fail(JobEventKind Kind, string? Message, string? Exception) : JobOutcome;
    public sealed record ConfirmCancel(string? Message) : JobOutcome;
    public sealed record Release(bool Ran) : JobOutcome;
}

internal sealed class Execution                                                      // Task 11
{
    public Execution(LeasedJob job, string queue, long validUntil, ExecutionState initialState);
    public LeasedJob Job { get; }
    public string Queue { get; }
    public LeaseRef Lease { get; }                         // new(Job.Id, Job.LeaseId)
    public bool FromReserved { get; set; }                 // слот, выданный SlotAllocator.Take
    public CancellationToken Token { get; }                // отменяется Cancel(...)
    public CancelReason FirstReason { get; }               // первая причина отмены, None — не отменялось
    public bool Cancel(CancelReason reason);               // CAS FirstReason None→reason (true, если первая), затем отмена Token (всегда)
    public long ValidUntil { get; }                        // Volatile
    public void ExtendValidUntil(long candidate);          // CAS max
    public ExecutionState State { get; }                   // Volatile
    public bool TryTransition(ExecutionState from, ExecutionState to);   // CAS; переходы вне §6.2 — InvalidOperationException
    public JobOutcome? Outcome { get; set; }               // ставится до перехода в OutcomePending
    public long OutcomeFirstAttemptAt { get; set; }        // момент первой попытки отправки (бюджет LeaseDuration), 0 — не начиналась
}

internal sealed class ExecutionTable                                                 // Task 11, ConcurrentDictionary по LeaseId
{
    public void Add(Execution execution);
    public bool Remove(Execution execution);
    public bool TryGet(Guid leaseId, [MaybeNullWhen(false)] out Execution execution);
    public IReadOnlyList<Execution> Snapshot();
    public int Count { get; }
}

internal sealed record ServerRegistration(Guid Id, string Name, string Host, string[] Queues, int Capacity);

internal interface IServerStore                                                      // Task 11
{
    Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct);
    Task<int> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct);
    Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind, string? message, string? exception, CancellationToken ct);
    Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind, string? message, string? exception, CancellationToken ct);
    Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct);
    Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct);
    Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct);
    Task RegisterServerAsync(ServerRegistration server, CancellationToken ct);
    Task RemoveServerAsync(Guid serverId, CancellationToken ct);
    Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct);
    Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct);
    Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct);
    Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct);
}
internal sealed class PgBoltServerStore(PgBoltStore store) : IServerStore;          // Task 11, чистый проброс

internal sealed class LeaseKeeper                                                   // Task 12
{
    public LeaseKeeper(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options,
                       MonoClock clock, ILogger logger, Action onRecovered);
    public bool HeartbeatLost { get; }                      // now ≥ lastSuccess + LeaseDuration − HeartbeatInterval (§6.2)
    public Execution Register(LeasedJob job, string queue, long validUntil);            // Running, Add в таблицу, перевзвод таймера сроков
    public Execution RegisterForRelease(LeasedJob job, string queue, long validUntil);  // OutcomePending, Outcome = Release(false)
    public bool Unregister(Execution execution);            // Task 15: откат Register незапущенного выполнения — CAS Running→Removed, Remove из таблицы; не бросает
    public void Start();                                    // heartbeat-цикл + таймер сроков
    public Task StopAsync();                                // останавливает цикл и таймер, ждёт текущий вызов
    internal Task HeartbeatOnceAsync(CancellationToken ct); // один вызов (шаги §6.2 п. 1–6), для тестов
    internal void CheckDeadlines();                          // проверка сроков lease (для тестов; её же вызывает таймер)
}

internal sealed class CompletionSink                                                // Task 13
{
    public CompletionSink(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options,
                          MonoClock clock, ILogger logger);
    public bool Complete(Execution execution, JobOutcome outcome);   // Running→OutcomePending и постановка в отправку; false — исход отброшен (Removed / sink остановлен)
    public void EnqueueRegistered(Execution execution);              // для RegisterForRelease: запись уже OutcomePending с Outcome; sink остановлен — Removed, не отправляется
    public void Start();
    public Task StopAsync(CancellationToken ct);                     // отправить всё накопленное (с повтором по §6.1), затем перестать принимать;
                                                                     // при отмене ct — отправки прерываются, ack из буфера отбрасываются с логом
    // каждая попытка отправки ограничена остатком бюджета LeaseDuration (WaitAsync — и при store, игнорирующем токен)
}

internal sealed class FilterPipeline(IReadOnlyList<Type> filterTypes)              // Task 14
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate terminal);
}

internal static class OutcomeClassifier                                             // Task 14
{
    /// error == null — успех. Реализует таблицу §5.2 (строки 2–6 и NoExecutor; строка 1 — CompletionSink/Removed).
    public static JobOutcome Classify(Execution execution, Exception? error, RetryDecider retry, TimeSpan? timeout);
    public static JobOutcome NoExecutor(Execution execution, RetryDecider retry);
    public static string Truncate(string? text);            // 8192 байт UTF-8 без разрыва суррогатной пары; одиночный суррогат → U+FFFD
}

internal sealed class JobRunner                                                      // Task 14
{
    public JobRunner(JobRegistry registry, IServiceScopeFactory scopes, FilterPipeline filters, RetryDecider retry,
                     PgBoltServerOptions options, MonoClock clock, ILoggerFactory loggers);
    /// Выполняет задачу по §5.1 и возвращает исход; не бросает (кроме отмены самого вызова хостом — не бывает).
    public Task<JobOutcome> RunAsync(Execution execution, CancellationToken shutdown);
}

internal sealed class AsyncAutoResetEvent                                           // Task 15
{
    public void Set();                                       // latched: Set без ожидающего сохраняется до следующего WaitAsync
    public Task WaitAsync(TimeSpan timeout, TimeProvider time, CancellationToken ct);   // завершается по Set или timeout, не бросает на timeout
}

internal interface IFetchStrategy { Task<IReadOnlyList<LeasedJob>> FetchAsync(string queue, int max, CancellationToken ct); }  // Task 15
internal sealed class DequeueFetchStrategy(IServerStore store, Guid serverId, TimeSpan lease) : IFetchStrategy;

internal sealed class Dispatcher                                                     // Task 15
{
    public Dispatcher(PgBoltServerOptions options, SlotAllocator slots, IFetchStrategy fetch, LeaseKeeper keeper,
                      CompletionSink sink, IWakeupSignal wakeup, MonoClock clock, ILogger logger,
                      Action<Execution> startExecution);            // startExecution: либо запускает выполнение, либо бросает, не запустив
    public void Start();
    public void SignalStop();                                // новые dequeue не начинаются; in-flight dequeue не отменяется
    public Task Completion { get; }                          // цикл завершён, in-flight dequeue дождан
    public void OnSlotFreed(string queue);                   // флаг slotFreed[q] + wake
    public void Wake();                                      // wake без флагов (снятие паузы heartbeat)
}

internal sealed record ServerDefinition(string Name, PgBoltServerOptions Options);   // Task 16

internal sealed class PgBoltServer                                                   // Task 16
{
    public PgBoltServer(ServerDefinition definition, IServerStore store, JobRegistry registry, IServiceScopeFactory scopes,
                        IReadOnlyList<Type> globalFilters, IWakeupSignal wakeup, MonoClock clock, ILoggerFactory loggers);
    public Guid Id { get; }
    public string Name { get; }
    public PgBoltServerOptions Options { get; }
    public Task RegisterAsync(CancellationToken ct);         // §4.4 п. 3–4: регистрация в servers, предупреждение NoExecutor
    public void Start();                                     // §4.4 п. 6: LeaseKeeper, Sink, Dispatcher, watchdog — после регистрации всех серверов
    public void BeginStop();                                 // §6.4 п. 1: dispatcher.SignalStop + отмена shutdown-CTS
    public Task WaitExecutionsAsync(CancellationToken ct);   // §6.4 п. 2–3: dispatcher.Completion + выполнения до ShutdownTimeout от BeginStop; брошенные → Removed
    public Task FlushAndStopAsync(CancellationToken ct);     // §6.4 п. 4–6: sink.StopAsync → keeper.StopAsync → RemoveServerAsync (при отменённом ct — без RemoveServerAsync)
}

internal sealed class ProcessingMaintenance(IServerStore store, IWakeupSignal wakeup, PgBoltProcessingOptions options,
                                            MonoClock clock, ILogger logger)          // Task 16
{
    public void Start();
    public Task StopAsync();
    internal Task<int> PromoteOnceAsync(CancellationToken ct);   // до неполного пакета; для тестов
    internal Task<int> SweepOnceAsync(CancellationToken ct);
}

internal sealed class PgBoltProcessingService : IHostedService                     // Task 16, единственный hosted service
{
    public PgBoltProcessingService(IServiceProvider services);
    internal IReadOnlyList<PgBoltServer> Servers { get; }
}
```

### Тестовая поддержка (`tests/PgBolt.Server.Tests`)

```csharp
namespace PgBolt.Server.Tests.Fakes;                                                 // Task 11

internal sealed record StoreCall(string Op, Guid? JobId = null, Guid? LeaseId = null, object? Detail = null);

/// Программируемый IServerStore: поведение задаётся делегатами, все вызовы пишутся в Calls.
internal sealed class FakeServerStore : IServerStore
{
    public ConcurrentQueue<StoreCall> Calls { get; } = new();
    public Func<string, int, CancellationToken, Task<IReadOnlyList<LeasedJob>>> OnDequeue { get; set; }      // по умолчанию []
    public Func<IReadOnlyList<LeaseRef>, CancellationToken, Task<int>> OnAck { get; set; }                   // по умолчанию leases.Count
    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnRetry { get; set; }                      // по умолчанию Retried
    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnFail { get; set; }                       // по умолчанию Failed
    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnConfirmCancel { get; set; }              // по умолчанию Cancelled
    public Func<Guid, Guid, bool, CancellationToken, Task<ReleaseOutcome>> OnRelease { get; set; }           // по умолчанию Released
    public Func<IReadOnlyCollection<LeaseRef>, CancellationToken, Task<IReadOnlyList<HeartbeatLease>>> OnHeartbeat { get; set; }
                                                                  // по умолчанию: каждый lease из списка с CancelRequested = false
    public Func<int, CancellationToken, Task<PromoteResult>> OnPromote { get; set; }                        // по умолчанию (0, [])
    public Func<int, CancellationToken, Task<int>> OnSweep { get; set; }                                    // по умолчанию 0
    public Func<IReadOnlyList<string>, CancellationToken, Task<IReadOnlyList<string>>> OnJobTypeNames { get; set; } // по умолчанию []
    public Func<ServerRegistration, CancellationToken, Task> OnRegisterServer { get; set; }                  // по умолчанию Task.CompletedTask
    public IEnumerable<StoreCall> CallsOf(string op);   // op: "Dequeue", "Ack", "Retry", "Fail", "ConfirmCancel", "Release",
                                                        //     "Heartbeat", "RegisterServer", "RemoveServer", "Promote", "Sweep",
                                                        //     "PublishJobTypes", "JobTypeNames"
    // Detail: Dequeue — (string queue, int max); Ack — LeaseRef[]; Retry — (TimeSpan delay, JobEventKind kind, string? message);
    //         Fail — (JobEventKind kind, string? message); ConfirmCancel — string? message; Release — bool ran;
    //         Heartbeat — LeaseRef[]; RegisterServer — ServerRegistration; Publish — (IReadOnlyList<JobTypeRecord>, string publishedBy)
}

internal static class Leased                                                        // Task 11
{
    public static LeasedJob Job(Guid? id = null, string type = "test.job", string args = "{}", int attempt = 1,
                                int maxAttempts = 3, int[]? retryDelays = null);    // LeaseId = Guid.NewGuid()
}
```

Интеграционная поддержка (`Integration/ServerTestHost.cs`, `Integration/TestJobs.cs`, `PgBolt.Testing/TcpProxy.cs`) определяется в Task 17 и используется Tasks 18–20.

### Дополнения к контракту (согласованы между задачами)

| Тип / член | Задача | Назначение |
|---|---|---|
| `IJobFilter`, `JobExecutionDelegate`, `JobExecutingContext` (с `internal` конструктором `(JobContext, IServiceProvider, ReadOnlyMemory<byte>, CancellationToken)`) | 8 | Создаются вместе с опциями: `AddFilter<T>` ограничен `IJobFilter`. Task 14 их не создаёт заново |
| `PgBoltServerOptions.RetryPolicy` (умолчание `RetryPolicy.Default`) | 9 | Свойство появляется вместе с типом `RetryPolicy` |
| `internal RetryPolicy.Exponential(baseDelay, maxDelay, jitter, Func<double> random)` | 9 | Детерминированный jitter в тестах |
| `internal Func<double> LeaseKeeper.Jitter` | 12 | Детерминированное расписание heartbeat |
| `CompletionSink.BackoffWaits`, `Dispatcher.IdleWaits`, `ProcessingMaintenance.IdleWaits`, `NextRandom` | 13, 15, 16 | Тестовые хуки: «дождаться счётчика ожиданий, затем `Advance`» |
| `internal static readonly TimeSpan PgBoltServerOptions.MaxInterval` (1 сут), `MaxJobTimeout` (2³² − 2 мс) | 8 | Верхние границы длительностей в `Validate` (предел таймеров .NET); `MaxJobTimeout` урезает `[Job(TimeoutSeconds)]` в `JobRunner` с `Warning` один раз на тип задачи (Task 14; BOLT008 при компиляции — Task 2) |
| `public bool LeaseKeeper.Unregister(Execution)` | 15 | Откат `Register`, если диспетчер не смог запустить выполнение: запись не остаётся `Running` и не продлевается heartbeat'ом. Не бросает, поэтому `StartOne` вызывает его до `slots.Release` |
| `internal int PgBoltServer.CancelledExecutions` | 16 | Тестовый хук: число выполнений, у которых записан момент отмены (предупреждение §5.1); тест ждёт его перед `Advance` |
| `internal IReadOnlyList<Action<IServiceCollection>> PgBoltServerOptions.FilterRegistrations` | 16 | `AddFilter<T>` сохраняет `s => s.TryAddScoped<T>()`: регистрация по `Type` из списка даёт IL2072 |
| `internal sealed record GlobalJobFilter(Type FilterType)` | 16 | Хранение глобальных фильтров в DI |
| `internal PgBoltProcessingService(IServiceProvider, Func<CancellationToken, Task> waitReady)`, `internal ProcessingMaintenance? Maintenance` | 16 | Unit-тесты координатора без настоящего `PgBoltInitializer.Ready` |
| Пакеты `Microsoft.Extensions.Hosting` (Task 8, `HostOptions`) и `Microsoft.Extensions.Logging` (Task 17, `AddLogging` в тестах) | 8, 17 | `Directory.Packages.props`, версия 10.0.0 |
| `TestLogger`/`LogEntry` (Fakes, Task 9), `ListLoggerFactory`/`ListLogEntry` (Fakes, Task 13), `CollectingLoggerProvider`/`LogEntry` (Integration, Task 17) | 9, 13, 17 | Сбор логов в тестах; разные пространства имён/имена записей, конфликта нет |
| Строже контракта: `ExecutionTable.Add` бросает на дубликат `lease_id`, `Snapshot()` без `Removed`; `SlotAllocator` бросает на неизвестную очередь, лишний `Release`, некорректную запись плана от стратегии и `null` от стратегии при готовой очереди со свободным слотом; `Execution` стартует только `Running`/`OutcomePending`, `Cancel(None)` бросает | 10, 11 | Раннее обнаружение ошибок |
| После Task 20 обычный прогон — `dotnet test PgBolt.slnx --filter "Category!=Load"` | 20 | Нагрузочный smoke запускается отдельно |

## Review Focus

Входы и режимы отказа, которые спека подразумевает, но легко упустить; тест на каждый добавлен в задачу-владельца.

1. **Задача, игнорирующая `CancellationToken`, при остановке** — остальные задачи и серверы останавливаются вовремя, брошенная не отправляет исход позже, её lease истекает (Task 16 unit, Task 18 интеграция).
2. **`HeartbeatInterval` на нижней границе (`LeaseDuration = 3 × HeartbeatInterval`) и медленный, но успешный heartbeat** — нет ложного `HeartbeatLost` (Task 12).
3. **Очередь, чей исполнитель бросает синхронно (до первого `await`) или возвращает `null`-Task** — классифицируется как обычное исключение, слот освобождается (Task 14).
4. **Одна и та же задача повторно берётся тем же сервером после sweep** (два `Execution` с разными `lease_id` на один `job_id`) — таблица, heartbeat и sink различают их по `lease_id` (Tasks 11, 19).
5. **`RetryPolicy`, бросающая исключение, и `retry_delays` с отрицательными/нулевыми элементами** — бросающая политика даёт окончательный fail с логом Error; нулевой или отрицательный элемент `retry_delays` (и отрицательная задержка от политики) — немедленный retry с задержкой `0` (`ready = true`) (Task 9).

---

## Фаза 0. Изменения core

Точечные изменения `PgBolt.Abstractions`, генератора и `PgBolt.Postgres` из §3.5, §7 и §8 спеки. Runtime `PgBolt.Server` (Task 8+) опирается на них через «Контракт внутренних типов». В конце фазы — `dotnet test PgBolt.slnx`.

### Task 1: `TimeoutSeconds`, `JobAbortedException`, `JobArgumentsException` в `PgBolt.Abstractions`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §3.5, §8.4, §9.

**Files:**
- Modify: `src/PgBolt.Abstractions/JobAttribute.cs`
- Modify: `src/PgBolt.Abstractions/JobDescriptor.cs`
- Create: `src/PgBolt.Abstractions/JobAbortedException.cs`
- Create: `src/PgBolt.Abstractions/JobArgumentsException.cs`
- Test: `tests/PgBolt.Abstractions.Tests/ContractsTests.cs`, `tests/PgBolt.Abstractions.Tests/JobMetadataTests.cs`

**Interfaces:**
- Consumes: `JobAttribute`, `JobDescriptor`, `JobMetadataRegistry` (core).
- Produces (namespace `PgBolt`):
  - `public sealed class JobAttribute { ... public int TimeoutSeconds { get; init; } }` — `0` = нет таймаута.
  - `public sealed record JobDescriptor(string Name, string Queue, int MaxAttempts, short Priority, int[]? RetryDelaysSeconds) { public int TimeoutSeconds { get; init; } }` — `SameAs` сравнивает и `TimeoutSeconds`.
  - `public class JobAbortedException(string message, Exception? inner = null) : Exception(message, inner);` — **не** `sealed` (по контракту: пользователь может наследовать).
  - `public sealed class JobArgumentsException(string message, Exception? inner = null) : Exception(message, inner);`

- [x] **Step 1: Написать падающие тесты**

В `tests/PgBolt.Abstractions.Tests/ContractsTests.cs` добавить в класс `ContractsTests` (после `UnknownJobException_carries_name`):

```csharp
    [Fact]
    public void JobAttribute_TimeoutSeconds_defaults_to_zero_and_is_settable()
    {
        Assert.Equal(0, new JobAttribute("a.job").TimeoutSeconds);
        Assert.Equal(30, new JobAttribute("a.job") { TimeoutSeconds = 30 }.TimeoutSeconds);
    }

    [Fact]
    public void JobDescriptor_TimeoutSeconds_defaults_to_zero_and_participates_in_SameAs()
    {
        var a = new JobDescriptor("x", "q", 3, 1, [1, 2]);
        var b = a with { TimeoutSeconds = 30 };

        Assert.Equal(0, a.TimeoutSeconds);
        Assert.Equal(30, b.TimeoutSeconds);
        Assert.False(a.SameAs(b));
        Assert.False(b.SameAs(a));
        Assert.True(b.SameAs(new JobDescriptor("x", "q", 3, 1, [1, 2]) { TimeoutSeconds = 30 }));
        Assert.Contains("TimeoutSeconds = 30", b.ToString());
    }

    [Fact]
    public void JobAbortedException_carries_message_and_inner_and_is_not_sealed()
    {
        var inner = new InvalidOperationException("root");
        var e = new JobAbortedException("retry is pointless", inner);

        Assert.Equal("retry is pointless", e.Message);
        Assert.Same(inner, e.InnerException);
        Assert.Null(new JobAbortedException("m").InnerException);
        Assert.False(typeof(JobAbortedException).IsSealed);
    }

    [Fact]
    public void JobArgumentsException_carries_message_and_inner()
    {
        var inner = new FormatException("bad json");
        var e = new JobArgumentsException("cannot deserialize", inner);

        Assert.Equal("cannot deserialize", e.Message);
        Assert.Same(inner, e.InnerException);
        Assert.Null(new JobArgumentsException("m").InnerException);
        Assert.True(typeof(JobArgumentsException).IsSealed);
    }
```

В `tests/PgBolt.Abstractions.Tests/JobMetadataTests.cs` добавить в класс `JobMetadataTests` тест и args-тип (рядом с `ConflictArgs`):

```csharp
    [Fact]
    public void Conflicting_timeout_throws()
    {
        JobMetadataRegistry.Register<TimeoutConflictArgs>(new JobDescriptor("meta.e", "q", 3, 0, null) { TimeoutSeconds = 10 }, null);
        JobMetadataRegistry.Register<TimeoutConflictArgs>(new JobDescriptor("meta.e", "q", 3, 0, null) { TimeoutSeconds = 10 }, null);
        var e = Assert.Throws<InvalidOperationException>(() =>
            JobMetadataRegistry.Register<TimeoutConflictArgs>(new JobDescriptor("meta.e", "q", 3, 0, null), null));
        Assert.Contains(nameof(TimeoutConflictArgs), e.Message);
    }

    private sealed record TimeoutConflictArgs(int X);
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Abstractions.Tests --filter "FullyQualifiedName~ContractsTests|FullyQualifiedName~JobMetadataTests"`
Expected: FAIL — ошибки компиляции: `CS0117`/`CS1061` (`JobAttribute`/`JobDescriptor` не содержат `TimeoutSeconds`), `CS0246` (`JobAbortedException`, `JobArgumentsException` не найдены).

- [x] **Step 3: Реализовать**

`src/PgBolt.Abstractions/JobAttribute.cs` (весь файл):

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

    /// <summary>
    /// Таймаут одного выполнения в секундах; 0 — нет таймаута (по умолчанию). Применяется исполняющим сервером
    /// по своему registry, в job_types не публикуется. Отрицательное значение — ошибка генератора BOLT007; значение
    /// больше 4294967 (предел таймеров .NET, 2³² − 2 мс ≈ 49,7 сут) — предупреждение BOLT008, сервер урезает его до предела.
    /// </summary>
    public int TimeoutSeconds { get; init; }
}
```

`src/PgBolt.Abstractions/JobDescriptor.cs` (весь файл):

```csharp
namespace PgBolt;

/// <summary>Умолчания задачи из [Job] (или из job_types для динамического enqueue).</summary>
public sealed record JobDescriptor(string Name, string Queue, int MaxAttempts, short Priority, int[]? RetryDelaysSeconds)
{
    /// <summary>Таймаут выполнения в секундах из [Job]; 0 — нет. Init-свойство, чтобы не ломать позиционный конструктор.</summary>
    public int TimeoutSeconds { get; init; }

    /// <summary>Сравнение по значению, включая содержимое RetryDelaysSeconds и TimeoutSeconds.</summary>
    public bool SameAs(JobDescriptor other) =>
        Name == other.Name && Queue == other.Queue && MaxAttempts == other.MaxAttempts && Priority == other.Priority
        && TimeoutSeconds == other.TimeoutSeconds
        && (RetryDelaysSeconds ?? []).AsSpan().SequenceEqual(other.RetryDelaysSeconds ?? [])
        && (RetryDelaysSeconds is null) == (other.RetryDelaysSeconds is null);
}
```

`src/PgBolt.Abstractions/JobAbortedException.cs`:

```csharp
namespace PgBolt;

/// <summary>
/// Задача сообщает, что повтор бессмысленен: исход — окончательный Failed без retry (часть 3 §3.5, §5.2).
/// Не sealed: приложение может вводить собственные подтипы.
/// </summary>
public class JobAbortedException(string message, Exception? inner = null) : Exception(message, inner);
```

`src/PgBolt.Abstractions/JobArgumentsException.cs`:

```csharp
namespace PgBolt;

/// <summary>
/// Аргументы задачи не удалось десериализовать (битый JSON, неподдерживаемый тип, null вместо объекта).
/// Бросается invoker'ом до вызова исполнителя; исход — окончательный Failed (часть 3 §5.1, §5.2). Публичен для фильтров.
/// </summary>
public sealed class JobArgumentsException(string message, Exception? inner = null) : Exception(message, inner);
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Abstractions.Tests`
Expected: PASS (все тесты проекта, включая прежние `JobDescriptor_SameAs_compares_retry_delays_by_value` и `Conflicting_descriptor_throws`).

Run: `dotnet build PgBolt.slnx`
Expected: `Build succeeded`, 0 warnings (новые типы не ломают `PgBolt.Postgres`, генератор ещё не заполняет `TimeoutSeconds`).

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Abstractions tests/PgBolt.Abstractions.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(abstractions): job TimeoutSeconds, JobAbortedException, JobArgumentsException"
```

---

### Task 2: Генератор — `TimeoutSeconds` в дескрипторе, анализаторы BOLT007 и BOLT008

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §3.5, §8.4.

**Files:**
- Modify: `src/PgBolt.Generators/Models/JobTypeModel.cs`
- Modify: `src/PgBolt.Generators/Emitter.cs`
- Modify: `src/PgBolt.Generators/Analysis/Diagnostics.cs`
- Modify: `src/PgBolt.Generators/Analysis/AnalysisState.cs`
- Modify: `src/PgBolt.Generators/AnalyzerReleases.Unshipped.md`
- Test: `tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs`, `tests/PgBolt.Generators.Tests/RegistryGenerationTests.cs`, `tests/PgBolt.Generators.Tests/AnalyzerTests.cs`
- Create (снапшот, принимается в Step 6): `tests/PgBolt.Generators.Tests/MetadataGenerationTests.Timeout_is_emitted_only_when_positive#PgBolt.Metadata.g.verified.cs`

**Interfaces:**
- Consumes: `JobAttribute.TimeoutSeconds`, `JobDescriptor.TimeoutSeconds` (Task 1).
- Produces:
  - `internal sealed record JobTypeModel(string TypeFqn, string JobName, string Queue, int MaxAttempts, short Priority, EquatableArray<int>? RetryDelays, int TimeoutSeconds)`.
  - `Emitter.Descriptor(JobTypeModel)` дописывает ` { TimeoutSeconds = N }` к выражению `new global::PgBolt.JobDescriptor(...)` **только при N > 0**. При N ≤ 0 вывод байт-в-байт прежний — существующие `*.verified.cs` не меняются. Отрицательное значение не эмитится (runtime видит 0), но компиляция всё равно падает по BOLT007.
  - `Diagnostics.InvalidTimeout` — `BOLT007`, «Invalid job timeout», `Error`, категория `PgBolt`, место — синтаксис атрибута `[Job(...)]` (как у BOLT005), аргумент сообщения — значение в invariant culture.
  - `Diagnostics.TimeoutAboveLimit` — `BOLT008`, «Job timeout exceeds the runtime limit», `Warning`, категория `PgBolt`, то же место и тот же формат аргумента. Срабатывает при `TimeoutSeconds > 4294967` (`Diagnostics.MaxTimeoutSeconds`): предел таймеров .NET — 2³² − 2 мс = 4294967,294 с (`PgBoltServerOptions.MaxJobTimeout`, Task 8), больший таймаут сервер урезает до него (Task 14). Предупреждение, а не ошибка: код остаётся рабочим, таймаут ≈ 49,7 сут против номинальных до ≈ 68 лет на практике не отличается; §3.5 — генератор проверяет атрибут.

- [x] **Step 1: Написать падающие тесты генератора**

В `tests/PgBolt.Generators.Tests/MetadataGenerationTests.cs` добавить в класс `MetadataGenerationTests`:

```csharp
    [Fact]
    public Task Timeout_is_emitted_only_when_positive()
    {
        var (driver, output) = GeneratorHarness.Run("""
            namespace Orders.Contracts;

            [PgBolt.Job("orders.long", TimeoutSeconds = 300)]
            public sealed record LongArgs(int X);

            [PgBolt.Job("orders.short", TimeoutSeconds = 0)]
            public sealed record ShortArgs(int X);
            """);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.LongArgs>(new global::PgBolt.JobDescriptor("orders.long", "default", 10, (short)0, null) { TimeoutSeconds = 300 }, null);""", code);
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.ShortArgs>(new global::PgBolt.JobDescriptor("orders.short", "default", 10, (short)0, null), null);""", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }
```

В `tests/PgBolt.Generators.Tests/RegistryGenerationTests.cs` добавить в класс `RegistryGenerationTests` (без снапшота — достаточно assert'ов):

```csharp
    [Fact]
    public void Registry_descriptor_carries_timeout()
    {
        var (driver, output) = GeneratorHarness.Run($$"""
            using System.Threading;
            using System.Threading.Tasks;
            using PgBolt;

            [assembly: PgBoltJsonContext(typeof(AppJson))]

            [Job("slow.job", TimeoutSeconds = 45)]
            public sealed record SlowArgs(int X);

            public sealed class SlowJob : IJob<SlowArgs>
            {
                public Task ExecuteAsync(SlowArgs args, JobContext context, CancellationToken ct) => Task.CompletedTask;
            }

            [System.Text.Json.Serialization.JsonSerializable(typeof(SlowArgs))]
            {{MetadataGenerationTests.HandWrittenContext}}
            """);

        var registry = GeneratorHarness.Generated(driver, "PgBolt.JobRegistry.g.cs");
        Assert.Contains("""builder.AddExecutor<global::SlowArgs, global::SlowJob>(new global::PgBolt.JobDescriptor("slow.job", "default", 10, (short)0, null) { TimeoutSeconds = 45 }, """, registry);
        GeneratorHarness.AssertClean(output);
    }
```

- [x] **Step 2: Написать падающие тесты анализатора**

В `tests/PgBolt.Generators.Tests/AnalyzerTests.cs` добавить в класс `AnalyzerTests` (стиль BOLT005; значение передаётся строкой, чтобы исходник не зависел от культуры). `4294967` — последнее значение без BOLT008 (4294967 с = 4294967000 мс ≤ 2³² − 2 мс), `4294968` — первое с ним:

```csharp
    [Theory]
    [InlineData("-1")]
    [InlineData("-3600")]
    public Task Negative_timeout_BOLT007(string timeout) => VerifyAsync(Usings + $$"""
        [{|#0:Job("a.job", TimeoutSeconds = {{timeout}})|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.InvalidTimeout).WithLocation(0).WithArguments(timeout));

    [Fact]
    public Task Zero_and_positive_timeout_have_no_diagnostics() => VerifyAsync(Usings + """
        [Job("a.job", TimeoutSeconds = 0)] public sealed record A(int X);
        [Job("b.job", TimeoutSeconds = 300)] public sealed record B(int X);
        [Job("c.job", TimeoutSeconds = 4294967)] public sealed record C(int X);
        """);

    [Theory]
    [InlineData("4294968")]
    [InlineData("2147483647")]
    public Task Timeout_above_runtime_limit_BOLT008(string timeout) => VerifyAsync(Usings + $$"""
        [{|#0:Job("a.job", TimeoutSeconds = {{timeout}})|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.TimeoutAboveLimit).WithLocation(0).WithArguments(timeout));

    [Fact]
    public Task Invalid_name_and_negative_timeout_are_both_reported() => VerifyAsync(Usings + """
        [{|#0:Job("Bad Name", TimeoutSeconds = -5)|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.InvalidName).WithLocation(0).WithArguments("Bad Name"),
        Diagnostic(Diagnostics.InvalidTimeout).WithLocation(0).WithArguments("-5"));
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: FAIL — ошибка компиляции `CS0117: 'Diagnostics' does not contain a definition for 'InvalidTimeout'` (и то же для `'TimeoutAboveLimit'`).

- [x] **Step 4: Реализовать модель и эмиттер**

`src/PgBolt.Generators/Models/JobTypeModel.cs` (весь файл):

```csharp
using System.Linq;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal sealed record JobTypeModel(
        string TypeFqn, string JobName, string Queue, int MaxAttempts, short Priority, EquatableArray<int>? RetryDelays, int TimeoutSeconds)
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
                var timeoutSeconds = 0;
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
                        case "TimeoutSeconds" when arg.Value.Value is int t:
                            timeoutSeconds = t;
                            break;
                    }
                }
                return new JobTypeModel(type.ToDisplayString(KnownNames.Fq), name, queue, maxAttempts, priority, delays, timeoutSeconds);
            }
            return null;
        }
    }
}
```

`src/PgBolt.Generators/Emitter.cs` — добавить `using System.Globalization;` к using'ам и заменить метод `Descriptor` (остальной файл без изменений):

```csharp
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
```

```csharp
        /// <summary>
        /// TimeoutSeconds дописывается инициализатором только при значении &gt; 0: вывод для задач без таймаута
        /// не меняется (снапшоты), отрицательное значение отклоняет BOLT007.
        /// </summary>
        internal static string Descriptor(JobTypeModel t) =>
            $"new global::PgBolt.JobDescriptor({Literal(t.JobName)}, {Literal(t.Queue)}, {t.MaxAttempts}, (short){t.Priority}, {Delays(t.RetryDelays)})"
            + (t.TimeoutSeconds > 0
                ? " { TimeoutSeconds = " + t.TimeoutSeconds.ToString(CultureInfo.InvariantCulture) + " }"
                : "");
```

- [x] **Step 5: Реализовать BOLT007 и BOLT008**

`src/PgBolt.Generators/Analysis/Diagnostics.cs` — после `MissingJsonContext` добавить константу, оба дескриптора и включить их в `All` (генератор — `netstandard2.0` без ссылки на `PgBolt.Server`, поэтому предел задан константой; он равен `PgBoltServerOptions.MaxJobTimeout`, округлённому вниз до целых секунд):

```csharp
        /// <summary>Наибольший TimeoutSeconds, не превышающий предел таймеров .NET (2³² − 2 мс = 4294967,294 с).</summary>
        public const int MaxTimeoutSeconds = 4294967;

        public static readonly DiagnosticDescriptor InvalidTimeout = new(
            "BOLT007", "Invalid job timeout",
            "TimeoutSeconds must be >= 0 (0 = no timeout), but is {0}",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor TimeoutAboveLimit = new(
            "BOLT008", "Job timeout exceeds the runtime limit",
            "TimeoutSeconds {0} exceeds the .NET timer limit of 4294967 s (about 49.7 days); the server caps the job timeout at this limit",
            Category, DiagnosticSeverity.Warning, isEnabledByDefault: true);

        public static readonly ImmutableArray<DiagnosticDescriptor> All = ImmutableArray.Create(
            MissingJsonType, DuplicateName, ArgsNotJob, MultipleExecutors, InvalidName, MissingJsonContext, InvalidTimeout,
            TimeoutAboveLimit);
```

`src/PgBolt.Generators/Analysis/AnalysisState.cs` — добавить `using System.Globalization;` к using'ам:

```csharp
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;
```

и заменить **только начало** `AnalyzeNamedType` — от сигнатуры до строки `if (type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsGenericType) return;` включительно; всё после неё (цикл по `AllInterfaces`, BOLT003, `_executors`, `CheckCoverage`) остаётся как есть. Проверка таймаута — рядом с проверкой имени BOLT005, с той же локацией (синтаксис атрибута). Фрагмент ниже — не весь метод:

```csharp
        public void AnalyzeNamedType(SymbolAnalysisContext ctx)
        {
            var type = (INamedTypeSymbol)ctx.Symbol;

            var attr = type.GetAttributes().FirstOrDefault(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, _jobAttribute));
            if (attr is not null)
            {
                // Локация вычисляется сразу, а не локальной функцией: захваченный attr в ней считался бы maybe-null (CS8602).
                var location = attr.ApplicationSyntaxReference?.GetSyntax(ctx.CancellationToken).GetLocation() ?? type.Locations[0];

                if (attr.ConstructorArguments.Length == 1 && attr.ConstructorArguments[0].Value is string name)
                {
                    if (!JobNames.IsValid(name))
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidName, location, name));
                    _names.GetOrAdd(name, _ => new ConcurrentBag<INamedTypeSymbol>()).Add(type);
                }

                foreach (var arg in attr.NamedArguments)
                {
                    if (arg.Key != "TimeoutSeconds" || arg.Value.Value is not int timeout)
                        continue;
                    if (timeout < 0)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidTimeout, location,
                            timeout.ToString(CultureInfo.InvariantCulture)));
                    else if (timeout > Diagnostics.MaxTimeoutSeconds)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.TimeoutAboveLimit, location,
                            timeout.ToString(CultureInfo.InvariantCulture)));
                }
            }

            if (type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsGenericType)
                return;
            // Конец заменяемого фрагмента: дальше существующий код метода без изменений (цикл по AllInterfaces, BOLT003, _executors, CheckCoverage).
```

`src/PgBolt.Generators/AnalyzerReleases.Unshipped.md` (весь файл):

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
BOLT007 | PgBolt | Error | Diagnostics
BOLT008 | PgBolt | Warning | Diagnostics
```

- [x] **Step 6: Прогнать тесты и принять новый снапшот**

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: все тесты проходят, **кроме** `MetadataGenerationTests.Timeout_is_emitted_only_when_positive`: его assert'ы проходят, а Verify падает, потому что `*.verified.cs` ещё нет, и создаёт файл `tests/PgBolt.Generators.Tests/MetadataGenerationTests.Timeout_is_emitted_only_when_positive#PgBolt.Metadata.g.received.cs`. Существующие снапшоты (`Contracts_only_...`, `Enqueue_call_site_...`, `Registry_is_generated_...` — оба файла) проходят **без изменений**; если какой-то из них упал — `Descriptor` меняет вывод при `TimeoutSeconds = 0`, это ошибка реализации, а не повод перепринимать снапшот.

Открыть `.received.cs` и сверить с ожидаемым (первая строка `//HintName: ...` добавляется Verify.SourceGenerators и остаётся в `.verified.cs`; файл в UTF-8 с BOM, как остальные снапшоты):

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
            global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.LongArgs>(new global::PgBolt.JobDescriptor("orders.long", "default", 10, (short)0, null) { TimeoutSeconds = 300 }, null);
            global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.ShortArgs>(new global::PgBolt.JobDescriptor("orders.short", "default", 10, (short)0, null), null);
        }
    }
}
```

Если совпадает — принять снапшот переименованием:

```bash
cd tests/PgBolt.Generators.Tests
mv 'MetadataGenerationTests.Timeout_is_emitted_only_when_positive#PgBolt.Metadata.g.received.cs' \
   'MetadataGenerationTests.Timeout_is_emitted_only_when_positive#PgBolt.Metadata.g.verified.cs'
cd ../..
```

(`*.received.*` уже в `.gitignore`; других `.received.` файлов остаться не должно — проверить `ls tests/PgBolt.Generators.Tests/*.received.* 2>/dev/null` → пусто.)

Run: `dotnet test tests/PgBolt.Generators.Tests`
Expected: PASS (все тесты, включая 4 новых теста анализатора — 6 случаев с учётом `[InlineData]` — и 2 новых теста генератора).

Run: `dotnet build PgBolt.slnx`
Expected: `Build succeeded`, 0 warnings (в т.ч. нет RS2000/RS2001 — BOLT007 и BOLT008 внесены в `AnalyzerReleases.Unshipped.md`; ни один `[Job]` в репозитории не задаёт `TimeoutSeconds > 4294967`, поэтому BOLT008 при `TreatWarningsAsErrors` сборку не роняет).

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Generators tests/PgBolt.Generators.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(generators): emit job TimeoutSeconds, add BOLT007 for negative and BOLT008 for over-limit timeout"
```

---

### Task 3: Invoker оборачивает ошибки десериализации в `JobArgumentsException`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §5.1 п. 6, §5.2 строка 5, §8.4.

**Files:**
- Modify: `src/PgBolt.Postgres/Jobs/JobsBuilder.cs`
- Test: `tests/PgBolt.Postgres.Tests/JobRegistryTests.cs`

**Interfaces:**
- Consumes: `JobArgumentsException` (Task 1), `JobInvoker`, `JobExecutorRegistration`, `JobRegistry` (core).
- Produces: поведение `JobExecutorRegistration.Invoke` — `JsonException`, `NotSupportedException` и `null` при десериализации → `JobArgumentsException` (сообщение содержит имя задачи; `InnerException` — исходное исключение, для `null` — без inner). Бросается синхронно, до резолва исполнителя. Исключения самого исполнителя (в т.ч. его собственный `JsonException`) проходят без обёртки.

- [x] **Step 1: Написать падающие тесты**

В `tests/PgBolt.Postgres.Tests/JobRegistryTests.cs`:

1) Добавить новые типы после `OtherRegistryJob` (отдельные args-типы: `JobMetadataRegistry` — статический на `TArgs`, повторная регистрация `RegistryArgs` с другим дескриптором дала бы конфликт):

```csharp
public sealed record ThrowingArgs(string Mode);

/// <summary>Исполнитель, который сам бросает — его исключения invoker не оборачивает.</summary>
public sealed class ThrowingRegistryJob : IJob<ThrowingArgs>
{
    public Task ExecuteAsync(ThrowingArgs args, JobContext context, CancellationToken cancellationToken) =>
        args.Mode == "json"
            ? throw new JsonException("job's own json error")
            : throw new InvalidOperationException("job failure");
}

[JsonConverter(typeof(UnsupportedArgsConverter))]
public sealed record UnsupportedArgs(int X);

/// <summary>Конвертер, имитирующий NotSupportedException от System.Text.Json при чтении.</summary>
public sealed class UnsupportedArgsConverter : JsonConverter<UnsupportedArgs>
{
    public override UnsupportedArgs Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        throw new NotSupportedException("UnsupportedArgs cannot be deserialized.");

    public override void Write(Utf8JsonWriter writer, UnsupportedArgs value, JsonSerializerOptions options) =>
        throw new NotSupportedException("UnsupportedArgs cannot be serialized.");
}

public sealed class UnsupportedRegistryJob : IJob<UnsupportedArgs>
{
    public Task ExecuteAsync(UnsupportedArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}
```

2) Расширить контекст JSON (заменить объявление `RegistryJson`):

```csharp
[JsonSerializable(typeof(RegistryArgs))]
[JsonSerializable(typeof(ThrowingArgs))]
[JsonSerializable(typeof(UnsupportedArgs))]
internal sealed partial class RegistryJson : JsonSerializerContext;
```

3) Заменить тест `Null_arguments_throw_json_exception` (он проверял старое поведение) и добавить новые тесты в класс `JobRegistryTests`:

```csharp
    private static JobContext Ctx(string name) => new(JobId.New(), name, 1, 4, null, null, NullLogger.Instance);

    [Fact]
    public async Task Null_arguments_throw_job_arguments_exception()
    {
        var (services, registry, builder) = Create();
        services.AddSingleton<RegistryProbe>();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        await using var sp = services.BuildServiceProvider();
        registry.TryGet("registry.job", out var reg);

        var e = await Assert.ThrowsAsync<JobArgumentsException>(() => reg!.Invoke(sp, "null"u8.ToArray(), Ctx("registry.job"), CancellationToken.None));
        Assert.Contains("registry.job", e.Message);
        Assert.Empty(sp.GetRequiredService<RegistryProbe>().Seen);
    }

    [Theory]
    [InlineData("""{"Text":""")]                  // обрезанный JSON
    [InlineData("""not json""")]                  // не JSON
    [InlineData("""{"Text":1,"Count":2}""")]      // несовпадение типа поля
    public async Task Malformed_arguments_throw_job_arguments_exception_with_json_inner(string json)
    {
        var (services, registry, builder) = Create();
        services.AddSingleton<RegistryProbe>();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        await using var sp = services.BuildServiceProvider();
        registry.TryGet("registry.job", out var reg);

        var e = await Assert.ThrowsAsync<JobArgumentsException>(() => reg!.Invoke(sp, Encoding.UTF8.GetBytes(json), Ctx("registry.job"), CancellationToken.None));
        Assert.IsType<JsonException>(e.InnerException, exactMatch: false);
        Assert.Contains("registry.job", e.Message);
        Assert.Empty(sp.GetRequiredService<RegistryProbe>().Seen);
    }

    [Fact]
    public async Task Not_supported_during_deserialization_throws_job_arguments_exception()
    {
        var (services, registry, builder) = Create();
        builder.AddExecutor<UnsupportedArgs, UnsupportedRegistryJob>(
            new JobDescriptor("registry.unsupported", "reg", 4, 1, null), RegistryJson.Default.UnsupportedArgs);
        await using var sp = services.BuildServiceProvider();
        registry.TryGet("registry.unsupported", out var reg);

        var e = await Assert.ThrowsAsync<JobArgumentsException>(() => reg!.Invoke(sp, """{"X":1}"""u8.ToArray(), Ctx("registry.unsupported"), CancellationToken.None));
        Assert.IsType<NotSupportedException>(e.InnerException, exactMatch: false);
        Assert.Contains("registry.unsupported", e.Message);
    }

    [Theory]
    [InlineData("json", typeof(JsonException))]
    [InlineData("other", typeof(InvalidOperationException))]
    public async Task Exceptions_of_the_job_itself_are_not_wrapped(string mode, Type expected)
    {
        var (services, registry, builder) = Create();
        builder.AddExecutor<ThrowingArgs, ThrowingRegistryJob>(
            new JobDescriptor("registry.throwing", "reg", 4, 1, null), RegistryJson.Default.ThrowingArgs);
        await using var sp = services.BuildServiceProvider();
        await using var scope = sp.CreateAsyncScope();
        registry.TryGet("registry.throwing", out var reg);

        var e = await Record.ExceptionAsync(() =>
            reg!.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes($$"""{"Mode":"{{mode}}"}"""), Ctx("registry.throwing"), CancellationToken.None));
        Assert.NotNull(e);
        Assert.IsType(expected, e);
    }
```

Тест `Registered_invoker_deserializes_and_runs_scoped_job` (валидный JSON) остаётся без изменений и проверяет, что успешный путь не сломан.

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~JobRegistryTests"`
Expected: FAIL — `Null_arguments_throw_job_arguments_exception`, `Malformed_arguments_...` и `Not_supported_...` падают: `Assert.Throws() Failure: Exception type was not an exact match` (фактически `JsonException`/`NotSupportedException`). `Exceptions_of_the_job_itself_are_not_wrapped` и прежние тесты проходят.

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Jobs/JobsBuilder.cs` (весь файл):

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
            TArgs value;
            try
            {
                value = JsonSerializer.Deserialize(args.Span, argsTypeInfo)
                        ?? throw new JobArgumentsException($"Arguments of job '{descriptor.Name}' deserialized to null.");
            }
            catch (Exception e) when (e is JsonException or NotSupportedException)
            {
                // Часть 3 §5.1 п. 6: ошибка аргументов — окончательный Failed, а не retry. Исключения самого
                // исполнителя сюда не попадают: он вызывается вне try.
                throw new JobArgumentsException($"Arguments of job '{descriptor.Name}' cannot be deserialized: {e.Message}", e);
            }
            return sp.GetRequiredService<TJob>().ExecuteAsync(value, context, ct);
        }
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~JobRegistryTests"`
Expected: PASS.

Run: `dotnet build PgBolt.slnx`
Expected: `Build succeeded`, 0 warnings.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Postgres/Jobs/JobsBuilder.cs tests/PgBolt.Postgres.Tests/JobRegistryTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(jobs): wrap argument deserialization errors in JobArgumentsException"
```

---

### Task 4: Heartbeat по списку известных lease

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §8.1, §6.2 (шаги вызова, «сиротские» строки), core §5.3 (порядок захвата), core §5.5.

**Files:**
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.Heartbeat.cs`
- Test: `tests/PgBolt.Postgres.Tests/HeartbeatTests.cs` (переписывается, включая `HeartbeatAckDeadlockTests`)
- Modify (вызовы старой перегрузки): `tests/PgBolt.Postgres.Tests/SweeperTests.cs`, `tests/PgBolt.Postgres.Tests/CancelTests.cs`, `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`

**Interfaces:**
- Consumes: `LeaseRef`, `HeartbeatLease`, `PgBoltStore.QueryAsync`/`P.Add`/`ReadAllAsync` (core).
- Produces: `public Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)`.
- Removes: `HeartbeatAsync(Guid serverId, TimeSpan lease, CancellationToken ct)` — **удаляется** (breaking change публичного `PgBoltStore`; других вызовов в `src/` нет, в тестах обновляются в этой задаче).

Семантика: продлеваются только строки из списка, у которых `lease_owner = @server` и `lease_id` из списка; строка сервера вне списка («сиротская») не продлевается; строка, освобождённая и взятая заново (другой `lease_id`), не продлевается и отсутствует в ответе. Пустой список: statement **всё равно выполняется** (обновляет `servers.heartbeat_at`) с пустыми массивами `@ids`/`@lease_ids` и возвращает пустой ответ — короткого замыкания, как в `AckAsync`, здесь нет.

- [x] **Step 1: Переписать тесты heartbeat (падающие)**

`tests/PgBolt.Postgres.Tests/HeartbeatTests.cs` (весь файл):

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class HeartbeatTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly TimeSpan LongLease = TimeSpan.FromMinutes(10);

    private static LeaseRef Ref(LeasedJob job) => new(job.Id, job.LeaseId);

    private Task ShortenLeasesAsync() =>
        Db.ExecAsync("UPDATE {schema}.queue SET lease_until = now() + interval '1 second' WHERE lease_owner IS NOT NULL");

    private Task<bool> LeaseExtendedAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT lease_until > now() + interval '9 minutes' FROM {schema}.queue WHERE job_id = @id", ("id", id));

    private Task InsertServerRowAsync(Guid id) =>
        Db.ExecAsync("""
            INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
            VALUES (@id, 's', 'h', ARRAY['q'], 1, now() - interval '1 hour', now() - interval '1 hour')
            """, ("id", id));

    private Task<bool> ServerHeartbeatFreshAsync(Guid id) =>
        Db.ScalarAsync<bool>("SELECT heartbeat_at > now() - interval '1 minute' FROM {schema}.servers WHERE id = @id", ("id", id));

    [Fact]
    public async Task Extends_only_listed_leases_of_this_server()
    {
        for (var i = 0; i < 4; i++)
            await EnqueueAsync();
        var leasedA = await Store.DequeueAsync("q", 3, ServerA, Lease, Ct);
        var leasedB = Assert.Single(await Store.DequeueAsync("q", 1, ServerB, Lease, Ct));
        Assert.Equal(3, leasedA.Count);
        var listed = new[] { leasedA[0], leasedA[1] };
        var orphan = leasedA[2];
        await ShortenLeasesAsync();

        // lease сервера B в списке сервера A не продлевается: фильтр lease_owner = @server
        var result = await Store.HeartbeatAsync(ServerA, [Ref(listed[0]), Ref(listed[1]), Ref(leasedB)], LongLease, Ct);

        Assert.Equal(listed.Select(j => (j.Id, j.LeaseId)).OrderBy(x => x.Id), result.Select(h => (h.JobId, h.LeaseId)).OrderBy(x => x.JobId));
        Assert.All(result, h => Assert.False(h.CancelRequested));
        Assert.True(await LeaseExtendedAsync(listed[0].Id));
        Assert.True(await LeaseExtendedAsync(listed[1].Id));
        Assert.False(await LeaseExtendedAsync(orphan.Id));   // «сиротская» строка сервера A — вне списка, истекает (§6.2)
        Assert.False(await LeaseExtendedAsync(leasedB.Id));  // чужой lease_owner
    }

    [Fact]
    public async Task Released_and_re_leased_job_is_not_extended_by_stale_lease()
    {
        var id = await EnqueueAsync();
        var first = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Retried, await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, null, null, Ct));
        var second = await LeaseOneAsync();
        Assert.NotEqual(first.LeaseId, second.LeaseId);
        await ShortenLeasesAsync();

        Assert.Empty(await Store.HeartbeatAsync(ServerA, [Ref(first)], LongLease, Ct));
        Assert.False(await LeaseExtendedAsync(id));

        var hb = Assert.Single(await Store.HeartbeatAsync(ServerA, [Ref(first), Ref(second)], LongLease, Ct));
        Assert.Equal((id, second.LeaseId), (hb.JobId, hb.LeaseId));
        Assert.True(await LeaseExtendedAsync(id));
    }

    [Fact]
    public async Task Reports_cancel_requests()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

        var hb = Assert.Single(await Store.HeartbeatAsync(ServerA, [Ref(job)], Lease, Ct));
        Assert.Equal((id, job.LeaseId), (hb.JobId, hb.LeaseId));
        Assert.True(hb.CancelRequested);
    }

    [Fact]
    public async Task Updates_server_heartbeat_when_registered()
    {
        await InsertServerRowAsync(ServerA);
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();

        Assert.Single(await Store.HeartbeatAsync(ServerA, [Ref(job)], Lease, Ct));
        Assert.True(await ServerHeartbeatFreshAsync(ServerA));
        Assert.True(await InQueueAsync(id));
    }

    [Fact]
    public async Task Empty_list_updates_server_heartbeat_and_extends_nothing()
    {
        await InsertServerRowAsync(ServerA);
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        await ShortenLeasesAsync();

        Assert.Empty(await Store.HeartbeatAsync(ServerA, [], LongLease, Ct));

        Assert.True(await ServerHeartbeatFreshAsync(ServerA));
        Assert.False(await LeaseExtendedAsync(id));
    }

    [Fact]
    public async Task No_leases_returns_empty()
    {
        Assert.Empty(await Store.HeartbeatAsync(ServerA, [], Lease, Ct));
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
            // Список в перемешанном порядке: порядок захвата задаёт ORDER BY job_id в SQL, а не порядок списка.
            while (!stop.IsCancellationRequested)
                await store.HeartbeatAsync(ServerA, leased, Lease, Ct);
        }, Ct);

        var acks = leased.Chunk(10).Select(chunk => store.AckAsync(ServerA, chunk, Ct));
        var acked = await Task.WhenAll(acks);   // PostgresException 40P01 провалит тест
        stop.Cancel();
        await heartbeats;

        Assert.Equal(300, acked.Sum());
    }
}
```

- [x] **Step 2: Обновить остальные вызовы старой перегрузки в тестах**

`tests/PgBolt.Postgres.Tests/CancelTests.cs`, тест `Leased_job_gets_cancellation_request` — было:

```csharp
        var id = await EnqueueAsync();
        await LeaseOneAsync();
        Assert.Equal(CancelResult.CancellationRequested, await Store.CancelAsync(id, null, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT cancel_requested FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.True(Assert.Single(await Store.HeartbeatAsync(ServerA, Lease, Ct)).CancelRequested);
```

стало:

```csharp
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(CancelResult.CancellationRequested, await Store.CancelAsync(id, null, Ct));
        Assert.True(await Db.ScalarAsync<bool>("SELECT cancel_requested FROM {schema}.queue WHERE job_id = @id", ("id", id)));
        Assert.True(Assert.Single(await Store.HeartbeatAsync(ServerA, [new LeaseRef(id, job.LeaseId)], Lease, Ct)).CancelRequested);
```

`tests/PgBolt.Postgres.Tests/SweeperTests.cs`, тест `Fencing_after_sweep_and_re_lease_by_same_server` — было:

```csharp
        var hb = Assert.Single(await Store.HeartbeatAsync(ServerA, Lease, Ct));
        Assert.Equal(second.LeaseId, hb.LeaseId);
```

стало (в списке и устаревший, и текущий lease: в ответе только текущий):

```csharp
        var hb = Assert.Single(await Store.HeartbeatAsync(ServerA, [new LeaseRef(id, first.LeaseId), new LeaseRef(id, second.LeaseId)], Lease, Ct));
        Assert.Equal(second.LeaseId, hb.LeaseId);
```

`tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`, тест `Every_store_operation_works_on_slim_data_source` — было:

```csharp
        Assert.Equal(2, (await store.HeartbeatAsync(Server, lease, Ct)).Count);
```

стало (массивы `uuid[]` на slim data source — `EnableArrays()` уже включён):

```csharp
        Assert.Equal(2, (await store.HeartbeatAsync(Server, leased.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToArray(), lease, Ct)).Count);
        Assert.Empty(await store.HeartbeatAsync(Server, [], lease, Ct));
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~HeartbeatTests|FullyQualifiedName~HeartbeatAckDeadlockTests"`
Expected: FAIL — ошибка компиляции `CS1501: No overload for method 'HeartbeatAsync' takes 4 arguments` (и `CS1503` в местах с collection expression).

- [x] **Step 4: Реализовать**

`src/PgBolt.Postgres/Storage/PgBoltStore.Heartbeat.cs` (весь файл; SQL — §8.1 дословно):

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
            WHERE job_id = ANY(@ids) AND lease_id = ANY(@lease_ids) AND lease_owner = @server
            ORDER BY job_id                                -- порядок захвата core §5.3 не меняется
            FOR UPDATE
        )
        UPDATE {schema}.queue q SET lease_until = now() + @lease
        FROM l WHERE q.job_id = l.job_id
        RETURNING q.job_id, q.lease_id, q.cancel_requested
        """;

    private string HeartbeatSql => field ??= Render(HeartbeatTemplate);

    /// <summary>
    /// Продлевает lease из списка (только строки этого сервера с тем же lease_id), обновляет servers.heartbeat_at
    /// и доставляет запросы отмены (часть 3 §8.1). Lease из списка, отсутствующий в ответе, потерян.
    /// Пустой список: statement всё равно выполняется и обновляет только servers.heartbeat_at.
    /// </summary>
    public Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(leases);
        var ids = new Guid[leases.Count];
        var leaseIds = new Guid[leases.Count];
        var i = 0;
        foreach (var l in leases)
        {
            (ids[i], leaseIds[i]) = (l.JobId, l.LeaseId);
            i++;
        }

        return QueryAsync<IReadOnlyList<HeartbeatLease>>(HeartbeatSql, ps =>
        {
            P.Add(ps, "server", serverId);
            P.Add(ps, "ids", ids);
            P.Add(ps, "lease_ids", leaseIds);
            P.Add(ps, "lease", lease);
        }, async (r, c) => await r.ReadAllAsync(static r => new HeartbeatLease(r.GetGuid(0), r.GetGuid(1), r.GetBoolean(2)), c), ct);
    }
}
```

- [x] **Step 5: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~HeartbeatTests|FullyQualifiedName~HeartbeatAckDeadlockTests|FullyQualifiedName~SweeperTests|FullyQualifiedName~CancelTests"`
Expected: PASS (Direct и Bouncer).

Run: `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~SlimDataSourceTests"`
Expected: PASS (Direct и Bouncer).

Run: `grep -rn "HeartbeatAsync(" src tests --include='*.cs'`
Expected: каждый вызов передаёт список lease (4 аргумента); вызовов с 3 аргументами нет.

Run: `dotnet build PgBolt.slnx`
Expected: `Build succeeded`, 0 warnings.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Storage/PgBoltStore.Heartbeat.cs tests/PgBolt.Postgres.Tests/HeartbeatTests.cs \
        tests/PgBolt.Postgres.Tests/SweeperTests.cs tests/PgBolt.Postgres.Tests/CancelTests.cs \
        tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(store)!: heartbeat extends only listed leases of the server"
```

---

### Task 5: Release, регистрация сервера, типы задач по очередям

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §7.1, §7.2, §4.4 п. 3–4, §8.5.

**Files:**
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Release.cs`
- Create: `src/PgBolt.Postgres/Storage/PgBoltStore.Servers.cs`
- Modify: `src/PgBolt.Postgres/Storage/StoreRecords.cs` (`ReleaseOutcome`)
- Modify: `src/PgBolt.Postgres/Storage/PgBoltStore.JobTypes.cs` (`GetJobTypeNamesByQueuesAsync` — рядом с остальным SQL `job_types`; в «Структуре файлов» файл не указан, это единственное отличие размещения)
- Test (Create): `tests/PgBolt.Postgres.Tests/ReleaseTests.cs`, `tests/PgBolt.Postgres.Tests/ServersTests.cs`
- Test (Modify): `tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs` — новые операции на slim data source (`text[]`, `= ANY(text[])` при одном `EnableArrays()`)

**Interfaces:**
- Consumes: `PgBoltStore.QueryAsync`/`NonQueryAsync`/`Render`/`P`, `JobEventKind.Cancelled`, `HeartbeatAsync` (Task 4), `PublishJobTypesAsync`, `RetryAsync` (core).
- Produces (namespace `PgBolt.Postgres.Storage`):
  - `public enum ReleaseOutcome { LeaseLost, Released, Cancelled }`
  - `public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct)` — результат из двух счётчиков `released`/`cancelled`: `cancelled > 0` → `Cancelled`, `released > 0` → `Released`, оба 0 → `LeaseLost`.
  - `public Task RegisterServerAsync(Guid serverId, string name, string host, string[] queues, int capacity, CancellationToken ct)` — `queues` как `text[]`.
  - `public Task RemoveServerAsync(Guid serverId, CancellationToken ct)` — отсутствующая строка — не ошибка.
  - `public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct)` — имена по возрастанию; пустой список очередей → пустой результат без запроса.

- [x] **Step 1: Написать падающие тесты release**

`tests/PgBolt.Postgres.Tests/ReleaseTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ReleaseTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private Task RequestCancelAsync(Guid id) =>
        Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id));

    private Task<DateTime> LeasedAtAsync(Guid id) =>
        Db.ScalarAsync<DateTime>("SELECT leased_at FROM {schema}.queue WHERE job_id = @id", ("id", id));

    /// <summary>Вторая попытка: attempt до dequeue = 1, после dequeue = 2.</summary>
    private async Task<(Guid Id, LeasedJob First, LeasedJob Second)> SecondAttemptAsync()
    {
        var id = await EnqueueAsync(maxAttempts: 5);
        var first = await LeaseOneAsync();
        Assert.Equal(FailOutcome.Retried, await Store.RetryAsync(id, first.LeaseId, ServerA, TimeSpan.Zero, JobEventKind.Retry, "boom", null, Ct));
        var second = await LeaseOneAsync();
        Assert.Equal(2, second.Attempt);
        return (id, first, second);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Release_returns_job_without_spending_attempt(bool ran)
    {
        var (id, _, second) = await SecondAttemptAsync();
        var runAt = await Db.ScalarAsync<DateTime>("SELECT run_at FROM {schema}.queue WHERE job_id = @id", ("id", id));

        Assert.Equal(ReleaseOutcome.Released, await Store.ReleaseAsync(id, second.LeaseId, ServerA, ran, "stopping", Ct));

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT attempt = 1 AND ready AND lease_owner IS NULL AND lease_id IS NULL AND lease_until IS NULL
                   AND leased_at IS NULL AND NOT cancel_requested AND run_at = @run_at
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id), ("run_at", runAt)));
        Assert.Equal((short)0, await JobStateAsync(id));                                // jobs не тронута: Pending
        Assert.Equal(new[] { (short)JobEventKind.Retry }, await EventKindsAsync(id));   // release событий не пишет

        var again = await LeaseOneAsync(server: ServerB);                               // сразу доступна другому серверу
        Assert.Equal((id, 2), (again.Id, again.Attempt));
    }

    [Fact]
    public async Task Release_after_run_of_cancel_requested_job_cancels_with_interrupted_attempt()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        var leasedAt = await LeasedAtAsync(id);
        await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(id, job.LeaseId, ServerA, ran: true, "stopping", Ct));

        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 4 AND attempt = 1 AND started_at = @leased_at AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("id", id), ("leased_at", leasedAt), ("s", ServerA)));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT attempt = 1 AND server_id = @s AND message = 'stopping' AND exception IS NULL
            FROM {schema}.job_events WHERE job_id = @id
            """, ("id", id), ("s", ServerA)));
    }

    [Fact]
    public async Task Release_of_unstarted_cancel_requested_job_cancels_without_phantom_attempt()
    {
        var id = await EnqueueAsync();
        var job = await LeaseOneAsync();
        Assert.Equal(1, job.Attempt);
        await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(id, job.LeaseId, ServerA, ran: false, null, Ct));

        Assert.False(await InQueueAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 4 AND attempt = 0 AND started_at IS NULL AND finished_at IS NOT NULL AND server_id = @s
            FROM {schema}.jobs WHERE id = @id
            """, ("id", id), ("s", ServerA)));
        Assert.Equal(new[] { (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT attempt = 0 AND server_id = @s AND message IS NULL
            FROM {schema}.job_events WHERE job_id = @id
            """, ("id", id), ("s", ServerA)));
    }

    [Fact]
    public async Task Release_of_unstarted_second_attempt_keeps_previous_attempt_and_started_at()
    {
        var (id, _, second) = await SecondAttemptAsync();
        await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.Cancelled, await Store.ReleaseAsync(id, second.LeaseId, ServerA, ran: false, null, Ct));

        // attempt − 1 = 1 (последняя реально запускавшаяся попытка); started_at не меняется (retry его не пишет — NULL)
        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT state = 4 AND attempt = 1 AND started_at IS NULL FROM {schema}.jobs WHERE id = @id
            """, ("id", id)));
        Assert.Equal(new[] { (short)JobEventKind.Retry, (short)JobEventKind.Cancelled }, await EventKindsAsync(id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stale_or_foreign_lease_is_lease_lost_and_row_is_untouched(bool cancelRequested)
    {
        var (id, first, second) = await SecondAttemptAsync();
        if (cancelRequested)
            await RequestCancelAsync(id);

        Assert.Equal(ReleaseOutcome.LeaseLost, await Store.ReleaseAsync(id, first.LeaseId, ServerA, ran: true, null, Ct));
        Assert.Equal(ReleaseOutcome.LeaseLost, await Store.ReleaseAsync(id, Guid.NewGuid(), ServerA, ran: false, null, Ct));

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT lease_owner = @s AND lease_id = @lease AND attempt = 2 AND lease_until IS NOT NULL
            FROM {schema}.queue WHERE job_id = @id
            """, ("id", id), ("s", ServerA), ("lease", second.LeaseId)));
        Assert.Equal((short)0, await JobStateAsync(id));
        Assert.Equal(new[] { (short)JobEventKind.Retry }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Unknown_job_is_lease_lost()
    {
        Assert.Equal(ReleaseOutcome.LeaseLost, await Store.ReleaseAsync(Guid.CreateVersion7(), Guid.NewGuid(), ServerA, ran: true, null, Ct));
    }
}

public sealed class ReleaseTests_Direct(PostgresFixture pg) : ReleaseTests(pg, ConnectionMode.Direct);
public sealed class ReleaseTests_Bouncer(PostgresFixture pg) : ReleaseTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Написать падающие тесты серверов и типов по очередям**

`tests/PgBolt.Postgres.Tests/ServersTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ServersTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private const string ArgsSchema = """{"type":"object"}""";

    private Task<long> ServerRowsAsync(Guid id) =>
        Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.servers WHERE id = @id", ("id", id));

    [Fact]
    public async Task Register_inserts_server_row()
    {
        await Store.RegisterServerAsync(ServerA, "orders", "host-1", ["critical", "default"], 25, Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT name = 'orders' AND host = 'host-1' AND capacity = 25
                   AND started_at > now() - interval '1 minute' AND heartbeat_at = started_at
            FROM {schema}.servers WHERE id = @id
            """, ("id", ServerA)));
        Assert.Equal(new[] { "critical", "default" },
            await Db.ScalarAsync<string[]>("SELECT queues FROM {schema}.servers WHERE id = @id", ("id", ServerA)));
    }

    [Fact]
    public async Task Remove_deletes_only_its_row_and_missing_row_is_noop()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        await Store.RegisterServerAsync(ServerB, "b", "h", ["q"], 1, Ct);

        await Store.RemoveServerAsync(ServerA, Ct);
        await Store.RemoveServerAsync(ServerA, Ct);

        Assert.Equal(0L, await ServerRowsAsync(ServerA));
        Assert.Equal(1L, await ServerRowsAsync(ServerB));
    }

    [Fact]
    public async Task Registered_server_heartbeat_is_refreshed_by_empty_heartbeat()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        await Db.ExecAsync("UPDATE {schema}.servers SET heartbeat_at = now() - interval '1 hour' WHERE id = @id", ("id", ServerA));

        Assert.Empty(await Store.HeartbeatAsync(ServerA, [], Lease, Ct));

        Assert.True(await Db.ScalarAsync<bool>("SELECT heartbeat_at > now() - interval '1 minute' FROM {schema}.servers WHERE id = @id", ("id", ServerA)));
    }

    [Fact]
    public async Task Job_type_names_by_queues_returns_sorted_names_of_matching_default_queues()
    {
        await Store.PublishJobTypesAsync(
        [
            new JobTypeRecord("c.job", ArgsSchema, "q1", 3, 0, null),
            new JobTypeRecord("a.job", ArgsSchema, "q1", 3, 0, null),
            new JobTypeRecord("b.job", ArgsSchema, "q2", 3, 0, null),
            new JobTypeRecord("d.job", ArgsSchema, "other", 3, 0, null),
        ], "test-host/1", Ct);

        Assert.Equal(new[] { "a.job", "b.job", "c.job" }, await Store.GetJobTypeNamesByQueuesAsync(["q1", "q2"], Ct));
        Assert.Equal(new[] { "d.job" }, await Store.GetJobTypeNamesByQueuesAsync(["other"], Ct));
        Assert.Empty(await Store.GetJobTypeNamesByQueuesAsync(["missing"], Ct));
        Assert.Empty(await Store.GetJobTypeNamesByQueuesAsync([], Ct));
    }
}

public sealed class ServersTests_Direct(PostgresFixture pg) : ServersTests(pg, ConnectionMode.Direct);
public sealed class ServersTests_Bouncer(PostgresFixture pg) : ServersTests(pg, ConnectionMode.Bouncer);
```

`tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs`, тест `Every_store_operation_works_on_slim_data_source` — в конец теста, после строки `Assert.NotNull(await store.GetJobTypeAsync("slim.job", Ct));`, добавить (slim builder включает только `EnableArrays()`: здесь проявилась бы нехватка обработчика `text[]`):

```csharp

        var fresh = (await store.EnqueueAsync(One(), null, Ct))[0].Id!.Value;
        var toRelease = Assert.Single(await store.DequeueAsync("slim", 10, Server, lease, Ct));
        Assert.Equal(fresh, toRelease.Id);
        Assert.Equal(ReleaseOutcome.Released, await store.ReleaseAsync(fresh, toRelease.LeaseId, Server, ran: false, "slim", Ct));

        await store.RegisterServerAsync(Server, "slim", "slim-host", ["slim", "other"], 2, Ct);
        Assert.Equal(new[] { "slim.job" }, await store.GetJobTypeNamesByQueuesAsync(["slim", "other"], Ct));
        await store.RemoveServerAsync(Server, Ct);
```

(К этому месту очередь `slim` пуста: `a` отменена, `delayed` подтверждена, поэтому dequeue отдаёт ровно новую задачу; `slim.job` опубликован строкой выше с `default_queue = "slim"`.)

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~ReleaseTests|FullyQualifiedName~ServersTests"`
Expected: FAIL — ошибки компиляции `CS0246: The type or namespace name 'ReleaseOutcome' could not be found`, `CS1061: 'PgBoltStore' does not contain a definition for 'ReleaseAsync'` (а также `RegisterServerAsync`, `RemoveServerAsync`, `GetJobTypeNamesByQueuesAsync`). Сборка `PgBolt.E2E.Tests` падает с теми же ошибками.

- [x] **Step 4: Реализовать `ReleaseOutcome` и release**

`src/PgBolt.Postgres/Storage/StoreRecords.cs` — после строки `public enum FailOutcome { LeaseLost, Retried, Cancelled, Failed }` добавить:

```csharp
/// <summary>Исход release (часть 3 §7.1): возврат без траты попытки, финализация отмены или потерянный lease.</summary>
public enum ReleaseOutcome { LeaseLost, Released, Cancelled }
```

`src/PgBolt.Postgres/Storage/PgBoltStore.Release.cs` (SQL — §7.1 дословно):

```csharp
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
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
            RETURNING q.job_id, q.attempt - CASE WHEN @ran THEN 0 ELSE 1 END AS attempt, q.leased_at
        ),
        cj AS (
            UPDATE {schema}.jobs j SET state = 4, attempt = c.attempt,
                                       started_at = CASE WHEN @ran THEN c.leased_at ELSE j.started_at END,
                                       finished_at = now(), server_id = @server
            FROM c WHERE j.id = c.job_id AND j.id = @id
        ),
        ev AS (
            INSERT INTO {schema}.job_events (job_id, at, kind, server_id, attempt, message)
            SELECT job_id, now(), @kind_cancelled, @server, attempt, @message FROM c
        )
        SELECT (SELECT count(*) FROM r) AS released, (SELECT count(*) FROM c) AS cancelled;
        """;

    private string ReleaseSql => field ??= Render(ReleaseTemplate);

    /// <summary>
    /// Возвращает задачу в очередь без траты попытки (attempt − 1, ready = true); при запрошенной отмене финализирует
    /// её как Cancelled с attempt последней реально запускавшейся попытки (часть 3 §7.1). Fencing по lease_id.
    /// </summary>
    /// <param name="ran">true — выполнение было прервано shutdown'ом; false — задача не запускалась.</param>
    public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct) =>
        QueryAsync(ReleaseSql, ps =>
        {
            P.Add(ps, "id", jobId);
            P.Add(ps, "lease_id", leaseId);
            P.Add(ps, "server", serverId);
            P.Add(ps, "ran", ran);
            P.Add(ps, "kind_cancelled", (short)JobEventKind.Cancelled);
            P.AddNullable(ps, "message", message, NpgsqlDbType.Text);
        }, async (r, c) =>
        {
            if (!await r.ReadAsync(c))
                throw new InvalidOperationException("Release statement returned no row.");
            var released = r.GetInt64(0);
            var cancelled = r.GetInt64(1);
            return cancelled > 0 ? ReleaseOutcome.Cancelled
                : released > 0 ? ReleaseOutcome.Released
                : ReleaseOutcome.LeaseLost;
        }, ct);
}
```

- [x] **Step 5: Реализовать регистрацию сервера и выборку типов по очередям**

`src/PgBolt.Postgres/Storage/PgBoltStore.Servers.cs` (SQL — §7.2 дословно):

```csharp
namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string RegisterServerTemplate = """
        INSERT INTO {schema}.servers (id, name, host, queues, capacity, started_at, heartbeat_at)
        VALUES (@id, @name, @host, @queues, @capacity, now(), now());
        """;

    private const string RemoveServerTemplate = """
        DELETE FROM {schema}.servers WHERE id = @id;
        """;

    private string RegisterServerSql => field ??= Render(RegisterServerTemplate);
    private string RemoveServerSql => field ??= Render(RemoveServerTemplate);

    /// <summary>Регистрирует виртуальный сервер (часть 3 §7.2). host — Environment.MachineName.</summary>
    public Task RegisterServerAsync(Guid serverId, string name, string host, string[] queues, int capacity, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(queues);
        return NonQueryAsync(RegisterServerSql, ps =>
        {
            P.Add(ps, "id", serverId);
            P.Add(ps, "name", name);
            P.Add(ps, "host", host);
            P.Add(ps, "queues", queues);
            P.Add(ps, "capacity", capacity);
        }, ct);
    }

    /// <summary>Удаляет строку сервера при остановке (часть 3 §6.4 п. 6). Отсутствующая строка — не ошибка.</summary>
    public Task RemoveServerAsync(Guid serverId, CancellationToken ct) =>
        NonQueryAsync(RemoveServerSql, ps => P.Add(ps, "id", serverId), ct);
}
```

`src/PgBolt.Postgres/Storage/PgBoltStore.JobTypes.cs` — добавить шаблон рядом с `GetJobTypeTemplate`, свойство рядом с `GetJobTypeSql` и метод в конец класса (остальное без изменений):

```csharp
    private const string JobTypeNamesByQueuesTemplate = """
        SELECT name FROM {schema}.job_types WHERE default_queue = ANY(@queues) ORDER BY name
        """;
```

```csharp
    private string JobTypeNamesByQueuesSql => field ??= Render(JobTypeNamesByQueuesTemplate);
```

```csharp
    /// <summary>Имена job_types, у которых default_queue ∈ queues (предупреждение NoExecutor на старте, часть 3 §4.4 п. 4).</summary>
    public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(queues);
        if (queues.Count == 0)
            return Task.FromResult<IReadOnlyList<string>>([]);
        var array = queues.ToArray();
        return QueryAsync<IReadOnlyList<string>>(JobTypeNamesByQueuesSql, ps => P.Add(ps, "queues", array),
            async (r, c) => await r.ReadAllAsync(static r => r.GetString(0), c), ct);
    }
```

- [x] **Step 6: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~ReleaseTests|FullyQualifiedName~ServersTests"`
Expected: PASS (Direct и Bouncer).

Run: `dotnet test tests/E2E/PgBolt.E2E.Tests --filter "FullyQualifiedName~SlimDataSourceTests"`
Expected: PASS (Direct и Bouncer).

Run: `dotnet build PgBolt.slnx`
Expected: `Build succeeded`, 0 warnings.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Postgres/Storage tests/PgBolt.Postgres.Tests/ReleaseTests.cs tests/PgBolt.Postgres.Tests/ServersTests.cs \
        tests/E2E/PgBolt.E2E.Tests/SlimDataSourceTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(store): release without spending attempt, server registration, job types by queue"
```

---

### Task 6: `IWakeupSignal.Subscribe` вместо `WaitAsync`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §8.2, §4.3 («Пробуждение»), §2 (строка «Пробуждение диспетчера»).

**Files:**
- Modify: `src/PgBolt.Postgres/Client/IWakeupSignal.cs`
- Modify: `src/PgBolt.Postgres/Client/InProcessWakeupSignal.cs`
- Test: `tests/PgBolt.Postgres.Tests/WakeupSignalTests.cs` (переписывается)
- Modify: `tests/PgBolt.Postgres.Tests/ClientTestSupport.cs` (`RecordingWakeup`)

**Interfaces:**
- Consumes: —
- Produces (namespace `PgBolt.Postgres.Client`):
  - `public interface IWakeupSignal { void Signal(string queue); IDisposable Subscribe(string queue, Action callback); }` — `WaitAsync` **удалён** (breaking change публичного API, §8.2).
  - `public sealed class InProcessWakeupSignal : IWakeupSignal` — подписчики по очереди в `ConcurrentDictionary<string, Action[]>` (copy-on-write, CAS через `AddOrUpdate`/`TryUpdate`/`TryRemove`); `Signal` вызывает callback'и синхронно в порядке подписки, исключение callback'а перехватывается; `Subscribe` возвращает `IDisposable` с идемпотентным `Dispose`.

Latch-семантику обеспечивает подписчик (событие `wake` диспетчера, Task 15); сигнал без подписчиков теряется — это допустимо (§8.2). Callback, отписанный параллельно с `Signal`, может быть вызван последний раз по уже взятому снимку массива.

- [x] **Step 1: Переписать тесты (падающие)**

`tests/PgBolt.Postgres.Tests/WakeupSignalTests.cs` (весь файл):

```csharp
using System.Collections.Concurrent;
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Tests;

public sealed class WakeupSignalTests
{
    [Fact]
    public void Signal_invokes_callback_of_that_queue_synchronously()
    {
        var signal = new InProcessWakeupSignal();
        var a = 0;
        var b = 0;
        using var subA = signal.Subscribe("a", () => a++);
        using var subB = signal.Subscribe("b", () => b++);

        signal.Signal("b");

        Assert.Equal((0, 1), (a, b));   // проверка сразу после Signal: вызов синхронный
    }

    [Fact]
    public void Other_queue_does_not_invoke_callback()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        using var sub = signal.Subscribe("a", () => calls++);

        signal.Signal("z");
        signal.Signal("A");   // имена очередей сравниваются ordinal

        Assert.Equal(0, calls);
    }

    [Fact]
    public void Signal_without_subscribers_is_noop()
    {
        var signal = new InProcessWakeupSignal();
        Assert.Null(Record.Exception(() => signal.Signal("nobody")));
    }

    [Fact]
    public void Dispose_unsubscribes_and_is_idempotent()
    {
        var signal = new InProcessWakeupSignal();
        var removed = 0;
        var kept = 0;
        var sub = signal.Subscribe("q", () => removed++);
        using var other = signal.Subscribe("q", () => kept++);

        sub.Dispose();
        sub.Dispose();
        signal.Signal("q");

        Assert.Equal((0, 1), (removed, kept));
    }

    [Fact]
    public void Resubscribe_after_last_dispose_works()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        signal.Subscribe("q", () => calls++).Dispose();
        using var again = signal.Subscribe("q", () => calls++);

        signal.Signal("q");

        Assert.Equal(1, calls);
    }

    [Fact]
    public void Callback_exception_does_not_reach_Signal_and_other_subscribers_still_run()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        using var bad = signal.Subscribe("q", () => throw new InvalidOperationException("subscriber bug"));
        using var good = signal.Subscribe("q", () => calls++);

        var e = Record.Exception(() => signal.Signal("q"));

        Assert.Null(e);
        Assert.Equal(1, calls);
    }

    [Fact]
    public void All_subscribers_of_one_queue_are_invoked_in_subscription_order()
    {
        var signal = new InProcessWakeupSignal();
        var order = new List<int>();
        using var s1 = signal.Subscribe("q", () => order.Add(1));
        var s2 = signal.Subscribe("q", () => order.Add(2));
        using var s3 = signal.Subscribe("q", () => order.Add(3));

        signal.Signal("q");
        s2.Dispose();
        signal.Signal("q");

        Assert.Equal(new[] { 1, 2, 3, 1, 3 }, order);
    }

    [Fact]
    public void Same_delegate_subscribed_twice_is_removed_once_per_dispose()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        Action callback = () => calls++;
        var first = signal.Subscribe("q", callback);
        using var second = signal.Subscribe("q", callback);

        first.Dispose();
        signal.Signal("q");

        Assert.Equal(1, calls);
    }

    [Fact]
    public void Concurrent_subscribe_and_dispose_keep_subscriber_set_consistent()
    {
        var signal = new InProcessWakeupSignal();
        var calls = 0;
        var subs = new ConcurrentBag<IDisposable>();

        Parallel.For(0, 200, _ => subs.Add(signal.Subscribe("q", () => Interlocked.Increment(ref calls))));
        signal.Signal("q");
        Assert.Equal(200, Volatile.Read(ref calls));

        Parallel.ForEach(subs, s => s.Dispose());
        signal.Signal("q");
        Assert.Equal(200, Volatile.Read(ref calls));
    }
}
```

`tests/PgBolt.Postgres.Tests/ClientTestSupport.cs` — заменить класс `RecordingWakeup`:

```csharp
public sealed class RecordingWakeup : IWakeupSignal
{
    public ConcurrentQueue<string> Signals { get; } = new();
    public void Signal(string queue) => Signals.Enqueue(queue);
    public IDisposable Subscribe(string queue, Action callback) => NoopSubscription.Instance;

    private sealed class NoopSubscription : IDisposable
    {
        public static readonly NoopSubscription Instance = new();
        public void Dispose() { }
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WakeupSignalTests"`
Expected: FAIL — ошибка компиляции `CS1061: 'InProcessWakeupSignal' does not contain a definition for 'Subscribe'` (и `CS0535` для `RecordingWakeup`, пока в интерфейсе есть `WaitAsync`).

- [x] **Step 3: Реализовать**

`src/PgBolt.Postgres/Client/IWakeupSignal.cs` (весь файл):

```csharp
namespace PgBolt.Postgres.Client;

/// <summary>
/// Пробуждение воркеров после enqueue/trigger/promote (§5.10, часть 3 §8.2). Сигнал не гарантирован — воркер
/// страхуется polling'ом. Точка расширения (например, Redis pub/sub между подами).
/// </summary>
public interface IWakeupSignal
{
    void Signal(string queue);

    /// <summary>callback вызывается синхронно из Signal; должен быть дешёвым и не блокировать.</summary>
    /// <returns>Подписка; Dispose отписывает (идемпотентно).</returns>
    IDisposable Subscribe(string queue, Action callback);
}
```

`src/PgBolt.Postgres/Client/InProcessWakeupSignal.cs` (весь файл):

```csharp
using System.Collections.Concurrent;

namespace PgBolt.Postgres.Client;

/// <summary>
/// In-process сигнал: подписчики по очереди в copy-on-write массиве (часть 3 §8.2). Signal не аллоцирует и не берёт
/// блокировок; подписка/отписка — CAS по словарю. Сигнал без подписчиков теряется: latch обеспечивает подписчик.
/// </summary>
public sealed class InProcessWakeupSignal : IWakeupSignal
{
    private readonly ConcurrentDictionary<string, Action[]> _subscribers = new(StringComparer.Ordinal);

    public void Signal(string queue)
    {
        if (!_subscribers.TryGetValue(queue, out var callbacks))
            return;
        foreach (var callback in callbacks)
        {
            try
            {
                callback();
            }
            catch (Exception)
            {
                // Ошибка подписчика не должна доходить до вызывающего enqueue (§8.2); остальные подписчики вызываются.
            }
        }
    }

    public IDisposable Subscribe(string queue, Action callback)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(callback);
        _subscribers.AddOrUpdate(queue,
            static (_, cb) => [cb],
            static (_, existing, cb) => [.. existing, cb],
            callback);
        return new Subscription(this, queue, callback);
    }

    private void Unsubscribe(string queue, Action callback)
    {
        while (_subscribers.TryGetValue(queue, out var existing))
        {
            var index = Array.IndexOf(existing, callback);
            if (index < 0)
                return;
            if (existing.Length == 1)
            {
                if (_subscribers.TryRemove(new KeyValuePair<string, Action[]>(queue, existing)))
                    return;
                continue;
            }
            var next = new Action[existing.Length - 1];
            Array.Copy(existing, 0, next, 0, index);
            Array.Copy(existing, index + 1, next, index, existing.Length - index - 1);
            if (_subscribers.TryUpdate(queue, next, existing))
                return;
        }
    }

    private sealed class Subscription(InProcessWakeupSignal owner, string queue, Action callback) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                owner.Unsubscribe(queue, callback);
        }
    }
}
```

(`TryRemove(KeyValuePair)` и `TryUpdate` сравнивают массив по ссылке — `EqualityComparer<Action[]>.Default`, поэтому параллельная подписка между чтением и CAS даёт повтор цикла, а не потерю подписчика.)

- [x] **Step 4: Проверить, что клиент не использует `WaitAsync`**

Run: `grep -rn "WaitAsync" src/PgBolt.Postgres/Client`
Expected: пусто (`PgBoltJobClient` вызывает только `wakeup.Signal(...)`; DI в `PgBoltServiceCollectionExtensions` регистрирует `new InProcessWakeupSignal()` без изменений).

- [x] **Step 5: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~WakeupSignalTests|FullyQualifiedName~ClientEnqueueTests|FullyQualifiedName~ClientManagementTests"`
Expected: PASS.

Run: `dotnet build PgBolt.slnx`
Expected: `Build succeeded`, 0 warnings.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Client/IWakeupSignal.cs src/PgBolt.Postgres/Client/InProcessWakeupSignal.cs \
        tests/PgBolt.Postgres.Tests/WakeupSignalTests.cs tests/PgBolt.Postgres.Tests/ClientTestSupport.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(client)!: IWakeupSignal.Subscribe replaces WaitAsync"
```

---

### Task 7: `PgBoltInitializer.Ready` и `InternalsVisibleTo` для `PgBolt.Server`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §8.3, §8.5, §4.4 п. 1.

**Files:**
- Modify: `src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs`
- Modify: `src/PgBolt.Postgres/PgBolt.Postgres.csproj`
- Test: `tests/PgBolt.Postgres.Tests/HostingTests.cs`

**Interfaces:**
- Consumes: `MigrationRunner`, `PartitionManager`, `PgBoltOptions` (core).
- Produces: `public sealed class PgBoltInitializer { internal Task Ready { get; } }` — на `TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)`:
  - успех — после проверки изоляции, миграций и партиций на сегодня, в том числе когда будущие партиции ушли в фоновый повтор (`BackgroundRetry`, он не ждётся);
  - исключение — при любом исключении `StartAsync` (то же исключение пробрасывается дальше, как раньше);
  - отмена — при `OperationCanceledException`, когда отменён токен `StartAsync` (исключение пробрасывается дальше);
  - `StartAsync` не вызывался — `Ready` не завершается (координатор Task 16 ждёт его с токеном старта хоста).
- `PgBolt.Postgres.csproj`: `InternalsVisibleTo` для `PgBolt.Server` и `PgBolt.Server.Tests` (проекты появятся в Task 8; атрибут на ещё не существующую сборку допустим).

- [x] **Step 1: Написать падающие тесты**

В `tests/PgBolt.Postgres.Tests/HostingTests.cs`, класс `HostingTests`:

1) Добавить тесты:

```csharp
    [Fact]
    public async Task Ready_completes_after_successful_start()
    {
        await using var sp = Build();
        var initializer = sp.GetRequiredService<PgBoltInitializer>();
        Assert.False(initializer.Ready.IsCompleted);

        await initializer.StartAsync(Ct);

        await initializer.Ready.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.True(initializer.Ready.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Ready_faults_with_start_exception()
    {
        await using var sp = Build(o => o.AutoMigrate = false);
        var initializer = sp.GetRequiredService<PgBoltInitializer>();

        var startError = await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.StartAsync(Ct));

        Assert.True(initializer.Ready.IsFaulted);
        var readyError = await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.Ready);
        Assert.Same(startError, readyError);
        Assert.Contains("schema version", readyError.Message);
    }

    [Fact]
    public async Task Ready_is_cancelled_when_start_token_is_cancelled()
    {
        await using var sp = Build();
        var initializer = sp.GetRequiredService<PgBoltInitializer>();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => initializer.StartAsync(cts.Token));

        Assert.True(initializer.Ready.IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => initializer.Ready);
    }
```

2) В существующем тесте `Missing_future_partitions_are_retried_in_background` — `Ready` успешен, хотя будущие партиции ушли в фоновый повтор. Было:

```csharp
            await initializer.StartAsync(Ct);
            Assert.NotNull(initializer.BackgroundRetry);
        }
```

стало:

```csharp
            await initializer.StartAsync(Ct);
            Assert.NotNull(initializer.BackgroundRetry);
            Assert.True(initializer.Ready.IsCompletedSuccessfully); // фоновый повтор не ждётся (§8.3)
        }
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~HostingTests"`
Expected: FAIL — ошибка компиляции `CS1061: 'PgBoltInitializer' does not contain a definition for 'Ready'`.

- [x] **Step 3: Реализовать `Ready`**

`src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs` (весь файл; прежнее тело `StartAsync` без изменений перенесено в `InitializeAsync`):

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
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
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

    /// <summary>
    /// Готовность схемы (часть 3 §8.3): успех — после проверки изоляции, миграций и партиций на сегодня (фоновое создание
    /// будущих партиций не ждётся); исключение — при падении StartAsync; отмена — при отмене его токена.
    /// Координатор PgBolt.Server ждёт её явно, не полагаясь на порядок hosted services.
    /// </summary>
    internal Task Ready => _ready.Task;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await InitializeAsync(cancellationToken);
            _ready.TrySetResult();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ready.TrySetCanceled(cancellationToken);
            throw;
        }
        catch (Exception e)
        {
            _ready.TrySetException(e);
            throw;
        }
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

    private async Task InitializeAsync(CancellationToken cancellationToken)
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

(`TrySet*` вместо `Set*`: повторный вызов `StartAsync` не бросает `InvalidOperationException` из TCS — `Ready` отражает первый исход.)

- [x] **Step 4: Открыть internals для `PgBolt.Server`**

`src/PgBolt.Postgres/PgBolt.Postgres.csproj` (весь файл):

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
    <InternalsVisibleTo Include="PgBolt.Server" />
    <InternalsVisibleTo Include="PgBolt.Server.Tests" />
  </ItemGroup>
</Project>
```

- [x] **Step 5: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Postgres.Tests --filter "FullyQualifiedName~HostingTests|FullyQualifiedName~HostingIsolationTests"`
Expected: PASS (Direct и Bouncer; прежние тесты, включая `AutoMigrate_off_on_unmigrated_schema_fails_start` и `Non_read_committed_default_isolation_fails_start_before_migrations`, по-прежнему получают исключение из `StartAsync`).

Run: `dotnet build PgBolt.slnx`
Expected: `Build succeeded`, 0 warnings.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Postgres/Hosting/PgBoltInitializer.cs src/PgBolt.Postgres/PgBolt.Postgres.csproj tests/PgBolt.Postgres.Tests/HostingTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(hosting): PgBoltInitializer.Ready and internals for PgBolt.Server"
```

- [x] **Step 7: Проверка фазы**

Run: `dotnet test PgBolt.slnx`
Expected: все тесты проходят. Падение — остановиться и доложить (протокол, п. 6).

## Фаза 1. Компоненты сервера

### Task 8: Проекты `PgBolt.Server` и `PgBolt.Server.Tests`, опции, `MonoClock`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Modify: `Directory.Packages.props` (+ `Microsoft.Extensions.Hosting` 10.0.0 — нужен `HostOptions`, Task 16), `PgBolt.slnx`
- Create: `src/PgBolt.Server/PgBolt.Server.csproj`
- Create: `src/PgBolt.Server/QueueOptions.cs`, `src/PgBolt.Server/PgBoltServerOptions.cs`, `src/PgBolt.Server/PgBoltProcessingOptions.cs`
- Create: `src/PgBolt.Server/IJobFilter.cs`, `src/PgBolt.Server/JobExecutingContext.cs` (только объявления из контракта — нужны `AddFilter<T>`; конвейер фильтров — Task 14)
- Create: `src/PgBolt.Server/Runtime/MonoClock.cs`
- Create: `tests/PgBolt.Server.Tests/PgBolt.Server.Tests.csproj`, `tests/PgBolt.Server.Tests/AssemblyInfo.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs`, `tests/PgBolt.Server.Tests/Unit/MonoClockTests.cs`

**Interfaces:**
- Consumes: `PgBolt.Postgres` (ссылка на проект; `InternalsVisibleTo("PgBolt.Server")` и `("PgBolt.Server.Tests")` — Task 7), `JobContext` (`PgBolt.Abstractions`), `PostgresFixture` (`tests/PgBolt.Testing`).
- Produces:
  - `public sealed class QueueOptions { string Name { get; } int Reserved; int? MaxConcurrency; TimeSpan MinPollInterval; TimeSpan MaxPollInterval; }`
  - `public sealed class PgBoltServerOptions` — `Workers`, `LeaseDuration`, `HeartbeatInterval`, `ShutdownTimeout`, `DefaultJobTimeout`, `AckBatchSize`, `AckFlushInterval`, `Queue(string name, Action<QueueOptions>? configure = null)`, `AddFilter<[DynamicallyAccessedMembers(PublicConstructors)] T>() where T : class, IJobFilter`, `internal IReadOnlyList<QueueOptions> Queues`, `internal IReadOnlyList<Type> FilterTypes`, `internal int Capacity`, `internal void Validate(string serverName)`, `internal static readonly TimeSpan MaxInterval` (1 сут), `internal static readonly TimeSpan MaxJobTimeout` (2³² − 2 мс). Свойство `RetryPolicy` добавляет Task 9.
  - `public sealed class PgBoltProcessingOptions { bool RunMaintenance; TimeSpan PromoterInterval; TimeSpan SweeperInterval; internal void Validate(); }`
  - `public delegate ValueTask JobExecutionDelegate();`, `public interface IJobFilter { ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next); }`, `public sealed class JobExecutingContext` (свойства контракта + `internal JobExecutingContext(JobContext job, IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, CancellationToken cancellationToken)`).
  - `internal sealed class MonoClock(TimeProvider time) { TimeProvider Time; long Now; long Add(long, TimeSpan); TimeSpan Until(long); TimeSpan Elapsed(long); }`

Решения задачи:
- **`RetryPolicy` в опциях появляется в Task 9**, а не заглушкой здесь: абстрактный класс без `Default` дал бы свойство с `null` по умолчанию и промежуточный публичный тип, который Task 9 всё равно переписывает. Task 9 добавляет свойство, его умолчание `RetryPolicy.Default` и проверку на `null` одной правкой.
- **`IJobFilter`, `JobExecutionDelegate`, `JobExecutingContext` объявляются здесь**, потому что `AddFilter<T>` ограничен `IJobFilter`. Task 14 их не создаёт, а только использует (конструктор контекста — internal, сигнатура выше).
- Сообщения валидации: `PgBolt server '<имя>': <поле> <правило>.`; поле очереди — `Queue('<имя очереди>').<свойство>`.
- `HeartbeatInterval ≤ LeaseDuration / 3` проверяется в целых тиках (`H.Ticks > L.Ticks / 3` ⇔ `3H > L`), без округления `TimeSpan / double`.
- **Верхние границы длительностей** (§3.1–§3.3 спеки, ред. 4; см. Global Constraints). Каждая длительность из опций становится задержкой таймера .NET: `LeaseDuration` — бюджет попытки отправки исхода (`CancellationTokenSource`, Task 13) и `validUntil`, `HeartbeatInterval` — таймаут dequeue и период heartbeat (Tasks 12, 15), `ShutdownTimeout` — ожидание выполнений (Task 16), `AckFlushInterval` — таймер ack (Task 13), `MaxPollInterval` — ожидание диспетчера (Task 15), интервалы обслуживания — `Task.Delay` (Task 16), `DefaultJobTimeout` — таймаут задачи (Task 14). Задержка больше 2³² − 2 мс бросает `ArgumentOutOfRangeException` внутри runtime: например, при `LeaseDuration` в 60 сут ни один исход не был бы отправлен (каждая попытка падала бы на создании токена бюджета). Поэтому `Validate` ограничивает их сразу, с понятным сообщением: `MaxInterval` = 1 сут для интервалов (с запасом на jitter ×1,1 polling и heartbeat, ×1,2 обслуживания; lease или опрос реже суток бессмысленны), `MaxJobTimeout` = 2³² − 2 мс для `DefaultJobTimeout` (долгие задачи легитимны — берётся весь предел таймера). Отдельная граница `HeartbeatInterval` не нужна: `H ≤ L / 3 ≤ 8 ч`; `MinPollInterval ≤ MaxPollInterval`.
- `MonoClock` переводит `TimeSpan` ↔ единицы `TimestampFrequency` через `Int128` без потери точности; `Until` округляет вверх (таймер не срабатывает раньше момента), `Elapsed` — вниз.

- [x] **Step 1: Пакет, проект библиотеки, решение**

В `Directory.Packages.props` сразу после строки `Microsoft.Extensions.Hosting.Abstractions` добавить:

```xml
    <PackageVersion Include="Microsoft.Extensions.Hosting" Version="10.0.0" />
```

`src/PgBolt.Server/PgBolt.Server.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <RootNamespace>PgBolt.Server</RootNamespace>
    <IsAotCompatible>true</IsAotCompatible>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.Hosting" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\PgBolt.Postgres\PgBolt.Postgres.csproj" />
  </ItemGroup>
  <ItemGroup>
    <InternalsVisibleTo Include="PgBolt.Server.Tests" />
  </ItemGroup>
</Project>
```

В `PgBolt.slnx` добавить в папку `/src/` после `PgBolt.Postgres`:

```xml
    <Project Path="src/PgBolt.Server/PgBolt.Server.csproj" />
```

и в папку `/tests/` после `PgBolt.Postgres.Tests`:

```xml
    <Project Path="tests/PgBolt.Server.Tests/PgBolt.Server.Tests.csproj" />
```

- [x] **Step 2: Тестовый проект**

`tests/PgBolt.Server.Tests/PgBolt.Server.Tests.csproj`:

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
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" />
    <PackageReference Include="Microsoft.Extensions.TimeProvider.Testing" />
    <ProjectReference Include="..\PgBolt.Testing\PgBolt.Testing.csproj" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\PgBolt.Server\PgBolt.Server.csproj" />
  </ItemGroup>
</Project>
```

`tests/PgBolt.Server.Tests/AssemblyInfo.cs` (контейнеры нужны интеграционным тестам Tasks 17–20; фикстура сборки поднимается при любом прогоне, поэтому Docker обязателен и для unit-тестов):

```csharp
using PgBolt.Testing;

[assembly: AssemblyFixture(typeof(PostgresFixture))]
```

- [x] **Step 3: Написать падающие тесты**

`tests/PgBolt.Server.Tests/Unit/ServerOptionsTests.cs`:

```csharp
namespace PgBolt.Server.Tests.Unit;

public sealed class ServerOptionsTests
{
    private const string Server = "orders";

    private static PgBoltServerOptions Valid() => new PgBoltServerOptions { Workers = 4 }.Queue("default");

    private static void AssertInvalid(PgBoltServerOptions options, string field)
    {
        var e = Assert.Throws<InvalidOperationException>(() => options.Validate(Server));
        Assert.Contains($"'{Server}'", e.Message, StringComparison.Ordinal);
        Assert.Contains(field, e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Defaults_match_spec()
    {
        var o = new PgBoltServerOptions();
        Assert.Equal(Environment.ProcessorCount * 2, o.Workers);
        Assert.Equal(TimeSpan.FromSeconds(60), o.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(15), o.HeartbeatInterval);
        Assert.Equal(TimeSpan.FromSeconds(25), o.ShutdownTimeout);
        Assert.Null(o.DefaultJobTimeout);
        Assert.Equal(100, o.AckBatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(20), o.AckFlushInterval);
        Assert.Empty(o.Queues);
        Assert.Empty(o.FilterTypes);

        var q = Assert.Single(new PgBoltServerOptions().Queue("q").Queues);
        Assert.Equal("q", q.Name);
        Assert.Equal(0, q.Reserved);
        Assert.Null(q.MaxConcurrency);
        Assert.Equal(TimeSpan.FromMilliseconds(50), q.MinPollInterval);
        Assert.Equal(TimeSpan.FromSeconds(10), q.MaxPollInterval);

        var p = new PgBoltProcessingOptions();
        Assert.True(p.RunMaintenance);
        Assert.Equal(TimeSpan.FromSeconds(1), p.PromoterInterval);
        Assert.Equal(TimeSpan.FromSeconds(15), p.SweeperInterval);
    }

    [Fact]
    public void Valid_options_pass() => Valid().Validate(Server);

    [Fact]
    public void Queues_keep_declaration_order_and_configuration()
    {
        var o = new PgBoltServerOptions()
            .Queue("critical", q => q.Reserved = 5)
            .Queue("default")
            .Queue("emails", q => { q.MaxConcurrency = 4; q.MaxPollInterval = TimeSpan.FromSeconds(2); });

        Assert.Equal(["critical", "default", "emails"], o.Queues.Select(q => q.Name));
        Assert.Equal(5, o.Queues[0].Reserved);
        Assert.Equal(4, o.Queues[2].MaxConcurrency);
        Assert.Equal(TimeSpan.FromSeconds(2), o.Queues[2].MaxPollInterval);
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void Queue_rejects_blank_name(string name) =>
        Assert.Throws<ArgumentException>(() => new PgBoltServerOptions().Queue(name));

    [Fact]
    public void Queue_rejects_null_name() =>
        Assert.Throws<ArgumentNullException>(() => new PgBoltServerOptions().Queue(null!));

    [Fact]
    public void Capacity_is_workers_plus_reserved()
    {
        var o = new PgBoltServerOptions { Workers = 20 }
            .Queue("critical", q => q.Reserved = 5)
            .Queue("default")
            .Queue("bulk", q => q.Reserved = 2);
        Assert.Equal(27, o.Capacity);
    }

    [Fact]
    public void AddFilter_keeps_registration_order()
    {
        var o = new PgBoltServerOptions().AddFilter<FilterA>().AddFilter<FilterB>();
        Assert.Equal([typeof(FilterA), typeof(FilterB)], o.FilterTypes);
    }

    [Fact]
    public void Negative_workers_are_rejected() => AssertInvalid(Valid().With(o => o.Workers = -1), "Workers");

    [Fact]
    public void No_queues_are_rejected() => AssertInvalid(new PgBoltServerOptions { Workers = 1 }, "Queue");

    [Fact]
    public void Duplicate_queue_is_rejected()
    {
        var o = Valid().Queue("other").Queue("default");
        var e = Assert.Throws<InvalidOperationException>(() => o.Validate(Server));
        Assert.Contains($"'{Server}'", e.Message, StringComparison.Ordinal);
        Assert.Contains("'default'", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Zero_workers_without_reserved_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 0 }.Queue("a").Queue("b"), "Workers");

    [Fact]
    public void Zero_workers_with_reserved_is_valid()
    {
        var o = new PgBoltServerOptions { Workers = 0 }.Queue("a", q => q.Reserved = 1);
        o.Validate(Server);
        Assert.Equal(1, o.Capacity);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_lease_duration_is_rejected(int seconds) =>
        AssertInvalid(Valid().With(o => o.LeaseDuration = TimeSpan.FromSeconds(seconds)), "LeaseDuration");

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_heartbeat_interval_is_rejected(int seconds) =>
        AssertInvalid(Valid().With(o => o.HeartbeatInterval = TimeSpan.FromSeconds(seconds)), "HeartbeatInterval");

    [Fact]
    public void Heartbeat_interval_above_a_third_of_lease_is_rejected() =>
        AssertInvalid(Valid().With(o =>
        {
            o.LeaseDuration = TimeSpan.FromSeconds(30);
            o.HeartbeatInterval = TimeSpan.FromSeconds(10) + TimeSpan.FromTicks(1);
        }), "HeartbeatInterval");

    [Fact]
    public void Heartbeat_interval_exactly_a_third_of_lease_is_valid() =>
        Valid().With(o =>
        {
            o.LeaseDuration = TimeSpan.FromSeconds(30);
            o.HeartbeatInterval = TimeSpan.FromSeconds(10);
        }).Validate(Server);

    [Fact]
    public void Negative_shutdown_timeout_is_rejected() =>
        AssertInvalid(Valid().With(o => o.ShutdownTimeout = TimeSpan.FromTicks(-1)), "ShutdownTimeout");

    [Fact]
    public void Zero_shutdown_timeout_is_valid() => Valid().With(o => o.ShutdownTimeout = TimeSpan.Zero).Validate(Server);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_default_job_timeout_is_rejected(int seconds) =>
        AssertInvalid(Valid().With(o => o.DefaultJobTimeout = TimeSpan.FromSeconds(seconds)), "DefaultJobTimeout");

    [Fact]
    public void Positive_default_job_timeout_is_valid() =>
        Valid().With(o => o.DefaultJobTimeout = TimeSpan.FromMinutes(5)).Validate(Server);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_ack_batch_size_is_rejected(int size) =>
        AssertInvalid(Valid().With(o => o.AckBatchSize = size), "AckBatchSize");

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_ack_flush_interval_is_rejected(int ms) =>
        AssertInvalid(Valid().With(o => o.AckFlushInterval = TimeSpan.FromMilliseconds(ms)), "AckFlushInterval");

    [Fact]
    public void Negative_reserved_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.Reserved = -1), "Queue('q').Reserved");

    [Fact]
    public void Zero_max_concurrency_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.MaxConcurrency = 0), "Queue('q').MaxConcurrency");

    [Fact]
    public void Max_concurrency_below_reserved_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => { q.Reserved = 3; q.MaxConcurrency = 2; }),
            "Queue('q').MaxConcurrency");

    [Fact]
    public void Max_concurrency_equal_to_reserved_is_valid() =>
        new PgBoltServerOptions { Workers = 1 }.Queue("q", q => { q.Reserved = 3; q.MaxConcurrency = 3; }).Validate(Server);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_min_poll_interval_is_rejected(int ms) =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.MinPollInterval = TimeSpan.FromMilliseconds(ms)),
            "Queue('q').MinPollInterval");

    [Fact]
    public void Max_poll_interval_below_min_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromSeconds(1);
            q.MaxPollInterval = TimeSpan.FromMilliseconds(999);
        }), "Queue('q').MaxPollInterval");

    [Fact]
    public void Max_poll_interval_equal_to_min_is_valid() =>
        new PgBoltServerOptions { Workers = 1 }.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromSeconds(1);
            q.MaxPollInterval = TimeSpan.FromSeconds(1);
        }).Validate(Server);

    [Fact]
    public void Processing_defaults_are_valid() => new PgBoltProcessingOptions().Validate();

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_promoter_interval_is_rejected(int seconds)
    {
        var e = Assert.Throws<InvalidOperationException>(() =>
            new PgBoltProcessingOptions { PromoterInterval = TimeSpan.FromSeconds(seconds) }.Validate());
        Assert.Contains("PromoterInterval", e.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_sweeper_interval_is_rejected(int seconds)
    {
        var e = Assert.Throws<InvalidOperationException>(() =>
            new PgBoltProcessingOptions { SweeperInterval = TimeSpan.FromSeconds(seconds) }.Validate());
        Assert.Contains("SweeperInterval", e.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("LeaseDuration")]
    [InlineData("ShutdownTimeout")]
    [InlineData("AckFlushInterval")]
    [InlineData("Queue('q').MaxPollInterval")]
    public void Durations_above_one_day_are_rejected(string field)
    {
        var tooLong = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1);
        var o = new PgBoltServerOptions { Workers = 1 }.Queue("q", q =>
        {
            if (field == "Queue('q').MaxPollInterval")
                q.MaxPollInterval = tooLong;
        });
        switch (field)
        {
            case "LeaseDuration": o.LeaseDuration = tooLong; break;
            case "ShutdownTimeout": o.ShutdownTimeout = tooLong; break;
            case "AckFlushInterval": o.AckFlushInterval = tooLong; break;
        }

        AssertInvalid(o, field);
    }

    [Fact]
    public void Default_job_timeout_above_timer_limit_is_rejected() =>
        AssertInvalid(Valid().With(o => o.DefaultJobTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1) + TimeSpan.FromTicks(1)),
            "DefaultJobTimeout");

    [Fact]
    public void Maximum_durations_are_valid()
    {
        Assert.Equal(TimeSpan.FromDays(1), PgBoltServerOptions.MaxInterval);
        Assert.Equal(TimeSpan.FromMilliseconds(uint.MaxValue - 1), PgBoltServerOptions.MaxJobTimeout);

        new PgBoltServerOptions
        {
            Workers = 1,
            LeaseDuration = TimeSpan.FromDays(1),
            HeartbeatInterval = TimeSpan.FromHours(8),     // = LeaseDuration / 3: её граница следует из границы LeaseDuration
            ShutdownTimeout = TimeSpan.FromDays(1),
            AckFlushInterval = TimeSpan.FromDays(1),
            DefaultJobTimeout = PgBoltServerOptions.MaxJobTimeout,
        }.Queue("q", q => q.MaxPollInterval = TimeSpan.FromDays(1)).Validate(Server);

        new PgBoltProcessingOptions { PromoterInterval = TimeSpan.FromDays(1), SweeperInterval = TimeSpan.FromDays(1) }.Validate();
    }

    [Theory]
    [InlineData("PromoterInterval")]
    [InlineData("SweeperInterval")]
    public void Processing_intervals_above_one_day_are_rejected(string field)
    {
        var tooLong = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1);
        var options = field == "PromoterInterval"
            ? new PgBoltProcessingOptions { PromoterInterval = tooLong }
            : new PgBoltProcessingOptions { SweeperInterval = tooLong };

        var e = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.Contains(field, e.Message, StringComparison.Ordinal);
    }

    private sealed class FilterA : IJobFilter
    {
        public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
    }

    private sealed class FilterB : IJobFilter
    {
        public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
    }
}

internal static class ServerOptionsTestExtensions
{
    public static PgBoltServerOptions With(this PgBoltServerOptions options, Action<PgBoltServerOptions> change)
    {
        change(options);
        return options;
    }
}
```

`tests/PgBolt.Server.Tests/Unit/MonoClockTests.cs`:

```csharp
using Microsoft.Extensions.Time.Testing;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class MonoClockTests
{
    [Fact]
    public void Now_follows_time_provider()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var start = clock.Now;

        Assert.Equal(time.GetTimestamp(), start);
        Assert.Same(time, clock.Time);
        time.Advance(TimeSpan.FromSeconds(3));
        Assert.Equal(TimeSpan.FromSeconds(3), time.GetElapsedTime(start, clock.Now));
    }

    [Fact]
    public void Add_matches_advance()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var target = clock.Add(clock.Now, TimeSpan.FromSeconds(5));

        time.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal(target, clock.Now);
    }

    [Fact]
    public void Add_accepts_negative_duration()
    {
        var clock = new MonoClock(new FakeTimeProvider());
        var now = clock.Now;
        Assert.Equal(now, clock.Add(clock.Add(now, TimeSpan.FromSeconds(7)), TimeSpan.FromSeconds(-7)));
    }

    [Fact]
    public void Until_counts_down_to_zero_and_stays_there()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var target = clock.Add(clock.Now, TimeSpan.FromSeconds(10));

        Assert.Equal(TimeSpan.FromSeconds(10), clock.Until(target));
        time.Advance(TimeSpan.FromSeconds(4));
        Assert.Equal(TimeSpan.FromSeconds(6), clock.Until(target));
        time.Advance(TimeSpan.FromSeconds(6));
        Assert.Equal(TimeSpan.Zero, clock.Until(target));
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(TimeSpan.Zero, clock.Until(target));
    }

    [Fact]
    public void Elapsed_grows_and_is_never_negative()
    {
        var time = new FakeTimeProvider();
        var clock = new MonoClock(time);
        var since = clock.Now;

        Assert.Equal(TimeSpan.Zero, clock.Elapsed(since));
        time.Advance(TimeSpan.FromMilliseconds(1500));
        Assert.Equal(TimeSpan.FromMilliseconds(1500), clock.Elapsed(since));
        Assert.Equal(TimeSpan.Zero, clock.Elapsed(clock.Add(clock.Now, TimeSpan.FromSeconds(1))));
    }

    [Fact]
    public void Converts_between_units_at_nanosecond_frequency()
    {
        var time = new ManualTimestamps(frequency: 1_000_000_000) { Timestamp = 1_000 };
        var clock = new MonoClock(time);

        Assert.Equal(1_000 + 100, clock.Add(1_000, TimeSpan.FromTicks(1)));          // 1 тик = 100 нс
        Assert.Equal(1_000 - 2_000_000_000, clock.Add(1_000, TimeSpan.FromSeconds(-2)));
        Assert.Equal(TimeSpan.FromTicks(2), clock.Until(1_000 + 150));               // 150 нс → вверх до 2 тиков
        Assert.Equal(TimeSpan.FromTicks(1), clock.Elapsed(1_000 - 150));             // 150 нс → вниз до 1 тика
    }

    [Fact]
    public void Rejects_null_time_provider() => Assert.Throws<ArgumentNullException>(() => new MonoClock(null!));

    private sealed class ManualTimestamps(long frequency) : TimeProvider
    {
        public long Timestamp { get; set; }
        public override long GetTimestamp() => Timestamp;
        public override long TimestampFrequency => frequency;
    }
}
```

- [x] **Step 4: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ServerOptionsTests|FullyQualifiedName~MonoClockTests"`
Expected: FAIL — ошибка компиляции: `PgBoltServerOptions`, `QueueOptions`, `PgBoltProcessingOptions`, `IJobFilter`, `MonoClock` не найдены.

- [x] **Step 5: Реализовать опции**

`src/PgBolt.Server/QueueOptions.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Очередь виртуального сервера (§3.3). Создаётся только через <see cref="PgBoltServerOptions.Queue"/>.</summary>
public sealed class QueueOptions
{
    internal QueueOptions(string name) => Name = name;

    public string Name { get; }

    /// <summary>Слоты, доступные только этой очереди.</summary>
    public int Reserved { get; set; }

    /// <summary>Потолок одновременных задач очереди на этом сервере (reserved + общие); null — нет.</summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>Нижняя граница адаптивного polling.</summary>
    public TimeSpan MinPollInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Верхняя граница адаптивного polling.</summary>
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(10);

    internal void Validate(string serverName)
    {
        var field = $"Queue('{Name}')";
        if (Reserved < 0)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(Reserved)}", "must be >= 0");
        if (MaxConcurrency is int max && max < Math.Max(1, Reserved))
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxConcurrency)}", "must be null or >= max(1, Reserved)");
        if (MinPollInterval <= TimeSpan.Zero)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MinPollInterval)}", "must be > 0");
        if (MaxPollInterval < MinPollInterval)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxPollInterval)}", "must be >= MinPollInterval");
        if (MaxPollInterval > PgBoltServerOptions.MaxInterval)
            throw PgBoltServerOptions.Invalid(serverName, $"{field}.{nameof(MaxPollInterval)}", $"must be <= {PgBoltServerOptions.MaxInterval}");
    }
}
```

`src/PgBolt.Server/PgBoltServerOptions.cs`:

```csharp
using System.Diagnostics.CodeAnalysis;

namespace PgBolt.Server;

/// <summary>Настройки виртуального сервера (§3.2). Проверяются <see cref="Validate"/> при регистрации.</summary>
public sealed class PgBoltServerOptions
{
    /// <summary>
    /// Верхняя граница интервалов (LeaseDuration, ShutdownTimeout, AckFlushInterval, MaxPollInterval, интервалы
    /// обслуживания): все они становятся задержками таймеров .NET (предел 2³² − 2 мс), 1 сут — с запасом на jitter.
    /// </summary>
    internal static readonly TimeSpan MaxInterval = TimeSpan.FromDays(1);

    /// <summary>Наибольшая задержка таймера .NET (2³² − 2 мс ≈ 49,7 сут): предел DefaultJobTimeout и [Job(TimeoutSeconds)].</summary>
    internal static readonly TimeSpan MaxJobTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly List<QueueOptions> _queues = [];
    private readonly List<Type> _filterTypes = [];

    /// <summary>Общий пул слотов.</summary>
    public int Workers { get; set; } = Environment.ProcessorCount * 2;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Не больше <c>LeaseDuration / 3</c>.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(25);

    /// <summary>Таймаут задачи без <c>[Job(TimeoutSeconds)]</c>; null — нет таймаута.</summary>
    public TimeSpan? DefaultJobTimeout { get; set; }

    public int AckBatchSize { get; set; } = 100;

    public TimeSpan AckFlushInterval { get; set; } = TimeSpan.FromMilliseconds(20);

    /// <summary>Добавляет очередь; порядок вызовов — приоритет в общем пуле (§4.2).</summary>
    public PgBoltServerOptions Queue(string name, Action<QueueOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var queue = new QueueOptions(name);
        configure?.Invoke(queue);
        _queues.Add(queue);
        return this;
    }

    /// <summary>Фильтр только этого сервера; выполняется после глобальных (§3.6).</summary>
    public PgBoltServerOptions AddFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>()
        where T : class, IJobFilter
    {
        _filterTypes.Add(typeof(T));
        return this;
    }

    internal IReadOnlyList<QueueOptions> Queues => _queues;

    internal IReadOnlyList<Type> FilterTypes => _filterTypes;

    /// <summary>Ёмкость сервера <c>Workers + Σ Reserved</c> (§3.3), публикуется в <c>servers.capacity</c>.</summary>
    internal int Capacity
    {
        get
        {
            var capacity = Workers;
            foreach (var queue in _queues)
                capacity += queue.Reserved;
            return capacity;
        }
    }

    internal void Validate(string serverName)
    {
        if (Workers < 0)
            throw Invalid(serverName, nameof(Workers), "must be >= 0");
        if (_queues.Count == 0)
            throw Invalid(serverName, nameof(Queue), "at least one queue must be declared");

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var queue in _queues)
        {
            if (!names.Add(queue.Name))
                throw Invalid(serverName, nameof(Queue), $"'{queue.Name}' is declared more than once");
            queue.Validate(serverName);
        }

        if (Capacity < 1)
            throw Invalid(serverName, nameof(Workers), "Workers + sum of Reserved must be >= 1");
        if (LeaseDuration <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(LeaseDuration), "must be > 0");
        if (LeaseDuration > MaxInterval)
            throw Invalid(serverName, nameof(LeaseDuration), $"must be <= {MaxInterval}");
        if (HeartbeatInterval <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(HeartbeatInterval), "must be > 0");
        if (HeartbeatInterval.Ticks > LeaseDuration.Ticks / 3)
            throw Invalid(serverName, nameof(HeartbeatInterval), "must be <= LeaseDuration / 3");   // отсюда и H ≤ MaxInterval / 3
        if (ShutdownTimeout < TimeSpan.Zero)
            throw Invalid(serverName, nameof(ShutdownTimeout), "must be >= 0");
        if (ShutdownTimeout > MaxInterval)
            throw Invalid(serverName, nameof(ShutdownTimeout), $"must be <= {MaxInterval}");
        if (DefaultJobTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(DefaultJobTimeout), "must be null or > 0");
        if (DefaultJobTimeout > MaxJobTimeout)
            throw Invalid(serverName, nameof(DefaultJobTimeout), $"must be null or <= {MaxJobTimeout}");
        if (AckBatchSize < 1)
            throw Invalid(serverName, nameof(AckBatchSize), "must be >= 1");
        if (AckFlushInterval <= TimeSpan.Zero)
            throw Invalid(serverName, nameof(AckFlushInterval), "must be > 0");
        if (AckFlushInterval > MaxInterval)
            throw Invalid(serverName, nameof(AckFlushInterval), $"must be <= {MaxInterval}");
    }

    internal static InvalidOperationException Invalid(string serverName, string field, string rule) =>
        new($"PgBolt server '{serverName}': {field} {rule}.");
}
```

`src/PgBolt.Server/PgBoltProcessingOptions.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Общие для процесса настройки обслуживания (§3.1, §6.3).</summary>
public sealed class PgBoltProcessingOptions
{
    /// <summary>Запускать promoter и sweeper в этом процессе.</summary>
    public bool RunMaintenance { get; set; } = true;

    /// <summary>Пауза promoter'а (±20 %).</summary>
    public TimeSpan PromoterInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Пауза sweeper'а (±20 %).</summary>
    public TimeSpan SweeperInterval { get; set; } = TimeSpan.FromSeconds(15);

    internal void Validate()
    {
        if (PromoterInterval <= TimeSpan.Zero || PromoterInterval > PgBoltServerOptions.MaxInterval)
            throw new InvalidOperationException(
                $"PgBolt processing: {nameof(PromoterInterval)} must be > 0 and <= {PgBoltServerOptions.MaxInterval}.");
        if (SweeperInterval <= TimeSpan.Zero || SweeperInterval > PgBoltServerOptions.MaxInterval)
            throw new InvalidOperationException(
                $"PgBolt processing: {nameof(SweeperInterval)} must be > 0 and <= {PgBoltServerOptions.MaxInterval}.");
    }
}
```

`src/PgBolt.Server/IJobFilter.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Продолжение конвейера фильтров: следующий фильтр или вызов исполнителя (§3.6).</summary>
public delegate ValueTask JobExecutionDelegate();

/// <summary>Фильтр выполнения задачи (§3.6). Резолвится как scoped из scope задачи.</summary>
public interface IJobFilter
{
    ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next);
}
```

`src/PgBolt.Server/JobExecutingContext.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Контекст выполнения задачи для фильтров (§3.6).</summary>
public sealed class JobExecutingContext
{
    internal JobExecutingContext(JobContext job, IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, CancellationToken cancellationToken)
    {
        Job = job;
        Services = services;
        ArgsUtf8 = argsUtf8;
        CancellationToken = cancellationToken;
    }

    public JobContext Job { get; }

    /// <summary>Scope задачи.</summary>
    public IServiceProvider Services { get; }

    public ReadOnlyMemory<byte> ArgsUtf8 { get; }

    public CancellationToken CancellationToken { get; }
}
```

- [x] **Step 6: Реализовать `MonoClock`**

`src/PgBolt.Server/Runtime/MonoClock.cs`:

```csharp
namespace PgBolt.Server.Runtime;

/// <summary>
/// Монотонное время runtime поверх <see cref="TimeProvider.GetTimestamp"/>: моменты — в единицах
/// <see cref="TimeProvider.TimestampFrequency"/>, интервалы — <see cref="TimeSpan"/>. Wall clock не используется.
/// </summary>
internal sealed class MonoClock(TimeProvider time)
{
    public TimeProvider Time { get; } = time ?? throw new ArgumentNullException(nameof(time));

    public long Now => Time.GetTimestamp();

    /// <summary><paramref name="timestamp"/> + <paramref name="duration"/> (duration может быть отрицательной).</summary>
    public long Add(long timestamp, TimeSpan duration) =>
        timestamp + (long)((Int128)duration.Ticks * Time.TimestampFrequency / TimeSpan.TicksPerSecond);

    /// <summary>Сколько осталось до <paramref name="timestamp"/>; ≥ 0, округление вверх — ожидание не заканчивается раньше момента.</summary>
    public TimeSpan Until(long timestamp)
    {
        var delta = timestamp - Now;
        if (delta <= 0)
            return TimeSpan.Zero;
        var frequency = Time.TimestampFrequency;
        return TimeSpan.FromTicks((long)(((Int128)delta * TimeSpan.TicksPerSecond + frequency - 1) / frequency));
    }

    /// <summary>Сколько прошло с <paramref name="since"/>; ≥ 0, округление вниз.</summary>
    public TimeSpan Elapsed(long since)
    {
        var delta = Now - since;
        return delta <= 0
            ? TimeSpan.Zero
            : TimeSpan.FromTicks((long)((Int128)delta * TimeSpan.TicksPerSecond / Time.TimestampFrequency));
    }
}
```

- [x] **Step 7: Прогнать тесты и сборку решения**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ServerOptionsTests|FullyQualifiedName~MonoClockTests"`
Expected: PASS — `Failed: 0, Passed: 56` (49 `ServerOptionsTests` + 7 `MonoClockTests`).

Run: `dotnet build PgBolt.slnx`
Expected: сборка без предупреждений и ошибок (AOT-анализ `PgBolt.Server` включён `IsAotCompatible`).

- [x] **Step 8: Commit**

```bash
git add Directory.Packages.props PgBolt.slnx src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): server project, options validation and monotonic clock"
```

---

### Task 9: `RetryPolicy`, `JobExecutorNotFoundException`, `RetryDecider`

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Server/RetryPolicy.cs`, `src/PgBolt.Server/JobExecutorNotFoundException.cs`, `src/PgBolt.Server/Runtime/RetryDecider.cs`
- Modify: `src/PgBolt.Server/PgBoltServerOptions.cs` (+ `RetryPolicy`, проверка на `null`)
- Create: `tests/PgBolt.Server.Tests/Fakes/TestLogger.cs` (общий логгер тестов, используется и в Tasks 11–16)
- Test: `tests/PgBolt.Server.Tests/Unit/RetryPolicyTests.cs`, `tests/PgBolt.Server.Tests/Unit/RetryDeciderTests.cs`

**Interfaces:**
- Consumes: `PgBoltServerOptions.Validate`/`Invalid` (Task 8).
- Produces:
  - `public abstract class RetryPolicy { abstract TimeSpan? GetDelay(int attempt, Exception error); static RetryPolicy Exponential(TimeSpan baseDelay, TimeSpan maxDelay, double jitter); static RetryPolicy Custom(Func<int, Exception, TimeSpan?> getDelay); static RetryPolicy Default { get; } }` + `internal static RetryPolicy Exponential(TimeSpan baseDelay, TimeSpan maxDelay, double jitter, Func<double> random)` (источник U[0, 1) для тестов).
  - `public sealed class JobExecutorNotFoundException(string jobName) : Exception { string JobName { get; } }`
  - `internal sealed class RetryDecider(RetryPolicy policy, ILogger logger) { TimeSpan? GetDelay(int attempt, int[]? retryDelaysSeconds, Exception error); }`
  - `PgBoltServerOptions.RetryPolicy { get; set; }` = `RetryPolicy.Default`.
  - Тестовая поддержка: `internal sealed record LogEntry(LogLevel Level, string Message, Exception? Exception)`, `internal sealed class TestLogger : ILogger { ConcurrentQueue<LogEntry> Entries; IEnumerable<LogEntry> At(LogLevel level); }` (namespace `PgBolt.Server.Tests.Fakes`).

Поведение (§3.4, Review Focus п. 5):
- `Exponential`: `0 < baseDelay ≤ maxDelay`, `jitter ∈ [0, 1)` (NaN отвергается) — иначе `ArgumentOutOfRangeException`. Потолок считается в целых тиках **до** умножения: `exponent ≥ 62` или `baseDelay > maxDelay / 2^exponent` → сразу `maxDelay`, переполнения нет при любом `attempt`. Jitter применяется **после** потолка: `capped × (1 + jitter × (2U − 1))`, `U ∈ [0, 1)`; при `jitter = 0` умножения нет (ряд точный). `attempt < 1` → `ArgumentOutOfRangeException`.
- `RetryDecider`: непустой `retryDelaysSeconds` → элемент `[min(attempt − 1, len − 1)]`; элемент `≤ 0` → `TimeSpan.Zero` (немедленный retry, `ready = true` в core §5.4). Пустой или `null` массив → политика. Исключение политики → `LogError` с исключением и `null` (окончательный fail). Отрицательная задержка политики → `TimeSpan.Zero`.

- [x] **Step 1: Написать общий тестовый логгер**

`tests/PgBolt.Server.Tests/Fakes/TestLogger.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace PgBolt.Server.Tests.Fakes;

internal sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

/// <summary>ILogger, запоминающий все записи; потокобезопасен.</summary>
internal sealed class TestLogger : ILogger
{
    public ConcurrentQueue<LogEntry> Entries { get; } = new();

    public IEnumerable<LogEntry> At(LogLevel level) => Entries.Where(e => e.Level == level);

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter) =>
        Entries.Enqueue(new LogEntry(logLevel, formatter(state, exception), exception));
}
```

- [x] **Step 2: Написать падающие тесты**

`tests/PgBolt.Server.Tests/Unit/RetryPolicyTests.cs`:

```csharp
namespace PgBolt.Server.Tests.Unit;

public sealed class RetryPolicyTests
{
    private static readonly Exception Error = new InvalidOperationException("boom");

    [Fact]
    public void Exponential_without_jitter_doubles_up_to_the_cap()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0);

        TimeSpan?[] expected =
        [
            TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(8), TimeSpan.FromMinutes(16), TimeSpan.FromMinutes(32),
            TimeSpan.FromHours(1), TimeSpan.FromHours(1),
        ];
        Assert.Equal(expected, Enumerable.Range(1, 10).Select(a => policy.GetDelay(a, Error)));
    }

    [Theory]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(10_000)]
    [InlineData(int.MaxValue)]
    public void Large_attempt_returns_the_cap_without_overflow(int attempt)
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0);
        Assert.Equal(TimeSpan.FromHours(1), policy.GetDelay(attempt, Error));
    }

    [Fact]
    public void Huge_cap_with_huge_attempt_does_not_overflow()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromTicks(1), TimeSpan.MaxValue, 0);
        Assert.Equal(TimeSpan.FromTicks(1L << 40), policy.GetDelay(41, Error));
        Assert.Equal(TimeSpan.MaxValue, policy.GetDelay(10_000, Error));
    }

    [Fact]
    public void Jitter_lower_bound_is_applied_after_the_cap()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2, () => 0.0);

        AssertClose(TimeSpan.FromSeconds(15 * 0.8), policy.GetDelay(1, Error));
        AssertClose(TimeSpan.FromSeconds(3600 * 0.8), policy.GetDelay(10_000, Error));
    }

    [Fact]
    public void Jitter_upper_bound_stays_below_one_plus_jitter()
    {
        var almostOne = Math.BitDecrement(1.0);
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2, () => almostOne);

        var capped = policy.GetDelay(10_000, Error)!.Value;
        AssertClose(TimeSpan.FromSeconds(3600 * 1.2), capped);
        Assert.True(capped <= TimeSpan.FromSeconds(3600 * 1.2));
    }

    [Fact]
    public void Jitter_midpoint_returns_the_nominal_delay()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2, () => 0.5);
        Assert.Equal(TimeSpan.FromSeconds(30), policy.GetDelay(2, Error));
    }

    [Fact]
    public void Random_jitter_stays_within_bounds()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2);
        for (var i = 0; i < 1000; i++)
            Assert.InRange(policy.GetDelay(3, Error)!.Value, TimeSpan.FromSeconds(48), TimeSpan.FromSeconds(72));
    }

    [Fact]
    public void Default_is_exponential_15s_1h_20_percent()
    {
        Assert.Same(RetryPolicy.Default, RetryPolicy.Default);
        for (var i = 0; i < 200; i++)
        {
            Assert.InRange(RetryPolicy.Default.GetDelay(1, Error)!.Value, TimeSpan.FromSeconds(12), TimeSpan.FromSeconds(18));
            Assert.InRange(RetryPolicy.Default.GetDelay(9, Error)!.Value, TimeSpan.FromSeconds(2880), TimeSpan.FromSeconds(4320));
        }
    }

    [Fact]
    public void Server_options_default_to_the_default_policy() =>
        Assert.Same(RetryPolicy.Default, new PgBoltServerOptions().RetryPolicy);

    [Fact]
    public void Null_retry_policy_is_rejected_by_validation()
    {
        var o = new PgBoltServerOptions { Workers = 1, RetryPolicy = null! }.Queue("q");
        var e = Assert.Throws<InvalidOperationException>(() => o.Validate("orders"));
        Assert.Contains("'orders'", e.Message, StringComparison.Ordinal);
        Assert.Contains("RetryPolicy", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Exponential_rejects_attempt_below_one() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryPolicy.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), 0).GetDelay(0, Error));

    [Theory]
    [InlineData(0, 10, 0.2)]      // baseDelay = 0
    [InlineData(-1, 10, 0.2)]     // baseDelay < 0
    [InlineData(10, 9, 0.2)]      // maxDelay < baseDelay
    [InlineData(1, 10, -0.01)]    // jitter < 0
    [InlineData(1, 10, 1.0)]      // jitter = 1
    [InlineData(1, 10, double.NaN)]
    public void Exponential_validates_arguments(int baseSeconds, int maxSeconds, double jitter) =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryPolicy.Exponential(TimeSpan.FromSeconds(baseSeconds), TimeSpan.FromSeconds(maxSeconds), jitter));

    [Fact]
    public void Exponential_accepts_equal_base_and_max()
    {
        var policy = RetryPolicy.Exponential(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), 0);
        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(1, Error));
        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(7, Error));
    }

    [Fact]
    public void Custom_passes_attempt_and_error_through()
    {
        (int Attempt, Exception Error)? seen = null;
        var policy = RetryPolicy.Custom((attempt, error) =>
        {
            seen = (attempt, error);
            return attempt < 3 ? TimeSpan.FromSeconds(attempt) : null;
        });

        Assert.Equal(TimeSpan.FromSeconds(2), policy.GetDelay(2, Error));
        Assert.Equal((2, Error), seen);
        Assert.Null(policy.GetDelay(3, Error));
    }

    [Fact]
    public void Custom_rejects_null_delegate() => Assert.Throws<ArgumentNullException>(() => RetryPolicy.Custom(null!));

    [Fact]
    public void Executor_not_found_exception_carries_job_name()
    {
        var e = new JobExecutorNotFoundException("orders.ship");
        Assert.Equal("orders.ship", e.JobName);
        Assert.Contains("orders.ship", e.Message, StringComparison.Ordinal);
    }

    private static void AssertClose(TimeSpan expected, TimeSpan? actual) =>
        Assert.InRange(actual!.Value, expected - TimeSpan.FromMilliseconds(1), expected + TimeSpan.FromMilliseconds(1));
}
```

`tests/PgBolt.Server.Tests/Unit/RetryDeciderTests.cs`:

```csharp
using Microsoft.Extensions.Logging;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class RetryDeciderTests
{
    private static readonly Exception Error = new InvalidOperationException("boom");

    /// <summary>Политика «attempt минут» — отличима от любых retry_delays в тестах.</summary>
    private static readonly RetryPolicy Minutes = RetryPolicy.Custom((attempt, _) => TimeSpan.FromMinutes(attempt));

    private readonly TestLogger _logger = new();

    [Theory]
    [InlineData(1, 5)]
    [InlineData(2, 10)]
    [InlineData(3, 20)]
    public void Job_retry_delays_win_over_policy(int attempt, int expectedSeconds)
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), decider.GetDelay(attempt, [5, 10, 20], Error));
    }

    [Theory]
    [InlineData(4)]
    [InlineData(100)]
    public void Last_retry_delay_repeats_when_array_is_short(int attempt)
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromSeconds(20), decider.GetDelay(attempt, [5, 10, 20], Error));
    }

    [Fact]
    public void Empty_retry_delays_fall_back_to_policy()
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromMinutes(2), decider.GetDelay(2, [], Error));
    }

    [Fact]
    public void Null_retry_delays_fall_back_to_policy()
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.FromMinutes(3), decider.GetDelay(3, null, Error));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Zero_or_negative_retry_delay_means_immediate_retry(int seconds)
    {
        var decider = new RetryDecider(Minutes, _logger);
        Assert.Equal(TimeSpan.Zero, decider.GetDelay(1, [seconds], Error));
    }

    [Fact]
    public void Policy_null_means_no_retry()
    {
        var decider = new RetryDecider(RetryPolicy.Custom((_, _) => null), _logger);
        Assert.Null(decider.GetDelay(1, null, Error));
    }

    [Fact]
    public void Negative_policy_delay_is_clamped_to_zero()
    {
        var decider = new RetryDecider(RetryPolicy.Custom((_, _) => TimeSpan.FromSeconds(-1)), _logger);
        Assert.Equal(TimeSpan.Zero, decider.GetDelay(1, null, Error));
    }

    [Fact]
    public void Throwing_policy_is_logged_and_means_no_retry()
    {
        var thrown = new NotSupportedException("policy bug");
        var decider = new RetryDecider(RetryPolicy.Custom((_, _) => throw thrown), _logger);

        Assert.Null(decider.GetDelay(2, null, Error));

        var entry = Assert.Single(_logger.At(LogLevel.Error));
        Assert.Same(thrown, entry.Exception);
    }

    [Fact]
    public void Policy_receives_one_based_attempt_and_the_job_error()
    {
        var calls = new List<(int, Exception)>();
        var decider = new RetryDecider(RetryPolicy.Custom((attempt, error) =>
        {
            calls.Add((attempt, error));
            return TimeSpan.Zero;
        }), _logger);

        decider.GetDelay(1, null, Error);
        decider.GetDelay(2, [], Error);

        Assert.Equal([(1, Error), (2, Error)], calls);
    }

    [Fact]
    public void No_executor_error_is_passed_to_policy()
    {
        Exception? seen = null;
        var decider = new RetryDecider(RetryPolicy.Custom((_, error) => { seen = error; return null; }), _logger);

        Assert.Null(decider.GetDelay(1, null, new JobExecutorNotFoundException("x.job")));
        Assert.Equal("x.job", Assert.IsType<JobExecutorNotFoundException>(seen).JobName);
    }

    [Fact]
    public void Attempt_below_one_is_rejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new RetryDecider(Minutes, _logger).GetDelay(0, [1], Error));
}
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~RetryPolicyTests|FullyQualifiedName~RetryDeciderTests"`
Expected: FAIL — ошибка компиляции: `RetryPolicy`, `RetryDecider`, `JobExecutorNotFoundException`, `PgBoltServerOptions.RetryPolicy` не найдены.

- [x] **Step 4: Реализовать `RetryPolicy` и `JobExecutorNotFoundException`**

`src/PgBolt.Server/RetryPolicy.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Задержка до следующей попытки после ошибки задачи (§3.4).</summary>
public abstract class RetryPolicy
{
    /// <param name="attempt">Номер завершившейся попытки, 1-based (как <c>JobContext.Attempt</c>).</param>
    /// <returns>Задержка до следующей попытки; null — не повторять.</returns>
    public abstract TimeSpan? GetDelay(int attempt, Exception error);

    /// <summary>По умолчанию: <c>Exponential(15 с, 1 ч, 0.2)</c>.</summary>
    public static RetryPolicy Default { get; } = Exponential(TimeSpan.FromSeconds(15), TimeSpan.FromHours(1), 0.2);

    /// <summary><c>min(baseDelay × 2^(attempt−1), maxDelay) × (1 + U(−jitter, +jitter))</c>.</summary>
    public static RetryPolicy Exponential(TimeSpan baseDelay, TimeSpan maxDelay, double jitter) =>
        Exponential(baseDelay, maxDelay, jitter, static () => Random.Shared.NextDouble());

    /// <param name="random">Источник U[0, 1) — для детерминированных тестов.</param>
    internal static RetryPolicy Exponential(TimeSpan baseDelay, TimeSpan maxDelay, double jitter, Func<double> random)
    {
        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(baseDelay), baseDelay, "baseDelay must be > 0.");
        if (maxDelay < baseDelay)
            throw new ArgumentOutOfRangeException(nameof(maxDelay), maxDelay, "maxDelay must be >= baseDelay.");
        if (!(jitter >= 0 && jitter < 1))
            throw new ArgumentOutOfRangeException(nameof(jitter), jitter, "jitter must be in [0, 1).");
        ArgumentNullException.ThrowIfNull(random);
        return new ExponentialRetryPolicy(baseDelay, maxDelay, jitter, random);
    }

    /// <summary>Политика на делегате; исключение делегата обрабатывается как null (§3.4).</summary>
    public static RetryPolicy Custom(Func<int, Exception, TimeSpan?> getDelay)
    {
        ArgumentNullException.ThrowIfNull(getDelay);
        return new CustomRetryPolicy(getDelay);
    }

    private sealed class ExponentialRetryPolicy(TimeSpan baseDelay, TimeSpan maxDelay, double jitter, Func<double> random)
        : RetryPolicy
    {
        public override TimeSpan? GetDelay(int attempt, Exception error)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
            var capped = Capped(attempt - 1);
            if (jitter == 0)
                return capped;
            // Jitter после потолка: результат в capped × (1 ± jitter).
            var factor = 1 + jitter * (2 * random() - 1);
            var ticks = capped.Ticks * factor;
            return ticks >= TimeSpan.MaxValue.Ticks ? TimeSpan.MaxValue : TimeSpan.FromTicks((long)Math.Round(ticks));
        }

        /// <summary>min(baseDelay × 2^exponent, maxDelay) в целых тиках, без переполнения при любом exponent.</summary>
        private TimeSpan Capped(int exponent)
        {
            if (exponent >= 62)
                return maxDelay;
            var multiplier = 1L << exponent;
            return baseDelay.Ticks > maxDelay.Ticks / multiplier
                ? maxDelay
                : TimeSpan.FromTicks(Math.Min(baseDelay.Ticks * multiplier, maxDelay.Ticks));
        }
    }

    private sealed class CustomRetryPolicy(Func<int, Exception, TimeSpan?> getDelay) : RetryPolicy
    {
        public override TimeSpan? GetDelay(int attempt, Exception error) => getDelay(attempt, error);
    }
}
```

`src/PgBolt.Server/JobExecutorNotFoundException.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Для типа задачи нет исполнителя в registry процесса; передаётся в <see cref="RetryPolicy"/> на пути NoExecutor (§3.4, §5.2).</summary>
public sealed class JobExecutorNotFoundException(string jobName)
    : Exception($"No executor is registered for job type '{jobName}'.")
{
    public string JobName { get; } = jobName;
}
```

- [x] **Step 5: Добавить `RetryPolicy` в опции сервера**

В `src/PgBolt.Server/PgBoltServerOptions.cs` после свойства `DefaultJobTimeout` вставить:

```csharp

    /// <summary>Политика повтора, если у задачи нет <c>retry_delays</c> (§3.4).</summary>
    public RetryPolicy RetryPolicy { get; set; } = RetryPolicy.Default;
```

и в `Validate` непосредственно перед проверкой `AckBatchSize` вставить:

```csharp
        if (RetryPolicy is null)
            throw Invalid(serverName, nameof(RetryPolicy), "must not be null");
```

- [x] **Step 6: Реализовать `RetryDecider`**

`src/PgBolt.Server/Runtime/RetryDecider.cs`:

```csharp
using Microsoft.Extensions.Logging;

namespace PgBolt.Server.Runtime;

/// <summary>Выбор задержки повтора (§3.4): <c>retry_delays</c> задачи → политика сервера.</summary>
internal sealed class RetryDecider(RetryPolicy policy, ILogger logger)
{
    private readonly RetryPolicy _policy = policy ?? throw new ArgumentNullException(nameof(policy));
    private readonly ILogger _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <param name="attempt">Номер завершившейся попытки, 1-based.</param>
    /// <param name="retryDelaysSeconds">retry_delays задачи; элемент [attempt−1], при нехватке — последний; пустой = null.</param>
    /// <returns>Задержка (≥ 0); null — не повторять.</returns>
    public TimeSpan? GetDelay(int attempt, int[]? retryDelaysSeconds, Exception error)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        if (retryDelaysSeconds is { Length: > 0 })
        {
            var seconds = retryDelaysSeconds[Math.Min(attempt - 1, retryDelaysSeconds.Length - 1)];
            return seconds <= 0 ? TimeSpan.Zero : TimeSpan.FromSeconds(seconds);
        }

        TimeSpan? delay;
        try
        {
            delay = _policy.GetDelay(attempt, error);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "PgBolt: RetryPolicy threw for attempt {Attempt}; the job is failed without retry.", attempt);
            return null;
        }

        return delay < TimeSpan.Zero ? TimeSpan.Zero : delay;
    }
}
```

- [x] **Step 7: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~RetryPolicyTests|FullyQualifiedName~RetryDeciderTests|FullyQualifiedName~ServerOptionsTests"`
Expected: PASS — `Failed: 0, Passed: 88` (39 новых + 49 из `ServerOptionsTests`).

- [x] **Step 8: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): retry policy with capped exponential backoff and retry decider"
```

---

### Task 10: `SlotAllocator` и план выборки

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Спека:** §4.1 (`SlotAllocator` выдаёт план `[(queue, n)]`), §4.2, §4.3 (`for (queue, n) in allocator.Plan()`), §10 unit (`SlotAllocator`).

**Files:**
- Create: `src/PgBolt.Server/Runtime/SlotAllocator.cs` (`SlotAllocator`, `ISharedPoolDistribution`, `StrictOrderDistribution`)
- Test: `tests/PgBolt.Server.Tests/Unit/SlotAllocatorTests.cs`

**Interfaces:**
- Consumes: `QueueOptions`, `PgBoltServerOptions.Queue`/`Queues` (Task 8).
- Produces (ровно по контракту):
  ```csharp
  internal sealed class SlotAllocator
  {
      public SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues);             // StrictOrderDistribution
      internal SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, ISharedPoolDistribution distribution);
      public IEnumerable<(string Queue, int N)> Plan(Func<string, bool> isDue);
      public int Available(string queue);
      public bool Take(string queue);
      public void Release(string queue, bool fromReserved);
      public int InUse(string queue);
  }
  internal interface ISharedPoolDistribution
  {
      (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available);
  }
  internal sealed class StrictOrderDistribution : ISharedPoolDistribution;
  ```
  Потокобезопасен (`System.Threading.Lock`).

Поведение (§4.2):
- `Available(q) = max(0, min(Reserved(q) − reservedInUse(q) + Workers − sharedInUse, MaxConcurrency(q) − inUse(q)))`; без `MaxConcurrency` второго члена нет.
- `Take` берёт сначала свободный Reserved очереди (`true`), затем общий (`false`); при `Available == 0` — `InvalidOperationException`. `Release` возвращает слот того же вида; освобождение невзятого слота — `InvalidOperationException` (ошибка учёта runtime, а не нормальный путь).
- Неизвестная очередь — `ArgumentException`.
- **План итерации — `Plan(isDue)`** (§4.1, §4.3): ленивый перечислитель `[(queue, n)]`. Каждая следующая запись вычисляется в момент `MoveNext`, то есть **после** `Take` по предыдущей записи (диспетчер между ними делает dequeue и занимает слоты), поэтому общие слоты, которые очередь не добрала фактическим dequeue, в той же итерации достаются следующим очередям. Каждая очередь попадает в план не больше одного раза (иначе «горячая» очередь с `nextPoll = now` занимала бы итерацию бесконечно); в план попадают только очереди с `isDue(q) = true` (наступил `nextPoll`) и `n ≥ 1` — очередь с `n = 0` в план не попадает, и диспетчер не трогает её `nextPoll` (§4.2). `isDue` вызывается под lock allocator'а — по разу на очередь для каждой записи плана (снимок `candidates`: стратегия и проверка её ответа видят один набор кандидаток, даже если время идёт) — и должен быть дешёвым (диспетчер читает свой `nextPoll`).
- **Стратегия распределения общего пула** — внутренняя `ISharedPoolDistribution` (§4.2: «веса (часть 3b) заменяют её без изменения `Dispatcher`»). Allocator передаёт ей число очередей (индексы — порядок объявления), `candidate(i)` (не выдавалась в этом плане и `isDue`) и `available(i)` (`Available` очереди); стратегия возвращает индекс и `n` или `null`. Allocator проверяет ответ (индекс-кандидат, `1 ≤ n ≤ Available`), иначе `InvalidOperationException` — ошибка стратегии. **`null` допустим, только если ни у одной кандидатки нет `available ≥ 1`**; иначе тоже `InvalidOperationException`. Это гарантия кодом, а не соглашение: диспетчер ждёт до ближайшего `nextPoll` среди очередей с `Available ≥ 1` (Task 15), и стратегия, пропустившая такую готовую очередь, дала бы пустой план при нулевой задержке — цикл крутился бы без ожидания. С проверкой пустой план означает «готовых очередей со свободным слотом нет», а сбойная стратегия даёт исключение, которое диспетчер логирует и за которым ставит паузу 1 → 30 с (Task 15). `StrictOrderDistribution` (по умолчанию, §2): первая по порядку объявления кандидатка с `available ≥ 1`, `n = available`. Стратегия получает `n` целиком: как делить общий пул (например, по весам) — её решение; `Dispatcher` только обходит план.

- [x] **Step 1: Написать падающие тесты**

`tests/PgBolt.Server.Tests/Unit/SlotAllocatorTests.cs`:

```csharp
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class SlotAllocatorTests
{
    private static readonly Func<string, bool> AllDue = _ => true;

    private static SlotAllocator Create(int workers, Action<PgBoltServerOptions> queues)
    {
        var o = new PgBoltServerOptions { Workers = workers };
        queues(o);
        return new SlotAllocator(o.Workers, o.Queues);
    }

    private static void TakeMany(SlotAllocator slots, string queue, int count)
    {
        for (var i = 0; i < count; i++)
            slots.Take(queue);
    }

    [Fact]
    public void First_queue_sees_its_reserved_plus_all_shared()
    {
        var slots = Create(10, o => o.Queue("critical", q => q.Reserved = 5).Queue("default"));

        Assert.Equal(15, slots.Available("critical"));
        Assert.Equal(10, slots.Available("default"));
    }

    [Fact]
    public void Plan_follows_declaration_order_and_yields_each_queue_once()
    {
        var slots = Create(4, o => o.Queue("a").Queue("b").Queue("c", q => q.Reserved = 1));

        // Ничего не занято: каждая очередь видит весь общий пул (c — ещё и свой Reserved), но попадает в план один раз.
        Assert.Equal([("a", 4), ("b", 4), ("c", 5)], slots.Plan(AllDue));
    }

    [Fact]
    public void Plan_is_recomputed_after_each_take_so_unused_shared_slots_move_on()
    {
        var slots = Create(10, o => o.Queue("a").Queue("b").Queue("c"));
        var plan = new List<(string Queue, int N)>();

        foreach (var (queue, n) in slots.Plan(AllDue))
        {
            plan.Add((queue, n));
            TakeMany(slots, queue, queue == "a" ? 7 : n);   // a добрала 7 из 10, b — всё, что ей досталось
        }

        Assert.Equal([("a", 10), ("b", 3)], plan);          // недобор a перешёл к b; c осталась без слотов — не в плане
    }

    [Fact]
    public void Reserved_slots_are_not_given_to_other_queues()
    {
        var slots = Create(2, o => o.Queue("critical", q => q.Reserved = 3).Queue("default"));

        TakeMany(slots, "default", 2);

        Assert.Equal([("critical", 3)], slots.Plan(AllDue));   // default: общих нет, Reserved чужой
        Assert.Equal(0, slots.Available("default"));
        Assert.Throws<InvalidOperationException>(() => slots.Take("default"));
    }

    [Fact]
    public void Queue_with_no_free_slots_is_not_in_plan()
    {
        var slots = Create(3, o => o.Queue("a", q => q.MaxConcurrency = 1).Queue("b"));

        slots.Take("a");

        Assert.Equal([("b", 2)], slots.Plan(AllDue));   // a упёрлась в MaxConcurrency: n = 0
    }

    [Fact]
    public void Plan_skips_queues_that_are_not_due()
    {
        var slots = Create(4, o => o.Queue("a").Queue("b"));

        Assert.Equal([("b", 4)], slots.Plan(q => q != "a"));
    }

    [Fact]
    public void Distribution_strategy_is_replaceable()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a").Queue("b");
        var slots = new SlotAllocator(o.Workers, o.Queues, new ReverseOrder());

        Assert.Equal([("b", 2), ("a", 2)], slots.Plan(AllDue));
    }

    [Fact]
    public void Distribution_ending_plan_while_a_due_queue_has_free_slots_is_rejected()
    {
        var o = new PgBoltServerOptions { Workers = 2 }.Queue("a").Queue("b", q => q.MaxConcurrency = 1);
        var slots = new SlotAllocator(o.Workers, o.Queues, new Declining());

        // Пустой план при готовой очереди со свободным слотом: диспетчер крутился бы без ожидания — ошибка стратегии.
        Assert.Throws<InvalidOperationException>(() => slots.Plan(AllDue).ToList());

        Assert.Empty(slots.Plan(_ => false));      // готовых очередей нет — пустой план законен
        slots.Take("a");
        slots.Take("b");
        Assert.Empty(slots.Plan(AllDue));          // свободных слотов нет (a: общий пул занят, b: MaxConcurrency) — тоже
    }

    [Fact]
    public void Take_uses_reserved_first_then_shared()
    {
        var slots = Create(5, o => o.Queue("q", q => q.Reserved = 2));

        Assert.True(slots.Take("q"));
        Assert.True(slots.Take("q"));
        Assert.False(slots.Take("q"));
        Assert.Equal(3, slots.InUse("q"));
        Assert.Equal(4, slots.Available("q"));
    }

    [Fact]
    public void Max_concurrency_counts_reserved_and_shared()
    {
        var slots = Create(10, o => o.Queue("emails", q => { q.Reserved = 2; q.MaxConcurrency = 4; }).Queue("other"));

        Assert.Equal(4, slots.Available("emails"));
        TakeMany(slots, "emails", 3);             // 2 reserved + 1 shared
        Assert.Equal(1, slots.Available("emails"));
        slots.Take("emails");
        Assert.Equal(0, slots.Available("emails"));
        Assert.Equal(8, slots.Available("other"));
    }

    [Fact]
    public void Max_concurrency_never_goes_negative_when_shared_is_scarce()
    {
        var slots = Create(1, o => o.Queue("a").Queue("b", q => q.MaxConcurrency = 5));

        slots.Take("a");

        Assert.Equal(0, slots.Available("b"));
    }

    [Fact]
    public void Release_returns_slot_of_the_same_kind()
    {
        var slots = Create(1, o => o.Queue("a", q => q.Reserved = 1).Queue("b"));
        var reserved = slots.Take("a");
        var shared = slots.Take("a");
        Assert.Equal(0, slots.Available("b"));

        slots.Release("a", shared);
        Assert.Equal(1, slots.Available("b"));    // общий слот вернулся в пул

        slots.Release("a", reserved);
        Assert.Equal(1, slots.Available("b"));    // reserved-слот другим не достаётся
        Assert.Equal(2, slots.Available("a"));
        Assert.Equal(0, slots.InUse("a"));
    }

    [Fact]
    public void Release_of_a_slot_that_was_not_taken_throws()
    {
        var slots = Create(1, o => o.Queue("a", q => q.Reserved = 1));

        Assert.Throws<InvalidOperationException>(() => slots.Release("a", fromReserved: true));
        Assert.Throws<InvalidOperationException>(() => slots.Release("a", fromReserved: false));
    }

    [Fact]
    public void Available_is_zero_when_exhausted_and_take_throws()
    {
        var slots = Create(2, o => o.Queue("a"));
        TakeMany(slots, "a", 2);

        Assert.Equal(0, slots.Available("a"));
        Assert.Throws<InvalidOperationException>(() => slots.Take("a"));
        Assert.Equal(2, slots.InUse("a"));
        Assert.Empty(slots.Plan(AllDue));
    }

    [Fact]
    public void Zero_workers_queue_runs_only_on_reserved()
    {
        var slots = Create(0, o => o.Queue("a", q => q.Reserved = 2).Queue("b"));

        Assert.Equal(2, slots.Available("a"));
        Assert.Equal(0, slots.Available("b"));
        Assert.True(slots.Take("a"));
        Assert.True(slots.Take("a"));
        Assert.Equal(0, slots.Available("a"));
    }

    [Fact]
    public void Unknown_queue_is_rejected()
    {
        var slots = Create(1, o => o.Queue("a"));
        Assert.Throws<ArgumentException>(() => slots.Available("nope"));
        Assert.Throws<ArgumentException>(() => slots.Take("nope"));
    }

    [Fact]
    public void Concurrent_take_and_release_never_exceed_capacity()
    {
        const int workers = 4;
        var slots = Create(workers, o => o.Queue("a", q => q.Reserved = 2).Queue("b", q => q.MaxConcurrency = 3));
        int heldA = 0, heldB = 0, heldShared = 0, maxA = 0, maxB = 0, maxShared = 0, rejected = 0, taken = 0;

        Parallel.For(0, 8, new ParallelOptions { MaxDegreeOfParallelism = 8 }, worker =>
        {
            var queue = worker % 2 == 0 ? "a" : "b";
            for (var i = 0; i < 20_000; i++)
            {
                bool fromReserved;
                try
                {
                    fromReserved = slots.Take(queue);
                }
                catch (InvalidOperationException)
                {
                    Interlocked.Increment(ref rejected);
                    continue;
                }

                Interlocked.Increment(ref taken);
                if (queue == "a") Max(ref maxA, Interlocked.Increment(ref heldA));
                else Max(ref maxB, Interlocked.Increment(ref heldB));
                if (!fromReserved) Max(ref maxShared, Interlocked.Increment(ref heldShared));

                if (!fromReserved) Interlocked.Decrement(ref heldShared);
                if (queue == "a") Interlocked.Decrement(ref heldA);
                else Interlocked.Decrement(ref heldB);
                slots.Release(queue, fromReserved);
            }
        });

        Assert.True(taken > 0);
        Assert.InRange(maxA, 1, 2 + workers);
        Assert.InRange(maxB, 1, 3);
        Assert.InRange(maxShared, 1, workers);
        Assert.Equal(0, slots.InUse("a"));
        Assert.Equal(0, slots.InUse("b"));
        Assert.Equal(2 + workers, slots.Available("a"));
        Assert.Equal(3, slots.Available("b"));
        Assert.Equal(8 * 20_000, taken + rejected);
    }

    private static void Max(ref int target, int value)
    {
        var current = Volatile.Read(ref target);
        while (value > current)
        {
            var seen = Interlocked.CompareExchange(ref target, value, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>Подменная стратегия, заканчивающая план сразу (нарушает контракт при готовой очереди со свободным слотом).</summary>
    private sealed class Declining : ISharedPoolDistribution
    {
        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available) => null;
    }

    /// <summary>Подменная стратегия: обратный порядок объявления (проверка, что план берётся из стратегии).</summary>
    private sealed class ReverseOrder : ISharedPoolDistribution
    {
        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available)
        {
            for (var i = queueCount - 1; i >= 0; i--)
            {
                if (!candidate(i))
                    continue;
                var n = available(i);
                if (n >= 1)
                    return (i, n);
            }
            return null;
        }
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~SlotAllocatorTests"`
Expected: FAIL — ошибка компиляции: `SlotAllocator`, `ISharedPoolDistribution` не найдены.

- [x] **Step 3: Реализовать**

`src/PgBolt.Server/Runtime/SlotAllocator.cs`:

```csharp
namespace PgBolt.Server.Runtime;

/// <summary>
/// Распределение общего пула между очередями (§4.2) — внутренняя стратегия <see cref="SlotAllocator"/>.
/// Веса части 3b заменяют её без изменения Dispatcher. Вызывается под lock allocator'а.
/// </summary>
internal interface ISharedPoolDistribution
{
    /// <summary>Следующая запись плана выборки.</summary>
    /// <param name="queueCount">Число очередей; индексы — порядок объявления.</param>
    /// <param name="candidate">Очередь ещё не выдавалась в этом плане и её nextPoll наступил.</param>
    /// <param name="available">Свободные слоты очереди (reserved + общие с учётом MaxConcurrency).</param>
    /// <returns>
    /// Индекс очереди и n ≥ 1; null — план исчерпан. null допустим, только если ни у одной кандидатки нет available ≥ 1
    /// (иначе SlotAllocator бросает InvalidOperationException).
    /// </returns>
    (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available);
}

/// <summary>Строгий порядок объявления (§2): первая кандидатка со свободным слотом получает всё, что ей доступно.</summary>
internal sealed class StrictOrderDistribution : ISharedPoolDistribution
{
    public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available)
    {
        for (var i = 0; i < queueCount; i++)
        {
            if (!candidate(i))
                continue;
            var n = available(i);
            if (n >= 1)
                return (i, n);
        }
        return null;
    }
}

/// <summary>
/// Счётчики слотов виртуального сервера и план выборки (§4.1, §4.2): Reserved очереди доступны только ей, общий
/// пул — Workers; MaxConcurrency ограничивает очередь целиком. Порядок и доли общего пула задаёт
/// <see cref="ISharedPoolDistribution"/>. Потокобезопасен.
/// </summary>
internal sealed class SlotAllocator
{
    private readonly Lock _lock = new();
    private readonly int _workers;
    private readonly ISharedPoolDistribution _distribution;
    private readonly QueueSlots[] _order;
    private readonly Dictionary<string, QueueSlots> _queues;
    private int _sharedInUse;

    public SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues)
        : this(workers, queues, new StrictOrderDistribution())
    {
    }

    internal SlotAllocator(int workers, IReadOnlyList<QueueOptions> queues, ISharedPoolDistribution distribution)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(workers);
        ArgumentNullException.ThrowIfNull(queues);
        ArgumentNullException.ThrowIfNull(distribution);
        _workers = workers;
        _distribution = distribution;
        _order = [.. queues.Select(q => new QueueSlots(q.Name, q.Reserved, q.MaxConcurrency))];
        _queues = new Dictionary<string, QueueSlots>(_order.Length, StringComparer.Ordinal);
        foreach (var slots in _order)
            _queues.Add(slots.Name, slots);
    }

    /// <summary>
    /// План итерации [(queue, n)] (§4.2, §4.3). Ленивый: следующая запись вычисляется при MoveNext, то есть после
    /// Take по предыдущей, поэтому недобранные общие слоты переходят к следующим очередям той же итерации.
    /// Очередь — не больше раза за план; только isDue и n ≥ 1.
    /// </summary>
    /// <param name="isDue">nextPoll очереди наступил; вызывается под lock, должен быть дешёвым.</param>
    public IEnumerable<(string Queue, int N)> Plan(Func<string, bool> isDue)
    {
        ArgumentNullException.ThrowIfNull(isDue);
        return PlanIterator(isDue);
    }

    /// <summary>min(свободные Reserved очереди + свободные общие, MaxConcurrency − занято), ≥ 0.</summary>
    public int Available(string queue)
    {
        lock (_lock)
            return AvailableCore(Get(queue));
    }

    /// <summary>Занимает один слот: сначала Reserved очереди, затем общий.</summary>
    /// <returns>true — слот из Reserved, false — общий.</returns>
    /// <exception cref="InvalidOperationException">Свободных слотов для очереди нет.</exception>
    public bool Take(string queue)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            if (AvailableCore(slots) == 0)
                throw new InvalidOperationException($"No free slot for queue '{queue}'.");
            if (slots.ReservedInUse < slots.Reserved)
            {
                slots.ReservedInUse++;
                return true;
            }
            slots.SharedInUse++;
            _sharedInUse++;
            return false;
        }
    }

    /// <summary>Возвращает слот того же вида, что выдал <see cref="Take"/>.</summary>
    public void Release(string queue, bool fromReserved)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            if (fromReserved)
            {
                if (slots.ReservedInUse == 0)
                    throw new InvalidOperationException($"No reserved slot of queue '{queue}' is taken.");
                slots.ReservedInUse--;
            }
            else
            {
                if (slots.SharedInUse == 0)
                    throw new InvalidOperationException($"No shared slot of queue '{queue}' is taken.");
                slots.SharedInUse--;
                _sharedInUse--;
            }
        }
    }

    /// <summary>Занято слотов очереди (reserved + общие).</summary>
    public int InUse(string queue)
    {
        lock (_lock)
        {
            var slots = Get(queue);
            return slots.ReservedInUse + slots.SharedInUse;
        }
    }

    private IEnumerable<(string Queue, int N)> PlanIterator(Func<string, bool> isDue)
    {
        var planned = new bool[_order.Length];
        var candidates = new bool[_order.Length];
        while (NextEntry(planned, candidates, isDue) is { } entry)
        {
            planned[entry.Queue] = true;
            yield return (_order[entry.Queue].Name, entry.N);
        }
    }

    /// <summary>
    /// Следующая запись по текущим счётчикам; под lock (вне итератора: yield под lock запрещён). isDue вычисляется один
    /// раз на запись (в candidates), чтобы стратегия и проверка её ответа видели один и тот же набор кандидаток: иначе
    /// очередь, ставшая готовой между ними по ходу времени, дала бы ложное нарушение контракта.
    /// </summary>
    private (int Queue, int N)? NextEntry(bool[] planned, bool[] candidates, Func<string, bool> isDue)
    {
        lock (_lock)
        {
            for (var i = 0; i < _order.Length; i++)
                candidates[i] = !planned[i] && isDue(_order[i].Name);
            var next = _distribution.Next(_order.Length, i => candidates[i], i => AvailableCore(_order[i]));
            if (next is not { } entry)
            {
                // Контракт стратегии: план кончается, только когда готовых очередей со свободным слотом нет. Иначе
                // диспетчер (NextPollDelay = 0 для такой очереди) крутился бы без ожидания — лучше явная ошибка и пауза.
                for (var i = 0; i < _order.Length; i++)
                {
                    if (candidates[i] && AvailableCore(_order[i]) >= 1)
                        throw new InvalidOperationException(
                            $"Shared pool distribution ended the plan while queue '{_order[i].Name}' is due and has free slots.");
                }
                return null;
            }
            if (entry.Queue < 0 || entry.Queue >= _order.Length || !candidates[entry.Queue]
                || entry.N < 1 || entry.N > AvailableCore(_order[entry.Queue]))
                throw new InvalidOperationException(
                    $"Shared pool distribution returned an invalid plan entry ({entry.Queue}, {entry.N}).");
            return entry;
        }
    }

    private int AvailableCore(QueueSlots slots)
    {
        var free = slots.Reserved - slots.ReservedInUse + (_workers - _sharedInUse);
        if (slots.MaxConcurrency is int max)
            free = Math.Min(free, max - slots.ReservedInUse - slots.SharedInUse);
        return Math.Max(0, free);
    }

    private QueueSlots Get(string queue) =>
        _queues.TryGetValue(queue, out var slots)
            ? slots
            : throw new ArgumentException($"Queue '{queue}' is not declared on this server.", nameof(queue));

    private sealed class QueueSlots(string name, int reserved, int? maxConcurrency)
    {
        public string Name { get; } = name;
        public int Reserved { get; } = reserved;
        public int? MaxConcurrency { get; } = maxConcurrency;
        public int ReservedInUse { get; set; }
        public int SharedInUse { get; set; }
    }
}
```

`isDue` и стратегия вызываются под `_lock` и получают только делегаты над уже захваченным состоянием; публичные методы allocator'а из них не вызываются. `yield return` — вне `lock` (отдельный метод `NextEntry`): в итераторе нельзя держать `Lock.Scope` через `yield`, а держать lock, пока диспетчер делает dequeue, нельзя тем более.

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~SlotAllocatorTests"`
Expected: PASS — `Failed: 0, Passed: 17`.

Контроль, что тесты ловят регрессию (каждую правку внести отдельно, прогнать, убедиться в падении, вернуть код Step 3 — файл ещё не закоммичен):
- в `PlanIterator` вычислить все записи заранее (`ToList()` внутри итератора до первого `yield`) → падает `Plan_is_recomputed_after_each_take_so_unused_shared_slots_move_on`;
- в `NextEntry` убрать проверку готовых очередей при `null` от стратегии → падает `Distribution_ending_plan_while_a_due_queue_has_free_slots_is_rejected`.

- [x] **Step 5: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): slot allocator with fetch plan and replaceable shared pool distribution"
```

---

### Task 11: `Execution`, `ExecutionTable`, `JobOutcome`, `IServerStore` и тестовая поддержка

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Server/Runtime/Execution.cs` (`ExecutionState`, `CancelReason`, `Execution`), `src/PgBolt.Server/Runtime/ExecutionTable.cs`, `src/PgBolt.Server/Runtime/JobOutcome.cs`
- Create: `src/PgBolt.Server/Runtime/IServerStore.cs` (`ServerRegistration`, `IServerStore`), `src/PgBolt.Server/Runtime/PgBoltServerStore.cs`
- Create: `tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`, `tests/PgBolt.Server.Tests/Fakes/Leased.cs`, `tests/PgBolt.Server.Tests/Fakes/Eventually.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/ExecutionTests.cs`, `tests/PgBolt.Server.Tests/Unit/ExecutionTableTests.cs`, `tests/PgBolt.Server.Tests/Unit/FakeServerStoreTests.cs`

**Interfaces:**
- Consumes: `LeasedJob`, `LeaseRef`, `HeartbeatLease`, `FailOutcome`, `PromoteResult`, `JobTypeRecord` (`PgBolt.Postgres.Storage`); `ReleaseOutcome`, `PgBoltStore.HeartbeatAsync(Guid, IReadOnlyCollection<LeaseRef>, TimeSpan, CancellationToken)`, `ReleaseAsync`, `RegisterServerAsync`, `RemoveServerAsync`, `GetJobTypeNamesByQueuesAsync` (Tasks 4–5); `JobEventKind` (`PgBolt.Abstractions`).
- Produces (ровно по контракту):
  - `internal enum ExecutionState { Running, OutcomePending, OutcomeInFlight, Removed }`, `internal enum CancelReason { None, Shutdown, CancelRequested, HeartbeatLost, LeaseLost, Timeout }`
  - `internal abstract record JobOutcome` с `Ack`, `Retry(TimeSpan Delay, JobEventKind Kind, string? Message, string? Exception)`, `Fail(JobEventKind Kind, string? Message, string? Exception)`, `ConfirmCancel(string? Message)`, `Release(bool Ran)`
  - `internal sealed class Execution` — `Execution(LeasedJob job, string queue, long validUntil, ExecutionState initialState)`, `Job`, `Queue`, `Lease`, `FromReserved`, `Token`, `FirstReason`, `bool Cancel(CancelReason)`, `ValidUntil`, `ExtendValidUntil(long)`, `State`, `bool TryTransition(ExecutionState from, ExecutionState to)`, `Outcome`, `OutcomeFirstAttemptAt`
  - `internal sealed class ExecutionTable` — `Add`, `Remove`, `TryGet(Guid leaseId, [MaybeNullWhen(false)] out Execution)`, `Snapshot()`, `Count`
  - `internal sealed record ServerRegistration(Guid Id, string Name, string Host, string[] Queues, int Capacity)`, `internal interface IServerStore` (13 методов контракта), `internal sealed class PgBoltServerStore(PgBoltStore store) : IServerStore`
  - Тестовая поддержка (namespace `PgBolt.Server.Tests.Fakes`): `StoreCall`, `FakeServerStore` (+ `public static IReadOnlyList<HeartbeatLease> AllAlive(IEnumerable<LeaseRef> leases)` — ответ «все живы»), `Leased.Job(...)`, `Eventually.True(Func<bool> condition, string because)`.

Поведение (§6.2):
- **Переходы.** Разрешены только `Running→OutcomePending`, `OutcomePending→OutcomeInFlight`, `OutcomeInFlight→OutcomePending`, `Running→Removed`, `OutcomePending→Removed`, `OutcomeInFlight→Removed`; любая другая пара `from→to` — `InvalidOperationException` (ошибка программиста, независимо от текущего состояния). Разрешённая пара при несовпадении текущего состояния — `false` без изменений (`Interlocked.CompareExchange`). Начальное состояние — только `Running` или `OutcomePending` (§6.2: задача регистрируется одним из них), иначе `ArgumentOutOfRangeException`.
- **Отмена.** `Cancel(reason)`: CAS `FirstReason None→reason` (`true`, если первая), затем всегда отмена токена. `CancellationTokenSource` создаётся в конструкторе и не освобождается (таймеров у него нет). Отмена — `CancelAsync()`: `IsCancellationRequested` становится `true` синхронно, а колбэки токена (связанный CTS runner'а и продолжения кода задачи) выполняются на пуле потоков — код задачи не исполняется на потоке heartbeat или таймера сроков и не держит их. `Cancel(None)` — `ArgumentOutOfRangeException`.
- **`ExtendValidUntil`** — CAS max: значение только растёт.
- **`Outcome`** — обычное свойство: пишется до CAS-перехода в `OutcomePending`, `Interlocked.CompareExchange` — полный барьер, поэтому читатель, увидевший состояние, видит и исход.
- **`ExecutionTable`** — `ConcurrentDictionary` по `LeaseId`: одна задача, повторно взятая после sweep, — две записи с разными `lease_id` (Review Focus п. 4). `Add` дубля `lease_id` — `InvalidOperationException`. `Remove` удаляет только тот же объект (`TryRemove(KeyValuePair)`). `Snapshot()` — копия записей **без** `Removed` (запись в `Removed` удаляется из таблицы сразу после перехода; окно между ними снимок не видит).
- **`PgBoltServerStore`** — чистый проброс; `RegisterServerAsync(s)` → `store.RegisterServerAsync(s.Id, s.Name, s.Host, s.Queues, s.Capacity, ct)`. Отдельного unit-теста нет: логики в нём нет, а проверка «делегирует» без БД свелась бы к тавтологии; компиляция проверяет соответствие сигнатур, поведение покрывают интеграционные тесты Tasks 17–20 (каждый метод вызывается реальным `PgBoltServer` через этот класс).
- **`Eventually.True`** — единственное допустимое реальное ожидание unit-тестов: опрос условия раз в 1 мс, не дольше 5 с, затем `Assert.Fail` с `because`. Нужен, потому что после `FakeTimeProvider.Advance` таймеры и `Task.Delay` срабатывают синхронно, но продолжения `async`-методов (цикл heartbeat, завершение вызова фейка с `RunContinuationsAsynchronously`) выполняются на пуле потоков позже. Время runtime по-прежнему двигает только `FakeTimeProvider`; «ничего не произошло» проверяется синхронно сразу после `Advance` (таймеры уже отработали).

- [x] **Step 1: Написать тестовую поддержку**

`tests/PgBolt.Server.Tests/Fakes/FakeServerStore.cs`:

```csharp
using System.Collections.Concurrent;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Fakes;

internal sealed record StoreCall(string Op, Guid? JobId = null, Guid? LeaseId = null, object? Detail = null);

/// <summary>Программируемый IServerStore: поведение задаётся делегатами, все вызовы пишутся в Calls до вызова делегата.</summary>
internal sealed class FakeServerStore : IServerStore
{
    public ConcurrentQueue<StoreCall> Calls { get; } = new();

    public Func<string, int, CancellationToken, Task<IReadOnlyList<LeasedJob>>> OnDequeue { get; set; } =
        (_, _, _) => Task.FromResult<IReadOnlyList<LeasedJob>>([]);

    public Func<IReadOnlyList<LeaseRef>, CancellationToken, Task<int>> OnAck { get; set; } =
        (leases, _) => Task.FromResult(leases.Count);

    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnRetry { get; set; } =
        (_, _, _) => Task.FromResult(FailOutcome.Retried);

    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnFail { get; set; } =
        (_, _, _) => Task.FromResult(FailOutcome.Failed);

    public Func<Guid, Guid, CancellationToken, Task<FailOutcome>> OnConfirmCancel { get; set; } =
        (_, _, _) => Task.FromResult(FailOutcome.Cancelled);

    public Func<Guid, Guid, bool, CancellationToken, Task<ReleaseOutcome>> OnRelease { get; set; } =
        (_, _, _, _) => Task.FromResult(ReleaseOutcome.Released);

    public Func<IReadOnlyCollection<LeaseRef>, CancellationToken, Task<IReadOnlyList<HeartbeatLease>>> OnHeartbeat { get; set; } =
        (leases, _) => Task.FromResult(AllAlive(leases));

    public Func<int, CancellationToken, Task<PromoteResult>> OnPromote { get; set; } =
        (_, _) => Task.FromResult(new PromoteResult(0, []));

    public Func<int, CancellationToken, Task<int>> OnSweep { get; set; } = (_, _) => Task.FromResult(0);

    public Func<IReadOnlyList<string>, CancellationToken, Task<IReadOnlyList<string>>> OnJobTypeNames { get; set; } =
        (_, _) => Task.FromResult<IReadOnlyList<string>>([]);

    public Func<ServerRegistration, CancellationToken, Task> OnRegisterServer { get; set; } = (_, _) => Task.CompletedTask;

    /// <summary>Ответ heartbeat «все lease живы, отмены нет».</summary>
    public static IReadOnlyList<HeartbeatLease> AllAlive(IEnumerable<LeaseRef> leases) =>
        [.. leases.Select(l => new HeartbeatLease(l.JobId, l.LeaseId, false))];

    public IEnumerable<StoreCall> CallsOf(string op) => Calls.Where(c => c.Op == op);

    public Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Dequeue", Detail: (queue, max)));
        return OnDequeue(queue, max, ct);
    }

    public Task<int> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Ack", Detail: leases.ToArray()));
        return OnAck(leases, ct);
    }

    public Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Retry", jobId, leaseId, (delay, kind, message)));
        return OnRetry(jobId, leaseId, ct);
    }

    public Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Fail", jobId, leaseId, (kind, message)));
        return OnFail(jobId, leaseId, ct);
    }

    public Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("ConfirmCancel", jobId, leaseId, message));
        return OnConfirmCancel(jobId, leaseId, ct);
    }

    public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Release", jobId, leaseId, ran));
        return OnRelease(jobId, leaseId, ran, ct);
    }

    public Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Heartbeat", Detail: leases.ToArray()));
        return OnHeartbeat(leases, ct);
    }

    public Task RegisterServerAsync(ServerRegistration server, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("RegisterServer", Detail: server));
        return OnRegisterServer(server, ct);
    }

    public Task RemoveServerAsync(Guid serverId, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("RemoveServer", Detail: serverId));
        return Task.CompletedTask;
    }

    public Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Promote", Detail: batchSize));
        return OnPromote(batchSize, ct);
    }

    public Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("Sweep", Detail: batchSize));
        return OnSweep(batchSize, ct);
    }

    public Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("PublishJobTypes", Detail: (types, publishedBy)));
        return Task.FromResult<IReadOnlyList<string>>([]);
    }

    public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct)
    {
        Calls.Enqueue(new StoreCall("JobTypeNames", Detail: queues.ToArray()));
        return OnJobTypeNames(queues, ct);
    }
}
```

`tests/PgBolt.Server.Tests/Fakes/Leased.cs`:

```csharp
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Tests.Fakes;

/// <summary>Фабрика LeasedJob для unit-тестов.</summary>
internal static class Leased
{
    /// <summary>Новый lease (LeaseId = Guid.NewGuid()); тот же <paramref name="id"/> даёт повторно взятую задачу.</summary>
    public static LeasedJob Job(Guid? id = null, string type = "test.job", string args = "{}", int attempt = 1,
        int maxAttempts = 3, int[]? retryDelays = null) =>
        new(id ?? Guid.CreateVersion7(), Guid.NewGuid(), type, args, attempt, maxAttempts, retryDelays,
            ParentId: null, BatchId: null, TraceParent: null);
}
```

`tests/PgBolt.Server.Tests/Fakes/Eventually.cs`:

```csharp
using System.Diagnostics;

namespace PgBolt.Server.Tests.Fakes;

/// <summary>
/// Ожидание условия, которое выставит асинхронное продолжение после <c>FakeTimeProvider.Advance</c> или завершения
/// задачи фейка. Единственное допустимое реальное ожидание в unit-тестах: опрос раз в 1 мс, не дольше 5 с.
/// Логика runtime от него не зависит — время тестов по-прежнему двигает только FakeTimeProvider.
/// </summary>
internal static class Eventually
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(5);

    public static async Task True(Func<bool> condition, string because)
    {
        var watch = Stopwatch.StartNew();
        while (!condition())
        {
            if (watch.Elapsed > Limit)
                Assert.Fail($"Condition was not met within {Limit.TotalSeconds} s: {because}");
            await Task.Delay(1, TestContext.Current.CancellationToken);
        }
    }
}
```

- [x] **Step 2: Написать падающие тесты**

`tests/PgBolt.Server.Tests/Unit/ExecutionTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class ExecutionTests
{
    /// <summary>Theory-параметры — строки: internal-enum нельзя сделать параметром public-метода теста.</summary>
    private static ExecutionState S(string name) => Enum.Parse<ExecutionState>(name);

    private static Execution Create(ExecutionState state = ExecutionState.Running, long validUntil = 1_000) =>
        new(Leased.Job(), "q", validUntil, state);

    /// <summary>Переводит свежую запись в нужное состояние только разрешёнными переходами.</summary>
    private static Execution In(ExecutionState state)
    {
        var e = Create();
        switch (state)
        {
            case ExecutionState.Running:
                break;
            case ExecutionState.OutcomePending:
                Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
                break;
            case ExecutionState.OutcomeInFlight:
                Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
                Assert.True(e.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight));
                break;
            case ExecutionState.Removed:
                Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.Removed));
                break;
        }
        Assert.Equal(state, e.State);
        return e;
    }

    [Fact]
    public void Constructor_sets_identity_and_initial_state()
    {
        var job = Leased.Job();
        var e = new Execution(job, "emails", 42, ExecutionState.OutcomePending);

        Assert.Same(job, e.Job);
        Assert.Equal("emails", e.Queue);
        Assert.Equal(new LeaseRef(job.Id, job.LeaseId), e.Lease);
        Assert.Equal(42, e.ValidUntil);
        Assert.Equal(ExecutionState.OutcomePending, e.State);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.False(e.Token.IsCancellationRequested);
        Assert.Null(e.Outcome);
        Assert.Equal(0, e.OutcomeFirstAttemptAt);
    }

    [Theory]
    [InlineData("OutcomeInFlight")]
    [InlineData("Removed")]
    public void Constructor_rejects_states_an_execution_cannot_start_in(string state) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Create(S(state)));

    [Theory]
    [InlineData("Running", "OutcomePending")]
    [InlineData("OutcomePending", "OutcomeInFlight")]
    [InlineData("OutcomeInFlight", "OutcomePending")]
    [InlineData("Running", "Removed")]
    [InlineData("OutcomePending", "Removed")]
    [InlineData("OutcomeInFlight", "Removed")]
    public void Allowed_transition_succeeds_from_the_expected_state(string from, string to)
    {
        var e = In(S(from));
        Assert.True(e.TryTransition(S(from), S(to)));
        Assert.Equal(S(to), e.State);
    }

    [Theory]
    [InlineData("Running", "OutcomePending", "OutcomePending")]
    [InlineData("OutcomePending", "OutcomeInFlight", "Running")]
    [InlineData("OutcomePending", "Removed", "OutcomeInFlight")]
    [InlineData("Running", "Removed", "Removed")]
    public void Allowed_transition_from_another_state_returns_false_and_keeps_state(string from, string to, string actual)
    {
        var e = In(S(actual));
        Assert.False(e.TryTransition(S(from), S(to)));
        Assert.Equal(S(actual), e.State);
    }

    [Theory]
    [InlineData("Running", "OutcomeInFlight")]
    [InlineData("Running", "Running")]
    [InlineData("OutcomePending", "Running")]
    [InlineData("OutcomeInFlight", "Running")]
    [InlineData("Removed", "Running")]
    [InlineData("Removed", "OutcomePending")]
    [InlineData("Removed", "Removed")]
    public void Transition_outside_the_spec_throws(string from, string to)
    {
        var e = In(S(from));
        Assert.Throws<InvalidOperationException>(() => e.TryTransition(S(from), S(to)));
        Assert.Equal(S(from), e.State);
    }

    [Fact]
    public async Task Sink_and_heartbeat_race_on_outcome_pending_has_exactly_one_winner()
    {
        const int threads = 8;
        for (var round = 0; round < 500; round++)
        {
            var e = In(ExecutionState.OutcomePending);
            using var start = new Barrier(threads);
            var wins = new int[threads];

            var tasks = Enumerable.Range(0, threads).Select(i => Task.Factory.StartNew(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                var to = i % 2 == 0 ? ExecutionState.OutcomeInFlight : ExecutionState.Removed;
                if (e.TryTransition(ExecutionState.OutcomePending, to))
                    wins[i] = 1;
            }, TestContext.Current.CancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
            await Task.WhenAll(tasks);

            Assert.Equal(1, wins.Sum());
            var winner = Array.IndexOf(wins, 1);
            Assert.Equal(winner % 2 == 0 ? ExecutionState.OutcomeInFlight : ExecutionState.Removed, e.State);
        }
    }

    [Fact]
    public void First_cancel_reason_wins_and_token_is_cancelled()
    {
        var e = Create();

        Assert.True(e.Cancel(CancelReason.Timeout));
        Assert.True(e.Token.IsCancellationRequested);
        Assert.False(e.Cancel(CancelReason.Shutdown));
        Assert.False(e.Cancel(CancelReason.Timeout));

        Assert.Equal(CancelReason.Timeout, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
    }

    [Fact]
    public void Cancel_with_none_is_rejected()
    {
        var e = Create();
        Assert.Throws<ArgumentOutOfRangeException>(() => e.Cancel(CancelReason.None));
        Assert.False(e.Token.IsCancellationRequested);
    }

    [Fact]
    public void Cancel_does_not_depend_on_state()
    {
        var e = In(ExecutionState.Removed);
        Assert.True(e.Cancel(CancelReason.LeaseLost));
        Assert.True(e.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task Concurrent_cancels_record_exactly_one_first_reason()
    {
        CancelReason[] reasons = [CancelReason.Shutdown, CancelReason.CancelRequested, CancelReason.HeartbeatLost,
            CancelReason.LeaseLost, CancelReason.Timeout];
        for (var round = 0; round < 500; round++)
        {
            var e = Create();
            using var start = new Barrier(reasons.Length);
            var firsts = new bool[reasons.Length];

            await Task.WhenAll(reasons.Select((reason, i) => Task.Factory.StartNew(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                firsts[i] = e.Cancel(reason);
            }, TestContext.Current.CancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default)));

            Assert.Single(firsts, f => f);
            Assert.Equal(reasons[Array.IndexOf(firsts, true)], e.FirstReason);
            Assert.True(e.Token.IsCancellationRequested);
        }
    }

    [Fact]
    public void Valid_until_only_grows()
    {
        var e = Create(validUntil: 100);

        e.ExtendValidUntil(90);
        Assert.Equal(100, e.ValidUntil);
        e.ExtendValidUntil(150);
        Assert.Equal(150, e.ValidUntil);
        e.ExtendValidUntil(120);
        Assert.Equal(150, e.ValidUntil);
    }

    [Fact]
    public void Concurrent_extensions_keep_the_maximum()
    {
        var e = Create(validUntil: 0);
        Parallel.For(1, 10_001, i => e.ExtendValidUntil(i));
        Assert.Equal(10_000, e.ValidUntil);
    }
}
```

`tests/PgBolt.Server.Tests/Unit/ExecutionTableTests.cs`:

```csharp
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class ExecutionTableTests
{
    private static Execution Running(Guid? jobId = null) => new(Leased.Job(jobId), "q", 0, ExecutionState.Running);

    [Fact]
    public void Add_get_remove()
    {
        var table = new ExecutionTable();
        var e = Running();

        table.Add(e);
        Assert.Equal(1, table.Count);
        Assert.True(table.TryGet(e.Lease.LeaseId, out var found));
        Assert.Same(e, found);

        Assert.True(table.Remove(e));
        Assert.False(table.Remove(e));
        Assert.Equal(0, table.Count);
        Assert.False(table.TryGet(e.Lease.LeaseId, out _));
    }

    [Fact]
    public void Duplicate_lease_is_rejected()
    {
        var table = new ExecutionTable();
        var e = Running();
        table.Add(e);

        var twin = new Execution(e.Job, "q", 0, ExecutionState.Running);
        Assert.Throws<InvalidOperationException>(() => table.Add(twin));
        Assert.False(table.Remove(twin));             // удаляется только тот же объект
        Assert.True(table.TryGet(e.Lease.LeaseId, out var found));
        Assert.Same(e, found);
    }

    [Fact]
    public void Same_job_taken_again_with_a_new_lease_is_a_separate_entry()
    {
        var table = new ExecutionTable();
        var jobId = Guid.CreateVersion7();
        var stale = Running(jobId);
        var fresh = Running(jobId);
        Assert.NotEqual(stale.Lease.LeaseId, fresh.Lease.LeaseId);

        table.Add(stale);
        table.Add(fresh);
        Assert.Equal(2, table.Count);
        Assert.True(table.TryGet(stale.Lease.LeaseId, out var a));
        Assert.True(table.TryGet(fresh.Lease.LeaseId, out var b));
        Assert.Same(stale, a);
        Assert.Same(fresh, b);

        Assert.True(table.Remove(stale));
        Assert.True(table.TryGet(fresh.Lease.LeaseId, out var left));
        Assert.Same(fresh, left);
        Assert.Equal([fresh.Lease], table.Snapshot().Select(x => x.Lease));
    }

    [Fact]
    public void Snapshot_skips_removed_and_is_a_copy()
    {
        var table = new ExecutionTable();
        var running = Running();
        var pending = new Execution(Leased.Job(), "q", 0, ExecutionState.OutcomePending);
        var removed = Running();
        Assert.True(removed.TryTransition(ExecutionState.Running, ExecutionState.Removed));
        table.Add(running);
        table.Add(pending);
        table.Add(removed);

        var snapshot = table.Snapshot();
        table.Add(Running());

        Assert.Equal(2, snapshot.Count);
        Assert.Contains(running, snapshot);
        Assert.Contains(pending, snapshot);
        Assert.Equal(4, table.Count);
    }
}
```

`tests/PgBolt.Server.Tests/Unit/FakeServerStoreTests.cs`:

```csharp
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

/// <summary>Проверка тестовой поддержки: умолчания фейка и запись вызовов, на которые опираются тесты Tasks 12–16.</summary>
public sealed class FakeServerStoreTests
{
    private static readonly Guid Server = Guid.CreateVersion7();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Defaults_and_recorded_calls()
    {
        var store = new FakeServerStore();
        var job = Leased.Job();
        var lease = new LeaseRef(job.Id, job.LeaseId);

        Assert.Empty(await store.DequeueAsync("q", 5, Server, TimeSpan.FromMinutes(1), Ct));
        Assert.Equal(1, await store.AckAsync(Server, [lease], Ct));
        Assert.Equal(FailOutcome.Retried, await store.RetryAsync(job.Id, job.LeaseId, Server, TimeSpan.FromSeconds(3), JobEventKind.Retry, "m", "x", Ct));
        Assert.Equal(FailOutcome.Failed, await store.FailAsync(job.Id, job.LeaseId, Server, JobEventKind.Failed, "m", "x", Ct));
        Assert.Equal(FailOutcome.Cancelled, await store.ConfirmCancelAsync(job.Id, job.LeaseId, Server, "c", Ct));
        Assert.Equal(ReleaseOutcome.Released, await store.ReleaseAsync(job.Id, job.LeaseId, Server, true, null, Ct));
        var hb = Assert.Single(await store.HeartbeatAsync(Server, [lease], TimeSpan.FromMinutes(1), Ct));
        Assert.Equal(new HeartbeatLease(job.Id, job.LeaseId, false), hb);
        var registration = new ServerRegistration(Server, "s", "h", ["q"], 3);
        await store.RegisterServerAsync(registration, Ct);
        await store.RemoveServerAsync(Server, Ct);
        Assert.Equal(0, (await store.PromoteDueAsync(1000, Ct)).Promoted);
        Assert.Equal(0, await store.SweepExpiredLeasesAsync(1000, Ct));
        Assert.Empty(await store.PublishJobTypesAsync([], "me", Ct));
        Assert.Empty(await store.GetJobTypeNamesByQueuesAsync(["q"], Ct));

        Assert.Equal(["Dequeue", "Ack", "Retry", "Fail", "ConfirmCancel", "Release", "Heartbeat", "RegisterServer",
            "RemoveServer", "Promote", "Sweep", "PublishJobTypes", "JobTypeNames"], store.Calls.Select(c => c.Op));
        Assert.Equal(("q", 5), Assert.Single(store.CallsOf("Dequeue")).Detail);
        Assert.Equal([lease], (LeaseRef[])Assert.Single(store.CallsOf("Ack")).Detail!);
        var retry = Assert.Single(store.CallsOf("Retry"));
        Assert.Equal((job.Id, job.LeaseId), (retry.JobId!.Value, retry.LeaseId!.Value));
        Assert.Equal((TimeSpan.FromSeconds(3), JobEventKind.Retry, (string?)"m"), retry.Detail);
        Assert.Equal((JobEventKind.Failed, (string?)"m"), Assert.Single(store.CallsOf("Fail")).Detail);
        Assert.Equal((object?)true, Assert.Single(store.CallsOf("Release")).Detail);
        Assert.Equal([lease], (LeaseRef[])Assert.Single(store.CallsOf("Heartbeat")).Detail!);
        Assert.Same(registration, Assert.Single(store.CallsOf("RegisterServer")).Detail);
    }

    [Fact]
    public async Task Delegates_override_behaviour()
    {
        var store = new FakeServerStore
        {
            OnRelease = (_, _, ran, _) => Task.FromResult(ran ? ReleaseOutcome.Cancelled : ReleaseOutcome.LeaseLost),
            OnHeartbeat = (_, _) => Task.FromResult<IReadOnlyList<HeartbeatLease>>([]),
        };
        var job = Leased.Job();

        Assert.Equal(ReleaseOutcome.Cancelled, await store.ReleaseAsync(job.Id, job.LeaseId, Server, true, null, Ct));
        Assert.Equal(ReleaseOutcome.LeaseLost, await store.ReleaseAsync(job.Id, job.LeaseId, Server, false, null, Ct));
        Assert.Empty(await store.HeartbeatAsync(Server, [new LeaseRef(job.Id, job.LeaseId)], TimeSpan.FromMinutes(1), Ct));
    }

    [Fact]
    public void Leased_job_defaults_and_fresh_lease_per_call()
    {
        var id = Guid.CreateVersion7();
        var a = Leased.Job(id);
        var b = Leased.Job(id, type: "x.job", attempt: 2, maxAttempts: 5, retryDelays: [1, 2]);

        Assert.Equal(id, a.Id);
        Assert.Equal(id, b.Id);
        Assert.NotEqual(a.LeaseId, b.LeaseId);
        Assert.Equal(("test.job", "{}", 1, 3), (a.Type, a.ArgsJson, a.Attempt, a.MaxAttempts));
        Assert.Null(a.RetryDelaysSeconds);
        Assert.Equal(("x.job", 2, 5), (b.Type, b.Attempt, b.MaxAttempts));
        Assert.Equal([1, 2], b.RetryDelaysSeconds!);
    }
}
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ExecutionTests|FullyQualifiedName~ExecutionTableTests|FullyQualifiedName~FakeServerStoreTests"`
Expected: FAIL — ошибка компиляции: `IServerStore`, `ServerRegistration`, `Execution`, `ExecutionState`, `CancelReason`, `ExecutionTable`, `JobOutcome` не найдены.

- [x] **Step 4: Реализовать исходы и записи выполнений**

`src/PgBolt.Server/Runtime/JobOutcome.cs`:

```csharp
namespace PgBolt.Server.Runtime;

/// <summary>Исход выполнения, который отправляет CompletionSink (§5.2, §6.1).</summary>
internal abstract record JobOutcome
{
    /// <summary>Успех — пакетный ack (core §5.3).</summary>
    public sealed record Ack : JobOutcome;

    /// <summary>Повтор через <paramref name="Delay"/> (core §5.4); Kind — Retry или NoExecutor.</summary>
    public sealed record Retry(TimeSpan Delay, JobEventKind Kind, string? Message, string? Exception) : JobOutcome;

    /// <summary>Окончательный fail (core §5.4); Kind — Failed или NoExecutor.</summary>
    public sealed record Fail(JobEventKind Kind, string? Message, string? Exception) : JobOutcome;

    /// <summary>Подтверждение запрошенной отмены (core §5.4, state = 4).</summary>
    public sealed record ConfirmCancel(string? Message) : JobOutcome;

    /// <summary>Возврат без траты попытки (§7.1).</summary>
    public sealed record Release(bool Ran) : JobOutcome;
}
```

`src/PgBolt.Server/Runtime/Execution.cs`:

```csharp
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Состояние записи выполнения в таблице LeaseKeeper (§6.2). Removed терминально.</summary>
internal enum ExecutionState { Running, OutcomePending, OutcomeInFlight, Removed }

/// <summary>Причина отмены выполнения (§5.1); значима первая.</summary>
internal enum CancelReason { None, Shutdown, CancelRequested, HeartbeatLost, LeaseLost, Timeout }

/// <summary>
/// Запись таблицы выполнений (§6.2): одна взятая задача с её lease. Состояние, первая причина отмены и
/// validUntil меняются атомарно (CAS) и читаются без блокировок.
/// </summary>
internal sealed class Execution
{
    private readonly CancellationTokenSource _cts = new();
    private int _state;
    private int _firstReason;
    private long _validUntil;

    public Execution(LeasedJob job, string queue, long validUntil, ExecutionState initialState)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(queue);
        if (initialState is not (ExecutionState.Running or ExecutionState.OutcomePending))
            throw new ArgumentOutOfRangeException(nameof(initialState), initialState, "An execution starts as Running or OutcomePending.");
        Job = job;
        Queue = queue;
        Lease = new LeaseRef(job.Id, job.LeaseId);
        _validUntil = validUntil;
        _state = (int)initialState;
    }

    public LeasedJob Job { get; }

    public string Queue { get; }

    public LeaseRef Lease { get; }

    /// <summary>Вид слота, выданного <see cref="SlotAllocator.Take"/>.</summary>
    public bool FromReserved { get; set; }

    /// <summary>Отменяется <see cref="Cancel"/>; источник живёт столько же, сколько запись, и не освобождается (таймеров нет).</summary>
    public CancellationToken Token => _cts.Token;

    public CancelReason FirstReason => (CancelReason)Volatile.Read(ref _firstReason);

    /// <summary>Нижняя граница lease_until по монотонным часам (<see cref="MonoClock"/>).</summary>
    public long ValidUntil => Volatile.Read(ref _validUntil);

    public ExecutionState State => (ExecutionState)Volatile.Read(ref _state);

    /// <summary>Ставится до перехода в OutcomePending; видимость обеспечивает барьер CAS перехода.</summary>
    public JobOutcome? Outcome { get; set; }

    /// <summary>Момент первой попытки отправки исхода (бюджет LeaseDuration, §6.1); 0 — не начиналась.</summary>
    public long OutcomeFirstAttemptAt { get; set; }

    /// <summary>
    /// Запоминает причину, если она первая, и отменяет <see cref="Token"/> (всегда, идемпотентно).
    /// Колбэки токена запускаются асинхронно (<see cref="CancellationTokenSource.CancelAsync"/>):
    /// код задачи, продолжающийся после отмены, не выполняется на потоке heartbeat или таймера.
    /// </summary>
    /// <returns>true — причина стала первой.</returns>
    public bool Cancel(CancelReason reason)
    {
        if (reason == CancelReason.None)
            throw new ArgumentOutOfRangeException(nameof(reason), reason, "None is not a cancellation reason.");
        var first = Interlocked.CompareExchange(ref _firstReason, (int)reason, (int)CancelReason.None) == (int)CancelReason.None;
        _ = _cts.CancelAsync();
        return first;
    }

    /// <summary>validUntil = max(validUntil, candidate).</summary>
    public void ExtendValidUntil(long candidate)
    {
        var current = Volatile.Read(ref _validUntil);
        while (candidate > current)
        {
            var seen = Interlocked.CompareExchange(ref _validUntil, candidate, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>CAS from → to. Переходы вне §6.2 — <see cref="InvalidOperationException"/>.</summary>
    /// <returns>false — текущее состояние не равно <paramref name="from"/>; состояние не меняется.</returns>
    public bool TryTransition(ExecutionState from, ExecutionState to)
    {
        if (!IsAllowed(from, to))
            throw new InvalidOperationException($"Execution state transition {from} -> {to} is not allowed.");
        return Interlocked.CompareExchange(ref _state, (int)to, (int)from) == (int)from;
    }

    private static bool IsAllowed(ExecutionState from, ExecutionState to) => (from, to) switch
    {
        (ExecutionState.Running, ExecutionState.OutcomePending) => true,
        (ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight) => true,
        (ExecutionState.OutcomeInFlight, ExecutionState.OutcomePending) => true,
        (ExecutionState.Running, ExecutionState.Removed) => true,
        (ExecutionState.OutcomePending, ExecutionState.Removed) => true,
        (ExecutionState.OutcomeInFlight, ExecutionState.Removed) => true,
        _ => false,
    };
}
```

`src/PgBolt.Server/Runtime/ExecutionTable.cs`:

```csharp
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace PgBolt.Server.Runtime;

/// <summary>Таблица выполнений сервера по lease_id (§6.2): одна задача, взятая повторно, — две разные записи.</summary>
internal sealed class ExecutionTable
{
    private readonly ConcurrentDictionary<Guid, Execution> _byLease = new();

    public int Count => _byLease.Count;

    /// <exception cref="InvalidOperationException">Запись с таким lease_id уже есть.</exception>
    public void Add(Execution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        if (!_byLease.TryAdd(execution.Lease.LeaseId, execution))
            throw new InvalidOperationException($"Lease {execution.Lease.LeaseId} is already registered.");
    }

    /// <summary>Удаляет именно этот объект (не другую запись с тем же lease_id).</summary>
    public bool Remove(Execution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        return _byLease.TryRemove(new KeyValuePair<Guid, Execution>(execution.Lease.LeaseId, execution));
    }

    public bool TryGet(Guid leaseId, [MaybeNullWhen(false)] out Execution execution) =>
        _byLease.TryGetValue(leaseId, out execution);

    /// <summary>Копия записей, кроме уже <see cref="ExecutionState.Removed"/>.</summary>
    public IReadOnlyList<Execution> Snapshot()
    {
        var result = new List<Execution>(_byLease.Count);
        foreach (var (_, execution) in _byLease)
            if (execution.State != ExecutionState.Removed)
                result.Add(execution);
        return result;
    }
}
```

- [x] **Step 5: Реализовать доступ к БД**

`src/PgBolt.Server/Runtime/IServerStore.cs`:

```csharp
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Строка servers (§7.2).</summary>
internal sealed record ServerRegistration(Guid Id, string Name, string Host, string[] Queues, int Capacity);

/// <summary>Доступ runtime к БД; единственная реализация — <see cref="PgBoltServerStore"/>, в тестах — фейк.</summary>
internal interface IServerStore
{
    Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct);
    Task<int> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct);
    Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind, string? message, string? exception, CancellationToken ct);
    Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind, string? message, string? exception, CancellationToken ct);
    Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct);
    Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct);
    Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct);
    Task RegisterServerAsync(ServerRegistration server, CancellationToken ct);
    Task RemoveServerAsync(Guid serverId, CancellationToken ct);
    Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct);
    Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct);
    Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct);
    Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct);
}
```

`src/PgBolt.Server/Runtime/PgBoltServerStore.cs`:

```csharp
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Чистый проброс <see cref="IServerStore"/> в <see cref="PgBoltStore"/>.</summary>
internal sealed class PgBoltServerStore(PgBoltStore store) : IServerStore
{
    private readonly PgBoltStore _store = store ?? throw new ArgumentNullException(nameof(store));

    public Task<IReadOnlyList<LeasedJob>> DequeueAsync(string queue, int max, Guid serverId, TimeSpan lease, CancellationToken ct) =>
        _store.DequeueAsync(queue, max, serverId, lease, ct);

    public Task<int> AckAsync(Guid serverId, IReadOnlyList<LeaseRef> leases, CancellationToken ct) =>
        _store.AckAsync(serverId, leases, ct);

    public Task<FailOutcome> RetryAsync(Guid jobId, Guid leaseId, Guid serverId, TimeSpan delay, JobEventKind kind,
        string? message, string? exception, CancellationToken ct) =>
        _store.RetryAsync(jobId, leaseId, serverId, delay, kind, message, exception, ct);

    public Task<FailOutcome> FailAsync(Guid jobId, Guid leaseId, Guid serverId, JobEventKind kind,
        string? message, string? exception, CancellationToken ct) =>
        _store.FailAsync(jobId, leaseId, serverId, kind, message, exception, ct);

    public Task<FailOutcome> ConfirmCancelAsync(Guid jobId, Guid leaseId, Guid serverId, string? message, CancellationToken ct) =>
        _store.ConfirmCancelAsync(jobId, leaseId, serverId, message, ct);

    public Task<ReleaseOutcome> ReleaseAsync(Guid jobId, Guid leaseId, Guid serverId, bool ran, string? message, CancellationToken ct) =>
        _store.ReleaseAsync(jobId, leaseId, serverId, ran, message, ct);

    public Task<IReadOnlyList<HeartbeatLease>> HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct) =>
        _store.HeartbeatAsync(serverId, leases, lease, ct);

    public Task RegisterServerAsync(ServerRegistration server, CancellationToken ct) =>
        _store.RegisterServerAsync(server.Id, server.Name, server.Host, server.Queues, server.Capacity, ct);

    public Task RemoveServerAsync(Guid serverId, CancellationToken ct) =>
        _store.RemoveServerAsync(serverId, ct);

    public Task<PromoteResult> PromoteDueAsync(int batchSize, CancellationToken ct) =>
        _store.PromoteDueAsync(batchSize, ct);

    public Task<int> SweepExpiredLeasesAsync(int batchSize, CancellationToken ct) =>
        _store.SweepExpiredLeasesAsync(batchSize, ct);

    public Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct) =>
        _store.PublishJobTypesAsync(types, publishedBy, ct);

    public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct) =>
        _store.GetJobTypeNamesByQueuesAsync(queues, ct);
}
```

- [x] **Step 6: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ExecutionTests|FullyQualifiedName~ExecutionTableTests|FullyQualifiedName~FakeServerStoreTests"`
Expected: PASS — `Failed: 0, Passed: 34`. Тесты гонок (`Sink_and_heartbeat_race_on_outcome_pending_has_exactly_one_winner`, `Concurrent_cancels_record_exactly_one_first_reason`) используют реальные потоки и `Barrier`, но не реальное время.

- [x] **Step 7: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): execution table, job outcomes and server store abstraction"
```

---

### Task 12: `LeaseKeeper` — heartbeat по известным lease и срок lease каждого выполнения

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Files:**
- Create: `src/PgBolt.Server/Runtime/LeaseKeeper.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/LeaseKeeperTests.cs`

**Interfaces:**
- Consumes: `IServerStore.HeartbeatAsync`, `ExecutionTable`, `Execution`, `ExecutionState`, `CancelReason`, `JobOutcome.Release` (Task 11); `MonoClock` (Task 8); `PgBoltServerOptions.LeaseDuration`/`HeartbeatInterval` (Task 8); тестовые `FakeServerStore`, `Leased`, `Eventually` (Task 11), `TestLogger` (Task 9).
- Produces:
  - `internal sealed class LeaseKeeper` — `LeaseKeeper(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options, MonoClock clock, ILogger logger, Action onRecovered)`, `bool HeartbeatLost`, `Execution Register(LeasedJob job, string queue, long validUntil)`, `Execution RegisterForRelease(LeasedJob job, string queue, long validUntil)`, `void Start()`, `Task StopAsync()`, `internal Task HeartbeatOnceAsync(CancellationToken ct)`, `internal void CheckDeadlines()`.
  - Сверх контракта: `internal Func<double> Jitter { get; set; }` — источник U[0, 1) для разброса расписания (по умолчанию `Random.Shared.NextDouble`); тесты задают его для детерминированного шага.

Поведение (§6.2; обозначения: `H = HeartbeatInterval`, `L = LeaseDuration`):

1. **Регистрация.** `Register` → `new Execution(job, queue, validUntil, Running)`, `table.Add`, затем таймер сроков сдвигается на рубеж новой записи, если он раньше текущего (`ArmAtMost`). `RegisterForRelease` → `Execution` в `OutcomePending` с `Outcome = new JobOutcome.Release(false)` (исход ставится до `table.Add`); таймер сроков такие записи не отслеживает. `validUntil` передаёт вызывающий (`fetchStartedAt + L`, §4.3).
2. **Старт.** `Start()`: `startedAt = clock.Now`, `lastSuccess = startedAt` (до первого успеха «heartbeat потерян» отсчитывается от старта), создаётся таймер `clock.Time.CreateTimer(…, Infinite, Infinite)` и взводится по таблице, запускается heartbeat-цикл. Повторный `Start` — `InvalidOperationException`.
3. **Heartbeat-цикл.** `next = prevSentAt + H × (1 + 0.1 × (2U − 1))`, первый `prevSentAt = startedAt`; ожидание `Task.Delay(clock.Until(next), clock.Time, stop)`; затем `sentAt = clock.Now`, вызов, `prevSentAt = sentAt`. Расписание от `sentAt`, а не от завершения: медленный успешный вызов его не сдвигает (Review Focus п. 2). Так как расписание абсолютное, запоздавшее продолжение цикла (например, после большого `Advance` в тесте) просто вызывает heartbeat сразу — без накопления сдвига.
4. **Вызов (`HeartbeatCoreAsync(sentAt, ct)`, он же `HeartbeatOnceAsync`).** Снимок `table.Snapshot()` (все записи, кроме `Removed`) → `LeaseRef[]`; `store.HeartbeatAsync(serverId, leases, L, linked)`, где `linked` связан с `ct` и `new CancellationTokenSource(H / 2, clock.Time)`. Отмена по `ct` (остановка) — выход без лога; таймаут — `Warning` «timed out»; другая ошибка — `Warning` с исключением. Исключения наружу не выходят.
5. **Успех.** `wasLost = HeartbeatLost`; `lastSuccess = max(lastSuccess, sentAt)`. Для каждой строки ответа: `table.TryGet(leaseId)` → `ExtendValidUntil(sentAt + L)`; `CancelRequested` → `Cancel(CancelRequested)` (лог `Information` только при первой причине). Для каждой записи **снимка**, чей `lease_id` не вернулся: CAS `Running→Removed` → `Cancel(LeaseLost)`, `table.Remove`, `Warning`; иначе CAS `OutcomePending→Removed` → `table.Remove`, `Warning` «pending outcome dropped»; `OutcomeInFlight` и уже `Removed` не трогаются. Записи, зарегистрированные после снимка, не проверяются. Затем таймер перевзводится; если `wasLost && !HeartbeatLost` — лог и `onRecovered()` (исключение колбэка — `LogError`, не наружу). Уже отменённые выполнения не восстанавливаются: причина и токен остаются, продлевается только `validUntil`.
6. **`HeartbeatLost`** = `clock.Now ≥ clock.Add(lastSuccess, L − H)`.
7. **Проверка сроков (`CheckDeadlines`).** Рубеж записи — `clock.Add(ValidUntil, −H)`. Отслеживаются только записи `Running` с `FirstReason == None`: каждая, у которой `now ≥ рубеж`, получает `Cancel(HeartbeatLost)` (лог `Warning`). Выполнение, уже отменённое по любой причине (в том числе `HeartbeatLost`), не отслеживается: повторная отмена ничего не меняет (`Cancel` идемпотентен, первенство причины — у первой), а его прошедший рубеж иначе взводил бы таймер на ноль снова и снова. После проверки таймер перевзводится на ближайший рубеж отслеживаемых записей или выключается (`Timeout.InfiniteTimeSpan`).
8. **Таймер.** Один `ITimer` на сервер, `Change(due, Infinite)`; `due = clock.Until(рубеж)` (округление вверх — не раньше рубежа), не больше предела таймера ≈ 49 дней (раннее срабатывание безвредно). Взвод — под `Lock _timerLock` вместе с `_armedFor` (рубеж, на который он взведён): `ArmAtMost` только сдвигает раньше, `RearmDeadlineTimer` пересчитывает по снимку таблицы (регистрация без блокировки не теряется: либо снимок её видит, либо её `ArmAtMost` выполнится после). `FakeTimeProvider` исполняет таймер с нулевой задержкой синхронно внутри `Change`; `System.Threading.Lock` реентерабелен, а `_armedFor` пишется до `Change`, поэтому вложенный `CheckDeadlines → RearmDeadlineTimer` корректен. Таймер не зависит от heartbeat-цикла: срабатывает и при висящем вызове.
9. **`StopAsync`.** `await stop.CancelAsync()` (отменяет и текущий вызов через связанный токен), `await loop`, затем `Dispose` таймера под блокировкой. Регистрации после остановки допустимы (таймер уже не взводится). До `Start` — no-op.

- [x] **Step 1: Написать падающие тесты**

Приёмы (все — на `FakeTimeProvider`, частота `GetTimestamp` у него — тики `TimeSpan`, поэтому «рубеж − 1 тик» точен):
- `Jitter = 0.5` → шаг расписания ровно `H`; `Math.BitDecrement(1.0)` → максимальный шаг `1.1 H` (`TimeSpan × double` округляет до тика: 10 с × 1.1 = 11 с).
- Вызов heartbeat управляется `TaskCompletionSource` (`RunContinuationsAsynchronously`) в `OnHeartbeat`: тест держит вызов «в полёте», регистрирует выполнения, затем завершает его.
- «Висящий» вызов, игнорирующий токен, перед `StopAsync` завершается тестом (иначе `StopAsync` ждал бы его вечно — в реальном store токен соблюдается).
- Асинхронные последствия ждутся через `Eventually.True`, синхронные (таймеры) проверяются сразу после `Advance`.

`tests/PgBolt.Server.Tests/Unit/LeaseKeeperTests.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class LeaseKeeperTests
{
    private static readonly TimeSpan H = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan L = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);
    private static readonly Guid ServerId = Guid.CreateVersion7();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly FakeTimeProvider _time = new();
    private readonly MonoClock _clock;
    private readonly FakeServerStore _store = new();
    private readonly ExecutionTable _table = new();
    private readonly TestLogger _logger = new();
    private int _recovered;

    public LeaseKeeperTests() => _clock = new MonoClock(_time);

    /// <summary>Jitter = 0.5 → шаг расписания ровно HeartbeatInterval.</summary>
    private LeaseKeeper Create(TimeSpan? lease = null, TimeSpan? interval = null, double jitter = 0.5) =>
        new(ServerId, _store, _table, new PgBoltServerOptions { LeaseDuration = lease ?? L, HeartbeatInterval = interval ?? H },
            _clock, _logger, () => Interlocked.Increment(ref _recovered))
        {
            Jitter = () => jitter,
        };

    private long FromNow(TimeSpan duration) => _clock.Add(_clock.Now, duration);

    private void AdvanceTo(long timestamp) => _time.Advance(_clock.Until(timestamp));

    private int HeartbeatCalls => _store.CallsOf("Heartbeat").Count();

    private static Task<IReadOnlyList<HeartbeatLease>> DbDown() =>
        Task.FromException<IReadOnlyList<HeartbeatLease>>(new TimeoutException("db down"));

    private static TaskCompletionSource<IReadOnlyList<HeartbeatLease>> NewCall() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static IReadOnlyList<HeartbeatLease> Rows(params (Execution Execution, bool CancelRequested)[] rows) =>
        [.. rows.Select(r => new HeartbeatLease(r.Execution.Lease.JobId, r.Execution.Lease.LeaseId, r.CancelRequested))];

    private void Respond(IReadOnlyList<HeartbeatLease> rows) =>
        _store.OnHeartbeat = (_, _) => Task.FromResult(rows);

    // ---- регистрация и снимок --------------------------------------------------------------

    [Fact]
    public void Register_adds_running_execution_with_given_valid_until()
    {
        var keeper = Create();
        var job = Leased.Job();
        var validUntil = FromNow(L);

        var e = keeper.Register(job, "q", validUntil);

        Assert.Equal(ExecutionState.Running, e.State);
        Assert.Equal(validUntil, e.ValidUntil);
        Assert.Equal("q", e.Queue);
        Assert.True(_table.TryGet(job.LeaseId, out var found));
        Assert.Same(e, found);
    }

    [Fact]
    public void Register_for_release_adds_pending_release_outcome()
    {
        var keeper = Create();

        var e = keeper.RegisterForRelease(Leased.Job(), "q", FromNow(L));

        Assert.Equal(ExecutionState.OutcomePending, e.State);
        Assert.Equal(new JobOutcome.Release(false), e.Outcome);
        Assert.Equal(1, _table.Count);
    }

    [Fact]
    public async Task Heartbeat_sends_only_leases_of_live_table_entries()
    {
        var keeper = Create();
        var running = keeper.Register(Leased.Job(), "q", FromNow(L));
        var release = keeper.RegisterForRelease(Leased.Job(), "q", FromNow(L));
        var abandoned = keeper.Register(Leased.Job(), "q", FromNow(L));
        Assert.True(abandoned.TryTransition(ExecutionState.Running, ExecutionState.Removed));   // ещё в таблице, но Removed

        await keeper.HeartbeatOnceAsync(Ct);

        var sent = (LeaseRef[])Assert.Single(_store.CallsOf("Heartbeat")).Detail!;
        Assert.Equal(new[] { running.Lease, release.Lease }.OrderBy(l => l.LeaseId), sent.OrderBy(l => l.LeaseId));
    }

    [Fact]
    public async Task Execution_registered_after_snapshot_is_not_treated_as_lost()
    {
        var keeper = Create();
        var before = keeper.Register(Leased.Job(), "q", FromNow(L));
        var call = NewCall();
        _store.OnHeartbeat = (_, _) => call.Task;

        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        var afterValidUntil = FromNow(L);
        var after = keeper.Register(Leased.Job(), "q", afterValidUntil);   // dequeue закоммичен после снимка
        call.SetResult(Rows((before, false)));
        await heartbeat;

        Assert.Equal([before.Lease], (LeaseRef[])Assert.Single(_store.CallsOf("Heartbeat")).Detail!);
        Assert.Equal(ExecutionState.Running, after.State);
        Assert.Equal(CancelReason.None, after.FirstReason);
        Assert.False(after.Token.IsCancellationRequested);
        Assert.Equal(afterValidUntil, after.ValidUntil);
        Assert.True(_table.TryGet(after.Lease.LeaseId, out _));
    }

    // ---- ответ heartbeat ------------------------------------------------------------------

    [Fact]
    public async Task Missing_running_lease_is_removed_and_cancelled_as_lease_lost()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(ExecutionState.Removed, e.State);
        Assert.Equal(CancelReason.LeaseLost, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
        Assert.False(_table.TryGet(e.Lease.LeaseId, out _));
        Assert.Contains(_logger.At(LogLevel.Warning), l => l.Message.Contains(e.Lease.LeaseId.ToString(), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Missing_pending_outcome_is_dropped()
    {
        var keeper = Create();
        var completed = keeper.Register(Leased.Job(), "q", FromNow(L));
        completed.Outcome = new JobOutcome.Ack();
        Assert.True(completed.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
        var notStarted = keeper.RegisterForRelease(Leased.Job(), "q", FromNow(L));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);

        foreach (var e in new[] { completed, notStarted })
        {
            Assert.Equal(ExecutionState.Removed, e.State);
            Assert.Equal(CancelReason.None, e.FirstReason);
            Assert.False(_table.TryGet(e.Lease.LeaseId, out _));
        }
        Assert.Equal(0, _table.Count);
    }

    [Fact]
    public async Task Missing_lease_with_outcome_in_flight_is_left_to_the_sink()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        e.Outcome = new JobOutcome.Ack();
        Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
        Assert.True(e.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight));
        Respond([]);

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(ExecutionState.OutcomeInFlight, e.State);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.True(_table.TryGet(e.Lease.LeaseId, out _));
    }

    [Fact]
    public async Task Lease_already_removed_by_the_sink_is_not_reported_again()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var call = NewCall();
        _store.OnHeartbeat = (_, _) => call.Task;

        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        e.Outcome = new JobOutcome.Ack();                    // ack того же сервера удалил строку раньше heartbeat
        Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
        Assert.True(e.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight));
        Assert.True(e.TryTransition(ExecutionState.OutcomeInFlight, ExecutionState.Removed));
        Assert.True(_table.Remove(e));
        call.SetResult([]);
        await heartbeat;

        Assert.Equal(ExecutionState.Removed, e.State);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.Empty(_logger.At(LogLevel.Warning));
    }

    [Fact]
    public async Task Cancel_request_cancels_the_execution_once()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        Respond(Rows((e, true)));

        await keeper.HeartbeatOnceAsync(Ct);
        await keeper.HeartbeatOnceAsync(Ct);

        Assert.Equal(CancelReason.CancelRequested, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
        Assert.Equal(ExecutionState.Running, e.State);          // исход задачи сдаст runner
        Assert.True(_table.TryGet(e.Lease.LeaseId, out _));
        Assert.Single(_logger.At(LogLevel.Information));
    }

    [Fact]
    public async Task Valid_until_grows_to_sent_at_plus_lease_only_for_returned_leases()
    {
        var keeper = Create();
        var fetchStartedAt = _clock.Now;
        _time.Advance(TimeSpan.FromSeconds(2));                         // dequeue длился 2 с
        var renewed = keeper.Register(Leased.Job(), "q", _clock.Add(fetchStartedAt, L));
        var farAhead = keeper.Register(Leased.Job(), "q", FromNow(L * 2));
        Assert.Equal(_clock.Add(fetchStartedAt, L), renewed.ValidUntil);

        _time.Advance(TimeSpan.FromSeconds(10));
        var sentAt = _clock.Now;
        var call = NewCall();
        _store.OnHeartbeat = (_, _) => call.Task;
        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        var lateValidUntil = _clock.Add(sentAt, L - TimeSpan.FromSeconds(1));
        var late = keeper.Register(Leased.Job(), "q", lateValidUntil); // после снимка: в ответе его нет
        _time.Advance(TimeSpan.FromSeconds(1));                         // ответ пришёл через 1 с
        call.SetResult(Rows((renewed, false), (farAhead, false)));
        await heartbeat;

        Assert.Equal(_clock.Add(sentAt, L), renewed.ValidUntil);        // от sentAt, не от ответа
        Assert.Equal(_clock.Add(fetchStartedAt, L * 2 + TimeSpan.FromSeconds(2)), farAhead.ValidUntil);   // не уменьшается
        Assert.Equal(lateValidUntil, late.ValidUntil);
    }

    // ---- таймаут вызова и цикл ------------------------------------------------------------

    [Fact]
    public async Task Heartbeat_call_is_aborted_after_half_the_interval()
    {
        var keeper = Create();
        CancellationToken seen = default;
        _store.OnHeartbeat = (_, ct) =>
        {
            seen = ct;
            var call = NewCall();
            ct.Register(() => call.TrySetCanceled(ct));
            return call.Task;
        };

        var heartbeat = keeper.HeartbeatOnceAsync(Ct);
        _time.Advance(H / 2 - Tick);
        Assert.False(seen.IsCancellationRequested);
        _time.Advance(Tick);
        Assert.True(seen.IsCancellationRequested);
        await heartbeat;                                                 // не бросает

        Assert.Contains(_logger.At(LogLevel.Warning), l => l.Message.Contains("timed out", StringComparison.Ordinal));
        Assert.False(keeper.HeartbeatLost);
    }

    [Fact]
    public async Task Failed_heartbeat_is_logged_and_does_not_throw()
    {
        var keeper = Create();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var validUntil = e.ValidUntil;
        _store.OnHeartbeat = (_, _) => DbDown();

        await keeper.HeartbeatOnceAsync(Ct);

        Assert.IsType<TimeoutException>(Assert.Single(_logger.At(LogLevel.Warning)).Exception);
        Assert.Equal(ExecutionState.Running, e.State);
        Assert.Equal(validUntil, e.ValidUntil);
    }

    [Fact]
    public async Task Loop_calls_heartbeat_every_interval_and_stop_ends_it()
    {
        var keeper = Create();
        keeper.Start();

        _time.Advance(H - Tick);
        Assert.Equal(0, HeartbeatCalls);
        _time.Advance(Tick);
        await Eventually.True(() => HeartbeatCalls == 1, "first heartbeat one interval after start");
        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 2, "second heartbeat one interval later");

        await keeper.StopAsync();
        _time.Advance(H * 3);
        Assert.Equal(2, HeartbeatCalls);
    }

    [Fact]
    public async Task Stop_cancels_the_call_in_flight_without_warning()
    {
        var keeper = Create();
        CancellationToken seen = default;
        _store.OnHeartbeat = (_, ct) =>
        {
            seen = ct;
            var call = NewCall();
            ct.Register(() => call.TrySetCanceled(ct));
            return call.Task;
        };
        keeper.Start();
        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "heartbeat call started");

        await keeper.StopAsync();

        Assert.True(seen.IsCancellationRequested);
        Assert.Empty(_logger.At(LogLevel.Warning));
    }

    [Fact]
    public void Start_twice_throws()
    {
        var keeper = Create();
        keeper.Start();
        Assert.Throws<InvalidOperationException>(keeper.Start);
    }

    // ---- проверка сроков lease -----------------------------------------------------------

    [Fact]
    public async Task Deadline_check_cancels_exactly_at_valid_until_minus_interval()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var deadline = _clock.Add(e.ValidUntil, -H);

        _time.Advance(_clock.Until(deadline) - Tick);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.False(e.Token.IsCancellationRequested);

        _time.Advance(Tick);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
        Assert.Equal(ExecutionState.Running, e.State);                  // исход не отбрасывается
        Assert.True(_table.TryGet(e.Lease.LeaseId, out _));

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Deadline_check_fires_while_heartbeat_call_hangs()
    {
        var hung = NewCall();
        _store.OnHeartbeat = (_, _) => hung.Task;                      // игнорирует токен — вызов висит
        var keeper = Create();
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        var deadline = _clock.Add(e.ValidUntil, -H);

        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "heartbeat call started and hangs");
        _time.Advance(_clock.Until(deadline) - Tick);
        Assert.Equal(CancelReason.None, e.FirstReason);
        _time.Advance(Tick);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);
        Assert.Equal(1, HeartbeatCalls);

        hung.SetException(new TimeoutException("released by test"));
        await keeper.StopAsync();
    }

    [Fact]
    public async Task Each_execution_is_cancelled_at_its_own_deadline()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();
        var early = keeper.Register(Leased.Job(), "q", FromNow(L));            // рубеж: +45 с
        _time.Advance(TimeSpan.FromSeconds(10));
        var earlier = keeper.Register(Leased.Job(), "q", FromNow(TimeSpan.FromSeconds(30)));   // рубеж: +25 с — таймер сдвигается раньше

        AdvanceTo(_clock.Add(earlier.ValidUntil, -H));
        Assert.Equal(CancelReason.HeartbeatLost, earlier.FirstReason);
        Assert.Equal(CancelReason.None, early.FirstReason);

        _time.Advance(_clock.Until(_clock.Add(early.ValidUntil, -H)) - Tick);
        Assert.Equal(CancelReason.None, early.FirstReason);
        _time.Advance(Tick);                                             // таймер перевзведён после срабатывания
        Assert.Equal(CancelReason.HeartbeatLost, early.FirstReason);

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Execution_registered_past_its_deadline_is_cancelled_at_once()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();

        var e = keeper.Register(Leased.Job(), "q", FromNow(H - TimeSpan.FromSeconds(1)));

        await Eventually.True(() => e.FirstReason == CancelReason.HeartbeatLost, "deadline already passed at registration");
        await keeper.StopAsync();
    }

    [Fact]
    public async Task Already_cancelled_execution_does_not_get_heartbeat_lost_and_timer_does_not_spin()
    {
        _store.OnHeartbeat = (_, _) => DbDown();
        var keeper = Create();
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));
        e.Cancel(CancelReason.Shutdown);

        _time.Advance(L);                                                // рубеж пройден

        Assert.Equal(CancelReason.Shutdown, e.FirstReason);
        Assert.DoesNotContain(_logger.At(LogLevel.Warning), l => l.Message.Contains("not confirmed", StringComparison.Ordinal));
        await keeper.StopAsync();
    }

    // ---- сценарии сбоя (§6.2) ---------------------------------------------------------------

    [Fact]
    public async Task Job_registered_after_snapshot_of_the_last_success_is_cancelled_by_its_own_valid_until()
    {
        // H = 15 с, L = 60 с. Старт в T, heartbeat в T + 15 с. Dequeue начат в t0 = T + 1 с и длился ≈ H:
        // задача зарегистрирована после снимка успешного heartbeat (sentAt = t0 + 14 с), дальше БД недоступна.
        var keeper = Create();
        var start = _clock.Now;
        var t0 = _clock.Add(start, TimeSpan.FromSeconds(1));
        var first = NewCall();
        var calls = 0;
        _store.OnHeartbeat = (_, _) => Interlocked.Increment(ref calls) == 1 ? first.Task : DbDown();
        keeper.Start();

        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "heartbeat snapshot taken at T + 15 s");
        var sentAt = _clock.Now;
        var e = keeper.Register(Leased.Job(), "q", _clock.Add(t0, L));
        first.SetResult([]);                                             // успех: lastSuccess = T + 15 с

        var deadline = _clock.Add(t0, L - H);                            // t0 + L − H = T + 46 с
        Assert.True(deadline < _clock.Add(sentAt, L - H));               // серверный watchdog сработал бы только в T + 60 с
        _time.Advance(_clock.Until(deadline) - Tick);
        Assert.Equal(CancelReason.None, e.FirstReason);
        _time.Advance(Tick);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Heartbeat_lost_starts_at_last_success_plus_lease_minus_interval_and_lifts_on_success()
    {
        var failing = 1;
        _store.OnHeartbeat = (leases, _) =>
            Volatile.Read(ref failing) == 1 ? DbDown() : Task.FromResult(FakeServerStore.AllAlive(leases));
        var keeper = Create();
        var start = _clock.Now;
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(L));

        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 1, "call at T + 15 s");
        _time.Advance(H);
        await Eventually.True(() => HeartbeatCalls == 2, "call at T + 30 s");
        _time.Advance(H - Tick);
        Assert.False(keeper.HeartbeatLost);
        Assert.Equal(CancelReason.None, e.FirstReason);

        _time.Advance(Tick);                                             // T + 45 с = старт + L − H
        Assert.True(keeper.HeartbeatLost);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);         // рубеж выполнения совпал со входом в состояние
        await Eventually.True(() => HeartbeatCalls == 3, "call at T + 45 s fails too");
        Assert.Equal(0, Volatile.Read(ref _recovered));

        Volatile.Write(ref failing, 0);
        _time.Advance(H);                                                // T + 60 с
        var sentAt = _clock.Now;
        await Eventually.True(() => Volatile.Read(ref _recovered) == 1, "first successful heartbeat lifts HeartbeatLost");
        Assert.False(keeper.HeartbeatLost);
        Assert.Equal(CancelReason.HeartbeatLost, e.FirstReason);         // отменённое не восстанавливается
        Assert.True(e.Token.IsCancellationRequested);
        Assert.Equal(_clock.Add(sentAt, L), e.ValidUntil);               // lease из снимка продлевается снова

        _time.Advance(H);
        var nextSentAt = _clock.Now;
        await Eventually.True(() => e.ValidUntil == _clock.Add(nextSentAt, L), "next heartbeat processed");
        Assert.Equal(1, Volatile.Read(ref _recovered));                  // onRecovered — один раз на восстановление
        Assert.True(start < sentAt);

        await keeper.StopAsync();
    }

    [Fact]
    public async Task Slow_successful_heartbeat_at_minimum_lease_does_not_raise_heartbeat_lost()
    {
        // L = 3H, максимальный разброс расписания (1.1H) и вызовы длиной почти H/2: успех приходит
        // не позже sentAt + 1.6H, раньше рубежа sentAt + 2H. Расписание — от sentAt, не от завершения.
        var h = TimeSpan.FromSeconds(10);
        var l = h * 3;
        var step = h * 1.1;
        var slow = h / 2 - Tick;
        var pending = new ConcurrentQueue<(TaskCompletionSource<IReadOnlyList<HeartbeatLease>> Call, LeaseRef[] Leases)>();
        _store.OnHeartbeat = (leases, _) =>
        {
            var call = NewCall();
            pending.Enqueue((call, leases.ToArray()));
            return call.Task;
        };
        var keeper = Create(l, h, jitter: Math.BitDecrement(1.0));
        keeper.Start();
        var e = keeper.Register(Leased.Job(), "q", FromNow(l));

        for (var i = 1; i <= 5; i++)
        {
            _time.Advance(i == 1 ? step : step - slow);
            await Eventually.True(() => pending.Count == 1, $"call {i} starts {step} after the previous sentAt");
            var sentAt = _clock.Now;
            _time.Advance(slow);

            Assert.False(keeper.HeartbeatLost);
            Assert.Equal(CancelReason.None, e.FirstReason);
            Assert.True(pending.TryDequeue(out var current));
            current.Call.SetResult(FakeServerStore.AllAlive(current.Leases));
            await Eventually.True(() => e.ValidUntil == _clock.Add(sentAt, l), $"call {i} processed");
        }

        Assert.Equal(0, Volatile.Read(ref _recovered));
        await keeper.StopAsync();
    }
}
```

- [x] **Step 2: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~LeaseKeeperTests"`
Expected: FAIL — ошибка компиляции: `LeaseKeeper` не найден.

- [x] **Step 3: Реализовать**

`src/PgBolt.Server/Runtime/LeaseKeeper.cs`:

```csharp
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Таблица выполнений сервера, heartbeat по известным lease и проверка срока lease каждого выполнения (§6.2).
/// </summary>
/// <remarks>
/// Heartbeat-цикл: вызов раз в <c>HeartbeatInterval × (1 ± 0.1)</c>, расписание — от sentAt предыдущего вызова;
/// таймаут вызова — <c>HeartbeatInterval / 2</c>, поэтому вызовы не перекрываются.
/// Таймер сроков взведён на ближайший рубеж <c>validUntil − HeartbeatInterval</c> среди неотменённых записей
/// <see cref="ExecutionState.Running"/> и не зависит от того, завершился ли текущий вызов heartbeat.
/// </remarks>
internal sealed class LeaseKeeper
{
    private readonly Guid _serverId;
    private readonly IServerStore _store;
    private readonly ExecutionTable _table;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly Action _onRecovered;
    private readonly TimeSpan _lease;
    private readonly TimeSpan _interval;
    private readonly CancellationTokenSource _stop = new();
    private readonly Lock _timerLock = new();
    private ITimer? _deadlineTimer;          // под _timerLock
    private long _armedFor = long.MaxValue;  // под _timerLock: рубеж, на который взведён таймер
    private long _lastSuccess;
    private Task _loop = Task.CompletedTask;
    private int _started;

    public LeaseKeeper(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options,
                       MonoClock clock, ILogger logger, Action onRecovered)
    {
        ArgumentNullException.ThrowIfNull(options);
        _serverId = serverId;
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _onRecovered = onRecovered ?? throw new ArgumentNullException(nameof(onRecovered));
        _lease = options.LeaseDuration;
        _interval = options.HeartbeatInterval;
        _lastSuccess = clock.Now;
    }

    /// <summary>Источник U[0, 1) для разброса расписания ±10 %; заменяется в тестах.</summary>
    internal Func<double> Jitter { get; set; } = static () => Random.Shared.NextDouble();

    /// <summary>now ≥ lastSuccess + LeaseDuration − HeartbeatInterval; до первого успеха отсчёт — от <see cref="Start"/>.</summary>
    public bool HeartbeatLost => _clock.Now >= _clock.Add(Volatile.Read(ref _lastSuccess), _lease - _interval);

    /// <summary>Регистрирует взятую задачу как <see cref="ExecutionState.Running"/> (до запуска JobRunner).</summary>
    /// <param name="validUntil">fetchStartedAt + LeaseDuration (§4.3).</param>
    public Execution Register(LeasedJob job, string queue, long validUntil)
    {
        var execution = new Execution(job, queue, validUntil, ExecutionState.Running);
        _table.Add(execution);
        ArmAtMost(DeadlineOf(execution));
        return execution;
    }

    /// <summary>Регистрирует задачу, которая не будет запускаться: сразу OutcomePending с исходом release, ran = false.</summary>
    public Execution RegisterForRelease(LeasedJob job, string queue, long validUntil)
    {
        var execution = new Execution(job, queue, validUntil, ExecutionState.OutcomePending)
        {
            Outcome = new JobOutcome.Release(false),
        };
        _table.Add(execution);
        return execution;
    }

    public void Start()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("LeaseKeeper is already started.");
        var startedAt = _clock.Now;
        Volatile.Write(ref _lastSuccess, startedAt);
        lock (_timerLock)
            _deadlineTimer = _clock.Time.CreateTimer(static state => ((LeaseKeeper)state!).OnDeadlineTimer(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        RearmDeadlineTimer();
        _loop = RunHeartbeatLoopAsync(startedAt, _stop.Token);
    }

    /// <summary>Останавливает цикл (текущий вызов heartbeat отменяется и дожидается) и таймер сроков.</summary>
    public async Task StopAsync()
    {
        if (Volatile.Read(ref _started) == 0)
            return;
        await _stop.CancelAsync();
        await _loop;
        lock (_timerLock)
        {
            _deadlineTimer?.Dispose();
            _deadlineTimer = null;
            _armedFor = long.MaxValue;
        }
    }

    /// <summary>Один вызов heartbeat (§6.2 п. 1–6); не бросает.</summary>
    internal Task HeartbeatOnceAsync(CancellationToken ct) => HeartbeatCoreAsync(_clock.Now, ct);

    /// <summary>
    /// Отменяет с причиной HeartbeatLost каждое неотменённое выполнение Running, достигшее рубежа
    /// validUntil − HeartbeatInterval, и перевзводит таймер. Её же вызывает таймер.
    /// </summary>
    internal void CheckDeadlines()
    {
        var now = _clock.Now;
        foreach (var execution in _table.Snapshot())
        {
            if (!IsWatched(execution) || now < DeadlineOf(execution))
                continue;
            if (execution.Cancel(CancelReason.HeartbeatLost))
                _logger.LogWarning(
                    "PgBolt: lease renewal of job {JobId} (lease {LeaseId}) is not confirmed; cancelling the execution.",
                    execution.Job.Id, execution.Lease.LeaseId);
        }
        RearmDeadlineTimer();
    }

    private async Task RunHeartbeatLoopAsync(long startedAt, CancellationToken stop)
    {
        var previousSentAt = startedAt;
        while (true)
        {
            var next = _clock.Add(previousSentAt, _interval * (1 + 0.1 * (2 * Jitter() - 1)));
            try
            {
                await Task.Delay(_clock.Until(next), _clock.Time, stop);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }

            previousSentAt = _clock.Now;
            await HeartbeatCoreAsync(previousSentAt, stop);
            if (stop.IsCancellationRequested)
                return;
        }
    }

    private async Task HeartbeatCoreAsync(long sentAt, CancellationToken ct)
    {
        var snapshot = _table.Snapshot();
        var leases = new LeaseRef[snapshot.Count];
        for (var i = 0; i < leases.Length; i++)
            leases[i] = snapshot[i].Lease;

        IReadOnlyList<HeartbeatLease> alive;
        using (var timeout = new CancellationTokenSource(_interval / 2, _clock.Time))
        using (var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token))
        {
            try
            {
                alive = await _store.HeartbeatAsync(_serverId, leases, _lease, linked.Token);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e) when (timeout.IsCancellationRequested)
            {
                _logger.LogWarning(e, "PgBolt: heartbeat of server {ServerId} timed out after {Timeout}; {Count} leases not renewed.",
                    _serverId, _interval / 2, leases.Length);
                return;
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "PgBolt: heartbeat of server {ServerId} failed; {Count} leases not renewed.",
                    _serverId, leases.Length);
                return;
            }
        }

        var wasLost = HeartbeatLost;
        AdvanceLastSuccess(sentAt);

        // п. 3–4: now() БД не раньше sentAt, значит lease_until ≥ sentAt + LeaseDuration.
        var renewedUntil = _clock.Add(sentAt, _lease);
        var returned = new HashSet<Guid>(alive.Count);
        foreach (var row in alive)
        {
            returned.Add(row.LeaseId);
            if (!_table.TryGet(row.LeaseId, out var execution))
                continue;
            execution.ExtendValidUntil(renewedUntil);
            if (row.CancelRequested && execution.Cancel(CancelReason.CancelRequested))
                _logger.LogInformation("PgBolt: cancellation of job {JobId} requested; cancelling the execution.", execution.Job.Id);
        }

        // п. 5: lease из снимка, которого нет в ответе, подтверждённо потерян. Зарегистрированные после снимка не проверяются.
        foreach (var execution in snapshot)
        {
            if (returned.Contains(execution.Lease.LeaseId))
                continue;
            if (execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
            {
                execution.Cancel(CancelReason.LeaseLost);
                _table.Remove(execution);
                _logger.LogWarning("PgBolt: lease {LeaseId} of job {JobId} is lost; the execution is cancelled and its outcome will be dropped.",
                    execution.Lease.LeaseId, execution.Job.Id);
            }
            else if (execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
            {
                _table.Remove(execution);
                _logger.LogWarning("PgBolt: lease {LeaseId} of job {JobId} is lost; pending outcome {Outcome} is dropped.",
                    execution.Lease.LeaseId, execution.Job.Id, execution.Outcome);
            }
            // OutcomeInFlight не трогается: авторитетен результат statement'а (§6.1).
        }

        RearmDeadlineTimer();

        if (wasLost && !HeartbeatLost)
        {
            _logger.LogInformation("PgBolt: heartbeat of server {ServerId} recovered.", _serverId);
            try
            {
                _onRecovered();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "PgBolt: heartbeat recovery callback of server {ServerId} failed.", _serverId);
            }
        }
    }

    private void AdvanceLastSuccess(long sentAt)
    {
        var current = Volatile.Read(ref _lastSuccess);
        while (sentAt > current)
        {
            var seen = Interlocked.CompareExchange(ref _lastSuccess, sentAt, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>
    /// Таймер следит только за Running без причины отмены: выполнение, уже отменённое по любой причине
    /// (в том числе HeartbeatLost), повторная отмена не меняет, а его прошедший рубеж зациклил бы таймер.
    /// </summary>
    private static bool IsWatched(Execution execution) =>
        execution.State == ExecutionState.Running && execution.FirstReason == CancelReason.None;

    private long DeadlineOf(Execution execution) => _clock.Add(execution.ValidUntil, -_interval);

    /// <summary>Сдвигает таймер на более ранний рубеж (регистрация). Change может сработать синхронно — Lock реентерабелен.</summary>
    private void ArmAtMost(long deadline)
    {
        lock (_timerLock)
        {
            if (_deadlineTimer is null || deadline >= _armedFor)
                return;
            _armedFor = deadline;
            _deadlineTimer.Change(DueIn(deadline), Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Взводит таймер на ближайший рубеж среди отслеживаемых записей (или выключает).</summary>
    private void RearmDeadlineTimer()
    {
        lock (_timerLock)
        {
            if (_deadlineTimer is null)
                return;
            var next = long.MaxValue;
            foreach (var execution in _table.Snapshot())
                if (IsWatched(execution))
                    next = Math.Min(next, DeadlineOf(execution));
            _armedFor = next;
            _deadlineTimer.Change(next == long.MaxValue ? Timeout.InfiniteTimeSpan : DueIn(next), Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Задержка до рубежа, не больше предела таймера (≈ 49 дней); раннее срабатывание безвредно — таймер перевзведётся.</summary>
    private TimeSpan DueIn(long deadline)
    {
        var due = _clock.Until(deadline);
        return due > MaxTimerDue ? MaxTimerDue : due;
    }

    private static readonly TimeSpan MaxTimerDue = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private void OnDeadlineTimer()
    {
        try
        {
            CheckDeadlines();
        }
        catch (Exception e)
        {
            _logger.LogError(e, "PgBolt: lease deadline check of server {ServerId} failed.", _serverId);
        }
    }
}
```

- [x] **Step 4: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~LeaseKeeperTests"`
Expected: PASS — `Failed: 0, Passed: 23`, длительность — доли секунды (реальное ожидание только в `Eventually`).

Проверка стабильности (гонки продолжений после `Advance`):

Run: `for i in $(seq 1 10); do dotnet test tests/PgBolt.Server.Tests --no-build --filter "FullyQualifiedName~LeaseKeeperTests|FullyQualifiedName~ExecutionTests" || break; done`
Expected: все 10 прогонов PASS.

Контроль, что ключевые тесты ловят регрессии (внести правку, прогнать `LeaseKeeperTests`, убедиться в падении и вернуть код Step 3 — файл ещё не закоммичен, поэтому откат вручную):
- в `RunHeartbeatLoopAsync` после `await HeartbeatCoreAsync(previousSentAt, stop);` добавить `previousSentAt = _clock.Now;` (расписание от завершения) → падает `Slow_successful_heartbeat_at_minimum_lease_does_not_raise_heartbeat_lost`;
- в `Register` убрать `ArmAtMost(DeadlineOf(execution));` → падают `Deadline_check_cancels_exactly_at_valid_until_minus_interval`, `Deadline_check_fires_while_heartbeat_call_hangs`, `Each_execution_is_cancelled_at_its_own_deadline`, `Execution_registered_past_its_deadline_is_cancelled_at_once`, `Heartbeat_lost_starts_at_last_success_plus_lease_minus_interval_and_lifts_on_success`.

- [x] **Step 5: Полный прогон проекта и сборка решения**

Run: `dotnet test tests/PgBolt.Server.Tests`
Expected: PASS — `Failed: 0, Passed: 169` (все unit-тесты Tasks 8–12: 56 + 39 + 17 + 34 + 23).

Run: `dotnet build PgBolt.slnx`
Expected: без предупреждений и ошибок.

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Server tests/PgBolt.Server.Tests
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): lease keeper with known-lease heartbeat and per-execution lease deadlines"
```

---

### Task 13: `CompletionSink` — отправка исходов

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Спека:** §6.1, §6.2 (переходы состояния), §6.4 п. 4, §10 (`CompletionSink`).

**Files:**
- Create: `src/PgBolt.Server/Runtime/CompletionSink.cs`
- Create: `tests/PgBolt.Server.Tests/Fakes/ListLoggerFactory.cs` (тестовая поддержка, используется и в Tasks 14–16)
- Test: `tests/PgBolt.Server.Tests/Unit/CompletionSinkTests.cs`

**Interfaces:**
- Consumes: `Execution`, `ExecutionState`, `ExecutionTable`, `JobOutcome`, `IServerStore` (Task 11); `MonoClock`, `PgBoltServerOptions` (Task 8); `LeaseKeeper(Guid, IServerStore, ExecutionTable, PgBoltServerOptions, MonoClock, ILogger, Action onRecovered)` и `internal Task HeartbeatOnceAsync(CancellationToken)` (Task 12, только в тесте бюджета); `FakeServerStore`, `Leased` (Task 11), `Eventually.True(Func<bool>, string)`; `LeaseRef`, `FailOutcome`, `ReleaseOutcome` (core).
- Produces (ровно по контракту):
  ```csharp
  internal sealed class CompletionSink
  {
      public CompletionSink(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options,
                            MonoClock clock, ILogger logger);
      public bool Complete(Execution execution, JobOutcome outcome);
      public void EnqueueRegistered(Execution execution);
      public void Start();
      public Task StopAsync(CancellationToken ct);
      internal int BackoffWaits { get; }      // дополнение: для тестов — сколько раз отправка встала на ожидание backoff
  }
  ```
  Тестовая поддержка (дополнение): `internal sealed record ListLogEntry(string Category, LogLevel Level, string Message, Exception? Exception)`, `internal sealed class ListLoggerFactory : ILoggerFactory { ConcurrentQueue<ListLogEntry> Entries; IEnumerable<ListLogEntry> At(LogLevel level); }`.

**Поведение (сверка с §6.1):**
- `Complete`: `Outcome` ставится до перехода. Sink остановлен → `Running → Removed` + `table.Remove`, лог, `false`. Переход `Running → OutcomePending` не удался (запись `Removed`) → лог «discarded», `false`. Иначе — постановка в отправку, `true`. Проверка «остановлен» и постановка выполняются под одним lock со сбросом буфера в `StopAsync`, поэтому исход не может попасть в буфер после финального сброса.
- Ack: буфер; сброс при `AckBatchSize` или таймером `clock.Time.CreateTimer` через `AckFlushInterval` после первого элемента (таймер создаётся в `Start`, взводится при первом элементе, снимается при сбросе). Перед каждой попыткой — CAS `OutcomePending → OutcomeInFlight` каждой записи; не удалось (heartbeat перевёл в `Removed`) — запись исключается из пакета. Успех → `OutcomeInFlight → Removed` + `table.Remove` для всех записей пакета; `AckAsync` вернул меньше — `Warning` «lease of the rest was lost» (после ошибки в той же отправке — `Information` «probably already applied»); job_id потерянных неизвестны — `AckAsync` возвращает только число.
- `EnqueueRegistered` (запись уже `OutcomePending` с `Release(false)`, §4.3): под тем же lock, что `Complete`; sink остановлен → `OutcomePending → Removed` + `table.Remove`, лог «discarded», в отправку не ставится (иначе отправка стартовала бы после финального сброса, и её никто не ждал бы).
- Остальные исходы — отдельная отправка на каждый (`Task.Run`, параллельно). Маппинг: `Retry → RetryAsync(id, leaseId, server, delay, kind, message, exception)`, `Fail → FailAsync(…, kind, message, exception)`, `ConfirmCancel → ConfirmCancelAsync(…, message)`, `Release(ran) → ReleaseAsync(id, leaseId, server, ran, message: null)`. Ответ `LeaseLost` завершает отправку (после неоднозначной ошибки — `Information` «probably already applied», иначе `Warning`).
- **Таймаут попытки** — остаток бюджета: каждая попытка (ack-пакет — по минимуму остатка его записей) получает токен `abort` ∪ `new CancellationTokenSource(LeaseDuration − elapsed, clock.Time)`, и вызов store ожидается через `.WaitAsync(attemptToken)`. Так зависший вызов (TCP black hole на соединении из пула, `Command Timeout=0` в строке подключения, store, игнорирующий токен) не держит запись в `OutcomeInFlight` — а значит, и продление её lease heartbeat'ом — дольше бюджета: по его исчерпании ожидание обрывается `OperationCanceledException`, дальше — обычная обработка ошибки ниже. Брошенный вызов может ещё завершиться в фоне: его результат не нужен, fencing по `lease_id` не даёт применить исход после того, как lease забрал sweeper. Попытка с нулевым остатком (последняя пауза урезана ровно до конца бюджета) получает уже отменённый токен и сразу завершается ошибкой.
- Ошибка любого вызова: `OutcomeInFlight → OutcomePending`; если `clock.Elapsed(OutcomeFirstAttemptAt) ≥ LeaseDuration` — `OutcomePending → Removed`, `table.Remove`, лог `Error`; иначе пауза `min(backoff, остаток бюджета)` через `Task.Delay(d, clock.Time, abort)`, backoff 100 мс → 5 с (удвоение). `OutcomeFirstAttemptAt` ставится при первой попытке (0 — не начиналась). Урезание последней паузы и таймаут попытки остатком бюджета гарантируют, что запись покидает таблицу (и перестаёт продлеваться) не позже `LeaseDuration` с первой попытки — критерий §1 «lease … истекает не позже чем через `LeaseDuration`».
- `StopAsync(ct)`: под lock сбрасывает ack-буфер и снимает список отправок; ждёт их (они продолжают повторы в рамках бюджета); повторяет, пока появляются новые; затем `_stopped = true`. Отмена `ct` → отмена внутреннего `abort`-токена: паузы и вызовы store прерываются, записи переводятся в `Removed` и удаляются из таблицы (лог `Warning`), `StopAsync` возвращается без исключения. Ack, принятые `Complete` после последнего сброса буфера (sink ещё не остановлен, пока `StopAsync` ждёт отправки), при отмене не отправляются: под тем же lock, где ставится `_stopped`, каждая запись буфера `OutcomePending → Removed`, `table.Remove`, `Warning` «discarded» — как любой исход, пришедший после остановки (§6.1). Без этого запись оставалась бы `OutcomePending` в таблице без отправки и без лога.
- Почему `BackoffWaits`: после `FakeTimeProvider.Advance` продолжения идут асинхронно, и тест не может знать, взведён ли таймер паузы. Счётчик увеличивается сразу после `Task.Delay(...)` (таймер уже создан), поэтому «дождаться `BackoffWaits == k`, затем `Advance`» детерминировано.
- Почему тесты сброса ждут не только `State == Removed`: `Abandon` выполняется на потоке отправки и делает три шага по порядку — CAS в `Removed`, `table.Remove`, лог (`Error` по бюджету / `Warning` при отмене остановки). Условие только на `Removed` могло выполниться между первым и вторым шагом, и следующий `Assert` видел запись в таблице (`Expected 0, Actual 1` в `Cancelled_stop_aborts_retries_and_drops_executions` под нагрузкой). Поэтому `Eventually` в `Exhausted_budget_…`, `Hung_send_…` и `Cancelled_stop_…` ждёт последний наблюдаемый эффект — `Removed` ∧ запись удалена из таблицы ∧ лог записан; последующие `Assert` оставлены без изменений.

- [x] **Step 1: Добавить тестовый логгер**

`tests/PgBolt.Server.Tests/Fakes/ListLoggerFactory.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace PgBolt.Server.Tests.Fakes;

internal sealed record ListLogEntry(string Category, LogLevel Level, string Message, Exception? Exception);

/// <summary>Фабрика логгеров, складывающая все записи в Entries (для проверки предупреждений).</summary>
internal sealed class ListLoggerFactory : ILoggerFactory
{
    public ConcurrentQueue<ListLogEntry> Entries { get; } = new();

    public IEnumerable<ListLogEntry> At(LogLevel level) => Entries.Where(e => e.Level == level);

    public ILogger CreateLogger(string categoryName) => new ListLogger(this, categoryName);

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }

    private sealed class ListLogger(ListLoggerFactory owner, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            owner.Entries.Enqueue(new ListLogEntry(category, logLevel, formatter(state, exception), exception));
    }
}
```

- [x] **Step 2: Написать падающие тесты**

`tests/PgBolt.Server.Tests/Unit/CompletionSinkTests.cs`:

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class CompletionSinkTests
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly ExecutionTable _table = new();
    private readonly ListLoggerFactory _logs = new();
    private readonly MonoClock _clock;
    private readonly PgBoltServerOptions _options = new PgBoltServerOptions
    {
        LeaseDuration = TimeSpan.FromSeconds(60),
        HeartbeatInterval = TimeSpan.FromSeconds(15),
        AckBatchSize = 100,
        AckFlushInterval = TimeSpan.FromMilliseconds(20),
    }.Queue("q");

    public CompletionSinkTests() => _clock = new MonoClock(_time);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private CompletionSink Sink()
    {
        var sink = new CompletionSink(Guid.NewGuid(), _store, _table, _options, _clock, _logs.CreateLogger("test"));
        sink.Start();
        return sink;
    }

    private Execution Running()
    {
        var execution = new Execution(Leased.Job(), "q", _clock.Add(_clock.Now, _options.LeaseDuration), ExecutionState.Running);
        _table.Add(execution);
        return execution;
    }

    [Fact]
    public async Task Ack_batch_is_sent_when_size_is_reached()
    {
        _options.AckBatchSize = 3;
        var sink = Sink();
        var executions = Enumerable.Range(0, 3).Select(_ => Running()).ToArray();

        foreach (var execution in executions)
            Assert.True(sink.Complete(execution, new JobOutcome.Ack()));

        await Eventually.True(() => _table.Count == 0, "the batch of 3 is acked without advancing time");
        var ack = Assert.Single(_store.CallsOf("Ack"));
        Assert.Equal(executions.Select(e => e.Lease), (LeaseRef[])ack.Detail!);
        Assert.All(executions, e => Assert.Equal(ExecutionState.Removed, e.State));
    }

    [Fact]
    public async Task Ack_batch_is_sent_after_flush_interval_from_first_item()
    {
        var sink = Sink();
        var first = Running();
        var second = Running();

        sink.Complete(first, new JobOutcome.Ack());
        _time.Advance(10 * Ms);
        sink.Complete(second, new JobOutcome.Ack());
        _time.Advance(9 * Ms);
        Assert.Empty(_store.CallsOf("Ack"));

        _time.Advance(1 * Ms);
        await Eventually.True(() => _table.Count == 0, "the window of 20 ms from the first item has passed");
        var ack = Assert.Single(_store.CallsOf("Ack"));
        Assert.Equal(new[] { first.Lease, second.Lease }, (LeaseRef[])ack.Detail!);
    }

    [Fact]
    public async Task Execution_stays_in_table_until_send_completes()
    {
        _options.AckBatchSize = 1;
        var gate = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnAck = (_, _) => gate.Task;
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Ack());
        await Eventually.True(() => _store.CallsOf("Ack").Any(), "ack is sent");

        Assert.Equal(ExecutionState.OutcomeInFlight, execution.State);
        Assert.True(_table.TryGet(execution.Job.LeaseId, out _));

        gate.SetResult(1);
        await Eventually.True(() => _table.Count == 0, "record leaves the table after the answer");
        Assert.Equal(ExecutionState.Removed, execution.State);
    }

    [Fact]
    public async Task Any_send_error_is_retried_with_doubling_backoff_capped_at_5s()
    {
        _options.AckBatchSize = 1;
        var times = new List<DateTimeOffset>();
        _store.OnAck = (leases, _) =>
        {
            lock (times)
                times.Add(_time.GetUtcNow());
            return times.Count <= 8 ? throw new IOException("network") : Task.FromResult(leases.Count);
        };
        var sink = Sink();
        var execution = Running();
        sink.Complete(execution, new JobOutcome.Ack());

        int[] expectedMs = [100, 200, 400, 800, 1600, 3200, 5000, 5000];
        for (var k = 1; k <= expectedMs.Length; k++)
        {
            await Eventually.True(() => sink.BackoffWaits == k, $"backoff wait #{k} is armed");
            Assert.Equal(ExecutionState.OutcomePending, execution.State);
            Assert.True(_table.TryGet(execution.Job.LeaseId, out _));
            _time.Advance(expectedMs[k - 1] * Ms);
        }

        await Eventually.True(() => _table.Count == 0, "the 9th attempt succeeds");
        Assert.Equal(9, times.Count);
        Assert.Equal(expectedMs, times.Zip(times.Skip(1), (a, b) => (int)(b - a).TotalMilliseconds));
        Assert.Equal(ExecutionState.Removed, execution.State);
    }

    [Fact]
    public async Task Lease_lost_after_ambiguous_error_finishes_send()
    {
        var calls = 0;
        _store.OnFail = (_, _, _) => Interlocked.Increment(ref calls) == 1
            ? throw new TimeoutException("commit state unknown")
            : Task.FromResult(FailOutcome.LeaseLost);
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "boom", "trace"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");
        _time.Advance(100 * Ms);

        await Eventually.True(() => _table.Count == 0, "LeaseLost ends the send");
        Assert.Equal(2, _store.CallsOf("Fail").Count());
        Assert.Equal(ExecutionState.Removed, execution.State);
        Assert.Contains(_logs.At(LogLevel.Information), e => e.Message.Contains("probably already applied"));
    }

    [Fact]
    public async Task Lease_lost_without_error_is_logged_as_lost()
    {
        _store.OnRetry = (_, _, _) => Task.FromResult(FailOutcome.LeaseLost);
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Retry(TimeSpan.FromSeconds(1), JobEventKind.Retry, "m", "x"));

        await Eventually.True(() => _table.Count == 0, "LeaseLost ends the send");
        Assert.Single(_store.CallsOf("Retry"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("lease was lost"));
    }

    [Fact]
    public async Task Exhausted_budget_drops_execution_from_table_and_next_heartbeat()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.AckBatchSize = 1;
        _store.OnAck = (_, _) => throw new IOException("db down");
        var sink = Sink();
        var execution = Running();
        var other = Running();

        sink.Complete(execution, new JobOutcome.Ack());
        int[] expectedMs = [100, 200, 400, 800, 1500];   // последняя пауза урезана до остатка бюджета 3 с
        for (var k = 1; k <= expectedMs.Length; k++)
        {
            await Eventually.True(() => sink.BackoffWaits == k, $"backoff wait #{k} is armed");
            _time.Advance(expectedMs[k - 1] * Ms);
        }

        // Abandon переводит в Removed, затем убирает из таблицы и последним пишет Error — ждём последний эффект.
        await Eventually.True(() => execution.State == ExecutionState.Removed
                && !_table.TryGet(execution.Job.LeaseId, out _)
                && _logs.At(LogLevel.Error).Any(e => e.Message.Contains(execution.Job.Id.ToString())),
            "budget of LeaseDuration is exhausted");
        Assert.Equal(6, _store.CallsOf("Ack").Count());
        Assert.False(_table.TryGet(execution.Job.LeaseId, out _));
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains(execution.Job.Id.ToString()));

        var keeper = new LeaseKeeper(Guid.NewGuid(), _store, _table, _options, _clock, NullLogger.Instance, () => { });
        await keeper.HeartbeatOnceAsync(Ct);
        var heartbeat = (LeaseRef[])_store.CallsOf("Heartbeat").Last().Detail!;
        Assert.Equal(new[] { other.Lease }, heartbeat);
    }

    [Fact]
    public async Task Hung_send_is_cut_off_when_budget_ends_even_if_store_ignores_token()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        var hung = new TaskCompletionSource<FailOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.OnFail = (_, _, _) => hung.Task;       // токен игнорируется: чёрная дыра TCP, Command Timeout=0
        var sink = Sink();
        var execution = Running();

        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => _store.CallsOf("Fail").Any(), "fail is sent");

        _time.Advance(TimeSpan.FromSeconds(3) - Ms);
        Assert.Equal(ExecutionState.OutcomeInFlight, execution.State);   // heartbeat пока держит lease
        _time.Advance(Ms);                               // бюджет LeaseDuration с первой попытки исчерпан

        // Abandon переводит в Removed, затем убирает из таблицы и последним пишет Error — ждём последний эффект.
        await Eventually.True(() => execution.State == ExecutionState.Removed
                && !_table.TryGet(execution.Job.LeaseId, out _)
                && _logs.At(LogLevel.Error).Any(e => e.Message.Contains(execution.Job.Id.ToString())),
            "hung attempt is cut off at the end of the budget");
        Assert.False(_table.TryGet(execution.Job.LeaseId, out _));
        Assert.Single(_store.CallsOf("Fail"));
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains(execution.Job.Id.ToString()));

        hung.SetResult(FailOutcome.Failed);             // поздний ответ брошенного вызова ничего не меняет
        Assert.Equal(ExecutionState.Removed, execution.State);
    }

    [Fact]
    public async Task Complete_on_removed_execution_returns_false_and_sends_nothing()
    {
        var sink = Sink();
        var execution = Running();
        Assert.True(execution.TryTransition(ExecutionState.Running, ExecutionState.Removed));   // брошено при остановке / LeaseLost
        _table.Remove(execution);

        Assert.False(sink.Complete(execution, new JobOutcome.Ack()));

        await sink.StopAsync(Ct);
        Assert.DoesNotContain(_store.Calls, c => c.Op is "Ack" or "Fail");
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("discarded"));
    }

    [Fact]
    public async Task Complete_after_stop_returns_false_and_removes_execution()
    {
        var sink = Sink();
        await sink.StopAsync(Ct);
        var execution = Running();

        Assert.False(sink.Complete(execution, new JobOutcome.Ack()));

        Assert.Equal(ExecutionState.Removed, execution.State);
        Assert.Equal(0, _table.Count);
        Assert.Empty(_store.Calls);
    }

    [Fact]
    public async Task Stop_sends_buffered_acks_immediately()
    {
        var sink = Sink();
        var a = Running();
        var b = Running();
        sink.Complete(a, new JobOutcome.Ack());
        sink.Complete(b, new JobOutcome.Ack());

        await sink.StopAsync(Ct);

        var ack = Assert.Single(_store.CallsOf("Ack"));
        Assert.Equal(new[] { a.Lease, b.Lease }, (LeaseRef[])ack.Detail!);
        Assert.Equal(0, _table.Count);
    }

    [Fact]
    public async Task Stop_waits_for_retrying_sends_within_budget()
    {
        var calls = 0;
        _store.OnFail = (_, _, _) => Interlocked.Increment(ref calls) == 1
            ? throw new IOException("network")
            : Task.FromResult(FailOutcome.Failed);
        var sink = Sink();
        var execution = Running();
        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");

        var stop = sink.StopAsync(Ct);
        Assert.False(stop.IsCompleted);
        _time.Advance(100 * Ms);
        await stop;

        Assert.Equal(2, _store.CallsOf("Fail").Count());
        Assert.Equal(0, _table.Count);
    }

    [Fact]
    public async Task Cancelled_stop_aborts_retries_and_drops_executions()
    {
        _store.OnFail = (_, _, _) => throw new IOException("db down");
        var sink = Sink();
        var execution = Running();
        sink.Complete(execution, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");

        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);
        await cts.CancelAsync();
        await stop;

        // Abandon переводит в Removed до удаления из таблицы (на другом потоке) — ждём и удаление, и лог.
        await Eventually.True(() => execution.State == ExecutionState.Removed
                && _table.Count == 0
                && _logs.At(LogLevel.Warning).Any(e => e.Message.Contains(execution.Job.Id.ToString())),
            "aborted send drops the record");
        Assert.Equal(0, _table.Count);
        Assert.Single(_store.CallsOf("Fail"));
        Assert.False(sink.Complete(Running(), new JobOutcome.Ack()));
    }

    [Fact]
    public async Task Ack_buffered_while_stop_is_cancelled_is_discarded_with_log()
    {
        _store.OnFail = (_, _, _) => throw new IOException("db down");
        var sink = Sink();
        var failing = Running();
        sink.Complete(failing, new JobOutcome.Fail(JobEventKind.Failed, "m", "x"));
        await Eventually.True(() => sink.BackoffWaits == 1, "first attempt failed");

        using var cts = new CancellationTokenSource();
        var stop = sink.StopAsync(cts.Token);                          // буфер ack сброшен (пуст), ждёт отправку Fail
        Assert.False(stop.IsCompleted);
        var late = Running();
        Assert.True(sink.Complete(late, new JobOutcome.Ack()));        // sink ещё принимает: ack лёг в буфер после сброса
        await cts.CancelAsync();
        await stop;

        Assert.Equal(ExecutionState.Removed, late.State);
        Assert.False(_table.TryGet(late.Job.LeaseId, out _));
        await Eventually.True(() => _table.Count == 0, "aborted send leaves the table too");
        Assert.Empty(_store.CallsOf("Ack"));
        Assert.Contains(_logs.At(LogLevel.Warning),
            e => e.Message.Contains("discarded") && e.Message.Contains(late.Job.Id.ToString()));
    }

    [Fact]
    public async Task Each_outcome_calls_matching_store_method()
    {
        var sink = Sink();
        var retry = Running();
        var fail = Running();
        var cancel = Running();
        var release = Running();

        sink.Complete(retry, new JobOutcome.Retry(TimeSpan.FromSeconds(5), JobEventKind.NoExecutor, "r-msg", "r-exc"));
        sink.Complete(fail, new JobOutcome.Fail(JobEventKind.Failed, "f-msg", "f-exc"));
        sink.Complete(cancel, new JobOutcome.ConfirmCancel("c-msg"));
        sink.Complete(release, new JobOutcome.Release(Ran: true));

        await Eventually.True(() => _table.Count == 0, "all four outcomes are sent without batching");
        var r = Assert.Single(_store.CallsOf("Retry"));
        Assert.Equal((retry.Job.Id, retry.Job.LeaseId), (r.JobId!.Value, r.LeaseId!.Value));
        Assert.Equal((TimeSpan.FromSeconds(5), JobEventKind.NoExecutor, "r-msg"), r.Detail);
        var f = Assert.Single(_store.CallsOf("Fail"));
        Assert.Equal((fail.Job.Id, fail.Job.LeaseId), (f.JobId!.Value, f.LeaseId!.Value));
        Assert.Equal((JobEventKind.Failed, "f-msg"), f.Detail);
        var c = Assert.Single(_store.CallsOf("ConfirmCancel"));
        Assert.Equal((cancel.Job.Id, cancel.Job.LeaseId, (object?)"c-msg"), (c.JobId!.Value, c.LeaseId!.Value, c.Detail));
        var rel = Assert.Single(_store.CallsOf("Release"));
        Assert.Equal((release.Job.Id, release.Job.LeaseId, (object?)true), (rel.JobId!.Value, rel.LeaseId!.Value, rel.Detail));
        Assert.Empty(_store.CallsOf("Ack"));
    }

    [Fact]
    public async Task Registered_release_is_sent_with_ran_false()
    {
        var sink = Sink();
        var execution = new Execution(Leased.Job(), "q", _clock.Add(_clock.Now, _options.LeaseDuration), ExecutionState.OutcomePending)
        {
            Outcome = new JobOutcome.Release(Ran: false),
        };
        _table.Add(execution);

        sink.EnqueueRegistered(execution);

        await Eventually.True(() => _table.Count == 0, "release is sent");
        Assert.Equal((object?)false, Assert.Single(_store.CallsOf("Release")).Detail);
    }

    [Fact]
    public async Task Registered_release_after_stop_is_dropped_not_sent()
    {
        var sink = Sink();
        await sink.StopAsync(Ct);
        var execution = new Execution(Leased.Job(), "q", _clock.Add(_clock.Now, _options.LeaseDuration), ExecutionState.OutcomePending)
        {
            Outcome = new JobOutcome.Release(Ran: false),
        };
        _table.Add(execution);

        sink.EnqueueRegistered(execution);

        Assert.Equal(ExecutionState.Removed, execution.State);
        Assert.Equal(0, _table.Count);
        Assert.Empty(_store.Calls);
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("discarded"));
    }

    [Fact]
    public async Task Ack_batch_skips_records_removed_by_heartbeat_and_logs_partial_ack()
    {
        _options.AckBatchSize = 3;
        _store.OnAck = (leases, _) => Task.FromResult(leases.Count - 1);
        var sink = Sink();
        var removed = Running();
        var a = Running();
        var b = Running();

        sink.Complete(removed, new JobOutcome.Ack());
        Assert.True(removed.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed));   // heartbeat: lease потерян
        _table.Remove(removed);
        sink.Complete(a, new JobOutcome.Ack());
        sink.Complete(b, new JobOutcome.Ack());

        await Eventually.True(() => _table.Count == 0, "batch is acked");
        Assert.Equal(new[] { a.Lease, b.Lease }, (LeaseRef[])Assert.Single(_store.CallsOf("Ack")).Detail!);
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("affected 1 of 2"));
    }

    [Fact]
    public async Task Concurrent_complete_and_heartbeat_removal_have_exactly_one_winner()
    {
        _options.AckBatchSize = 7;
        var sink = Sink();
        var executions = Enumerable.Range(0, 500).Select(_ => Running()).ToArray();
        var removedByHeartbeat = new System.Collections.Concurrent.ConcurrentBag<Execution>();
        var accepted = new System.Collections.Concurrent.ConcurrentBag<Execution>();
        using var start = new Barrier(2);

        var runner = Task.Run(() =>
        {
            start.SignalAndWait(Ct);
            foreach (var e in executions)
                if (sink.Complete(e, new JobOutcome.Ack()))
                    accepted.Add(e);
        }, Ct);
        var heartbeat = Task.Run(() =>
        {
            start.SignalAndWait(Ct);
            foreach (var e in executions.Reverse())
                if (e.TryTransition(ExecutionState.Running, ExecutionState.Removed)
                    || e.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                {
                    _table.Remove(e);
                    removedByHeartbeat.Add(e);
                }
        }, Ct);
        await Task.WhenAll(runner, heartbeat);
        await sink.StopAsync(Ct);

        var acked = _store.CallsOf("Ack").SelectMany(c => (LeaseRef[])c.Detail!).ToHashSet();
        var removed = removedByHeartbeat.Select(e => e.Lease).ToHashSet();
        Assert.Empty(acked.Intersect(removed));
        Assert.Equal(executions.Length, acked.Count + removed.Count);
        Assert.Subset(accepted.Select(e => e.Lease).ToHashSet(), acked);
        Assert.All(executions, e => Assert.Equal(ExecutionState.Removed, e.State));
        Assert.Equal(0, _table.Count);
    }
}
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~CompletionSinkTests"`
Expected: FAIL при компиляции — CS0246: не найден тип `CompletionSink`.

- [x] **Step 4: Реализовать**

`src/PgBolt.Server/Runtime/CompletionSink.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Отправка исходов (§6.1): ack — пакетами (размер или окно от первого элемента), остальное — поштучно и параллельно.
/// Каждая попытка начинается CAS OutcomePending → OutcomeInFlight; ошибка — обратно в OutcomePending и повтор
/// с backoff 100 мс → 5 с в пределах бюджета LeaseDuration от первой попытки. Запись покидает таблицу только
/// после завершения отправки (успех, LeaseLost, бюджет, прерывание остановкой).
/// </summary>
internal sealed class CompletionSink
{
    private static readonly TimeSpan MinBackoff = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(5);

    private readonly Guid _serverId;
    private readonly IServerStore _store;
    private readonly ExecutionTable _table;
    private readonly PgBoltServerOptions _options;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly Lock _lock = new();
    private readonly ConcurrentDictionary<Task, byte> _sends = new();
    private readonly CancellationTokenSource _abort = new();
    private List<Execution> _ackBuffer = [];
    private ITimer? _ackTimer;
    private bool _stopped;
    private int _backoffWaits;

    public CompletionSink(Guid serverId, IServerStore store, ExecutionTable table, PgBoltServerOptions options,
        MonoClock clock, ILogger logger)
    {
        _serverId = serverId;
        _store = store;
        _table = table;
        _options = options;
        _clock = clock;
        _logger = logger;
    }

    /// <summary>Для тестов: сколько раз отправка встала на ожидание backoff (таймер уже взведён).</summary>
    internal int BackoffWaits => Volatile.Read(ref _backoffWaits);

    public void Start()
    {
        lock (_lock)
        {
            _ackTimer ??= _clock.Time.CreateTimer(static s => ((CompletionSink)s!).OnAckTimer(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            if (_ackBuffer.Count > 0)
                _ackTimer.Change(_options.AckFlushInterval, Timeout.InfiniteTimeSpan);
        }
    }

    public bool Complete(Execution execution, JobOutcome outcome)
    {
        execution.Outcome = outcome;
        lock (_lock)
        {
            if (_stopped)
            {
                if (execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
                    _table.Remove(execution);
                _logger.LogWarning("PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink is stopped.",
                    outcome.GetType().Name, execution.Job.Id);
                return false;
            }
            if (!execution.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending))
            {
                _logger.LogWarning("PgBolt: outcome {Outcome} of job {JobId} discarded: execution is {State} (abandoned or lease lost).",
                    outcome.GetType().Name, execution.Job.Id, execution.State);
                return false;
            }
            EnqueueLocked(execution);
            return true;
        }
    }

    public void EnqueueRegistered(Execution execution)
    {
        lock (_lock)
        {
            if (_stopped)
            {
                // Как в Complete: после финального сброса отправку никто не ждал бы. Lease истечёт, задачу заберёт sweeper.
                if (execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                    _table.Remove(execution);
                _logger.LogWarning("PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink is stopped.",
                    execution.Outcome?.GetType().Name, execution.Job.Id);
                return;
            }
            EnqueueLocked(execution);
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        using var abort = ct.Register(static s => ((CancellationTokenSource)s!).Cancel(), _abort);
        while (true)
        {
            Task[] pending;
            lock (_lock)
            {
                FlushAckLocked();
                pending = [.. _sends.Keys];
                if (pending.Length == 0)
                {
                    _stopped = true;
                    _ackTimer?.Dispose();
                    return;
                }
            }
            try
            {
                await Task.WhenAll(pending).WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Отправки прерываются через _abort и сами снимают свои записи; дольше не ждём.
                await _abort.CancelAsync();
                _logger.LogWarning("PgBolt: completion sink stop was cancelled; {Count} outcome send(s) aborted.", pending.Length);
                lock (_lock)
                {
                    DiscardAckBufferLocked();
                    _stopped = true;
                    _ackTimer?.Dispose();
                }
                return;
            }
        }
    }

    private void EnqueueLocked(Execution execution)
    {
        if (execution.Outcome is JobOutcome.Ack)
        {
            _ackBuffer.Add(execution);
            if (_ackBuffer.Count >= _options.AckBatchSize)
                FlushAckLocked();
            else if (_ackBuffer.Count == 1)
                _ackTimer?.Change(_options.AckFlushInterval, Timeout.InfiniteTimeSpan);
            return;
        }
        StartSend(() => SendSingleAsync(execution));
    }

    private void OnAckTimer()
    {
        lock (_lock)
            FlushAckLocked();
    }

    private void FlushAckLocked()
    {
        if (_ackBuffer.Count == 0)
            return;
        var batch = _ackBuffer;
        _ackBuffer = new List<Execution>(Math.Min(_options.AckBatchSize, 1024));
        _ackTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        StartSend(() => SendAckBatchAsync(batch));
    }

    /// <summary>
    /// Отменённая остановка: ack, принятые после последнего сброса, не отправляются (отправка уже прервана) — отбрасываются
    /// с логом, как исход после остановки (§6.1), и покидают таблицу; lease истечёт, задачу заберёт sweeper.
    /// </summary>
    private void DiscardAckBufferLocked()
    {
        foreach (var execution in _ackBuffer)
        {
            if (!execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
                continue;                                  // уже снята heartbeat'ом (lease потерян)
            _table.Remove(execution);
            _logger.LogWarning("PgBolt: outcome {Outcome} of job {JobId} discarded: completion sink stop was cancelled; its lease will expire.",
                nameof(JobOutcome.Ack), execution.Job.Id);
        }
        _ackBuffer.Clear();
        _ackTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    private void StartSend(Func<Task> send)
    {
        var task = Task.Run(send);
        _sends.TryAdd(task, 0);
        _ = task.ContinueWith(static (t, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(t, out _), _sends,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private async Task SendAckBatchAsync(List<Execution> batch)
    {
        var backoff = MinBackoff;
        var ambiguous = false;
        var pending = batch;
        while (true)
        {
            var inFlight = BeginAttempt(pending);
            if (inFlight.Count == 0)
                return;
            try
            {
                var leases = new LeaseRef[inFlight.Count];
                for (var i = 0; i < leases.Length; i++)
                    leases[i] = inFlight[i].Lease;
                using var budget = new CancellationTokenSource(RemainingBudget(inFlight), _clock.Time);
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token, budget.Token);
                var acked = await _store.AckAsync(_serverId, leases, attempt.Token).WaitAsync(attempt.Token);
                if (acked < leases.Length)
                {
                    if (ambiguous)
                        _logger.LogInformation("PgBolt: ack affected {Acked} of {Count} jobs after a failed attempt; the rest was probably already applied.",
                            acked, leases.Length);
                    else
                        _logger.LogWarning("PgBolt: ack affected {Acked} of {Count} jobs; lease of the rest was lost.", acked, leases.Length);
                }
                foreach (var execution in inFlight)
                    Finish(execution);
                return;
            }
            catch (Exception error)
            {
                ambiguous = true;
                pending = await AfterFailureAsync(inFlight, error, backoff);
                if (pending.Count == 0)
                    return;
                backoff = Next(backoff);
            }
        }
    }

    private async Task SendSingleAsync(Execution execution)
    {
        var backoff = MinBackoff;
        var ambiguous = false;
        List<Execution> single = [execution];
        while (true)
        {
            if (BeginAttempt(single).Count == 0)
                return;
            try
            {
                using var budget = new CancellationTokenSource(RemainingBudget(single), _clock.Time);
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token, budget.Token);
                if (!await SendOutcomeAsync(execution, attempt.Token))
                {
                    if (ambiguous)
                        _logger.LogInformation("PgBolt: outcome {Outcome} of job {JobId}: lease not found after a failed attempt; the outcome was probably already applied.",
                            execution.Outcome!.GetType().Name, execution.Job.Id);
                    else
                        _logger.LogWarning("PgBolt: outcome {Outcome} of job {JobId} discarded: lease was lost.",
                            execution.Outcome!.GetType().Name, execution.Job.Id);
                }
                Finish(execution);
                return;
            }
            catch (Exception error)
            {
                ambiguous = true;
                if ((await AfterFailureAsync(single, error, backoff)).Count == 0)
                    return;
                backoff = Next(backoff);
            }
        }
    }

    /// <returns>true — исход применён; false — LeaseLost.</returns>
    /// <remarks>ct — токен попытки; WaitAsync обрывает ожидание и тогда, когда вызов store токен не соблюдает.</remarks>
    private async Task<bool> SendOutcomeAsync(Execution execution, CancellationToken ct)
    {
        var job = execution.Job;
        switch (execution.Outcome)
        {
            case JobOutcome.Retry r:
                return await _store.RetryAsync(job.Id, job.LeaseId, _serverId, r.Delay, r.Kind, r.Message, r.Exception, ct).WaitAsync(ct) != FailOutcome.LeaseLost;
            case JobOutcome.Fail f:
                return await _store.FailAsync(job.Id, job.LeaseId, _serverId, f.Kind, f.Message, f.Exception, ct).WaitAsync(ct) != FailOutcome.LeaseLost;
            case JobOutcome.ConfirmCancel c:
                return await _store.ConfirmCancelAsync(job.Id, job.LeaseId, _serverId, c.Message, ct).WaitAsync(ct) != FailOutcome.LeaseLost;
            case JobOutcome.Release r:
                return await _store.ReleaseAsync(job.Id, job.LeaseId, _serverId, r.Ran, message: null, ct).WaitAsync(ct) != ReleaseOutcome.LeaseLost;
            default:
                throw new InvalidOperationException($"Outcome {execution.Outcome?.GetType().Name ?? "null"} cannot be sent individually.");
        }
    }

    /// <summary>OutcomePending → OutcomeInFlight для каждой записи; не удалось (Removed heartbeat'ом) — запись исключается.</summary>
    private List<Execution> BeginAttempt(List<Execution> executions)
    {
        var inFlight = new List<Execution>(executions.Count);
        var now = _clock.Now;
        foreach (var execution in executions)
        {
            if (execution.OutcomeFirstAttemptAt == 0)
                execution.OutcomeFirstAttemptAt = now;
            if (execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight))
                inFlight.Add(execution);
            else
                _logger.LogDebug("PgBolt: outcome of job {JobId} is not sent: execution is {State}.", execution.Job.Id, execution.State);
        }
        return inFlight;
    }

    /// <summary>
    /// Таймаут попытки (§6.1, критерий §1): остаток бюджета LeaseDuration с первой попытки, для ack-пакета — минимальный
    /// среди записей; ≥ 0 (нулевой — уже отменённый токен).
    /// </summary>
    private TimeSpan RemainingBudget(List<Execution> executions)
    {
        var remaining = _options.LeaseDuration;
        foreach (var execution in executions)
        {
            var left = _options.LeaseDuration - _clock.Elapsed(execution.OutcomeFirstAttemptAt);
            if (left < remaining)
                remaining = left;
        }
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    private void Finish(Execution execution)
    {
        if (execution.TryTransition(ExecutionState.OutcomeInFlight, ExecutionState.Removed))
            _table.Remove(execution);
    }

    /// <returns>Записи, которые нужно повторить (после ожидания backoff); пусто — отправка окончена.</returns>
    private async Task<List<Execution>> AfterFailureAsync(List<Execution> inFlight, Exception error, TimeSpan backoff)
    {
        var retry = new List<Execution>(inFlight.Count);
        var budgetLeft = TimeSpan.MaxValue;
        foreach (var execution in inFlight)
        {
            execution.TryTransition(ExecutionState.OutcomeInFlight, ExecutionState.OutcomePending);
            var elapsed = _clock.Elapsed(execution.OutcomeFirstAttemptAt);
            if (_abort.IsCancellationRequested || elapsed >= _options.LeaseDuration)
            {
                Abandon(execution, error);
                continue;
            }
            retry.Add(execution);
            budgetLeft = budgetLeft < _options.LeaseDuration - elapsed ? budgetLeft : _options.LeaseDuration - elapsed;
        }
        if (retry.Count == 0)
            return retry;

        var delay = backoff < budgetLeft ? backoff : budgetLeft;
        _logger.LogWarning(error, "PgBolt: sending outcome of {Count} job(s) failed; retrying in {Delay}.", retry.Count, delay);
        try
        {
            var wait = Task.Delay(delay, _clock.Time, _abort.Token);
            Interlocked.Increment(ref _backoffWaits);
            await wait;
        }
        catch (OperationCanceledException)
        {
            foreach (var execution in retry)
                Abandon(execution, error);
            return [];
        }
        return retry;
    }

    private void Abandon(Execution execution, Exception error)
    {
        if (!execution.TryTransition(ExecutionState.OutcomePending, ExecutionState.Removed))
            return;
        _table.Remove(execution);
        if (_abort.IsCancellationRequested)
            _logger.LogWarning(error, "PgBolt: outcome of job {JobId} was not sent before stop was cancelled; its lease will expire.",
                execution.Job.Id);
        else
            _logger.LogError(error, "PgBolt: outcome of job {JobId} was not sent within {Budget}; the execution is dropped, its lease will expire.",
                execution.Job.Id, _options.LeaseDuration);
    }

    private static TimeSpan Next(TimeSpan backoff) => backoff * 2 < MaxBackoff ? backoff * 2 : MaxBackoff;
}
```

- [x] **Step 5: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~CompletionSinkTests"`
Expected: PASS — 19 тестов. Прогнать 3 раза подряд: тест гонки (`Concurrent_complete_and_heartbeat_removal_have_exactly_one_winner`) и тесты backoff не должны мигать.

Контроль, что тесты ловят регрессию (каждую правку внести отдельно, прогнать, убедиться в падении, вернуть код Step 4 — файл ещё не закоммичен):
- в `SendOutcomeAsync` убрать `.WaitAsync(ct)` у `FailAsync` → падает `Hung_send_is_cut_off_when_budget_ends_even_if_store_ignores_token`;
- в `StopAsync` убрать вызов `DiscardAckBufferLocked()` → падает `Ack_buffered_while_stop_is_cancelled_is_discarded_with_log`.

Run: `dotnet build src/PgBolt.Server`
Expected: `0 предупреждений` (TreatWarningsAsErrors, AOT/trim-анализ).

- [x] **Step 6: Commit**

```bash
git add src/PgBolt.Server/Runtime/CompletionSink.cs tests/PgBolt.Server.Tests/Fakes/ListLoggerFactory.cs tests/PgBolt.Server.Tests/Unit/CompletionSinkTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): completion sink with batched ack and budgeted retries"
```

---

### Task 14: Фильтры, `JobRunner` и классификация исхода

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Спека:** §3.6, §5.1, §5.2, §3.4 (порядок выбора задержки), Review Focus п. 3.

**Files:**
- Modify: `src/PgBolt.Server/IJobFilter.cs`, `src/PgBolt.Server/JobExecutingContext.cs` (созданы в Task 8; здесь только XML-комментарии, сигнатуры не меняются)
- Create: `src/PgBolt.Server/Runtime/FilterPipeline.cs`, `src/PgBolt.Server/Runtime/OutcomeClassifier.cs`, `src/PgBolt.Server/Runtime/JobRunner.cs`
- Test: `tests/PgBolt.Server.Tests/Unit/OutcomeClassifierTests.cs`, `tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs`

**Interfaces:**
- Consumes: `JobRegistry`, `JobExecutorRegistration`, `JobsBuilder` (core; `JobsBuilder` internal, IVT для `PgBolt.Server.Tests` — Task 7); `JobDescriptor.TimeoutSeconds`, `JobAbortedException`, `JobArgumentsException` (Task 1); invoker, оборачивающий ошибки десериализации в `JobArgumentsException` (Task 3); `Execution`, `CancelReason`, `JobOutcome` (Task 11); `RetryDecider`, `RetryPolicy`, `JobExecutorNotFoundException` (Task 9); `MonoClock`, `PgBoltServerOptions` (Task 8); `Leased` (Task 11); `ListLoggerFactory` (Fakes, Task 13 — проверка предупреждения об урезанном таймауте).
- Produces:
  ```csharp
  namespace PgBolt.Server;
  public delegate ValueTask JobExecutionDelegate();
  public interface IJobFilter { ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next); }
  public sealed class JobExecutingContext
  {
      internal JobExecutingContext(JobContext job, IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, CancellationToken cancellationToken);
      public JobContext Job { get; }
      public IServiceProvider Services { get; }
      public ReadOnlyMemory<byte> ArgsUtf8 { get; }
      public CancellationToken CancellationToken { get; }
  }

  namespace PgBolt.Server.Runtime;
  internal sealed class FilterPipeline(IReadOnlyList<Type> filterTypes)
  { public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate terminal); }
  internal static class OutcomeClassifier
  {
      public static JobOutcome Classify(Execution execution, Exception? error, RetryDecider retry, TimeSpan? timeout);
      public static JobOutcome NoExecutor(Execution execution, RetryDecider retry);
      public static string Truncate(string? text);            // null → "", одиночный суррогат → U+FFFD, ≤ 8192 байт UTF-8 без разрыва суррогатной пары
  }
  internal sealed class JobRunner
  {
      public JobRunner(JobRegistry registry, IServiceScopeFactory scopes, FilterPipeline filters, RetryDecider retry,
                       PgBoltServerOptions options, MonoClock clock, ILoggerFactory loggers);
      public Task<JobOutcome> RunAsync(Execution execution, CancellationToken shutdown);
  }
  ```

**Поведение:**
- `FilterPipeline`: фильтры резолвятся `context.Services.GetRequiredService(type)` из scope задачи — без reflection-активации (AOT): типы заранее зарегистрированы как scoped (Task 16). `GetRequiredService(Type)` не аннотирован, trim-анализ его пропускает. Порядок — порядок списка (глобальные уже впереди, список собирает `PgBoltServer`), первый — внешний; `next` вызывает следующий фильтр или terminal. Синхронное исключение фильтра выходит из `InvokeAsync` синхронно и ловится runner'ом.
- `JobRunner.RunAsync` (§5.1): (1) shutdown уже отменён → `Release(false)` без поиска исполнителя; (2) нет в registry → `OutcomeClassifier.NoExecutor`; (3) `shutdown.Register(() => execution.Cancel(Shutdown))` — для уже отменённого токена колбэк срабатывает сразу; (4) таймаут = `TimeoutSeconds` дескриптора > 0, иначе `DefaultJobTimeout`; есть — `new CancellationTokenSource(timeout, clock.Time)` с колбэком `execution.Cancel(Timeout)`. `DefaultJobTimeout` уже ограничен `MaxJobTimeout` (Task 8), а `TimeoutSeconds` больше `MaxJobTimeout` (2³² − 2 мс ≈ 49,7 сут; `int` допускает до ≈ 68 лет) урезается до него: иначе конструктор токена бросил бы `ArgumentOutOfRangeException` мимо `try`, `RunAsync` нарушил бы «не бросает», а выполнение осталось бы `Running` с занятым слотом. Урезанное значение идёт и в сообщение `timeout after … s`. Урезание не молчаливое: для `[Job]` генератор уже выдал предупреждение BOLT008 (Task 2), а runner при первом урезании таймаута типа задачи пишет `Warning` (категория `PgBolt.Server`, имя задачи, заданное и фактическое значения) — один раз на тип задачи в runner'е сервера (`ConcurrentDictionary.TryAdd` по имени), так что и дескриптор, зарегистрированный вручную через `IPgBoltJobsBuilder.AddExecutor` в обход генератора, не урезается незаметно, а лог не растёт с числом выполнений; (5) `AsyncServiceScope`, `Encoding.UTF8.GetBytes(ArgsJson)` один раз, `JobContext` с логгером `PgBolt.Job`, конвейер фильтров → terminal. Terminal превращает синхронное исключение исполнителя и `null` вместо `Task` в faulted `ValueTask` (`null` → `InvalidOperationException`), поэтому фильтр видит сбой через `await next()`, а не синхронный throw. Любое исключение (включая `DisposeAsync` scope) → `Classify(execution, e, …)`; метод не бросает.
- `OutcomeClassifier.Classify` — таблица §5.2 сверху вниз: `error == null` → `Ack`; `FirstReason == CancelRequested` → `ConfirmCancel(Truncate(error.Message))`; `FirstReason == Shutdown && error is OperationCanceledException` → `Release(true)`; `JobAbortedException`/`JobArgumentsException` → `Fail(Failed, msg, exc)`; иначе сообщение: `Timeout` → `timeout after {s:0.###} s` (InvariantCulture), `HeartbeatLost` → `cancelled: lease renewal not confirmed`, иначе `error.Message`; `attempt < max_attempts` и `retry.GetDelay(attempt, retryDelays, error) is {} d` → `Retry(d, Retry, …)`, иначе `Fail(Failed, …)`. `NoExecutor`: `error = new JobExecutorNotFoundException(type)`, те же правила с `kind = NoExecutor` в обоих исходах. Политика retry не вызывается при `attempt ≥ max_attempts` (короткое замыкание; наблюдаемый исход тот же). `exception` = `Truncate(error.ToString())`.
- `Truncate` — «8 КБ» §5.2 как 8192 **байт** UTF-8 (так текст хранится в PostgreSQL): кириллица занимает 2 байта на символ, CJK — 3, эмодзи — 4, поэтому обрезка по 8192 символам давала бы до 16–24 КБ. Режет по границе кодовой точки (`Rune`) и не разрывает суррогатную пару: Npgsql кодирует строки строгим UTF-8, одиночный суррогат дал бы `EncoderFallbackException` при каждой попытке отправки и исчерпание бюджета. По той же причине **одиночные суррогаты, уже бывшие в исходном тексте** (сообщение или `ToString()` исключения из пользовательского кода), заменяются на U+FFFD до обрезки — иначе такой исход не отправился бы ни разу и задача вместо Retry/Failed с сообщением закончилась бы через `LeaseExpired`. Поиск суррогатов — векторизованный `IndexOfAnyInRange('\uD800', '\uDFFF')`; строка без них (обычный случай) не копируется, строка с валидными парами — тоже. Быстрый путь после замены: строка ≤ 2730 символов (≤ 3 байт на UTF-16 символ) или `Encoding.UTF8.GetByteCount ≤ 8192` возвращается как есть.

- [x] **Step 1: Написать падающие тесты классификации**

`tests/PgBolt.Server.Tests/Unit/OutcomeClassifierTests.cs`:

```csharp
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class OutcomeClassifierTests
{
    private static readonly RetryDecider Retry =
        new(RetryPolicy.Custom((attempt, _) => TimeSpan.FromSeconds(10 * attempt)), NullLogger.Instance);

    private static Execution Exec(int attempt = 1, int maxAttempts = 3, int[]? retryDelays = null, params CancelReason[] reasons)
    {
        var execution = new Execution(Leased.Job(attempt: attempt, maxAttempts: maxAttempts, retryDelays: retryDelays), "q", 0, ExecutionState.Running);
        foreach (var reason in reasons)
            execution.Cancel(reason);
        return execution;
    }

    [Fact]
    public void Success_is_ack() =>
        Assert.IsType<JobOutcome.Ack>(OutcomeClassifier.Classify(Exec(), null, Retry, null));

    [Fact]
    public void Success_after_cancellation_is_still_ack() =>
        Assert.IsType<JobOutcome.Ack>(OutcomeClassifier.Classify(Exec(reasons: CancelReason.CancelRequested), null, Retry, null));

    [Theory]
    [InlineData("cancelled")]
    [InlineData("invalid")]
    [InlineData("aborted")]
    public void Cancel_requested_first_confirms_cancel_for_any_exception(string kind)
    {
        Exception error = kind switch
        {
            "cancelled" => new OperationCanceledException("stopped by user"),
            "invalid" => new InvalidOperationException("stopped by user"),
            _ => new JobAbortedException("stopped by user"),
        };
        var outcome = OutcomeClassifier.Classify(Exec(reasons: [CancelReason.CancelRequested, CancelReason.Shutdown]), error, Retry, null);
        Assert.Equal(new JobOutcome.ConfirmCancel("stopped by user"), outcome);
    }

    [Fact]
    public void Shutdown_first_with_operation_cancelled_releases_as_ran()
    {
        var outcome = OutcomeClassifier.Classify(Exec(reasons: CancelReason.Shutdown), new TaskCanceledException(), Retry, null);
        Assert.Equal(new JobOutcome.Release(Ran: true), outcome);
    }

    [Fact]
    public void Shutdown_first_with_other_exception_is_retried()
    {
        var outcome = OutcomeClassifier.Classify(Exec(reasons: CancelReason.Shutdown), new IOException("disk"), Retry, null);
        var retry = Assert.IsType<JobOutcome.Retry>(outcome);
        Assert.Equal((TimeSpan.FromSeconds(10), JobEventKind.Retry, "disk"), (retry.Delay, retry.Kind, retry.Message));
    }

    [Fact]
    public void Shutdown_after_cancel_requested_confirms_cancel()
    {
        var outcome = OutcomeClassifier.Classify(Exec(reasons: [CancelReason.CancelRequested, CancelReason.Shutdown]),
            new OperationCanceledException("cancelled"), Retry, null);
        Assert.IsType<JobOutcome.ConfirmCancel>(outcome);
    }

    [Fact]
    public void Aborted_and_argument_errors_fail_without_retry()
    {
        var aborted = OutcomeClassifier.Classify(Exec(), new JobAbortedException("pointless"), Retry, null);
        var args = OutcomeClassifier.Classify(Exec(), new JobArgumentsException("bad args", new JsonException("x")), Retry, null);

        var a = Assert.IsType<JobOutcome.Fail>(aborted);
        Assert.Equal((JobEventKind.Failed, "pointless"), (a.Kind, a.Message));
        Assert.Contains(nameof(JobAbortedException), a.Exception);
        Assert.Equal((JobEventKind.Failed, "bad args"), (((JobOutcome.Fail)args).Kind, ((JobOutcome.Fail)args).Message));
    }

    [Fact]
    public void Other_exception_is_retried_with_policy_delay_and_details()
    {
        var error = new InvalidOperationException("boom");
        var retry = Assert.IsType<JobOutcome.Retry>(OutcomeClassifier.Classify(Exec(attempt: 2), error, Retry, null));
        Assert.Equal((TimeSpan.FromSeconds(20), JobEventKind.Retry, "boom", error.ToString()),
            (retry.Delay, retry.Kind, retry.Message, retry.Exception));
    }

    [Fact]
    public void Job_retry_delays_take_precedence_over_policy()
    {
        var retry = Assert.IsType<JobOutcome.Retry>(OutcomeClassifier.Classify(Exec(retryDelays: [7]), new Exception("x"), Retry, null));
        Assert.Equal(TimeSpan.FromSeconds(7), retry.Delay);
    }

    [Fact]
    public void Last_attempt_fails()
    {
        var fail = Assert.IsType<JobOutcome.Fail>(OutcomeClassifier.Classify(Exec(attempt: 3, maxAttempts: 3), new Exception("x"), Retry, null));
        Assert.Equal((JobEventKind.Failed, "x"), (fail.Kind, fail.Message));
    }

    [Fact]
    public void Null_delay_from_policy_fails()
    {
        var never = new RetryDecider(RetryPolicy.Custom((_, _) => null), NullLogger.Instance);
        Assert.IsType<JobOutcome.Fail>(OutcomeClassifier.Classify(Exec(), new Exception("x"), never, null));
    }

    [Fact]
    public void Timeout_first_is_retried_with_timeout_message()
    {
        var outcome = OutcomeClassifier.Classify(Exec(reasons: CancelReason.Timeout), new TaskCanceledException(), Retry, TimeSpan.FromMilliseconds(2500));
        Assert.Equal("timeout after 2.5 s", Assert.IsType<JobOutcome.Retry>(outcome).Message);
    }

    [Fact]
    public void Heartbeat_lost_then_shutdown_spends_attempt()
    {
        var execution = Exec(reasons: [CancelReason.HeartbeatLost, CancelReason.Shutdown]);
        var retry = Assert.IsType<JobOutcome.Retry>(OutcomeClassifier.Classify(execution, new OperationCanceledException(), Retry, null));
        Assert.Equal((JobEventKind.Retry, "cancelled: lease renewal not confirmed"), (retry.Kind, retry.Message));
    }

    [Fact]
    public void Heartbeat_lost_on_last_attempt_fails()
    {
        var execution = Exec(attempt: 3, maxAttempts: 3, reasons: [CancelReason.HeartbeatLost, CancelReason.Shutdown]);
        var fail = Assert.IsType<JobOutcome.Fail>(OutcomeClassifier.Classify(execution, new OperationCanceledException(), Retry, null));
        Assert.Equal((JobEventKind.Failed, "cancelled: lease renewal not confirmed"), (fail.Kind, fail.Message));
    }

    [Fact]
    public void No_executor_retries_with_no_executor_kind_then_fails()
    {
        JobExecutorNotFoundException? seen = null;
        var decider = new RetryDecider(RetryPolicy.Custom((_, e) =>
        {
            seen = (JobExecutorNotFoundException)e;
            return TimeSpan.FromMinutes(1);
        }), NullLogger.Instance);

        var retry = Assert.IsType<JobOutcome.Retry>(OutcomeClassifier.NoExecutor(Exec(), decider));
        Assert.Equal((TimeSpan.FromMinutes(1), JobEventKind.NoExecutor), (retry.Delay, retry.Kind));
        Assert.Equal("test.job", seen!.JobName);
        Assert.Contains("test.job", retry.Message);

        var fail = Assert.IsType<JobOutcome.Fail>(OutcomeClassifier.NoExecutor(Exec(attempt: 3, maxAttempts: 3), decider));
        Assert.Equal(JobEventKind.NoExecutor, fail.Kind);
    }

    [Fact]
    public void Truncate_limits_to_8192_utf8_bytes_and_replaces_lone_surrogates()
    {
        Assert.Equal(8192, OutcomeClassifier.Truncate(new string('a', 10_000)).Length);
        Assert.Equal("short", OutcomeClassifier.Truncate("short"));
        Assert.Equal("", OutcomeClassifier.Truncate(null));

        var cyrillic = OutcomeClassifier.Truncate(new string('я', 5000));   // 2 байта на символ: 10 000 байт
        Assert.Equal(4096, cyrillic.Length);
        Assert.Equal(8192, Encoding.UTF8.GetByteCount(cyrillic));

        var text = new string('a', 8190) + "😀" + "tail";   // эмодзи — 4 байта: 8190 + 4 > 8192
        var cut = OutcomeClassifier.Truncate(text);
        Assert.Equal(8190, cut.Length);
        Assert.False(char.IsHighSurrogate(cut[^1]));

        var exact = new string('a', 8188) + "😀" + "tail";  // 8188 + 4 = 8192: пара помещается целиком
        Assert.Equal(8190, OutcomeClassifier.Truncate(exact).Length);

        // Одиночные суррогаты исходного текста (невалидный UTF-16) → U+FFFD; валидная пара остаётся.
        Assert.Equal("a\uFFFDb", OutcomeClassifier.Truncate("a\uD800b"));
        Assert.Equal("\uFFFDx😀\uFFFD", OutcomeClassifier.Truncate("\uDC00x😀\uD83D"));
        var strict = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        Assert.Equal(5, strict.GetByteCount(OutcomeClassifier.Truncate("a\uD800b")));   // строгий энкодер (как у Npgsql) не бросает

        var lone = OutcomeClassifier.Truncate(new string('\uD800', 3000));   // 3000 × U+FFFD = 9000 байт
        Assert.Equal(new string('\uFFFD', 2730), lone);                      // 2730 × 3 = 8190 ≤ 8192
    }

    [Fact]
    public void Long_messages_are_truncated_in_outcomes()
    {
        var retry = Assert.IsType<JobOutcome.Retry>(OutcomeClassifier.Classify(Exec(), new Exception(new string('m', 9000)), Retry, null));
        Assert.Equal(8192, retry.Message!.Length);
        Assert.Equal(8192, retry.Exception!.Length);
    }
}
```

- [x] **Step 2: Написать падающие тесты runner'а и фильтров**

`tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs` (исполнители регистрируются через настоящие `JobRegistry` + `JobsBuilder`; `RunnerArgs`/`TimedArgs` — разные args-типы, потому что `JobMetadata<TArgs>` хранит один дескриптор на тип):

```csharp
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Jobs;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed record RunnerArgs(int N);
public sealed record TimedArgs(int N);
public sealed record LongTimedArgs(int N);

[JsonSerializable(typeof(RunnerArgs))]
[JsonSerializable(typeof(TimedArgs))]
[JsonSerializable(typeof(LongTimedArgs))]
internal sealed partial class RunnerJson : JsonSerializerContext;

/// <summary>Поведение исполнителя задаётся тестом; все вызовы и контексты записываются.</summary>
public sealed class RunnerProbe
{
    public Func<JobContext, CancellationToken, Task> Behavior { get; set; } = (_, _) => Task.CompletedTask;
    public ConcurrentQueue<string> Trace { get; } = new();
    public ConcurrentQueue<JobContext> Contexts { get; } = new();
    public ConcurrentQueue<CancellationToken> Tokens { get; } = new();
    public ConcurrentQueue<JobExecutingContext> FilterContexts { get; } = new();
    public ConcurrentQueue<object> FilterInstances { get; } = new();
    public ConcurrentQueue<bool> ResolvedFromJobScope { get; } = new();
}

public sealed class ProbeJob(RunnerProbe probe) : IJob<RunnerArgs>, IJob<TimedArgs>, IJob<LongTimedArgs>
{
    public Task ExecuteAsync(RunnerArgs args, JobContext context, CancellationToken cancellationToken) => Run(context, cancellationToken);

    public Task ExecuteAsync(TimedArgs args, JobContext context, CancellationToken cancellationToken) => Run(context, cancellationToken);

    public Task ExecuteAsync(LongTimedArgs args, JobContext context, CancellationToken cancellationToken) => Run(context, cancellationToken);

    private Task Run(JobContext context, CancellationToken ct)
    {
        probe.Contexts.Enqueue(context);
        probe.Tokens.Enqueue(ct);
        probe.Trace.Enqueue("job");
        return probe.Behavior(context, ct);
    }
}

public sealed class OuterFilter(RunnerProbe probe) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        probe.FilterContexts.Enqueue(context);
        probe.FilterInstances.Enqueue(this);
        probe.ResolvedFromJobScope.Enqueue(ReferenceEquals(context.Services.GetRequiredService<OuterFilter>(), this));
        probe.Trace.Enqueue("outer>");
        await next();
        probe.Trace.Enqueue("<outer");
    }
}

public sealed class InnerFilter(RunnerProbe probe) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        probe.Trace.Enqueue("inner>");
        await next();
        probe.Trace.Enqueue("<inner");
    }
}

public sealed class ShortCircuitFilter(RunnerProbe probe) : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        probe.Trace.Enqueue("short");
        return ValueTask.CompletedTask;
    }
}

public sealed class ThrowingFilter : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) =>
        throw new InvalidOperationException("filter failed");
}

public sealed class JobRunnerTests : IAsyncDisposable
{
    private static readonly JobDescriptor Plain = new("runner.job", "q", 3, 0, null);
    private static readonly JobDescriptor Timed = new("runner.timed", "q", 3, 0, null) { TimeoutSeconds = 5 };
    private static readonly JobDescriptor LongTimed = new("runner.long", "q", 3, 0, null) { TimeoutSeconds = int.MaxValue };   // ≈ 68 лет

    private readonly FakeTimeProvider _time = new();
    private readonly MonoClock _clock;
    private readonly RunnerProbe _probe = new();
    private readonly JobRegistry _registry = new();
    private readonly PgBoltServerOptions _options = new PgBoltServerOptions().Queue("q");
    private readonly ServiceCollection _services = new();
    private readonly ListLoggerFactory _logs = new();
    private ServiceProvider? _provider;

    public JobRunnerTests()
    {
        _clock = new MonoClock(_time);
        _services.AddSingleton(_probe);
        _services.AddScoped<OuterFilter>();
        _services.AddScoped<InnerFilter>();
        _services.AddScoped<ShortCircuitFilter>();
        _services.AddScoped<ThrowingFilter>();
        var jobs = new JobsBuilder(_services, _registry);
        jobs.AddExecutor<RunnerArgs, ProbeJob>(Plain, RunnerJson.Default.RunnerArgs);
        jobs.AddExecutor<TimedArgs, ProbeJob>(Timed, RunnerJson.Default.TimedArgs);
        jobs.AddExecutor<LongTimedArgs, ProbeJob>(LongTimed, RunnerJson.Default.LongTimedArgs);
    }

    public async ValueTask DisposeAsync()
    {
        if (_provider is not null)
            await _provider.DisposeAsync();
    }

    private JobRunner Runner(params Type[] filters)
    {
        _provider ??= _services.BuildServiceProvider(validateScopes: true);
        var retry = new RetryDecider(RetryPolicy.Custom((attempt, _) => TimeSpan.FromSeconds(10 * attempt)), NullLogger.Instance);
        return new JobRunner(_registry, _provider.GetRequiredService<IServiceScopeFactory>(), new FilterPipeline(filters), retry,
            _options, _clock, _logs);
    }

    private static Execution Exec(string type = "runner.job", string args = """{"N":1}""", int attempt = 1, int maxAttempts = 3) =>
        new(Leased.Job(type: type, args: args, attempt: attempt, maxAttempts: maxAttempts), "q", 0, ExecutionState.Running);

    private static Task WaitForCancellation(CancellationToken ct) => Task.Delay(Timeout.InfiniteTimeSpan, ct);

    [Fact]
    public async Task Successful_job_is_acked_with_job_context()
    {
        var execution = Exec(attempt: 2, maxAttempts: 5);

        var outcome = await Runner().RunAsync(execution, CancellationToken.None);

        Assert.IsType<JobOutcome.Ack>(outcome);
        var context = Assert.Single(_probe.Contexts);
        Assert.Equal((new JobId(execution.Job.Id), "runner.job", 2, 5, (JobId?)null, (Guid?)null),
            (context.JobId, context.JobName, context.Attempt, context.MaxAttempts, context.ParentId, context.BatchId));
        Assert.Equal(execution.Token, Assert.Single(_probe.Tokens));
    }

    [Fact]
    public async Task Already_cancelled_shutdown_releases_without_lookup_or_call()
    {
        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();

        var unknown = await Runner().RunAsync(Exec(type: "missing.job"), shutdown.Token);
        var known = await Runner().RunAsync(Exec(), shutdown.Token);

        Assert.Equal(new JobOutcome.Release(Ran: false), unknown);   // не NoExecutor: исполнитель не искался
        Assert.Equal(new JobOutcome.Release(Ran: false), known);
        Assert.Empty(_probe.Contexts);
    }

    [Fact]
    public async Task Unknown_type_goes_to_no_executor_retry()
    {
        var outcome = await Runner().RunAsync(Exec(type: "missing.job"), CancellationToken.None);

        var retry = Assert.IsType<JobOutcome.Retry>(outcome);
        Assert.Equal((TimeSpan.FromSeconds(10), JobEventKind.NoExecutor), (retry.Delay, retry.Kind));
        Assert.Contains("missing.job", retry.Message);
    }

    [Fact]
    public async Task Shutdown_between_check_and_call_releases_as_ran()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = (_, ct) =>
        {
            shutdown.Cancel();                    // отмена пришла уже после проверки п. 1
            ct.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        };

        var outcome = await Runner().RunAsync(Exec(), shutdown.Token);

        Assert.Equal(new JobOutcome.Release(Ran: true), outcome);
    }

    [Fact]
    public async Task Job_stopping_on_shutdown_token_is_released_as_ran()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec();

        var run = Runner().RunAsync(execution, shutdown.Token);
        await shutdown.CancelAsync();

        Assert.Equal(new JobOutcome.Release(Ran: true), await run);
        Assert.Equal(CancelReason.Shutdown, execution.FirstReason);
    }

    [Fact]
    public async Task Non_cancellation_exception_after_shutdown_is_retried()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = async (_, ct) =>
        {
            try { await WaitForCancellation(ct); }
            catch (OperationCanceledException) { throw new IOException("cleanup failed"); }
        };

        var run = Runner().RunAsync(Exec(), shutdown.Token);
        await shutdown.CancelAsync();

        Assert.Equal("cleanup failed", Assert.IsType<JobOutcome.Retry>(await run).Message);
    }

    [Fact]
    public async Task Heartbeat_lost_then_shutdown_spends_attempt()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec();

        var run = Runner().RunAsync(execution, shutdown.Token);
        execution.Cancel(CancelReason.HeartbeatLost);
        await shutdown.CancelAsync();

        var retry = Assert.IsType<JobOutcome.Retry>(await run);
        Assert.Equal((JobEventKind.Retry, "cancelled: lease renewal not confirmed"), (retry.Kind, retry.Message));
    }

    [Fact]
    public async Task Heartbeat_lost_on_last_attempt_fails()
    {
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec(attempt: 3, maxAttempts: 3);

        var run = Runner().RunAsync(execution, CancellationToken.None);
        execution.Cancel(CancelReason.HeartbeatLost);

        Assert.Equal(JobEventKind.Failed, Assert.IsType<JobOutcome.Fail>(await run).Kind);
    }

    [Fact]
    public async Task Cancel_requested_then_any_exception_confirms_cancel()
    {
        using var shutdown = new CancellationTokenSource();
        _probe.Behavior = async (_, ct) =>
        {
            try { await WaitForCancellation(ct); }
            catch (OperationCanceledException) { throw new InvalidOperationException("rolled back"); }
        };
        var execution = Exec();

        var run = Runner().RunAsync(execution, shutdown.Token);
        execution.Cancel(CancelReason.CancelRequested);
        await shutdown.CancelAsync();

        Assert.Equal(new JobOutcome.ConfirmCancel("rolled back"), await run);
    }

    [Fact]
    public async Task Descriptor_timeout_cancels_job_and_retries_with_timeout_message()
    {
        _options.DefaultJobTimeout = TimeSpan.FromSeconds(1);   // таймаут задачи важнее
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec(type: "runner.timed");

        var run = Runner().RunAsync(execution, CancellationToken.None);
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(execution.Token.IsCancellationRequested);
        _time.Advance(TimeSpan.FromSeconds(4));

        var retry = Assert.IsType<JobOutcome.Retry>(await run);
        Assert.Equal("timeout after 5 s", retry.Message);
        Assert.Equal(CancelReason.Timeout, execution.FirstReason);
    }

    [Fact]
    public async Task Default_timeout_applies_to_job_without_own_timeout()
    {
        _options.DefaultJobTimeout = TimeSpan.FromMilliseconds(2500);
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);

        var run = Runner().RunAsync(Exec(), CancellationToken.None);
        _time.Advance(TimeSpan.FromMilliseconds(2500));

        Assert.Equal("timeout after 2.5 s", Assert.IsType<JobOutcome.Retry>(await run).Message);
    }

    [Fact]
    public async Task Descriptor_timeout_beyond_timer_limit_is_capped_at_max_job_timeout()
    {
        _probe.Behavior = (_, ct) => WaitForCancellation(ct);
        var execution = Exec(type: "runner.long");

        // int.MaxValue с таймер .NET не принимает: без урезания конструктор токена бросил бы из RunAsync.
        var run = Runner().RunAsync(execution, CancellationToken.None);
        _time.Advance(PgBoltServerOptions.MaxJobTimeout - TimeSpan.FromMilliseconds(1));
        Assert.False(execution.Token.IsCancellationRequested);
        _time.Advance(TimeSpan.FromMilliseconds(1));

        var retry = Assert.IsType<JobOutcome.Retry>(await run);
        Assert.Equal("timeout after 4294967.294 s", retry.Message);   // 2³² − 2 мс
        Assert.Equal(CancelReason.Timeout, execution.FirstReason);
    }

    [Fact]
    public async Task Capped_descriptor_timeout_is_logged_once_per_job_type()
    {
        var runner = Runner();

        await runner.RunAsync(Exec(type: "runner.long"), CancellationToken.None);
        await runner.RunAsync(Exec(type: "runner.long"), CancellationToken.None);
        await runner.RunAsync(Exec(type: "runner.timed"), CancellationToken.None);   // таймаут в пределе — без лога

        var warning = Assert.Single(_logs.At(LogLevel.Warning));
        Assert.Equal("PgBolt.Server", warning.Category);
        Assert.Contains("runner.long", warning.Message);
        Assert.Contains("2147483647", warning.Message);
    }

    [Fact]
    public async Task Job_without_timeout_is_never_cancelled()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _probe.Behavior = (_, _) => gate.Task;
        var execution = Exec();

        var run = Runner().RunAsync(execution, CancellationToken.None);
        _time.Advance(TimeSpan.FromDays(1));

        Assert.False(execution.Token.IsCancellationRequested);
        Assert.False(run.IsCompleted);
        gate.SetResult();
        Assert.IsType<JobOutcome.Ack>(await run);
    }

    [Fact]
    public async Task Synchronous_throw_before_first_await_is_a_regular_failure()
    {
        _probe.Behavior = (_, _) => throw new InvalidOperationException("sync boom");

        var outcome = await Runner().RunAsync(Exec(), CancellationToken.None);

        Assert.Equal("sync boom", Assert.IsType<JobOutcome.Retry>(outcome).Message);
    }

    [Fact]
    public async Task Null_task_from_executor_is_a_regular_failure()
    {
        _probe.Behavior = (_, _) => null!;

        var outcome = await Runner().RunAsync(Exec(), CancellationToken.None);

        var retry = Assert.IsType<JobOutcome.Retry>(outcome);
        Assert.Contains("returned a null Task", retry.Message);
        Assert.Contains(nameof(InvalidOperationException), retry.Exception);
    }

    [Theory]
    [InlineData("{")]
    [InlineData("null")]
    public async Task Broken_arguments_fail_without_retry(string args)
    {
        var outcome = await Runner().RunAsync(Exec(args: args), CancellationToken.None);

        var fail = Assert.IsType<JobOutcome.Fail>(outcome);
        Assert.Equal(JobEventKind.Failed, fail.Kind);
        Assert.Contains(nameof(JobArgumentsException), fail.Exception);
        Assert.Empty(_probe.Contexts);
    }

    [Fact]
    public async Task Job_aborted_exception_fails_without_retry()
    {
        _probe.Behavior = (_, _) => Task.FromException(new JobAbortedException("no point"));

        var fail = Assert.IsType<JobOutcome.Fail>(await Runner().RunAsync(Exec(), CancellationToken.None));

        Assert.Equal((JobEventKind.Failed, "no point"), (fail.Kind, fail.Message));
    }

    [Fact]
    public async Task Filters_run_in_list_order_first_is_outermost()
    {
        var outcome = await Runner(typeof(OuterFilter), typeof(InnerFilter)).RunAsync(Exec(), CancellationToken.None);

        Assert.IsType<JobOutcome.Ack>(outcome);
        Assert.Equal(["outer>", "inner>", "job", "<inner", "<outer"], _probe.Trace);
    }

    [Fact]
    public async Task Filter_not_calling_next_short_circuits_to_success()
    {
        var outcome = await Runner(typeof(ShortCircuitFilter), typeof(InnerFilter)).RunAsync(Exec(), CancellationToken.None);

        Assert.IsType<JobOutcome.Ack>(outcome);
        Assert.Equal(["short"], _probe.Trace);
    }

    [Fact]
    public async Task Filter_exception_is_job_exception()
    {
        var outcome = await Runner(typeof(ThrowingFilter)).RunAsync(Exec(), CancellationToken.None);

        Assert.Equal("filter failed", Assert.IsType<JobOutcome.Retry>(outcome).Message);
        Assert.Empty(_probe.Contexts);
    }

    [Fact]
    public async Task Filters_are_scoped_per_job()
    {
        var runner = Runner(typeof(OuterFilter), typeof(OuterFilter));

        await runner.RunAsync(Exec(), CancellationToken.None);
        await runner.RunAsync(Exec(), CancellationToken.None);

        var instances = _probe.FilterInstances.ToArray();
        Assert.Equal(4, instances.Length);
        Assert.Same(instances[0], instances[1]);
        Assert.Same(instances[2], instances[3]);
        Assert.NotSame(instances[0], instances[2]);
    }

    [Fact]
    public async Task Filter_context_carries_args_utf8_scope_and_token()
    {
        var execution = Exec(args: """{"N":42}""");

        await Runner(typeof(OuterFilter)).RunAsync(execution, CancellationToken.None);

        var context = Assert.Single(_probe.FilterContexts);
        Assert.Equal(Encoding.UTF8.GetBytes("""{"N":42}"""), context.ArgsUtf8.ToArray());
        Assert.Equal(execution.Token, context.CancellationToken);
        Assert.Same(Assert.Single(_probe.Contexts), context.Job);
        Assert.True(Assert.Single(_probe.ResolvedFromJobScope));   // context.Services — scope задачи
    }
}
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~OutcomeClassifierTests|FullyQualifiedName~JobRunnerTests"`
Expected: FAIL при компиляции — CS0246: не найдены `OutcomeClassifier`, `JobRunner`, `FilterPipeline`.

- [x] **Step 4: Публичные типы фильтров**

Файлы созданы в Task 8 с теми же сигнатурами; заменить их содержимое итоговым (добавляются только XML-комментарии). Если содержимое уже совпадает — шаг пропустить.

`src/PgBolt.Server/IJobFilter.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Продолжение конвейера: следующий фильтр или вызов исполнителя (§3.6).</summary>
public delegate ValueTask JobExecutionDelegate();

/// <summary>
/// Фильтр выполнения (§3.6). Резолвится из DI scope задачи (регистрируется как scoped). Может выполнить код до и
/// после <c>next</c>, перехватить или заменить исключение, не вызывать <c>next</c> (задача считается успешной).
/// Исключение фильтра — исключение задачи (§5.2).
/// </summary>
public interface IJobFilter
{
    ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next);
}
```

`src/PgBolt.Server/JobExecutingContext.cs`:

```csharp
namespace PgBolt.Server;

/// <summary>Контекст выполнения задачи для фильтров (§3.6).</summary>
public sealed class JobExecutingContext
{
    internal JobExecutingContext(JobContext job, IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, CancellationToken cancellationToken)
    {
        Job = job;
        Services = services;
        ArgsUtf8 = argsUtf8;
        CancellationToken = cancellationToken;
    }

    public JobContext Job { get; }

    /// <summary>Провайдер DI scope задачи.</summary>
    public IServiceProvider Services { get; }

    /// <summary>Аргументы задачи в UTF-8 (тот же буфер получает invoker).</summary>
    public ReadOnlyMemory<byte> ArgsUtf8 { get; }

    /// <summary>Токен выполнения: shutdown, отмена, таймаут, потеря lease.</summary>
    public CancellationToken CancellationToken { get; }
}
```

- [x] **Step 5: Конвейер фильтров**

`src/PgBolt.Server/Runtime/FilterPipeline.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Конвейер фильтров (§3.6): порядок списка (глобальные уже впереди), первый — внешний. Экземпляры резолвятся из
/// scope задачи; типы зарегистрированы как scoped при регистрации (Task 16), поэтому здесь нет reflection-активации.
/// </summary>
internal sealed class FilterPipeline(IReadOnlyList<Type> filterTypes)
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate terminal) =>
        filterTypes.Count == 0 ? terminal() : InvokeAt(0, context, terminal);

    private ValueTask InvokeAt(int index, JobExecutingContext context, JobExecutionDelegate terminal)
    {
        if (index == filterTypes.Count)
            return terminal();
        var filter = (IJobFilter)context.Services.GetRequiredService(filterTypes[index]);
        return filter.InvokeAsync(context, () => InvokeAt(index + 1, context, terminal));
    }
}
```

- [x] **Step 6: Классификация исхода**

`src/PgBolt.Server/Runtime/OutcomeClassifier.cs`:

```csharp
using System.Globalization;
using System.Text;

namespace PgBolt.Server.Runtime;

/// <summary>Таблица классификации исхода §5.2 (строки 2–6 и NoExecutor; строка 1 — CompletionSink).</summary>
internal static class OutcomeClassifier
{
    private const int MaxTextBytes = 8192;

    public static JobOutcome Classify(Execution execution, Exception? error, RetryDecider retry, TimeSpan? timeout)
    {
        if (error is null)
            return new JobOutcome.Ack();

        var reason = execution.FirstReason;
        if (reason == CancelReason.CancelRequested)
            return new JobOutcome.ConfirmCancel(Truncate(error.Message));
        if (reason == CancelReason.Shutdown && error is OperationCanceledException)
            return new JobOutcome.Release(Ran: true);
        if (error is JobAbortedException or JobArgumentsException)
            return new JobOutcome.Fail(JobEventKind.Failed, Truncate(error.Message), Truncate(error.ToString()));

        var message = reason switch
        {
            CancelReason.Timeout when timeout is { } t =>
                string.Create(CultureInfo.InvariantCulture, $"timeout after {t.TotalSeconds:0.###} s"),
            CancelReason.HeartbeatLost => "cancelled: lease renewal not confirmed",
            _ => error.Message,
        };
        return RetryOrFail(execution, error, retry, message, JobEventKind.Retry, JobEventKind.Failed);
    }

    public static JobOutcome NoExecutor(Execution execution, RetryDecider retry)
    {
        var error = new JobExecutorNotFoundException(execution.Job.Type);
        return RetryOrFail(execution, error, retry, error.Message, JobEventKind.NoExecutor, JobEventKind.NoExecutor);
    }

    /// <summary>
    /// Обрезает до 8 КБ (§5.2) — 8192 байт UTF-8, по границе кодовой точки: суррогатная пара не разрывается, одиночные
    /// суррогаты исходного текста заменяются на U+FFFD (строгий UTF-8 энкодер Npgsql отверг бы их при каждой отправке).
    /// </summary>
    public static string Truncate(string? text)
    {
        if (text is null)
            return string.Empty;
        text = ReplaceLoneSurrogates(text);
        if (text.Length <= MaxTextBytes / 3 || Encoding.UTF8.GetByteCount(text) <= MaxTextBytes)
            return text;
        var bytes = 0;
        var length = 0;
        while (length < text.Length)
        {
            var rune = Rune.GetRuneAt(text, length);   // после замены текст — валидный UTF-16
            if (bytes + rune.Utf8SequenceLength > MaxTextBytes)
                break;
            bytes += rune.Utf8SequenceLength;
            length += rune.Utf16SequenceLength;
        }
        return text[..length];
    }

    /// <summary>Одиночный суррогат → U+FFFD (3 байта UTF-8, 1 символ UTF-16). Без одиночных суррогатов — та же строка.</summary>
    private static string ReplaceLoneSurrogates(string text)
    {
        var i = text.AsSpan().IndexOfAnyInRange('\uD800', '\uDFFF');
        if (i < 0)
            return text;
        StringBuilder? builder = null;
        var copied = 0;
        while (i < text.Length)
        {
            if (!char.IsSurrogate(text[i]))
            {
                i++;
                continue;
            }
            if (Rune.TryGetRuneAt(text, i, out var pair))
            {
                i += pair.Utf16SequenceLength;          // валидная пара — остаётся
                continue;
            }
            builder ??= new StringBuilder(text.Length);
            builder.Append(text, copied, i - copied).Append('\uFFFD');
            i++;
            copied = i;
        }
        return builder is null ? text : builder.Append(text, copied, text.Length - copied).ToString();
    }

    private static JobOutcome RetryOrFail(Execution execution, Exception error, RetryDecider retry, string message,
        JobEventKind retryKind, JobEventKind failKind)
    {
        var job = execution.Job;
        var text = Truncate(message);
        var details = Truncate(error.ToString());
        return job.Attempt < job.MaxAttempts && retry.GetDelay(job.Attempt, job.RetryDelaysSeconds, error) is { } delay
            ? new JobOutcome.Retry(delay, retryKind, text, details)
            : new JobOutcome.Fail(failKind, text, details);
    }
}
```

- [x] **Step 7: `JobRunner`**

`src/PgBolt.Server/Runtime/JobRunner.cs`:

```csharp
using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Jobs;

namespace PgBolt.Server.Runtime;

/// <summary>Выполнение одной задачи (§5.1) и классификация исхода (§5.2). Не бросает.</summary>
internal sealed class JobRunner
{
    private readonly JobRegistry _registry;
    private readonly IServiceScopeFactory _scopes;
    private readonly FilterPipeline _filters;
    private readonly RetryDecider _retry;
    private readonly PgBoltServerOptions _options;
    private readonly MonoClock _clock;
    private readonly ILogger _jobLogger;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, byte> _cappedTimeoutLogged = new(StringComparer.Ordinal);

    public JobRunner(JobRegistry registry, IServiceScopeFactory scopes, FilterPipeline filters, RetryDecider retry,
        PgBoltServerOptions options, MonoClock clock, ILoggerFactory loggers)
    {
        _registry = registry;
        _scopes = scopes;
        _filters = filters;
        _retry = retry;
        _options = options;
        _clock = clock;
        _jobLogger = loggers.CreateLogger("PgBolt.Job");
        _logger = loggers.CreateLogger("PgBolt.Server");
    }

    public async Task<JobOutcome> RunAsync(Execution execution, CancellationToken shutdown)
    {
        // §5.1 п. 1: сигнал остановки пришёл после проверки диспетчера — задача не запускалась.
        if (shutdown.IsCancellationRequested)
            return new JobOutcome.Release(Ran: false);

        if (!_registry.TryGet(execution.Job.Type, out var registration))
            return OutcomeClassifier.NoExecutor(execution, _retry);

        // Уже отменённый shutdown вызывает callback сразу: задача получит отменённый токен (§5.1 п. 4).
        using var onShutdown = shutdown.Register(static s => ((Execution)s!).Cancel(CancelReason.Shutdown), execution);

        var timeout = TimeoutOf(registration.Descriptor);
        using var timeoutSource = timeout is { } t ? new CancellationTokenSource(t, _clock.Time) : null;
        using var onTimeout = timeoutSource is null
            ? default
            : timeoutSource.Token.Register(static s => ((Execution)s!).Cancel(CancelReason.Timeout), execution);

        try
        {
            await ExecuteAsync(execution, registration);
            return OutcomeClassifier.Classify(execution, null, _retry, timeout);
        }
        catch (Exception error)
        {
            return OutcomeClassifier.Classify(execution, error, _retry, timeout);
        }
    }

    /// <summary>
    /// §5.1: TimeoutSeconds задачи > 0, иначе DefaultJobTimeout (он уже ≤ MaxJobTimeout, Task 8). TimeoutSeconds больше
    /// предела таймеров .NET урезается до него: иначе CancellationTokenSource бросил бы ArgumentOutOfRangeException.
    /// Урезание пишется в лог один раз на тип задачи (для [Job] его уже отметил BOLT008 при компиляции).
    /// </summary>
    private TimeSpan? TimeoutOf(JobDescriptor descriptor)
    {
        if (descriptor.TimeoutSeconds <= 0)
            return _options.DefaultJobTimeout;
        var own = TimeSpan.FromSeconds(descriptor.TimeoutSeconds);
        if (own <= PgBoltServerOptions.MaxJobTimeout)
            return own;
        if (_cappedTimeoutLogged.TryAdd(descriptor.Name, 0))
            _logger.LogWarning(
                "PgBolt: job '{JobName}' has TimeoutSeconds = {TimeoutSeconds}, above the .NET timer limit; its timeout is capped at {MaxJobTimeout}.",
                descriptor.Name, descriptor.TimeoutSeconds, PgBoltServerOptions.MaxJobTimeout);
        return PgBoltServerOptions.MaxJobTimeout;
    }

    private async Task ExecuteAsync(Execution execution, JobExecutorRegistration registration)
    {
        var job = execution.Job;
        await using var scope = _scopes.CreateAsyncScope();
        var context = new JobContext(new JobId(job.Id), job.Type, job.Attempt, job.MaxAttempts,
            job.ParentId is { } parent ? new JobId(parent) : null, job.BatchId, _jobLogger);
        var executing = new JobExecutingContext(context, scope.ServiceProvider, Encoding.UTF8.GetBytes(job.ArgsJson), execution.Token);
        await _filters.InvokeAsync(executing, () => InvokeExecutor(registration, executing));
    }

    /// <summary>Синхронное исключение исполнителя и null вместо Task превращаются в обычный сбой (Review Focus п. 3).</summary>
    private static ValueTask InvokeExecutor(JobExecutorRegistration registration, JobExecutingContext context)
    {
        Task? task;
        try
        {
            task = registration.Invoke(context.Services, context.ArgsUtf8, context.Job, context.CancellationToken);
        }
        catch (Exception e)
        {
            return ValueTask.FromException(e);
        }
        return task is null
            ? ValueTask.FromException(new InvalidOperationException(
                $"Executor {registration.JobType.FullName} of job '{registration.Descriptor.Name}' returned a null Task."))
            : new ValueTask(task);
    }
}
```

- [x] **Step 8: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~OutcomeClassifierTests|FullyQualifiedName~JobRunnerTests"`
Expected: PASS — 43 теста (19 + 24).

Контроль, что тест предупреждения ловит регрессию (внести правку, прогнать, убедиться в падении, вернуть код Step 7): в `TimeoutOf` убрать условие `_cappedTimeoutLogged.TryAdd(...)` (писать `Warning` при каждом урезании) → падает `Capped_descriptor_timeout_is_logged_once_per_job_type` (два предупреждения вместо одного).

Run: `dotnet build src/PgBolt.Server`
Expected: `0 предупреждений`.

- [x] **Step 9: Commit**

```bash
git add src/PgBolt.Server/IJobFilter.cs src/PgBolt.Server/JobExecutingContext.cs src/PgBolt.Server/Runtime/FilterPipeline.cs src/PgBolt.Server/Runtime/OutcomeClassifier.cs src/PgBolt.Server/Runtime/JobRunner.cs tests/PgBolt.Server.Tests/Unit/OutcomeClassifierTests.cs tests/PgBolt.Server.Tests/Unit/JobRunnerTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): job runner, execution filters and outcome classification"
```

---

### Task 15: `Dispatcher` — fetch-цикл и пробуждение

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Спека:** §4.2, §4.3, §6.2 («heartbeat потерян»), §6.4 п. 1–2, §10 (адаптивный polling, dequeue).

**Files:**
- Create: `src/PgBolt.Server/Runtime/AsyncAutoResetEvent.cs`, `src/PgBolt.Server/Runtime/IFetchStrategy.cs` (`IFetchStrategy`, `DequeueFetchStrategy`), `src/PgBolt.Server/Runtime/Dispatcher.cs`
- Modify: `src/PgBolt.Server/Runtime/LeaseKeeper.cs` (+ `Unregister` — откат `Register`, нужен только диспетчеру)
- Test: `tests/PgBolt.Server.Tests/Unit/AsyncAutoResetEventTests.cs`, `tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs`

**Interfaces:**
- Consumes: `IWakeupSignal.Subscribe`, `InProcessWakeupSignal` (Task 6); `SlotAllocator.Plan/Take/Release/Available`, `ISharedPoolDistribution`, `StrictOrderDistribution` (Task 10); `LeaseKeeper.Register/RegisterForRelease/HeartbeatLost/Start/StopAsync/HeartbeatOnceAsync` (Task 12); `CompletionSink.Complete/EnqueueRegistered/Start/StopAsync` (Task 13); `IServerStore.DequeueAsync`, `ExecutionTable`, `Execution`, `JobOutcome` (Task 11); `MonoClock`, `PgBoltServerOptions`, `QueueOptions` (Task 8); `FakeServerStore`, `Leased`, `Eventually` (Task 11), `ListLoggerFactory` (Task 13).
- Produces:
  ```csharp
  internal sealed class AsyncAutoResetEvent
  {
      public void Set();
      public Task WaitAsync(TimeSpan timeout, TimeProvider time, CancellationToken ct);   // Timeout.InfiniteTimeSpan допустим; TimeSpan.Zero — без ожидания
  }
  internal interface IFetchStrategy { Task<IReadOnlyList<LeasedJob>> FetchAsync(string queue, int max, CancellationToken ct); }
  internal sealed class DequeueFetchStrategy(IServerStore store, Guid serverId, TimeSpan lease) : IFetchStrategy;
  internal sealed class Dispatcher
  {
      public Dispatcher(PgBoltServerOptions options, SlotAllocator slots, IFetchStrategy fetch, LeaseKeeper keeper,
                        CompletionSink sink, IWakeupSignal wakeup, MonoClock clock, ILogger logger,
                        Action<Execution> startExecution);
      public void Start();
      public void SignalStop();
      public Task Completion { get; }          // до Start — Task.CompletedTask
      public void OnSlotFreed(string queue);
      public void Wake();
      internal Func<double> NextRandom { get; set; }   // дополнение: U[0,1) для jitter ±10 %, тесты ставят () => 0.5
      internal int IdleWaits { get; }                  // дополнение: сколько раз цикл встал на ожидание (таймер уже взведён)
  }
  // Новый член LeaseKeeper (класс — Task 12):
  public bool Unregister(Execution execution);         // откат Register незапущенного выполнения: CAS Running → Removed, table.Remove
  ```

**Поведение (§4.3):**
- Состояние очереди: `Interval = MinPollInterval`, `NextPoll = 0` (опрос сразу), флаги `Signaled`/`SlotFreed` (`int`, сброс `Interlocked.Exchange`). Подписки `IWakeupSignal.Subscribe(q, …)` на все очереди создаются в начале цикла и освобождаются при его завершении.
- Итерация: `stopping` → выход. `keeper.HeartbeatLost` → ожидание только `wake` (страховочный таймаут `HeartbeatInterval`), флаги, `continue`. Пауза после ошибки (`pausedUntil`) → ожидание `wake` или конца паузы, флаги, `continue` (пробуждение во время паузы не снимает её, но флаги сохраняются). **Обход плана** `foreach (var (queue, n) in slots.Plan(isDue))`, `isDue = q => NextPoll(q) ≤ now` (§4.3: `for (queue, n) in allocator.Plan()`): порядок очередей, `n`, исключение очередей с `n = 0` (их `NextPoll` не меняется) и распределение общего пула решает `SlotAllocator` и его стратегия (Task 10) — диспетчер не вычисляет `n` и не обходит `Queues` сам, поэтому веса части 3b его не меняют. Перед каждым dequeue — повторная проверка `stopping`/`HeartbeatLost`.
- Dequeue: `fetchStartedAt = clock.Now` до вызова; токен — `new CancellationTokenSource(HeartbeatInterval, clock.Time)`, **не** связан с остановкой. Ошибка (включая таймаут) → лог, `pausedUntil = now + pause` (1 с, удвоение до 30 с; успешный dequeue сбрасывает), `break`. `validUntil = clock.Add(fetchStartedAt, LeaseDuration)`. `stopping || HeartbeatLost` после dequeue → `sink.EnqueueRegistered(keeper.RegisterForRelease(job, q, validUntil))` для каждой, `break` (слоты не занимаются, `startExecution` не вызывается). Иначе для каждой: `Register` → `FromReserved = slots.Take(q)` → `startExecution` (метод `StartOne`, откат при сбое — ниже).
- Интервалы: `len == n` → `Interval = Min`, `NextPoll = now`; `len == 0` → `Interval = min(Interval × 2, Max)`, `NextPoll = now + Interval × (1 + 0.1 × (2r − 1))`; иначе `Interval = Min`, `NextPoll = now + Min`. Недобор общих слотов переходит к следующим очередям той же итерации: следующая запись плана вычисляется после `Take` по предыдущей (Task 10). «Горячая» очередь (`NextPoll = now`) в той же итерации повторно не опрашивается — план выдаёт очередь один раз; следующая итерация начинается без ожидания (`NextPollDelay` = 0).
- **Неожиданная ошибка итерации** (не ошибка dequeue — та обработана выше): исключение из `startExecution`, `keeper.Register`, `slots.Take`, `sink.EnqueueRegistered`, стратегии плана (в том числе нарушение её контракта, которое ловит `SlotAllocator`) → `LogCritical`, пауза как после ошибки dequeue (1 с, удвоение до 30 с), цикл продолжается. Исключение `IWakeupSignal.Subscribe` (пользовательская реализация) → `LogError`, очередь опрашивается без сигналов; исключение `Dispose` подписки → `LogWarning`. Так `Completion` не завершается с ошибкой: молчаливой остановки выборки при живом heartbeat нет, и остановка сервера (Task 16) не спотыкается о сбойный `Completion`.
- **Откат незапущенного выполнения** (`StartOne`): если после `keeper.Register` бросает `slots.Take` или `startExecution`, диспетчер до передачи исключения в обработчик итерации снимает запись (`keeper.Unregister`: CAS `Running → Removed`, `table.Remove`), затем возвращает взятый слот (`slots.Release`) и пишет `Warning`. Запись снимается первой: `Unregister` не бросает, а `Release` бросает `InvalidOperationException` при ошибке учёта слотов — если бы он шёл первым, такая ошибка пропустила бы `Unregister`, и запись осталась бы `Running`. Исключение `Release` ловится и пишется в лог `Error`, исходное исключение запуска уходит в обработчик итерации без замены. Иначе запись осталась бы `Running` навсегда: исход её никто не сдаст, а heartbeat продлевал бы lease, пока жив процесс, — нарушение критерия §1 «lease … истекает не позже чем через `LeaseDuration`»; слот тоже остался бы занятым. После отката lease истекает не позже `validUntil`, sweeper тратит попытку (`LeaseExpired`) — так даже постоянный сбой запуска заканчивается исчерпанием попыток, а не бесконечным циклом. Остальные задачи того же пакета не регистрируются: их строки — сироты, их забирает sweeper. Контракт `startExecution`: либо запускает выполнение, либо бросает, не запустив его (`PgBoltServer.StartExecution` стартует задачу последним действием, Task 16), поэтому откат не снимает работающее выполнение.
- После прохода: флаги (`Signaled` → `NextPoll = now`, `Interval = Min`; `SlotFreed` → `NextPoll = now`), ожидание `wake` до ближайшего `NextPoll` среди очередей с `Available ≥ 1` (нет таких — без таймаута), снова флаги. **Холостого цикла нет при любой стратегии распределения:** нулевая задержка значит, что есть готовая очередь со свободным слотом, а для неё `SlotAllocator` не даёт стратегии закончить план (Task 10) — следующая итерация либо делает dequeue, либо получает исключение стратегии и паузу 1 → 30 с. Отдельный тест — стратегия, всегда возвращающая `null`. Ожидание — latched `AsyncAutoResetEvent` без токена: освобождение слота не порождает исключений; `Set` во время dequeue или до ожидания сохраняется.
- `IdleWaits` увеличивается после вызова `WaitAsync` (таймер ожидания уже создан); `NextPoll` абсолютный, поэтому `Advance` после `IdleWaits == k` детерминирован.
- В тестах `LeaseKeeper` создаётся с `onRecovered: () => { }` — лишних пробуждений нет, счётчик `IdleWaits` точен; снятие паузы проверяется явным `Wake()` после `HeartbeatOnceAsync` (связка `onRecovered → Wake` — Task 16).

- [x] **Step 1: Написать падающие тесты события**

`tests/PgBolt.Server.Tests/Unit/AsyncAutoResetEventTests.cs`:

```csharp
using Microsoft.Extensions.Time.Testing;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class AsyncAutoResetEventTests
{
    private readonly FakeTimeProvider _time = new();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Set_without_waiter_is_latched_until_next_wait()
    {
        var wake = new AsyncAutoResetEvent();
        wake.Set();

        Assert.True(wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct).IsCompletedSuccessfully);
        Assert.False(wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct).IsCompleted);   // auto-reset: латч израсходован
    }

    [Fact]
    public void Repeated_sets_without_waiter_collapse_into_one()
    {
        var wake = new AsyncAutoResetEvent();
        wake.Set();
        wake.Set();

        Assert.True(wake.WaitAsync(TimeSpan.FromSeconds(1), _time, Ct).IsCompletedSuccessfully);
        Assert.False(wake.WaitAsync(TimeSpan.FromSeconds(1), _time, Ct).IsCompleted);
    }

    [Fact]
    public async Task Set_wakes_waiter()
    {
        var wake = new AsyncAutoResetEvent();
        var wait = wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct);
        Assert.False(wait.IsCompleted);

        wake.Set();

        await wait;
        Assert.False(wake.WaitAsync(TimeSpan.FromSeconds(1), _time, Ct).IsCompleted);   // сигнал не остался в латче
    }

    [Fact]
    public async Task Timeout_completes_without_exception()
    {
        var wake = new AsyncAutoResetEvent();
        var wait = wake.WaitAsync(TimeSpan.FromMilliseconds(100), _time, Ct);

        _time.Advance(TimeSpan.FromMilliseconds(99));
        Assert.False(wait.IsCompleted);
        _time.Advance(TimeSpan.FromMilliseconds(1));

        await wait;
    }

    [Fact]
    public async Task Set_after_timed_out_wait_is_not_lost()
    {
        var wake = new AsyncAutoResetEvent();
        var wait = wake.WaitAsync(TimeSpan.FromMilliseconds(10), _time, Ct);
        _time.Advance(TimeSpan.FromMilliseconds(10));
        await wait;

        wake.Set();

        Assert.True(wake.WaitAsync(Timeout.InfiniteTimeSpan, _time, Ct).IsCompletedSuccessfully);
    }

    [Fact]
    public void Zero_timeout_returns_immediately()
    {
        var wake = new AsyncAutoResetEvent();
        Assert.True(wake.WaitAsync(TimeSpan.Zero, _time, Ct).IsCompletedSuccessfully);
    }
}
```

- [x] **Step 2: Написать падающие тесты диспетчера**

`tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs` (настоящие `SlotAllocator`, `LeaseKeeper`, `CompletionSink`, `InProcessWakeupSignal`, `DequeueFetchStrategy` над `FakeServerStore`; моменты вызовов dequeue пишутся по `FakeTimeProvider`):

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class DispatcherTests : IAsyncDisposable
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);
    private static readonly Guid ServerId = Guid.CreateVersion7();

    private readonly FakeTimeProvider _time = new();
    private readonly MonoClock _clock;
    private readonly FakeServerStore _store = new();
    private readonly ExecutionTable _table = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly ListLoggerFactory _logs = new();
    private readonly ConcurrentQueue<Execution> _started = new();
    private readonly ConcurrentQueue<(string Queue, int Max, DateTimeOffset At)> _dequeues = new();
    private readonly PgBoltServerOptions _options = new()
    {
        Workers = 4,
        LeaseDuration = TimeSpan.FromSeconds(60),
        HeartbeatInterval = TimeSpan.FromSeconds(15),
    };
    private SlotAllocator _slots = null!;
    private LeaseKeeper _keeper = null!;
    private CompletionSink _sink = null!;
    private Dispatcher _dispatcher = null!;

    public DispatcherTests() => _clock = new MonoClock(_time);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Ответ dequeue по (очередь, max, ct); по умолчанию пусто.</summary>
    private Func<string, int, CancellationToken, Task<IReadOnlyList<LeasedJob>>> Respond { get; set; } =
        (_, _, _) => Task.FromResult<IReadOnlyList<LeasedJob>>([]);

    /// <summary>Вызывается вместо запуска JobRunner (по умолчанию выполнение «висит», слот занят).</summary>
    private Action<Execution> OnStart { get; set; } = _ => { };

    /// <summary>Стратегия общего пула для SlotAllocator (по умолчанию — строгий порядок).</summary>
    private ISharedPoolDistribution Distribution { get; set; } = new StrictOrderDistribution();

    private void Start()
    {
        _store.OnDequeue = (queue, max, ct) =>
        {
            _dequeues.Enqueue((queue, max, _time.GetUtcNow()));
            return Respond(queue, max, ct);
        };
        _slots = new SlotAllocator(_options.Workers, _options.Queues, Distribution);
        _keeper = new LeaseKeeper(ServerId, _store, _table, _options, _clock, NullLogger.Instance, onRecovered: () => { });
        _sink = new CompletionSink(ServerId, _store, _table, _options, _clock, NullLogger.Instance);
        _dispatcher = new Dispatcher(_options, _slots, new DequeueFetchStrategy(_store, ServerId, _options.LeaseDuration), _keeper, _sink,
            _wakeup, _clock, _logs.CreateLogger("dispatcher"), e =>
            {
                _started.Enqueue(e);
                OnStart(e);
            })
        {
            NextRandom = () => 0.5,   // jitter ×1.0
        };
        _sink.Start();
        _keeper.Start();
        _dispatcher.Start();
    }

    public async ValueTask DisposeAsync()
    {
        if (_dispatcher is null)
            return;
        _dispatcher.SignalStop();
        await _dispatcher.Completion;
        await _sink.StopAsync(CancellationToken.None);
        await _keeper.StopAsync();
    }

    private static Task<IReadOnlyList<LeasedJob>> Jobs(int count) =>
        Task.FromResult<IReadOnlyList<LeasedJob>>(Enumerable.Range(0, count).Select(_ => Leased.Job()).ToArray());

    private Task Idle(int waits) =>
        Eventually.True(() => _dispatcher.IdleWaits == waits, $"dispatcher enters wait #{waits}");

    private Task Dequeues(int count) =>
        Eventually.True(() => _dequeues.Count == count, $"dequeue #{count} happens");

    [Fact]
    public async Task Queues_are_polled_in_declaration_order_and_unused_shared_slots_move_on()
    {
        _options.Queue("a").Queue("b").Queue("c", q => q.Reserved = 1);
        Respond = (queue, _, _) => queue switch
        {
            "a" => Jobs(1),
            "b" => Jobs(3),
            _ => Jobs(0),
        };
        Start();

        await Idle(1);

        Assert.Equal([("a", 4), ("b", 3), ("c", 1)], _dequeues.Select(d => (d.Queue, d.Max)));
        Assert.Equal(4, _started.Count);
        Assert.Equal((1, 3, 0), (_slots.InUse("a"), _slots.InUse("b"), _slots.InUse("c")));
        Assert.All(_started, e => Assert.Equal(ExecutionState.Running, e.State));
        Assert.Equal(4, _table.Count);
    }

    [Fact]
    public async Task Full_batch_is_followed_by_immediate_poll()
    {
        _options.Workers = 10;
        _options.Queue("q");
        var calls = 0;
        Respond = (_, max, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(max) : Jobs(0);
        OnStart = e => _slots.Release(e.Queue, e.FromReserved);   // слоты свободны сразу
        Start();

        await Idle(2);

        var polls = _dequeues.ToArray();
        Assert.Equal(2, polls.Length);
        Assert.Equal(polls[0].At, polls[1].At);
        Assert.Equal(10, _started.Count);
    }

    [Fact]
    public async Task Empty_queue_doubles_interval_up_to_max()
    {
        _options.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromMilliseconds(50);
            q.MaxPollInterval = TimeSpan.FromMilliseconds(400);
        });
        Start();

        int[] gapsMs = [100, 200, 400, 400];
        for (var k = 1; k <= gapsMs.Length; k++)
        {
            await Idle(k);
            _time.Advance((gapsMs[k - 1] - 1) * Ms);
            Assert.Equal(k, _dequeues.Count);
            _time.Advance(Ms);
            await Dequeues(k + 1);
        }
    }

    [Fact]
    public async Task Wakeup_signal_resets_next_poll_and_interval()
    {
        _options.Queue("q", q => q.MinPollInterval = TimeSpan.FromMilliseconds(50));
        Start();
        await Idle(1);
        _time.Advance(100 * Ms);
        await Idle(2);                         // второй пустой dequeue: интервал 200 мс

        _wakeup.Signal("q");
        await Dequeues(3);                     // без сдвига времени
        await Idle(3);

        _time.Advance(99 * Ms);                // интервал снова от Min: 100 мс, а не 400
        Assert.Equal(3, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(4);
    }

    [Fact]
    public async Task Signal_during_dequeue_is_not_lost()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = (queue, _, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                _wakeup.Signal(queue);         // enqueue пришёл, пока шёл dequeue
            return Jobs(0);
        };
        Start();

        await Dequeues(2);

        var polls = _dequeues.ToArray();
        Assert.Equal(polls[0].At, polls[1].At);
    }

    [Fact]
    public async Task Freed_slot_wakes_dispatcher()
    {
        _options.Workers = 1;
        _options.Queue("q");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(1) : Jobs(0);
        Start();
        await Idle(1);                         // слотов нет — ожидание без таймаута

        var execution = Assert.Single(_started);
        _slots.Release(execution.Queue, execution.FromReserved);
        _dispatcher.OnSlotFreed(execution.Queue);

        await Dequeues(2);
        Assert.Equal(_dequeues.First().At, _dequeues.Last().At);
    }

    [Fact]
    public async Task Queue_without_available_slots_is_not_polled()
    {
        _options.Workers = 2;
        _options.Queue("a", q => q.MaxConcurrency = 1).Queue("b");
        Respond = (queue, max, _) => queue == "a" ? Jobs(max) : Jobs(0);
        Start();
        await Idle(1);                         // a взяла полный пакет, слотов у неё нет; ждём только b

        _time.Advance(100 * Ms);
        await Dequeues(3);
        await Idle(2);                         // дождаться, пока взведено следующее ожидание, до сдвига времени
        _time.Advance(200 * Ms);
        await Dequeues(4);

        Assert.Equal(["a", "b", "b", "b"], _dequeues.Select(d => d.Queue));
    }

    [Fact]
    public async Task Hung_dequeue_times_out_after_heartbeat_interval_and_pauses_fetching()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = async (_, _, ct) =>
        {
            switch (Interlocked.Increment(ref calls))
            {
                case 1:
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);   // зависшее соединение
                    return [];
                case 2:
                    throw new IOException("connection refused");
                default:
                    return [];
            }
        };
        Start();
        await Dequeues(1);

        _time.Advance(TimeSpan.FromSeconds(15) - Ms);
        Assert.Equal(0, _dispatcher.IdleWaits);
        _time.Advance(Ms);                     // таймаут dequeue = HeartbeatInterval
        await Idle(1);

        _time.Advance(999 * Ms);               // пауза 1 с
        Assert.Single(_dequeues);
        _time.Advance(Ms);
        await Dequeues(2);
        await Idle(2);

        _time.Advance(1999 * Ms);              // вторая ошибка подряд — пауза 2 с
        Assert.Equal(2, _dequeues.Count);
        _time.Advance(Ms);
        await Dequeues(3);
    }

    [Fact]
    public async Task Stop_does_not_cancel_in_flight_dequeue_and_taken_jobs_are_released_not_run()
    {
        _options.Queue("q");
        var gate = new TaskCompletionSource<IReadOnlyList<LeasedJob>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var dequeueToken = CancellationToken.None;
        Respond = (_, _, ct) =>
        {
            dequeueToken = ct;
            return gate.Task;
        };
        Start();
        await Dequeues(1);

        _dispatcher.SignalStop();
        Assert.False(dequeueToken.IsCancellationRequested);
        Assert.False(_dispatcher.Completion.IsCompleted);

        gate.SetResult(await Jobs(2));
        await _dispatcher.Completion;

        await Eventually.True(() => _store.CallsOf("Release").Count() == 2, "taken jobs are released");
        Assert.All(_store.CallsOf("Release"), c => Assert.Equal((object?)false, c.Detail));
        Assert.Empty(_started);
        Assert.Equal(0, _slots.InUse("q"));
        await Eventually.True(() => _table.Count == 0, "released executions leave the table");
    }

    [Fact]
    public async Task Jobs_returned_after_heartbeat_loss_are_released_not_run()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromMilliseconds(1500);
            q.MaxPollInterval = TimeSpan.FromMilliseconds(1500);
        });
        _store.OnHeartbeat = (_, _) => throw new IOException("db unreachable");
        var gate = new TaskCompletionSource<IReadOnlyList<LeasedJob>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(0) : gate.Task;
        Start();
        await Idle(1);

        _time.Advance(1500 * Ms);              // dequeue №2 начат до входа в «heartbeat потерян»
        await Dequeues(2);
        _time.Advance(500 * Ms);               // lastSuccess(старт) + L − H = 2 с
        Assert.True(_keeper.HeartbeatLost);

        gate.SetResult(await Jobs(1));

        await Eventually.True(() => _store.CallsOf("Release").Any(), "job is released");
        Assert.Equal((object?)false, Assert.Single(_store.CallsOf("Release")).Detail);
        Assert.Empty(_started);
    }

    [Fact]
    public async Task Heartbeat_loss_pauses_new_dequeues_until_recovery_and_wake()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.Queue("q");
        _store.OnHeartbeat = (_, _) => throw new IOException("db unreachable");
        Start();
        await Idle(1);

        _time.Advance(TimeSpan.FromSeconds(2));   // срок опроса наступил, но heartbeat потерян
        await Idle(2);
        _time.Advance(TimeSpan.FromSeconds(1));   // страховочный таймаут HeartbeatInterval — всё ещё потерян
        await Idle(3);
        Assert.Single(_dequeues);

        _store.OnHeartbeat = (leases, _) => Task.FromResult<IReadOnlyList<HeartbeatLease>>(
            leases.Select(l => new HeartbeatLease(l.JobId, l.LeaseId, false)).ToArray());
        await _keeper.HeartbeatOnceAsync(Ct);
        Assert.False(_keeper.HeartbeatLost);
        _dispatcher.Wake();

        await Dequeues(2);
    }

    [Fact]
    public async Task Outcomes_are_still_sent_while_heartbeat_is_lost()
    {
        _options.LeaseDuration = TimeSpan.FromSeconds(3);
        _options.HeartbeatInterval = TimeSpan.FromSeconds(1);
        _options.Queue("q");
        _store.OnHeartbeat = (_, _) => throw new IOException("db unreachable");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(1) : Jobs(0);
        Start();
        await Idle(1);
        var execution = Assert.Single(_started);

        _time.Advance(TimeSpan.FromSeconds(2));   // рубеж выполнения (validUntil − H) и вход в «heartbeat потерян»
        Assert.True(_keeper.HeartbeatLost);
        await Eventually.True(() => execution.FirstReason == CancelReason.HeartbeatLost, "execution is cancelled at its deadline");

        // Runner сдаёт исход остановленной задачи (§5.2: HeartbeatLost → retry): sink отправляет его и во время сбоя.
        Assert.True(_sink.Complete(execution, new JobOutcome.Retry(TimeSpan.Zero, JobEventKind.Retry, "cancelled: lease renewal not confirmed", null)));

        await Eventually.True(() => _table.Count == 0, "outcome is sent despite heartbeat loss");
        Assert.Equal(execution.Job.Id, Assert.Single(_store.CallsOf("Retry")).JobId!.Value);
        Assert.Single(_dequeues);                 // выборка при этом на паузе
    }

    [Fact]
    public async Task Unexpected_iteration_failure_is_logged_and_fetching_resumes_after_pause()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(1) : Jobs(0);
        var starts = 0;
        OnStart = _ =>
        {
            if (Interlocked.Increment(ref starts) == 1)
                throw new InvalidOperationException("start failed");
        };
        Start();
        await Idle(1);                            // пауза 1 с после неожиданной ошибки

        Assert.Contains(_logs.At(LogLevel.Critical), e => e.Exception is InvalidOperationException);
        Assert.False(_dispatcher.Completion.IsCompleted);
        _time.Advance(999 * Ms);
        Assert.Single(_dequeues);
        _time.Advance(Ms);
        await Dequeues(2);
    }

    [Fact]
    public async Task Failed_start_rolls_back_registration_and_slot()
    {
        _options.Queue("q");
        var calls = 0;
        Respond = (_, _, _) => Interlocked.Increment(ref calls) == 1 ? Jobs(2) : Jobs(0);
        OnStart = _ => throw new InvalidOperationException("start failed");
        Start();
        await Idle(1);                            // откат, затем пауза после неожиданной ошибки

        var failed = Assert.Single(_started);     // вторая задача пакета не регистрировалась (сирота — её заберёт sweeper)
        Assert.Equal(ExecutionState.Removed, failed.State);
        Assert.Equal(0, _table.Count);            // не Running в таблице: heartbeat не продлевает её lease
        Assert.Equal(0, _slots.InUse("q"));       // слот возвращён
        Assert.Empty(_logs.At(LogLevel.Error));   // без ошибки учёта слотов
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains(failed.Job.Id.ToString()));

        await _keeper.HeartbeatOnceAsync(Ct);
        Assert.All(_store.CallsOf("Heartbeat"), c => Assert.Empty((LeaseRef[])c.Detail!));
    }

    [Fact]
    public async Task Distribution_declining_a_due_queue_pauses_fetching_instead_of_spinning()
    {
        _options.Queue("q");
        Distribution = new DecliningDistribution();
        Start();
        await Idle(1);                            // план пуст при готовой очереди со свободными слотами — ошибка, пауза 1 с

        Assert.Contains(_logs.At(LogLevel.Critical), e => e.Exception is InvalidOperationException);
        _time.Advance(999 * Ms);
        Assert.Equal(1, _dispatcher.IdleWaits);   // без холостого цикла: до конца паузы новых ожиданий нет
        _time.Advance(Ms);
        await Idle(2);                            // снова отказ стратегии — пауза 2 с

        _time.Advance(1999 * Ms);
        Assert.Equal(2, _dispatcher.IdleWaits);   // и во второй паузе ни новых ожиданий, ни выборки
        Assert.Empty(_dequeues);
        _time.Advance(Ms);
        await Idle(3);                            // третий отказ — пауза 4 с

        // Явная проверка «нет холостого цикла»: цикл стоит на паузе, и за окно реального времени ни счётчик ожиданий,
        // ни число выборок, ни число отказов стратегии (Critical на каждую итерацию) не растут. Холостой цикл
        // (IdleAsync(0) на каждой итерации) увеличивал бы IdleWaits и Critical без Advance.
        await Task.Delay(50, Ct);
        Assert.Equal(3, _dispatcher.IdleWaits);
        Assert.Equal(3, _logs.At(LogLevel.Critical).Count());
        Assert.Empty(_dequeues);
        Assert.False(_dispatcher.Completion.IsCompleted);
    }

    /// <summary>Стратегия, всегда заканчивающая план (нарушает контракт ISharedPoolDistribution).</summary>
    private sealed class DecliningDistribution : ISharedPoolDistribution
    {
        public (int Queue, int N)? Next(int queueCount, Func<int, bool> candidate, Func<int, int> available) => null;
    }
}
```

- [x] **Step 3: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~AsyncAutoResetEventTests|FullyQualifiedName~DispatcherTests"`
Expected: FAIL при компиляции — CS0246: не найдены `AsyncAutoResetEvent`, `Dispatcher`, `DequeueFetchStrategy`.

- [x] **Step 4: Реализовать событие**

`src/PgBolt.Server/Runtime/AsyncAutoResetEvent.cs`:

```csharp
namespace PgBolt.Server.Runtime;

/// <summary>
/// Latched auto-reset событие (§4.3): Set без ожидающего сохраняется до следующего WaitAsync; Set будит ровно одного
/// ожидающего. WaitAsync не бросает на таймаут.
/// </summary>
internal sealed class AsyncAutoResetEvent
{
    private readonly Lock _lock = new();
    private readonly Queue<TaskCompletionSource> _waiters = new();
    private bool _signaled;

    public void Set()
    {
        TaskCompletionSource? waiter = null;
        lock (_lock)
        {
            while (_waiters.Count > 0 && waiter is null)
            {
                var candidate = _waiters.Dequeue();
                if (!candidate.Task.IsCompleted)
                    waiter = candidate;
            }
            if (waiter is null)
            {
                _signaled = true;
                return;
            }
        }
        waiter.TrySetResult();
    }

    public async Task WaitAsync(TimeSpan timeout, TimeProvider time, CancellationToken ct)
    {
        TaskCompletionSource waiter;
        lock (_lock)
        {
            if (_signaled)
            {
                _signaled = false;
                return;
            }
            if (timeout == TimeSpan.Zero)
                return;
            waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Enqueue(waiter);
        }
        try
        {
            await waiter.Task.WaitAsync(timeout, time, ct);
        }
        catch (TimeoutException)
        {
        }
        finally
        {
            // Снятый по таймауту/отмене ожидающий помечается завершённым: Set пропустит его и не потеряет сигнал.
            waiter.TrySetCanceled(CancellationToken.None);
        }
    }
}
```

- [x] **Step 5: Стратегия выборки**

`src/PgBolt.Server/Runtime/IFetchStrategy.cs`:

```csharp
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Стратегия выборки задач очереди (§4.1). Точка расширения части 3b (кластерный лимит, веса).</summary>
internal interface IFetchStrategy
{
    Task<IReadOnlyList<LeasedJob>> FetchAsync(string queue, int max, CancellationToken ct);
}

/// <summary>Выборка поверх PgBoltStore.DequeueAsync (core §5.2).</summary>
internal sealed class DequeueFetchStrategy(IServerStore store, Guid serverId, TimeSpan lease) : IFetchStrategy
{
    public Task<IReadOnlyList<LeasedJob>> FetchAsync(string queue, int max, CancellationToken ct) =>
        store.DequeueAsync(queue, max, serverId, lease, ct);
}
```

- [x] **Step 6: Откат регистрации и диспетчер**

В `src/PgBolt.Server/Runtime/LeaseKeeper.cs` (Task 12) сразу после метода `RegisterForRelease` вставить:

```csharp

    /// <summary>
    /// Откат <see cref="Register"/> для выполнения, которое диспетчер не смог запустить: Running → Removed и удаление из
    /// таблицы — heartbeat его больше не продлевает, lease истекает не позже validUntil (критерий §1).
    /// </summary>
    /// <returns>false — запись уже не Running (откатывать нечего).</returns>
    public bool Unregister(Execution execution)
    {
        if (!execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
            return false;
        _table.Remove(execution);
        return true;
    }
```

Таймер сроков не перевзводится: при срабатывании он смотрит `table.Snapshot()`, где записи уже нет, и взводится на следующий рубеж.

`src/PgBolt.Server/Runtime/Dispatcher.cs`:

```csharp
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Fetch-цикл виртуального сервера (§4.3): обход плана SlotAllocator (порядок и доли общего пула — его стратегия),
/// адаптивный polling, пробуждение по latched событию (слот освобождён, wakeup-сигнал, остановка, снятие паузы
/// heartbeat). Dequeue ограничен собственным таймаутом HeartbeatInterval и остановкой не отменяется.
/// Неожиданная ошибка итерации логируется и ставит выборку на паузу; цикл не падает.
/// </summary>
internal sealed class Dispatcher
{
    private static readonly TimeSpan MinErrorPause = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxErrorPause = TimeSpan.FromSeconds(30);
    private const double PollJitter = 0.1;

    private readonly PgBoltServerOptions _options;
    private readonly SlotAllocator _slots;
    private readonly IFetchStrategy _fetch;
    private readonly LeaseKeeper _keeper;
    private readonly CompletionSink _sink;
    private readonly IWakeupSignal _wakeup;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly Action<Execution> _startExecution;
    private readonly AsyncAutoResetEvent _wake = new();
    private readonly QueueState[] _queues;
    private readonly Dictionary<string, QueueState> _byName;
    private readonly Func<string, bool> _isDue;
    private volatile bool _stopping;
    private int _idleWaits;

    public Dispatcher(PgBoltServerOptions options, SlotAllocator slots, IFetchStrategy fetch, LeaseKeeper keeper,
        CompletionSink sink, IWakeupSignal wakeup, MonoClock clock, ILogger logger, Action<Execution> startExecution)
    {
        _options = options;
        _slots = slots;
        _fetch = fetch;
        _keeper = keeper;
        _sink = sink;
        _wakeup = wakeup;
        _clock = clock;
        _logger = logger;
        _startExecution = startExecution;
        _queues = [.. options.Queues.Select(q => new QueueState(q))];
        _byName = _queues.ToDictionary(q => q.Options.Name, StringComparer.Ordinal);
        _isDue = queue => _byName[queue].NextPoll <= _clock.Now;   // вызывается SlotAllocator.Plan под его lock
    }

    public Task Completion { get; private set; } = Task.CompletedTask;

    /// <summary>Источник U[0, 1) для jitter интервала polling (±10 %); тесты подменяют.</summary>
    internal Func<double> NextRandom { get; set; } = Random.Shared.NextDouble;

    /// <summary>Для тестов: сколько раз цикл встал на ожидание (таймер ожидания уже взведён).</summary>
    internal int IdleWaits => Volatile.Read(ref _idleWaits);

    public void Start() => Completion = Task.Run(RunAsync);

    public void SignalStop()
    {
        _stopping = true;
        _wake.Set();
    }

    public void OnSlotFreed(string queue)
    {
        if (_byName.TryGetValue(queue, out var q))
            Volatile.Write(ref q.SlotFreed, 1);
        _wake.Set();
    }

    public void Wake() => _wake.Set();

    private async Task RunAsync()
    {
        var subscriptions = Subscribe();
        try
        {
            var errorPause = TimeSpan.Zero;
            long pausedUntil = 0;
            while (!_stopping)
            {
                try
                {
                    if (_keeper.HeartbeatLost)
                    {
                        // Пауза выборки до снятия состояния (onRecovered → Wake); таймаут — страховка.
                        await IdleAsync(_options.HeartbeatInterval);
                        ApplyFlags();
                        continue;
                    }
                    if (pausedUntil != 0 && _clock.Now < pausedUntil)
                    {
                        await IdleAsync(_clock.Until(pausedUntil));
                        ApplyFlags();
                        continue;
                    }

                    // §4.3: for (queue, n) in allocator.Plan() — следующая запись вычисляется после Take по предыдущей.
                    foreach (var (name, n) in _slots.Plan(_isDue))
                    {
                        if (_stopping || _keeper.HeartbeatLost)
                            break;
                        var q = _byName[name];

                        var fetchStartedAt = _clock.Now;
                        IReadOnlyList<LeasedJob> jobs;
                        try
                        {
                            using var timeout = new CancellationTokenSource(_options.HeartbeatInterval, _clock.Time);
                            jobs = await _fetch.FetchAsync(name, n, timeout.Token);
                        }
                        catch (Exception e)
                        {
                            errorPause = NextErrorPause(errorPause);
                            pausedUntil = _clock.Add(_clock.Now, errorPause);
                            _logger.LogError(e, "PgBolt: dequeue from queue {Queue} failed; fetching paused for {Pause}.",
                                name, errorPause);
                            break;
                        }
                        errorPause = TimeSpan.Zero;

                        var validUntil = _clock.Add(fetchStartedAt, _options.LeaseDuration);
                        if (_stopping || _keeper.HeartbeatLost)
                        {
                            // Взяты после сигнала остановки или во время сбоя heartbeat: не запускаются (§4.3, §7.1).
                            foreach (var job in jobs)
                                _sink.EnqueueRegistered(_keeper.RegisterForRelease(job, name, validUntil));
                            break;
                        }
                        foreach (var job in jobs)
                            StartOne(job, name, validUntil);

                        var now = _clock.Now;
                        var min = q.Options.MinPollInterval;
                        if (jobs.Count >= n)
                        {
                            q.Interval = min;
                            q.NextPoll = now;
                        }
                        else if (jobs.Count == 0)
                        {
                            q.Interval = Min(q.Interval * 2, q.Options.MaxPollInterval);
                            q.NextPoll = _clock.Add(now, q.Interval * (1 + PollJitter * (2 * NextRandom() - 1)));
                        }
                        else
                        {
                            q.Interval = min;
                            q.NextPoll = _clock.Add(now, min);
                        }
                    }

                    if (_stopping)
                        break;
                    if (pausedUntil != 0 && _clock.Now < pausedUntil)
                        continue;                  // пауза после ошибки — ожидание в начале цикла
                    ApplyFlags();
                    await IdleAsync(NextPollDelay());
                    ApplyFlags();
                }
                catch (Exception e)
                {
                    // Не ошибка dequeue (та обработана выше): сбой запуска выполнения (уже откачен в StartOne), регистрации,
                    // стратегии плана.
                    // Цикл не падает — иначе выборка молча остановилась бы при живом heartbeat.
                    errorPause = NextErrorPause(errorPause);
                    pausedUntil = _clock.Add(_clock.Now, errorPause);
                    _logger.LogCritical(e, "PgBolt: dispatcher iteration failed unexpectedly; fetching paused for {Pause}.", errorPause);
                }
            }
        }
        finally
        {
            foreach (var subscription in subscriptions)
            {
                try
                {
                    subscription.Dispose();
                }
                catch (Exception e)
                {
                    _logger.LogWarning(e, "PgBolt: disposing a wakeup subscription failed.");
                }
            }
        }
    }

    /// <summary>Подписки IWakeupSignal на все очереди; сбой пользовательской реализации — очередь без сигналов.</summary>
    private List<IDisposable> Subscribe()
    {
        var subscriptions = new List<IDisposable>(_queues.Length);
        foreach (var q in _queues)
        {
            try
            {
                subscriptions.Add(_wakeup.Subscribe(q.Options.Name, () =>
                {
                    Volatile.Write(ref q.Signaled, 1);
                    _wake.Set();
                }));
            }
            catch (Exception e)
            {
                _logger.LogError(e, "PgBolt: wakeup subscription for queue {Queue} failed; the queue is polled without wakeup signals.",
                    q.Options.Name);
            }
        }
        return subscriptions;
    }

    /// <summary>
    /// Register → Take → startExecution одной задачи. Сбой после Register откатывается до передачи исключения в обработчик
    /// итерации: сначала снимается запись (не остаётся Running, heartbeat её не продлевает — критерий §1), затем
    /// возвращается слот. Порядок важен: Unregister не бросает, а Release бросает при ошибке учёта слотов — такая ошибка
    /// логируется и не мешает снятию записи и передаче исходного исключения.
    /// startExecution по контракту либо запускает выполнение, либо бросает, не запустив его.
    /// </summary>
    private void StartOne(LeasedJob job, string queue, long validUntil)
    {
        var execution = _keeper.Register(job, queue, validUntil);
        var slotTaken = false;
        try
        {
            execution.FromReserved = _slots.Take(queue);
            slotTaken = true;
            _startExecution(execution);
        }
        catch
        {
            _keeper.Unregister(execution);
            if (slotTaken)
            {
                try
                {
                    _slots.Release(queue, execution.FromReserved);
                }
                catch (Exception releaseError)
                {
                    _logger.LogError(releaseError, "PgBolt: slot of queue {Queue} was not returned after the failed start of job {JobId}.",
                        queue, job.Id);
                }
            }
            _logger.LogWarning("PgBolt: job {JobId} of queue {Queue} was not started; its registration is rolled back, the lease will expire.",
                job.Id, queue);
            throw;
        }
    }

    /// <summary>
    /// До ближайшего nextPoll среди очередей со свободным слотом; таких нет — до пробуждения. Ноль не ведёт к холостому
    /// циклу при любой стратегии: для готовой очереди со свободным слотом SlotAllocator не даёт закончить план (Task 10).
    /// </summary>
    private TimeSpan NextPollDelay()
    {
        var delay = Timeout.InfiniteTimeSpan;
        foreach (var q in _queues)
        {
            if (_slots.Available(q.Options.Name) < 1)
                continue;
            var until = _clock.Until(q.NextPoll);
            if (delay == Timeout.InfiniteTimeSpan || until < delay)
                delay = until;
        }
        return delay;
    }

    private void ApplyFlags()
    {
        var now = _clock.Now;
        foreach (var q in _queues)
        {
            if (Interlocked.Exchange(ref q.Signaled, 0) == 1)
            {
                q.NextPoll = now;
                q.Interval = q.Options.MinPollInterval;
            }
            if (Interlocked.Exchange(ref q.SlotFreed, 0) == 1)
                q.NextPoll = now;
        }
    }

    private async Task IdleAsync(TimeSpan timeout)
    {
        var wait = _wake.WaitAsync(timeout, _clock.Time, CancellationToken.None);
        Interlocked.Increment(ref _idleWaits);
        await wait;
    }

    private static TimeSpan NextErrorPause(TimeSpan current) =>
        current == TimeSpan.Zero ? MinErrorPause : Min(current * 2, MaxErrorPause);

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;

    private sealed class QueueState(QueueOptions options)
    {
        public QueueOptions Options { get; } = options;
        public TimeSpan Interval = options.MinPollInterval;
        public long NextPoll;
        public int Signaled;
        public int SlotFreed;
    }
}
```

- [x] **Step 7: Прогнать тесты**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~AsyncAutoResetEventTests|FullyQualifiedName~DispatcherTests"`
Expected: PASS — 21 тест (6 + 15). Прогнать 3 раза подряд: результат стабилен.

Контроль, что тест отката ловит регрессию (внести правку, прогнать, убедиться в падении, вернуть код Step 6 — файлы ещё не закоммичены): в `StartOne` убрать `try`/`catch` (оставить Register → Take → start) → падает `Failed_start_rolls_back_registration_and_slot`.

Run: `dotnet build src/PgBolt.Server`
Expected: `0 предупреждений`.

- [x] **Step 8: Commit**

```bash
git add src/PgBolt.Server/Runtime/AsyncAutoResetEvent.cs src/PgBolt.Server/Runtime/IFetchStrategy.cs src/PgBolt.Server/Runtime/Dispatcher.cs src/PgBolt.Server/Runtime/LeaseKeeper.cs tests/PgBolt.Server.Tests/Unit/AsyncAutoResetEventTests.cs tests/PgBolt.Server.Tests/Unit/DispatcherTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): dispatcher fetch loop with adaptive polling and latched wakeup"
```

---

### Task 16: Виртуальный сервер, координатор, обслуживание и DI

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Спека:** §3.1, §3.6 (регистрация фильтров), §4.1, §4.4, §5.1 (предупреждение о задачах, игнорирующих отмену), §6.3, §6.4, §10 (координатор), Review Focus п. 1.

**Files:**
- Create: `src/PgBolt.Server/Runtime/ServerDefinition.cs` (`ServerDefinition`, `GlobalJobFilter`), `src/PgBolt.Server/Runtime/ProcessingMaintenance.cs`, `src/PgBolt.Server/Runtime/PgBoltServer.cs`, `src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`, `src/PgBolt.Server/PgBoltServerServiceCollectionExtensions.cs`
- Modify: `src/PgBolt.Server/PgBoltServerOptions.cs` (регистраторы фильтров), `src/PgBolt.Server/PgBolt.Server.csproj` и `Directory.Packages.props` (пакет `Microsoft.Extensions.Hosting`, если его нет)
- Test: `tests/PgBolt.Server.Tests/Unit/ServerRegistrationTests.cs`, `tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs`, `tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs`

**Interfaces:**
- Consumes: всё из Tasks 8–15; `AddPgBolt`, `PgBoltOptions`, `PgBoltStore`, `JobRegistry.ToJobTypeRecords`, `IWakeupSignal` (core); `PgBoltInitializer.Ready` (Task 7); `HostOptions` (`Microsoft.Extensions.Hosting`).
- Produces (контракт) + дополнения:
  ```csharp
  internal sealed record ServerDefinition(string Name, PgBoltServerOptions Options);
  internal sealed record GlobalJobFilter(Type FilterType);                           // дополнение: DI-описание глобального фильтра

  public sealed class PgBoltServerOptions
  {   // дополнение к Task 8: регистратор scoped-типа на каждый AddFilter<T>, порядок как у FilterTypes
      internal IReadOnlyList<Action<IServiceCollection>> FilterRegistrations { get; }
  }

  internal sealed class PgBoltServer
  {
      public PgBoltServer(ServerDefinition definition, IServerStore store, JobRegistry registry, IServiceScopeFactory scopes,
                          IReadOnlyList<Type> globalFilters, IWakeupSignal wakeup, MonoClock clock, ILoggerFactory loggers);
      public Guid Id { get; }  public string Name { get; }  public PgBoltServerOptions Options { get; }
      public Task RegisterAsync(CancellationToken ct);          // §4.4 п. 3–4
      public void Start();                                      // §4.4 п. 6
      public void BeginStop();
      public Task WaitExecutionsAsync(CancellationToken ct);    // отмена ct и сбойный dispatcher.Completion — лог, без исключения
      public Task FlushAndStopAsync(CancellationToken ct);      // отмена ct — sink/keeper/watchdog остановлены, RemoveServer пропущен
      internal int CancelledExecutions { get; }                 // дополнение: для тестов — выполнения с записанным моментом отмены
  }

  internal sealed class ProcessingMaintenance(IServerStore store, IWakeupSignal wakeup, PgBoltProcessingOptions options,
                                              MonoClock clock, ILogger logger)
  {
      internal const int BatchSize = 1000;
      public void Start();
      public Task StopAsync();
      internal Task<int> PromoteOnceAsync(CancellationToken ct);
      internal Task<int> SweepOnceAsync(CancellationToken ct);
      internal Func<double> NextRandom { get; set; }   // дополнение: jitter ±20 %
      internal int IdleWaits { get; }                  // дополнение: для тестов
  }

  internal sealed class PgBoltProcessingService : IHostedService
  {
      public PgBoltProcessingService(IServiceProvider services);
      internal PgBoltProcessingService(IServiceProvider services, Func<CancellationToken, Task> waitReady);   // дополнение: подмена Ready в unit-тестах
      internal IReadOnlyList<PgBoltServer> Servers { get; }
      internal ProcessingMaintenance? Maintenance { get; }    // дополнение: null — обслуживание не запускалось
  }

  public static class PgBoltServerServiceCollectionExtensions   // namespace Microsoft.Extensions.DependencyInjection
  {
      public static IServiceCollection AddPgBoltServer(this IServiceCollection services, string name, Action<PgBoltServerOptions> configure);
      public static IServiceCollection AddPgBoltJobFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(this IServiceCollection services) where T : class, IJobFilter;
      public static IServiceCollection ConfigurePgBoltProcessing(this IServiceCollection services, Action<PgBoltProcessingOptions> configure);
  }
  ```

**Решения:**
- **Регистрация фильтров под AOT.** `services.TryAdd(ServiceDescriptor.Scoped(type, type))` с `Type` из `IReadOnlyList<Type>` даёт `IL2072` (проверено: trim-анализ требует аннотацию `PublicConstructors` у `implementationType`, а элементы списка её не несут). Поэтому `AddFilter<T>` дополнительно сохраняет регистратор `static s => s.TryAddScoped<T>()` — в generic-методе `T` аннотирован, анализ чистый, suppress не нужен. `FilterTypes` (`IReadOnlyList<Type>`) остаётся как есть и используется `FilterPipeline` для резолва. Это **дополнение к контракту Task 8** (`FilterRegistrations`); `AddPgBoltServer` вызывает регистраторы. `AddPgBoltJobFilter<T>` регистрирует `new GlobalJobFilter(typeof(T))` и `TryAddScoped<T>()` напрямую.
- **Готовность инициализатора в unit-тестах.** `PgBoltInitializer.Ready` — `internal` get-only на `TaskCompletionSource` внутри инициализатора; завершить её снаружи нельзя, а настоящий `StartAsync` инициализатора требует PostgreSQL. Поэтому у `PgBoltProcessingService` есть `internal`-конструктор с `Func<CancellationToken, Task> waitReady`; публичный конструктор передаёт `ct => sp.GetRequiredService<PgBoltInitializer>().Ready.WaitAsync(ct)`. **Дополнение к контракту.**
- **Предупреждение о задачах, игнорирующих отмену (§5.1)** — периодический таймер `PgBoltServer` (`clock.Time.CreateTimer`, период `HeartbeatInterval`), запускается в `Start`, освобождается в `FlushAndStopAsync` (в том числе при отменённом токене хоста). Момент отмены запоминается регистрацией на `execution.Token` при старте выполнения; таймер пишет `Warning` для каждого выполнения, отменённого не меньше `HeartbeatInterval` назад и ещё не завершившегося (любая причина: shutdown, отмена, таймаут, потеря lease). Отдельный таймер, а не `WaitExecutionsAsync`, потому что задача может игнорировать отмену и вне остановки.
- **`BeginStop` вызывает `_shutdown.CancelAsync()`** (не `Cancel()`): `IsCancellationRequested` выставляется синхронно (runner, стартующий после сигнала, видит его и отдаёт `Release(false)`), а колбэки — отмена токенов выполнений и продолжения кода задач — идут в пуле потоков. Так код задач не выполняется синхронно внутри `BeginStop` одного сервера и не задерживает отмену у следующих. `WaitExecutionsAsync` дожидается этих колбэков (исключение колбэка задачи — лог).
- **Отслеживание выполнений:** `ConcurrentDictionary<Execution, Tracked>`, запись добавляется **до** старта задачи (`new Task<Task>(…)` → `Unwrap` → добавление → `Start`), поэтому быстрое выполнение не «теряет» удаление. Брошенные при таймауте остановки определяются CAS `Running → Removed`: выполнение, уже сдавшее исход (`OutcomePending`), не трогается.
- **`WaitExecutionsAsync`**: `await dispatcher.Completion` (in-flight dequeue дожидается своим таймаутом; сбойный `Completion` — `LogCritical` и остановка продолжается: диспетчер сам ловит ошибки итераций (Task 15), это последний рубеж, чтобы исключение одного сервера не сорвало flush всех), затем `Task.WhenAll(выполнения).WaitAsync(clock.Until(stopStartedAt + ShutdownTimeout), clock.Time, ct)`; дедлайн абсолютный от `BeginStop`, поэтому ожидание dequeue и выполнений укладывается в один `ShutdownTimeout`. `TimeoutException` → брошенные: `Running → Removed`, `table.Remove`, `Warning`.
- **Предупреждение NoExecutor** (§4.4 п. 4) — не входит в шаги 1–3, поэтому ошибка `GetJobTypeNamesByQueuesAsync` логируется и не роняет старт.
- **Порядок старта — ровно §4.4:** `Ready` (п. 1) → `PublishJobTypesAsync` один раз (п. 2) → `RegisterAsync` **всех** серверов: строка `servers` и предупреждение NoExecutor (п. 3–4) → предупреждение про `HostOptions` (п. 5) → `ProcessingMaintenance` (если `RunMaintenance` и есть хотя бы один сервер), затем `Start` каждого сервера: `keeper.Start`, `sink.Start`, `dispatcher.Start`, watchdog (п. 6). Поэтому ошибка регистрации любого сервера (п. 3) роняет старт до того, как хоть один сервер начал брать задачи: хост не вызывает `StopAsync` после ошибки `StartAsync`, и задачи, взятые в этом окне, иначе оборвались бы при выходе процесса с тратой попытки sweeper'ом. Строки `servers`, вставленные до ошибки, убирает уборка части 2 (§4.4). Ошибка на п. 6 (последний рубеж: `Start` синхронный и на практике не бросает) — `LogCritical`, `StopAsync(CancellationToken.None)` для всего уже запущенного, исключение наружу.
- **Остановка:** `maintenance.StopAsync()` запускается первым, но не ожидается до `BeginStop` всех серверов; затем `WaitExecutionsAsync` всех серверов параллельно; затем `FlushAndStopAsync` всех параллельно — **и при отменённом `ct`**: sink с отменённым токеном прерывает отправку сразу, `LeaseKeeper` и watchdog всё равно останавливаются, пропускается только `RemoveServerAsync` (`Warning`). Иначе в процессе, который живёт дольше хоста (тестовые хосты, несколько хостов в процессе), heartbeat продолжал бы продлевать lease незавершённых выполнений, и они не «истекли бы сами» (§6.4: «оставшиеся шаги пропускаются (lease истекут сами)» — пропускаются шаги с обращением к БД, а не освобождение локальных циклов). В конце — ожидание обслуживания (при отменённом `ct` — `Warning`).
- **Promoter/sweeper:** `PromoteDueAsync` в core уже повторяет пакеты внутри вызова и возвращает сумму; внешний повтор «пока `Promoted ≥ 1000`» по заданию сохранён (при сумме ≥ 1000 будет один лишний пустой вызов). `SweepExpiredLeasesAsync` возвращает число событий `LeaseExpired` (без `Cancelled`), поэтому полный пакет с отменёнными строками может завершить повтор раньше — остаток заберёт следующий интервал.

- [x] **Step 1: Пакет `Microsoft.Extensions.Hosting`**

`HostOptions` (предупреждение §4.4 п. 5) живёт в `Microsoft.Extensions.Hosting`. Если в `Directory.Packages.props` нет строки — добавить рядом с `Microsoft.Extensions.Hosting.Abstractions`:

```xml
    <PackageVersion Include="Microsoft.Extensions.Hosting" Version="10.0.0" />
```

Если в `src/PgBolt.Server/PgBolt.Server.csproj` нет ссылки — добавить в `ItemGroup` с `PackageReference`:

```xml
    <PackageReference Include="Microsoft.Extensions.Hosting" />
```

Тестовый проект получает пакет транзитивно (нужен `services.Configure<HostOptions>`).

- [x] **Step 2: Регистраторы фильтров в `PgBoltServerOptions`**

В `src/PgBolt.Server/PgBoltServerOptions.cs`:
- добавить `using Microsoft.Extensions.DependencyInjection;` и `using Microsoft.Extensions.DependencyInjection.Extensions;`;
- добавить поле и свойство:

```csharp
    private readonly List<Action<IServiceCollection>> _filterRegistrations = [];

    /// <summary>Регистрирует тип фильтра как scoped (AOT: T аннотирован в AddFilter), в порядке AddFilter.</summary>
    internal IReadOnlyList<Action<IServiceCollection>> FilterRegistrations => _filterRegistrations;
```

- в `AddFilter<T>()` сразу после добавления `typeof(T)` в список `FilterTypes` (перед `return this;`) добавить:

```csharp
        _filterRegistrations.Add(static services => services.TryAddScoped<T>());
```

- [x] **Step 3: Написать падающие тесты регистрации**

`tests/PgBolt.Server.Tests/Unit/ServerRegistrationTests.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class GlobalProbeFilter : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
}

public sealed class ServerProbeFilter : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
}

public sealed class ServerRegistrationTests
{
    private static ServiceCollection WithCore()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IServerStore>(new FakeServerStore());
        services.AddPgBolt(o => o.ConnectionString = "Host=unused");
        return services;
    }

    [Fact]
    public async Task Several_servers_share_one_hosted_service()
    {
        var services = WithCore();
        services.AddPgBoltServer("a", s => s.Queue("qa"));
        services.AddPgBoltServer("b", s => s.Queue("qb"));
        await using var sp = services.BuildServiceProvider();

        Assert.Single(sp.GetServices<IHostedService>().OfType<PgBoltProcessingService>());
        Assert.Equal(["a", "b"], sp.GetServices<ServerDefinition>().Select(d => d.Name));
        Assert.IsType<FakeServerStore>(sp.GetRequiredService<IServerStore>());   // регистрация до AddPgBoltServer сохраняется
        Assert.NotNull(sp.GetRequiredService<MonoClock>());
        Assert.NotNull(sp.GetRequiredService<PgBoltProcessingOptions>());
    }

    [Fact]
    public void Server_without_core_registration_throws()
    {
        var e = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddPgBoltServer("a", s => s.Queue("q")));
        Assert.Contains("AddPgBolt", e.Message);
    }

    [Fact]
    public void Duplicate_server_name_throws()
    {
        var services = WithCore();
        services.AddPgBoltServer("orders", s => s.Queue("q"));

        var e = Assert.Throws<InvalidOperationException>(() => services.AddPgBoltServer("orders", s => s.Queue("q2")));
        Assert.Contains("orders", e.Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("has space")]
    [InlineData("имя")]
    [InlineData("a/b")]
    public void Invalid_server_name_throws(string name)
    {
        Assert.Throws<ArgumentException>(() => WithCore().AddPgBoltServer(name, s => s.Queue("q")));
    }

    [Fact]
    public void Name_longer_than_64_chars_throws_and_64_is_accepted()
    {
        WithCore().AddPgBoltServer(new string('a', 64), s => s.Queue("q"));
        Assert.Throws<ArgumentException>(() => WithCore().AddPgBoltServer(new string('a', 65), s => s.Queue("q")));
    }

    [Fact]
    public void Invalid_options_are_rejected_at_registration()
    {
        var e = Assert.Throws<InvalidOperationException>(() => WithCore().AddPgBoltServer("empty", _ => { }));
        Assert.Contains("empty", e.Message);
    }

    [Fact]
    public void Filters_are_registered_as_scoped_and_globals_are_listed_in_order()
    {
        var services = WithCore();
        services.AddPgBoltJobFilter<GlobalProbeFilter>();
        services.AddPgBoltServer("a", s => s.Queue("q").AddFilter<ServerProbeFilter>());

        Assert.Contains(services, d => d.ServiceType == typeof(GlobalProbeFilter) && d.Lifetime == ServiceLifetime.Scoped);
        Assert.Contains(services, d => d.ServiceType == typeof(ServerProbeFilter) && d.Lifetime == ServiceLifetime.Scoped);
        var global = Assert.Single(services, d => d.ServiceType == typeof(GlobalJobFilter));
        Assert.Equal(typeof(GlobalProbeFilter), ((GlobalJobFilter)global.ImplementationInstance!).FilterType);
    }

    [Fact]
    public async Task Processing_options_can_be_configured_before_or_after_servers()
    {
        var before = WithCore();
        before.ConfigurePgBoltProcessing(p => p.PromoterInterval = TimeSpan.FromSeconds(3));
        before.AddPgBoltServer("a", s => s.Queue("q"));
        var after = WithCore();
        after.AddPgBoltServer("a", s => s.Queue("q"));
        after.ConfigurePgBoltProcessing(p => p.RunMaintenance = false);

        await using var sp1 = before.BuildServiceProvider();
        await using var sp2 = after.BuildServiceProvider();

        Assert.Equal(TimeSpan.FromSeconds(3), sp1.GetRequiredService<PgBoltProcessingOptions>().PromoterInterval);
        Assert.False(sp2.GetRequiredService<PgBoltProcessingOptions>().RunMaintenance);
        Assert.Single(after, d => d.ServiceType == typeof(PgBoltProcessingOptions));
    }

    [Fact]
    public void Invalid_processing_options_throw()
    {
        Assert.Throws<InvalidOperationException>(() =>
            WithCore().ConfigurePgBoltProcessing(p => p.SweeperInterval = TimeSpan.Zero));
    }
}
```

- [x] **Step 4: Написать падающие тесты обслуживания**

`tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class ProcessingMaintenanceTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly InProcessWakeupSignal _wakeup = new();
    private readonly ListLoggerFactory _logs = new();
    private readonly PgBoltProcessingOptions _options = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ProcessingMaintenance Create() =>
        new(_store, _wakeup, _options, new MonoClock(_time), _logs.CreateLogger("test")) { NextRandom = () => 0.5 };

    [Fact]
    public async Task Promote_repeats_while_batch_is_full_and_signals_queues()
    {
        var signals = new ConcurrentQueue<string>();
        foreach (var q in new[] { "a", "b", "c" })
            _wakeup.Subscribe(q, () => signals.Enqueue(q));
        var batches = new ConcurrentQueue<int>();
        _store.OnPromote = (batch, _) =>
        {
            batches.Enqueue(batch);
            return Task.FromResult(batches.Count == 1 ? new PromoteResult(batch, ["a", "b"]) : new PromoteResult(3, ["c"]));
        };

        var promoted = await Create().PromoteOnceAsync(Ct);

        Assert.Equal(1003, promoted);
        Assert.Equal([1000, 1000], batches);
        Assert.Equal(["a", "b", "c"], signals);
    }

    [Fact]
    public async Task Sweep_repeats_while_batch_is_full()
    {
        var results = new Queue<int>([1000, 1000, 5]);
        _store.OnSweep = (_, _) => Task.FromResult(results.Dequeue());

        Assert.Equal(2005, await Create().SweepOnceAsync(Ct));
        Assert.Equal(3, _store.CallsOf("Sweep").Count());
    }

    [Fact]
    public async Task Loop_error_is_logged_and_retried_after_interval()
    {
        var calls = 0;
        _store.OnPromote = (_, _) => Interlocked.Increment(ref calls) == 1
            ? throw new IOException("db down")
            : Task.FromResult(new PromoteResult(0, []));
        var maintenance = Create();
        maintenance.Start();

        await Eventually.True(() => maintenance.IdleWaits == 2, "promoter and sweeper wait for their intervals");
        Assert.Contains(_logs.At(LogLevel.Error), e => e.Message.Contains("promoter"));
        _time.Advance(_options.PromoterInterval);

        await Eventually.True(() => _store.CallsOf("Promote").Count() == 2, "promoter runs again after 1 s");
        Assert.Single(_store.CallsOf("Sweep"));   // sweeper ждёт свои 15 с
        await maintenance.StopAsync();
    }

    [Fact]
    public async Task Stop_ends_both_loops()
    {
        var maintenance = Create();
        maintenance.Start();
        await Eventually.True(() => maintenance.IdleWaits == 2, "loops are waiting");

        await maintenance.StopAsync();

        _time.Advance(TimeSpan.FromMinutes(1));
        Assert.Single(_store.CallsOf("Promote"));
        Assert.Single(_store.CallsOf("Sweep"));
    }
}
```

- [x] **Step 5: Написать падающие тесты координатора**

`tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs` (`AddPgBolt` с `ConnectionString = "Host=unused"` — соединение не открывается; `IServerStore`, `TimeProvider` и `ILoggerFactory` зарегистрированы до `AddPgBolt`/`AddPgBoltServer`, поэтому `TryAdd` их не заменяет; готовность инициализатора подменяется `internal`-конструктором):

```csharp
using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed record StubbornArgs(int N);
public sealed record PoliteArgs(int N);

[JsonSerializable(typeof(StubbornArgs))]
[JsonSerializable(typeof(PoliteArgs))]
internal sealed partial class ServerJson : JsonSerializerContext;

public sealed class ServerProbe
{
    public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public ConcurrentQueue<(string Job, CancellationToken Token)> Started { get; } = new();
}

/// <summary>Игнорирует токен: завершается только по Gate.</summary>
public sealed class StubbornJob(ServerProbe probe) : IJob<StubbornArgs>
{
    public Task ExecuteAsync(StubbornArgs args, JobContext context, CancellationToken cancellationToken)
    {
        probe.Started.Enqueue(("stubborn", cancellationToken));
        return probe.Gate.Task;
    }
}

/// <summary>Останавливается по токену.</summary>
public sealed class PoliteJob(ServerProbe probe) : IJob<PoliteArgs>
{
    public Task ExecuteAsync(PoliteArgs args, JobContext context, CancellationToken cancellationToken)
    {
        probe.Started.Enqueue(("polite", cancellationToken));
        return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
}

public sealed class FilterTrace
{
    public ConcurrentQueue<string> Calls { get; } = new();
}

public sealed class TraceGlobalFilter(FilterTrace trace) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        trace.Calls.Enqueue("global:before");
        await next();
        trace.Calls.Enqueue("global:after");
    }
}

public sealed class TraceServerFilter(FilterTrace trace) : IJobFilter
{
    public async ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next)
    {
        trace.Calls.Enqueue("server:before");
        await next();
        trace.Calls.Enqueue("server:after");
    }
}

public sealed class ProcessingServiceTests : IAsyncDisposable
{
    private static readonly JobDescriptor Stubborn = new("server.stubborn", "slow", 3, 0, null);
    private static readonly JobDescriptor Polite = new("server.polite", "fast", 3, 0, null);

    private readonly FakeTimeProvider _time = new();
    private readonly FakeServerStore _store = new();
    private readonly ListLoggerFactory _logs = new();
    private readonly ServerProbe _probe = new();
    private readonly ServiceCollection _services = new();
    private readonly ConcurrentDictionary<string, int> _polls = new();
    private ServiceProvider? _provider;
    private PgBoltProcessingService? _service;

    public ProcessingServiceTests()
    {
        _services.AddSingleton<TimeProvider>(_time);
        _services.AddSingleton<IServerStore>(_store);
        _services.AddSingleton<ILoggerFactory>(_logs);
        _services.AddSingleton(_probe);
        _services.AddPgBolt(o =>
        {
            o.ConnectionString = "Host=unused";
            o.AddJobs(b => b
                .AddExecutor<StubbornArgs, StubbornJob>(Stubborn, ServerJson.Default.StubbornArgs)
                .AddExecutor<PoliteArgs, PoliteJob>(Polite, ServerJson.Default.PoliteArgs));
        });
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Первый dequeue очереди отдаёт одну задачу типа из map, дальше — пусто.</summary>
    private void OneJobPerQueue(IReadOnlyDictionary<string, string> typeByQueue) =>
        _store.OnDequeue = (queue, _, _) => Task.FromResult<IReadOnlyList<LeasedJob>>(
            _polls.AddOrUpdate(queue, 1, (_, n) => n + 1) == 1 && typeByQueue.TryGetValue(queue, out var type)
                ? [Leased.Job(type: type)]
                : []);

    private async Task<PgBoltProcessingService> StartAsync(Func<CancellationToken, Task>? waitReady = null)
    {
        _provider = _services.BuildServiceProvider();
        _service = new PgBoltProcessingService(_provider, waitReady ?? (_ => Task.CompletedTask));
        await _service.StartAsync(Ct);
        return _service;
    }

    public async ValueTask DisposeAsync()
    {
        _probe.Gate.TrySetResult();
        if (_service is not null)
            await _service.StopAsync(CancellationToken.None);
        if (_provider is not null)
            await _provider.DisposeAsync();
    }

    [Fact]
    public async Task Start_publishes_job_types_once_and_registers_every_server()
    {
        _services.AddPgBoltServer("orders", s => s.Queue("o1").Queue("o2", q => q.Reserved = 2));
        _services.AddPgBoltServer("mail", s =>
        {
            s.Workers = 3;
            s.Queue("m");
        });

        var service = await StartAsync();

        var publish = Assert.Single(_store.CallsOf("PublishJobTypes"));
        var (types, publishedBy) = ((IReadOnlyList<JobTypeRecord>, string))publish.Detail!;
        Assert.Equal($"{Environment.MachineName}/{Environment.ProcessId}", publishedBy);
        Assert.Equal(["server.polite", "server.stubborn"], types.Select(t => t.Name));

        var registrations = _store.CallsOf("RegisterServer").Select(c => (ServerRegistration)c.Detail!).ToArray();
        Assert.Equal(2, registrations.Length);
        var orders = registrations.Single(r => r.Name == "orders");
        Assert.Equal((Environment.MachineName, Environment.ProcessorCount * 2 + 2), (orders.Host, orders.Capacity));
        Assert.Equal(["o1", "o2"], orders.Queues);
        Assert.Equal(service.Servers.Select(s => s.Id).Order(), registrations.Select(r => r.Id).Order());
        Assert.Equal(3, registrations.Single(r => r.Name == "mail").Capacity);
    }

    [Fact]
    public async Task Start_waits_for_initializer_and_fails_with_it()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            StartAsync(_ => Task.FromException(new InvalidOperationException("migrations failed"))));

        Assert.Empty(_store.Calls);
    }

    [Fact]
    public async Task Servers_start_only_after_all_are_registered_and_failed_registration_starts_none()
    {
        _services.AddPgBoltServer("a", s => s.Queue("qa"));
        _services.AddPgBoltServer("b", s => s.Queue("qb"));
        bool? startedBeforeLastRegistration = null;
        _store.OnRegisterServer = (server, _) =>
        {
            if (server.Name != "b")
                return Task.CompletedTask;
            // PgBoltServer.Start пишет «started» синхронно: при регистрации b ни один сервер ещё не должен работать.
            startedBeforeLastRegistration = _logs.Entries.Any(e => e.Message.Contains(" started: "));
            return Task.FromException(new IOException("db down"));
        };

        await Assert.ThrowsAsync<IOException>(() => StartAsync());

        Assert.False(startedBeforeLastRegistration);
        Assert.Equal(["a", "b"], _store.CallsOf("RegisterServer").Select(c => ((ServerRegistration)c.Detail!).Name));
        Assert.DoesNotContain(_logs.Entries, e => e.Message.Contains(" started: "));
        Assert.Empty(_store.CallsOf("Promote"));   // обслуживание тоже не запускалось
    }

    [Fact]
    public async Task Global_filters_wrap_server_filters()
    {
        var trace = new FilterTrace();
        _services.AddSingleton(trace);
        _services.AddPgBoltJobFilter<TraceGlobalFilter>();
        _services.AddPgBoltServer("a", s => s.Queue("slow").AddFilter<TraceServerFilter>());
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn" });
        _probe.Gate.SetResult();                  // задача завершается сразу

        await StartAsync();

        await Eventually.True(() => trace.Calls.Count == 4, "both filters ran around the job");
        Assert.Equal(["global:before", "server:before", "server:after", "global:after"], trace.Calls);
    }

    [Fact]
    public async Task Start_warns_about_job_types_without_executor()
    {
        _services.AddPgBoltServer("a", s => s.Queue("slow"));
        IReadOnlyList<string>? asked = null;
        _store.OnJobTypeNames = (queues, _) =>
        {
            asked = queues;
            return Task.FromResult<IReadOnlyList<string>>(["server.stubborn", "ghost.job"]);
        };

        await StartAsync();

        Assert.Equal(["slow"], asked);
        var warning = Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("NoExecutor"));
        Assert.Contains("ghost.job", warning.Message);
        Assert.DoesNotContain("server.stubborn", warning.Message);
    }

    [Fact]
    public async Task Stop_cancels_shutdown_tokens_of_all_servers_before_waiting_for_any()
    {
        _services.AddPgBoltServer("a", s => s.Queue("slow"));
        _services.AddPgBoltServer("b", s => s.Queue("slow2"));
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn", ["slow2"] = "server.stubborn" });
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 2, "a job runs on each server");

        var stop = service.StopAsync(Ct);

        // Обе задачи игнорируют токен и ещё не завершились: последовательная остановка не отменила бы токен второго сервера.
        await Eventually.True(() => _probe.Started.All(s => s.Token.IsCancellationRequested), "both servers cancelled their jobs");
        Assert.False(stop.IsCompleted);

        _probe.Gate.SetResult();
        await stop;
        Assert.Equal(2, _store.CallsOf("RemoveServer").Count());
    }

    [Fact]
    public async Task Job_ignoring_token_is_abandoned_after_shutdown_timeout_without_blocking_others()
    {
        _services.AddPgBoltServer("stubborn", s =>
        {
            s.LeaseDuration = TimeSpan.FromSeconds(3);
            s.HeartbeatInterval = TimeSpan.FromSeconds(1);
            s.ShutdownTimeout = TimeSpan.FromSeconds(5);
            s.Queue("slow");
        });
        _services.AddPgBoltServer("polite", s =>
        {
            s.ShutdownTimeout = TimeSpan.FromSeconds(5);
            s.Queue("fast");
        });
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn", ["fast"] = "server.polite" });
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 2, "both jobs run");

        var stop = service.StopAsync(Ct);
        await Eventually.True(() => _store.CallsOf("Release").Any(), "polite job is released right away");
        Assert.Equal((object?)true, Assert.Single(_store.CallsOf("Release")).Detail);
        Assert.False(stop.IsCompleted);
        // Момент отмены пишется колбэком на токене выполнения в пуле потоков (после отмены токена задачи):
        // без этого ожидания Advance мог бы опередить запись, и предупреждение «ignored cancellation» не появилось бы.
        var stubborn = service.Servers.Single(s => s.Name == "stubborn");
        await Eventually.True(() => stubborn.CancelledExecutions == 1, "cancellation of the stubborn job is recorded");

        _time.Advance(TimeSpan.FromSeconds(5));   // ShutdownTimeout от начала остановки
        await stop;

        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("is abandoned"));
        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("ignored cancellation"));
        Assert.Equal(2, _store.CallsOf("RemoveServer").Count());

        var outcomesBefore = _store.Calls.Count(c => c.Op is "Ack" or "Retry" or "Fail" or "ConfirmCancel" or "Release");
        _probe.Gate.SetResult();                  // брошенная задача завершается позже
        await Eventually.True(() => _logs.Entries.Any(e => e.Message.Contains("discarded")), "late outcome is discarded");
        Assert.Equal(outcomesBefore, _store.Calls.Count(c => c.Op is "Ack" or "Retry" or "Fail" or "ConfirmCancel" or "Release"));
    }

    [Fact]
    public async Task Cancelled_stop_still_stops_heartbeat_sink_and_watchdog_but_keeps_servers_row()
    {
        _services.AddPgBoltServer("a", s =>
        {
            s.LeaseDuration = TimeSpan.FromSeconds(3);
            s.HeartbeatInterval = TimeSpan.FromSeconds(1);
            s.Queue("slow");
        });
        OneJobPerQueue(new Dictionary<string, string> { ["slow"] = "server.stubborn" });
        var service = await StartAsync();
        await Eventually.True(() => _probe.Started.Count == 1, "the job runs");

        using var expired = new CancellationTokenSource();
        await expired.CancelAsync();
        await service.StopAsync(expired.Token);   // токен хоста уже сработал

        Assert.Contains(_logs.At(LogLevel.Warning), e => e.Message.Contains("host stop timeout"));
        Assert.Empty(_store.CallsOf("RemoveServer"));                  // строку servers уберёт уборка части 2
        var server = Assert.Single(service.Servers);
        await Eventually.True(() => server.CancelledExecutions == 1, "cancellation of the stubborn job is recorded");
        var heartbeats = _store.CallsOf("Heartbeat").Count();

        _time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(heartbeats, _store.CallsOf("Heartbeat").Count());                          // LeaseKeeper остановлен: lease истекут сами
        Assert.DoesNotContain(_logs.Entries, e => e.Message.Contains("ignored cancellation"));   // watchdog освобождён
    }

    [Fact]
    public async Task Maintenance_runs_only_when_enabled()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));
        _services.ConfigurePgBoltProcessing(p => p.RunMaintenance = false);

        var service = await StartAsync();

        Assert.Null(service.Maintenance);
        Assert.Empty(_store.CallsOf("Promote"));
        Assert.Empty(_store.CallsOf("Sweep"));
    }

    [Fact]
    public async Task Maintenance_starts_with_servers()
    {
        _services.AddPgBoltServer("a", s => s.Queue("q"));

        var service = await StartAsync();

        Assert.NotNull(service.Maintenance);
        await Eventually.True(() => _store.CallsOf("Promote").Any() && _store.CallsOf("Sweep").Any(), "promoter and sweeper run");
    }

    [Fact]
    public async Task Warns_when_host_shutdown_timeout_is_shorter_than_server_needs()
    {
        _services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(20));
        _services.AddPgBoltServer("a", s => s.Queue("q"));   // 25 с + 20 мс + 2 с > 20 с

        await StartAsync();

        Assert.Single(_logs.At(LogLevel.Warning), e => e.Message.Contains("HostOptions.ShutdownTimeout"));
    }

    [Fact]
    public async Task Shutdown_timeout_warning_uses_max_not_sum_of_servers()
    {
        _services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(28));
        _services.AddPgBoltServer("a", s => s.Queue("q"));                                                   // 25 с
        _services.AddPgBoltServer("b", s => { s.ShutdownTimeout = TimeSpan.FromSeconds(20); s.Queue("r"); });

        await StartAsync();

        Assert.DoesNotContain(_logs.At(LogLevel.Warning), e => e.Message.Contains("HostOptions.ShutdownTimeout"));
    }
}
```

- [x] **Step 6: Убедиться, что тесты падают**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ServerRegistrationTests|FullyQualifiedName~ProcessingMaintenanceTests|FullyQualifiedName~ProcessingServiceTests"`
Expected: FAIL при компиляции — CS1061/CS0246: нет `AddPgBoltServer`, `ProcessingMaintenance`, `PgBoltProcessingService`, `ServerDefinition`.

- [x] **Step 7: Описания серверов и обслуживание**

`src/PgBolt.Server/Runtime/ServerDefinition.cs`:

```csharp
namespace PgBolt.Server.Runtime;

/// <summary>Описание виртуального сервера в DI: одно на вызов AddPgBoltServer (§3.1).</summary>
internal sealed record ServerDefinition(string Name, PgBoltServerOptions Options);

/// <summary>Глобальный фильтр процесса (AddPgBoltJobFilter), в порядке регистрации (§3.6).</summary>
internal sealed record GlobalJobFilter(Type FilterType);
```

`src/PgBolt.Server/Runtime/ProcessingMaintenance.cs`:

```csharp
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Client;

namespace PgBolt.Server.Runtime;

/// <summary>Promoter и sweeper процесса (§6.3). Все поды выполняют оба цикла: SKIP LOCKED делает это безопасным.</summary>
internal sealed class ProcessingMaintenance(IServerStore store, IWakeupSignal wakeup, PgBoltProcessingOptions options,
    MonoClock clock, ILogger logger)
{
    internal const int BatchSize = 1000;
    private const double IntervalJitter = 0.2;

    private readonly CancellationTokenSource _stop = new();
    private Task _loops = Task.CompletedTask;
    private int _idleWaits;

    /// <summary>Источник U[0, 1) для jitter интервалов (±20 %); тесты подменяют.</summary>
    internal Func<double> NextRandom { get; set; } = Random.Shared.NextDouble;

    /// <summary>Для тестов: сколько раз циклы встали на паузу (таймер паузы уже взведён).</summary>
    internal int IdleWaits => Volatile.Read(ref _idleWaits);

    public void Start() =>
        _loops = Task.WhenAll(
            Task.Run(() => LoopAsync("promoter", PromoteOnceAsync, options.PromoterInterval)),
            Task.Run(() => LoopAsync("sweeper", SweepOnceAsync, options.SweeperInterval)));

    public Task StopAsync()
    {
        _stop.Cancel();
        return _loops;
    }

    /// <summary>PromoteDueAsync(1000), пока пакет полный; каждая очередь с новыми задачами получает сигнал.</summary>
    internal async Task<int> PromoteOnceAsync(CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var result = await store.PromoteDueAsync(BatchSize, ct);
            total += result.Promoted;
            foreach (var queue in result.Queues)
                wakeup.Signal(queue);
            if (result.Promoted < BatchSize)
                return total;
        }
    }

    /// <summary>SweepExpiredLeasesAsync(1000), пока пакет полный.</summary>
    internal async Task<int> SweepOnceAsync(CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var swept = await store.SweepExpiredLeasesAsync(BatchSize, ct);
            total += swept;
            if (swept < BatchSize)
                return total;
        }
    }

    private async Task LoopAsync(string name, Func<CancellationToken, Task<int>> once, TimeSpan interval)
    {
        var ct = _stop.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await once(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                logger.LogError(e, "PgBolt: {Loop} iteration failed; retrying after the next interval.", name);
            }

            try
            {
                var pause = Task.Delay(interval * (1 + IntervalJitter * (2 * NextRandom() - 1)), clock.Time, ct);
                Interlocked.Increment(ref _idleWaits);
                await pause;
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
```

- [x] **Step 8: Виртуальный сервер**

`src/PgBolt.Server/Runtime/PgBoltServer.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Jobs;

namespace PgBolt.Server.Runtime;

/// <summary>Виртуальный сервер (§4.1): слоты, диспетчер, runner, sink, lease keeper. Не hosted service.</summary>
internal sealed class PgBoltServer
{
    private readonly IServerStore _store;
    private readonly JobRegistry _registry;
    private readonly MonoClock _clock;
    private readonly ILogger _logger;
    private readonly ExecutionTable _table = new();
    private readonly SlotAllocator _slots;
    private readonly LeaseKeeper _keeper;
    private readonly CompletionSink _sink;
    private readonly JobRunner _runner;
    private readonly Dispatcher _dispatcher;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<Execution, Tracked> _running = new();
    private Task _shutdownCallbacks = Task.CompletedTask;
    private ITimer? _watchdog;
    private long _stopStartedAt;

    public PgBoltServer(ServerDefinition definition, IServerStore store, JobRegistry registry, IServiceScopeFactory scopes,
        IReadOnlyList<Type> globalFilters, IWakeupSignal wakeup, MonoClock clock, ILoggerFactory loggers)
    {
        Id = Guid.CreateVersion7();
        Name = definition.Name;
        Options = definition.Options;
        _store = store;
        _registry = registry;
        _clock = clock;
        _logger = loggers.CreateLogger("PgBolt.Server");

        _slots = new SlotAllocator(Options.Workers, Options.Queues);
        Dispatcher? dispatcher = null;   // onRecovered снимает паузу выборки (§6.2); keeper стартует после создания диспетчера
        _keeper = new LeaseKeeper(Id, store, _table, Options, clock, _logger, () => dispatcher?.Wake());
        _sink = new CompletionSink(Id, store, _table, Options, clock, _logger);
        var filters = new FilterPipeline([.. globalFilters, .. Options.FilterTypes]);
        _runner = new JobRunner(registry, scopes, filters, new RetryDecider(Options.RetryPolicy, _logger), Options, clock, loggers);
        _dispatcher = dispatcher = new Dispatcher(Options, _slots, new DequeueFetchStrategy(store, Id, Options.LeaseDuration), _keeper, _sink,
            wakeup, clock, _logger, StartExecution);
    }

    public Guid Id { get; }
    public string Name { get; }
    public PgBoltServerOptions Options { get; }

    /// <summary>Для тестов: выполнения, у которых записан момент отмены (основа предупреждения §5.1).</summary>
    internal int CancelledExecutions => _running.Values.Count(t => Volatile.Read(ref t.CancelledAt) != 0);

    /// <summary>§4.4 п. 3–4: строка servers и предупреждение NoExecutor. Runtime не запускается (<see cref="Start"/>).</summary>
    public async Task RegisterAsync(CancellationToken ct)
    {
        var queues = Options.Queues.Select(q => q.Name).ToArray();
        await _store.RegisterServerAsync(new ServerRegistration(Id, Name, Environment.MachineName, queues, Options.Capacity), ct);
        await WarnMissingExecutorsAsync(queues, ct);
    }

    /// <summary>§4.4 п. 6: LeaseKeeper, sink, диспетчер и watchdog — после регистрации всех серверов процесса.</summary>
    public void Start()
    {
        _keeper.Start();
        _sink.Start();
        _dispatcher.Start();
        _watchdog = _clock.Time.CreateTimer(static s => ((PgBoltServer)s!).WarnIgnoredCancellations(), this,
            Options.HeartbeatInterval, Options.HeartbeatInterval);
        _logger.LogInformation("PgBolt server {Server} ({ServerId}) started: queues {Queues}, capacity {Capacity}.",
            Name, Id, string.Join(", ", Options.Queues.Select(q => q.Name)), Options.Capacity);
    }

    /// <summary>§6.4 п. 1: новые dequeue не начинаются, shutdown-токен отменён. Колбэки отмены идут в пуле потоков,
    /// чтобы код задач не выполнялся синхронно внутри остановки и не задерживал остальные серверы.</summary>
    public void BeginStop()
    {
        if (Interlocked.CompareExchange(ref _stopStartedAt, _clock.Now, 0) != 0)
            return;
        _dispatcher.SignalStop();
        _shutdownCallbacks = _shutdown.CancelAsync();
    }

    /// <summary>§6.4 п. 2–3: in-flight dequeue, затем выполнения до ShutdownTimeout от BeginStop; не успевшие — брошены.</summary>
    public async Task WaitExecutionsAsync(CancellationToken ct)
    {
        try
        {
            await WaitDispatcherAsync(ct);
            await ObserveShutdownCallbacksAsync(ct);
            var deadline = _clock.Add(Volatile.Read(ref _stopStartedAt), Options.ShutdownTimeout);
            var running = _running.Values.Select(t => t.Task).ToArray();
            await Task.WhenAll(running).WaitAsync(_clock.Until(deadline), _clock.Time, ct);
        }
        catch (TimeoutException)
        {
            AbandonRunning();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogWarning("PgBolt server {Server}: waiting for executions was cancelled by the host.", Name);
        }
    }

    /// <summary>
    /// §6.4 п. 4–6: исходы отправлены, lease больше не держатся, строка servers удалена. При отменённом ct sink прерывает
    /// отправку сразу, LeaseKeeper и watchdog всё равно останавливаются; пропускается только RemoveServerAsync.
    /// </summary>
    public async Task FlushAndStopAsync(CancellationToken ct)
    {
        await _sink.StopAsync(ct);
        await _keeper.StopAsync();
        _watchdog?.Dispose();
        if (ct.IsCancellationRequested)
        {
            _logger.LogWarning("PgBolt server {Server}: stop was cancelled by the host; the servers row is left for cleanup.", Name);
            return;
        }
        try
        {
            await _store.RemoveServerAsync(Id, ct);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "PgBolt server {Server}: failed to remove the servers row.", Name);
        }
        _logger.LogInformation("PgBolt server {Server} ({ServerId}) stopped.", Name, Id);
    }

    /// <summary>Сбойный Completion — последний рубеж (ошибки итераций Dispatcher ловит сам): лог, остановка продолжается.</summary>
    private async Task WaitDispatcherAsync(CancellationToken ct)
    {
        try
        {
            await _dispatcher.Completion.WaitAsync(ct);
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            _logger.LogCritical(e, "PgBolt server {Server}: dispatcher failed; stopping continues.", Name);
        }
    }

    private async Task ObserveShutdownCallbacksAsync(CancellationToken ct)
    {
        try
        {
            await _shutdownCallbacks.WaitAsync(ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            _logger.LogWarning(e, "PgBolt server {Server}: a cancellation callback of a job threw.", Name);
        }
    }

    private async Task WarnMissingExecutorsAsync(string[] queues, CancellationToken ct)
    {
        try
        {
            var missing = (await _store.GetJobTypeNamesByQueuesAsync(queues, ct))
                .Where(name => !_registry.TryGet(name, out _))
                .ToArray();
            if (missing.Length > 0)
                _logger.LogWarning(
                    "PgBolt server {Server}: job types {JobTypes} default to its queues but have no executor in this process; such jobs go to NoExecutor.",
                    Name, string.Join(", ", missing));
        }
        catch (Exception e) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning(e, "PgBolt server {Server}: failed to check job types without executors.", Name);
        }
    }

    private void StartExecution(Execution execution)
    {
        var tracked = new Tracked();
        tracked.Registration = execution.Token.Register(static s =>
        {
            var (t, clock) = ((Tracked, MonoClock))s!;
            Interlocked.CompareExchange(ref t.CancelledAt, clock.Now, 0);
        }, (tracked, _clock));
        var run = new Task<Task>(() => RunExecutionAsync(execution, tracked));
        tracked.Task = run.Unwrap();
        _running[execution] = tracked;
        run.Start(TaskScheduler.Default);   // последним: контракт startExecution (Task 15) — бросает только до запуска
    }

    private async Task RunExecutionAsync(Execution execution, Tracked tracked)
    {
        try
        {
            var outcome = await _runner.RunAsync(execution, _shutdown.Token);
            _slots.Release(execution.Queue, execution.FromReserved);
            _dispatcher.OnSlotFreed(execution.Queue);
            _sink.Complete(execution, outcome);
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "PgBolt server {Server}: unexpected failure while finishing job {JobId}.", Name, execution.Job.Id);
        }
        finally
        {
            await tracked.Registration.DisposeAsync();
            _running.TryRemove(execution, out _);
        }
    }

    /// <summary>§6.4 п. 3: не остановившиеся выполнения бросаются; исход, сданный позже, отбрасывается sink'ом.</summary>
    private void AbandonRunning()
    {
        foreach (var execution in _running.Keys)
        {
            if (!execution.TryTransition(ExecutionState.Running, ExecutionState.Removed))
                continue;
            _table.Remove(execution);
            _logger.LogWarning(
                "PgBolt server {Server}: job {JobId} did not stop within {ShutdownTimeout} and is abandoned; its lease will expire and the sweeper will count the attempt.",
                Name, execution.Job.Id, Options.ShutdownTimeout);
        }
    }

    /// <summary>§5.1: раз в HeartbeatInterval — предупреждение о выполнениях, отменённых, но не завершившихся.</summary>
    private void WarnIgnoredCancellations()
    {
        foreach (var (execution, tracked) in _running)
        {
            var cancelledAt = Volatile.Read(ref tracked.CancelledAt);
            if (cancelledAt == 0)
                continue;
            var elapsed = _clock.Elapsed(cancelledAt);
            if (elapsed >= Options.HeartbeatInterval)
                _logger.LogWarning(
                    "PgBolt server {Server}: job {JobId} has ignored cancellation ({Reason}) for {Elapsed}; its slot stays occupied.",
                    Name, execution.Job.Id, execution.FirstReason, elapsed);
        }
    }

    private sealed class Tracked
    {
        public Task Task = Task.CompletedTask;
        public CancellationTokenRegistration Registration;
        public long CancelledAt;
    }
}
```

- [x] **Step 9: Координатор**

`src/PgBolt.Server/Runtime/PgBoltProcessingService.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Единственный hosted service процесса (§4.1): запускает виртуальные серверы и обслуживание (§4.4), останавливает
/// все серверы одновременно (§6.4) независимо от порядка остановки hosted services хоста.
/// </summary>
internal sealed class PgBoltProcessingService : IHostedService
{
    private static readonly TimeSpan ShutdownReserve = TimeSpan.FromSeconds(2);

    private readonly IServiceProvider _services;
    private readonly Func<CancellationToken, Task> _waitReady;
    private readonly ILoggerFactory _loggers;
    private readonly ILogger _logger;
    private readonly List<PgBoltServer> _servers = [];

    public PgBoltProcessingService(IServiceProvider services)
        : this(services, ct => services.GetRequiredService<PgBoltInitializer>().Ready.WaitAsync(ct))
    {
    }

    /// <summary>Для тестов: подмена ожидания PgBoltInitializer.Ready (его TaskCompletionSource снаружи не завершить).</summary>
    internal PgBoltProcessingService(IServiceProvider services, Func<CancellationToken, Task> waitReady)
    {
        _services = services;
        _waitReady = waitReady;
        _loggers = services.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        _logger = _loggers.CreateLogger("PgBolt.Processing");
    }

    internal IReadOnlyList<PgBoltServer> Servers => _servers;

    /// <summary>Для тестов: null — обслуживание не запускалось.</summary>
    internal ProcessingMaintenance? Maintenance { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _waitReady(cancellationToken);

        var store = _services.GetRequiredService<IServerStore>();
        var registry = _services.GetRequiredService<JobRegistry>();
        var wakeup = _services.GetRequiredService<IWakeupSignal>();
        var clock = _services.GetRequiredService<MonoClock>();
        var scopes = _services.GetRequiredService<IServiceScopeFactory>();

        var mismatches = await store.PublishJobTypesAsync(registry.ToJobTypeRecords(),
            $"{Environment.MachineName}/{Environment.ProcessId}", cancellationToken);
        if (mismatches.Count > 0)
            _logger.LogWarning(
                "PgBolt: job types {JobTypes} were published by another process with a different argument schema.",
                string.Join(", ", mismatches));

        var globalFilters = _services.GetServices<GlobalJobFilter>().Select(f => f.FilterType).ToArray();
        foreach (var definition in _services.GetServices<ServerDefinition>())
            _servers.Add(new PgBoltServer(definition, store, registry, scopes, globalFilters, wakeup, clock, _loggers));

        // §4.4 п. 3–4: все серверы регистрируются до запуска любого — ошибка регистрации не оставляет работающих серверов.
        foreach (var server in _servers)
            await server.RegisterAsync(cancellationToken);

        WarnIfHostShutdownTooShort();                                  // §4.4 п. 5

        try                                                            // §4.4 п. 6
        {
            var processing = _services.GetRequiredService<PgBoltProcessingOptions>();
            if (processing.RunMaintenance && _servers.Count > 0)
            {
                Maintenance = new ProcessingMaintenance(store, wakeup, processing, clock, _logger);
                Maintenance.Start();
            }
            foreach (var server in _servers)
                server.Start();
        }
        catch (Exception e)
        {
            // Хост не вызывает StopAsync после ошибки StartAsync: уже запущенное останавливается здесь.
            _logger.LogCritical(e, "PgBolt: starting processing failed; stopping what was started.");
            await StopAsync(CancellationToken.None);
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var maintenance = Maintenance?.StopAsync() ?? Task.CompletedTask;
        foreach (var server in _servers)
            server.BeginStop();

        await Task.WhenAll(_servers.Select(s => s.WaitExecutionsAsync(cancellationToken)));
        if (cancellationToken.IsCancellationRequested)
            _logger.LogWarning("PgBolt: host stop timeout reached; pending outcomes are dropped and servers rows are left, leases will expire.");
        // И при отменённом токене: sink прерывает отправку сразу, LeaseKeeper и watchdog останавливаются (иначе heartbeat
        // продлевал бы lease и после StopAsync, если процесс живёт дальше); RemoveServerAsync пропускается.
        await Task.WhenAll(_servers.Select(s => s.FlushAndStopAsync(cancellationToken)));
        try
        {
            await maintenance.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("PgBolt: host stop timeout reached while stopping promoter/sweeper.");
        }
    }

    /// <summary>§4.4 п. 5: серверы останавливаются параллельно, поэтому берётся максимум, а не сумма.</summary>
    private void WarnIfHostShutdownTooShort()
    {
        if (_servers.Count == 0 || _services.GetService<IOptions<HostOptions>>() is not { } host)
            return;
        var required = _servers.Max(s => s.Options.ShutdownTimeout + s.Options.AckFlushInterval) + ShutdownReserve;
        if (required > host.Value.ShutdownTimeout)
            _logger.LogWarning(
                "PgBolt: HostOptions.ShutdownTimeout {HostTimeout} is shorter than {Required} (max server ShutdownTimeout + AckFlushInterval + 2 s); executions may be abandoned and their attempts counted.",
                host.Value.ShutdownTimeout, required);
    }
}
```

- [x] **Step 10: Регистрация в DI**

`src/PgBolt.Server/PgBoltServerServiceCollectionExtensions.cs` (первый вызов `AddPgBoltServer` определяется по отсутствию `ServerDefinition` в коллекции — отдельный маркер не нужен; `ImplementationInstance` читается только у не-keyed дескрипторов, у keyed он бросает):

```csharp
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using PgBolt.Postgres;
using PgBolt.Postgres.Storage;
using PgBolt.Server;
using PgBolt.Server.Runtime;

namespace Microsoft.Extensions.DependencyInjection;

public static class PgBoltServerServiceCollectionExtensions
{
    /// <summary>Добавляет виртуальный сервер (§3.1). Требует AddPgBolt; первый вызов регистрирует единственный hosted service.</summary>
    public static IServiceCollection AddPgBoltServer(this IServiceCollection services, string name, Action<PgBoltServerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);
        if (!services.Any(d => d.ServiceType == typeof(PgBoltOptions)))
            throw new InvalidOperationException("AddPgBoltServer requires PgBolt core: call services.AddPgBolt(...) first.");
        if (!IsValidName(name))
            throw new ArgumentException(
                $"PgBolt server name '{name}' is invalid: use 1-64 characters from [A-Za-z0-9._-].", nameof(name));

        var existing = services
            .Where(d => d.ServiceType == typeof(ServerDefinition) && !d.IsKeyedService)
            .Select(d => d.ImplementationInstance)
            .OfType<ServerDefinition>()
            .ToList();
        if (existing.Any(s => s.Name == name))
            throw new InvalidOperationException($"PgBolt server '{name}' is already registered.");

        var options = new PgBoltServerOptions();
        configure(options);
        options.Validate(name);
        services.AddSingleton(new ServerDefinition(name, options));
        foreach (var register in options.FilterRegistrations)
            register(services);

        if (existing.Count == 0)
        {
            services.TryAddSingleton(sp => new MonoClock(sp.GetRequiredService<TimeProvider>()));
            services.TryAddSingleton<IServerStore>(sp => new PgBoltServerStore(sp.GetRequiredService<PgBoltStore>()));
            services.TryAddSingleton(new PgBoltProcessingOptions());
            services.AddHostedService(sp => new PgBoltProcessingService(sp));
        }
        return services;
    }

    /// <summary>Глобальный фильтр для всех серверов процесса; выполняется раньше фильтров сервера (§3.6).</summary>
    public static IServiceCollection AddPgBoltJobFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(
        this IServiceCollection services) where T : class, IJobFilter
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton(new GlobalJobFilter(typeof(T)));
        services.TryAddScoped<T>();
        return services;
    }

    /// <summary>Общие настройки обработки процесса (§3.1); порядок относительно AddPgBoltServer не важен.</summary>
    public static IServiceCollection ConfigurePgBoltProcessing(this IServiceCollection services, Action<PgBoltProcessingOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        var options = services
            .Where(d => d.ServiceType == typeof(PgBoltProcessingOptions) && !d.IsKeyedService)
            .Select(d => d.ImplementationInstance)
            .OfType<PgBoltProcessingOptions>()
            .FirstOrDefault();
        if (options is null)
        {
            options = new PgBoltProcessingOptions();
            services.AddSingleton(options);
        }
        configure(options);
        options.Validate();
        return services;
    }

    private static bool IsValidName(string name)
    {
        if (name.Length is < 1 or > 64)
            return false;
        foreach (var c in name)
            if (!char.IsAsciiLetterOrDigit(c) && c is not ('.' or '_' or '-'))
                return false;
        return true;
    }
}
```

- [x] **Step 11: Прогнать тесты задачи**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ServerRegistrationTests|FullyQualifiedName~ProcessingMaintenanceTests|FullyQualifiedName~ProcessingServiceTests"`
Expected: PASS — 28 тестов (12 + 4 + 12).

- [x] **Step 12: Прогнать все unit-тесты сервера и сборку**

Run: `dotnet test tests/PgBolt.Server.Tests`
Expected: PASS — все тесты Tasks 8–16, 3 прогона подряд без мигания.

Run: `dotnet build src/PgBolt.Server`
Expected: `0 предупреждений` (в том числе нет IL2xxx/IL3xxx).

- [x] **Step 13: Commit**

```bash
git add Directory.Packages.props src/PgBolt.Server tests/PgBolt.Server.Tests/Unit/ServerRegistrationTests.cs tests/PgBolt.Server.Tests/Unit/ProcessingMaintenanceTests.cs tests/PgBolt.Server.Tests/Unit/ProcessingServiceTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "feat(server): virtual servers, processing coordinator, maintenance and DI registration"
```

- [x] **Step 14: Конец фазы 1**

Run: `dotnet test PgBolt.slnx`
Expected: PASS — весь набор (core, генератор, сервер). Падение — остановиться и доложить (протокол, п. 6).

## Фаза 2. Сквозные сценарии

Интеграционные тесты `PgBolt.Server` на Testcontainers: весь runtime (Tasks 8–16) поверх реального PostgreSQL, напрямую и через PgBouncer (transaction mode). Новый продакшен-код в фазе не пишется: падение сценария — дефект задачи-владельца компонента (см. «Покрытие спеки»); исполнитель фиксирует его в отчёте как `BLOCKED` с выводом теста и гипотезой, тест не ослабляет.

Общие правила фазы:
- Тесты работают в реальном времени с малыми интервалами (`ServerTestHost.Fast`: `LeaseDuration = 3 с`, `HeartbeatInterval = 1 с`, `ShutdownTimeout = 2 с`, `AckFlushInterval = 10 мс`, `MaxPollInterval = 200 мс`; `PromoterInterval = 200 мс`, `SweeperInterval = 500 мс`). Сценарий, которому нужны другие значения, задаёт их явно.
- Условия ожидаются поллингом БД или `JobProbe` с таймаутом (`WaitUntilAsync`, `WaitForStateAsync`) и понятным сообщением. Фиксированный `Task.Delay` допустим только как явно помеченная проверка «ничего не произошло за N» или как длительность искусственного сбоя.
- Классы с жёсткими временными допусками (Tasks 18, 19) входят в коллекцию `TimingSensitiveCollection` (`DisableParallelization = true`) и не конкурируют за CPU с другими тестами.
- Нагрузочный smoke (Task 20) помечен `[Trait("Category", "Load")]`; обычный прогон с этого момента — `--filter "Category!=Load"`.

### Task 17: Инфраструктура сквозных тестов и базовые сценарии

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §3.1, §3.4, §4.4 п. 4, §5.1, §5.2, §6.3, §10 «Интеграционные» п. 1–5.

**Files:**
- Modify: `Directory.Packages.props` (`Microsoft.Extensions.Logging`)
- Modify: `tests/PgBolt.Server.Tests/PgBolt.Server.Tests.csproj`
- Create: `tests/PgBolt.Testing/TcpProxy.cs`
- Create: `tests/PgBolt.Server.Tests/Integration/TestJobs.cs`
- Create: `tests/PgBolt.Server.Tests/Integration/ServerTestHost.cs`
- Test (Create): `tests/PgBolt.Server.Tests/Integration/TcpProxyTests.cs`, `tests/PgBolt.Server.Tests/Integration/BasicScenarioTests.cs`

**Interfaces:**
- Consumes: `AddPgBolt`/`PgBoltOptions.AddJobs` (core), `AddPgBoltServer`, `ConfigurePgBoltProcessing` (Task 16), `PgBoltServerOptions`/`QueueOptions`/`PgBoltProcessingOptions` (Task 8), `RetryPolicy.Exponential` (Task 9), `PgBoltProcessingService.Servers`, `PgBoltServer.Id` (Task 16, internal — `InternalsVisibleTo("PgBolt.Server.Tests")` из Task 8), `JobDescriptor.TimeoutSeconds`, `JobAbortedException` (Task 1), `PostgresFixture`, `TestDb` (core), `[assembly: AssemblyFixture(typeof(PostgresFixture))]` (Task 8).
- Produces (namespace `PgBolt.Testing`):
  - `public sealed class TcpProxy : IAsyncDisposable` — `static TcpProxy Start(string targetHost, int targetPort)`, `static TcpProxy For(string connectionString)` (target = `Host`/`Port` строки), `int Port`, `bool IsPaused`, `int ActiveConnections`, `void Pause()`, `void Resume()`, `void DropAll()`, `string ConnectionStringVia(string connectionString)` (меняет только `Host = 127.0.0.1` и `Port`).
- Produces (namespace `PgBolt.Server.Tests.Integration`):
  - Args-типы `SuccessArgs(int N)`, `FailArgs(string Message)`, `FlakyArgs(int FailUntilAttempt)`, `AbortArgs(string Reason)`, `BlockArgs(int N)`, `StubbornArgs(int N)`, `SlowArgs(int Milliseconds)`, `TimeLimitedArgs(int N)`, `PoolHogArgs(int Connections)`, `OrphanArgs(int N)`; `static class TestJobs` — дескрипторы (`Queue = "q"`), `Register(IPgBoltJobsBuilder)` (все, кроме `test.orphan`), `RegisterWithOrphan(IPgBoltJobsBuilder)`.
  - `sealed record HostTag(string Name)`; `enum RunEnd { Running, Completed, Threw }`; `sealed class JobRun` (`JobId`, `Attempt`, `Host`, `StartedAt`, `CancelledAt`, `FinishedAt` — метки `Stopwatch.GetTimestamp()`, `End`, `WasCancelled`, `Duration`).
  - `sealed class JobProbe` — `RunAsync(JobContext, HostTag, CancellationToken, Func<JobRun, Task>)`, `Runs(JobId)`, `Run(JobId, int attempt)`, `RunCount(JobId)`, `RunCounts()`, `TotalStarted`, `TotalCompleted`, `GateAsync(Guid jobId, int attempt)`, `Release(JobId, int? attempt = null)`, `ReleaseAll()`.
  - `static class DbState` (коды `jobs.state`), `sealed record QueueRow(int Attempt, Guid? LeaseOwner, Guid? LeaseId, DateTime? LeaseUntil, bool Ready, bool CancelRequested)`, `sealed record EventRow(JobEventKind Kind, int? Attempt, Guid? ServerId, string? Message)`, `sealed record LogEntry(string Category, LogLevel Level, string Message)`, `sealed class CollectingLoggerProvider : ILoggerProvider`.
  - `sealed class TestHostSettings` — `Name`, `Proxy`, `DataSource`, `Jobs`, `RunMaintenance`, `Configure`, `AddServer(name, configure)`.
  - `sealed class ServerTestHost : IAsyncDisposable` — `static Task<ServerTestHost> StartAsync(TestDb, JobProbe, Action<TestHostSettings>?, CancellationToken)`, `Task<TimeSpan> StopAsync()`, `Name`, `Services`, `Client`, `Logs`, `HostedServices`, `ServerIds`, `ServerId`, `internal Processing`; константы `LeaseDuration`, `HeartbeatInterval`, `ShutdownTimeout`; `static Fast(PgBoltServerOptions)`, `static FastQueue(QueueOptions)`, `static SlowHeartbeat(PgBoltServerOptions)`.
  - `[CollectionDefinition] TimingSensitiveCollection` (`Name = "timing-sensitive"`).
  - `abstract class ServerIntegrationTestBase(PostgresFixture, ConnectionMode) : IAsyncLifetime` — `Db`, `Probe`, `Client` (отдельный клиент без сервера, напрямую/через PgBouncer), `ImmediateRetry`, `StartHostAsync`, `StartProxy`, `Track`, `EnqueueAsync`, `EnqueueByNameAsync`, `WaitUntilAsync` (две перегрузки), `WaitForStateAsync`, `JobStateAsync`, `AttemptAsync`, `JobServerIdAsync`, `QueueRowAsync`, `EventsAsync`, `EventKindsAsync`, `CountAsync`, `ServerRowExistsAsync`, `HeartbeatAtAsync`, `DbNowAsync`, `DelayUntilDbTimeAsync`, `ParkedDequeueCountAsync`, `WaitForParkedDequeueAsync`.

- [x] **Step 1: Подключить `Microsoft.Extensions.Logging` к тестовому проекту**

`AddLogging` живёт в пакете `Microsoft.Extensions.Logging`, которого нет в `Directory.Packages.props` core. Если строки ещё нет, добавить её после `Microsoft.Extensions.Logging.Abstractions`:

```xml
    <PackageVersion Include="Microsoft.Extensions.Logging" Version="10.0.0" />
```

В `tests/PgBolt.Server.Tests/PgBolt.Server.Tests.csproj` должны присутствовать (добавить недостающие строки, существующие из Task 8 не дублировать):

```xml
  <ItemGroup>
    <Using Include="Xunit" />
  </ItemGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" />
    <PackageReference Include="Microsoft.Extensions.Logging" />
    <ProjectReference Include="..\PgBolt.Testing\PgBolt.Testing.csproj" />
  </ItemGroup>
```

Run: `dotnet build tests/PgBolt.Server.Tests`
Expected: сборка без ошибок и предупреждений.

- [x] **Step 2: Написать тесты прокси**

`tests/PgBolt.Server.Tests/Integration/TcpProxyTests.cs`:

```csharp
using System.Net;
using System.Net.Sockets;
using System.Text;
using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>TcpProxy против локального echo-сервера (без PostgreSQL).</summary>
public sealed class TcpProxyTests : IAsyncLifetime
{
    private readonly TcpListener _echo = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private Task _echoLoop = Task.CompletedTask;
    private TcpProxy _proxy = null!;
    private int _received;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Io = TimeSpan.FromSeconds(5);

    public ValueTask InitializeAsync()
    {
        _echo.Start();
        _echoLoop = EchoLoopAsync();
        _proxy = TcpProxy.Start("127.0.0.1", ((IPEndPoint)_echo.LocalEndpoint).Port);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _proxy.DisposeAsync();
        await _stop.CancelAsync();
        _echo.Stop();
        await _echoLoop;
        _stop.Dispose();
    }

    [Fact]
    public async Task Bytes_pass_through_in_both_directions()
    {
        using var client = await ConnectAsync();
        var stream = client.GetStream();

        await stream.WriteAsync("ping"u8.ToArray(), Ct);

        Assert.Equal("ping", await ReadAsync(stream, 4).WaitAsync(Io, Ct));
    }

    [Fact]
    public async Task Pause_holds_bytes_without_breaking_the_connection_and_resume_delivers_them()
    {
        using var client = await ConnectAsync();
        var stream = client.GetStream();
        await stream.WriteAsync("warm"u8.ToArray(), Ct);
        Assert.Equal("warm", await ReadAsync(stream, 4).WaitAsync(Io, Ct));

        _proxy.Pause();
        await stream.WriteAsync("held"u8.ToArray(), Ct);
        var read = ReadAsync(stream, 4);
        await Task.Delay(500, Ct); // явно: за 500 мс паузы байты не прошли

        Assert.False(read.IsCompleted, "bytes must not pass while the proxy is paused");
        Assert.True(_proxy.IsPaused);
        Assert.Equal(1, _proxy.ActiveConnections);
        Assert.Equal(4, Volatile.Read(ref _received));

        _proxy.Resume();

        Assert.Equal("held", await read.WaitAsync(Io, Ct));
    }

    [Fact]
    public async Task Side_closing_during_pause_drops_the_connection_and_the_held_bytes()
    {
        using (var client = await ConnectAsync())
        {
            var stream = client.GetStream();
            await stream.WriteAsync("warm"u8.ToArray(), Ct);
            Assert.Equal("warm", await ReadAsync(stream, 4).WaitAsync(Io, Ct));

            _proxy.Pause();
            await stream.WriteAsync("lost"u8.ToArray(), Ct);
        }

        await WaitAsync(() => _proxy.ActiveConnections == 0, "proxy tears down the connection closed during pause");
        _proxy.Resume();
        await Task.Delay(300, Ct); // явно: после Resume удержанные байты не дописываются

        Assert.Equal(4, Volatile.Read(ref _received));
    }

    [Fact]
    public async Task DropAll_breaks_existing_connections_and_new_ones_work()
    {
        using var first = await ConnectAsync();
        var s1 = first.GetStream();
        await s1.WriteAsync("one1"u8.ToArray(), Ct);
        Assert.Equal("one1", await ReadAsync(s1, 4).WaitAsync(Io, Ct));

        _proxy.DropAll();

        Assert.True(await IsClosedAsync(s1), "DropAll must break the existing connection");
        using var second = await ConnectAsync();
        var s2 = second.GetStream();
        await s2.WriteAsync("two2"u8.ToArray(), Ct);
        Assert.Equal("two2", await ReadAsync(s2, 4).WaitAsync(Io, Ct));
    }

    [Fact]
    public void ConnectionStringVia_rewrites_only_host_and_port()
    {
        var via = _proxy.ConnectionStringVia(
            "Host=db.example;Port=6432;Database=pgbolt;Username=u;Password=p;No Reset On Close=true;Max Auto Prepare=0");
        var b = new NpgsqlConnectionStringBuilder(via);

        Assert.Equal("127.0.0.1", b.Host);
        Assert.Equal(_proxy.Port, b.Port);
        Assert.Equal("pgbolt", b.Database);
        Assert.Equal("u", b.Username);
        Assert.True(b.NoResetOnClose);
        Assert.Equal(0, b.MaxAutoPrepare);
    }

    private async Task<TcpClient> ConnectAsync()
    {
        var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, _proxy.Port, Ct);
        return client;
    }

    private static async Task<string> ReadAsync(NetworkStream stream, int count)
    {
        var buffer = new byte[count];
        await stream.ReadExactlyAsync(buffer, Ct);
        return Encoding.ASCII.GetString(buffer);
    }

    private static async Task<bool> IsClosedAsync(NetworkStream stream)
    {
        var buffer = new byte[1];
        try
        {
            return await stream.ReadAsync(buffer, Ct).AsTask().WaitAsync(Io, Ct) == 0;
        }
        catch (IOException)
        {
            return true; // RST от прокси
        }
    }

    private static async Task WaitAsync(Func<bool> condition, string because)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (!condition())
        {
            if (System.Diagnostics.Stopwatch.GetElapsedTime(started) > Io)
                Assert.Fail($"Timed out waiting for: {because}");
            await Task.Delay(20, Ct);
        }
    }

    private async Task EchoLoopAsync()
    {
        var sessions = new List<Task>();
        while (true)
        {
            TcpClient accepted;
            try
            {
                accepted = await _echo.AcceptTcpClientAsync(_stop.Token);
            }
            catch (Exception e) when (e is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                break; // echo-сервер остановлен
            }
            sessions.Add(EchoAsync(accepted));
        }
        await Task.WhenAll(sessions);
    }

    private async Task EchoAsync(TcpClient accepted)
    {
        using (accepted)
        {
            var stream = accepted.GetStream();
            var buffer = new byte[1024];
            try
            {
                int n;
                while ((n = await stream.ReadAsync(buffer, _stop.Token)) > 0)
                {
                    Interlocked.Add(ref _received, n);
                    await stream.WriteAsync(buffer.AsMemory(0, n), _stop.Token);
                }
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
            {
                return; // соединение оборвано прокси или тестом
            }
        }
    }
}
```

- [x] **Step 3: Убедиться, что тесты не собираются**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~TcpProxyTests"`
Expected: FAIL — ошибка компиляции `CS0246: The type or namespace name 'TcpProxy' could not be found`.

- [x] **Step 4: Реализовать прокси**

`tests/PgBolt.Testing/TcpProxy.cs`:

```csharp
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Npgsql;

namespace PgBolt.Testing;

/// <summary>
/// Управляемый TCP-прокси 127.0.0.1:случайный порт → target. <see cref="Pause"/> перестаёт пересылать байты в обе
/// стороны, не разрывая соединений: прочитанное держится в памяти и уходит после <see cref="Resume"/> (фрагмент, запись
/// которого уже началась в момент паузы, может пройти). Если во время паузы одна из сторон закрыла соединение, оно
/// рвётся целиком, а удержанные байты отбрасываются — как пакеты, потерянные вместе с соединением: отменённый клиентом
/// запрос не выполнится на сервере после восстановления. <see cref="DropAll"/> рвёт все текущие соединения (RST).
/// Установка TCP-соединения с target паузой не блокируется — задерживаются только данные.
/// </summary>
public sealed class TcpProxy : IAsyncDisposable
{
    private readonly string _targetHost;
    private readonly int _targetPort;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<Link, byte> _links = new();
    private readonly Lock _pauseGate = new();
    private readonly Task _acceptLoop;
    private TaskCompletionSource _resumed = NewResumed(completed: true);
    private volatile bool _paused;

    private TcpProxy(string targetHost, int targetPort)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = AcceptLoopAsync();
    }

    public int Port { get; }
    public bool IsPaused => _paused;
    public int ActiveConnections => _links.Count;

    public static TcpProxy Start(string targetHost, int targetPort) => new(targetHost, targetPort);

    /// <summary>Прокси к Host/Port строки подключения Npgsql.</summary>
    public static TcpProxy For(string connectionString)
    {
        var csb = new NpgsqlConnectionStringBuilder(connectionString);
        var host = csb.Host ?? throw new ArgumentException("Connection string has no Host.", nameof(connectionString));
        return Start(host, csb.Port);
    }

    /// <summary>Та же строка подключения, но через прокси: меняются только Host и Port.</summary>
    public string ConnectionStringVia(string connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString) { Host = "127.0.0.1", Port = Port }.ConnectionString;

    public void Pause()
    {
        lock (_pauseGate)
        {
            if (_paused)
                return;
            _resumed = NewResumed(completed: false);
            _paused = true;
        }
    }

    public void Resume()
    {
        TaskCompletionSource resumed;
        lock (_pauseGate)
        {
            if (!_paused)
                return;
            _paused = false;
            resumed = _resumed;
        }
        resumed.TrySetResult();
    }

    public void DropAll()
    {
        foreach (var link in _links.Keys)
            link.Abort();
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        Resume();
        DropAll();
        await _acceptLoop;
        _stop.Dispose();
    }

    private static TaskCompletionSource NewResumed(bool completed)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (completed)
            tcs.SetResult();
        return tcs;
    }

    private Task WaitResumedAsync(CancellationToken ct)
    {
        Task resumed;
        lock (_pauseGate)
            resumed = _resumed.Task;
        return resumed.WaitAsync(ct);
    }

    private async Task AcceptLoopAsync()
    {
        while (true)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stop.Token);
            }
            catch (Exception e) when (e is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                return; // прокси остановлен
            }
            _ = ConnectAsync(client);
        }
    }

    private async Task ConnectAsync(TcpClient client)
    {
        var target = new TcpClient();
        try
        {
            await target.ConnectAsync(_targetHost, _targetPort, _stop.Token);
        }
        catch (Exception e) when (e is SocketException or OperationCanceledException or ObjectDisposedException)
        {
            Link.Kill(client);
            Link.Kill(target);
            return;
        }

        client.NoDelay = true;
        target.NoDelay = true;
        var link = new Link(this, client, target);
        _links.TryAdd(link, 0);
        if (_stop.IsCancellationRequested)
        {
            link.Abort();
            return;
        }
        await link.RunAsync();
    }

    private sealed class Link
    {
        private readonly TcpProxy _proxy;
        private readonly TcpClient _client;
        private readonly TcpClient _target;
        private readonly NetworkStream _clientStream;
        private readonly NetworkStream _targetStream;
        private readonly CancellationTokenSource _cts = new();
        private int _aborted;

        public Link(TcpProxy proxy, TcpClient client, TcpClient target)
        {
            _proxy = proxy;
            _client = client;
            _target = target;
            _clientStream = client.GetStream();
            _targetStream = target.GetStream();
        }

        public Task RunAsync()
        {
            var up = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
            var down = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
            return Task.WhenAll(
                ReadLoopAsync(_clientStream, up.Writer),
                WriteLoopAsync(up.Reader, _targetStream),
                ReadLoopAsync(_targetStream, down.Writer),
                WriteLoopAsync(down.Reader, _clientStream));
        }

        /// <summary>Читает непрерывно (и во время паузы), чтобы заметить закрытие стороны.</summary>
        private async Task ReadLoopAsync(NetworkStream source, ChannelWriter<byte[]> pending)
        {
            var buffer = new byte[16 * 1024];
            try
            {
                while (true)
                {
                    var n = await source.ReadAsync(buffer, _cts.Token);
                    if (n == 0)
                        break;
                    pending.TryWrite(buffer.AsSpan(0, n).ToArray());
                }
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
            {
                Abort();
                return;
            }

            if (_proxy.IsPaused)
                Abort();                // сторона закрылась во время паузы: удержанное теряется вместе с соединением
            else
                pending.TryComplete();  // дописать остаток, затем закрыть обе стороны (WriteLoopAsync)
        }

        private async Task WriteLoopAsync(ChannelReader<byte[]> pending, NetworkStream destination)
        {
            try
            {
                await foreach (var chunk in pending.ReadAllAsync(_cts.Token))
                {
                    await _proxy.WaitResumedAsync(_cts.Token);
                    await destination.WriteAsync(chunk, _cts.Token);
                }
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
            {
                Abort();
                return;
            }
            Abort(); // источник закрылся и остаток дописан
        }

        public void Abort()
        {
            if (Interlocked.Exchange(ref _aborted, 1) == 1)
                return;
            _proxy._links.TryRemove(this, out _);
            _cts.Cancel();
            Kill(_client);
            Kill(_target);
        }

        public static void Kill(TcpClient connection)
        {
            if (connection.Client is { Connected: true } socket)
                socket.LingerState = new LingerOption(true, 0); // RST вместо FIN
            connection.Dispose();
        }
    }
}
```

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~TcpProxyTests"`
Expected: PASS — 5 тестов.

- [x] **Step 5: Тестовые задачи и `JobProbe`**

`tests/PgBolt.Server.Tests/Integration/TestJobs.cs`:

```csharp
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization;
using Npgsql;

namespace PgBolt.Server.Tests.Integration;

public sealed record SuccessArgs(int N);
public sealed record FailArgs(string Message);
/// <summary>Бросает, пока context.Attempt ≤ FailUntilAttempt, затем успешна.</summary>
public sealed record FlakyArgs(int FailUntilAttempt);
public sealed record AbortArgs(string Reason);
/// <summary>Ждёт JobProbe.Release или отмены токена (корректно останавливается).</summary>
public sealed record BlockArgs(int N);
/// <summary>Ждёт JobProbe.Release, токен игнорирует (отмену только фиксирует в JobRun).</summary>
public sealed record StubbornArgs(int N);
public sealed record SlowArgs(int Milliseconds);
/// <summary>Тип с [Job(TimeoutSeconds = 1)]: ждёт отмены токена.</summary>
public sealed record TimeLimitedArgs(int N);
/// <summary>Держит Connections соединений пула NpgsqlDataSource хоста, пока токен не отменён.</summary>
public sealed record PoolHogArgs(int Connections);
/// <summary>Публикуется только хостом с TestJobs.RegisterWithOrphan — у остальных для него нет исполнителя.</summary>
public sealed record OrphanArgs(int N);

[JsonSerializable(typeof(SuccessArgs))]
[JsonSerializable(typeof(FailArgs))]
[JsonSerializable(typeof(FlakyArgs))]
[JsonSerializable(typeof(AbortArgs))]
[JsonSerializable(typeof(BlockArgs))]
[JsonSerializable(typeof(StubbornArgs))]
[JsonSerializable(typeof(SlowArgs))]
[JsonSerializable(typeof(TimeLimitedArgs))]
[JsonSerializable(typeof(PoolHogArgs))]
[JsonSerializable(typeof(OrphanArgs))]
internal sealed partial class TestJobsJson : JsonSerializerContext;

/// <summary>
/// Дескрипторы задач. Это единственные экземпляры на процесс: JobMetadata&lt;TArgs&gt; — статический и глобальный,
/// повторная регистрация того же args-типа с другим дескриптором бросает. Вариации (очередь, MaxAttempts, задержки
/// повтора) задаются через EnqueueOptions.
/// </summary>
public static class TestJobs
{
    public const string Queue = "q";

    public static readonly JobDescriptor Success = new("test.success", Queue, 3, 0, null);
    public static readonly JobDescriptor Fail = new("test.fail", Queue, 3, 0, null);
    public static readonly JobDescriptor Flaky = new("test.flaky", Queue, 3, 0, [0]);
    public static readonly JobDescriptor Abort = new("test.abort", Queue, 3, 0, null);
    public static readonly JobDescriptor Block = new("test.block", Queue, 3, 0, [0]);
    public static readonly JobDescriptor Stubborn = new("test.stubborn", Queue, 3, 0, [0]);
    public static readonly JobDescriptor Slow = new("test.slow", Queue, 3, 0, null);
    public static readonly JobDescriptor TimeLimited = new("test.timeout", Queue, 2, 0, [0]) { TimeoutSeconds = 1 };
    public static readonly JobDescriptor PoolHog = new("test.poolhog", Queue, 2, 0, [0]);
    public static readonly JobDescriptor Orphan = new("test.orphan", Queue, 2, 0, [0]);

    /// <summary>Все исполнители, кроме test.orphan.</summary>
    public static void Register(IPgBoltJobsBuilder b)
    {
        b.AddExecutor<SuccessArgs, SuccessJob>(Success, TestJobsJson.Default.SuccessArgs);
        b.AddExecutor<FailArgs, FailJob>(Fail, TestJobsJson.Default.FailArgs);
        b.AddExecutor<FlakyArgs, FlakyJob>(Flaky, TestJobsJson.Default.FlakyArgs);
        b.AddExecutor<AbortArgs, AbortJob>(Abort, TestJobsJson.Default.AbortArgs);
        b.AddExecutor<BlockArgs, BlockJob>(Block, TestJobsJson.Default.BlockArgs);
        b.AddExecutor<StubbornArgs, StubbornJob>(Stubborn, TestJobsJson.Default.StubbornArgs);
        b.AddExecutor<SlowArgs, SlowJob>(Slow, TestJobsJson.Default.SlowArgs);
        b.AddExecutor<TimeLimitedArgs, TimeLimitedJob>(TimeLimited, TestJobsJson.Default.TimeLimitedArgs);
        b.AddExecutor<PoolHogArgs, PoolHogJob>(PoolHog, TestJobsJson.Default.PoolHogArgs);
    }

    public static void RegisterWithOrphan(IPgBoltJobsBuilder b)
    {
        Register(b);
        b.AddExecutor<OrphanArgs, OrphanJob>(Orphan, TestJobsJson.Default.OrphanArgs);
    }
}

/// <summary>Имя ServerTestHost, на котором выполняется задача (синглтон в DI каждого хоста).</summary>
public sealed record HostTag(string Name);

public enum RunEnd { Running, Completed, Threw }

/// <summary>Одно выполнение (попытка) задачи. Метки времени — Stopwatch.GetTimestamp(), 0 — не было.</summary>
public sealed class JobRun
{
    private long _cancelledAt;
    private long _finishedAt;
    private int _end;

    internal JobRun(Guid jobId, int attempt, string host)
    {
        JobId = jobId;
        Attempt = attempt;
        Host = host;
        StartedAt = Stopwatch.GetTimestamp();
    }

    public Guid JobId { get; }
    public int Attempt { get; }
    public string Host { get; }
    public long StartedAt { get; }
    /// <summary>Момент отмены токена выполнения (любой причиной); 0 — токен не отменялся.</summary>
    public long CancelledAt => Volatile.Read(ref _cancelledAt);
    public long FinishedAt => Volatile.Read(ref _finishedAt);
    public RunEnd End => (RunEnd)Volatile.Read(ref _end);
    public bool WasCancelled => CancelledAt != 0;
    public TimeSpan Duration => Stopwatch.GetElapsedTime(StartedAt, FinishedAt);

    internal void MarkCancelled() => Interlocked.CompareExchange(ref _cancelledAt, Stopwatch.GetTimestamp(), 0);

    internal void Finish(RunEnd end)
    {
        Volatile.Write(ref _finishedAt, Stopwatch.GetTimestamp());
        Volatile.Write(ref _end, (int)end);
    }
}

/// <summary>
/// Управляющий синглтон теста: общий для всех хостов теста (передаётся в ServerTestHost.StartAsync), считает выполнения
/// по job_id и держит «ворота» Block/Stubborn-задач по (job_id, attempt).
/// </summary>
public sealed class JobProbe
{
    private readonly ConcurrentDictionary<Guid, List<JobRun>> _runs = new();
    private readonly Lock _gatesLock = new();
    private readonly Dictionary<(Guid JobId, int Attempt), TaskCompletionSource> _gates = [];
    private readonly HashSet<Guid> _releasedJobs = [];
    private bool _releasedAll;
    private int _started;
    private int _completed;

    public int TotalStarted => Volatile.Read(ref _started);
    public int TotalCompleted => Volatile.Read(ref _completed);

    public IReadOnlyList<JobRun> Runs(JobId id)
    {
        if (!_runs.TryGetValue(id.Value, out var list))
            return [];
        lock (list)
            return list.ToArray();
    }

    public JobRun? Run(JobId id, int attempt) => Runs(id).FirstOrDefault(r => r.Attempt == attempt);

    public int RunCount(JobId id) => Runs(id).Count;

    public IReadOnlyDictionary<Guid, int> RunCounts() => _runs.ToDictionary(p => p.Key, p => { lock (p.Value) return p.Value.Count; });

    /// <summary>Открывает ворота задачи: attempt = null — все попытки, включая будущие.</summary>
    public void Release(JobId id, int? attempt = null)
    {
        List<TaskCompletionSource> open;
        lock (_gatesLock)
        {
            if (attempt is null)
                _releasedJobs.Add(id.Value);
            else if (!_gates.ContainsKey((id.Value, attempt.Value)))
                _gates[(id.Value, attempt.Value)] = NewGate();
            open = _gates.Where(p => p.Key.JobId == id.Value && (attempt is null || p.Key.Attempt == attempt))
                .Select(p => p.Value).ToList();
        }
        foreach (var gate in open)
            gate.TrySetResult();
    }

    /// <summary>Открывает все ворота (в том числе будущие) — вызывается при уборке теста.</summary>
    public void ReleaseAll()
    {
        List<TaskCompletionSource> open;
        lock (_gatesLock)
        {
            _releasedAll = true;
            open = _gates.Values.ToList();
        }
        foreach (var gate in open)
            gate.TrySetResult();
    }

    public Task GateAsync(Guid jobId, int attempt)
    {
        lock (_gatesLock)
        {
            if (!_gates.TryGetValue((jobId, attempt), out var gate))
            {
                gate = NewGate();
                if (_releasedAll || _releasedJobs.Contains(jobId))
                    gate.SetResult();
                _gates[(jobId, attempt)] = gate;
            }
            return gate.Task;
        }
    }

    public async Task RunAsync(JobContext context, HostTag host, CancellationToken ct, Func<JobRun, Task> body)
    {
        var run = new JobRun(context.JobId.Value, context.Attempt, host.Name);
        var list = _runs.GetOrAdd(run.JobId, static _ => new List<JobRun>(1));
        lock (list)
            list.Add(run);
        Interlocked.Increment(ref _started);
        using var registration = ct.Register(static state => ((JobRun)state!).MarkCancelled(), run);
        try
        {
            await body(run);
        }
        catch
        {
            // Колбэки токена идут в обратном порядке регистрации: WaitAsync(ct) тела завершает его синхронно внутри
            // Cancel(), и Dispose регистрации выше снимает MarkCancelled до вызова — отмену фиксируем здесь.
            if (ct.IsCancellationRequested)
                run.MarkCancelled();
            run.Finish(RunEnd.Threw);
            throw;
        }
        if (ct.IsCancellationRequested)
            run.MarkCancelled();
        run.Finish(RunEnd.Completed);
        Interlocked.Increment(ref _completed);
    }

    private static TaskCompletionSource NewGate() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class SuccessJob(JobProbe probe, HostTag host) : IJob<SuccessArgs>
{
    public Task ExecuteAsync(SuccessArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, static _ => Task.CompletedTask);
}

public sealed class FailJob(JobProbe probe, HostTag host) : IJob<FailArgs>
{
    public Task ExecuteAsync(FailArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => throw new InvalidOperationException(args.Message));
}

public sealed class FlakyJob(JobProbe probe, HostTag host) : IJob<FlakyArgs>
{
    public Task ExecuteAsync(FlakyArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => context.Attempt <= args.FailUntilAttempt
            ? throw new InvalidOperationException($"flaky attempt {context.Attempt}")
            : Task.CompletedTask);
}

public sealed class AbortJob(JobProbe probe, HostTag host) : IJob<AbortArgs>
{
    public Task ExecuteAsync(AbortArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => throw new JobAbortedException(args.Reason));
}

public sealed class BlockJob(JobProbe probe, HostTag host) : IJob<BlockArgs>
{
    public Task ExecuteAsync(BlockArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken,
            run => probe.GateAsync(run.JobId, run.Attempt).WaitAsync(cancellationToken));
}

public sealed class StubbornJob(JobProbe probe, HostTag host) : IJob<StubbornArgs>
{
    public Task ExecuteAsync(StubbornArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, run => probe.GateAsync(run.JobId, run.Attempt));
}

public sealed class SlowJob(JobProbe probe, HostTag host) : IJob<SlowArgs>
{
    public Task ExecuteAsync(SlowArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => Task.Delay(args.Milliseconds, cancellationToken));
}

public sealed class TimeLimitedJob(JobProbe probe, HostTag host) : IJob<TimeLimitedArgs>
{
    public Task ExecuteAsync(TimeLimitedArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, _ => Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken));
}

/// <summary>Берёт соединения из того же NpgsqlDataSource, что и runtime (TestHostSettings.DataSource).</summary>
public sealed class PoolHogJob(JobProbe probe, HostTag host, NpgsqlDataSource dataSource) : IJob<PoolHogArgs>
{
    public Task ExecuteAsync(PoolHogArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, async _ =>
        {
            var held = new List<NpgsqlConnection>();
            try
            {
                for (var i = 0; i < args.Connections; i++)
                    held.Add(await dataSource.OpenConnectionAsync(cancellationToken));
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            finally
            {
                foreach (var connection in held)
                    await connection.DisposeAsync();
            }
        });
}

public sealed class OrphanJob(JobProbe probe, HostTag host) : IJob<OrphanArgs>
{
    public Task ExecuteAsync(OrphanArgs args, JobContext context, CancellationToken cancellationToken) =>
        probe.RunAsync(context, host, cancellationToken, static _ => Task.CompletedTask);
}
```

- [x] **Step 6: Хост, сбор логов и базовый класс тестов**

`tests/PgBolt.Server.Tests/Integration/ServerTestHost.cs`:

```csharp
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PgBolt.Server.Runtime;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Коды jobs.state.</summary>
public static class DbState
{
    public const short Pending = 0;
    public const short Succeeded = 2;
    public const short Failed = 3;
    public const short Cancelled = 4;
}

public sealed record QueueRow(int Attempt, Guid? LeaseOwner, Guid? LeaseId, DateTime? LeaseUntil, bool Ready, bool CancelRequested);

public sealed record EventRow(JobEventKind Kind, int? Attempt, Guid? ServerId, string? Message);

public sealed record LogEntry(string Category, LogLevel Level, string Message);

/// <summary>ILoggerProvider-заглушка: собирает сообщения хоста для проверок (например, предупреждения §4.4 п. 4).</summary>
public sealed class CollectingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<LogEntry> _entries = new();

    public IReadOnlyCollection<LogEntry> Entries => _entries.ToArray();

    public ILogger CreateLogger(string categoryName) => new Collector(this, categoryName);

    public void Dispose()
    {
    }

    private sealed class Collector(CollectingLoggerProvider owner, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            owner._entries.Enqueue(new LogEntry(category, logLevel, formatter(state, exception)));
    }
}

public sealed class TestHostSettings
{
    internal List<(string Name, Action<PgBoltServerOptions> Configure)> Servers { get; } = [];

    /// <summary>Имя хоста — попадает в JobRun.Host.</summary>
    public string Name { get; set; } = "host";

    /// <summary>Подключение runtime через прокси (Cancellation Timeout = -1: отменённый запрос сразу рвёт соединение).</summary>
    public TcpProxy? Proxy { get; set; }

    /// <summary>Пользовательский источник данных (o.DataSource); регистрируется и в DI — для PoolHogJob. Важнее Proxy.</summary>
    public NpgsqlDataSource? DataSource { get; set; }

    public Action<IPgBoltJobsBuilder> Jobs { get; set; } = TestJobs.Register;

    public bool RunMaintenance { get; set; } = true;

    /// <summary>Применяется к каждому серверу после Fast и до его собственной настройки. Очереди здесь не добавлять.</summary>
    public Action<PgBoltServerOptions>? Configure { get; set; }

    /// <summary>Виртуальный сервер; без вызова — один сервер "s1" с очередью TestJobs.Queue.</summary>
    public TestHostSettings AddServer(string name, Action<PgBoltServerOptions> configure)
    {
        Servers.Add((name, configure));
        return this;
    }
}

/// <summary>
/// «Процесс» с PgBolt-сервером: свой ServiceProvider, hosted services, пул соединений и IWakeupSignal. Несколько хостов
/// в одном тесте работают на одной схеме как разные процессы.
/// </summary>
public sealed class ServerTestHost : IAsyncDisposable
{
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(3);
    public static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(2);

    private readonly ServiceProvider _provider;
    private readonly List<IHostedService> _started = [];
    private bool _stopped;

    private ServerTestHost(string name, ServiceProvider provider, CollectingLoggerProvider logs)
    {
        Name = name;
        _provider = provider;
        Logs = logs;
        HostedServices = provider.GetServices<IHostedService>().ToList();
        Processing = HostedServices.OfType<PgBoltProcessingService>().Single();
    }

    public string Name { get; }
    public IServiceProvider Services => _provider;
    public IJobClient Client => _provider.GetRequiredService<IJobClient>();
    public CollectingLoggerProvider Logs { get; }
    public IReadOnlyList<IHostedService> HostedServices { get; }
    internal PgBoltProcessingService Processing { get; }

    /// <summary>server_id виртуальных серверов (после StartAsync).</summary>
    public IReadOnlyList<Guid> ServerIds => Processing.Servers.Select(s => s.Id).ToList();

    public Guid ServerId => Assert.Single(ServerIds);

    public static void Fast(PgBoltServerOptions s)
    {
        s.Workers = 8;
        s.LeaseDuration = LeaseDuration;
        s.HeartbeatInterval = HeartbeatInterval;
        s.ShutdownTimeout = ShutdownTimeout;
        s.AckFlushInterval = TimeSpan.FromMilliseconds(10);
        s.RetryPolicy = RetryPolicy.Exponential(TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1), 0);
    }

    public static void FastQueue(QueueOptions q) => q.MaxPollInterval = TimeSpan.FromMilliseconds(200);

    /// <summary>L = 30 с, H = 10 с: heartbeat гарантированно не срабатывает в окне сценария.</summary>
    public static void SlowHeartbeat(PgBoltServerOptions s)
    {
        s.LeaseDuration = TimeSpan.FromSeconds(30);
        s.HeartbeatInterval = TimeSpan.FromSeconds(10);
    }

    public static async Task<ServerTestHost> StartAsync(TestDb db, JobProbe probe, Action<TestHostSettings>? configure,
        CancellationToken ct)
    {
        var settings = new TestHostSettings();
        configure?.Invoke(settings);
        if (settings.Servers.Count == 0)
            settings.AddServer("s1", s => s.Queue(TestJobs.Queue, FastQueue));

        var logs = new CollectingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Information).AddProvider(logs));
        services.AddSingleton(probe);
        services.AddSingleton(new HostTag(settings.Name));
        if (settings.DataSource is { } dataSource)
            services.AddSingleton(dataSource);

        var connectionString = settings.Proxy is { } proxy
            ? new NpgsqlConnectionStringBuilder(proxy.ConnectionStringVia(db.ConnectionString)) { CancellationTimeout = -1 }.ConnectionString
            : db.ConnectionString;
        services.AddPgBolt(o =>
        {
            if (settings.DataSource is { } source)
                o.DataSource = source;
            else
                o.ConnectionString = connectionString;
            o.Schema = db.Schema;
            o.AddJobs(settings.Jobs);
        });
        foreach (var (name, configureServer) in settings.Servers)
            services.AddPgBoltServer(name, s =>
            {
                Fast(s);
                settings.Configure?.Invoke(s);
                configureServer(s);
            });
        services.ConfigurePgBoltProcessing(p =>
        {
            p.RunMaintenance = settings.RunMaintenance;
            p.PromoterInterval = TimeSpan.FromMilliseconds(200);
            p.SweeperInterval = TimeSpan.FromMilliseconds(500);
        });

        var host = new ServerTestHost(settings.Name, services.BuildServiceProvider(), logs);
        try
        {
            foreach (var hosted in host.HostedServices)
            {
                await hosted.StartAsync(ct);
                host._started.Add(hosted);
            }
        }
        catch
        {
            await host.DisposeAsync();
            throw;
        }
        return host;
    }

    /// <summary>Останавливает hosted services в обратном порядке (как хост). Идемпотентно. Возвращает длительность.</summary>
    public async Task<TimeSpan> StopAsync()
    {
        if (_stopped)
            return TimeSpan.Zero;
        _stopped = true;
        var clock = Stopwatch.StartNew();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        for (var i = _started.Count - 1; i >= 0; i--)
            await _started[i].StopAsync(deadline.Token);
        return clock.Elapsed;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _provider.DisposeAsync();
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TimingSensitiveCollection
{
    public const string Name = "timing-sensitive";
}

public abstract class ServerIntegrationTestBase(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private readonly List<ServerTestHost> _hosts = [];
    private readonly List<TcpProxy> _proxies = [];
    private readonly List<IAsyncDisposable> _tracked = [];
    private ServiceProvider _clientServices = null!;

    /// <summary>retry_delays = [0]: повтор сразу (ready = true), без promoter.</summary>
    protected static readonly EnqueueOptions ImmediateRetry = new() { RetryDelays = [TimeSpan.Zero] };

    protected PostgresFixture Pg { get; } = pg;
    protected ConnectionMode Mode { get; } = mode;
    protected TestDb Db { get; private set; } = null!;
    protected JobProbe Probe { get; } = new();
    /// <summary>Клиент без сервера, в обход прокси: enqueue работает, даже когда прокси хоста на паузе.</summary>
    protected IJobClient Client { get; private set; } = null!;
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    public virtual async ValueTask InitializeAsync()
    {
        Db = await TestDb.CreateAsync(Pg, Mode);
        var services = new ServiceCollection();
        services.AddPgBolt(o =>
        {
            o.ConnectionString = Db.ConnectionString;
            o.Schema = Db.Schema;
            o.AddJobs(TestJobs.RegisterWithOrphan);
        });
        _clientServices = services.BuildServiceProvider();
        Client = _clientServices.GetRequiredService<IJobClient>();
    }

    public virtual async ValueTask DisposeAsync()
    {
        Probe.ReleaseAll();
        foreach (var proxy in _proxies)
            proxy.Resume();
        for (var i = _hosts.Count - 1; i >= 0; i--)
            await _hosts[i].DisposeAsync();
        foreach (var tracked in _tracked)
            await tracked.DisposeAsync();
        foreach (var proxy in _proxies)
            await proxy.DisposeAsync();
        await _clientServices.DisposeAsync();
        await Db.DisposeAsync();
    }

    protected async Task<ServerTestHost> StartHostAsync(Action<TestHostSettings>? configure = null)
    {
        var host = await ServerTestHost.StartAsync(Db, Probe, configure, Ct);
        _hosts.Add(host);
        return host;
    }

    /// <summary>Прокси к адресу текущего режима (PostgreSQL или PgBouncer).</summary>
    protected TcpProxy StartProxy()
    {
        var proxy = TcpProxy.For(Pg.ConnectionString(Mode));
        _proxies.Add(proxy);
        return proxy;
    }

    /// <summary>Освобождается после остановки хостов (например, NpgsqlDataSource, отданный хосту).</summary>
    protected T Track<T>(T disposable) where T : IAsyncDisposable
    {
        _tracked.Add(disposable);
        return disposable;
    }

    protected async Task<JobId> EnqueueAsync<TArgs>(TArgs args, EnqueueOptions? options = null, IJobClient? client = null)
    {
        var result = await (client ?? Client).EnqueueAsync(args, options, Ct);
        Assert.True(result.Created);
        return result.Id ?? throw new InvalidOperationException("Created job without id.");
    }

    protected async Task<JobId> EnqueueByNameAsync(string jobName, string argsJson, EnqueueOptions? options = null)
    {
        using var doc = JsonDocument.Parse(argsJson);
        var result = await Client.EnqueueAsync(jobName, doc.RootElement, options, Ct);
        Assert.True(result.Created);
        return result.Id ?? throw new InvalidOperationException("Created job without id.");
    }

    protected static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string because)
    {
        var started = Stopwatch.GetTimestamp();
        while (!await condition())
        {
            if (Stopwatch.GetElapsedTime(started) > timeout)
                Assert.Fail($"Timed out after {timeout.TotalSeconds:0.##} s waiting for: {because}");
            await Task.Delay(25, Ct);
        }
    }

    protected static Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string because) =>
        WaitUntilAsync(() => Task.FromResult(condition()), timeout, because);

    protected async Task WaitForStateAsync(JobId id, short state, TimeSpan timeout)
    {
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            var current = await Db.ScalarAsync<short?>("SELECT state FROM {schema}.jobs WHERE id = @id", ("id", id.Value));
            if (current == state)
                return;
            if (Stopwatch.GetElapsedTime(started) > timeout)
                Assert.Fail($"Job {id} did not reach state {state} within {timeout.TotalSeconds:0.##} s (last: {current?.ToString() ?? "no row"})");
            await Task.Delay(25, Ct);
        }
    }

    protected async Task<short> JobStateAsync(JobId id) =>
        await Db.ScalarAsync<short?>("SELECT state FROM {schema}.jobs WHERE id = @id", ("id", id.Value))
        ?? throw new InvalidOperationException($"No jobs row for {id}.");

    protected async Task<int> AttemptAsync(JobId id) =>
        await Db.ScalarAsync<int?>("SELECT attempt FROM {schema}.jobs WHERE id = @id", ("id", id.Value))
        ?? throw new InvalidOperationException($"No jobs row for {id}.");

    protected Task<Guid?> JobServerIdAsync(JobId id) =>
        Db.ScalarAsync<Guid?>("SELECT server_id FROM {schema}.jobs WHERE id = @id", ("id", id.Value));

    protected async Task<QueueRow?> QueueRowAsync(JobId id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql(
            "SELECT attempt, lease_owner, lease_id, lease_until, ready, cancel_requested FROM {schema}.queue WHERE job_id = @id"));
        cmd.Parameters.AddWithValue("id", id.Value);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        if (!await r.ReadAsync(Ct))
            return null;
        return new QueueRow(
            r.GetInt32(0),
            r.IsDBNull(1) ? null : r.GetGuid(1),
            r.IsDBNull(2) ? null : r.GetGuid(2),
            r.IsDBNull(3) ? null : r.GetDateTime(3),
            r.GetBoolean(4),
            r.GetBoolean(5));
    }

    protected async Task<List<EventRow>> EventsAsync(JobId id)
    {
        await using var cmd = Db.Admin.CreateCommand(Db.Sql(
            "SELECT kind, attempt, server_id, message FROM {schema}.job_events WHERE job_id = @id ORDER BY at, kind"));
        cmd.Parameters.AddWithValue("id", id.Value);
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var list = new List<EventRow>();
        while (await r.ReadAsync(Ct))
            list.Add(new EventRow(
                (JobEventKind)r.GetInt16(0),
                r.IsDBNull(1) ? null : r.GetInt32(1),
                r.IsDBNull(2) ? null : r.GetGuid(2),
                r.IsDBNull(3) ? null : r.GetString(3)));
        return list;
    }

    protected async Task<List<JobEventKind>> EventKindsAsync(JobId id) =>
        (await EventsAsync(id)).Select(e => e.Kind).ToList();

    protected Task<long> CountAsync(string sql, params (string Name, object? Value)[] ps) => Db.ScalarAsync<long>(sql, ps);

    protected Task<bool> ServerRowExistsAsync(Guid serverId) =>
        Db.ScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM {schema}.servers WHERE id = @id)", ("id", serverId));

    protected Task<DateTime> HeartbeatAtAsync(Guid serverId) =>
        Db.ScalarAsync<DateTime>("SELECT heartbeat_at FROM {schema}.servers WHERE id = @id", ("id", serverId));

    protected Task<DateTime> DbNowAsync() => Db.ScalarAsync<DateTime>("SELECT clock_timestamp()");

    /// <summary>Ждёт момента target по часам БД (явная задержка сценария).</summary>
    protected async Task DelayUntilDbTimeAsync(DateTime target)
    {
        var seconds = await Db.ScalarAsync<double>(
            "SELECT GREATEST(0, EXTRACT(EPOCH FROM (@t - clock_timestamp())))::float8", ("t", target));
        await Task.Delay(TimeSpan.FromSeconds(seconds), Ct);
    }

    /// <summary>Dequeue (§5.2 core), ждущий блокировку таблицы queue: виден в pg_stat_activity по тексту statement'а.</summary>
    protected Task<long> ParkedDequeueCountAsync() =>
        Db.ScalarAsync<long>(
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE @p",
            ("p", $"%{Db.Schema}.queue%attempt = q.attempt + 1%"));

    protected Task WaitForParkedDequeueAsync(TimeSpan timeout) =>
        WaitUntilAsync(async () => await ParkedDequeueCountAsync() > 0, timeout, "a dequeue parks on the queue table lock");
}
```

Run: `dotnet build tests/PgBolt.Server.Tests`
Expected: сборка без ошибок и предупреждений.

- [x] **Step 7: Написать базовые сценарии**

`tests/PgBolt.Server.Tests/Integration/BasicScenarioTests.cs`:

```csharp
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class BasicScenarioTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Enqueued_job_is_executed_and_succeeds()
    {
        var host = await StartHostAsync();
        var id = await EnqueueAsync(new SuccessArgs(1), client: host.Client);

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Equal(host.ServerId, await JobServerIdAsync(id));
        Assert.Null(await QueueRowAsync(id));
        Assert.Empty(await EventsAsync(id));
        var run = Assert.Single(Probe.Runs(id));
        Assert.Equal(RunEnd.Completed, run.End);
        Assert.Equal("host", run.Host);
    }

    [Fact]
    public async Task Five_hundred_jobs_are_executed_once_and_acked_in_batches()
    {
        var host = await StartHostAsync();
        var results = await host.Client.EnqueueManyAsync(
            Enumerable.Range(0, 500).Select(n => new SuccessArgs(n)).ToList(), ct: Ct);
        Assert.All(results, r => Assert.True(r.Created));

        await WaitUntilAsync(async () => await CountAsync("SELECT count(*) FROM {schema}.jobs WHERE state = 2") == 500,
            TimeSpan.FromSeconds(30), "all 500 jobs succeed");

        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.queue"));
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.job_events"));
        Assert.All(results, r => Assert.Equal(1, Probe.RunCount(r.Id ?? throw new InvalidOperationException())));
        // Один ack-statement — одно now(): число различных finished_at = число пакетов ack.
        var batches = await CountAsync("SELECT count(DISTINCT finished_at) FROM {schema}.jobs");
        Assert.True(batches < 250, $"acks must be batched: {batches} distinct ack statements for 500 jobs");
    }

    [Fact]
    public async Task Retry_with_delay_waits_for_promotion_and_succeeds_on_second_attempt()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new FlakyArgs(1), new EnqueueOptions { RetryDelays = [TimeSpan.FromSeconds(1)] });

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        var retry = Assert.Single(await EventsAsync(id));
        Assert.Equal(JobEventKind.Retry, retry.Kind);
        Assert.Equal(1, retry.Attempt);
        Assert.Contains("flaky attempt 1", retry.Message);
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        var gap = Stopwatch.GetElapsedTime(runs[0].FinishedAt, runs[1].StartedAt);
        Assert.True(gap >= TimeSpan.FromMilliseconds(900), $"second attempt must wait for run_at and promotion, gap {gap}");
    }

    [Fact]
    public async Task Exhausted_retries_end_in_failed_with_retry_events()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new FailArgs("boom"), ImmediateRetry);

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(15));

        Assert.Equal(3, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.Retry, JobEventKind.Retry, JobEventKind.Failed }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Equal("boom", e.Message));
        Assert.Equal(new int?[] { 1, 2, 3 }, events.Select(e => e.Attempt));
        Assert.Equal(3, Probe.RunCount(id));
        Assert.Null(await QueueRowAsync(id));
    }

    [Fact]
    public async Task JobAbortedException_fails_immediately()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new AbortArgs("pointless"));

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        var failed = Assert.Single(await EventsAsync(id));
        Assert.Equal(JobEventKind.Failed, failed.Kind);
        Assert.Equal("pointless", failed.Message);
        Assert.Equal(1, Probe.RunCount(id));
    }

    [Fact]
    public async Task Malformed_arguments_fail_immediately_without_invoking_the_job()
    {
        await StartHostAsync(); // публикует job_types — нужен для enqueue по имени
        var id = await EnqueueByNameAsync("test.success", """{"N":"not-a-number"}""");

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.Failed }, await EventKindsAsync(id));
        Assert.Equal(0, Probe.RunCount(id));
    }

    [Fact]
    public async Task Cancelling_a_running_job_is_delivered_by_heartbeat()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new BlockArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "job starts");

        Assert.Equal(CancelResult.CancellationRequested, await Client.CancelAsync(id, "test", Ct));
        await WaitForStateAsync(id, DbState.Cancelled, ServerTestHost.HeartbeatInterval + TimeSpan.FromSeconds(2));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Contains(JobEventKind.Cancelled, await EventKindsAsync(id));
        var run = Assert.Single(Probe.Runs(id));
        Assert.True(run.WasCancelled);
        Assert.Equal(RunEnd.Threw, run.End);
    }

    [Fact]
    public async Task Timeout_cancels_the_job_and_retries_with_timeout_message()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new TimeLimitedArgs(1)); // [Job(TimeoutSeconds = 1)], MaxAttempts = 2, retry_delays = [0]

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.Retry, JobEventKind.Failed }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Contains("timeout after 1 s", e.Message));
        Assert.All(Probe.Runs(id), r =>
        {
            Assert.True(r.WasCancelled);
            Assert.InRange(r.Duration, TimeSpan.FromMilliseconds(900), TimeSpan.FromSeconds(5));
        });
    }

    [Fact]
    public async Task Job_without_timeout_runs_longer_than_two_lease_durations()
    {
        await StartHostAsync();
        var id = await EnqueueAsync(new SlowArgs(7000)); // 7 с > 2 × LeaseDuration (3 с)

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(20));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Empty(await EventsAsync(id));
        var run = Assert.Single(Probe.Runs(id));
        Assert.False(run.WasCancelled);
        Assert.True(run.Duration >= 2 * ServerTestHost.LeaseDuration, $"run took {run.Duration}");
    }

    [Fact]
    public async Task Job_type_without_executor_is_warned_at_start_retried_as_NoExecutor_and_failed()
    {
        // «Другой процесс» публикует test.orphan (default_queue = "q"), сам его очередь не обслуживает.
        var publisher = await StartHostAsync(h =>
        {
            h.Name = "P";
            h.Jobs = TestJobs.RegisterWithOrphan;
            h.AddServer("publisher", s => s.Queue("p-idle", ServerTestHost.FastQueue));
        });
        await publisher.StopAsync();

        var executor = await StartHostAsync(h => h.Name = "E");
        Assert.Contains(executor.Logs.Entries,
            e => e.Level == LogLevel.Warning && e.Message.Contains("test.orphan", StringComparison.Ordinal));

        var id = await EnqueueByNameAsync("test.orphan", """{"N":1}"""); // max_attempts = 2, retry_delays = [0] из job_types

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.NoExecutor, JobEventKind.NoExecutor }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Contains("test.orphan", e.Message));
        Assert.Equal(0, Probe.RunCount(id));
    }
}

public sealed class BasicScenarioTests_Direct(PostgresFixture pg) : BasicScenarioTests(pg, ConnectionMode.Direct);
public sealed class BasicScenarioTests_Bouncer(PostgresFixture pg) : BasicScenarioTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 8: Прогнать сценарии**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~Integration"`
Expected: PASS — 5 тестов `TcpProxyTests` + 10 × 2 `BasicScenarioTests`. Падение сценария — дефект runtime: исполнитель находит задачу-владельца по «Покрытию спеки», сообщает `BLOCKED` с выводом теста; сценарий не меняется.

- [x] **Step 9: Commit**

```bash
git add Directory.Packages.props tests/PgBolt.Server.Tests/PgBolt.Server.Tests.csproj tests/PgBolt.Testing/TcpProxy.cs tests/PgBolt.Server.Tests/Integration
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(server): tcp proxy, integration host and basic end-to-end scenarios"
```

---

### Task 18: Graceful shutdown

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §2 «Shutdown», «Hosted service», §4.3 (in-flight dequeue), §5.2 строки 4 и 6, §6.4, §7.1, §7.2, §10 «Интеграционные» п. 6–9; Review Focus 1.

**Files:**
- Test (Create): `tests/PgBolt.Server.Tests/Integration/ShutdownTests.cs`

**Interfaces:**
- Consumes: всё из Task 17; `PgBoltProcessingService` (Task 16, internal), `PgBoltServerOptions` (Task 8).
- Produces: доказательство §1 «Деплой не расходует попытки»: остановленная по токену задача возвращается с прежним `attempt`; брошенная не присылает исход; незапускавшиеся задачи из in-flight dequeue возвращаются без попытки; строки `servers` удаляются.

Сценарии и почему они детерминированы:
- **Отмена + shutdown.** `cancel_requested` ставится прямо в БД перед `StopAsync`, а heartbeat хоста замедлен (`SlowHeartbeat`: раз в 10 с): отмену доставить некому, первым срабатывает shutdown-токен → release с `ran = true` (§7.1, ветка отмены).
- **Shutdown во время dequeue.** Dequeue паркуется на `LOCK TABLE queue IN EXCLUSIVE MODE` (виден в `pg_stat_activity`), прокси ставится на паузу, блокировка снимается: statement коммитится, а ответ задерживается прокси. Затем `StopAsync` и `Resume` — dispatcher получает строки уже после сигнала остановки (§4.3, §6.4 п. 2). Одна из задач заранее помечена `cancel_requested`: её `Cancelled` с `attempt = 0` доказывает, что dequeue действительно взял строки (иначе ветка «ничего не взято» была бы неотличима от release с декрементом). Enqueue идёт через `Client` базы (мимо прокси) до взятия блокировки.

- [x] **Step 1: Написать тесты**

`tests/PgBolt.Server.Tests/Integration/ShutdownTests.cs`:

```csharp
using Npgsql;
using PgBolt.Server.Runtime;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class ShutdownTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Job_stopped_by_shutdown_token_returns_with_previous_attempt_and_is_taken_by_another_server()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var id = await EnqueueAsync(new BlockArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var aId = a.ServerId;
        Assert.True(await ServerRowExistsAsync(aId));

        var elapsed = await a.StopAsync();

        Assert.True(elapsed < ServerTestHost.ShutdownTimeout, $"the job honours the token; stop took {elapsed}");
        var row = await QueueRowAsync(id);
        Assert.NotNull(row);
        Assert.Equal(0, row.Attempt);
        Assert.Null(row.LeaseOwner);
        Assert.Null(row.LeaseId);
        Assert.Null(row.LeaseUntil);
        Assert.True(row.Ready);
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Empty(await EventsAsync(id));
        Assert.False(await ServerRowExistsAsync(aId));
        var first = Assert.Single(Probe.Runs(id));
        Assert.True(first.WasCancelled);
        Assert.Equal(RunEnd.Threw, first.End);

        var b = await StartHostAsync(h => h.Name = "B");
        Probe.Release(id);
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));

        Assert.Equal(1, await AttemptAsync(id));
        Assert.Equal(b.ServerId, await JobServerIdAsync(id));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.Equal("B", runs[1].Host);
        Assert.Equal(1, runs[1].Attempt);
    }

    [Fact]
    public async Task Job_ignoring_token_is_abandoned_its_late_outcome_is_not_sent_and_sweeper_spends_the_attempt()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var id = await EnqueueAsync(new StubbornArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var aId = a.ServerId;
        var leased = await QueueRowAsync(id);
        Assert.NotNull(leased);

        var elapsed = await a.StopAsync();

        Assert.InRange(elapsed, ServerTestHost.ShutdownTimeout - TimeSpan.FromMilliseconds(100),
            ServerTestHost.ShutdownTimeout + TimeSpan.FromSeconds(3));
        var afterStop = await QueueRowAsync(id);
        Assert.NotNull(afterStop);
        Assert.Equal(aId, afterStop.LeaseOwner);
        Assert.Equal(leased.LeaseId, afterStop.LeaseId);
        Assert.Equal(1, afterStop.Attempt);
        Assert.False(await ServerRowExistsAsync(aId));

        // Брошенное выполнение завершается успешно, пока lease A ещё действует: ack отправлять уже некому.
        Probe.Release(id);
        await WaitUntilAsync(() => Probe.Runs(id)[0].End == RunEnd.Completed, TimeSpan.FromSeconds(5),
            "abandoned execution finishes");
        await Task.Delay(500, Ct); // явно: за 500 мс (≫ AckFlushInterval) исход брошенного выполнения не пришёл

        var late = await QueueRowAsync(id);
        Assert.NotNull(late);
        Assert.Equal(afterStop.LeaseId, late.LeaseId);
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Empty(await EventsAsync(id));

        var b = await StartHostAsync(h => h.Name = "B");
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(b.ServerId, await JobServerIdAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.Equal("A", runs[0].Host);
        Assert.True(runs[0].WasCancelled);
        Assert.Equal("B", runs[1].Host);
        Assert.Equal(2, runs[1].Attempt);
    }

    [Fact]
    public async Task Release_of_job_with_cancel_requested_finalizes_it_as_cancelled_with_interrupted_attempt()
    {
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Configure = ServerTestHost.SlowHeartbeat;
        });
        var id = await EnqueueAsync(new BlockArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var aId = a.ServerId;

        // Флаг — напрямую в БД: heartbeat (раз в 10 с) его не доставит, первым срабатывает shutdown-токен.
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", id.Value));
        await a.StopAsync();

        Assert.Null(await QueueRowAsync(id));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(id));
        Assert.Equal(1, await AttemptAsync(id));
        var cancelled = Assert.Single(await EventsAsync(id));
        Assert.Equal(JobEventKind.Cancelled, cancelled.Kind);
        Assert.Equal(1, cancelled.Attempt);
        Assert.Equal(aId, cancelled.ServerId);
        var run = Assert.Single(Probe.Runs(id));
        Assert.True(run.WasCancelled);
        Assert.Equal(RunEnd.Threw, run.End);
    }

    [Fact]
    public async Task Jobs_returned_by_inflight_dequeue_after_stop_signal_are_released_without_spending_attempts()
    {
        var plain1 = await EnqueueAsync(new BlockArgs(1));
        var plain2 = await EnqueueAsync(new BlockArgs(2));
        var cancelled = await EnqueueAsync(new BlockArgs(3));
        await Db.ExecAsync("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id", ("id", cancelled.Value));

        var proxy = StartProxy();
        await using var locker = await Db.Admin.OpenConnectionAsync(Ct);
        await using var lockTx = await locker.BeginTransactionAsync(Ct);
        await using (var lk = new NpgsqlCommand(Db.Sql("LOCK TABLE {schema}.queue IN EXCLUSIVE MODE"), locker, lockTx))
            await lk.ExecuteNonQueryAsync(Ct);

        // H = 10 с: таймаут dequeue и heartbeat не срабатывают в окне сценария.
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
            h.RunMaintenance = false;
            h.Configure = ServerTestHost.SlowHeartbeat;
        });
        var aId = a.ServerId;
        await WaitForParkedDequeueAsync(TimeSpan.FromSeconds(5));

        proxy.Pause();                 // ответ dequeue будет задержан
        await lockTx.CommitAsync(Ct);  // dequeue выполняется и коммитится до сигнала остановки
        await WaitUntilAsync(async () =>
                await CountAsync("SELECT count(*) FROM {schema}.queue WHERE lease_owner = @s", ("s", aId)) == 3,
            TimeSpan.FromSeconds(5), "parked dequeue commits all three jobs");

        var stop = a.StopAsync();
        await Task.Delay(200, Ct);     // явно: сигнал остановки доставлен раньше ответа dequeue
        Assert.False(stop.IsCompleted, "stop must wait for the in-flight dequeue");
        proxy.Resume();
        await stop.WaitAsync(TimeSpan.FromSeconds(20), Ct);

        foreach (var id in new[] { plain1, plain2 })
        {
            var row = await QueueRowAsync(id);
            Assert.NotNull(row);
            Assert.Equal(0, row.Attempt);
            Assert.Null(row.LeaseOwner);
            Assert.True(row.Ready);
            Assert.Equal(DbState.Pending, await JobStateAsync(id));
            Assert.Empty(await EventsAsync(id));
        }
        Assert.Null(await QueueRowAsync(cancelled));
        Assert.Equal(DbState.Cancelled, await JobStateAsync(cancelled));
        Assert.Equal(0, await AttemptAsync(cancelled));
        var ev = Assert.Single(await EventsAsync(cancelled));
        Assert.Equal(JobEventKind.Cancelled, ev.Kind);
        Assert.Equal(0, ev.Attempt);
        Assert.Equal(0, Probe.TotalStarted);
        Assert.False(await ServerRowExistsAsync(aId));
    }

    [Fact]
    public async Task Several_virtual_servers_in_one_process_stop_together_without_spending_attempts()
    {
        var host = await StartHostAsync(h =>
        {
            h.AddServer("orders", s => s.Queue("qa", ServerTestHost.FastQueue));
            h.AddServer("emails", s => s.Queue("qb", ServerTestHost.FastQueue));
        });
        var ours = Assert.Single(host.HostedServices, s => s.GetType().Assembly == typeof(PgBoltServerOptions).Assembly);
        Assert.IsType<PgBoltProcessingService>(ours); // promoter и sweeper — один ProcessingMaintenance на процесс
        var serverIds = host.ServerIds;
        Assert.Equal(2, serverIds.Count);

        var ids = new List<JobId>();
        foreach (var queue in new[] { "qa", "qb" })
            for (var i = 0; i < 3; i++)
                ids.Add(await EnqueueAsync(new BlockArgs(i), new EnqueueOptions { Queue = queue }));
        await WaitUntilAsync(() => ids.All(id => Probe.RunCount(id) == 1), TimeSpan.FromSeconds(10), "all six jobs start");
        Assert.Equal(2L, await CountAsync("SELECT count(DISTINCT lease_owner) FROM {schema}.queue WHERE lease_owner IS NOT NULL"));

        var elapsed = await host.StopAsync();

        Assert.True(elapsed < ServerTestHost.ShutdownTimeout, $"servers stop concurrently and do not wait for timeouts; took {elapsed}");
        foreach (var id in ids)
        {
            var row = await QueueRowAsync(id);
            Assert.NotNull(row);
            Assert.Equal(0, row.Attempt);
            Assert.Null(row.LeaseOwner);
            Assert.Equal(DbState.Pending, await JobStateAsync(id));
            Assert.Empty(await EventsAsync(id));
        }
        foreach (var serverId in serverIds)
            Assert.False(await ServerRowExistsAsync(serverId));
    }
}

[Collection(TimingSensitiveCollection.Name)]
public sealed class ShutdownTests_Direct(PostgresFixture pg) : ShutdownTests(pg, ConnectionMode.Direct);

[Collection(TimingSensitiveCollection.Name)]
public sealed class ShutdownTests_Bouncer(PostgresFixture pg) : ShutdownTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Прогнать**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~ShutdownTests"`
Expected: PASS — 5 × 2 теста. Падение — дефект Tasks 14–16 (см. «Покрытие спеки»): `BLOCKED` с выводом.

- [x] **Step 3: Commit**

```bash
git add tests/PgBolt.Server.Tests/Integration/ShutdownTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(server): graceful shutdown integration scenarios"
```

---

### Task 19: Lease, сбои БД и исчерпание пула

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §1 «Критерии успеха», §2 «Heartbeat», «Сбой БД во время выполнения», §4.3, §5.2 (`HeartbeatLost`, `LeaseLost`), §6.1, §6.2, §6.3, §10 «Интеграционные» п. 11–15; Review Focus 4; core §4.4 (протокол удаления партиции).

**Files:**
- Test (Create): `tests/PgBolt.Server.Tests/Integration/LeaseFailureTests.cs`

**Interfaces:**
- Consumes: всё из Task 17; `IPgBoltMaintenance.EnsurePartitionsAsync` (core).
- Produces: доказательство, что выполнение с неподтверждённым продлением останавливается до истечения своего lease, исход потерянного lease не применяется, сиротская строка и брошенный исход уходят sweeper'у, кратковременный сбой ничего не теряет, а задача, роняющая heartbeat, доходит до `Failed`.

Сценарии и почему они детерминированы:
- **Потеря lease.** Прокси хоста A на паузе: его heartbeat и sweeper молчат; lease истекает, sweeper хоста B отдаёт задачу, B её берёт. Задача `Stubborn`: A замечает отмену (`HeartbeatLost`), но доводит выполнение до успеха — ack A после `Resume` не должен ничего изменить.
- **Тот же сервер берёт задачу снова (Review Focus 4).** Пока прокси A на паузе, lease сбрасывает sweeper отдельного хоста; после `Resume` A берёт ту же задачу с новым `lease_id`, а первое выполнение ещё идёт. Ворота по `(job_id, attempt)` позволяют завершить сначала первую копию и убедиться, что её исход не применился к новому lease.
- **Сбой исхода.** Партиция `job_events` на сегодня удаляется по протоколу core §4.4: retry-исход не пишется дольше бюджета `LeaseDuration`, sweeper тоже не может писать `LeaseExpired`, поэтому задача остаётся в `queue` с истёкшим lease — это и проверяется. Затем партиция возвращается (`EnsurePartitionsAsync`), sweeper засчитывает попытку, задача выполняется.
- **Кратковременный сбой.** `L = 6 с`, `H = 1 с`, пауза 2 с < `L − H`: ничего не отменяется, исходы доставляются, lease работающей задачи продлевается.
- **Длительный сбой.** Пауза 7 с > `L − H`: каждое выполнение отменяется не позже чем за ~`H` до своего `lease_until`; момент отмены переводится в часы БД через пару (`clock_timestamp()`, `Stopwatch`), снятую сразу после `Pause` (admin-подключение идёт мимо прокси). `lease_until` читается после паузы — это верхняя граница lease, известного выполнению, поэтому проверка не ослабляется гонкой с heartbeat.
- **Dequeue, завершившийся после входа в «heartbeat потерян».** Полная пауза прокси не может дать такой dequeue: ответ не дойдёт в пределах его таймаута `H`, а «heartbeat потерян» наступает через `L − H ≥ 2H`. Поэтому сбой делается выборочным: блокировка строки `servers` сервера (её обновляет только heartbeat) роняет heartbeat, а dequeue работает. `L = 9 с`, `H = 3 с`: «heartbeat потерян» наступает в `S + 6 с` (`S` — `heartbeat_at` последнего успеха, прочитанный под этой блокировкой); в `S + 4.5 с` dequeue паркуется на `LOCK TABLE queue IN EXCLUSIVE MODE` (его таймаут истекает не раньше `S + 7.5 с`). Вход в состояние виден по отмене canary-задачи (её рубеж совпадает с ним, §6.2), после чего блокировка снимается. Задачи, которые dequeue берёт, ставятся транзакционным enqueue в той же транзакции, что держит блокировку, одна с `cancel_requested` — её `Cancelled` с `attempt = 0` доказывает release с `ran = false`.
- **Исчерпание пула (Review Focus в §5.2).** `o.DataSource` с `MaxPoolSize = 2`, `Workers = 1`; задача держит оба соединения: heartbeat не получает соединение за `H/2`, выполнение отменяется по `HeartbeatLost`, каждая остановка тратит попытку, после `max_attempts = 2` — `Failed`.

- [x] **Step 1: Написать тесты**

`tests/PgBolt.Server.Tests/Integration/LeaseFailureTests.cs`:

```csharp
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PgBolt.Postgres.Hosting;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class LeaseFailureTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    private const string LeaseRenewalNotConfirmed = "cancelled: lease renewal not confirmed";

    private static void LongLease(PgBoltServerOptions s) => s.LeaseDuration = TimeSpan.FromSeconds(6); // H = 1 с

    [Fact]
    public async Task Sweeper_of_another_server_takes_job_from_server_with_lost_heartbeat_and_late_ack_is_rejected()
    {
        var proxy = StartProxy();
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
        });
        var id = await EnqueueAsync(new StubbornArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var b = await StartHostAsync(h => h.Name = "B");

        proxy.Pause();
        await WaitUntilAsync(() => Probe.Run(id, 1)!.WasCancelled, TimeSpan.FromSeconds(5),
            "A cancels its execution when lease renewal is not confirmed");
        await WaitUntilAsync(() => Probe.Run(id, 2) is not null, TimeSpan.FromSeconds(15),
            "B takes the job after its sweeper resets the expired lease");

        var onA = Probe.Run(id, 1)!;
        var onB = Probe.Run(id, 2)!;
        Assert.Equal("A", onA.Host);
        Assert.Equal("B", onB.Host);
        Assert.True(onA.CancelledAt < onB.StartedAt, "A must be cancelled before the job is handed to B");

        Probe.Release(id); // обе копии завершаются успешно; у A — с устаревшим lease_id
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));
        proxy.Resume();
        await a.StopAsync(); // A отправляет всё накопленное — это не должно ничего изменить

        Assert.Equal(DbState.Succeeded, await JobStateAsync(id));
        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(b.ServerId, await JobServerIdAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        Assert.Equal(RunEnd.Completed, onA.End);
        Assert.Equal(RunEnd.Completed, onB.End);
    }

    [Fact]
    public async Task Same_server_retaking_a_swept_job_tracks_executions_by_lease_id()
    {
        var proxy = StartProxy();
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
        });
        await StartHostAsync(h =>
        {
            h.Name = "Sweeper";
            h.AddServer("idle", s => s.Queue("idle", ServerTestHost.FastQueue));
        });
        var id = await EnqueueAsync(new StubbornArgs(1));
        await WaitUntilAsync(() => Probe.RunCount(id) == 1, TimeSpan.FromSeconds(10), "A starts the job");
        var firstLease = (await QueueRowAsync(id))!.LeaseId;

        proxy.Pause();
        await WaitUntilAsync(async () => (await QueueRowAsync(id))?.LeaseOwner is null, TimeSpan.FromSeconds(15),
            "sweeper of another process resets the expired lease of A");
        proxy.Resume();
        await WaitUntilAsync(() => Probe.Run(id, 2) is not null, TimeSpan.FromSeconds(15),
            "A takes the same job again while the first execution is still running");

        var second = await QueueRowAsync(id);
        Assert.NotNull(second);
        Assert.Equal(a.ServerId, second.LeaseOwner);
        Assert.NotEqual(firstLease, second.LeaseId);
        Assert.Equal("A", Probe.Run(id, 2)!.Host);

        Probe.Release(id, attempt: 1); // первая копия завершается успешно — её исход относится к потерянному lease
        await WaitUntilAsync(() => Probe.Run(id, 1)!.End == RunEnd.Completed, TimeSpan.FromSeconds(5), "first copy finishes");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct); // явно: за 1 с (≫ AckFlushInterval, ≈ H) исход первой копии не применился

        var still = await QueueRowAsync(id);
        Assert.NotNull(still);
        Assert.Equal(second.LeaseId, still.LeaseId);
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Equal(RunEnd.Running, Probe.Run(id, 2)!.End);

        Probe.Release(id, attempt: 2);
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));
        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
    }

    [Fact]
    public async Task Orphan_row_with_unknown_lease_is_not_renewed_and_is_handled_by_sweeper()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var aId = a.ServerId;
        var id = await EnqueueAsync(new SuccessArgs(1), new EnqueueOptions { Delay = TimeSpan.FromHours(1) });

        // Строка с lease_owner = A и lease_id, которого runtime A не знает (как после dequeue без полученного ответа).
        var orphanLeaseUntil = await Db.ScalarAsync<DateTime>("""
            UPDATE {schema}.queue
            SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '3 seconds',
                leased_at = now(), attempt = attempt + 1, ready = true
            WHERE job_id = @id
            RETURNING lease_until
            """, ("s", aId), ("id", id.Value));
        var orphanedAt = await DbNowAsync();

        await WaitUntilAsync(async () => await HeartbeatAtAsync(aId) > orphanedAt, TimeSpan.FromSeconds(5),
            "A sends a heartbeat after the orphan row appears");
        var row = await QueueRowAsync(id);
        Assert.NotNull(row);
        Assert.Equal(aId, row.LeaseOwner);
        Assert.Equal(orphanLeaseUntil, row.LeaseUntil); // heartbeat не продлил неизвестный lease

        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));
        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        Assert.Equal(2, Assert.Single(Probe.Runs(id)).Attempt);
    }

    [Fact]
    public async Task Outcome_that_cannot_be_written_within_its_budget_expires_and_sweeper_spends_the_attempt()
    {
        var a = await StartHostAsync(h => h.Name = "A");
        var aId = a.ServerId;
        await DropTodayEventsPartitionAsync();
        var id = await EnqueueAsync(new FlakyArgs(1), ImmediateRetry);

        await WaitUntilAsync(() => Probe.Run(id, 1)?.End == RunEnd.Threw, TimeSpan.FromSeconds(10), "first attempt fails");
        await WaitUntilAsync(async () => await Db.ScalarAsync<bool>(
                "SELECT lease_owner = @s AND lease_until < now() FROM {schema}.queue WHERE job_id = @id",
                ("s", aId), ("id", id.Value)),
            TimeSpan.FromSeconds(15),
            "retry outcome is abandoned after its LeaseDuration budget and the lease is no longer renewed");

        // Sweeper тоже не может записать LeaseExpired — задача ждёт в queue, а не пропадает.
        Assert.Empty(await EventsAsync(id));
        Assert.Equal(DbState.Pending, await JobStateAsync(id));
        Assert.Equal(1, Probe.RunCount(id));

        await a.Services.GetRequiredService<IPgBoltMaintenance>().EnsurePartitionsAsync(2, Ct);
        await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(2, await AttemptAsync(id));
        Assert.Equal(new[] { JobEventKind.LeaseExpired }, await EventKindsAsync(id));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.Equal(RunEnd.Threw, runs[0].End);
        Assert.Equal(RunEnd.Completed, runs[1].End);
    }

    [Fact]
    public async Task Short_outage_below_L_minus_H_cancels_nothing_and_delivers_outcomes_after_recovery()
    {
        var proxy = StartProxy();
        await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
            h.Configure = LongLease;
        });
        var ids = new List<JobId>();
        for (var i = 0; i < 4; i++)
            ids.Add(await EnqueueAsync(new BlockArgs(i)));
        await WaitUntilAsync(() => ids.All(id => Probe.RunCount(id) == 1), TimeSpan.FromSeconds(10), "all four jobs start");
        var running = ids[3];
        var leaseBefore = (await QueueRowAsync(running))!.LeaseUntil!.Value;

        proxy.Pause();
        for (var i = 0; i < 3; i++)
            Probe.Release(ids[i]); // завершаются во время сбоя: ack не может уйти
        await WaitUntilAsync(() => ids.Take(3).All(id => Probe.Run(id, 1)!.End == RunEnd.Completed), TimeSpan.FromSeconds(5),
            "three jobs finish during the outage");
        await Task.Delay(TimeSpan.FromSeconds(2), Ct); // длительность сбоя: 2 с < L − H = 5 с
        proxy.Resume();

        foreach (var id in ids.Take(3))
            await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(10));
        await WaitUntilAsync(async () => (await QueueRowAsync(running))!.LeaseUntil > leaseBefore + TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(5), "lease of the still running execution is renewed after recovery");
        Probe.Release(running);
        await WaitForStateAsync(running, DbState.Succeeded, TimeSpan.FromSeconds(10));

        foreach (var id in ids)
        {
            var run = Assert.Single(Probe.Runs(id));
            Assert.False(run.WasCancelled);
            Assert.Equal(1, await AttemptAsync(id));
            Assert.Empty(await EventsAsync(id));
        }
    }

    [Fact]
    public async Task Outage_longer_than_L_minus_H_cancels_executions_before_their_lease_expires_and_spends_the_attempt()
    {
        var proxy = StartProxy();
        await StartHostAsync(h =>
        {
            h.Name = "A";
            h.Proxy = proxy;
            h.Configure = LongLease;
        });
        var ids = new List<JobId>();
        for (var i = 0; i < 3; i++)
            ids.Add(await EnqueueAsync(new BlockArgs(i)));
        await WaitUntilAsync(() => ids.All(id => Probe.RunCount(id) == 1), TimeSpan.FromSeconds(10), "all three jobs start");

        proxy.Pause();
        var pausedLocal = Stopwatch.GetTimestamp();
        var pausedDb = await DbNowAsync(); // admin-подключение — мимо прокси
        var leaseUntil = new Dictionary<JobId, DateTime>();
        foreach (var id in ids)
            leaseUntil[id] = (await QueueRowAsync(id))!.LeaseUntil!.Value;

        await WaitUntilAsync(() => ids.All(id => Probe.Run(id, 1)!.WasCancelled), TimeSpan.FromSeconds(7),
            "executions are cancelled while the database is unreachable");
        foreach (var id in ids)
        {
            var cancelledDb = pausedDb + Stopwatch.GetElapsedTime(pausedLocal, Probe.Run(id, 1)!.CancelledAt);
            Assert.True(cancelledDb <= leaseUntil[id] - TimeSpan.FromMilliseconds(500),
                $"execution of {id} cancelled at {cancelledDb:O}, its lease is valid until {leaseUntil[id]:O}");
        }

        var rest = TimeSpan.FromSeconds(7) - Stopwatch.GetElapsedTime(pausedLocal);
        if (rest > TimeSpan.Zero)
            await Task.Delay(rest, Ct); // длительность сбоя: 7 с > L − H = 5 с
        proxy.Resume();
        foreach (var id in ids)
            Probe.Release(id);

        foreach (var id in ids)
        {
            await WaitForStateAsync(id, DbState.Succeeded, TimeSpan.FromSeconds(20));
            Assert.Equal(2, await AttemptAsync(id));
            // Попытку засчитывает либо retry-исход после восстановления, либо sweeper, если lease истёк раньше.
            var ev = Assert.Single(await EventsAsync(id));
            Assert.Contains(ev.Kind, new[] { JobEventKind.Retry, JobEventKind.LeaseExpired });
            if (ev.Kind == JobEventKind.Retry)
                Assert.Contains(LeaseRenewalNotConfirmed, ev.Message);
            Assert.Equal(2, Probe.RunCount(id));
        }
    }

    [Fact]
    public async Task Dequeue_completing_after_heartbeat_loss_releases_its_jobs_without_running_them()
    {
        var a = await StartHostAsync(h =>
        {
            h.Name = "A";
            h.RunMaintenance = false;
            h.Configure = s =>
            {
                s.Workers = 4;
                s.LeaseDuration = TimeSpan.FromSeconds(9);
                s.HeartbeatInterval = TimeSpan.FromSeconds(3);
            };
        });
        var aId = a.ServerId;
        var canary = await EnqueueAsync(new BlockArgs(0), ImmediateRetry);
        await WaitUntilAsync(() => Probe.RunCount(canary) == 1, TimeSpan.FromSeconds(10), "canary starts");
        var canaryStarted = await DbNowAsync();
        await WaitUntilAsync(async () => await HeartbeatAtAsync(aId) > canaryStarted + TimeSpan.FromMilliseconds(200),
            TimeSpan.FromSeconds(10), "a successful heartbeat covers the canary lease");

        // 1. Heartbeat блокируется на строке servers (её обновляет только он); dequeue продолжает работать.
        await using var hbLock = await Db.Admin.OpenConnectionAsync(Ct);
        await using var hbTx = await hbLock.BeginTransactionAsync(Ct);
        DateTime lastHeartbeat;
        await using (var cmd = new NpgsqlCommand(Db.Sql("SELECT heartbeat_at FROM {schema}.servers WHERE id = @id FOR UPDATE"), hbLock, hbTx))
        {
            cmd.Parameters.AddWithValue("id", aId);
            lastHeartbeat = (DateTime)(await cmd.ExecuteScalarAsync(Ct))!;
        }

        // 2. «Heartbeat потерян» наступит в lastHeartbeat + L − H = +6 с; за 1.5 с до этого паркуем dequeue (таймаут H = 3 с).
        await DelayUntilDbTimeAsync(lastHeartbeat + TimeSpan.FromSeconds(4.5));
        await using var qLock = await Db.Admin.OpenConnectionAsync(Ct);
        await using var qTx = await qLock.BeginTransactionAsync(Ct);
        await using (var cmd = new NpgsqlCommand(Db.Sql("LOCK TABLE {schema}.queue IN EXCLUSIVE MODE"), qLock, qTx))
            await cmd.ExecuteNonQueryAsync(Ct);
        var plain = await EnqueueAsync(new SuccessArgs(1), new EnqueueOptions { Transaction = qTx });
        var cancelled = await EnqueueAsync(new SuccessArgs(2), new EnqueueOptions { Transaction = qTx });
        await using (var cmd = new NpgsqlCommand(Db.Sql("UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id"), qLock, qTx))
        {
            cmd.Parameters.AddWithValue("id", cancelled.Value);
            await cmd.ExecuteNonQueryAsync(Ct);
        }
        await WaitForParkedDequeueAsync(TimeSpan.FromSeconds(1));

        // 3. Вход в «heartbeat потерян» виден по отмене canary: её рубеж validUntil − H совпадает с ним (§6.2).
        await WaitUntilAsync(() => Probe.Run(canary, 1)!.WasCancelled, TimeSpan.FromSeconds(4),
            "canary is cancelled at lastHeartbeat + L − H");
        Assert.True(await ParkedDequeueCountAsync() > 0, "dequeue must still be in flight when heartbeat is lost");
        await qTx.CommitAsync(Ct); // dequeue завершается уже в состоянии «heartbeat потерян»

        await WaitForStateAsync(cancelled, DbState.Cancelled, TimeSpan.FromSeconds(5));
        await WaitUntilAsync(async () => (await QueueRowAsync(plain))?.LeaseOwner is null, TimeSpan.FromSeconds(5),
            "plain job is released");
        Assert.Equal(0, (await QueueRowAsync(plain))!.Attempt);
        Assert.Equal(0, await AttemptAsync(cancelled));
        Assert.Equal(new[] { JobEventKind.Cancelled }, await EventKindsAsync(cancelled));
        Assert.Equal(0, Probe.RunCount(plain));
        Assert.Equal(0, Probe.RunCount(cancelled));

        // 4. Heartbeat восстанавливается: plain выполняется, canary повторяется с тратой попытки.
        await hbTx.CommitAsync(Ct);
        Probe.Release(canary);
        await WaitForStateAsync(plain, DbState.Succeeded, TimeSpan.FromSeconds(15));
        await WaitForStateAsync(canary, DbState.Succeeded, TimeSpan.FromSeconds(15));

        Assert.Equal(1, await AttemptAsync(plain));
        Assert.Equal(2, await AttemptAsync(canary));
        var retry = Assert.Single(await EventsAsync(canary));
        Assert.Equal(JobEventKind.Retry, retry.Kind);
        Assert.Contains(LeaseRenewalNotConfirmed, retry.Message);
    }

    [Fact]
    public async Task Job_exhausting_the_pool_is_cancelled_by_HeartbeatLost_and_fails_after_max_attempts()
    {
        var csb = new NpgsqlConnectionStringBuilder(Db.ConnectionString) { MaxPoolSize = 2 };
        var dataSource = Track(NpgsqlDataSource.Create(csb.ConnectionString));
        await StartHostAsync(h =>
        {
            h.Name = "A";
            h.DataSource = dataSource;
            h.RunMaintenance = false;
            h.Configure = s => s.Workers = 1;
        });

        var id = await EnqueueAsync(new PoolHogArgs(2)); // max_attempts = 2, retry_delays = [0]

        await WaitForStateAsync(id, DbState.Failed, TimeSpan.FromSeconds(20));
        Assert.Equal(2, await AttemptAsync(id));
        var events = await EventsAsync(id);
        Assert.Equal(new[] { JobEventKind.Retry, JobEventKind.Failed }, events.Select(e => e.Kind));
        Assert.All(events, e => Assert.Contains(LeaseRenewalNotConfirmed, e.Message));
        var runs = Probe.Runs(id);
        Assert.Equal(2, runs.Count);
        Assert.All(runs, r =>
        {
            Assert.True(r.WasCancelled);
            Assert.Equal(RunEnd.Threw, r.End);
        });
    }

    /// <summary>Протокол удаления партиции core §4.4: короткий lock_timeout на родителе, DETACH + DROP, повтор.</summary>
    private async Task DropTodayEventsPartitionAsync()
    {
        var suffix = await Db.ScalarAsync<string>("SELECT to_char((now() AT TIME ZONE 'UTC')::date, 'YYYYMMDD')");
        for (var attempt = 1; ; attempt++)
        {
            await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
            await using var tx = await conn.BeginTransactionAsync(Ct);
            try
            {
                foreach (var sql in new[]
                         {
                             "SET LOCAL lock_timeout = '100ms'",
                             "LOCK TABLE {schema}.job_events IN ACCESS EXCLUSIVE MODE",
                             $"ALTER TABLE {{schema}}.job_events DETACH PARTITION {{schema}}.job_events_{suffix}",
                             $"DROP TABLE {{schema}}.job_events_{suffix}",
                         })
                {
                    await using var cmd = new NpgsqlCommand(Db.Sql(sql), conn, tx);
                    await cmd.ExecuteNonQueryAsync(Ct);
                }
                await tx.CommitAsync(Ct);
                return;
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable && attempt < 50)
            {
                await tx.RollbackAsync(Ct);
            }
            await Task.Delay(50, Ct);
        }
    }
}

[Collection(TimingSensitiveCollection.Name)]
public sealed class LeaseFailureTests_Direct(PostgresFixture pg) : LeaseFailureTests(pg, ConnectionMode.Direct);

[Collection(TimingSensitiveCollection.Name)]
public sealed class LeaseFailureTests_Bouncer(PostgresFixture pg) : LeaseFailureTests(pg, ConnectionMode.Bouncer);
```

Замечание для исполнителя: `Outcome_that_cannot_be_written_within_its_budget…` удаляет партицию текущих суток UTC; прогон, пересекающий полночь UTC, может упасть на этом тесте — это ограничение сценария, а не дефект; повторить прогон.

- [x] **Step 2: Прогнать**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~LeaseFailureTests"`
Expected: PASS — 8 × 2 теста. Падение — дефект Tasks 12, 13, 15 (см. «Покрытие спеки»): `BLOCKED` с выводом и хронологией (время `Pause`/`Resume`, метки `JobRun`).

- [x] **Step 3: Commit**

```bash
git add tests/PgBolt.Server.Tests/Integration/LeaseFailureTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(server): lease loss, database outage and pool exhaustion scenarios"
```

---

### Task 20: Инвариант «ровно один раз» и нагрузочный smoke

**Model:** исполнитель — Opus 5.5 (`claude-opus-5-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §1 «Критерии успеха» (throughput, пул), §2 «Архитектура исполнения», §10 «Интеграционные» п. 16 (инвариант), §10 «Нагрузочный smoke».

**Files:**
- Test (Create): `tests/PgBolt.Server.Tests/Integration/InvariantTests.cs`, `tests/PgBolt.Server.Tests/Integration/LoadSmokeTests.cs`

**Interfaces:**
- Consumes: всё из Task 17.
- Produces: инвариант N процессов × M виртуальных серверов × K задач без сбоев (каждая выполнена ровно один раз); smoke 100k задач — throughput по окнам не деградирует, соединений не больше пула. Нагрузочный тест помечен `[Trait("Category", "Load")]`.

Параметры:
- Инвариант: 3 хоста × 2 виртуальных сервера (очереди `q1`, `q2` в разном порядке), 3000 задач; `SlowHeartbeat` (`L = 30 с`, `H = 10 с`), чтобы паузы GC/CPU на загруженной машине не давали ложного `HeartbeatLost` в сценарии «без сбоев».
- Smoke: только Direct — в режиме PgBouncer `pg_stat_activity` показывает серверные соединения пула bouncer'а, а не соединения процесса. Все 100 000 задач ставятся до старта хоста (измеряется обработка, а не enqueue); `DataSource` с `ApplicationName = pgbolt-load-{schema}` и `MaxPoolSize = 20`; значения runtime по умолчанию, кроме `Workers = 32` и `ShutdownTimeout = 5 с`. Throughput меряется по `JobProbe.TotalCompleted` с шагом 100 мс; окно — 10 с, если обработка длится ≥ 40 с, иначе четверть длительности (не меньше 1 с), чтобы окон было не меньше четырёх; неполное последнее окно отбрасывается; каждое окно ≥ 50 % первого. Соединения — максимум `count(*) FROM pg_stat_activity WHERE application_name = …` за прогон, ≤ `MaxPoolSize`.

- [x] **Step 1: Написать тест инварианта**

`tests/PgBolt.Server.Tests/Integration/InvariantTests.cs`:

```csharp
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

public abstract class InvariantTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Three_processes_with_two_servers_each_execute_every_job_exactly_once()
    {
        const int perQueue = 1500;
        for (var p = 0; p < 3; p++)
        {
            var name = $"P{p}";
            await StartHostAsync(h =>
            {
                h.Name = name;
                h.Configure = ServerTestHost.SlowHeartbeat;
                h.AddServer("a", s => s.Queue("q1", ServerTestHost.FastQueue).Queue("q2", ServerTestHost.FastQueue));
                h.AddServer("b", s => s.Queue("q2", ServerTestHost.FastQueue).Queue("q1", ServerTestHost.FastQueue));
            });
        }

        foreach (var queue in new[] { "q1", "q2" })
            for (var i = 0; i < perQueue; i += 500)
                await Client.EnqueueManyAsync(Enumerable.Range(i, 500).Select(n => new SuccessArgs(n)).ToList(),
                    new EnqueueOptions { Queue = queue }, null, Ct);

        await WaitUntilAsync(async () => await CountAsync("SELECT count(*) FROM {schema}.jobs WHERE state = 2") == 2 * perQueue,
            TimeSpan.FromSeconds(120), "all jobs succeed");

        var counts = Probe.RunCounts();
        Assert.Equal(2 * perQueue, counts.Count);
        Assert.All(counts, c => Assert.Equal(1, c.Value));
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.queue"));
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.job_events"));
        Assert.Equal((long)(2 * perQueue), await CountAsync("SELECT count(*) FROM {schema}.jobs"));
        Assert.True(await CountAsync("SELECT count(DISTINCT server_id) FROM {schema}.jobs") >= 2,
            "jobs must be spread over several servers for the invariant to be exercised");
    }
}

public sealed class InvariantTests_Direct(PostgresFixture pg) : InvariantTests(pg, ConnectionMode.Direct);
public sealed class InvariantTests_Bouncer(PostgresFixture pg) : InvariantTests(pg, ConnectionMode.Bouncer);
```

- [x] **Step 2: Прогнать инвариант**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "FullyQualifiedName~InvariantTests"`
Expected: PASS — 2 теста (Direct, Bouncer).

- [x] **Step 3: Написать нагрузочный smoke**

`tests/PgBolt.Server.Tests/Integration/LoadSmokeTests.cs`:

```csharp
using System.Diagnostics;
using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>Нагрузочный smoke (SLO — часть 7). Запуск: dotnet test tests/PgBolt.Server.Tests --filter "Category=Load".</summary>
[Trait("Category", "Load")]
[Collection(TimingSensitiveCollection.Name)]
public sealed class LoadSmokeTests(PostgresFixture pg) : ServerIntegrationTestBase(pg, ConnectionMode.Direct)
{
    private const int Total = 100_000;
    private const int MaxPoolSize = 20;

    [Fact]
    public async Task Hundred_thousand_short_jobs_keep_throughput_and_stay_within_the_pool()
    {
        var appName = "pgbolt-load-" + Db.Schema;
        var dataSource = Track(NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(Db.ConnectionString)
        {
            ApplicationName = appName,
            MaxPoolSize = MaxPoolSize,
        }.ConnectionString));

        for (var i = 0; i < Total; i += 1000)
            await Client.EnqueueManyAsync(Enumerable.Range(i, 1000).Select(n => new SuccessArgs(n)).ToList(), null, null, Ct);

        var clock = Stopwatch.StartNew();
        await StartHostAsync(h =>
        {
            h.Name = "load";
            h.DataSource = dataSource;
            h.Configure = s =>
            {
                s.Workers = 32;
                s.LeaseDuration = TimeSpan.FromSeconds(60);
                s.HeartbeatInterval = TimeSpan.FromSeconds(15);
                s.ShutdownTimeout = TimeSpan.FromSeconds(5);
                s.AckFlushInterval = TimeSpan.FromMilliseconds(20);
            };
        });

        using var sampling = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var samples = new List<(TimeSpan At, int Completed)>();
        long maxConnections = 0;
        var sampler = Task.Run(async () =>
        {
            while (!sampling.IsCancellationRequested)
            {
                samples.Add((clock.Elapsed, Probe.TotalCompleted));
                maxConnections = Math.Max(maxConnections, await Db.ScalarAsync<long>(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = @a", ("a", appName)));
                try
                {
                    await Task.Delay(100, sampling.Token);
                }
                catch (OperationCanceledException)
                {
                    break; // прогон завершён
                }
            }
        }, Ct);

        await WaitUntilAsync(() => Probe.TotalCompleted >= Total, TimeSpan.FromMinutes(10), $"all {Total} jobs complete");
        var finishedAt = clock.Elapsed;
        await sampling.CancelAsync();
        await sampler;
        samples.Add((finishedAt, Probe.TotalCompleted));

        await WaitUntilAsync(async () => await CountAsync("SELECT count(*) FROM {schema}.jobs WHERE state = 2") == Total,
            TimeSpan.FromSeconds(60), "all acks are flushed");
        Assert.Equal(0L, await CountAsync("SELECT count(*) FROM {schema}.queue"));
        var counts = Probe.RunCounts();
        Assert.Equal(Total, counts.Count);
        Assert.All(counts, c => Assert.Equal(1, c.Value));

        var (window, rates) = ThroughputWindows(samples);
        var output = TestContext.Current.TestOutputHelper;
        output?.WriteLine($"processed {Total} jobs in {finishedAt.TotalSeconds:0.0} s; window {window.TotalSeconds:0.##} s; " +
                          $"jobs/s: {string.Join(", ", rates.Select(r => r.ToString("0")))}; max connections {maxConnections}");

        Assert.True(rates.Count >= 2, $"need at least two full windows, got {rates.Count}");
        Assert.All(rates.Skip(1), r => Assert.True(r >= rates[0] * 0.5,
            $"throughput degraded: {r:0} jobs/s vs first window {rates[0]:0} jobs/s"));
        Assert.InRange(maxConnections, 1, MaxPoolSize);
    }

    /// <summary>Окна от первой завершённой задачи до последней; неполное последнее окно отбрасывается.</summary>
    private static (TimeSpan Window, List<double> Rates) ThroughputWindows(List<(TimeSpan At, int Completed)> samples)
    {
        var start = samples.First(s => s.Completed > 0).At;
        var end = samples.First(s => s.Completed >= Total).At;
        var span = end - start;
        var window = span >= TimeSpan.FromSeconds(40)
            ? TimeSpan.FromSeconds(10)
            : TimeSpan.FromSeconds(Math.Max(1, span.TotalSeconds / 4));
        var rates = new List<double>();
        for (var from = start; from + window <= end; from += window)
            rates.Add((CompletedAt(samples, from + window) - CompletedAt(samples, from)) / window.TotalSeconds);
        return (window, rates);
    }

    private static int CompletedAt(List<(TimeSpan At, int Completed)> samples, TimeSpan at) =>
        samples.Last(s => s.At <= at).Completed;
}
```

- [x] **Step 4: Прогнать smoke отдельно**

Run: `dotnet test tests/PgBolt.Server.Tests --filter "Category=Load" --logger "console;verbosity=detailed"`
Expected: PASS — 1 тест; в выводе строка `processed 100000 jobs in … s; window … s; jobs/s: …; max connections N` с `N ≤ 20`. Цифры throughput — в отчёт исполнителя (ориентир для части 7, не SLO).

- [x] **Step 5: Прогнать всю фазу без нагрузочного теста**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: PASS — все тесты решения, включая Tasks 17–20 в обоих режимах. С этого момента «прогон в конце фазы» (Протокол исполнения, п. 6) выполняется с `--filter "Category!=Load"`; smoke запускается отдельно командой из Step 4.

- [x] **Step 6: Commit**

```bash
git add tests/PgBolt.Server.Tests/Integration/InvariantTests.cs tests/PgBolt.Server.Tests/Integration/LoadSmokeTests.cs
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "test(server): exactly-once invariant across processes and load smoke"
```

---

## Фаза 3. Документация

### Task 21: Приведение core-спеки и карты проекта в соответствие с частью 3

**Model:** исполнитель — Sonnet 5 (`claude-sonnet-5`); ревьюер — Opus 5.5 (`claude-opus-5-5`).

**Spec:** §8 (вводный абзац: «тексты core §4.5, §5.5, §5.10, §6.3 при реализации приводятся в соответствие»), §8.1, §8.2, §8.5.

**Files:**
- Modify: `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md` (§4.5, §5.5, §5.10, §6.3)
- Modify: `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md` (только строка статуса)
- Modify: `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md` (часть 3)
- Create: `CHANGELOG.md` (корень репозитория; §8.2: breaking change `IWakeupSignal` «отмечается в changelog»)

**Interfaces:**
- Consumes: реализованные Tasks 4 (heartbeat по списку lease), 6 (`IWakeupSignal.Subscribe`), 16 (публикация `job_types` раз на процесс).
- Produces: core-спека описывает фактический API и SQL; часть 3 помечена реализованной; breaking changes публичного API `PgBolt.Postgres` записаны в `CHANGELOG.md`.

Каждая правка — точная замена фрагмента «было» на «стало» (Edit по уникальной строке). Остальной текст файлов не меняется.

Спека части 3 по существу **не правится**: уточнения, появившиеся при ревью плана (верхние границы длительностей §3.1–§3.3, урезание `[Job(TimeoutSeconds)]` с BOLT008 и предупреждением §3.5/§5.1, план `SlotAllocator` и контракт стратегии §4.2, неожиданные ошибки итерации и откат незапущенного выполнения §4.3, порядок старта и ошибка шага 6 §4.4, обрезка в байтах UTF-8 §5.2, таймаут попытки и отбрасывания при остановке §6.1, остановка при сработавшем токене §6.4, способы проверки в §10), внесены в неё заранее, вместе с планом (ред. 4). Здесь меняется только строка статуса (Step 5), а Step 8 проверяет, что уточнения на месте.

- [x] **Step 1: core §4.5 — `published_by`**

Файл `docs/superpowers/specs/2026-09-26-pgbolt-core-design.md`.

Было:

```
    published_by         text        NOT NULL,   -- имя сервера
```

Стало:

```
    published_by         text        NOT NULL,   -- идентификатор процесса-публикатора ({MachineName}/{ProcessId})
```

Было (абзац сразу после блока SQL §4.5):

```
Сервер при старте делает upsert своих задач (все `default_*` — из `[Job]`). Если схема аргументов отличается от опубликованной другим живым сервером — предупреждение в лог (last writer wins).
```

Стало:

```
Процесс при старте (один раз на процесс, не на виртуальный сервер — часть 3 §4.4) делает upsert своих задач (все `default_*` — из `[Job]`). Если схема аргументов отличается от опубликованной другим живым процессом (`published_by` другой) — предупреждение в лог (last writer wins).
```

- [x] **Step 2: core §5.5 — heartbeat по списку lease**

Было (весь раздел от заголовка до `### 5.6`, не включая его):

````markdown
### 5.5 Heartbeat (один statement на сервер)

```sql
WITH s AS (
    UPDATE {schema}.servers SET heartbeat_at = now() WHERE id = @server
),
l AS MATERIALIZED (
    SELECT job_id FROM {schema}.queue
    WHERE lease_owner = @server
    ORDER BY job_id                                -- тот же порядок захвата, что у ack (§5.3)
    FOR UPDATE
)
UPDATE {schema}.queue q SET lease_until = now() + @lease
FROM l WHERE q.job_id = l.job_id
RETURNING q.job_id, q.lease_id, q.cancel_requested;
```

- Воркер сопоставляет строки результата со своими выполнениями по `lease_id`.
- `cancel_requested = true` → воркер отменяет `CancellationToken` соответствующего выполнения.
- Выполнение, `lease_id` которого нет в результате, считается потерянным (lease забран sweeper'ом): его результат не подтверждается. Это правило применяется только к выполнениям, dequeue которых завершился до отправки heartbeat. «Завершился» означает, что reader прочитан до конца и получен ReadyForQuery (неявная транзакция statement'а коммитится на Sync, уже после отправки DataRow), а не получение первой строки: в READ COMMITTED heartbeat не видит строки, взятые dequeue'ем, закоммиченным после снимка heartbeat. Детали — часть 3.
````

Стало:

````markdown
### 5.5 Heartbeat (один statement на сервер, по списку известных lease)

```sql
WITH s AS (
    UPDATE {schema}.servers SET heartbeat_at = now() WHERE id = @server
),
l AS MATERIALIZED (
    SELECT job_id FROM {schema}.queue
    WHERE job_id = ANY(@ids) AND lease_id = ANY(@lease_ids) AND lease_owner = @server
    ORDER BY job_id                                -- тот же порядок захвата, что у ack (§5.3)
    FOR UPDATE
)
UPDATE {schema}.queue q SET lease_until = now() + @lease
FROM l WHERE q.job_id = l.job_id
RETURNING q.job_id, q.lease_id, q.cancel_requested;
```

- Сигнатура: `HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)`; `@ids`/`@lease_ids` — поэлементные массивы из `leases`.
- Продлеваются только lease из списка — выполнения, известные runtime (выполняются или ждут отправки исхода). Строки сервера вне списка («сиротские»: исход брошен по бюджету, dequeue закоммичен без полученного ответа, выполнение брошено при остановке) не продлеваются, истекают и уходят sweeper'у (§5.6). Без фильтра брошенная строка продлевалась бы, пока жив процесс.
- Воркер сопоставляет строки результата со своими выполнениями по `lease_id`; `cancel_requested = true` → отмена `CancellationToken` соответствующего выполнения.
- Правило потери: lease из списка, отсутствующий в ответе, потерян (забран sweeper'ом или освобождён) — результат такого выполнения не подтверждается. Отдельная проверка «dequeue завершился до отправки heartbeat» не нужна: в список попадают только выполнения, зарегистрированные после полного чтения результата dequeue (reader прочитан до конца, получен ReadyForQuery).
- Условие по `job_id` даёт доступ по PK, как у ack; после ожидания блокировки READ COMMITTED перепроверяет `lease_id`, поэтому строка, освобождённая и взятая заново, не продлевается.
- Пустой список: обновляется только `servers.heartbeat_at`.
- Расписание вызовов, таймаут вызова и проверка срока lease каждого выполнения — часть 3 (`2026-09-28-pgbolt-worker-design.md`, §6.2, §8.1).
````

- [x] **Step 3: core §5.10 — `IWakeupSignal.Subscribe`**

Было:

```markdown
### 5.10 Wakeup

`NOTIFY` отправлять через PgBouncer можно, но при коммите он берёт глобальный lock и на 5–20k/s сериализует коммиты. Поэтому core определяет только интерфейс `IWakeupSignal` и in-process реализацию; polling, `LISTEN` через прямую строку подключения и троттлинг `NOTIFY` — часть 3.
```

Стало:

````markdown
### 5.10 Wakeup

In-process сигнал о появлении работы в очереди:

```csharp
public interface IWakeupSignal
{
    void Signal(string queue);

    /// <summary>callback вызывается синхронно из Signal; должен быть дешёвым и не блокировать.</summary>
    IDisposable Subscribe(string queue, Action callback);
}
```

- `InProcessWakeupSignal` (регистрация по умолчанию через `TryAddSingleton`): подписчики по очереди (copy-on-write массив), `Signal` вызывает их синхронно; исключение callback'а перехватывается и не доходит до вызывающего enqueue. Сигнал до подписки не хранится — latch обеспечивает подписчик (событие диспетчера, часть 3 §4.3).
- Сигнал не гарантирован и не выходит за пределы процесса: воркеры страхуются адаптивным polling'ом и сигналом promoter'а (часть 3 §4.3, §6.3). Межпроцессная реализация (например, Redis pub/sub) — пользовательская, заменой регистрации `IWakeupSignal`.
- `LISTEN`/`NOTIFY` исключены из проекта: `LISTEN` держит постоянное прямое соединение в обход PgBouncer на каждый процесс, `NOTIFY` при коммите берёт глобальный lock и на 5–20k/s сериализует коммиты (часть 3 §2).
- Прежний `WaitAsync(queues, timeout, ct)` удалён (breaking change до 1.0): он не сообщал, какая очередь сработала, и терял сигнал, пришедший без ожидающего.
````

- [x] **Step 4: core §6.3 — ссылка на часть 3**

Было:

```markdown
Серверы (`AddPgBoltServer`) — часть 3, включая SQL регистрации сервера в `servers` и освобождение lease при graceful shutdown (по `lease_id`, с финализацией `cancel_requested`-строк как Cancelled, см. инвариант §4.1).
```

Стало:

```markdown
Серверы (`AddPgBoltServer`, `AddPgBoltJobFilter`, `ConfigurePgBoltProcessing`) — часть 3: `2026-09-28-pgbolt-worker-design.md` (§3.1 регистрация, §7.2 SQL регистрации сервера в `servers`, §7.1 освобождение lease при graceful shutdown по `lease_id` с финализацией `cancel_requested`-строк как Cancelled — инвариант §4.1).
```

- [x] **Step 5: Статус спеки части 3**

Файл `docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md`.

Было:

```markdown
Статус: черновик на ревью (ред. 4: уточнения по ревью плана реализации) · Дата: 2026-09-28 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»)
```

Стало:

```markdown
Статус: одобрено ревью, реализовано (ред. 4; план `docs/superpowers/plans/2026-09-28-pgbolt-worker.md`) · Дата: 2026-09-28 · Карта проекта: `2026-09-26-pgbolt-roadmap.md` · Основа: `2026-09-26-pgbolt-core-design.md` (далее «core»)
```

- [x] **Step 6: Карта проекта — часть 3 реализована**

Файл `docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md`.

Было:

```markdown
### 3. Worker runtime (Server)
Зависит от 1. Спека: `2026-09-28-pgbolt-worker-design.md`.
```

Стало:

```markdown
### 3. Worker runtime (Server)
Зависит от 1. Спека: `2026-09-28-pgbolt-worker-design.md`, план: `docs/superpowers/plans/2026-09-28-pgbolt-worker.md`. **Статус: реализовано.**
```

- [x] **Step 7: Changelog — breaking changes части 3**

Файла `CHANGELOG.md` в репозитории ещё нет — создать в корне (весь файл):

```markdown
# Changelog

## Unreleased

### Breaking changes

- `PgBolt.Postgres.Client.IWakeupSignal`: метод `Task<bool> WaitAsync(IReadOnlyCollection<string> queues, TimeSpan timeout, CancellationToken ct)`
  удалён, вместо него — `IDisposable Subscribe(string queue, Action callback)`. Пользовательские реализации (например,
  Redis pub/sub) должны реализовать `Subscribe`: callback вызывается синхронно из `Signal`, должен быть дешёвым и не
  блокировать; `Dispose` результата отписывает.
- `PgBolt.Postgres.Storage.PgBoltStore.HeartbeatAsync(Guid serverId, TimeSpan lease, CancellationToken ct)` заменён на
  `HeartbeatAsync(Guid serverId, IReadOnlyCollection<LeaseRef> leases, TimeSpan lease, CancellationToken ct)`:
  продлеваются только lease из списка; строки сервера вне списка больше не продлеваются и уходят sweeper'у.

### Added

- Пакет `PgBolt.Server`: worker runtime — `AddPgBoltServer`, `AddPgBoltJobFilter`, `ConfigurePgBoltProcessing`,
  `RetryPolicy`, `IJobFilter`, `JobExecutorNotFoundException`.
- `PgBolt.Abstractions`: `JobAttribute.TimeoutSeconds`, `JobDescriptor.TimeoutSeconds`, `JobAbortedException`,
  `JobArgumentsException`; диагностики генератора BOLT007 (отрицательный `TimeoutSeconds`, ошибка) и BOLT008
  (`TimeoutSeconds` больше предела таймеров .NET 4294967 с, предупреждение: сервер урезает таймаут до предела).
- `PgBoltStore`: `ReleaseAsync`, `RegisterServerAsync`, `RemoveServerAsync`, `GetJobTypeNamesByQueuesAsync`.

### Changed

- Invoker исполнителей оборачивает ошибки десериализации аргументов (`JsonException`, `NotSupportedException`,
  `null` вместо объекта) в `JobArgumentsException`.
- `job_types.published_by` — идентификатор процесса-публикатора `{MachineName}/{ProcessId}`.
```

- [x] **Step 8: Проверить правки**

Run:

```bash
grep -n "идентификатор процесса-публикатора" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "lease_id = ANY(@lease_ids) AND lease_owner = @server" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "WaitAsync\|имя сервера\|Детали — часть 3" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "IDisposable Subscribe" docs/superpowers/specs/2026-09-26-pgbolt-core-design.md
grep -n "^Статус" docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md
grep -c "≤ 1 сут" docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md
grep -c "BOLT008" docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md
grep -n "урезается до 2³² − 2 мс\|SlotAllocator.Plan(isDue)\|Откат незапущенного выполнения\|Таймаут попытки" docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md
grep -n "Статус: реализовано" docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md
grep -n "IDisposable Subscribe\|IReadOnlyCollection<LeaseRef> leases" CHANGELOG.md
grep -n "BOLT008" CHANGELOG.md
git status --short
```

Expected:
- по одной строке в §4.5, §5.5, §5.10 для первого, второго и четвёртого `grep`;
- третий `grep` — ровно одна строка: пункт «Прежний `WaitAsync(queues, timeout, ct)` удалён» в §5.10 (строк «имя сервера» и «Детали — часть 3» нет);
- статус части 3 — «одобрено ревью, реализовано (ред. 4; …)»; в карте — одна строка «Статус: реализовано»;
- уточнения ред. 4 в спеке части 3 на месте (внесены до исполнения плана): `≤ 1 сут` — 5 строк (§3.1 интервалы обслуживания, §3.2 `LeaseDuration`, `ShutdownTimeout`, `AckFlushInterval`, §3.3 `MaxPollInterval`); `BOLT008` — 3 строки (§3.5, §5.1, §10); последний `grep` — по одной строке в §5.1, §4.2, §4.3, §6.1. Меньше — спека не соответствует плану: остановиться и доложить;
- в `CHANGELOG.md` — по строке на каждую из двух breaking changes; `BOLT008` — одна строка в разделе Added;
- `git status --short` — 3 изменённых файла в `docs/superpowers/specs/` и новый `CHANGELOG.md`, других изменений нет.

- [x] **Step 9: Прогон в конце фазы**

Run: `dotnet test PgBolt.slnx --filter "Category!=Load"`
Expected: PASS (правки только в документации; прогон подтверждает итог плана).

- [x] **Step 10: Commit**

```bash
git add docs/superpowers/specs/2026-09-26-pgbolt-core-design.md docs/superpowers/specs/2026-09-28-pgbolt-worker-design.md docs/superpowers/specs/2026-09-26-pgbolt-roadmap.md CHANGELOG.md
git -c user.name=NightNo1r -c user.email=57496218+NightNo1r@users.noreply.github.com commit -m "docs: align core spec with worker runtime, record breaking changes, mark part 3 implemented"
```

---

## Покрытие спеки

| Раздел спеки | Задачи |
|---|---|
| §1 рамки: runtime поверх core, два новых statement'а, изменения core | 1–7, 8–16 |
| §1 критерий: throughput на одном узле PG, напрямую и через PgBouncer | 20 (smoke; SLO — часть 7), 17–19 (оба режима) |
| §1 критерий: ни одна задача не теряется; исход потерянного lease не подтверждается | 12, 13, 19 (потеря lease, повторный захват тем же сервером), 20 (инвариант) |
| §1 критерий: отмена за `HeartbeatInterval` до истечения lease при неподтверждённом продлении | 12, 15, 19 (длительный сбой, dequeue после потери heartbeat, исчерпание пула) |
| §1 критерий: lease брошенной задачи истекает не позже `LeaseDuration` | 12, 13, 15 (откат регистрации при сбое запуска), 16, 18 (игнорирует токен), 19 (сиротская строка, сбой исхода) |
| §1 критерий: отмена доходит за `HeartbeatInterval` | 12, 17 |
| §1 критерий: деплой не тратит попытки при любом числе виртуальных серверов | 14, 15, 16, 18 |
| §1 критерий: соединения ограничены пулом, постоянных нет | 11, 16, 20 |
| §2 wakeup без `LISTEN`/`NOTIFY`, подписка вместо ожидания | 6, 15, 21 |
| §2 heartbeat по известным lease | 4, 12, 19, 21 |
| §2 сбой БД во время выполнения, `validUntil`, пауза выборки, трата попытки | 12, 14, 15, 19 |
| §2 распределение общего пула, один диспетчер на сервер | 10, 15 |
| §2 один координирующий hosted service | 16, 18 |
| §2 retry по умолчанию | 9 |
| §2 shutdown без траты попытки | 14, 15, 16, 18 |
| §2 опциональный таймаут | 1, 2, 14, 17 |
| §3.1 `AddPgBoltServer`, `AddPgBoltJobFilter`, `ConfigurePgBoltProcessing` (границы интервалов обслуживания) | 8 (валидация `PgBoltProcessingOptions`), 16, 17 |
| §3.2 `PgBoltServerOptions`, производные таймауты | 8 (опции, `MonoClock`, верхние границы длительностей), 12, 13, 15 (производные), 14 (`[Job(TimeoutSeconds)]` урезается до предела таймера, §5.1) |
| §3.3 `QueueOptions` (в том числе `MaxPollInterval` ≤ 1 сут), `capacity` | 8, 5 (`servers.capacity`), 16 |
| §3.4 `RetryPolicy`, `retry_delays`, `JobExecutorNotFoundException` | 9, 14, 17 |
| §3.5 `TimeoutSeconds` (BOLT007 — ошибка, BOLT008 — предупреждение о пределе таймера), `JobAbortedException`, `JobArgumentsException` | 1, 2, 3 |
| §3.6 фильтры | 14, 16 |
| §4.1 состав: координатор и компоненты сервера (`SlotAllocator` выдаёт план) | 10, 15, 16, 18 |
| §4.2 слоты и план выборки (`SlotAllocator.Plan`, заменяемая стратегия общего пула; стратегия не может закончить план при готовой очереди со свободным слотом) | 10, 15 (диспетчер обходит план; отказ стратегии — пауза, не холостой цикл) |
| §4.3 fetch-цикл (`for (queue, n) in allocator.Plan()`), пробуждение, таймаут и ошибки dequeue, неожиданные ошибки итерации (с откатом незапущенного выполнения) | 15, 18 (in-flight dequeue при остановке), 19 (dequeue после потери heartbeat) |
| §4.4 старт: `Ready`, публикация `job_types`, регистрация всех серверов до запуска любого, предупреждения, обслуживание, затем `LeaseKeeper`/`Dispatcher`; ошибка шага 6 — остановка запущенного | 7, 5, 16, 17 (NoExecutor-предупреждение) |
| §5.1 `JobRunner`: release до вызова, scope, связанный токен, таймаут (урезание `TimeoutSeconds` до предела таймера с `Warning` один раз на тип задачи), invoker | 3, 14, 17 |
| §5.2 классификация исходов, обрезка `message`/`exception` до 8 КБ UTF-8 (одиночные суррогаты → U+FFFD) | 14, 17 (retry, fail, abort, аргументы, отмена, таймаут, NoExecutor), 18 (shutdown), 19 (`HeartbeatLost`, `LeaseLost`) |
| §6.1 `CompletionSink`: пакетный ack, повтор, бюджет (в том числе таймаут зависшей попытки), отбрасывание (в том числе после остановки sink и ack из буфера при отменённой остановке) | 13, 17 (пакетный ack), 18 (исход брошенной задачи), 19 (сбой исхода, повтор после сбоя) |
| §6.2 `LeaseKeeper`: таблица выполнений, CAS, heartbeat, проверка срока lease, «heartbeat потерян» | 11 (таблица, `Execution`), 12, 19 |
| §6.3 promoter и sweeper на процесс | 16, 17 (promotion retry), 18 (один на процесс), 19 (sweeper) |
| §6.4 остановка (при сработавшем токене хоста — локальные циклы остановлены, шаги с БД пропущены) | 16, 18 |
| §7.1 release SQL | 5 (обе ветки, `ran`, fencing), 18 |
| §7.2 регистрация и снятие сервера | 5 (в том числе slim data source), 16, 18 |
| §8.1 heartbeat по списку lease | 4, 21 (core §5.5, `CHANGELOG.md`) |
| §8.2 `IWakeupSignal.Subscribe`, запись в changelog | 6, 15 (сбой `Subscribe` пользовательской реализации), 21 (core §5.10, `CHANGELOG.md`) |
| §8.3 `PgBoltInitializer.Ready` | 7 |
| §8.4 invoker и `JobDescriptor` | 1, 2, 3 |
| §8.5 `InternalsVisibleTo`, SQL в `PgBoltStore`, `published_by` | 5, 7, 16, 21 (core §4.5) |
| §9 пакет `PgBolt.Server` | 8, 16 |
| §10 unit: `SlotAllocator` (строгий порядок, `Reserved`, `MaxConcurrency`, недобор переходит к следующей очереди, слот того же вида, `n = 0` не в плане) | 10 |
| §10 unit: адаптивный polling и пробуждение | 15 |
| §10 unit: dequeue (таймаут, остановка, release `ran = false`), runner после отмены | 15, 14 |
| §10 unit: классификация исходов | 14 |
| §10 unit: `RetryPolicy` | 9 |
| §10 unit: `LeaseKeeper` | 11, 12 |
| §10 unit: сценарии сбоя heartbeat (включая «исходы не отбрасываются» в «heartbeat потерян») | 12, 15 |
| §10 unit: `CompletionSink` | 13 |
| §10 unit: координатор (один hosted service, отмена всех shutdown-токенов до ожидания, предупреждение по процессу, порядок старта, остановка при сработавшем токене) | 16 |
| §10 unit: `PgBoltInitializer.Ready` | 7 |
| §10 unit: `InProcessWakeupSignal.Subscribe`, `RecordingWakeup` | 6 |
| §10 unit: фильтры (порядок глобальные → серверные — 14 по списку, 16 сквозь DI; короткое замыкание, исключение, scoped-резолв) | 14, 16 |
| §10 unit: валидация опций (в том числе верхние границы длительностей); урезание `[Job(TimeoutSeconds)]` с предупреждением; BOLT007/BOLT008 | 8, 14, 2 |
| §10 интеграционные: enqueue → Succeeded; пакетный ack | 17 |
| §10 интеграционные: retry с promotion; исчерпание; `JobAbortedException`, битые аргументы | 17 |
| §10 интеграционные: отмена через heartbeat | 17 |
| §10 интеграционные: таймаут; задача без таймаута дольше значений по умолчанию | 17 |
| §10 интеграционные: NoExecutor и предупреждение на старте | 17 |
| §10 интеграционные: shutdown (токен, игнорирование токена, `cancel_requested`, строка `servers`) | 18 |
| §10 интеграционные: shutdown во время dequeue | 18 |
| §10 интеграционные: release SQL, обе ветки, fencing | 5, 18 |
| §10 интеграционные: несколько виртуальных серверов, promoter/sweeper по одному на процесс | 18 |
| §10 интеграционные: потеря lease | 19 |
| §10 интеграционные: сиротская строка | 19 |
| §10 интеграционные: сбой исхода | 19 |
| §10 интеграционные: кратковременная и длительная недоступность БД | 19 |
| §10 интеграционные: исчерпание пула | 19 |
| §10 интеграционные: инвариант N серверов × M задач | 20 |
| §10 нагрузочный smoke | 20 |
| Review Focus 1–5 | 16 + 18 (1), 12 (2), 14 (3), 11 + 19 (4), 9 (5) |

Отступления от спеки (с обоснованием в задачах). Уточнения, появившиеся при ревью плана, — верхние границы длительностей, урезание `[Job(TimeoutSeconds)]` с BOLT008 и предупреждением, ленивый `SlotAllocator.Plan` и контракт стратегии, неожиданные ошибки итерации и откат незапущенного выполнения, регистрация всех серверов до запуска любого и ошибка шага 6, обрезка `message`/`exception` в байтах UTF-8, таймаут попытки отправки и отбрасывания при остановке sink, остановка локальных циклов при сработавшем токене хоста, способы проверки shutdown во время dequeue, dequeue во время сбоя heartbeat и нагрузочного smoke, — внесены в саму спеку (ред. 4, §3.1–§3.5, §4.2–§4.4, §5.1, §5.2, §6.1, §6.4, §10) и отступлениями не являются. Остаются только отступления в порядке работы:
- Tasks 17–20 (фаза 2) — проверочные сквозные тесты поверх уже реализованного runtime: продакшен-код в них не пишется, поэтому шага «убедиться, что тест падает» нет; падение сценария — дефект задачи-владельца (см. «Фаза 2»). Инфраструктура Task 17 (`TcpProxy`) — единственное место фазы с циклом «тест → реализация».
- Unit-тесты `PgBolt.Server.Tests` запускаются в одной сборке с интеграционными, поэтому фикстура сборки поднимает контейнеры и при чисто unit-прогоне (Task 8): Docker обязателен и для них (см. «Требования к окружению»).
