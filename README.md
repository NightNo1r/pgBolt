# PgBolt

[English](README.md) · [Русский](README.ru.md)

PgBolt runs .NET 10 background jobs on PostgreSQL. An API can save an order and enqueue a report in the same transaction. After commit, a worker picks up the job, calls its C# handler and records the result in PostgreSQL. The client and worker can share a process or run as separate applications.

## Contents

- [Choosing packages](#packages)
- [Quick start with PostgreSQL](#quickstart)
- [Separate client and worker applications](#services)
- [Enqueueing, deduplication and transactions](#enqueue)
- [Execution, retries and cancellation](#execution)
- [Queues and concurrency](#queues)
- [Cron schedules](#cron)
- [Job continuations and batches](#workflows)
- [Dashboard and observability](#dashboard)
- [PostgreSQL, PgBouncer and limitations](#operations)
- [Build, tests and load testing](#development)

<a id="packages"></a>

## Choosing packages

| What your application does | Package to reference |
| --- | --- |
| Declares job arguments or calls the typed API | Reference `PgBolt.Abstractions` directly for attributes, interfaces, the source generator and analyzers. |
| Enqueues jobs from an API, CLI or service | `PgBolt.Postgres` provides `AddPgBolt` and the `IJobClient` implementation. |
| Executes jobs | `PgBolt.Server` provides `AddPgBoltServer` and brings in `PgBolt.Postgres`. |
| Displays jobs in ASP.NET Core | Add `PgBolt.Dashboard` to the web application. |
| Exports metrics and traces through OpenTelemetry | Add `PgBolt.OpenTelemetry` alongside your provider and exporter. |

For a worker that uses typed jobs, install these packages from NuGet:

```sh
dotnet add package PgBolt.Abstractions --version 0.1.0
dotnet add package PgBolt.Server --version 0.1.0
```

Use the same version for all PgBolt packages.

<a id="quickstart"></a>

## Quick start with PostgreSQL

This console app enqueues one job on the `reports` queue every time it starts. A worker prints the report number. You need the .NET 10 SDK, Docker and a Bash or Zsh shell.

**1. Create the app and install the packages from NuGet.** Run these commands in the directory where you want to create the project.

```sh
dotnet new console -n PgBoltDemo -f net10.0
cd PgBoltDemo
dotnet add package PgBolt.Abstractions --version 0.1.0
dotnet add package PgBolt.Server --version 0.1.0
```

**2. Start PostgreSQL.** The container listens on localhost. To use an existing test database, set `PGBOLT_CONNECTION` to its connection string and go to step 3. The database user needs permission to create the schema, tables and partitions.

```sh
docker run --rm -d --name pgbolt-guide-db -p 127.0.0.1:55432:5432 \
  -e POSTGRES_DB=pgbolt_demo -e POSTGRES_PASSWORD=pgbolt_demo postgres:17-alpine
export PGBOLT_CONNECTION='Host=127.0.0.1;Port=55432;Database=pgbolt_demo;Username=postgres;Password=pgbolt_demo'
```

**3. Create `ReportJob.cs` next to `Program.cs`.** The `[Job]` attribute sets the persisted job name and default queue. The JSON context describes how to serialize the arguments. The handler can receive dependencies through constructor injection.

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
        Console.WriteLine($"Processing report {args.ReportId}, attempt {context.Attempt}");
        return Task.CompletedTask;
    }
}

[JsonSerializable(typeof(ReportArgs))]
internal sealed partial class ReportJson : JsonSerializerContext;
```

**4. Replace `Program.cs`.** `AddPgBolt` registers the client and schema initialization. `AddJobs` registers this assembly’s handlers through its generated registry. `AddPgBoltServer` starts workers for the selected queues.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgBolt;
using PgBoltDemo;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddPgBolt(options =>
{
    options.ConnectionString = Environment.GetEnvironmentVariable("PGBOLT_CONNECTION")
        ?? throw new InvalidOperationException("Set PGBOLT_CONNECTION");
    options.AddJobs(PgBoltJobRegistry.Register);
});
builder.Services.AddPgBoltServer("reports-worker", server => server.Queue("reports"));

using var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();
var result = await client.EnqueueAsync(new ReportArgs(42));
Console.WriteLine($"Enqueued job {result.Id}");
await host.WaitForShutdownAsync();
```

**5. Run the job.** Once PostgreSQL is ready, run the app in the same terminal.

```sh
dotnet run --no-restore
```

The app prints a job ID and `Processing report 42, attempt 1`. The host keeps listening. Press Ctrl+C to stop it. If you used the example container, run `docker stop pgbolt-guide-db` to remove it and its demo database.

<a id="services"></a>

## Separate client and worker applications

| Assembly | Responsibilities |
| --- | --- |
| Contracts | Declares the public `ReportArgs` type with `[Job("reports.generate", Queue = "reports")]`. Both the API and worker reference this assembly. |
| API | Calls `AddPgBolt` with the shared database and schema. Inject `IJobClient` into an endpoint or service and call `EnqueueAsync`. Typed calls require the generator and a JSON context that includes the argument type. |
| Worker | Contains `ReportJob` and a JSON context. Call `AddJobs(...Register)` for each handler assembly, then `AddPgBoltServer` with the queues this process serves. |

PgBolt generates `PgBoltJobRegistry` in the root namespace of each handler assembly. Use fully qualified registry names when registering multiple assemblies. If a typed enqueue call fails to compile, check the project containing the call for a direct reference to `PgBolt.Abstractions`, the `PgBoltJsonContext` assembly attribute and `[JsonSerializable(typeof(ReportArgs))]`. [Example contracts](tests/E2E/PgBolt.E2E.Contracts/Contracts.cs) · [Handler and JSON context](tests/E2E/PgBolt.E2E.JobsA/PingJob.cs) · [Multiple handler assemblies](tests/E2E/PgBolt.E2E.Tests/EndToEndTests.cs)

A client can also pass a job name and a `JsonElement` to `EnqueueAsync`. It only needs the name and JSON shape. Start a worker at least once so it can publish the type to `job_types`. An unknown name raises `UnknownJobException`. When changing the contract, account for jobs already queued with older arguments.

<a id="enqueue"></a>

## Enqueueing, deduplication and transactions

By default, a job is eligible to run immediately. Within a queue, workers pick ready jobs by highest priority, then earliest run time. Concurrent jobs can finish in a different order. Use `EnqueueOptions` to override the queue, priority and retry settings for one call. `Delay` uses the PostgreSQL clock. Use `RunAt` to specify a timestamp.

```csharp
var result = await client.EnqueueAsync(new ReportArgs(42), new EnqueueOptions
{
    Delay = TimeSpan.FromMinutes(5),
    UniqueKey = "reports.generate:42"
});
```

- **Deduplication.** `UniqueKey` is unique across active jobs in the schema, including other queues. A duplicate returns `result.Created == false`. If the original job has already finished, `Id` can be empty. Removing the queue row frees the key for reuse. The `pgbolt:` prefix is reserved.
- **Bulk enqueue.** `EnqueueManyAsync` takes a list of arguments of the same type and returns one result per item. Supply a `uniqueKeySelector` for per-item keys. Use bulk enqueue for independent jobs that are ready to run together.
- **Managing a job by ID.** `GetAsync(id)` returns the job state or `null`. Use `TriggerAsync(id)` to bring forward a delayed job and `RequeueAsync(id)` to requeue a `Failed` or `Cancelled` job. [Client contract](src/PgBolt.Abstractions/IJobClient.cs)

**Enqueue a job alongside application data.** Open a `ReadCommitted` Npgsql transaction in the same database. Run the application SQL and pass the transaction to the client before committing. A rollback discards both writes. A worker can see the job only after commit. Here, `connection` is an open Npgsql connection.

```csharp
await using var tx = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted);
// Save application data using connection and tx.
await client.EnqueueAsync(new ReportArgs(42), new EnqueueOptions { Transaction = tx });
await tx.CommitAsync();
```

<a id="execution"></a>

## Execution, retries and cancellation

PgBolt provides **at-least-once execution**. A worker leases a job and renews the lease periodically. If it makes an external call and crashes before recording the result, another worker can pick up the same job. When charging a card or sending a message, use a stable idempotency key with the external system or record completion in the same transaction as your business change. `UniqueKey` deduplicates active enqueues only.

| Event | What happens |
| --- | --- |
| The handler completes successfully | The worker acknowledges the job. `GetAsync` reports `Succeeded`. |
| The handler throws | PgBolt schedules a retry while attempts remain. The default `MaxAttempts = 10` includes the first attempt. The base delay grows from 15 seconds to a maximum of one hour, with ±20% jitter. |
| A caller invokes `CancelAsync` | A pending job can be cancelled immediately. A running job returns `CancellationRequested`. The handler receives the request through its `CancellationToken`. Pass the token to queries and long-running operations. |
| A job needs a timeout | Set `TimeoutSeconds` on `[Job]` or set the server’s `DefaultJobTimeout`. The handler must observe cancellation. |

Set `MaxAttempts` and `RetryDelaysSeconds` on the contract, override `MaxAttempts` and `RetryDelays` when enqueuing, or configure the server’s `RetryPolicy`. `JobContext` exposes the attempt number, job ID and logger. [Job attribute](src/PgBolt.Abstractions/JobAttribute.cs) · [Retry policy](src/PgBolt.Server/RetryPolicy.cs)

<a id="queues"></a>

## Queues and concurrency

`Workers` sizes the virtual server’s shared pool. This example has eight shared slots and two reserved for reports, for ten slots in total. Across all servers, at most four jobs from the `reports` queue can run at once.

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

`MaxConcurrency` limits a queue within one virtual server. `Weight` sets a weighted queue’s relative share of the common pool. Use `IQueueSettingsManager` to change the cluster limit and weight at runtime. A runtime limit of `0` pauses new dequeues while jobs already leased continue to run. Other processes pick up a newly introduced limit on their next settings refresh, within ten seconds by default. [Queue options](src/PgBolt.Server/QueueOptions.cs) · [Runtime settings](src/PgBolt.Abstractions/IQueueSettingsManager.cs)

<a id="cron"></a>

## Cron schedules

Define schedules that ship with your application inside `AddPgBolt`. This example enqueues a job at 08:00 UTC on weekdays. Use a stable `RecurringOwner` name shared by all replicas of the application.

```csharp
options.RecurringOwner = "reporting";
options.AddRecurring("weekday-report", "0 8 * * 1-5", new ReportArgs(42));
```

Cron accepts five fields, or six including seconds. Schedules use UTC by default. For local time, set `RecurringJobOptions.TimeZone`. After downtime, `FireOnce` creates one job for missed occurrences. `CatchUp` replays occurrences within the configured window. The default `OverlapPolicy.Skip` skips an occurrence while the previous job is active.

Use `IRecurringJobManager.AddOrUpdateAsync` for user-managed schedules. The manager can pause, resume and trigger a schedule manually. For code-defined schedules, the owning application controls the definition and parameters. At least one server must have `RunScheduler = true` to fire scheduled jobs (the default). [Scheduling API](src/PgBolt.Abstractions/IRecurringJobManager.cs) · [Time zones and policies](src/PgBolt.Abstractions/RecurringJobOptions.cs)

<a id="workflows"></a>

## Job continuations and batches

`Workflow()` defines dependencies between jobs. `SubmitAsync()` writes the whole graph in one transaction. Here, report 43 becomes eligible after report 42 succeeds.

```csharp
var first = client.Workflow().Enqueue(new ReportArgs(42));
var next = first.ContinueWith(new ReportArgs(43));
await next.SubmitAsync();
```

- `OnSuccess` is the default. If the parent fails, the continuation waits for the parent to be requeued. Cancelling the parent cancels the dependent continuation. `OnAnyFinished` runs the continuation after any terminal outcome.
- `Batch` groups jobs under a shared state and counters. `ContinueWithBatch` creates a group after the preceding step. For example, prepare files in parallel and then assemble them into an archive.
- A server with `RunMaintenance = true` processes continuations and batches. If all servers disable maintenance, completion signals accumulate and continuations stay pending. [Workflow contract](src/PgBolt.Abstractions/Workflow.cs) · [End-to-end batch example](tests/E2E/PgBolt.E2E.Tests/WorkflowEndToEndTests.cs)

<a id="dashboard"></a>

## Dashboard and observability

In ASP.NET Core, call `AddPgBolt` and `AddPgBoltDashboard` before `Build()`, then map the route. This example assumes authentication and a `PgBoltReaders` policy are already configured. It exposes a read-only dashboard at `/pgbolt`.

```csharp
builder.Services.AddPgBoltDashboard(options => options.ReadOnly = true);
var app = builder.Build();
app.MapPgBoltDashboard().RequireAuthorization("PgBoltReaders");
```

The dashboard displays queues, jobs, batches and schedules. To allow operator actions, enable writes and set a separate `ActionsPolicy`. When `ReadOnly` is off and `ActionsPolicy` is empty, anyone who can view the dashboard can perform actions. For remote access, apply a viewing policy to the route group. [Dashboard options](src/PgBolt.Dashboard/PgBoltDashboardOptions.cs)

PgBolt correlates enqueue and execution in a single trace. When using OpenTelemetry, add `AddPgBoltInstrumentation()` to the tracing and metrics builders. The [observability guide](docs/observability.md) covers exporters, PromQL queries and the difference between process and cluster metrics.

<a id="operations"></a>

## PostgreSQL, PgBouncer and limitations

| Area | Operational considerations |
| --- | --- |
| Database and schema | Every participant sharing a queue must use the same database and `Schema` (default `pgbolt`). At startup, PgBolt checks that the database’s default isolation level is `READ COMMITTED`. |
| Migrations | `AutoMigrate = true` by default. To run migrations separately, obtain SQL from `PgBoltMigrations.GetScript(schema)`, apply it before startup and set `AutoMigrate = false`. Initialization still checks the schema and prepares partitions. Early experimental schemas require manual recreation. [Changelog](CHANGELOG.md) |
| Partitions and maintenance | By default PgBolt prepares partitions seven days ahead. Keep `RunMaintenance` enabled on at least one server. If partition creation stops, enqueueing starts to fail once the prepared partitions run out. [Processing options](src/PgBolt.Server/PgBoltProcessingOptions.cs) |
| History retention | `JobsRetention` and `EventsRetention` both default to seven days. Finished jobs expire based on the UTC day they were **created**. Events expire based on the day they occurred. Unfinished jobs are retained. Use the same retention periods in every process sharing a schema. Set a retention period to `null` in code to keep that history indefinitely. |
| PgBouncer | PgBolt has been tested with PostgreSQL 17 and PgBouncer 1.24 in `pool_mode=transaction`. Put the pool address in `ConnectionString`. The default is `UsePreparedStatements = false`. Before enabling prepared statements, check your pool’s support and its `max_prepared_statements` setting. |
| Latency and throughput | Workers in separate processes discover new jobs by polling PostgreSQL. Latency depends on the polling interval, available slots and load. The benchmark sustained 10,000 enqueues/s for ten minutes both directly and through PgBouncer. SLOs 8 and 9 for sustained load and recovery after a held `xmin` remain open. [Methodology and results](docs/performance.md) |

<a id="development"></a>

## Build, tests and load testing

With Docker running, the following command checks enqueueing and execution both directly and through PgBouncer. Run it from the repository root. Expect two tests to pass.

```sh
dotnet test tests/E2E/PgBolt.E2E.Tests/PgBolt.E2E.Tests.csproj --filter FullyQualifiedName~Jobs_from_three_executor_assemblies_run_end_to_end
```

To build the solution and run the full test suite from the same directory:

```sh
dotnet build PgBolt.slnx
dotnet test PgBolt.slnx
```

To measure your own configuration, follow the [load test guide](benchmarks/PgBolt.LoadTest/README.md). The [performance report](docs/performance.md) records the test environment and results.
