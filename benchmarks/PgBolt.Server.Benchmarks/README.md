# PgBolt.Server.Benchmarks — JobRunner overhead

This benchmark measures one successful no-op job through `JobRunner.RunAsync`. It includes the DI scope, argument deserialization and executor call. It performs no database or network I/O. The acceptance criterion allows only the logging scope to add per-job allocations when tracing, metrics and logging have no listeners. The `Mean` column compares timings before and after changes. There is no timing threshold.

`Subscribed` selects the listeners. `None` runs without listeners. `Activity` attaches an `ActivityListener` to the `PgBolt` source and records every activity. `Meter` attaches a `MeterListener` to this instance's instruments. `Both` attaches both listeners. `GlobalSetup` runs the job once and checks that it returns `Ack` so every measurement exercises the successful path.

The project sets `IsTestProject=false`, so `dotnet test PgBolt.slnx` skips it. It accesses internal types in `PgBolt.Server` and `PgBolt.Postgres` through `InternalsVisibleTo`.

## Run

```bash
dotnet run -c Release --project benchmarks/PgBolt.Server.Benchmarks -- --filter '*JobRunner*'
```

BenchmarkDotNet writes the report to `BenchmarkDotNet.Artifacts/results/`.

## Results

Measurements used `DefaultJob` on one Arm64 machine running .NET 10.0.9. The baseline predates telemetry. The other rows compare telemetry before and after adding an early return.

| Variant | Mean | Allocated per job |
|---|---|---|
| Baseline (before telemetry) | 220 ns | 1.01 KB |
| Before early return, `Subscribed = None` | 250 ns | 1.04 KB |
| Before early return, `Subscribed = Meter` | 255 ns | 1.04 KB |
| Before early return, `Subscribed = Activity` | 461 ns | 1.98 KB |
| Before early return, `Subscribed = Both` | 468 ns | 1.98 KB |
| After early return, `Subscribed = None` | 253 ns | 1.04 KB |
| After early return, `Subscribed = Meter` | 252 ns | 1.04 KB |
| After early return, `Subscribed = Activity` | 474 ns | 1.98 KB |
| After early return, `Subscribed = Both` | 474 ns | 1.98 KB |

- **Trace context.** The benchmark job sets `TraceParent: null`, so no variant parses `trace_parent`. The `None` and `Meter` paths are equivalent for this comparison. A few nanoseconds of difference fall within measurement noise. These figures cannot establish the benefit of the early return for jobs that carry a `trace_parent`.
- **Allocations without listeners** rose by about 32 bytes (1.01 → 1.04 KB). The `JobLogScope` object accounts for that increase. With `NullLoggerFactory`, no scope node is created. This is the accepted overhead. No other per-job allocation was added. A `MeterListener` adds no allocations because the measurements use a stack-allocated `TagList`.
- **Time without listeners** rose by about 30 ns (220 → 250–253 ns). The runner records the attempt start, clears `Activity.Current`, records `job.duration` and `job.wait_time`, and opens the logging scope. This overhead is negligible for a job that accesses the database.
- **An `ActivityListener`** adds the `Activity` and its tags, about 1 KB and 210–220 ns per attempt.

For exact bytes per operation, use `BytesAllocatedPerOperation` in the BenchmarkDotNet JSON report (`--exporters json`). The `*-report-github.md` report rounds to kilobytes.
