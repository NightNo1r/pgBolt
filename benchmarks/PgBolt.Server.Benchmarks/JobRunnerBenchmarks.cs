using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Benchmarks;

public sealed record BenchArgs(int N);

[JsonSerializable(typeof(BenchArgs))]
internal sealed partial class BenchJson : JsonSerializerContext;

/// <summary>Задача без работы: в замер попадает только накладная JobRunner.</summary>
public sealed class NoopJob : IJob<BenchArgs>
{
    public Task ExecuteAsync(BenchArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>Какие слушатели телеметрии подключены на время замера.</summary>
public enum Listeners { None, Activity, Meter, Both }

/// <summary>Один <c>JobRunner.RunAsync</c> успешной задачи: scope, десериализация аргументов, вызов исполнителя.</summary>
[MemoryDiagnoser]
public class JobRunnerBenchmarks
{
    private ServiceProvider _provider = null!;
    private PgBoltInstrumentation _instrumentation = null!;
    private ActivityListener? _activityListener;
    private MeterListener? _meterListener;
    private JobRunner _runner = null!;
    private LeasedJob _job = null!;

    [Params(Listeners.None, Listeners.Activity, Listeners.Meter, Listeners.Both)]
    public Listeners Subscribed { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        var registry = new JobRegistry();
        new JobsBuilder(services, registry).AddExecutor<BenchArgs, NoopJob>(
            new JobDescriptor("bench.job", "q", 3, 0, null), BenchJson.Default.BenchArgs);
        _provider = services.BuildServiceProvider(validateScopes: true);

        var retry = new RetryDecider(RetryPolicy.Custom((attempt, _) => TimeSpan.FromSeconds(10 * attempt)), NullLogger.Instance);
        var clock = new MonoClock(TimeProvider.System);
        _instrumentation = new PgBoltInstrumentation(null, TimeProvider.System, null);
        var processing = new PgBoltProcessingOptions();
        var metrics = new PgBoltMetrics(_instrumentation, new ClusterQueueStats(clock, processing), new ClusterTableStats(clock, processing));
        _runner = new JobRunner(registry, _provider.GetRequiredService<IServiceScopeFactory>(), new FilterPipeline([]), retry,
            new PgBoltServerOptions().Queue("q"), clock, NullLoggerFactory.Instance, metrics, serverName: "bench");
        _job = new LeasedJob(Guid.CreateVersion7(), Guid.NewGuid(), "bench.job", """{"N":1}""", 1, 3, null,
            ParentId: null, BatchId: null, TraceParent: null);

        if (Subscribed is Listeners.Activity or Listeners.Both)
            _activityListener = SubscribeToActivities();
        if (Subscribed is Listeners.Meter or Listeners.Both)
            _meterListener = SubscribeToMeter();

        // Замеряется успешный путь: любой другой исход означал бы, что бенчмарк мерит не то.
        if (Run().GetAwaiter().GetResult() is not JobOutcome.Ack)
            throw new InvalidOperationException("The benchmark job did not finish with Ack.");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _activityListener?.Dispose();
        _meterListener?.Dispose();
        _provider.Dispose();
        _instrumentation.Dispose();
    }

    [Benchmark]
    public Task RunAsync_success() => Run();

    private Task<JobOutcome> Run() =>
        _runner.RunAsync(new Execution(_job, "q", 0, ExecutionState.Running), CancellationToken.None);

    private static ActivityListener SubscribeToActivities()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == PgBoltDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private MeterListener SubscribeToMeter()
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter == _instrumentation.Meter)
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<double>((_, _, _, _) => { });
        listener.SetMeasurementEventCallback<long>((_, _, _, _) => { });
        listener.Start();
        return listener;
    }
}
