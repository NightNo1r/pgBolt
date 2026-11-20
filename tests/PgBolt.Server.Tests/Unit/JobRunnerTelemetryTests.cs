using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;
using PgBolt.Testing;
using I = PgBolt.PgBoltDiagnostics.Instruments;

namespace PgBolt.Server.Tests.Unit;

// Источник PgBolt статический: слушатели этих тестов видят span'ы соседних классов, а бросающий слушатель ломает чужие тесты.
[Collection(TelemetryCollection.Name)]
public sealed class JobRunnerTelemetryTests : IAsyncDisposable
{
    private const string TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private readonly FakeTimeProvider _time = new();
    private readonly MonoClock _clock;
    private readonly RunnerProbe _probe = new();
    private readonly JobRegistry _registry = new();
    private readonly PgBoltServerOptions _options = new PgBoltServerOptions().Queue("q");
    private readonly ServiceCollection _services = new();
    private readonly ListLogger _logs = new();
    private readonly PgBoltInstrumentation _instrumentation;
    private readonly PgBoltMetrics _metrics;
    private readonly ConcurrentQueue<(LogEntry Entry, ActivitySpanId? Span)> _written = new();
    private ActivityCapture? _capture;
    private ServiceProvider? _provider;
    private Exception? _thrown;

    public JobRunnerTelemetryTests()
    {
        _clock = new MonoClock(_time);
        _instrumentation = TestTelemetry.Instrumentation(_time, _logs);
        _metrics = TestTelemetry.Metrics(_instrumentation);
        _logs.OnEntry = e => _written.Enqueue((e, Activity.Current?.SpanId));
        _services.AddSingleton(_probe);
        var jobs = new JobsBuilder(_services, _registry);
        jobs.AddExecutor<RunnerArgs, ProbeJob>(new JobDescriptor("runner.job", "q", 3, 0, null), RunnerJson.Default.RunnerArgs);
        jobs.AddExecutor<TimedArgs, ProbeJob>(new JobDescriptor("runner.timed", "q", 3, 0, null) { TimeoutSeconds = 5 },
            RunnerJson.Default.TimedArgs);
        jobs.AddExecutor<LongTimedArgs, ProbeJob>(new JobDescriptor("runner.long", "q", 3, 0, null) { TimeoutSeconds = int.MaxValue },
            RunnerJson.Default.LongTimedArgs);
    }

    public async ValueTask DisposeAsync()
    {
        _capture?.Dispose();
        _instrumentation.Dispose();
        if (_provider is not null)
            await _provider.DisposeAsync();
    }

    private JobRunner Runner(RetryPolicy? policy = null)
    {
        _provider ??= _services.BuildServiceProvider(validateScopes: true);
        var retry = new RetryDecider(policy ?? RetryPolicy.Custom((attempt, _) => TimeSpan.FromSeconds(10 * attempt)), NullLogger.Instance);
        return new JobRunner(_registry, _provider.GetRequiredService<IServiceScopeFactory>(), new FilterPipeline([]), retry,
            _options, _clock, _logs, _metrics, "srv");
    }

    private static Execution Exec(LeasedJob job) => new(job, "q", 0, ExecutionState.Running);

    private static LeasedJob Job(string type = "runner.job", int attempt = 1) =>
        Leased.Job(type: type, args: """{"N":1}""", attempt: attempt, maxAttempts: 3);

    private void Listen(Action<ActivityListener>? configure = null)
    {
        _capture = new ActivityCapture(PgBoltDiagnostics.ActivitySourceName);
        configure?.Invoke(_capture.Listener);
    }

    private static bool Of(Activity a, LeasedJob job) => (string?)a.GetTagItem("messaging.message.id") == job.Id.ToString();

    private Activity Span(LeasedJob job) => Assert.Single(_capture!.Stopped, a => Of(a, job));

    private async Task WaitAndCapture(CancellationToken ct)
    {
        try { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
        catch (OperationCanceledException e)
        {
            _thrown = e;
            throw;
        }
    }

    /// <summary>Сценарий исхода: задача, поведение исполнителя и внешнее событие во время выполнения.</summary>
    private async Task<(LeasedJob Job, JobOutcome Outcome)> Run(string scenario)
    {
        var job = Job(
            type: scenario switch
            {
                "timeout" => "runner.timed",
                "no_executor_retry" or "no_executor_failed" => "missing.job",
                _ => "runner.job",
            },
            attempt: scenario is "failed" or "no_executor_failed" ? 3 : 1);
        _probe.Behavior = scenario switch
        {
            "succeeded" => (_, _) => Task.CompletedTask,
            "retry" or "failed" => (_, _) => Task.FromException(_thrown = new InvalidOperationException("boom")),
            _ => (_, ct) => WaitAndCapture(ct),
        };
        var execution = Exec(job);
        using var shutdown = new CancellationTokenSource();

        var run = Runner().RunAsync(execution, shutdown.Token);
        switch (scenario)
        {
            case "timeout": _time.Advance(TimeSpan.FromSeconds(5)); break;
            case "lease_lost": execution.Cancel(CancelReason.LeaseLost); break;
            case "heartbeat_lost": execution.Cancel(CancelReason.HeartbeatLost); break;
            case "cancelled": execution.Cancel(CancelReason.CancelRequested); break;
            case "released": await shutdown.CancelAsync(); break;
        }
        return (job, await run);
    }

    [Fact]
    public async Task Consumer_span_is_remote_child_of_trace_parent()
    {
        Listen();
        var batch = Guid.CreateVersion7();
        var job = Job(attempt: 2) with { TraceParent = TraceParent, BatchId = batch, RecurringId = "r" };

        await Runner().RunAsync(Exec(job), CancellationToken.None);

        var span = Span(job);
        Assert.Equal(ActivityKind.Consumer, span.Kind);
        Assert.Equal("runner.job", span.DisplayName);
        Assert.Equal(TraceParent, span.ParentId);
        Assert.True(span.HasRemoteParent);
        var expected = new Dictionary<string, object>
        {
            ["messaging.system"] = "pgbolt",
            ["messaging.operation.type"] = "process",
            ["messaging.destination.name"] = "q",
            ["messaging.message.id"] = job.Id.ToString(),
            ["pgbolt.job.type"] = "runner.job",
            ["pgbolt.job.attempt"] = 2,
            ["pgbolt.job.max_attempts"] = 3,
            ["pgbolt.server.name"] = "srv",
            ["pgbolt.batch.id"] = batch.ToString(),
            ["pgbolt.recurring.id"] = "r",
        };
        foreach (var (key, value) in expected)
            Assert.Equal(value, span.GetTagItem(key));
    }

    [Theory]
    [InlineData("garbage")]
    [InlineData(null)]
    public async Task Malformed_trace_parent_starts_root(string? traceParent)
    {
        Listen();
        var job = Job() with { TraceParent = traceParent };

        await Runner().RunAsync(Exec(job), CancellationToken.None);

        var span = Span(job);
        Assert.Null(span.Parent);
        Assert.Equal(default, span.ParentSpanId);
        Assert.DoesNotContain(_logs.Entries, e => e.Level >= LogLevel.Warning);
    }

    [Fact]
    public async Task Ambient_activity_is_not_parent_without_trace_parent()
    {
        Listen();
        var job = Job();
        using var ambient = new Activity("host").Start();

        await Runner().RunAsync(Exec(job), CancellationToken.None);

        var span = Span(job);
        Assert.Equal(default, span.ParentSpanId);
        Assert.NotEqual(ambient.TraceId, span.TraceId);
        Assert.Same(ambient, Activity.Current);
    }

    [Fact]
    public async Task Current_activity_inside_executor_is_consumer_span()
    {
        Listen();
        Activity? seen = null;
        _probe.Behavior = (_, _) =>
        {
            seen = Activity.Current;
            return Task.CompletedTask;
        };
        var job = Job();

        await Runner().RunAsync(Exec(job), CancellationToken.None);

        Assert.Same(Span(job), seen);
    }

    [Theory]
    [InlineData("succeeded", "succeeded", ActivityStatusCode.Unset, null)]
    [InlineData("retry", "retry", ActivityStatusCode.Error, "System.InvalidOperationException")]
    [InlineData("failed", "failed", ActivityStatusCode.Error, "System.InvalidOperationException")]
    [InlineData("timeout", "retry", ActivityStatusCode.Error, "timeout")]
    [InlineData("lease_lost", "retry", ActivityStatusCode.Error, "lease_lost")]
    [InlineData("heartbeat_lost", "retry", ActivityStatusCode.Error, "heartbeat_lost")]
    [InlineData("cancelled", "cancelled", ActivityStatusCode.Unset, null)]
    [InlineData("released", "released", ActivityStatusCode.Unset, null)]
    public async Task Outcome_tags_and_status(string scenario, string outcome, ActivityStatusCode status, string? errorType)
    {
        Listen();

        var (job, _) = await Run(scenario);

        var span = Span(job);
        Assert.Equal(outcome, span.GetTagItem("pgbolt.job.outcome"));
        Assert.Equal(status, span.Status);
        Assert.Equal(errorType, span.GetTagItem("error.type"));
        Assert.Equal(errorType is not null, span.Events.Any(e => e.Name == "exception"));
    }

    [Theory]
    [InlineData((short)1, true, false)]
    [InlineData((short)2, true, false)]
    [InlineData((short)3, false, true)]
    [InlineData((short)4, false, true)]
    [InlineData((short)5, false, false)]
    public async Task Workflow_parent_attributes(short code, bool jobParent, bool batchParent)
    {
        Listen();
        var parent = Guid.CreateVersion7();
        var job = Job() with { ParentId = parent, BatchId = Guid.CreateVersion7(), Continuation = code };

        await Runner().RunAsync(Exec(job), CancellationToken.None);

        var span = Span(job);
        Assert.Equal(jobParent ? parent.ToString() : null, span.GetTagItem("pgbolt.job.parent_job_id"));
        Assert.Equal(batchParent ? parent.ToString() : null, span.GetTagItem("pgbolt.job.parent_batch_id"));
    }

    [Fact]
    public async Task No_executor_has_span_and_exception_event()
    {
        Listen();
        Exception? seenByPolicy = null;
        var policy = RetryPolicy.Custom((_, error) =>
        {
            seenByPolicy = error;
            return TimeSpan.FromSeconds(10);
        });
        var job = Job(type: "missing.job");

        var outcome = await Runner(policy).RunAsync(Exec(job), CancellationToken.None);

        var span = Span(job);
        Assert.Equal("PgBolt.Server.JobExecutorNotFoundException", span.GetTagItem("error.type"));
        var exception = Assert.Single(span.Events, e => e.Name == "exception");
        var logged = Assert.IsType<JobExecutorNotFoundException>(
            Assert.Single(_logs.Entries, e => e.EventId.Id == 3005).Exception);
        Assert.Same(logged, seenByPolicy);
        Assert.Equal(logged.Message, exception.Tags.Single(t => t.Key == "exception.message").Value);
        Assert.Equal(logged.ToString(), Assert.IsType<JobOutcome.Retry>(outcome).Exception);
    }

    [Fact]
    public async Task Shutdown_before_start_has_no_span()
    {
        Listen();
        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();
        var job = Job();

        Assert.Equal(new JobOutcome.Release(Ran: false), await Runner().RunAsync(Exec(job), shutdown.Token));

        Assert.DoesNotContain(_capture!.Started, a => Of(a, job));
        var released = Assert.Single(_logs.Entries, e => e.EventId.Id == 3008);
        Assert.Equal(("JobReleased", LogLevel.Debug, "PgBolt.Job"), (released.EventId.Name, released.Level, released.Category));
        Assert.Contains("ran: False", released.Message);
    }

    [Theory]
    [InlineData("succeeded", 3002, "JobSucceeded", LogLevel.Debug, false, "succeeded in 0 ms")]
    [InlineData("retry", 3003, "JobFailedWillRetry", LogLevel.Warning, true, "attempt 1 of 3 failed (None); retrying in 00:00:10")]
    [InlineData("timeout", 3003, "JobFailedWillRetry", LogLevel.Warning, true, "failed (Timeout)")]
    [InlineData("failed", 3004, "JobFailed", LogLevel.Error, true, "failed at attempt 3 of 3 (None); no attempts left")]
    [InlineData("no_executor_retry", 3005, "JobNoExecutorWillRetry", LogLevel.Warning, true, "of type missing.job; retrying in 00:00:10")]
    [InlineData("no_executor_failed", 3006, "JobNoExecutorFailed", LogLevel.Error, true, "of type missing.job; the job is failed")]
    [InlineData("cancelled", 3007, "JobCancelled", LogLevel.Information, true, "was cancelled")]
    [InlineData("released", 3008, "JobReleased", LogLevel.Debug, false, "(ran: True)")]
    public async Task Lifecycle_events_match_table(string scenario, int id, string name, LogLevel level, bool withException,
        string fragment)
    {
        Listen();

        var (job, _) = await Run(scenario);

        var span = Span(job);
        var events = _written.Where(w => w.Entry.Category == "PgBolt.Job").ToArray();
        Assert.Equal(new[] { 3001, id }, events.Select(w => w.Entry.EventId.Id));
        var started = events[0].Entry;
        Assert.Equal(("JobStarted", LogLevel.Debug), (started.EventId.Name, started.Level));
        Assert.Contains($"job {job.Id} ({job.Type}) attempt {job.Attempt} started on queue q", started.Message);

        var (entry, spanId) = events[1];
        Assert.Equal((name, level), (entry.EventId.Name, entry.Level));
        Assert.Contains(job.Id.ToString(), entry.Message);
        Assert.Contains(fragment, entry.Message);
        if (!withException)
            Assert.Null(entry.Exception);
        else if (scenario.StartsWith("no_executor", StringComparison.Ordinal))
            Assert.IsType<JobExecutorNotFoundException>(entry.Exception);
        else
            Assert.Same(_thrown, entry.Exception);
        Assert.Equal(span.SpanId, spanId);
        Assert.Equal(job.Id, Assert.Single(entry.Scopes.OfType<JobLogScope>())[0].Value);
    }

    [Fact]
    public async Task Scope_is_visible_to_executor_logs()
    {
        _probe.Behavior = (context, _) =>
        {
            context.Logger.LogInformation("inside");
            return Task.CompletedTask;
        };
        var job = Job(attempt: 2);

        await Runner().RunAsync(Exec(job), CancellationToken.None);

        var scope = Assert.Single(Assert.Single(_logs.Entries, e => e.Message == "inside").Scopes.OfType<JobLogScope>());
        KeyValuePair<string, object?>[] fields = [new("JobId", job.Id), new("JobType", "runner.job"), new("Queue", "q"), new("Attempt", 2)];
        Assert.Equal(fields, scope);
        Assert.Equal($"JobId:{job.Id} JobType:runner.job Queue:q Attempt:2", scope.ToString());
    }

    [Fact]
    public async Task Metrics_duration_and_wait()
    {
        using var duration = new MetricCollector<double>(_instrumentation.Meter, I.JobDuration);
        using var wait = new MetricCollector<double>(_instrumentation.Meter, I.JobWaitTime);
        _probe.Behavior = (_, _) =>
        {
            _time.Advance(TimeSpan.FromSeconds(3));
            return Task.CompletedTask;
        };
        var job = Job() with { WaitSeconds = 1.5 };

        await Runner().RunAsync(Exec(job), CancellationToken.None);   // без ActivityListener

        var d = Assert.Single(duration.GetMeasurementSnapshot());
        Assert.Equal(3, d.Value);
        Assert.Equal("q", d.Tags["pgbolt.queue"]);
        Assert.Equal("runner.job", d.Tags["pgbolt.job.type"]);
        Assert.Equal("succeeded", d.Tags["pgbolt.job.outcome"]);
        var w = Assert.Single(wait.GetMeasurementSnapshot());
        Assert.Equal(1.5, w.Value);
        Assert.Equal("q", w.Tags["pgbolt.queue"]);
        Assert.Equal("runner.job", w.Tags["pgbolt.job.type"]);

        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();
        await Runner().RunAsync(Exec(Job() with { WaitSeconds = 7 }), shutdown.Token);
        Assert.Single(wait.GetMeasurementSnapshot());
        Assert.Single(duration.GetMeasurementSnapshot());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Throwing_listeners_do_not_change_outcome(bool throwOnStop)
    {
        _probe.Behavior = (_, _) => Task.FromException(new InvalidOperationException("boom"));
        var expected = Assert.IsType<JobOutcome.Retry>(await Runner().RunAsync(Exec(Job()), CancellationToken.None));

        Listen(l =>
        {
            if (throwOnStop)
                l.ActivityStopped = _ => throw new InvalidOperationException("listener");
            else
                l.ActivityStarted = _ => throw new InvalidOperationException("listener");
        });
        using var meters = new MeterListener();
        meters.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == _instrumentation.Meter)
                l.EnableMeasurementEvents(instrument);
        };
        meters.SetMeasurementEventCallback<double>((_, _, _, _) => throw new InvalidOperationException("meter"));
        meters.Start();
        _logs.OnEntry = e =>
        {
            if (e.EventId.Id is >= 3001 and <= 3008)
                throw new InvalidOperationException("provider");
        };
        _logs.OnBeginScope = _ => throw new InvalidOperationException("scope");
        var current = new ConcurrentQueue<Activity?>();
        _probe.Behavior = (_, _) =>
        {
            current.Enqueue(Activity.Current);
            return Task.FromException(new InvalidOperationException("boom"));
        };

        var outcome = Assert.IsType<JobOutcome.Retry>(await Runner().RunAsync(Exec(Job()), CancellationToken.None));

        Assert.Equal(expected with { Exception = null }, outcome with { Exception = null });
        Assert.Single(_logs.Entries, e => e.EventId.Id == 8001);
        if (!throwOnStop)
            Assert.Null(Assert.Single(current));
    }

    [Fact]
    public async Task Exception_outside_protection_stops_span_and_scope()
    {
        Listen();
        var scopesAtWarning = -1;
        _logs.OnEntry = e =>
        {
            if (e.Category == "PgBolt.Server" && e.Level == LogLevel.Warning)
            {
                scopesAtWarning = _logs.OpenScopes;
                throw new InvalidOperationException("provider");
            }
        };
        var job = Job(type: "runner.long");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Runner().RunAsync(Exec(job), CancellationToken.None));

        Assert.Equal(1, scopesAtWarning);
        Assert.Equal(0, _logs.OpenScopes);
        Span(job);
    }

    [Fact]
    public async Task Duration_excludes_retry_policy_time()
    {
        using var duration = new MetricCollector<double>(_instrumentation.Meter, I.JobDuration);
        _probe.Behavior = (_, _) => Task.FromException(new InvalidOperationException("boom"));
        var policy = RetryPolicy.Custom((_, _) =>
        {
            _time.Advance(TimeSpan.FromMilliseconds(300));
            return TimeSpan.FromSeconds(1);
        });

        Assert.IsType<JobOutcome.Retry>(await Runner(policy).RunAsync(Exec(Job()), CancellationToken.None));

        Assert.Equal(0, Assert.Single(duration.GetMeasurementSnapshot()).Value);
    }

    [Fact]
    public async Task Reason_set_after_classification_reaches_neither_span_nor_log()
    {
        Listen();
        var job = Job();
        var execution = Exec(job);
        _probe.Behavior = (_, _) => Task.FromException(new InvalidOperationException("boom"));
        var policy = RetryPolicy.Custom((_, _) =>
        {
            execution.Cancel(CancelReason.HeartbeatLost);
            return TimeSpan.FromSeconds(1);
        });

        Assert.IsType<JobOutcome.Retry>(await Runner(policy).RunAsync(execution, CancellationToken.None));

        Assert.Equal("System.InvalidOperationException", Span(job).GetTagItem("error.type"));
        Assert.Contains("failed (None)", Assert.Single(_logs.Entries, e => e.EventId.Id == 3003).Message);
    }

    [Fact]
    public async Task No_listener_skips_trace_parent_parsing_but_resets_current()
    {
        using var other = new ActivitySource("PgBolt.Tests.Other");
        using var otherCapture = new ActivityCapture(other.Name);
        using var outer = other.StartActivity("outer");
        Assert.Same(outer, Activity.Current);
        Activity? seen = outer;
        _probe.Behavior = (_, _) =>
        {
            seen = Activity.Current;
            return Task.CompletedTask;
        };

        await Runner().RunAsync(Exec(Job() with { TraceParent = "garbage" }), CancellationToken.None);

        Assert.Null(seen);
        Assert.Same(outer, Activity.Current);
        Assert.DoesNotContain(_logs.Entries, e => e.Level >= LogLevel.Warning);
    }
}
