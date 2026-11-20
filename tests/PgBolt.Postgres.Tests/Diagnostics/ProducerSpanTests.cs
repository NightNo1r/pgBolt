using System.Diagnostics;
using Microsoft.Extensions.Time.Testing;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests.Diagnostics;

// Источник PgBolt статический: слушатели этих тестов видят span'ы соседних классов, а бросающий слушатель ломает чужие тесты.
[Collection("Serial")]
public sealed class ProducerSpanTests : IDisposable
{
    private readonly ListLogger _logs = new();
    private readonly PgBoltInstrumentation _instrumentation;
    private ActivityCapture? _capture;
    private Activity? _outer;

    public ProducerSpanTests() => _instrumentation = new PgBoltInstrumentation(null, new FakeTimeProvider(), _logs);

    public void Dispose()
    {
        _capture?.Dispose();
        _outer?.Stop();
        _instrumentation.Dispose();
    }

    private Activity StartOuter()
    {
        _outer = new Activity("outer");
        _outer.SetIdFormat(ActivityIdFormat.W3C);
        return _outer.Start();
    }

    private void Listen(Action<ActivityListener>? configure = null)
    {
        _capture = new ActivityCapture(PgBoltDiagnostics.ActivitySourceName);
        configure?.Invoke(_capture.Listener);
    }

    private ProducerSpan Start(string? queue = "q", int count = 1) =>
        ProducerSpan.Start(_instrumentation, "enqueue", queue, "t", count, null);

    private Activity Span(Activity outer) => Assert.Single(_capture!.Started, a => a.TraceId == outer.TraceId);

    private void AssertSingleTelemetryFailure() => Assert.Single(_logs.Entries, e => e.EventId.Id == 8001);

    [Fact]
    public void Producer_span_is_child_of_current_and_written_as_trace_parent()
    {
        Listen();
        var outer = StartOuter();

        var span = Start();
        var activity = Span(outer);
        Assert.Equal(activity.Id, span.TraceParent);
        span.Dispose();

        Assert.Equal(ActivityKind.Producer, activity.Kind);
        Assert.Equal(outer.Id, activity.ParentId);
        Assert.Equal("enqueue t", activity.OperationName);
        Assert.Equal("pgbolt", activity.GetTagItem("messaging.system"));
        Assert.Equal("send", activity.GetTagItem("messaging.operation.type"));
        Assert.Equal("q", activity.GetTagItem("messaging.destination.name"));
        Assert.Equal("t", activity.GetTagItem("pgbolt.job.type"));
        Assert.Null(activity.GetTagItem("messaging.batch.message_count"));
        Assert.Same(outer, Activity.Current);
    }

    [Fact]
    public void Batch_count_is_tagged_when_more_than_one()
    {
        Listen();
        var outer = StartOuter();

        Start(count: 3).Dispose();

        Assert.Equal(3, Span(outer).GetTagItem("messaging.batch.message_count"));
    }

    [Fact]
    public void Without_listener_no_span_and_trace_parent_is_outer_activity_id()
    {
        Assert.Null(Start().TraceParent);

        // Внешняя Activity другого источника со своим слушателем: PgBolt без слушателя span не создаёт.
        using var other = new ActivitySource("other");
        using var otherCapture = new ActivityCapture(other.Name);
        using var outer = other.StartActivity("outer")!;

        var span = Start();

        Assert.Equal(outer.Id, span.TraceParent);
        Assert.Same(outer, Activity.Current);
        span.Dispose();
        Assert.Same(outer, Activity.Current);
        Assert.DoesNotContain(otherCapture.Started, a => a.OperationName == "enqueue t");

        Listen();
        Start().Dispose();
        Assert.Contains(_capture!.Started, a => a.OperationName == "enqueue t");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Failure_is_reported_after_current_is_restored(bool throwOnStop)
    {
        Listen(l =>
        {
            if (throwOnStop)
                l.ActivityStopped = _ => throw new InvalidOperationException("listener");
            else
                l.ActivityStarted = _ => throw new InvalidOperationException("listener");
        });
        var outer = StartOuter();
        ActivitySpanId? currentOnReport = null;
        _logs.OnEntry = e =>
        {
            if (e.EventId.Id == 8001)
                currentOnReport = Activity.Current?.SpanId;
        };

        Start().Dispose();

        Assert.Equal(outer.SpanId, currentOnReport);
    }

    [Fact]
    public void Hierarchical_parent_is_not_written()
    {
        Listen();
        _outer = new Activity("outer");
        _outer.SetIdFormat(ActivityIdFormat.Hierarchical);
        _outer.Start();

        var span = Start();

        Assert.Equal(ActivityIdFormat.Hierarchical, Assert.Single(_capture!.Started).IdFormat);
        Assert.Null(span.TraceParent);
        span.Dispose();
    }

    [Fact]
    public void Throwing_ActivityStarted_restores_current() =>
        AssertThrowingStartRestoresCurrent(l => l.ActivityStarted = _ => throw new InvalidOperationException("listener"));

    [Fact]
    public void Throwing_Sample_restores_current() =>
        AssertThrowingStartRestoresCurrent(l =>
            l.Sample = (ref ActivityCreationOptions<ActivityContext> _) => throw new InvalidOperationException("listener"));

    private void AssertThrowingStartRestoresCurrent(Action<ActivityListener> configure)
    {
        Listen(configure);
        var outer = StartOuter();

        var span = Start();

        Assert.Same(outer, Activity.Current);
        Assert.Equal(outer.Id, span.TraceParent);
        AssertSingleTelemetryFailure();
        span.Dispose();
        Assert.Same(outer, Activity.Current);
    }

    [Fact]
    public void Throwing_ActivityStopped_restores_current()
    {
        Listen(l => l.ActivityStopped = _ => throw new InvalidOperationException("listener"));
        var outer = StartOuter();
        var span = Start();

        span.Dispose();

        Assert.Same(outer, Activity.Current);
        AssertSingleTelemetryFailure();
    }

    [Fact]
    public void SetQueue_sets_destination_name()
    {
        Listen();
        var outer = StartOuter();
        var span = Start(queue: null);
        var activity = Span(outer);
        Assert.Null(activity.GetTagItem("messaging.destination.name"));

        span.SetQueue(null);
        Assert.Null(activity.GetTagItem("messaging.destination.name"));

        span.SetQueue("q");
        Assert.Equal("q", activity.GetTagItem("messaging.destination.name"));
        span.Dispose();
    }

    [Fact]
    public void SetQueue_without_span_does_nothing()
    {
        var span = Start(queue: null);

        span.SetQueue("q");
        span.Dispose();

        Assert.Empty(_logs.Entries);
    }

    [Fact]
    public void Fail_sets_error_status()
    {
        Listen();
        var outer = StartOuter();
        var span = Start();

        span.Fail(new InvalidOperationException("boom"));
        span.Dispose();

        var activity = Span(outer);
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("System.InvalidOperationException", activity.GetTagItem("error.type"));
        Assert.Equal("exception", Assert.Single(activity.Events).Name);
    }
}
