using System.Data;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests.Diagnostics;

public abstract class ProducerTracingTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private ActivityCapture? _capture;
    private Activity? _outer;
    private ServiceProvider _sp = null!;
    private MetricCollector<long> _enqueued = null!;

    private IJobClient Client => _sp.GetRequiredService<IJobClient>();

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        RecurringTestSupport.EnsureRegistered();
        var services = new ServiceCollection();
        services.AddMetrics();
        services.AddPgBolt(o => { o.ConnectionString = Db.ConnectionString; o.Schema = Db.Schema; });
        _sp = services.BuildServiceProvider();
        _enqueued = new MetricCollector<long>(_sp.GetRequiredService<IMeterFactory>(), PgBoltDiagnostics.MeterName,
            PgBoltDiagnostics.Instruments.JobEnqueued);
        await Store.PublishJobTypesAsync(
        [
            new JobTypeRecord("dyn.job", "{}", "dyn", 3, 0, null),
            new JobTypeRecord("ta", "{}", "a", 3, 0, null),
            new JobTypeRecord("tb", "{}", "b", 3, 0, null),
            new JobTypeRecord("rec.job", "{}", "rq", 3, 1, null),
        ], "s", Ct);
    }

    public override async ValueTask DisposeAsync()
    {
        _capture?.Dispose();
        _outer?.Stop();
        await _sp.DisposeAsync();
        await base.DisposeAsync();
    }

    private Activity StartOuter()
    {
        _outer = new Activity("outer");
        _outer.SetIdFormat(ActivityIdFormat.W3C);
        return _outer.Start();
    }

    private void Listen() => _capture = new ActivityCapture(PgBoltDiagnostics.ActivitySourceName);

    private List<Activity> Spans(Activity outer) => [.. _capture!.Started.Where(a => a.TraceId == outer.TraceId)];

    private Activity Span(Activity outer) => Assert.Single(Spans(outer));

    private Task<string?> TraceParentAsync(Guid id) =>
        Db.ScalarAsync<string?>("SELECT trace_parent FROM {schema}.jobs WHERE id = @id", ("id", id));

    private static Guid Id(EnqueueResult r) => r.Id!.Value.Value;

    private static JsonElement Json() => JsonDocument.Parse("""{"a":1}""").RootElement;

    private static (string Queue, string Type, string Result, long Value)[] Enqueued(MetricCollector<long> collector) =>
    [
        .. collector.GetMeasurementSnapshot().Select(m => ((string)m.Tags["pgbolt.queue"]!, (string)m.Tags["pgbolt.job.type"]!,
            (string)m.Tags["pgbolt.enqueue.result"]!, m.Value)),
    ];

    [Fact]
    public async Task Enqueue_writes_producer_span_id()
    {
        Listen();
        var outer = StartOuter();

        var r = await Client.EnqueueAsync(new ClientArgs(1), ct: Ct);

        var span = Span(outer);
        Assert.Equal("enqueue client.test", span.OperationName);
        Assert.Equal(ActivityKind.Producer, span.Kind);
        Assert.Equal(outer.Id, span.ParentId);
        Assert.Equal("client", span.GetTagItem("messaging.destination.name"));
        Assert.Equal(span.Id, await TraceParentAsync(Id(r)));
        Assert.Equal([("client", "client.test", "created", 1L)], Enqueued(_enqueued));
    }

    [Fact]
    public async Task Duplicate_unique_key_counts_duplicate()
    {
        var options = new EnqueueOptions { UniqueKey = "u" };
        await Client.EnqueueAsync(new ClientArgs(1), options, Ct);

        var second = await Client.EnqueueAsync(new ClientArgs(2), options, Ct);

        Assert.False(second.Created);
        Assert.Equal([("client", "client.test", "created", 1L), ("client", "client.test", "duplicate", 1L)], Enqueued(_enqueued));
    }

    [Fact]
    public async Task EnqueueMany_counts_once_per_triple()
    {
        Listen();
        var outer = StartOuter();

        await Client.EnqueueManyAsync([new ClientArgs(1), new ClientArgs(2), new ClientArgs(3)], ct: Ct);

        Assert.Equal(3, Span(outer).GetTagItem("messaging.batch.message_count"));
        Assert.Equal([("client", "client.test", "created", 3L)], Enqueued(_enqueued));
    }

    [Fact]
    public async Task Transactional_enqueue_writes_trace_parent()
    {
        Listen();
        var outer = StartOuter();
        Guid id;
        await using (var conn = await Db.DataSource.OpenConnectionAsync(Ct))
        await using (var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, Ct))
        {
            id = Id(await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { Transaction = tx }, Ct));
            await tx.CommitAsync(Ct);
        }

        Assert.Equal(Span(outer).Id, await TraceParentAsync(id));
    }

    [Fact]
    public async Task Without_listener_writes_current_activity_id()
    {
        var outer = StartOuter();
        var withOuter = await Client.EnqueueAsync(new ClientArgs(1), ct: Ct);
        outer.Stop();
        Activity.Current = null;
        var withoutOuter = await Client.EnqueueAsync(new ClientArgs(2), ct: Ct);

        Assert.Equal(outer.Id, await TraceParentAsync(Id(withOuter)));
        Assert.Null(await TraceParentAsync(Id(withoutOuter)));
    }

    [Fact]
    public async Task Workflow_submit_has_one_span_for_all_nodes()
    {
        Listen();
        var outer = StartOuter();
        var workflow = Client.Workflow();
        workflow.Enqueue("ta", Json());
        workflow.Enqueue("tb", Json());

        var result = await workflow.SubmitAsync(Ct);

        var span = Span(outer);
        Assert.Equal("workflow submit", span.OperationName);
        Assert.Equal(2, span.GetTagItem("messaging.batch.message_count"));
        Assert.Null(span.GetTagItem("messaging.destination.name"));
        foreach (var job in result.Jobs)
            Assert.Equal(span.Id, await TraceParentAsync(job.Value));
        Assert.Equivalent(new[] { ("a", "ta", "created", 1L), ("b", "tb", "created", 1L) }, Enqueued(_enqueued));
    }

    [Fact]
    public async Task Workflow_single_queue_sets_destination()
    {
        Listen();
        var outer = StartOuter();
        var workflow = Client.Workflow();
        workflow.Enqueue("ta", Json());
        workflow.Enqueue("ta", Json());

        await workflow.SubmitAsync(Ct);

        Assert.Equal("a", Span(outer).GetTagItem("messaging.destination.name"));
    }

    [Fact]
    public async Task Recurring_trigger_writes_trace_parent()
    {
        Listen();
        var outer = StartOuter();
        var manager = _sp.GetRequiredService<IRecurringJobManager>();
        await manager.AddOrUpdateAsync("a", "0 3 * * *", new RecArgs(1), ct: Ct);

        var r = await manager.TriggerAsync("a", Ct);

        var span = Span(outer);
        Assert.Equal("recurring trigger", span.OperationName);
        Assert.Equal("a", span.GetTagItem("pgbolt.recurring.id"));
        Assert.Equal("rq", span.GetTagItem("messaging.destination.name"));
        Assert.Equal(span.Id, await TraceParentAsync(Id(r!.Value)));
        Assert.Equal([("rq", "rec.job", "created", 1L)], Enqueued(_enqueued));
    }

    [Fact]
    public async Task Dynamic_enqueue_sets_queue_after_descriptor()
    {
        Listen();
        var outer = StartOuter();

        await Client.EnqueueAsync("dyn.job", Json(), ct: Ct);
        await Client.EnqueueAsync("dyn.job", Json(), new EnqueueOptions { Queue = "other" }, Ct);

        var spans = Spans(outer);
        Assert.All(spans, s => Assert.Equal("enqueue dyn.job", s.OperationName));
        Assert.Equal(["dyn", "other"], spans.Select(s => s.GetTagItem("messaging.destination.name")));
    }

    [Fact]
    public async Task EnqueueMany_empty_list_has_no_span()
    {
        Listen();
        var outer = StartOuter();

        Assert.Empty(await Client.EnqueueManyAsync(Array.Empty<ClientArgs>(), ct: Ct));

        Assert.Empty(Spans(outer));
        Assert.Empty(_enqueued.GetMeasurementSnapshot());
    }

    [Fact]
    public async Task Failed_enqueue_marks_span_error()
    {
        Listen();
        var outer = StartOuter();

        await Assert.ThrowsAsync<UnknownJobException>(async () => await Client.EnqueueAsync("missing.job", Json(), ct: Ct));

        var span = Span(outer);
        Assert.Equal("enqueue missing.job", span.OperationName);
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Null(span.GetTagItem("messaging.destination.name"));
        Assert.Empty(_enqueued.GetMeasurementSnapshot());
    }
}

[Collection("Serial")]
public sealed class ProducerTracingTests_Direct(PostgresFixture pg) : ProducerTracingTests(pg, ConnectionMode.Direct);

[Collection("Serial")]
public sealed class ProducerTracingTests_Bouncer(PostgresFixture pg) : ProducerTracingTests(pg, ConnectionMode.Bouncer);
