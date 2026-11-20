using System.Text.Json;
using System.Text.Json.Serialization;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Часть 5 §5.2: правила построителя; БД нужна только для SubmitAsync.</summary>
public abstract class WorkflowBuilderTests(PostgresFixture pg, ConnectionMode mode) : WorkflowTestBase(pg, mode)
{
    private static readonly StepOptions Delayed = new() { Delay = TimeSpan.FromMinutes(1) };
    private static readonly StepOptions At = new() { RunAt = DateTimeOffset.UtcNow.AddMinutes(1) };

    [Fact]
    public void Steps_get_uuid_v7_ids_before_submit()
    {
        var wf = Client.Workflow();
        var job = wf.Enqueue(A(1));
        var batch = wf.StartBatch(b => b.Enqueue(A(2)));

        Assert.Equal(7, job.Id.Value.Version);
        Assert.Equal(7, batch.Id.Value.Version);
        Assert.NotEqual(job.Id.Value, batch.Id.Value);
    }

    [Fact]
    public void Run_at_and_delay_are_allowed_only_without_parent_and_outside_awaiting_batches()
    {
        var wf = Client.Workflow();
        var root = wf.Enqueue(A(1), Delayed);
        wf.StartBatch(b =>
        {
            b.Enqueue(A(2), At);
            b.StartBatch(n => n.Enqueue(A(3), Delayed));
            var member = b.Enqueue(A(4));
            Assert.Throws<ArgumentException>(() => member.ContinueWith(A(5), options: Delayed));
        });
        Assert.Throws<ArgumentException>(() => root.ContinueWith(A(6), options: At));
        root.ContinueWithBatch(b =>
        {
            b.Enqueue(A(7));
            Assert.Throws<ArgumentException>(() => b.Enqueue(A(8), Delayed));
            b.StartBatch(n =>
            {
                n.Enqueue(A(9));
                Assert.Throws<ArgumentException>(() => n.Enqueue(A(10), At));
            });
        });
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(11), new StepOptions { RunAt = DateTimeOffset.UtcNow, Delay = TimeSpan.Zero }));
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(12), new StepOptions { Delay = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(13), new StepOptions { MaxAttempts = 0 }));
        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(14), new StepOptions { Queue = "" }));
    }

    [Fact]
    public void Unique_keys_are_rejected_for_members_reserved_prefix_and_duplicates()
    {
        var wf = Client.Workflow();
        var root = wf.Enqueue(A(1), new StepOptions { UniqueKey = "a" });
        root.ContinueWith(A(2), options: new StepOptions { UniqueKey = "b" });
        wf.StartBatch(b =>
        {
            Assert.Throws<ArgumentException>(() => b.Enqueue(A(3), new StepOptions { UniqueKey = "c" }));
            Assert.Throws<ArgumentException>(() => b.EnqueueMany(new[] { A(4) }, new StepOptions { UniqueKey = "d" }));
            b.Enqueue(A(5));
        });

        Assert.Throws<ArgumentException>(() => wf.Enqueue(A(6), new StepOptions { UniqueKey = "pgbolt:x" }));
        Assert.Throws<ArgumentException>(() => root.ContinueWith(A(7), options: new StepOptions { UniqueKey = "a" }));
    }

    [Fact]
    public void Closed_batch_rejects_its_builder_and_member_steps_but_not_the_batch_step()   // §5.2 «Области»
    {
        var wf = Client.Workflow();
        IBatchBuilder? builder = null;
        IJobStep? member = null;
        var batch = wf.StartBatch(b =>
        {
            builder = b;
            member = b.Enqueue(A(1));
            member.ContinueWith(A(2));                       // член того же batch, пока его область открыта
            member.ContinueWithBatch(c => c.Enqueue(A(3)));  // вложенный batch-продолжение
        });

        Assert.Throws<InvalidOperationException>(() => builder!.Enqueue(A(4)));
        Assert.Throws<InvalidOperationException>(() => builder!.StartBatch(n => n.Enqueue(A(5))));
        Assert.Throws<InvalidOperationException>(() => member!.ContinueWith(A(6)));
        batch.ContinueWith(A(7));                            // продолжение batch вне его: корневая область открыта
    }

    [Fact]
    public void Member_of_closed_batch_rejects_continue_with_batch()   // §5.2 «Области»
    {
        var wf = Client.Workflow();
        IJobStep? member = null;
        wf.StartBatch(b => member = b.Enqueue(A(1)));

        Assert.Throws<InvalidOperationException>(() => member!.ContinueWithBatch(c => c.Enqueue(A(2))));
        wf.Enqueue(A(3));                                    // отказ из-за закрытой области не отравляет workflow
    }

    [Fact]
    public void Nested_batch_step_is_closed_with_its_outer_scope()   // §5.2: методы IBatchStep — пока открыта внешняя область
    {
        var wf = Client.Workflow();
        IBatchStep? nested = null;
        wf.StartBatch(b =>
        {
            nested = b.StartBatch(n => n.Enqueue(A(1)));
            nested.ContinueWith(A(2));                       // внешняя область (тело batch) ещё открыта
        });

        Assert.Throws<InvalidOperationException>(() => nested!.ContinueWith(A(3)));
        Assert.Throws<InvalidOperationException>(() => nested!.ContinueWithBatch(c => c.Enqueue(A(4))));
        wf.Enqueue(A(5));
    }

    [Fact]
    public async Task Failed_enqueue_many_adds_no_members()   // EnqueueMany атомарен: сериализация всех элементов до добавления
    {
        PoisonMetadata.EnsureRegistered();
        var wf = Client.Workflow();
        var batch = wf.StartBatch(b =>
        {
            b.Enqueue(A(1));
            Assert.Throws<FormatException>(() => b.EnqueueMany([new PoisonArgs(1), new PoisonArgs(-1)]));
        });

        var r = await wf.SubmitAsync(Ct);

        Assert.Single(r.Jobs);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT total FROM {schema}.batches WHERE id = @id", ("id", batch.Id.Value)));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Enqueue_many_failure_escaping_the_lambda_makes_workflow_unusable()   // решение плана 5
    {
        PoisonMetadata.EnsureRegistered();
        var wf = Client.Workflow();
        wf.Enqueue(A(1));

        Assert.Throws<FormatException>(() => wf.StartBatch(b => b.EnqueueMany([new PoisonArgs(1), new PoisonArgs(-1)])));

        Assert.Throws<InvalidOperationException>(() => wf.Enqueue(A(2)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await wf.SubmitAsync(Ct));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public void Empty_batch_throws_and_makes_workflow_unusable()
    {
        var wf = Client.Workflow();
        Assert.Throws<ArgumentException>(() => wf.StartBatch(_ => { }));
        Assert.Throws<InvalidOperationException>(() => wf.Enqueue(A(1)));

        var nested = Client.Workflow();
        Assert.Throws<ArgumentException>(() => nested.StartBatch(b =>
        {
            b.Enqueue(A(1));
            b.StartBatch(_ => { });
        }));
        Assert.Throws<InvalidOperationException>(() => nested.Enqueue(A(2)));
    }

    [Fact]
    public async Task Exception_in_batch_lambda_makes_workflow_unusable()   // решение плана 5
    {
        var wf = Client.Workflow();
        var root = wf.Enqueue(A(1));
        Assert.Throws<FormatException>(() => wf.StartBatch(b =>
        {
            b.Enqueue(A(2));
            throw new FormatException();
        }));

        Assert.Throws<InvalidOperationException>(() => wf.Enqueue(A(3)));
        Assert.Throws<InvalidOperationException>(() => root.ContinueWith(A(4)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await wf.SubmitAsync(Ct));

        var continuation = Client.Workflow();
        var parent = continuation.Enqueue(A(5));
        Assert.Throws<FormatException>(() => parent.ContinueWithBatch(b =>
        {
            b.Enqueue(A(6));
            throw new FormatException();
        }));
        Assert.Throws<InvalidOperationException>(() => continuation.Enqueue(A(7)));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Submit_requires_steps_and_continuations_of_After()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await Client.Workflow().SubmitAsync(Ct));
        var wf = Client.Workflow();
        wf.After(JobId.New());
        await Assert.ThrowsAsync<ArgumentException>(async () => await wf.SubmitAsync(Ct));
    }

    [Fact]
    public async Task Submit_inside_batch_lambda_throws_and_writes_nothing()   // иначе batch записался бы с total = 0
    {
        var wf = Client.Workflow();
        wf.Enqueue(A(1));
        ValueTask<WorkflowResult> pending = default;

        Assert.Throws<InvalidOperationException>(() => wf.StartBatch(b =>
        {
            pending = wf.SubmitAsync(Ct);
            b.Enqueue(A(2));
        }));

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.batches"));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Methods_after_submit_throw()
    {
        var wf = Client.Workflow();
        var step = wf.Enqueue(A(1));
        await step.SubmitAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await wf.SubmitAsync(Ct));
        Assert.Throws<InvalidOperationException>(() => wf.Enqueue(A(2)));
        Assert.Throws<InvalidOperationException>(() => step.ContinueWith(A(3)));
        Assert.Throws<InvalidOperationException>(() => wf.After(JobId.New()));

        var failed = Client.Workflow();
        await Assert.ThrowsAsync<ArgumentException>(async () => await failed.SubmitAsync(Ct));
        Assert.Throws<InvalidOperationException>(() => failed.Enqueue(A(4)));   // и после неуспешного SubmitAsync
    }

    [Fact]
    public void Typed_step_without_metadata_throws_on_creation() =>
        Assert.Throws<InvalidOperationException>(() => Client.Workflow().Enqueue(new UnregisteredArgs(1)));

    [Fact]
    public async Task Unknown_dynamic_job_throws_before_writing()   // Review Focus 6
    {
        using var args = JsonDocument.Parse("{}");
        var wf = Client.Workflow();
        wf.Enqueue(A(1)).ContinueWith("nope.job", args.RootElement);

        await Assert.ThrowsAsync<UnknownJobException>(async () => await wf.SubmitAsync(Ct));

        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }

    [Fact]
    public async Task Dynamic_step_takes_defaults_from_job_types()
    {
        await Flow.PublishJobTypesAsync([new PgBolt.Postgres.Storage.JobTypeRecord("dyn.job", "{}", "dyn", 6, 3, [7])], "test", Ct);
        using var args = JsonDocument.Parse("""{"x":1}""");

        var r = await Client.Workflow().Enqueue("dyn.job", args.RootElement).SubmitAsync(Ct);

        Assert.True(await Db.ScalarAsync<bool>("""
            SELECT q.queue = 'dyn' AND q.priority = 3 AND q.max_attempts = 6 AND j.retry_delays = ARRAY[7] AND j.args = '{"x":1}'::jsonb
            FROM {schema}.queue q JOIN {schema}.jobs j ON j.id = q.job_id WHERE q.job_id = @id
            """, ("id", r.Jobs[0].Value)));
    }

    [Fact]
    public void Transaction_must_be_npgsql() =>
        Assert.Throws<ArgumentException>(() => Client.Workflow(new WorkflowOptions { Transaction = new FakeTransaction() }));

    [Fact]
    public void Undefined_condition_is_rejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Client.Workflow().Enqueue(A(1)).ContinueWith(A(2), (ContinuationCondition)7));
}

public sealed class WorkflowBuilderTests_Direct(PostgresFixture pg) : WorkflowBuilderTests(pg, ConnectionMode.Direct);
public sealed class WorkflowBuilderTests_Bouncer(PostgresFixture pg) : WorkflowBuilderTests(pg, ConnectionMode.Bouncer);

/// <summary>Аргументы, сериализация которых бросает при отрицательном X (частично выполненный EnqueueMany).</summary>
public sealed record PoisonArgs(int X)
{
    public int Checked => X >= 0 ? X : throw new FormatException("PoisonArgs with negative X cannot be serialized.");
}

[JsonSerializable(typeof(PoisonArgs))]
internal sealed partial class PoisonJson : JsonSerializerContext;

internal static class PoisonMetadata
{
    public static void EnsureRegistered() =>
        JobMetadataRegistry.Register(new JobDescriptor("poison.test", "client", 4, 2, [1]), PoisonJson.Default.PoisonArgs);
}
