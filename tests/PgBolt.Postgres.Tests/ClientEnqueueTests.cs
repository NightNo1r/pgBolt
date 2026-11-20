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
        Client = ClientTelemetry.Client(Store, Wakeup);
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

    [Theory]
    [InlineData("pgbolt:recurring:x")]
    [InlineData("pgbolt:")]
    public async Task Reserved_unique_key_prefix_is_rejected(string key)
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await Client.EnqueueAsync(new ClientArgs(1), new EnqueueOptions { UniqueKey = key }, Ct));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await Client.EnqueueManyAsync([new ClientArgs(1)], uniqueKeySelector: _ => key, ct: Ct));
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }
}

public sealed class ClientEnqueueTests_Direct(PostgresFixture pg) : ClientEnqueueTests(pg, ConnectionMode.Direct);

public sealed class ClientEnqueueTests_Bouncer(PostgresFixture pg) : ClientEnqueueTests(pg, ConnectionMode.Bouncer);
