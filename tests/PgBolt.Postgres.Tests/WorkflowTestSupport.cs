using Npgsql;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>Считает in-process сигналы resolver'у (§6.3: signals &gt; 0 → ICompletionSignal.Signal()).</summary>
public sealed class RecordingCompletionSignal : ICompletionSignal
{
    private int _count;

    public int Count => Volatile.Read(ref _count);

    public void Signal() => Interlocked.Increment(ref _count);

    public IDisposable Subscribe(Action callback) => throw new NotSupportedException("Tests only count signals.");
}

public enum WorkflowOutcome { Succeeded, Failed, Cancelled }

/// <summary>Открытая пользовательская транзакция (соединение режима теста: напрямую или через PgBouncer).</summary>
public sealed record OpenTransaction(NpgsqlConnection Connection, NpgsqlTransaction Transaction) : IAsyncDisposable
{
    public Task CommitAsync() => Transaction.CommitAsync();

    public async ValueTask DisposeAsync()
    {
        await Transaction.DisposeAsync();
        await Connection.DisposeAsync();
    }
}

/// <summary>Клиент и хранилище с записывающими сигналами; помощники финализации конкретной задачи в обход порядка очереди.</summary>
public abstract class WorkflowTestBase(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    protected RecordingWakeup Wakeup { get; } = new();
    protected RecordingCompletionSignal Signal { get; } = new();
    protected PgBoltStore Flow { get; private set; } = null!;
    protected IJobClient Client { get; private set; } = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        ClientMetadata.EnsureRegistered();
        Flow = new PgBoltStore(Db.DataSource, Db.Schema, usePreparedStatements: false, retryOnConflict: true) { Completions = Signal };
        Client = ClientTelemetry.Client(Flow, Wakeup);
    }

    /// <summary>Аргументы зарегистрированного типа client.test (очередь client, 4 попытки, приоритет 2).</summary>
    protected static ClientArgs A(int n) => new(n);

    /// <summary>Lease готовых задач @ids сервером @s в обход порядка очереди; строки — (lease_id, job_id).</summary>
    private const string LeaseSql = """
        UPDATE {schema}.queue
        SET lease_owner = @s, lease_id = gen_random_uuid(), lease_until = now() + interval '5 minutes', leased_at = now(),
            attempt = attempt + 1
        WHERE job_id = ANY(@ids) AND lease_owner IS NULL AND ready AND await_condition IS NULL
        RETURNING lease_id, job_id
        """;

    /// <summary>Берёт в lease именно эту готовую задачу; возвращает lease_id.</summary>
    protected Task<Guid> LeaseAsync(Guid id) => Db.ScalarAsync<Guid>(LeaseSql, ("ids", new[] { id }), ("s", ServerA));

    protected async Task SucceedAsync(Guid id) =>
        Assert.Single(await Flow.AckAsync(ServerA, [new LeaseRef(id, await LeaseAsync(id))], Ct));

    /// <summary>Ack множества готовых задач одним statement'ом (lease в обход очереди).</summary>
    protected async Task SucceedManyAsync(Guid[] ids)
    {
        var leases = new List<LeaseRef>();
        await using (var cmd = Db.Admin.CreateCommand(Db.Sql(LeaseSql)))
        {
            cmd.Parameters.AddWithValue("ids", ids);
            cmd.Parameters.AddWithValue("s", ServerA);
            await using var r = await cmd.ExecuteReaderAsync(Ct);
            while (await r.ReadAsync(Ct))
                leases.Add(new LeaseRef(r.GetGuid(1), r.GetGuid(0)));
        }
        Assert.Equal(ids.Length, (await Flow.AckAsync(ServerA, leases, Ct)).Count);
    }

    protected async Task FailFinallyAsync(Guid id) =>
        Assert.Equal(FailOutcome.Failed, await Flow.FailAsync(id, await LeaseAsync(id), ServerA, JobEventKind.Failed, "boom", null, Ct));

    protected async Task CancelJobAsync(Guid id) => Assert.Equal(CancelResult.Cancelled, await Flow.CancelAsync(id, "test", Ct));

    /// <summary>queue.await_condition; null — не ждёт или строки нет.</summary>
    protected Task<short?> AwaitAsync(Guid id) =>
        Db.ScalarAsync<short?>("SELECT await_condition FROM {schema}.queue WHERE job_id = @id", ("id", id));

    /// <summary>Запущена: строка queue готова к dequeue (ready, не ждёт, без lease).</summary>
    protected Task<bool> ReadyAsync(Guid id) =>
        Db.ScalarAsync<bool>("""
            SELECT coalesce((SELECT ready AND await_condition IS NULL AND lease_owner IS NULL FROM {schema}.queue WHERE job_id = @id), false)
            """, ("id", id));

    protected Task<short?> BatchStateAsync(Guid id) =>
        Db.ScalarAsync<short?>("SELECT state FROM {schema}.batches WHERE id = @id", ("id", id));

    protected Task<long> SignalsAsync() => Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.completions");

    protected Task FinishAsync(Guid id, WorkflowOutcome outcome) => outcome switch
    {
        WorkflowOutcome.Succeeded => SucceedAsync(id),
        WorkflowOutcome.Failed => FailFinallyAsync(id),
        _ => CancelJobAsync(id),
    };

    /// <summary>awaiting — ждёт; launched — запущена (готова к dequeue); иначе по jobs.state: succeeded / failed / cancelled.</summary>
    protected async Task<string> FateAsync(Guid id)
    {
        if (await AwaitAsync(id) is not null)
            return "awaiting";
        if (await ReadyAsync(id))
            return "launched";
        return await JobStateAsync(id) switch
        {
            2 => "succeeded",
            3 => "failed",
            4 => "cancelled",
            var s => $"state {s}",
        };
    }

    /// <summary>Один проход resolver'а без CancelMembers: Т1, затем Т2.</summary>
    protected async Task ResolveOnceAsync()
    {
        await Flow.CountCompletionsAsync(500, Ct);
        await Flow.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), Ct);
    }

    /// <summary>Проходы Т1 + Т2 без CancelMembers, пока захватываются сигналы: идущая отмена batch не продвигается.</summary>
    protected Task ResolveWithoutCancelMembersAsync() => ResolverPasses.SettleAsync(Flow, cancelMembers: false, Ct);

    /// <summary>Итерации resolver'а (Т1, Т2, CancelMembers), пока захватываются сигналы и выполняются порции отмены.</summary>
    protected Task ResolveAllAsync() => ResolverPasses.SettleAsync(Flow, cancelMembers: true, Ct);

    /// <summary>
    /// UUIDv7 упорядочены только с точностью до миллисекунды (Guid.CreateVersion7: остальное — случайно). Ждёт следующую
    /// миллисекунду, чтобы id шагов, созданных после вызова, были больше id созданных до него (порядок обхода CancelMembers).
    /// </summary>
    protected static void NextMillisecond()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        SpinWait.SpinUntil(() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > now);
    }

    protected Task<string> EventMessageAsync(Guid id) =>
        Db.ScalarAsync<string>("SELECT message FROM {schema}.job_events WHERE job_id = @id ORDER BY at DESC LIMIT 1", ("id", id));

    protected async Task<OpenTransaction> BeginAsync()
    {
        var conn = await Db.DataSource.OpenConnectionAsync(Ct);
        return new OpenTransaction(conn, await conn.BeginTransactionAsync(Ct));
    }

    /// <summary>Держит блокировки sql в открытой транзакции прямого соединения; Dispose / Close откатывает её.</summary>
    protected async Task<NpgsqlConnection> HoldRowsAsync(string sql, params (string Name, object Value)[] ps)
    {
        var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var cmd = new NpgsqlCommand(Db.Sql("BEGIN; " + sql), conn);
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(Ct);
        return conn;
    }

    /// <summary>Ждёт, пока не меньше count сессий схемы теста встанут в ожидание блокировки (pg_stat_activity).</summary>
    protected Task WaitForLockWaitersAsync(int count = 1) => Db.WaitForLockWaitersAsync(count: count, ct: Ct);
}
