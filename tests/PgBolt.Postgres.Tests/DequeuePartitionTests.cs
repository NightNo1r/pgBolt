using Npgsql;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed class DequeuePartitionTests(PostgresFixture pg) : RetentionTestBase(pg, ConnectionMode.Direct)
{
    [Theory]
    [InlineData(DequeueTests.QueueKind.Plain)]
    [InlineData(DequeueTests.QueueKind.Limited)]
    public async Task OpensOnlyLeasedDays(DequeueTests.QueueKind kind)
    {
        // Один подготовленный запрос проверяет и пустую очередь, и задачи двух далёких суток.
        var oldDay = Ago(90);
        await CreatePartitionsAsync(oldDay);
        await using var conn = await Db.Admin.OpenConnectionAsync(Ct);
        await using var read = new NpgsqlCommand(kind == DequeueTests.QueueKind.Plain
            ? Store.DequeueSql : Store.DequeueLimitedSql, conn);
        PgBoltStore.BindDequeue(read.Parameters, "q", 32, ServerA, Lease);
        await using var force = new NpgsqlCommand("SET plan_cache_mode = force_generic_plan", conn);
        await force.ExecuteNonQueryAsync(Ct);
        await read.PrepareAsync(Ct);

        Guid[] ids = [];
        foreach (var days in new[] { 0, 2 })
        {
            if (days > 0)
            {
                ids = [.. await EnqueueLiveAsync(Today, 1), .. await EnqueueLiveAsync(oldDay, 1)];
            }

            await using var tx = await conn.BeginTransactionAsync(Ct);
            var actual = new List<Guid>();
            await using (var r = await read.ExecuteReaderAsync(Ct))
            {
                var id = r.GetOrdinal("id");
                while (await r.ReadAsync(Ct))
                {
                    if (!r.IsDBNull(id))
                    {
                        actual.Add(r.GetGuid(id));
                    }
                }
            }
            Assert.Equal(ids.Order(), actual.Order());

            await using var count = new NpgsqlCommand(Db.Sql("""
                SELECT count(DISTINCT l.relation) FROM pg_locks l JOIN pg_inherits i ON i.inhrelid = l.relation
                WHERE i.inhparent = '{schema}.jobs'::regclass AND l.pid = pg_backend_pid() AND l.granted
                """), conn);
            Assert.Equal((long)days, await count.ExecuteScalarAsync(Ct));
            await tx.RollbackAsync(Ct);
        }
    }

    [Theory]
    [InlineData(DequeueTests.QueueKind.Plain)]
    [InlineData(DequeueTests.QueueKind.Limited)]
    public async Task CancelReadRollsBackLease(DequeueTests.QueueKind kind)
    {
        var id = await EnqueueAsync();
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using (await HoldAsync(J(Today), "ACCESS EXCLUSIVE"))
        {
            Task dequeue = kind == DequeueTests.QueueKind.Plain
                ? Store.DequeueAsync("q", 1, ServerA, Lease, cancel.Token)
                : Store.DequeueLimitedAsync("q", 1, ServerA, Lease, cancel.Token);
            await WaitForLockWaitersAsync(J(Today), 1);
            await cancel.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dequeue);
        }
        Assert.True(await Db.ScalarAsync<bool>("SELECT lease_id IS NULL AND attempt = 0 FROM {schema}.queue WHERE job_id = @id", ("id", id)));
    }
}
