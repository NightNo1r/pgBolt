using System.Collections.Concurrent;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class ConcurrencyInvariantTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    [Fact]
    public async Task Every_job_is_executed_exactly_once_without_failures()
    {
        const int total = 2000;
        for (var i = 0; i < total; i += 500)
        {
            var ids = Enumerable.Range(0, 500).Select(_ => Guid.CreateVersion7()).ToArray();
            await Store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                ids, ids.Select(_ => "{}").ToArray(), new DateTime?[500], new string?[500]), null, Ct);
        }

        var executions = new ConcurrentBag<Guid>();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
        {
            var server = Guid.NewGuid();
            for (var empty = 0; empty < 3;)
            {
                var jobs = await Store.DequeueAsync("q", 20, server, Lease, Ct);
                if (jobs.Count == 0) { empty++; await Task.Delay(20, Ct); continue; }
                foreach (var j in jobs)
                    executions.Add(j.Id);
                Assert.Equal(jobs.Count, (await Store.AckAsync(server, jobs.Select(j => new LeaseRef(j.Id, j.LeaseId)).ToList(), Ct)).Count);
            }
        }));

        Assert.Equal(total, executions.Count);
        Assert.Equal(total, executions.Distinct().Count());
        Assert.Equal(0L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.queue"));
        Assert.Equal((long)total, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs WHERE state = 2"));
    }

    [Fact]
    public async Task Unique_key_race_creates_exactly_one_job()
    {
        var results = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ =>
            Store.EnqueueAsync(Batch(Guid.CreateVersion7(), uniqueKey: "race"), null, Ct)));

        var flat = results.Select(r => r[0]).ToList();
        var winner = Assert.Single(flat, r => r.Created);
        Assert.All(flat, r => Assert.Equal(winner.Id, r.Id));
        Assert.Equal(1L, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.jobs"));
    }
}

public sealed class ConcurrencyInvariantTests_Direct(PostgresFixture pg) : ConcurrencyInvariantTests(pg, ConnectionMode.Direct);
public sealed class ConcurrencyInvariantTests_Bouncer(PostgresFixture pg) : ConcurrencyInvariantTests(pg, ConnectionMode.Bouncer);
