using System.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

/// <summary>
/// Несвязанная с другими тестами коллекция без параллелизма: xUnit запускает её после всех параллельных коллекций.
/// PostgresFixture — assembly fixture (AssemblyInfo.cs), поэтому коллекции не нужен свой ICollectionFixture.
/// </summary>
[CollectionDefinition("Serial", DisableParallelization = true)]
public sealed class SerialCollection;

/// <summary>
/// Контракт HOT (§4.2, §11): терминальное обновление jobs остаётся HOT. Только Direct — нужна статистика pg_stat.
/// Page pruning вычищает старые версии, только если xmin-горизонт базы ушёл вперёд; любой снимок в той же базе — открытая
/// транзакция параллельного теста, ANALYZE autovacuum'а (в том числе раздутого тестами pg_attribute) — держит горизонт,
/// страницы переполняются, и доля HOT падает до ~25 %. Поэтому тест идёт в своей базе (горизонт обычных таблиц считается по
/// backend'ам базы), без параллельных тестов (Serial), и перед ack ждёт, пока в базе не останется чужих снимков и ANALYZE.
/// </summary>
[Collection("Serial")]
public sealed class HotUpdateTests(PostgresFixture pg) : StoreTestBase(pg, ConnectionMode.Direct)
{
    protected override Task<TestDb> CreateDbAsync() => TestDb.CreateInOwnDatabaseAsync(Pg);

    [Fact]
    public async Task Terminal_update_of_jobs_is_hot()
    {
        const int count = 2000;
        var args = $$"""{"payload":"{{new string('x', 180)}}"}""";
        var ids = Enumerable.Range(0, count).Select(_ => Guid.CreateVersion7()).ToArray();
        for (var i = 0; i < count; i += 200)
        {
            var chunk = ids[i..(i + 200)];
            await Store.EnqueueAsync(new EnqueueBatch("t", "q", 0, 3, null, null, TimeSpan.Zero,
                chunk, chunk.Select(_ => args).ToArray(), new DateTime?[chunk.Length], new string?[chunk.Length]), null, Ct);
        }
        var leases = new Dictionary<Guid, LeaseRef>(count);
        while (true)
        {
            var jobs = await Store.DequeueAsync("q", 200, ServerA, Lease, Ct);
            if (jobs.Count == 0)
                break;
            foreach (var j in jobs)
                leases.Add(j.Id, new LeaseRef(j.Id, j.LeaseId));
        }
        Assert.Equal(count, leases.Count);

        // Счётчики изменений сброшены — autovacuum не начнёт ANALYZE этой базы в окне ack; идущие снимки и ANALYZE — дождаться.
        await Db.ExecAsync("ANALYZE");
        await WaitForNoForeignSnapshotsAsync();

        // Строки одного enqueue-чанка лежат на соседних страницах (~18 строк на страницу), а запас
        // fillfactor=80 вмещает ~5 новых версий; старые версии той же транзакции не вычищаются.
        // Ack 200 соседних строк одним statement дал бы ~30 % HOT (проверено на PG 17), поэтому
        // пакеты берутся с шагом 10 в порядке вставки — ~2 строки на страницу за statement (§11).
        for (var k = 0; k < 10; k++)
            await Store.AckAsync(ServerA, ids.Where((_, i) => i % 10 == k).Select(id => leases[id]).ToList(), Ct);

        await Db.DataSource.DisposeAsync(); // backend'ы завершаются и сбрасывают статистику
        var deadline = DateTime.UtcNow.AddSeconds(20);
        long updates = 0, hot = 0;
        while (DateTime.UtcNow < deadline)
        {
            updates = await Db.ScalarAsync<long>("SELECT coalesce(sum(n_tup_upd), 0)::bigint FROM pg_stat_user_tables WHERE schemaname = @s AND relname LIKE 'jobs\\_%'", ("s", Db.Schema));
            hot = await Db.ScalarAsync<long>("SELECT coalesce(sum(n_tup_hot_upd), 0)::bigint FROM pg_stat_user_tables WHERE schemaname = @s AND relname LIKE 'jobs\\_%'", ("s", Db.Schema));
            if (updates >= count)
                break;
            await Task.Delay(500, Ct);
        }

        Assert.True(updates >= count, $"n_tup_upd = {updates}");
        Assert.True(hot >= updates * 0.95, $"HOT {hot} of {updates}");
    }

    /// <summary>
    /// Ждёт, пока в базе теста нет других backend'ов со снимком или xid и нет идущего ANALYZE, а в остальных базах — транзакций с
    /// xid: такая транзакция держит xmin снимков всех баз (RecentXmin), и backend не пересчитывает горизонт своей базы, пока
    /// RecentXmin не сдвинется (GlobalVisTestShouldUpdate) — page pruning снова не работает.
    /// </summary>
    private async Task WaitForNoForeignSnapshotsAsync()
    {
        const string busy = """
            SELECT (SELECT count(*) FROM pg_stat_activity
                    WHERE pid <> pg_backend_pid()
                      AND (backend_xid IS NOT NULL OR datname = current_database() AND backend_xmin IS NOT NULL))
                 + (SELECT count(*) FROM pg_stat_progress_analyze a JOIN pg_database d ON d.oid = a.datid
                    WHERE d.datname = current_database())
            """;
        var started = Stopwatch.GetTimestamp();
        while (await Db.ScalarAsync<long>(busy) > 0)
        {
            if (Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(60))
                throw new TimeoutException("Snapshots, transactions or ANALYZE blocking page pruning did not finish within 60 s.");
            await Task.Delay(50, Ct);
        }
    }
}
