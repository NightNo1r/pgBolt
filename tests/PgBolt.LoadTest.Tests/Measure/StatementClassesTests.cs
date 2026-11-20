using Npgsql;
using PgBolt.LoadTest.Measure;
using PgBolt.Postgres.Storage;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class StatementClassesTests
{
    [Fact]
    public void Each_statement_text_has_one_class()
    {
        using var dataSource = NpgsqlDataSource.Create("Host=localhost");
        var store = new PgBoltStore(dataSource, "pgbolt", usePreparedStatements: false);

        Assert.Equal("dequeue", StatementClasses.Classify(store.DequeueSql));
        Assert.Equal("dequeue_limited", StatementClasses.Classify(store.DequeueLimitedSql));
        Assert.Equal("ack", StatementClasses.Classify(store.AckSql));
        Assert.Equal("outcome", StatementClasses.Classify(store.RetrySql));
        Assert.Equal("outcome", StatementClasses.Classify(store.FinalFailSql));
        Assert.Equal("heartbeat", StatementClasses.Classify(store.HeartbeatSql));
        Assert.Equal("promoter", StatementClasses.Classify(store.PromoteSql));
        Assert.Equal("enqueue", StatementClasses.Classify(store.EnqueueSql));
        Assert.Null(StatementClasses.Classify("SELECT count(*) FROM pgbolt.queue WHERE NOT ready"));
    }
}
