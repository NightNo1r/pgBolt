using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class JobTypesTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static JobTypeRecord Type(string schema, string queue = "emails") =>
        new("orders.send-receipt", schema, queue, 5, 1, [10, 60]);

    [Fact]
    public async Task Publish_then_get()
    {
        Assert.Empty(await Store.PublishJobTypesAsync([Type("""{"type":"object"}""")], "server-a", Ct));
        var t = await Store.GetJobTypeAsync("orders.send-receipt", Ct);
        Assert.NotNull(t);
        Assert.Equal(("emails", 5, (short)1), (t.DefaultQueue, t.DefaultMaxAttempts, t.DefaultPriority));
        Assert.Equal(new[] { 10, 60 }, t.DefaultRetryDelaysSeconds);
        Assert.Null(await Store.GetJobTypeAsync("missing", Ct));
    }

    [Fact]
    public async Task Different_schema_from_other_live_server_is_reported_and_overwritten()
    {
        await Store.PublishJobTypesAsync([Type("""{"type":"object"}""")], "server-a", Ct);
        var mismatches = await Store.PublishJobTypesAsync([Type("""{"type":"string"}""", "other")], "server-b", Ct);
        Assert.Equal(new[] { "orders.send-receipt" }, mismatches);
        Assert.Equal("other", (await Store.GetJobTypeAsync("orders.send-receipt", Ct))!.DefaultQueue);
    }

    [Fact]
    public async Task Same_server_republish_is_not_a_mismatch()
    {
        await Store.PublishJobTypesAsync([Type("""{"type":"object"}""")], "server-a", Ct);
        Assert.Empty(await Store.PublishJobTypesAsync([Type("""{"type":"string"}""")], "server-a", Ct));
    }
}

public sealed class JobTypesTests_Direct(PostgresFixture pg) : JobTypesTests(pg, ConnectionMode.Direct);
public sealed class JobTypesTests_Bouncer(PostgresFixture pg) : JobTypesTests(pg, ConnectionMode.Bouncer);
