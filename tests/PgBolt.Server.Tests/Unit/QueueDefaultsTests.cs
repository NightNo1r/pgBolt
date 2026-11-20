using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

public sealed class QueueDefaultsTests
{
    private static ServerDefinition Server(string name, params (string Queue, int? Cluster, int? Weight)[] queues)
    {
        var options = new PgBoltServerOptions { Workers = 1 };
        foreach (var (queue, cluster, weight) in queues)
            options.Queue(queue, q => { q.ClusterMaxConcurrency = cluster; q.Weight = weight; });
        return new ServerDefinition(name, options);
    }

    [Fact]
    public void Same_queue_with_equal_values_on_two_servers_gives_one_entry()
    {
        var result = QueueDefaults.Collect([Server("a", ("q", 5, 2)), Server("b", ("q", 5, 2))]);

        Assert.Equal([new QueueDefault("q", 5, 2)], result);
    }

    [Fact]
    public void Different_cluster_max_concurrency_is_a_conflict_naming_both_servers()
    {
        var e = Assert.Throws<InvalidOperationException>(() =>
            QueueDefaults.Collect([Server("a", ("q", 5, null)), Server("b", ("q", null, null))]));

        Assert.Equal("PgBolt: queue 'q' has different ClusterMaxConcurrency on servers 'a' (5) and 'b' (null).", e.Message);
    }

    [Fact]
    public void Different_weight_is_a_conflict()
    {
        var e = Assert.Throws<InvalidOperationException>(() =>
            QueueDefaults.Collect([Server("a", ("q", null, 2)), Server("b", ("q", null, 3))]));

        Assert.Equal("PgBolt: queue 'q' has different Weight on servers 'a' (2) and 'b' (3).", e.Message);
    }

    [Fact]
    public void Entries_are_ordered_ordinal()
    {
        var result = QueueDefaults.Collect([Server("a", ("b", null, null), ("a", null, null), ("B", null, null))]);

        Assert.Equal(["B", "a", "b"], result.Select(d => d.Queue));
    }
}
