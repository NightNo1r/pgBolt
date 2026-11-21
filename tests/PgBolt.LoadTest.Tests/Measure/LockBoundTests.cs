using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class LockBoundTests
{
    [Fact]
    public void GenericLowerBound_matches_formula()
    {
        // Ack и enqueue: все партиции из range table и PK единственной пережившей отсечение.
        Assert.Equal(60 + 1 + 3, LockProbe.GenericLowerBound(60, 3, survivingPartitions: 1));
        Assert.Equal(400 + 1 + 2, LockProbe.GenericLowerBound(400, 2, survivingPartitions: 1));
    }
}
