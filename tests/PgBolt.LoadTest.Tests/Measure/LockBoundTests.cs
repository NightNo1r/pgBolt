using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class LockBoundTests
{
    [Fact]
    public void GenericLowerBound_matches_formula()
    {
        // Dequeue начального отсечения не имеет: все партиции и PK каждой, переживших партиций нет в формуле.
        Assert.Equal(60 + 60 + 2, LockProbe.GenericLowerBound(StatementClasses.Dequeue, 60, 2, survivingPartitions: 1));
        // Ack и enqueue: все партиции из range table и PK единственной пережившей отсечение.
        Assert.Equal(60 + 1 + 3, LockProbe.GenericLowerBound(StatementClasses.Ack, 60, 3, survivingPartitions: 1));
        Assert.Equal(400 + 1 + 2, LockProbe.GenericLowerBound(StatementClasses.Enqueue, 400, 2, survivingPartitions: 1));
    }
}
