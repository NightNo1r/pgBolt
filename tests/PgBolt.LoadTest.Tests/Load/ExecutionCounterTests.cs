using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Tests.Load;

public sealed class ExecutionCounterTests
{
    private const long ChunkSize = 1 << 20;

    [Fact]
    public void Increment_on_chunk_boundaries()
    {
        var counter = new ExecutionCounter(3 * ChunkSize);
        long[] seqs = [0, ChunkSize - 1, ChunkSize];

        foreach (var seq in seqs) counter.EnsureChunk(seq);
        foreach (var seq in seqs) Assert.Equal(1, counter.Increment(seq));
        Assert.Equal(2, counter.Increment(ChunkSize));

        Assert.Equal(1, counter.Get(0));
        Assert.Equal(1, counter.Get(ChunkSize - 1));
        Assert.Equal(2, counter.Get(ChunkSize));
        Assert.Equal(0, counter.Get(1));
    }

    [Fact]
    public void EnsureChunk_keeps_existing_chunk()
    {
        var counter = new ExecutionCounter(ChunkSize);
        counter.EnsureChunk(5);
        counter.Increment(5);

        counter.EnsureChunk(6);

        Assert.Equal(1, counter.Get(5));
    }

    [Fact]
    public void Concurrent_increments_are_not_lost()
    {
        var counter = new ExecutionCounter(ChunkSize);
        counter.EnsureChunk(0);

        var start = new Barrier(8);
        var threads = Enumerable.Range(0, 8).Select(_ => new Thread(() =>
        {
            start.SignalAndWait();
            for (var i = 0; i < 100_000; i++) counter.Increment(i % 1_000);
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        for (var seq = 0; seq < 1_000; seq++) Assert.Equal(800, counter.Get(seq));
    }
}
