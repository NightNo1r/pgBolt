namespace PgBolt.LoadTest.Load;

/// <summary>
/// Counts executions per job using chunks of 2^20 counters indexed by <c>Seq</c>. The outer reference array is sized for the upper job-count
/// estimate. The generator publishes a chunk before issuing its first <c>Seq</c>, so handlers increment existing counters. Growth never copies
/// counters, which would lose concurrent increments on the old array.
/// </summary>
internal sealed class ExecutionCounter(long capacity)
{
    private const int ChunkBits = 20;
    private const int ChunkMask = (1 << ChunkBits) - 1;

    private readonly int[]?[] _chunks = new int[(capacity + ChunkMask) >> ChunkBits][];

    /// <summary>Creates the chunk for <paramref name="seq"/> if needed. Only the generator calls this.</summary>
    public void EnsureChunk(long seq)
    {
        var chunk = (int)(seq >> ChunkBits);
        if (_chunks[chunk] is null) Volatile.Write(ref _chunks[chunk], new int[1 << ChunkBits]);
    }

    /// <summary>Records one execution and returns the new count. The chunk must already exist.</summary>
    public int Increment(long seq) => Interlocked.Increment(ref Slot(seq));

    public int Get(long seq) => Volatile.Read(ref Slot(seq));

    private ref int Slot(long seq) =>
        ref Volatile.Read(ref _chunks[(int)(seq >> ChunkBits)])![(int)seq & ChunkMask];
}
