using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Tests.Load;

public sealed class BatchAssemblerTests
{
    private static JobDraft Job(long seq, string queue = "default", bool delayed = false) =>
        new(seq, queue, delayed, false);

    [Fact]
    public void Full_batch_at_100()
    {
        var assembler = new BatchAssembler();

        for (var i = 0; i < 99; i++) Assert.Null(assembler.Add(Job(i), 0));
        var batch = assembler.Add(Job(99), 0);

        Assert.NotNull(batch);
        Assert.Equal(100, batch.Jobs.Count);
        Assert.Equal(new BatchKey("default", false), batch.Key);
        Assert.Empty(assembler.TakeAll());
    }

    [Fact]
    public void Aged_batch_at_10ms()
    {
        var assembler = new BatchAssembler();
        assembler.Add(Job(0), 1_000);
        assembler.Add(Job(1), 5_000);

        Assert.Empty(assembler.TakeAged(1_000 + 9_999));
        var aged = Assert.Single(assembler.TakeAged(1_000 + 10_000));

        Assert.Equal(1_000, aged.FirstAddedMicros);
        Assert.Equal([0L, 1L], aged.Jobs.Select(j => j.Seq));
        Assert.Empty(assembler.TakeAll());
    }

    [Fact]
    public void Keys_are_homogeneous()
    {
        var assembler = new BatchAssembler(maxSize: 5);
        var batches = new List<PendingBatch>();
        (string Queue, bool Delayed)[] mix =
            [("default", false), ("critical", false), ("default", true), ("critical", true)];

        for (var i = 0; i < 40; i++)
        {
            var (queue, delayed) = mix[i % mix.Length];
            if (assembler.Add(Job(i, queue, delayed), 0) is { } full) batches.Add(full);
        }
        batches.AddRange(assembler.TakeAll());

        Assert.Equal(40, batches.Sum(b => b.Jobs.Count));
        foreach (var batch in batches)
            Assert.All(batch.Jobs, j => Assert.Equal(batch.Key, new BatchKey(j.Queue, j.Delayed)));
        Assert.Equal(4, batches.Select(b => b.Key).Distinct().Count());
    }

    [Fact]
    public void TakeAll_flushes_partial()
    {
        var assembler = new BatchAssembler();
        assembler.Add(Job(0, "default"), 0);
        assembler.Add(Job(1, "critical"), 0);

        var all = assembler.TakeAll();

        Assert.Equal(2, all.Count);
        Assert.Equal(["critical", "default"], all.Select(b => b.Key.Queue).Order());
        Assert.Empty(assembler.TakeAll());
    }
}
