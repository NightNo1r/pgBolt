namespace PgBolt.LoadTest.Load;

/// <summary>Ключ пачки: у одного вызова <c>EnqueueManyAsync</c> одна очередь и одна задержка.</summary>
internal readonly record struct BatchKey(string Queue, bool Delayed);

internal sealed record PendingBatch(BatchKey Key, IReadOnlyList<JobDraft> Jobs, long FirstAddedMicros);

/// <summary>
/// Раскладывает задачи в пачки по ключу. Пачка готова, когда в ней <paramref name="maxSize"/> задач
/// или с добавления первой прошло <paramref name="maxAgeMicros"/>. Не потокобезопасен: им пользуется один генератор.
/// </summary>
internal sealed class BatchAssembler(int maxSize = 100, long maxAgeMicros = 10_000)
{
    private readonly Dictionary<BatchKey, (List<JobDraft> Jobs, long FirstAddedMicros)> _open = [];

    /// <summary>Добавляет задачу; возвращает пачку, если она заполнилась.</summary>
    public PendingBatch? Add(JobDraft job, long nowMicros)
    {
        var key = new BatchKey(job.Queue, job.Delayed);
        if (!_open.TryGetValue(key, out var open))
            _open[key] = open = ([], nowMicros);

        open.Jobs.Add(job);
        return open.Jobs.Count >= maxSize ? Close(key) : null;
    }

    /// <summary>Забирает пачки, дожидавшиеся не меньше <c>maxAgeMicros</c>.</summary>
    public IReadOnlyList<PendingBatch> TakeAged(long nowMicros) =>
        Take(open => nowMicros - open.FirstAddedMicros >= maxAgeMicros);

    /// <summary>Забирает все начатые пачки.</summary>
    public IReadOnlyList<PendingBatch> TakeAll() => Take(_ => true);

    private List<PendingBatch> Take(Func<(List<JobDraft> Jobs, long FirstAddedMicros), bool> ready) =>
        [.. _open.Where(pair => ready(pair.Value)).Select(pair => pair.Key).ToList().Select(Close)];

    private PendingBatch Close(BatchKey key)
    {
        var (jobs, firstAddedMicros) = _open[key];
        _open.Remove(key);
        return new PendingBatch(key, jobs, firstAddedMicros);
    }
}
