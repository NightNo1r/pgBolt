namespace PgBolt.LoadTest.Load;

/// <summary>
/// Сколько раз выполнена каждая задача: зубчатый массив кусков по 2^20 счётчиков, индекс — <c>Seq</c>.
/// Внешний массив ссылок фиксирован по верхней оценке числа задач, а кусок до выдачи первого <c>Seq</c>
/// из него создаёт и публикует генератор: так инкремент в обработчике идёт по уже опубликованному куску,
/// а рост ничего не копирует (копирование теряло бы инкременты по старому массиву).
/// </summary>
internal sealed class ExecutionCounter(long capacity)
{
    private const int ChunkBits = 20;
    private const int ChunkMask = (1 << ChunkBits) - 1;

    private readonly int[]?[] _chunks = new int[(capacity + ChunkMask) >> ChunkBits][];

    /// <summary>Создаёт кусок для <paramref name="seq"/>, если его ещё нет. Вызывает только генератор.</summary>
    public void EnsureChunk(long seq)
    {
        var chunk = (int)(seq >> ChunkBits);
        if (_chunks[chunk] is null) Volatile.Write(ref _chunks[chunk], new int[1 << ChunkBits]);
    }

    /// <summary>Добавляет выполнение; возвращает новое значение. Кусок должен существовать.</summary>
    public int Increment(long seq) => Interlocked.Increment(ref Slot(seq));

    public int Get(long seq) => Volatile.Read(ref Slot(seq));

    private ref int Slot(long seq) =>
        ref Volatile.Read(ref _chunks[(int)(seq >> ChunkBits)])![(int)seq & ChunkMask];
}
