using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Load;

/// <summary>Задача нагрузки до отправки: всё, что решает профиль.</summary>
internal readonly record struct JobDraft(long Seq, string Queue, bool Delayed, bool FailFirstAttempt);

/// <summary>
/// Профиль нагрузки: очередь <c>default</c> 80 % и <c>critical</c> 20 %, отложенных 10 %,
/// падающих на первой попытке 1 %. Не потокобезопасен: им пользуется один генератор.
/// </summary>
internal sealed class ProfileB(Random rng, QueueProfile queues = QueueProfile.Standard)
{
    public JobDraft Next(long seq) => new(
        seq,
        rng.NextDouble() < 0.2 && queues == QueueProfile.Standard ? "critical" : "default",
        rng.NextDouble() < 0.1,
        rng.NextDouble() < 0.01);

    /// <summary>
    /// Задержка отложенной пачки: целые секунды 1..30, равномерно. Вызывается отправителями параллельно,
    /// поэтому нужен потокобезопасный <paramref name="rng"/> (<see cref="Random.Shared"/>), а не генератор профиля.
    /// </summary>
    public static int PickDelaySeconds(Random rng) => rng.Next(1, 31);
}
