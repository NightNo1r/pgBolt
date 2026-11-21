using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Load;

/// <summary>Load job before submission, containing the choices made by the profile.</summary>
internal readonly record struct JobDraft(long Seq, string Queue, bool Delayed, bool FailFirstAttempt);

/// <summary>
/// Load profile: 80% <c>default</c> and 20% <c>critical</c>, with 10% delayed and 1% failing their first attempt. Intended for one generator
/// thread.
/// </summary>
internal sealed class ProfileB(Random rng, QueueProfile queues = QueueProfile.Standard)
{
    public JobDraft Next(long seq) => new(
        seq,
        rng.NextDouble() < 0.2 && queues == QueueProfile.Standard ? "critical" : "default",
        rng.NextDouble() < 0.1,
        rng.NextDouble() < 0.01);

    /// <summary>
    /// Picks a uniformly distributed whole-second delay from 1 through 30. Senders call this concurrently, so supply a thread-safe
    /// <paramref name="rng"/> such as <see cref="Random.Shared"/> instead of the profile generator's instance.
    /// </summary>
    public static int PickDelaySeconds(Random rng) => rng.Next(1, 31);
}
