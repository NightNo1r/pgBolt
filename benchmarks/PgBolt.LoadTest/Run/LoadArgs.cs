using System.Text.Json.Serialization;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Load job arguments. Timestamps use <see cref="Load.RunClock"/> microseconds. For immediate jobs, <see cref="RunAtMicros"/> equals
/// <see cref="EnqueuedAtMicros"/>. <see cref="Padding"/> brings JSON size to about 200 bytes.
/// </summary>
internal sealed record LoadArgs(
    long Seq, long EnqueuedAtMicros, long RunAtMicros, int WorkMillis, bool FailFirstAttempt, string Padding);

[JsonSerializable(typeof(LoadArgs))]
internal sealed partial class LoadJson : JsonSerializerContext;
