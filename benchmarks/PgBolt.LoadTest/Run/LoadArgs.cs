using System.Text.Json.Serialization;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Аргументы задачи нагрузки. Метки — микросекунды <see cref="Load.RunClock"/>; у немедленной задачи
/// <see cref="RunAtMicros"/> = <see cref="EnqueuedAtMicros"/>. <see cref="Padding"/> дополняет JSON до ~200 байт.
/// </summary>
internal sealed record LoadArgs(
    long Seq, long EnqueuedAtMicros, long RunAtMicros, int WorkMillis, bool FailFirstAttempt, string Padding);

[JsonSerializable(typeof(LoadArgs))]
internal sealed partial class LoadJson : JsonSerializerContext;
