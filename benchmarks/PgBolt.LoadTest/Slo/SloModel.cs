using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Slo;

/// <summary>Итог проверки SLO; <see cref="NoData"/> нарушением не считается.</summary>
internal enum Verdict { Pass, Fail, NoData }

/// <summary>Строка таблицы SLO; <see cref="Mark"/> — «enqueue», «ограничено стендом», обе через запятую или нет.</summary>
internal sealed record SloVerdict(int Number, string Name, string Threshold, string Value, Verdict Result, string? Mark);

/// <summary>Границы фаз <c>vacuum</c> от начала прогона.</summary>
internal sealed record PhaseMarks(TimeSpan Phase1Start, TimeSpan Phase2Start, TimeSpan Phase3Start, TimeSpan End);

/// <summary>
/// Результат проверки корректности после дренажа: сверка счётчика выполнений, строки <c>jobs</c> не в
/// Succeeded, строки <c>queue</c>, строки <c>jobs</c> с неверным <c>attempt</c> и счётчики PgBolt.
/// </summary>
internal sealed record CorrectnessData(
    long MissingExecutions, long DuplicatedExecutions, long ExcludedFailedCalls, long JobsNotSucceeded,
    long QueueRowsAfterDrain, bool DrainedInTime, long AttemptMismatches, long OutcomeDropped, long StorageErrors);

/// <summary>
/// Измерения прогона одного режима. <see cref="Windows"/> и <see cref="Samples"/> — все окна и срезы,
/// включая прогрев и дренаж; оцениваются только окна, целиком лежащие в [MeasureStart, MeasureEnd), и срезы
/// из этого отрезка. <see cref="Immediate"/> и <see cref="Delayed"/> — гистограммы за окно замера.
/// </summary>
internal sealed record RunData(
    int Rate, IReadOnlyList<WindowStats> Windows, IReadOnlyList<Sample> Samples,
    LatencySummary Immediate, LatencySummary Delayed, CorrectnessData? Correctness, LoadMode Mode,
    PhaseMarks? Phases, TimeSpan MeasureStart, TimeSpan MeasureEnd);
