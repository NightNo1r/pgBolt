using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Slo;

/// <summary>
/// Statement горячего пути в комбинации: итоги <c>pg_stat_statements</c> за окно замера (<c>null</c> — нет данных)
/// и блокировки до нагрузки. <see cref="LowerBound"/> — блокировки generic plan вычислены как нижняя оценка, а не
/// измерены; <see cref="Plan"/> — план в режимах <c>*-prepared</c>, <c>null</c> — statement не подготавливается.
/// </summary>
internal sealed record StatementResult(StatementClassStats? Stats, LockCount? Locks, bool LowerBound, PlanKind? Plan);

/// <summary>
/// Итог одной комбинации матрицы <c>partitions</c>: вердикты SLO 1 и 2, был ли упор в CPU стенда (комбинация
/// «ограничена стендом»), p99 задержки старта немедленных задач, statement'ы горячего пути по классам, число
/// backend'ов в ожидании <c>LockManager</c> по срезам окна замера, <c>pgbolt.storage.errors</c> (в том числе
/// «out of shared memory» при исчерпании таблицы блокировок) и <see cref="Failure"/> — сбой стенда, прервавший
/// комбинацию. Необязательные поля — <c>null</c>, если нет данных.
/// </summary>
internal sealed record CombinationResult(int P, IndexSet Indexes, LoadMode Mode, Verdict Slo1, Verdict Slo2,
    bool CpuBound, double? ImmediateP99Ms = null, IReadOnlyDictionary<string, StatementResult>? Statements = null,
    int? LockManagerWaitsMax = null, double? LockManagerWaitsMean = null, long? StorageErrors = null,
    string? Failure = null, FetchSummary? Fetches = null)
{
    /// <summary>SLO 1 или 2 нарушен; «нет данных» нарушением не считается.</summary>
    public bool Violated => Slo1 == Verdict.Fail || Slo2 == Verdict.Fail;
}

/// <summary>
/// Вердикт <c>partitions</c>. Обязательна только конфигурация по умолчанию (P = 15, текущие индексы): код 1 и упор
/// в CPU зависят только от неё, остальные комбинации дают порог P для выводов. Сбой стенда в любой комбинации — код 2.
/// </summary>
internal static class PartitionsVerdict
{
    private const int DefaultPartitions = 15;

    public static int ExitCode(IReadOnlyList<CombinationResult> results)
    {
        var defaults = results.Where(r => r.P == DefaultPartitions && r.Indexes == IndexSet.Current).ToList();
        return results.Any(r => r.Failure is not null) || defaults.Any(r => r.CpuBound) ? 2
            : defaults.Any(r => r.Violated) ? 1 : 0;
    }

    /// <summary>Наименьшее P, на котором режим и набор индексов нарушают SLO 1 или 2; <c>null</c> — не нарушают.</summary>
    public static int? Threshold(IReadOnlyList<CombinationResult> results, LoadMode mode, IndexSet indexes) =>
        results.Where(r => r.Mode == mode && r.Indexes == indexes && r.Violated)
            .Select(r => (int?)r.P).Min();
}
