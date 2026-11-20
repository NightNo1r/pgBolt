using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: свежесть снимка статистики (§6.4).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum StatsFreshnessKind { Fresh, NotRunning, Failing }

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: свежесть снимка и текст ошибки последнего прохода.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record StatsFreshness(StatsFreshnessKind Kind, string? Error)
{
    /// <summary>
    /// §6.4: строки нет или next_run_at просрочен дольше staleAfter + 0,5 × (next_run_at − last_run_at) — задачу никто не
    /// захватывает (захват ставит срок 0,8 интервала, законное опоздание — до 0,4 интервала); иначе last_error — Failing.
    /// </summary>
    public static StatsFreshness Evaluate(StatsTaskRow? row, DateTime dbNowUtc, TimeSpan staleAfter)
    {
        if (row is null || dbNowUtc - row.NextRunAt > staleAfter + (row.NextRunAt - (row.LastRunAt ?? row.NextRunAt)) * 0.5)
            return new StatsFreshness(StatsFreshnessKind.NotRunning, null);
        return row.LastError is { } error ? new StatsFreshness(StatsFreshnessKind.Failing, error) : new StatsFreshness(StatsFreshnessKind.Fresh, null);
    }
}
