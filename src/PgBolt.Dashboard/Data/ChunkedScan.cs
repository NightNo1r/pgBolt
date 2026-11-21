namespace PgBolt.Dashboard.Data;

/// <summary>Matching rows and the last key scanned. Scanned &lt; PortionSize indicates the end of the data.</summary>
internal sealed record Portion<T>(IReadOnlyList<T> Matches, int Scanned, ListKey? LastScanned);

/// <summary>A page of scan results. Next is the continuation key, or null at the end. Incomplete means the scan exhausted its budget.</summary>
internal sealed record ScanResult<T>(IReadOnlyList<T> Rows, ListKey? Next, bool Incomplete);

/// <summary>Scans in chunks within a time budget (§6.3).</summary>
internal static class ChunkedScan
{
    public const int PortionSize = 2000;

    /// <summary>
    /// Scans from the cursor until the page fills, a chunk returns fewer than <see cref="PortionSize"/> keys, or the budget expires between chunks. Requests one extra match to detect another page.
    /// </summary>
    public static async Task<ScanResult<T>> RunAsync<T>(ListKey? after, int pageSize, Func<ListKey?, int, CancellationToken, Task<Portion<T>>> next,
        Func<T, ListKey> keyOf, TimeSpan budget, TimeProvider time, CancellationToken ct)
    {
        var rows = new List<T>(pageSize);
        var cursor = after;
        var started = time.GetTimestamp();
        while (true)
        {
            var portion = await next(cursor, pageSize - rows.Count + 1, ct);
            foreach (var match in portion.Matches)
            {
                if (rows.Count == pageSize)
                    return new ScanResult<T>(rows, keyOf(rows[^1]), Incomplete: false);   // совпадение сверх страницы — данные есть
                rows.Add(match);
            }
            if (portion.Scanned < PortionSize)
                return new ScanResult<T>(rows, null, Incomplete: false);                  // данные кончились
            if (rows.Count == pageSize)
                return new ScanResult<T>(rows, keyOf(rows[^1]), Incomplete: false);
            cursor = portion.LastScanned;
            if (time.GetElapsedTime(started) >= budget)
                return new ScanResult<T>(rows, cursor, Incomplete: true);                 // «продолжить поиск» всегда продвигается
        }
    }
}
