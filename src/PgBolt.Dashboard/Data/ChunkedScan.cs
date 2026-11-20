namespace PgBolt.Dashboard.Data;

/// <summary>Результат порции: подходящие строки и последний просмотренный ключ (Scanned &lt; PortionSize — данные кончились).</summary>
internal sealed record Portion<T>(IReadOnlyList<T> Matches, int Scanned, ListKey? LastScanned);

/// <summary>Страница обхода: Next — ключ продолжения (null — данных больше нет), Incomplete — исчерпан бюджет.</summary>
internal sealed record ScanResult<T>(IReadOnlyList<T> Rows, ListKey? Next, bool Incomplete);

/// <summary>Обход порциями с бюджетом (§6.3).</summary>
internal static class ChunkedScan
{
    public const int PortionSize = 2000;

    /// <summary>
    /// Порции от курсора, пока страница не заполнена, порция не вернула меньше <see cref="PortionSize"/> ключей или не исчерпан
    /// бюджет (проверяется между порциями). need порции — недостающие строки плюс одна: лишнее совпадение значит «данные есть».
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
