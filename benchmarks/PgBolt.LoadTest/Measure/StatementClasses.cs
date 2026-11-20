namespace PgBolt.LoadTest.Measure;

/// <summary>
/// Класс statement'а <c>pg_stat_statements</c> по фрагменту текста SQL <c>PgBoltStore</c>. Фрагменты — только
/// идентификаторы и ключевые слова: <c>pg_stat_statements</c> заменяет параметры и константы на <c>$n</c>.
/// </summary>
internal static class StatementClasses
{
    public const string Dequeue = "dequeue";
    public const string DequeueLimited = "dequeue_limited";
    public const string Ack = "ack";
    public const string Outcome = "outcome";
    public const string Heartbeat = "heartbeat";
    public const string Enqueue = "enqueue";
    public const string Promoter = "promoter";

    /// <summary>Классы, которые профиль нагрузки выполняет в любом окне замера.</summary>
    public static readonly IReadOnlyList<string> Required = [Dequeue, Ack, Outcome, Heartbeat, Enqueue, Promoter];

    private static readonly (string Class, string Fragment)[] Fragments =
    [
        (Dequeue, "FROM c WHERE q.job_id = c.job_id"),
        (DequeueLimited, "AS room"),
        (Ack, "AS acked"),
        (Outcome, "AS signals FROM ev"),                      // повтор и окончательный отказ
        (Heartbeat, "AS server_present"),
        (Enqueue, "AS existed"),                              // все варианты вставки задач
        (Promoter, "FROM p WHERE q.job_id = p.job_id"),
    ];

    /// <summary>Класс запроса; <c>null</c> — запрос не PgBolt или подходит сразу к нескольким классам.</summary>
    public static string? Classify(string query)
    {
        var matches = Fragments.Where(f => query.Contains(f.Fragment, StringComparison.Ordinal)).Take(2).ToList();
        return matches.Count == 1 ? matches[0].Class : null;
    }
}
