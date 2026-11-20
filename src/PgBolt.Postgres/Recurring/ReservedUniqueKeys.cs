namespace PgBolt.Postgres.Recurring;

/// <summary>Часть 4 §4.4: префикс pgbolt: зарезервирован для внутренних ключей (наложение recurring).</summary>
internal static class ReservedUniqueKeys
{
    public const string Prefix = "pgbolt:";

    public static string Recurring(string id) => "pgbolt:recurring:" + id;

    public static void EnsureUserKeys(string?[] keys)
    {
        foreach (var key in keys)
            if (key is not null && key.StartsWith(Prefix, StringComparison.Ordinal))
                throw new ArgumentException($"Unique key '{key}' uses the reserved prefix '{Prefix}'.");
    }
}
