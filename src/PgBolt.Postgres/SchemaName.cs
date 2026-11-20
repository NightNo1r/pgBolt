using System.Text.RegularExpressions;

namespace PgBolt.Postgres;

internal static partial class SchemaName
{
    [GeneratedRegex("^[a-z_][a-z0-9_]{0,62}$", RegexOptions.CultureInvariant)]
    private static partial Regex Pattern();

    public static string Validate(string schema) =>
        schema is not null && Pattern().IsMatch(schema)
            ? schema
            : throw new ArgumentException($"Invalid PgBolt schema name '{schema}'. Allowed: ^[a-z_][a-z0-9_]{{0,62}}$.", nameof(schema));
}
