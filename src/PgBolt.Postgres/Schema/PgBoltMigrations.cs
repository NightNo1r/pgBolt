using System.Text;
using System.Text.RegularExpressions;

namespace PgBolt.Postgres.Schema;

public sealed record Migration(int Version, string Name, string Sql);

public static partial class PgBoltMigrations
{
    internal const string LockKeySql = "SELECT pg_advisory_xact_lock(hashtext(@lock_key))";

    internal const string BootstrapSql = """
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.schema_version (version int PRIMARY KEY, applied_at timestamptz NOT NULL);
        """;

    [GeneratedRegex(@"\.V(\d{3})__([a-z0-9_]+)\.sql$")]
    private static partial Regex ResourceName();

    public static IReadOnlyList<Migration> All { get; } = Load();

    public static int LatestVersion => All[^1].Version;

    /// <summary>True if the schema is missing (null) or older than the code (§4). A newer schema does not need an upgrade.</summary>
    internal static bool IsOutdated(int? version) => version is null || version < LatestVersion;

    /// <summary>Builds a script for manual application (AutoMigrate = false) containing migrations newer than fromVersion.</summary>
    public static string GetScript(string schema, int fromVersion = 0)
    {
        SchemaName.Validate(schema);
        var sb = new StringBuilder();
        sb.Append("BEGIN;\n");
        sb.Append($"SELECT pg_advisory_xact_lock(hashtext('{schema}'));\n");
        sb.Append(Render(BootstrapSql, schema)).Append('\n');
        foreach (var m in All.Where(m => m.Version > fromVersion))
        {
            sb.Append($"-- V{m.Version:D3} {m.Name}\n");
            sb.Append(Render(m.Sql, schema)).Append('\n');
            sb.Append($"INSERT INTO {schema}.schema_version (version, applied_at) VALUES ({m.Version}, now());\n");
        }
        sb.Append("COMMIT;\n");
        return sb.ToString();
    }

    internal static string Render(string sql, string schema) => sql.Replace("{schema}", schema, StringComparison.Ordinal);

    private static List<Migration> Load()
    {
        var assembly = typeof(PgBoltMigrations).Assembly;
        var list = new List<Migration>();
        foreach (var name in assembly.GetManifestResourceNames())
        {
            var match = ResourceName().Match(name);
            if (!match.Success)
                continue;
            using var stream = assembly.GetManifestResourceStream(name)!;
            using var reader = new StreamReader(stream);
            list.Add(new Migration(int.Parse(match.Groups[1].Value), match.Groups[2].Value, reader.ReadToEnd()));
        }
        list.Sort((a, b) => a.Version.CompareTo(b.Version));
        if (list.Count == 0 || list.Select((m, i) => m.Version != i + 1).Any(x => x))
            throw new InvalidOperationException("PgBolt migrations must be numbered V001, V002, ... without gaps.");
        return list;
    }
}
