using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Env;

/// <summary>PostgreSQL container startup command for the load harness.</summary>
internal static class PostgresCommand
{
    /// <summary>
    /// Server settings as <c>-c k=v</c> pairs. A <c>--pg-setting</c> pair overrides the default for the same key. Enable <c>track_planning</c> only
    /// when measuring planning time. Otherwise statements with the same structure on many connections compete to update one statistics row.
    /// </summary>
    public static IReadOnlyList<string> Build(LoadTestOptions options, bool trackPlanning)
    {
        var settings = new Dictionary<string, string>();
        // В штатном профиле память и WAL настраивает сам PostgreSQL.
        if (options.PgProfile == PostgresProfile.Bench)
        {
            settings["shared_buffers"] = "1GB";
            settings["max_wal_size"] = "4GB";
        }

        settings["shared_preload_libraries"] = "pg_stat_statements";
        settings["track_io_timing"] = "on";
        settings["pg_stat_statements.track_planning"] = trackPlanning ? "on" : "off";
        if (trackPlanning)
        {
            settings["pg_stat_statements.track"] = "all";
        }
        foreach (var (key, value) in options.PgSettings)
        {
            settings[key] = value;
        }

        return [.. settings.SelectMany(s => new[] { "-c", $"{s.Key}={s.Value}" })];
    }
}
