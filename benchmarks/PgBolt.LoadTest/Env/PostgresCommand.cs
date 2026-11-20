using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Env;

/// <summary>Команда запуска контейнера PostgreSQL стенда.</summary>
internal static class PostgresCommand
{
    /// <summary>
    /// Параметры сервера в виде <c>-c k=v</c>. Пара из <c>--pg-setting</c> с тем же ключом заменяет
    /// значение по умолчанию. <c>track_planning</c> нужен только там, где читается время планирования:
    /// иначе statement'ы одной структуры из многих соединений конкурируют за запись своей строки статистики.
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
        foreach (var (key, value) in options.PgSettings)
        {
            settings[key] = value;
        }

        return [.. settings.SelectMany(s => new[] { "-c", $"{s.Key}={s.Value}" })];
    }
}
