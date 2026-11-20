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
        var settings = new Dictionary<string, string>
        {
            ["shared_buffers"] = "1GB",
            ["max_wal_size"] = "4GB",
            ["shared_preload_libraries"] = "pg_stat_statements",
            ["track_io_timing"] = "on",
            ["pg_stat_statements.track_planning"] = trackPlanning ? "on" : "off",
        };
        foreach (var (key, value) in options.PgSettings)
            settings[key] = value;

        return [.. settings.SelectMany(s => new[] { "-c", $"{s.Key}={s.Value}" })];
    }
}
