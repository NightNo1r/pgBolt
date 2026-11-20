using System.Diagnostics;
using System.Runtime.InteropServices;
using Npgsql;
using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Env;

/// <summary>Сведения об окружении прогона для отчёта: хост, Docker, версии, параметры PostgreSQL, коммит.</summary>
internal static class EnvironmentInfo
{
    public const string NoData = "нет данных";

    private static readonly string[] Settings =
    [
        "shared_buffers", "max_connections", "max_wal_size", "max_locks_per_transaction", "autovacuum_naptime",
        "autovacuum", "autovacuum_max_workers", "autovacuum_vacuum_cost_delay", "pg_stat_statements.track_planning",
        "shared_preload_libraries", "track_io_timing", "fsync", "full_page_writes", "synchronous_commit",
    ];

    /// <param name="console">Админ-консоль своего PgBouncer; <c>null</c> — версии PgBouncer нет.</param>
    /// <param name="schema">Схема прогона: из неё читаются reloptions <c>queue</c> и <c>completions</c>.</param>
    public static async Task<IReadOnlyList<(string Key, string Value)>> CollectAsync(string connectionString,
        PgBouncerConsole? console, LoadTestOptions options, string schema, CancellationToken ct)
    {
        var info = new List<(string, string)>
        {
            ("CPU", $"{await CpuModelAsync(ct)}, {Environment.ProcessorCount} ядер"),
            ("Docker", await RunAsync("docker", ["info", "--format", "{{.NCPU}} CPU, {{.MemTotal}} байт"], ct) ?? NoData),
        };

        await using var conn = await StandConnection.OpenAsync(connectionString, "environment", ct);
        await using (var cmd = new NpgsqlCommand("SHOW server_version", conn))
            info.Add(("PostgreSQL", (string)(await cmd.ExecuteScalarAsync(ct))!));
        info.Add(("PgBouncer", console is null ? NoData : await console.VersionAsync(ct)));

        await using (var cmd = new NpgsqlCommand("SELECT n, current_setting(n, true) FROM unnest(@names) n", conn))
        {
            cmd.Parameters.AddWithValue("names", Settings);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
                info.Add((r.GetString(0), r.IsDBNull(1) ? NoData : r.GetString(1)));
        }

        // Профиль объясняет происхождение настроек; выше записаны значения, прочитанные с сервера.
        info.Add(("--pg-profile", options.Connection is null ? options.PgProfile.ToString().ToLowerInvariant() : "external"));
        info.Add(("--pg-setting", string.Join(",", options.PgSettings.Select(p => $"{p.Key}={p.Value}"))));

        await using (var cmd = new NpgsqlCommand("""
            SELECT t, (SELECT array_to_string(c.reloptions, ',') FROM pg_class c
                       WHERE c.oid = to_regclass(quote_ident(@schema) || '.' || t))
            FROM unnest(ARRAY['queue', 'completions']) t
            """, conn))
        {
            cmd.Parameters.AddWithValue("schema", schema);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
                info.Add(($"reloptions {r.GetString(0)}", r.IsDBNull(1) ? NoData : r.GetString(1)));
        }

        info.Add((".NET", Environment.Version.ToString()));
        info.Add(("Коммит", await RunAsync("git", ["rev-parse", "--short", "HEAD"], ct) ?? NoData));
        return info;
    }

    private static async Task<string> CpuModelAsync(CancellationToken ct)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            return await RunAsync("sysctl", ["-n", "machdep.cpu.brand_string"], ct) ?? NoData;
        if (File.Exists("/proc/cpuinfo"))
        {
            var line = (await File.ReadAllLinesAsync("/proc/cpuinfo", ct))
                .FirstOrDefault(l => l.StartsWith("model name", StringComparison.Ordinal));
            if (line is not null)
                return line[(line.IndexOf(':') + 1)..].Trim();
        }
        return NoData;
    }

    /// <summary>Вывод команды без завершающих пробелов; <c>null</c> — команды нет или она завершилась с ошибкой.</summary>
    private static async Task<string?> RunAsync(string file, string[] args, CancellationToken ct)
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo(file, args)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            })!;
            var output = process.StandardOutput.ReadToEndAsync(ct);
            await process.StandardError.ReadToEndAsync(ct);
            await process.WaitForExitAsync(ct);
            return process.ExitCode == 0 ? (await output).Trim() : null;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return null;
        }
    }
}
