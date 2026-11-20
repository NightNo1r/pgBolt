using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Report;

namespace PgBolt.LoadTest.Scenarios;

/// <summary>Сценарий стенда; код возврата — 0, 1 или 2. Сбой стенда — исключением.</summary>
internal interface IScenario
{
    Task<int> RunAsync(LoadTestOptions options, CancellationToken ct);

    /// <summary>Режимы по очереди в одном окружении без <c>track_planning</c>; код — наибольший по режимам.</summary>
    static async Task<int> RunModesAsync(LoadTestOptions options, Func<LoadEnvironment, LoadMode, Task<int>> runMode,
        CancellationToken ct)
    {
        await using var env = await LoadEnvironment.StartAsync(options, trackPlanning: false, ct);
        var code = 0;
        foreach (var mode in options.Modes)
            code = Math.Max(code, await runMode(env, mode));
        return code;
    }

    /// <summary>Пишет отчёт прогона и печатает его путь.</summary>
    static async Task ReportAsync(string outDir, ReportModel model) =>
        Console.WriteLine($"Отчёт: {await ReportWriter.WriteAsync(outDir, model)}");
}
