using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Report;

namespace PgBolt.LoadTest.Scenarios;

/// <summary>Harness scenario returning exit code 0, 1, or 2. Harness failures propagate as exceptions.</summary>
internal interface IScenario
{
    Task<int> RunAsync(LoadTestOptions options, CancellationToken ct);

    /// <summary>Runs modes sequentially in one environment without <c>track_planning</c>. Returns the highest exit code across modes.</summary>
    static async Task<int> RunModesAsync(LoadTestOptions options, Func<LoadEnvironment, LoadMode, Task<int>> runMode,
        CancellationToken ct)
    {
        await using var env = await LoadEnvironment.StartAsync(options, trackPlanning: false, ct);
        var code = 0;
        foreach (var mode in options.Modes)
            code = Math.Max(code, await runMode(env, mode));
        return code;
    }

    /// <summary>Writes a run report and prints its path.</summary>
    static async Task ReportAsync(string outDir, ReportModel model) =>
        Console.WriteLine($"Report: {await ReportWriter.WriteAsync(outDir, model)}");
}
