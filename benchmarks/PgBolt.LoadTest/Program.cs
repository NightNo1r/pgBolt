using System.Diagnostics;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Scenarios;

namespace PgBolt.LoadTest;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };
        return await RunAsync(args, cts.Token);
    }

    /// <summary>Returns the scenario exit code, or 2 for invalid arguments, harness exceptions, or cancellation.</summary>
    public static async Task<int> RunAsync(string[] args, CancellationToken ct)
    {
        var parsed = OptionsParser.Parse(args);
        if (parsed.Error is not null)
        {
            Console.Error.WriteLine(parsed.Error);
            return 2;
        }

        var options = parsed.Options!;
        try
        {
            IScenario scenario = options.Scenario switch
            {
                Scenario.Sustained => new Sustained(),
                Scenario.Ramp => new Ramp(),
                Scenario.Partitions => new Partitions(),
                Scenario.Vacuum => new Vacuum(),
                _ => throw new UnreachableException(),
            };
            return await scenario.RunAsync(options, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            Console.Error.WriteLine("Run aborted.");
            return 2;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Load harness failed: {e}");
            return 2;
        }
    }
}
