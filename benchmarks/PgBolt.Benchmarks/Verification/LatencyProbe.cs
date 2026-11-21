using System.Diagnostics;
using System.Globalization;
using System.Text;
using PgBolt.Benchmarks.Copies;

namespace PgBolt.Benchmarks.Verification;

/// <summary>
/// Computes ping-pong latency percentiles from individual round trips because BenchmarkDotNet reports statistics over iteration means.
/// Alternates variants across three runs of 200,000 trips each after 20,000 warmup trips. Outputs a Markdown table.
/// </summary>
internal static class LatencyProbe
{
    private const int Warmup = 20_000, RoundTrips = 200_000, Runs = 3;
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    public static void Run(string outputPath)
    {
        var rows = new List<string>();
        for (var run = 1; run <= Runs; run++)
        {
            rows.Add(Format("Lock (aaf1738)", run, Measure(new LockAsyncAutoResetEvent(), new LockAsyncAutoResetEvent(),
                static (e, t) => e.WaitAsync(t, TimeProvider.System, CancellationToken.None), static e => e.Set())));
            rows.Add(Format("CAS (52b4a8f)", run, Measure(new CasAsyncAutoResetEvent(), new CasAsyncAutoResetEvent(),
                static (e, t) => e.WaitAsync(t, TimeProvider.System, CancellationToken.None), static e => e.Set())));
        }
        var md = new StringBuilder()
            .AppendLine("# AsyncAutoResetEvent ping-pong round-trip latency (Set → wake → Set → wake)")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"{Environment.ProcessorCount} logical cores, {RuntimeInfo()}. Each variant ran {Runs} times for {RoundTrips:N0} round trips after {Warmup:N0} warmup round trips. The variants alternate. Times are in µs.")
            .AppendLine()
            .AppendLine("| Variant | Run | Mean | p50 | p90 | p99 | p99.9 | Max |")
            .AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var row in rows)
            md.AppendLine(row);
        File.WriteAllText(outputPath, md.ToString());
        Console.WriteLine(md.ToString());
        Console.WriteLine($"→ {outputPath}");
    }

    private static string RuntimeInfo() => $"{System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}, {System.Runtime.InteropServices.RuntimeInformation.OSDescription}";

    private static double[] Measure<T>(T ping, T pong, Func<T, TimeSpan, Task<bool>> wait, Action<T> set)
    {
        var stop = false;
        var echo = Task.Run(async () =>
        {
            while (!Volatile.Read(ref stop))
                if (await wait(ping, WaitTimeout))
                    set(pong);
        });
        var samples = new long[RoundTrips];
        for (var i = 0; i < Warmup + RoundTrips; i++)
        {
            var start = Stopwatch.GetTimestamp();
            set(ping);
            if (!wait(pong, WaitTimeout).GetAwaiter().GetResult())
                throw new InvalidOperationException("ping-pong: lost wakeup");
            if (i >= Warmup)
                samples[i - Warmup] = Stopwatch.GetTimestamp() - start;
        }
        Volatile.Write(ref stop, true);
        set(ping);
        echo.GetAwaiter().GetResult();
        var micros = samples.Select(t => t * 1_000_000.0 / Stopwatch.Frequency).ToArray();
        Array.Sort(micros);
        return micros;
    }

    private static string Format(string variant, int run, double[] sorted)
    {
        static double P(double[] s, double p) => s[Math.Min(s.Length - 1, (int)Math.Ceiling(p * s.Length) - 1)];
        return string.Create(CultureInfo.InvariantCulture,
            $"| {variant} | {run} | {sorted.Average():F2} | {P(sorted, 0.50):F2} | {P(sorted, 0.90):F2} | {P(sorted, 0.99):F2} | {P(sorted, 0.999):F2} | {sorted[^1]:F1} |");
    }
}
