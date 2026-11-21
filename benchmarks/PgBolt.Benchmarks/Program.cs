using BenchmarkDotNet.Running;
using PgBolt.Benchmarks.Verification;

// --verify [group] runs only invariant checks (event|sink|slots|table|deadlines). Failure returns exit code 1.
// --latency [path] writes AsyncAutoResetEvent ping-pong round-trip percentiles to the specified or default path.
// Otherwise, run the checks followed by BenchmarkDotNet with the remaining arguments. --skip-verify skips the checks.
if (Array.IndexOf(args, "--verify") is var verify and >= 0)
    return Verifier.Run(verify + 1 < args.Length ? args[verify + 1] : null) ? 0 : 1;

if (Array.IndexOf(args, "--latency") is var latency and >= 0)
{
    var output = latency + 1 < args.Length
        ? args[latency + 1]
        : Path.Combine(AppContext.BaseDirectory, "pingpong-latency.md");
    LatencyProbe.Run(output);
    return 0;
}

if (!args.Contains("--skip-verify") && !Verifier.Run())
{
    Console.Error.WriteLine("verify failed: benchmarks are not run.");
    return 1;
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run([.. args.Where(a => a != "--skip-verify")]);
return 0;
