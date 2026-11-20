using BenchmarkDotNet.Running;
using PgBolt.Benchmarks.Verification;

// --verify [область] — только стресс-проверки инвариантов (event|sink|slots|table|deadlines; код возврата 1 при провале).
// --latency  — перцентили задержки ping-pong AsyncAutoResetEvent → results/pingpong-latency.md (или путь после флага).
// иначе      — проверки, затем BenchmarkDotNet с остальными аргументами (--filter '*' и т. п.); --skip-verify пропускает проверки.
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
