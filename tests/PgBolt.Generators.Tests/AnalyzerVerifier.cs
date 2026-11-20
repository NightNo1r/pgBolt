using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using PgBolt.Generators.Analysis;

namespace PgBolt.Generators.Tests;

internal static class AnalyzerVerifier
{
    public static readonly ReferenceAssemblies Net10 = new("net10.0",
        new PackageIdentity("Microsoft.NETCore.App.Ref", "10.0.0"), Path.Combine("ref", "net10.0")); // PackageIdentity из Microsoft.CodeAnalysis.Testing

    public static void AddPgBoltReferences(SolutionState state, bool server = true)
    {
        state.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(PgBolt.JobAttribute).Assembly.Location));
        state.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(Microsoft.Extensions.Logging.ILogger).Assembly.Location));
        if (server)
            state.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(PgBolt.Server.PgBoltServerOptions).Assembly.Location));
        state.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(PgBolt.Postgres.PgBoltOptions).Assembly.Location));
    }

    public static Task VerifyAsync(string source, params DiagnosticResult[] expected) => VerifyAsync(source, true, expected);

    public static async Task VerifyAsync(string source, bool server, params DiagnosticResult[] expected)
    {
        var test = new CSharpAnalyzerTest<PgBoltAnalyzer, DefaultVerifier>
        {
            TestCode = source,
            ReferenceAssemblies = Net10,
            CompilerDiagnostics = CompilerDiagnostics.None,
        };
        AddPgBoltReferences(test.TestState, server);
        test.ExpectedDiagnostics.AddRange(expected);
        await test.RunAsync(TestContext.Current.CancellationToken);
    }

    public static DiagnosticResult Diagnostic(DiagnosticDescriptor descriptor) => new(descriptor);
}
