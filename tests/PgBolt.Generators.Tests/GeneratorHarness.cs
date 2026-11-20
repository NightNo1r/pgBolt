using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace PgBolt.Generators.Tests;

internal static class GeneratorHarness
{
    private static readonly ImmutableArray<MetadataReference> References =
        ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))
            .ToImmutableArray();

    private static readonly CSharpParseOptions Parse = new(LanguageVersion.Latest);

    /// <summary>Сборка-«библиотека» без генератора: её [Job]-типы видны тестируемой компиляции только как referenced.</summary>
    public static MetadataReference Library(string source, string assemblyName) =>
        CSharpCompilation.Create(assemblyName, [CSharpSyntaxTree.ParseText(source, Parse)], References,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)).ToMetadataReference();

    public static (GeneratorDriver Driver, Compilation Output) Run(string source, string? rootNamespace = "Test.Jobs", MetadataReference? library = null)
    {
        var compilation = CSharpCompilation.Create("Test.Jobs",
            [CSharpSyntaxTree.ParseText(source, Parse)], library is null ? References : References.Add(library),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new JobGenerator().AsSourceGenerator()], parseOptions: Parse, optionsProvider: new OptionsProvider(rootNamespace));
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);
        return (driver, output);
    }

    /// <summary>Итоговая компиляция (исходник + сгенерированный код) без ошибок и предупреждений.</summary>
    public static void AssertClean(Compilation output) =>
        Assert.Empty(output.GetDiagnostics(TestContext.Current.CancellationToken).Where(d => d.Severity >= DiagnosticSeverity.Warning));

    public static string Generated(GeneratorDriver driver, string hintName) =>
        driver.GetRunResult().Results.Single().GeneratedSources.Single(s => s.HintName == hintName).SourceText.ToString();

    public static IReadOnlyList<string> HintNames(GeneratorDriver driver) =>
        driver.GetRunResult().Results.Single().GeneratedSources.Select(s => s.HintName).ToList();

    private sealed class OptionsProvider(string? rootNamespace) : AnalyzerConfigOptionsProvider
    {
        public override AnalyzerConfigOptions GlobalOptions { get; } = new Options(rootNamespace is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string> { ["build_property.RootNamespace"] = rootNamespace });

        public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => Options.Empty;
        public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => Options.Empty;
    }

    private sealed class Options(Dictionary<string, string> values) : AnalyzerConfigOptions
    {
        public static readonly Options Empty = new([]);

        public override bool TryGetValue(string key, [NotNullWhen(true)] out string? value) => values.TryGetValue(key, out value);
    }
}
