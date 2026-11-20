using static VerifyXunit.Verifier;

namespace PgBolt.Generators.Tests;

public sealed class RegistryGenerationTests
{
    private const string Source = """
        using System.Threading;
        using System.Threading.Tasks;
        using PgBolt;

        [assembly: PgBoltJsonContext(typeof(Test.Jobs.AppJson))]

        namespace Test.Jobs;

        [Job("a.job")] public sealed record AArgs(int X);
        [Job("b.job", Queue = "b")] public sealed record BArgs(int X);

        public sealed class AJob : IJob<AArgs>
        {
            public Task ExecuteAsync(AArgs args, JobContext context, CancellationToken ct) => Task.CompletedTask;
        }

        public sealed record BJob : IJob<BArgs> // record class — тоже исполнитель (RecordDeclarationSyntax)
        {
            public Task ExecuteAsync(BArgs args, JobContext context, CancellationToken ct) => Task.CompletedTask;
        }

        public abstract class AbstractJob : IJob<AArgs>
        {
            public abstract Task ExecuteAsync(AArgs args, JobContext context, CancellationToken ct);
        }

        [System.Text.Json.Serialization.JsonSerializable(typeof(AArgs))]
        [System.Text.Json.Serialization.JsonSerializable(typeof(BArgs))]
        public sealed class AppJson : System.Text.Json.Serialization.JsonSerializerContext
        {
            public AppJson() : base(null) { }
            public static AppJson Default { get; } = new();
            protected override System.Text.Json.JsonSerializerOptions? GeneratedSerializerOptions => null;
            public override System.Text.Json.Serialization.Metadata.JsonTypeInfo? GetTypeInfo(System.Type type) => null;
        }
        """;

    [Fact]
    public Task Registry_is_generated_in_root_namespace_and_compiles()
    {
        var (driver, output) = GeneratorHarness.Run(Source, rootNamespace: "Test.Jobs");

        var code = GeneratorHarness.Generated(driver, "PgBolt.JobRegistry.g.cs");
        Assert.Contains("namespace Test.Jobs", code);
        Assert.Contains("public static class PgBoltJobRegistry", code);
        Assert.Contains("builder.AddExecutor<global::Test.Jobs.AArgs, global::Test.Jobs.AJob>(", code);
        Assert.Contains("builder.AddExecutor<global::Test.Jobs.BArgs, global::Test.Jobs.BJob>(", code);
        Assert.DoesNotContain("AbstractJob", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public void Missing_root_namespace_falls_back_to_assembly_name()
    {
        var (driver, _) = GeneratorHarness.Run(Source, rootNamespace: null);
        Assert.Contains("namespace Test.Jobs", GeneratorHarness.Generated(driver, "PgBolt.JobRegistry.g.cs"));
    }

    [Fact]
    public void No_executors_no_registry()
    {
        var (driver, _) = GeneratorHarness.Run("""
            [PgBolt.Job("only.contract")] public sealed record OnlyArgs(int X);
            """);
        Assert.DoesNotContain("PgBolt.JobRegistry.g.cs", GeneratorHarness.HintNames(driver));
    }

    [Fact]
    public void Registry_descriptor_carries_timeout()
    {
        var (driver, output) = GeneratorHarness.Run($$"""
            using System.Threading;
            using System.Threading.Tasks;
            using PgBolt;

            [assembly: PgBoltJsonContext(typeof(AppJson))]

            [Job("slow.job", TimeoutSeconds = 45)]
            public sealed record SlowArgs(int X);

            public sealed class SlowJob : IJob<SlowArgs>
            {
                public Task ExecuteAsync(SlowArgs args, JobContext context, CancellationToken ct) => Task.CompletedTask;
            }

            [System.Text.Json.Serialization.JsonSerializable(typeof(SlowArgs))]
            {{MetadataGenerationTests.HandWrittenContext}}
            """);

        var registry = GeneratorHarness.Generated(driver, "PgBolt.JobRegistry.g.cs");
        Assert.Contains("""builder.AddExecutor<global::SlowArgs, global::SlowJob>(new global::PgBolt.JobDescriptor("slow.job", "default", 10, (short)0, null) { TimeoutSeconds = 45 }, """, registry);
        GeneratorHarness.AssertClean(output);
    }
}
