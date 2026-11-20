using static VerifyXunit.Verifier;

namespace PgBolt.Generators.Tests;

public sealed class MetadataGenerationTests
{
    /// <summary>Рукописный контекст вместо STJ-генератора: сгенерированный код должен компилироваться без ошибок.</summary>
    internal const string HandWrittenContext = """
        public sealed class AppJson : System.Text.Json.Serialization.JsonSerializerContext
        {
            public AppJson() : base(null) { }
            public static AppJson Default { get; } = new();
            protected override System.Text.Json.JsonSerializerOptions? GeneratedSerializerOptions => null;
            public override System.Text.Json.Serialization.Metadata.JsonTypeInfo? GetTypeInfo(System.Type type) => null;
        }
        """;

    [Fact]
    public Task Contracts_only_assembly_registers_descriptors_without_type_info()
    {
        var (driver, output) = GeneratorHarness.Run("""
            namespace Orders.Contracts;

            [PgBolt.Job("orders.send-receipt", Queue = "emails", MaxAttempts = 5, Priority = -1, RetryDelaysSeconds = new[] { 10, 60 })]
            public sealed record SendReceiptArgs(long OrderId, string Email);
            """);

        Assert.Equal(new[] { "PgBolt.Metadata.g.cs" }, GeneratorHarness.HintNames(driver));
        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.SendReceiptArgs>(new global::PgBolt.JobDescriptor("orders.send-receipt", "emails", 5, (short)-1, new int[] { 10, 60 }), null);""", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public Task Enqueue_call_site_registers_type_info_from_context()
    {
        var (driver, output) = GeneratorHarness.Run($$"""
            using System.Threading.Tasks;
            using PgBolt;

            [assembly: PgBoltJsonContext(typeof(AppJson))]

            [Job("app.ping")]
            public sealed record PingArgs(string Message);

            public static class Sender
            {
                public static async Task SendAsync(IJobClient client) => await client.EnqueueAsync(new PingArgs("hi"));
            }

            [System.Text.Json.Serialization.JsonSerializable(typeof(PingArgs))]
            {{HandWrittenContext}}
            """);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("(global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::PingArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::PingArgs))!", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public void Call_sites_register_types_from_referenced_assemblies()
    {
        var contracts = GeneratorHarness.Library("""
            namespace Contracts;

            [PgBolt.Job("a.job")] public sealed record AArgs(int X);
            [PgBolt.Job("b.job")] public sealed record BArgs(int X);
            [PgBolt.Job("c.job")] public sealed record CArgs(int X);
            """, "Contracts");

        var (driver, output) = GeneratorHarness.Run("""
            using System.Threading.Tasks;
            using Contracts;
            using PgBolt;

            public static class Sender
            {
                public static async Task SendAsync(IJobClient client, IJobClient? maybe)
                {
                    await client.EnqueueAsync(new AArgs(1));
                    if (maybe?.EnqueueAsync(new BArgs(1)) is { } pending) // x?.EnqueueAsync — MemberBindingExpressionSyntax
                        await pending;
                }
            }
            """, library: contracts);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("Register<global::Contracts.AArgs>", code);
        Assert.Contains("Register<global::Contracts.BArgs>", code);
        Assert.DoesNotContain("CArgs", code);
        GeneratorHarness.AssertClean(output);
    }

    [Fact]
    public void Type_parameter_call_sites_are_ignored()
    {
        var (driver, _) = GeneratorHarness.Run("""
            using System.Threading.Tasks;
            using PgBolt;

            public static class Sender
            {
                public static async Task SendAsync<T>(IJobClient client, T args) => await client.EnqueueAsync(args);
            }
            """);

        Assert.Empty(GeneratorHarness.HintNames(driver));
    }

    [Fact]
    public Task Timeout_is_emitted_only_when_positive()
    {
        var (driver, output) = GeneratorHarness.Run("""
            namespace Orders.Contracts;

            [PgBolt.Job("orders.long", TimeoutSeconds = 300)]
            public sealed record LongArgs(int X);

            [PgBolt.Job("orders.short", TimeoutSeconds = 0)]
            public sealed record ShortArgs(int X);
            """);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.LongArgs>(new global::PgBolt.JobDescriptor("orders.long", "default", 10, (short)0, null) { TimeoutSeconds = 300 }, null);""", code);
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Orders.Contracts.ShortArgs>(new global::PgBolt.JobDescriptor("orders.short", "default", 10, (short)0, null), null);""", code);
        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    [Fact]
    public void Typed_recurring_call_sites_register_types_from_referenced_assemblies()   // часть 4: продюсер без исполнителя
    {
        var contracts = GeneratorHarness.Library("""
            namespace Contracts;

            [PgBolt.Job("a.job")] public sealed record AArgs(int X);
            [PgBolt.Job("b.job")] public sealed record BArgs(int X);
            [PgBolt.Job("c.job")] public sealed record CArgs(int X);
            [PgBolt.Job("d.job")] public sealed record DArgs(int X);
            """, "Contracts");

        var (driver, output) = GeneratorHarness.Run($$"""
            using System.Text.Json;
            using System.Threading.Tasks;
            using Contracts;
            using PgBolt;

            [assembly: PgBoltJsonContext(typeof(AppJson))]

            public static class Producer
            {
                public static void Configure(PgBolt.Postgres.PgBoltOptions o) =>
                    o.AddRecurring("a", "0 3 * * *", new AArgs(1))
                     .AddRecurring("d", "d.job", "* * * * *", default(JsonElement));        // нетипизированная — метаданные не нужны

                public static async Task ScheduleAsync(IRecurringJobManager manager, JsonElement raw)
                {
                    await manager.AddOrUpdateAsync("b", "*/5 * * * *", new BArgs(1));
                    await manager.AddOrUpdateAsync("c", "c.job", "* * * * *", raw);            // нетипизированная — метаданные не нужны
                }
            }

            [System.Text.Json.Serialization.JsonSerializable(typeof(AArgs))]
            [System.Text.Json.Serialization.JsonSerializable(typeof(BArgs))]
            {{HandWrittenContext}}
            """, library: contracts);

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Contracts.AArgs>(new global::PgBolt.JobDescriptor("a.job", "default", 10, (short)0, null), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::Contracts.AArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::Contracts.AArgs))!);""", code);
        Assert.Contains("""global::PgBolt.JobMetadataRegistry.Register<global::Contracts.BArgs>(new global::PgBolt.JobDescriptor("b.job", "default", 10, (short)0, null), (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::Contracts.BArgs>)global::AppJson.Default.GetTypeInfo(typeof(global::Contracts.BArgs))!);""", code);
        Assert.DoesNotContain("CArgs", code);
        Assert.DoesNotContain("DArgs", code);
        GeneratorHarness.AssertClean(output);
    }

    [Fact]
    public void Type_parameter_recurring_call_sites_are_ignored()
    {
        var (driver, _) = GeneratorHarness.Run($$"""
            using System.Threading.Tasks;
            using PgBolt;

            public static class Producer
            {
                public static ValueTask ScheduleAsync<T>(IRecurringJobManager manager, T args) => manager.AddOrUpdateAsync("x", "* * * * *", args);
                public static void Configure<T>(PgBolt.Postgres.PgBoltOptions o, T args) => o.AddRecurring("y", "* * * * *", args);
            }
            """);

        Assert.Empty(GeneratorHarness.HintNames(driver));
    }

    /// <summary>Типы шагов из referenced-сборки: регистрируются только по местам типизированных вызовов workflow (часть 5 §5.4).</summary>
    private const string WorkflowContracts = """
        namespace Contracts;

        [PgBolt.Job("flow.fetch")] public sealed record FetchArgs(int Id);
        [PgBolt.Job("flow.parse")] public sealed record ParseArgs(int Id);
        [PgBolt.Job("flow.item")] public sealed record ItemArgs(int Id);
        [PgBolt.Job("flow.raw")] public sealed record RawArgs(int Id);
        """;

    private const string WorkflowProducer = """
        using System.Text.Json;
        using System.Threading.Tasks;
        using Contracts;
        using PgBolt;

        [assembly: PgBoltJsonContext(typeof(AppJson))]

        public static class Producer
        {
            public static async Task RunAsync(IJobClient client, JsonElement raw) =>
                await client.Workflow()
                    .Enqueue(new FetchArgs(1))
                    .ContinueWith(new ParseArgs(1))
                    .ContinueWithBatch(b => b.EnqueueMany(new[] { new ItemArgs(1), new ItemArgs(2) }))
                    .ContinueWith("flow.raw", raw)                 // нетипизированный шаг — метаданные не нужны
                    .SubmitAsync();
        }

        [System.Text.Json.Serialization.JsonSerializable(typeof(FetchArgs))]
        [System.Text.Json.Serialization.JsonSerializable(typeof(ParseArgs))]
        [System.Text.Json.Serialization.JsonSerializable(typeof(ItemArgs))]
        """;

    [Fact]
    public void Workflow_call_sites_register_types_from_referenced_assemblies()
    {
        var (driver, output) = GeneratorHarness.Run(WorkflowProducer + HandWrittenContext,
            library: GeneratorHarness.Library(WorkflowContracts, "Contracts"));

        var code = GeneratorHarness.Generated(driver, "PgBolt.Metadata.g.cs");
        Assert.Contains("Register<global::Contracts.FetchArgs>", code);
        Assert.Contains("Register<global::Contracts.ParseArgs>", code);
        Assert.Contains("Register<global::Contracts.ItemArgs>", code);
        Assert.DoesNotContain("RawArgs", code);
        GeneratorHarness.AssertClean(output);
    }

    [Fact]
    public Task Workflow_call_sites_register_type_info_from_context()   // §12: snapshot метаданных Enqueue / ContinueWith / EnqueueMany
    {
        var (driver, output) = GeneratorHarness.Run(WorkflowProducer + HandWrittenContext,
            library: GeneratorHarness.Library(WorkflowContracts, "Contracts"));

        GeneratorHarness.AssertClean(output);
        return Verify(driver);
    }

    /// <summary>
    /// Чужие generic-методы с именами шагов workflow, вызванные с [Job]-типом: распознавание идёт по объявляющему интерфейсу PgBolt,
    /// а не по имени, поэтому метаданные не регистрируются (при сломанном распознавании FetchArgs попал бы в PgBolt.Metadata.g.cs).
    /// </summary>
    internal const string UnrelatedWorkflowLookalikes = """
        public sealed class Lookalike
        {
            public Lookalike Enqueue<T>(T args) => this;
            public Lookalike ContinueWith<T>(T args) => this;
            public Lookalike EnqueueMany<T>(System.Collections.Generic.IReadOnlyList<T> args) => this;
        }
        """;

    [Fact]
    public void Unrelated_generic_Enqueue_and_ContinueWith_are_ignored()
    {
        var (driver, output) = GeneratorHarness.Run($$"""
            using System.Collections.Generic;
            using System.Threading.Tasks;
            using Contracts;
            using PgBolt;

            [assembly: PgBoltJsonContext(typeof(AppJson))]

            public static class Other
            {
                public static void Go(Queue<FetchArgs> q, Task t, Lookalike l)
                {
                    q.Enqueue(new FetchArgs(1));
                    t.ContinueWith<FetchArgs>(_ => new FetchArgs(1));
                    l.Enqueue(new FetchArgs(1)).ContinueWith(new FetchArgs(2)).EnqueueMany(new[] { new FetchArgs(3) });
                }
            }

            {{UnrelatedWorkflowLookalikes}}

            [System.Text.Json.Serialization.JsonSerializable(typeof(FetchArgs))]
            {{HandWrittenContext}}
            """, library: GeneratorHarness.Library(WorkflowContracts, "Contracts"));

        Assert.Empty(GeneratorHarness.HintNames(driver));
        GeneratorHarness.AssertClean(output);
    }
}
