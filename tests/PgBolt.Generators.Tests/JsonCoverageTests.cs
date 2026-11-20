using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using PgBolt.Generators.Analysis;
using PgBolt.Generators.CodeFixes;
using static PgBolt.Generators.Tests.AnalyzerVerifier;

namespace PgBolt.Generators.Tests;

public sealed class JsonCoverageTests
{
    private const string Head = """
        using System.Threading;
        using System.Threading.Tasks;
        using System.Text.Json.Serialization;
        using PgBolt;

        [assembly: PgBoltJsonContext(typeof(AppJson))]
        [Job("a.job")] public sealed record A(int X);
        [Job("b.job")] public sealed record B(int X);

        """;

    [Fact]
    public Task Executor_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public sealed class {|#0:BJob|} : IJob<B> { public Task ExecuteAsync(B b, JobContext c, CancellationToken t) => Task.CompletedTask; }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));

    [Fact]
    public Task Enqueue_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public static class S { public static async Task Go(IJobClient c) => await {|#0:c.EnqueueAsync(new B(1))|}; }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));

    [Fact]
    public Task Recurring_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public static class S
        {
            public static async Task Go(IRecurringJobManager m) => await {|#0:m.AddOrUpdateAsync("b", "* * * * *", new B(1))|};
            public static async Task Ok(IRecurringJobManager m) => await m.AddOrUpdateAsync("a", "* * * * *", new A(1));
        }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));

    [Fact]
    public async Task Code_fix_adds_json_serializable_attribute()
    {
        var test = new CSharpCodeFixTest<PgBoltAnalyzer, AddJsonSerializableCodeFix, DefaultVerifier>
        {
            TestCode = Head + """
                public sealed class {|#0:BJob|} : IJob<B> { public Task ExecuteAsync(B b, JobContext c, CancellationToken t) => Task.CompletedTask; }

                [JsonSerializable(typeof(A))]
                public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
                """,
            FixedCode = Head + """
                public sealed class BJob : IJob<B> { public Task ExecuteAsync(B b, JobContext c, CancellationToken t) => Task.CompletedTask; }

                [JsonSerializable(typeof(A))]
                [JsonSerializable(typeof(B))]
                public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
                """,
            ReferenceAssemblies = Net10,
            CompilerDiagnostics = CompilerDiagnostics.None,
        };
        AddPgBoltReferences(test.TestState);
        AddPgBoltReferences(test.FixedState);
        test.ExpectedDiagnostics.Add(Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"));
        await test.RunAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public Task Workflow_step_args_missing_in_context_BOLT001() => VerifyAsync(Head + """
        public static class S
        {
            public static async Task Go(IJobClient c) =>
                await {|#0:c.Workflow().Enqueue(new A(1)).ContinueWith(new B(1))|}.SubmitAsync();

            public static async Task Many(IJobClient c) =>
                await c.Workflow().StartBatch(b => {|#1:b.EnqueueMany(new[] { new B(2) })|}).SubmitAsync();
        }

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """,
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(0).WithArguments("B", "AppJson"),
        Diagnostic(Diagnostics.MissingJsonType).WithLocation(1).WithArguments("B", "AppJson"));
}
