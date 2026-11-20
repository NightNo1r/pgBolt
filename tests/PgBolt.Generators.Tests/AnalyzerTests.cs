using PgBolt.Generators.Analysis;
using static PgBolt.Generators.Tests.AnalyzerVerifier;

namespace PgBolt.Generators.Tests;

public sealed class AnalyzerTests
{
    private const string Usings = """
        using System.Threading;
        using System.Threading.Tasks;
        using System.Text.Json.Serialization;
        using PgBolt;

        """;

    private const string Context = """

        [JsonSerializable(typeof(A))]
        public abstract partial class AppJson : JsonSerializerContext { protected AppJson() : base(null) { } }
        """;

    [Fact]
    public Task Valid_assembly_has_no_diagnostics() => VerifyAsync(Usings + """
        [assembly: PgBoltJsonContext(typeof(AppJson))]
        [Job("a.job")] public sealed record A(int X);
        public sealed class AJob : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """ + Context);

    [Fact]
    public Task Duplicate_names_BOLT002() => VerifyAsync(Usings + """
        [Job("dup.job")] public sealed record {|#0:A|}(int X);
        [Job("dup.job")] public sealed record {|#1:B|}(int X);
        """,
        Diagnostic(Diagnostics.DuplicateName).WithLocation(0).WithArguments("dup.job"),
        Diagnostic(Diagnostics.DuplicateName).WithLocation(1).WithArguments("dup.job"));

    [Fact]
    public Task Executor_of_non_job_args_BOLT003() => VerifyAsync(Usings + """
        [assembly: PgBoltJsonContext(typeof(AppJson))]
        public sealed record A(int X);
        public sealed class {|#0:AJob|} : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """ + Context,
        Diagnostic(Diagnostics.ArgsNotJob).WithLocation(0).WithArguments("AJob", "A"));

    [Fact]
    public Task Two_executors_for_same_args_BOLT004() => VerifyAsync(Usings + """
        [assembly: PgBoltJsonContext(typeof(AppJson))]
        [Job("a.job")] public sealed record A(int X);
        public sealed class {|#0:AJob|} : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        public sealed class {|#1:AJob2|} : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """ + Context,
        Diagnostic(Diagnostics.MultipleExecutors).WithLocation(0).WithArguments("A"),
        Diagnostic(Diagnostics.MultipleExecutors).WithLocation(1).WithArguments("A"));

    [Theory]
    [InlineData("Bad Name")]
    [InlineData("UPPER")]
    [InlineData("")]
    public Task Invalid_name_BOLT005(string name) => VerifyAsync(Usings + $$"""
        [{|#0:Job("{{name}}")|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.InvalidName).WithLocation(0).WithArguments(name));

    [Fact]
    public Task Too_long_name_BOLT005()
    {
        var name = new string('a', 129);
        return VerifyAsync(Usings + $$"""
            [{|#0:Job("{{name}}")|}] public sealed record A(int X);
            """,
            Diagnostic(Diagnostics.InvalidName).WithLocation(0).WithArguments(name));
    }

    [Fact]
    public Task Executor_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public sealed class AJob : IJob<A> { public Task ExecuteAsync(A a, JobContext c, CancellationToken t) => Task.CompletedTask; }
        """,
        Diagnostic(Diagnostics.MissingJsonContext));

    [Fact]
    public Task Enqueue_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public static class S { public static async Task Go(IJobClient c) => await c.EnqueueAsync(new A(1)); }
        """,
        Diagnostic(Diagnostics.MissingJsonContext));

    [Fact]
    public Task Code_first_recurring_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public static class S { public static void Go(PgBolt.Postgres.PgBoltOptions o) => o.AddRecurring("a", "* * * * *", new A(1)); }
        """,
        Diagnostic(Diagnostics.MissingJsonContext));

    [Fact]
    public Task Contracts_only_assembly_needs_no_context() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        """);

    [Theory]
    [InlineData("-1")]
    [InlineData("-3600")]
    public Task Negative_timeout_BOLT007(string timeout) => VerifyAsync(Usings + $$"""
        [{|#0:Job("a.job", TimeoutSeconds = {{timeout}})|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.InvalidTimeout).WithLocation(0).WithArguments(timeout));

    [Fact]
    public Task Zero_and_positive_timeout_have_no_diagnostics() => VerifyAsync(Usings + """
        [Job("a.job", TimeoutSeconds = 0)] public sealed record A(int X);
        [Job("b.job", TimeoutSeconds = 300)] public sealed record B(int X);
        [Job("c.job", TimeoutSeconds = 4294967)] public sealed record C(int X);
        """);

    [Theory]
    [InlineData("4294968")]
    [InlineData("2147483647")]
    public Task Timeout_above_runtime_limit_BOLT008(string timeout) => VerifyAsync(Usings + $$"""
        [{|#0:Job("a.job", TimeoutSeconds = {{timeout}})|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.TimeoutAboveLimit).WithLocation(0).WithArguments(timeout));

    [Fact]
    public Task Invalid_name_and_negative_timeout_are_both_reported() => VerifyAsync(Usings + """
        [{|#0:Job("Bad Name", TimeoutSeconds = -5)|}] public sealed record A(int X);
        """,
        Diagnostic(Diagnostics.InvalidName).WithLocation(0).WithArguments("Bad Name"),
        Diagnostic(Diagnostics.InvalidTimeout).WithLocation(0).WithArguments("-5"));

    private const string Workflows = """
        using System.Text.Json;
        using System.Threading.Tasks;
        using PgBolt;

        """;

    [Fact]
    public Task Workflow_typed_step_without_context_BOLT006() => VerifyAsync(Usings + """
        [Job("a.job")] public sealed record A(int X);
        public static class S { public static async Task Go(IJobClient c) => await c.Workflow().Enqueue(new A(1)).SubmitAsync(); }
        """,
        Diagnostic(Diagnostics.MissingJsonContext));

    [Fact]
    public Task Unrelated_generic_workflow_lookalikes_are_not_job_usages() => VerifyAsync(Usings + """
        using System.Collections.Generic;

        [Job("a.job")] public sealed record A(int X);
        public static class S
        {
            public static void Go(Queue<A> q, Task t, Lookalike l)
            {
                q.Enqueue(new A(1));
                t.ContinueWith<A>(_ => new A(1));
                l.Enqueue(new A(1)).ContinueWith(new A(2)).EnqueueMany(new[] { new A(3) });
            }
        }
        """ + MetadataGenerationTests.UnrelatedWorkflowLookalikes);

    [Fact]
    public Task Workflow_in_local_without_submit_BOLT009() => VerifyAsync(Workflows + """
        public static class S
        {
            public static void Go(IJobClient c, JsonElement a)
            {
                var wf = {|#0:c.Workflow()|};
                var fetch = wf.Enqueue("a.job", a);
                fetch.ContinueWith("b.job", a);
            }
        }
        """,
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(0));

    [Fact]
    public Task Workflow_chain_without_submit_BOLT009() => VerifyAsync(Workflows + """
        public static class S
        {
            public static void Go(IJobClient c, JsonElement a) => {|#0:c.Workflow()|}.Enqueue("a.job", a).ContinueWith("b.job", a);
        }
        """,
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(0));

    [Fact]
    public Task Workflow_used_only_inside_the_method_BOLT009() => VerifyAsync(Workflows + """
        public static class S
        {
            public static void IdOnly(IJobClient c, JsonElement a)
            {
                var s = {|#0:c.Workflow()|}.Enqueue("a.job", a);
                System.Console.WriteLine(s.Id);
            }

            public static System.Action VoidLambda(IJobClient c, JsonElement a) => () =>
            {
                var wf = {|#1:c.Workflow()|};
                wf.Enqueue("a.job", a);
            };

            public static System.Action VoidExpressionLambda(IJobClient c, JsonElement a) =>
                () => {|#5:c.Workflow()|}.Enqueue("a.job", a);

            public static async Task TwoWorkflows(IJobClient c, JsonElement a)
            {
                var sent = c.Workflow();
                sent.Enqueue("a.job", a);
                var lost = {|#2:c.Workflow()|};
                lost.Enqueue("b.job", a);
                await sent.SubmitAsync();
            }

            public static void After(IJobClient c, JsonElement a, JobId id) =>
                {|#3:c.Workflow()|}.After(id).ContinueWith("a.job", a);

            public static void Reassigned(IJobClient c, JsonElement a)
            {
                var wf = {|#4:c.Workflow()|};
                wf.Enqueue("a.job", a);
                wf = null;
            }
        }
        """,
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(0),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(1),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(2),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(3),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(4),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(5));

    [Fact]
    public Task Workflow_conditional_access_and_patterns_without_submit_BOLT009() => VerifyAsync(Workflows + """
        public static class S
        {
            public static void ConditionalChain(IJobClient c, JsonElement a) => {|#0:c.Workflow()|}?.Enqueue("a.job", a);

            public static void ConditionalLocal(IJobClient c, JsonElement a)
            {
                var wf = {|#1:c.Workflow()|};
                wf?.Enqueue("a.job", a);
            }

            public static void VarPattern(IJobClient c, JsonElement a)
            {
                if ({|#2:c.Workflow()|} is var wf)
                    wf.Enqueue("a.job", a);
            }

            public static void TypePattern(IJobClient c, JsonElement a)
            {
                if ({|#3:c.Workflow()|}.Enqueue("a.job", a) is IJobStep { Id: var id } s)
                    System.Console.WriteLine($"{id} {s.Id}");
            }

            public static void NullChecks(IJobClient c, JsonElement a)
            {
                var s = {|#4:c.Workflow()|}.Enqueue("a.job", a);
                if (s is null || s is IBatchStep)
                    return;
            }
        }
        """,
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(0),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(1),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(2),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(3),
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(4));

    [Fact]
    public Task Workflow_overload_not_returning_IWorkflow_is_not_a_workflow_root() => VerifyAsync(Workflows + """
        public interface IMyClient : IJobClient { int Workflow(int x); }

        public static class S
        {
            public static void Go(IMyClient c)
            {
                var n = c.Workflow(5);
                n++;
            }
        }
        """);

    [Fact]
    public Task Workflow_submitted_through_any_step_has_no_diagnostics() => VerifyAsync(Workflows + """
        public static class S
        {
            public static async Task Chain(IJobClient c, JsonElement a) =>
                await c.Workflow().Enqueue("a.job", a).ContinueWith("b.job", a).SubmitAsync();

            public static async Task Local(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                var fetch = wf.Enqueue("a.job", a);
                fetch.ContinueWith("b.job", a);
                await fetch.SubmitAsync();
            }

            public static async Task Batches(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                wf.StartBatch(b => b.Enqueue("a.job", a)).ContinueWith("b.job", a);
                await wf.SubmitAsync();
            }

            public static async Task Conditional(IJobClient c, JsonElement a, bool f)
            {
                var wf = c.Workflow();
                var s = f ? wf.Enqueue("a.job", a) : wf.Enqueue("b.job", a);
                await s.SubmitAsync();
            }

            public static async Task Coalesce(IJobClient c, JsonElement a, IJobStep? prev)
            {
                var wf = c.Workflow();
                var s = prev ?? wf.Enqueue("a.job", a);
                await s.SubmitAsync();
            }

            public static async Task Switch(IJobClient c, JsonElement a, int k)
            {
                var wf = c.Workflow();
                INodeStep s = k switch { 0 => wf.Enqueue("a.job", a), _ => wf.Enqueue("b.job", a) };
                await s.SubmitAsync();
            }

            public static async Task ConditionalAccess(IJobClient c, JsonElement a)
            {
                IJobStep? s = c.Workflow().Enqueue("a.job", a);
                await (s?.SubmitAsync() ?? default);
            }

            public static async Task ConditionalAccessResult(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                var s = wf?.Enqueue("a.job", a);
                await s!.SubmitAsync();
            }

            public static async Task IsPattern(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                if (wf.Enqueue("a.job", a) is IJobStep s)
                    await s.SubmitAsync();
            }

            public static async Task CombinedPattern(IJobClient c, JsonElement a)
            {
                if (c.Workflow().Enqueue("a.job", a) is not null and IJobStep { } s)
                    await s.SubmitAsync();
            }

            public static async Task SwitchStatement(IJobClient c, JsonElement a)
            {
                switch (c.Workflow().Enqueue("a.job", a))
                {
                    case null:
                        break;
                    case IJobStep s:
                        await s.SubmitAsync();
                        break;
                }
            }

            public static async Task SwitchExpression(IJobClient c, JsonElement a)
            {
                var submitted = c.Workflow().Enqueue("a.job", a) switch { IJobStep s => s.SubmitAsync(), _ => default };
                await submitted;
            }
        }
        """);

    [Fact]
    public Task Steps_created_in_batch_body_and_submitted_after_it_have_no_diagnostics() => VerifyAsync(Usings + """
        using System.Collections.Generic;
        using System.Text.Json;

        [assembly: PgBoltJsonContext(typeof(AppJson))]
        [Job("a.job")] public sealed record A(int X);
        public static class S
        {
            public static async Task StartBatchCaptured(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                IJobStep? first = null;
                wf.StartBatch(b => { first = b.Enqueue("a.job", a); });
                await first!.SubmitAsync();
            }

            public static async Task ContinueWithBatchCaptured(IJobClient c, JsonElement a)
            {
                IJobStep? last = null;
                c.Workflow().Enqueue("a.job", a).ContinueWithBatch(b => last = b.Enqueue("b.job", a));
                await last!.SubmitAsync();
            }

            public static async Task EnqueueManyCaptured(IJobClient c, IReadOnlyList<A> items)
            {
                IReadOnlyList<IJobStep>? steps = null;
                c.Workflow().StartBatch(b => steps = b.EnqueueMany(items));
                await steps![0].SubmitAsync();
            }
        }
        """ + Context);

    [Fact]
    public Task Steps_used_only_inside_batch_body_BOLT009() => VerifyAsync(Workflows + """
        public static class S
        {
            public static void Go(IJobClient c, JsonElement a)
            {
                var wf = {|#0:c.Workflow()|};
                wf.StartBatch(b =>
                {
                    var s = b.Enqueue("a.job", a);
                    s.ContinueWith("b.job", a);
                    b.StartBatch(i => i.Enqueue("c.job", a));
                });
            }
        }
        """,
        Diagnostic(Diagnostics.WorkflowNotSubmitted).WithLocation(0));

    [Fact]
    public Task Workflow_escaping_the_method_suppresses_BOLT009() => VerifyAsync(Workflows + """
        public sealed class S
        {
            private IWorkflow? _field;
            public INodeStep? Property { get; set; }

            public IWorkflow Returned(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                wf.Enqueue("a.job", a);
                return wf;
            }

            public void Passed(IJobClient c, JsonElement a) => Consume(c.Workflow().Enqueue("a.job", a));

            public void Field(IJobClient c)
            {
                var wf = c.Workflow();
                _field = wf;
            }

            public void Prop(IJobClient c, JsonElement a) => Property = c.Workflow().Enqueue("a.job", a);

            public void Out(IJobClient c, JsonElement a, out INodeStep s) { s = c.Workflow().Enqueue("a.job", a); }

            public void Ref(IJobClient c, JsonElement a, ref INodeStep s) { s = c.Workflow().Enqueue("a.job", a); }

            public void ArrayInitializer(IJobClient c, JsonElement a)
            {
                var steps = new INodeStep[] { c.Workflow().Enqueue("a.job", a) };
                ConsumeAll(steps);
            }

            public void CollectionInitializer(IJobClient c, JsonElement a)
            {
                var steps = new System.Collections.Generic.List<INodeStep> { c.Workflow().Enqueue("a.job", a) };
                ConsumeAll(steps);
            }

            public void CollectionExpression(IJobClient c, JsonElement a)
            {
                INodeStep[] steps = [c.Workflow().Enqueue("a.job", a)];
                ConsumeAll(steps);
            }

            public INodeStep? ConditionalAccessReturned(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                return wf?.Enqueue("a.job", a);
            }

            public void PositionalPattern(IJobClient c, JsonElement a)
            {
                if (c.Workflow().Enqueue("a.job", a) is (var first, _))
                    Keep(first);
            }

            public (INodeStep, int) Tuple(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                return (wf.Enqueue("a.job", a), 1);
            }

            private static readonly IJobClient Client = null!;
            private readonly IJobStep _initialized = Client.Workflow().Enqueue("a.job", default);
            public INodeStep Initialized { get; } = Client.Workflow().Enqueue("a.job", default);

            public void ArrayElement(IJobClient c, JsonElement a, INodeStep[] steps) => steps[0] = c.Workflow().Enqueue("a.job", a);

            public void Interpolated(IJobClient c, JsonElement a) => System.Console.WriteLine($"{c.Workflow().Enqueue("a.job", a)}");

            public void MethodGroup(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                System.Func<string, JsonElement, StepOptions?, IJobStep> enqueue = wf.Enqueue;
                Keep(enqueue);
            }

            public System.Func<INodeStep> Lambda(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                return () => wf.Enqueue("a.job", a);
            }

            public System.Collections.Generic.IEnumerable<INodeStep> Yielded(IJobClient c, JsonElement a)
            {
                var wf = c.Workflow();
                yield return wf.Enqueue("a.job", a);
            }

            public async Task CoalesceAssignment(IJobClient c, JsonElement a, IJobStep? s)
            {
                var wf = c.Workflow();
                s ??= wf.Enqueue("a.job", a);
                await s.SubmitAsync();
            }

            public async Task ChainedAssignment(IJobClient c, JsonElement a)
            {
                IJobStep s;
                var t = s = c.Workflow().Enqueue("a.job", a);
                await t.SubmitAsync();
            }

            public async Task Deconstructed(IJobClient c, JsonElement a)
            {
                var (s, _) = (c.Workflow().Enqueue("a.job", a), 1);
                await s.SubmitAsync();
            }

            private static void Keep(object value) { }

            private static void Consume(INodeStep step) { }

            private static void ConsumeAll(System.Collections.Generic.IEnumerable<INodeStep> steps) { }
        }

        public static class StepExtensions
        {
            public static void Deconstruct(this INodeStep step, out object first, out object second) => (first, second) = (step, step);
        }
        """);
}
