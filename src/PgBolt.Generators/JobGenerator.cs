using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace PgBolt.Generators
{
    [Generator(LanguageNames.CSharp)]
    public sealed class JobGenerator : IIncrementalGenerator
    {
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            var declared = context.SyntaxProvider.ForAttributeWithMetadataName(KnownNames.JobAttribute,
                    static (node, _) => node is TypeDeclarationSyntax,
                    static (ctx, _) => JobTypeModel.FromType((ITypeSymbol)ctx.TargetSymbol))
                .Where(static m => m is not null).Select(static (m, _) => m!).Collect();

            var executors = context.SyntaxProvider.CreateSyntaxProvider(
                    static (node, _) => node is ClassDeclarationSyntax { BaseList: not null } or RecordDeclarationSyntax { BaseList: not null },
                    static (ctx, ct) => ExecutorModel.From(ctx, ct))
                .Where(static m => m is not null).Select(static (m, _) => m!).Collect();

            var calls = context.SyntaxProvider.CreateSyntaxProvider(
                    static (node, _) => node is InvocationExpressionSyntax
                    {
                        Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "EnqueueAsync" or "EnqueueManyAsync" or "AddOrUpdateAsync" or "AddRecurring" or "Enqueue" or "ContinueWith" or "EnqueueMany" }
                            or MemberBindingExpressionSyntax { Name.Identifier.ValueText: "EnqueueAsync" or "EnqueueManyAsync" or "AddOrUpdateAsync" or "AddRecurring" or "Enqueue" or "ContinueWith" or "EnqueueMany" },
                    },
                    static (ctx, ct) => EnqueueCallModel.From(ctx, ct))
                .Where(static m => m is not null).Select(static (m, _) => m!).Collect();

            var json = context.CompilationProvider.Select(static (c, _) => JsonContextModel.From(c));

            var rootNamespace = context.AnalyzerConfigOptionsProvider.Combine(context.CompilationProvider)
                .Select(static (pair, _) =>
                    pair.Left.GlobalOptions.TryGetValue("build_property.RootNamespace", out var ns) && !string.IsNullOrWhiteSpace(ns)
                        ? ns!
                        : pair.Right.AssemblyName ?? "PgBoltGenerated");

            var all = declared.Combine(executors).Combine(calls).Combine(json).Combine(rootNamespace);
            context.RegisterSourceOutput(all, static (spc, data) =>
            {
                var ((((declaredTypes, executorModels), callTypes), jsonContext), ns) = data;
                Emitter.Emit(spc, declaredTypes, executorModels, callTypes, jsonContext, ns);
            });
        }
    }
}
