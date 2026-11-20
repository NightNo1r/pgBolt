using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace PgBolt.Generators.Analysis
{
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class PgBoltAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => Diagnostics.All;

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterCompilationStartAction(static start =>
            {
                if (QueueOptionsAnalysis.TryCreate(start.Compilation) is { } queues)
                    queues.Register(start);
                var state = AnalysisState.TryCreate(start.Compilation);
                if (state is null)
                    return; // PgBolt.Abstractions не подключён
                start.RegisterSymbolAction(state.AnalyzeNamedType, SymbolKind.NamedType);
                start.RegisterOperationAction(state.AnalyzeInvocation, OperationKind.Invocation);
                start.RegisterCompilationEndAction(state.OnCompilationEnd);
                if (WorkflowSubmitAnalysis.TryCreate(start.Compilation) is { } workflows)
                    start.RegisterOperationBlockStartAction(workflows.OnBlockStart);
            });
        }
    }
}
