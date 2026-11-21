using System.Threading;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    /// <summary>Call sites requiring TArgs metadata: EnqueueAsync, EnqueueManyAsync, typed recurring calls (part 4) and typed workflow steps (part 5).</summary>
    internal static class EnqueueCallModel
    {
        public static JobTypeModel? From(GeneratorSyntaxContext ctx, CancellationToken ct)
        {
            if (ctx.SemanticModel.GetSymbolInfo(ctx.Node, ct).Symbol is not IMethodSymbol method)
                return null;
            var compilation = ctx.SemanticModel.Compilation;
            var client = compilation.GetTypeByMetadataName(KnownNames.JobClientMetadata);
            var callSite = (client is not null && KnownNames.IsClientMethod(method, client))
                || KnownNames.IsRecurringMethod(method, compilation.GetTypeByMetadataName(KnownNames.RecurringManagerMetadata),
                    compilation.GetTypeByMetadataName(KnownNames.OptionsMetadata))
                || KnownNames.IsWorkflowMethod(method,
                    compilation.GetTypeByMetadataName(KnownNames.WorkflowScopeMetadata),
                    compilation.GetTypeByMetadataName(KnownNames.NodeStepMetadata),
                    compilation.GetTypeByMetadataName(KnownNames.BatchBuilderMetadata));
            if (!callSite)
                return null;
            return method.TypeArguments[0] is INamedTypeSymbol args ? JobTypeModel.FromType(args) : null;
        }
    }
}
