using System.Threading;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal sealed record ExecutorModel(string JobFqn, JobTypeModel Args)
    {
        public static ExecutorModel? From(GeneratorSyntaxContext ctx, CancellationToken ct)
        {
            if (ctx.SemanticModel.GetDeclaredSymbol(ctx.Node, ct) is not INamedTypeSymbol { TypeKind: TypeKind.Class, IsAbstract: false, IsGenericType: false } type)
                return null;
            var jobInterface = ctx.SemanticModel.Compilation.GetTypeByMetadataName(KnownNames.JobInterfaceMetadata);
            if (jobInterface is null)
                return null;
            foreach (var i in type.AllInterfaces)
                if (SymbolEqualityComparer.Default.Equals(i.OriginalDefinition, jobInterface) && JobTypeModel.FromType(i.TypeArguments[0]) is { } args)
                    return new ExecutorModel(type.ToDisplayString(KnownNames.Fq), args);
            return null;
        }
    }
}
