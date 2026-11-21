using System.Linq;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal static class KnownNames
    {
        public const string JobAttribute = "PgBolt.JobAttribute";
        public const string JobInterfaceMetadata = "PgBolt.IJob`1";
        public const string JobClientMetadata = "PgBolt.IJobClient";
        public const string JsonContextAttribute = "PgBolt.PgBoltJsonContextAttribute";
        public const string JsonSerializableAttribute = "System.Text.Json.Serialization.JsonSerializableAttribute";
        public const string RecurringManagerMetadata = "PgBolt.IRecurringJobManager";
        public const string OptionsMetadata = "PgBolt.Postgres.PgBoltOptions";
        public const string WorkflowScopeMetadata = "PgBolt.IWorkflowScope";
        public const string NodeStepMetadata = "PgBolt.INodeStep";
        public const string BatchBuilderMetadata = "PgBolt.IBatchBuilder";
        public const string WorkflowMetadata = "PgBolt.IWorkflow";
        public const string QueueOptionsMetadata = "PgBolt.Server.QueueOptions";
        public const string ServerOptionsMetadata = "PgBolt.Server.PgBoltServerOptions";

        public static readonly SymbolDisplayFormat Fq = SymbolDisplayFormat.FullyQualifiedFormat;

        public static bool IsClientMethod(IMethodSymbol method, INamedTypeSymbol client) =>
            method.Name is "EnqueueAsync" or "EnqueueManyAsync"
            && method.IsGenericMethod
            && (SymbolEqualityComparer.Default.Equals(method.ContainingType, client)
                || method.ContainingType.AllInterfaces.Contains(client, SymbolEqualityComparer.Default));

        /// <summary>
        /// Typed AddOrUpdateAsync&lt;TArgs&gt; and AddRecurring&lt;TArgs&gt; use JsonTypeInfo from JobMetadata&lt;TArgs&gt;
        /// like EnqueueAsync. Their call sites therefore register metadata even in a producer without a handler (part 4).
        /// </summary>
        public static bool IsRecurringMethod(IMethodSymbol method, INamedTypeSymbol? manager, INamedTypeSymbol? options) =>
            method.IsGenericMethod
            && ((method.Name == "AddOrUpdateAsync" && manager is not null
                    && (SymbolEqualityComparer.Default.Equals(method.ContainingType, manager)
                        || method.ContainingType.AllInterfaces.Contains(manager, SymbolEqualityComparer.Default)))
                || (method.Name == "AddRecurring" && options is not null
                    && SymbolEqualityComparer.Default.Equals(method.ContainingType, options)));

        /// <summary>
        /// Typed workflow steps Enqueue&lt;TArgs&gt;, ContinueWith&lt;TArgs&gt; and EnqueueMany&lt;TArgs&gt; obtain
        /// JsonTypeInfo from JobMetadata&lt;TArgs&gt; like EnqueueAsync. Their call sites register metadata (part 5, §5.4).
        /// Methods inherited by IWorkflow from IWorkflowScope are identified by ContainingType = IWorkflowScope.
        /// </summary>
        public static bool IsWorkflowMethod(IMethodSymbol method, INamedTypeSymbol? scope, INamedTypeSymbol? node, INamedTypeSymbol? batchBuilder) =>
            method.IsGenericMethod
            && method.Name switch
            {
                "Enqueue" => Implements(method.ContainingType, scope),
                "ContinueWith" => Implements(method.ContainingType, node),
                "EnqueueMany" => Implements(method.ContainingType, batchBuilder),
                _ => false,
            };

        public static bool Implements(INamedTypeSymbol type, INamedTypeSymbol? contract) =>
            contract is not null
            && (SymbolEqualityComparer.Default.Equals(type, contract) || type.AllInterfaces.Contains(contract, SymbolEqualityComparer.Default));

        /// <summary>Name used by Compilation.GetTypeByMetadataName, for example Ns.Outer+Inner`1.</summary>
        public static string MetadataName(INamedTypeSymbol type)
        {
            var name = type.MetadataName;
            for (var t = type.ContainingType; t is not null; t = t.ContainingType)
                name = t.MetadataName + "+" + name;
            var ns = type.ContainingNamespace;
            return ns is null || ns.IsGlobalNamespace ? name : ns.ToDisplayString() + "." + name;
        }
    }
}
