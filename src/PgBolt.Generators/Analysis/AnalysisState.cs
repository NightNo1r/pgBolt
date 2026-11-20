using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace PgBolt.Generators.Analysis
{
    internal sealed class AnalysisState
    {
        private readonly INamedTypeSymbol _jobAttribute;
        private readonly INamedTypeSymbol _jobInterface;
        private readonly INamedTypeSymbol _jobClient;
        private readonly INamedTypeSymbol? _recurringManager;
        private readonly INamedTypeSymbol? _options;
        private readonly INamedTypeSymbol? _context;
        private readonly INamedTypeSymbol? _workflowScope, _nodeStep, _batchBuilder;
        private readonly HashSet<ITypeSymbol> _contextTypes;
        private readonly ConcurrentDictionary<string, ConcurrentBag<INamedTypeSymbol>> _names = new();
        private readonly ConcurrentDictionary<ITypeSymbol, ConcurrentBag<INamedTypeSymbol>> _executors = new(SymbolEqualityComparer.Default);
        private int _usages;

        private AnalysisState(INamedTypeSymbol jobAttribute, INamedTypeSymbol jobInterface, INamedTypeSymbol jobClient,
            INamedTypeSymbol? context, HashSet<ITypeSymbol> contextTypes, INamedTypeSymbol? recurringManager, INamedTypeSymbol? options,
            INamedTypeSymbol? workflowScope, INamedTypeSymbol? nodeStep, INamedTypeSymbol? batchBuilder)
        {
            _jobAttribute = jobAttribute;
            _jobInterface = jobInterface;
            _jobClient = jobClient;
            _context = context;
            _contextTypes = contextTypes;
            _recurringManager = recurringManager;
            _options = options;
            _workflowScope = workflowScope;
            _nodeStep = nodeStep;
            _batchBuilder = batchBuilder;
        }

        public static AnalysisState? TryCreate(Compilation compilation)
        {
            var jobAttribute = compilation.GetTypeByMetadataName(KnownNames.JobAttribute);
            var jobInterface = compilation.GetTypeByMetadataName(KnownNames.JobInterfaceMetadata);
            var jobClient = compilation.GetTypeByMetadataName(KnownNames.JobClientMetadata);
            if (jobAttribute is null || jobInterface is null || jobClient is null)
                return null;

            INamedTypeSymbol? context = null;
            var types = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
            foreach (var attr in compilation.Assembly.GetAttributes())
            {
                if (attr.AttributeClass?.ToDisplayString() != KnownNames.JsonContextAttribute
                    || attr.ConstructorArguments.Length != 1
                    || attr.ConstructorArguments[0].Value is not INamedTypeSymbol ctx)
                    continue;
                context = ctx;
                foreach (var a in ctx.GetAttributes())
                    if (a.AttributeClass?.ToDisplayString() == KnownNames.JsonSerializableAttribute
                        && a.ConstructorArguments.Length > 0 && a.ConstructorArguments[0].Value is ITypeSymbol t)
                        types.Add(t);
            }
            return new AnalysisState(jobAttribute, jobInterface, jobClient, context, types,
                compilation.GetTypeByMetadataName(KnownNames.RecurringManagerMetadata),
                compilation.GetTypeByMetadataName(KnownNames.OptionsMetadata),
                compilation.GetTypeByMetadataName(KnownNames.WorkflowScopeMetadata),
                compilation.GetTypeByMetadataName(KnownNames.NodeStepMetadata),
                compilation.GetTypeByMetadataName(KnownNames.BatchBuilderMetadata));
        }

        public void AnalyzeNamedType(SymbolAnalysisContext ctx)
        {
            var type = (INamedTypeSymbol)ctx.Symbol;

            var attr = type.GetAttributes().FirstOrDefault(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, _jobAttribute));
            if (attr is not null)
            {
                // Локация вычисляется сразу, а не локальной функцией: захваченный attr в ней считался бы maybe-null (CS8602).
                var location = attr.ApplicationSyntaxReference?.GetSyntax(ctx.CancellationToken).GetLocation() ?? type.Locations[0];

                if (attr.ConstructorArguments.Length == 1 && attr.ConstructorArguments[0].Value is string name)
                {
                    if (!JobNames.IsValid(name))
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidName, location, name));
                    _names.GetOrAdd(name, _ => new ConcurrentBag<INamedTypeSymbol>()).Add(type);
                }

                foreach (var arg in attr.NamedArguments)
                {
                    if (arg.Key != "TimeoutSeconds" || arg.Value.Value is not int timeout)
                        continue;
                    if (timeout < 0)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidTimeout, location,
                            timeout.ToString(CultureInfo.InvariantCulture)));
                    else if (timeout > Diagnostics.MaxTimeoutSeconds)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.TimeoutAboveLimit, location,
                            timeout.ToString(CultureInfo.InvariantCulture)));
                }
            }

            if (type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsGenericType)
                return;
            foreach (var i in type.AllInterfaces)
            {
                if (!SymbolEqualityComparer.Default.Equals(i.OriginalDefinition, _jobInterface))
                    continue;
                var args = i.TypeArguments[0];
                Interlocked.Increment(ref _usages);
                if (!HasJobAttribute(args))
                {
                    ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.ArgsNotJob, type.Locations[0], type.Name, args.ToDisplayString()));
                    continue;
                }
                _executors.GetOrAdd(args, _ => new ConcurrentBag<INamedTypeSymbol>()).Add(type);
                CheckCoverage(args, type.Locations[0], ctx.ReportDiagnostic);
            }
        }

        public void AnalyzeInvocation(OperationAnalysisContext ctx)
        {
            var method = ((IInvocationOperation)ctx.Operation).TargetMethod;
            if (!KnownNames.IsClientMethod(method, _jobClient)
                && !KnownNames.IsRecurringMethod(method, _recurringManager, _options)
                && !KnownNames.IsWorkflowMethod(method, _workflowScope, _nodeStep, _batchBuilder))
                return;
            var args = method.TypeArguments[0];
            if (args is ITypeParameterSymbol || !HasJobAttribute(args))
                return;
            Interlocked.Increment(ref _usages);
            CheckCoverage(args, ctx.Operation.Syntax.GetLocation(), ctx.ReportDiagnostic);
        }

        public void OnCompilationEnd(CompilationAnalysisContext ctx)
        {
            foreach (var pair in _names)
            {
                var types = pair.Value.Distinct<INamedTypeSymbol>(SymbolEqualityComparer.Default).ToList();
                if (types.Count > 1)
                    foreach (var t in types)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.DuplicateName, t.Locations[0], pair.Key));
            }
            foreach (var pair in _executors)
            {
                var executors = pair.Value.Distinct<INamedTypeSymbol>(SymbolEqualityComparer.Default).ToList();
                if (executors.Count > 1)
                    foreach (var e in executors)
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.MultipleExecutors, e.Locations[0], pair.Key.ToDisplayString()));
            }
            if (_context is null && Volatile.Read(ref _usages) > 0)
                ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.MissingJsonContext, Location.None));
        }

        private bool HasJobAttribute(ITypeSymbol type) =>
            type.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, _jobAttribute));

        private void CheckCoverage(ITypeSymbol args, Location location, System.Action<Diagnostic> report)
        {
            if (_context is null || _contextTypes.Contains(args))
                return;
            var properties = ImmutableDictionary<string, string?>.Empty
                .Add("ArgsType", args.ToDisplayString(KnownNames.Fq))
                .Add("ContextType", KnownNames.MetadataName(_context));
            report(Diagnostic.Create(Diagnostics.MissingJsonType, location, properties, args.ToDisplayString(), _context.Name));
        }
    }
}
