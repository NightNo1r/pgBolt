using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace PgBolt.Generators.Analysis
{
    /// <summary>
    /// BOLT010 detects conflicting constant ClusterMaxConcurrency or Weight values for the same queue in
    /// PgBoltServerOptions.Queue calls within one compilation. BOLT011 detects invalid constant queue options or
    /// PgBoltOptions.QueueSettingsCacheDuration. Startup validation also covers nonconstant values and names and
    /// cross-assembly calls. The analyzer has no dependency on PgBolt.Abstractions. Without PgBolt.Server it skips queue
    /// rules but still checks PgBoltOptions.
    /// </summary>
    internal sealed class QueueOptionsAnalysis
    {
        /// <summary>Cluster-wide queue options. A queue must have the same default value in all Queue calls.</summary>
        private static readonly string[] ClusterOptions = ["ClusterMaxConcurrency", "Weight"];

        /// <summary>Maps TimeSpan constructor and factory parameter names (days, hours, etc.) to ticks per unit.</summary>
        private static readonly Dictionary<string, double> TicksPerUnit = new()
        {
            ["days"] = 864_000_000_000,
            ["hours"] = 36_000_000_000,
            ["minutes"] = 600_000_000,
            ["seconds"] = 10_000_000,
            ["milliseconds"] = 10_000,
            ["microseconds"] = 10,
            ["ticks"] = 1,
        };

        private readonly INamedTypeSymbol? _queue;
        private readonly INamedTypeSymbol? _server;
        private readonly INamedTypeSymbol? _options;
        private readonly INamedTypeSymbol? _timeSpan;
        private readonly ConcurrentBag<Setting> _settings = new();

        private QueueOptionsAnalysis(INamedTypeSymbol? queue, INamedTypeSymbol? server, INamedTypeSymbol? options, INamedTypeSymbol? timeSpan)
        {
            _queue = queue;
            _server = server;
            _options = options;
            _timeSpan = timeSpan;
        }

        public static QueueOptionsAnalysis? TryCreate(Compilation compilation)
        {
            var queue = compilation.GetTypeByMetadataName(KnownNames.QueueOptionsMetadata);
            var server = compilation.GetTypeByMetadataName(KnownNames.ServerOptionsMetadata);
            var options = compilation.GetTypeByMetadataName(KnownNames.OptionsMetadata);
            if (queue is null || server is null)
                queue = server = null;
            return queue is null && options is null
                ? null
                : new QueueOptionsAnalysis(queue, server, options, compilation.GetTypeByMetadataName("System.TimeSpan"));
        }

        public void Register(CompilationStartAnalysisContext start)
        {
            start.RegisterOperationAction(AnalyzeAssignment, OperationKind.SimpleAssignment);
            if (_server is null)
                return;
            start.RegisterOperationAction(AnalyzeQueueCall, OperationKind.Invocation);
            start.RegisterCompilationEndAction(OnCompilationEnd);
        }

        private void AnalyzeAssignment(OperationAnalysisContext ctx)
        {
            var assignment = (ISimpleAssignmentOperation)ctx.Operation;
            if (assignment.Target is not IPropertyReferenceOperation { Property: var property })
                return;
            string? reason = null;
            if (SymbolEqualityComparer.Default.Equals(property.ContainingType, _queue))
                reason = QueueOptionError(property.Name, assignment);
            else if (SymbolEqualityComparer.Default.Equals(property.ContainingType, _options)
                && property.Name == "QueueSettingsCacheDuration" && Ticks(assignment.Value) <= 0)
                reason = "must be > 0 (use null to disable the cache)";
            if (reason is not null)
            {
                ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.InvalidOptionValue, assignment.Syntax.GetLocation(),
                    property.Name, assignment.Value.Syntax.ToString(), reason));
            }
        }

        private string? QueueOptionError(string option, ISimpleAssignmentOperation assignment)
        {
            if (Number(assignment.Value) is not { } value)
                return null;
            return option switch
            {
                "Reserved" when value < 0 => "must be >= 0",
                "MaxConcurrency" or "ClusterMaxConcurrency" when value < 1 => "must be >= 1",
                "MaxConcurrency" when ReservedInSameLambda(assignment) is { } reserved && value < reserved =>
                    $"must be >= Reserved ({Format(reserved)})",
                "Weight" when value is < 1 or > 1000 => "must be in [1, 1000]",
                _ => null,
            };
        }

        /// <summary>Returns the last constant assigned to Reserved in the same lambda as assignment, or null.</summary>
        private double? ReservedInSameLambda(IOperation assignment)
        {
            var lambda = assignment.Parent;
            while (lambda is not null and not IAnonymousFunctionOperation)
                lambda = lambda.Parent;
            var reserved = lambda?.Descendants().OfType<ISimpleAssignmentOperation>().LastOrDefault(a =>
                a.Target is IPropertyReferenceOperation { Property: { Name: "Reserved" } property }
                && SymbolEqualityComparer.Default.Equals(property.ContainingType, _queue));
            return reserved is null ? null : Number(reserved.Value);
        }

        /// <summary>Collects known ClusterMaxConcurrency and Weight values from a Queue call with a constant name.</summary>
        private void AnalyzeQueueCall(OperationAnalysisContext ctx)
        {
            var call = (IInvocationOperation)ctx.Operation;
            if (call.TargetMethod.Name != "Queue" || !SymbolEqualityComparer.Default.Equals(call.TargetMethod.ContainingType, _server))
                return;
            var nameArgument = call.Arguments.FirstOrDefault(a => a.Parameter?.Ordinal == 0);
            var configure = call.Arguments.FirstOrDefault(a => a.Parameter?.Ordinal == 1);
            if (nameArgument?.Value.ConstantValue.Value is not string name || configure is null
                || SimpleAssignments(configure.Value) is not { } assignments)
                return;
            foreach (var option in ClusterOptions)
            {
                var last = assignments.LastOrDefault(a => ((IPropertyReferenceOperation)a.Target).Property.Name == option);
                if (last is null)
                    _settings.Add(new Setting(name, option, null, nameArgument.Value.Syntax.GetLocation()));
                else if (TryGetNumber(last.Value, out var value))
                    _settings.Add(new Setting(name, option, value, last.Syntax.GetLocation()));
            }
        }

        /// <summary>
        /// Reads assignments from configure. An omitted, null or default configure yields an empty set. Returns null for an
        /// unknown value, including lambdas that do more than assign properties on their parameter or read the parameter on
        /// the right-hand side.
        /// </summary>
        private static List<ISimpleAssignmentOperation>? SimpleAssignments(IOperation configure)
        {
            if (configure.ConstantValue is { HasValue: true, Value: null })
                return [];
            if (configure is not IDelegateCreationOperation { Target: IAnonymousFunctionOperation lambda } || lambda.Symbol.Parameters.Length != 1)
                return null;
            var parameter = lambda.Symbol.Parameters[0];
            bool IsParameter(IOperation op) =>
                op is IParameterReferenceOperation reference && SymbolEqualityComparer.Default.Equals(reference.Parameter, parameter);
            var assignments = new List<ISimpleAssignmentOperation>();
            foreach (var statement in lambda.Body.Operations)
            {
                if (statement is IReturnOperation { IsImplicit: true, ReturnedValue: null })
                    continue;
                if (statement is not IExpressionStatementOperation
                    {
                        Operation: ISimpleAssignmentOperation { Target: IPropertyReferenceOperation { Instance: { } instance } } assignment,
                    }
                    || !IsParameter(instance) || assignment.Value.DescendantsAndSelf().Any(IsParameter))
                    return null;
                assignments.Add(assignment);
            }
            return assignments;
        }

        private void OnCompilationEnd(CompilationAnalysisContext ctx)
        {
            // Порядок по месту в коде: при трёх и более вызовах дополнительная локация не зависит от порядка анализа.
            var settings = _settings.OrderBy(s => s.Location.SourceTree?.FilePath, StringComparer.Ordinal)
                .ThenBy(s => s.Location.SourceSpan.Start);
            foreach (var group in settings.GroupBy(s => (s.Queue, s.Option)))
            {
                foreach (var setting in group)
                {
                    if (group.FirstOrDefault(other => other.Value != setting.Value) is { } other)
                    {
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.ConflictingQueueSetting, setting.Location, [other.Location],
                            setting.Queue, setting.Option, Format(setting.Value), Format(other.Value)));
                    }
                }
            }
        }

        /// <summary>Duration of a constant TimeSpan in ticks. Returns null for unrecognized values, including the null literal.</summary>
        private double? Ticks(IOperation value)
            => Unwrap(value) switch
            {
                IFieldReferenceOperation { Field: { Name: "Zero" } field } when IsTimeSpan(field.ContainingType) => 0,
                IInvocationOperation { TargetMethod: { IsStatic: true } method } call
                    when IsTimeSpan(method.ContainingType) && method.Name.StartsWith("From", StringComparison.Ordinal) =>
                    SumTicks(call.Arguments, method.Name.Substring(4).ToLowerInvariant()),
                IObjectCreationOperation creation when IsTimeSpan(creation.Type) => SumTicks(creation.Arguments, null),
                _ => null,
            };

        /// <summary>
        /// Sums arguments in ticks by parameter name (days, hours, etc.). For a factory such as FromSeconds(double value), the
        /// method name determines the unit.
        /// </summary>
        private static double? SumTicks(ImmutableArray<IArgumentOperation> arguments, string? unitOfValue)
        {
            double ticks = 0;
            foreach (var argument in arguments)
            {
                var unit = argument.Parameter?.Name == "value" ? unitOfValue : argument.Parameter?.Name;
                if (unit is null || !TicksPerUnit.TryGetValue(unit, out var perUnit) || Number(argument.Value) is not { } value)
                    return null;
                ticks += value * perUnit;
            }
            return ticks;
        }

        private bool IsTimeSpan(ITypeSymbol? type) => SymbolEqualityComparer.Default.Equals(type, _timeSpan);

        /// <summary>Reads a numeric constant or null literal (number = null) after implicit conversions such as int to nullable int. Returns false otherwise.</summary>
        private static bool TryGetNumber(IOperation value, out double? number)
        {
            var constant = Unwrap(value).ConstantValue;
            number = constant.Value switch
            {
                char c => c,
                sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal =>
                    Convert.ToDouble(constant.Value, CultureInfo.InvariantCulture),
                _ => null,
            };
            return constant.HasValue && (number is not null || constant.Value is null);
        }

        /// <summary>Reads a numeric constant. Returns null for other values, including the null literal.</summary>
        private static double? Number(IOperation value) => TryGetNumber(value, out var number) ? number : null;

        private static IOperation Unwrap(IOperation op)
        {
            while (!op.ConstantValue.HasValue && op is IConversionOperation { OperatorMethod: null } conversion)
                op = conversion.Operand;
            return op;
        }

        private static string Format(double? value) => value?.ToString(CultureInfo.InvariantCulture) ?? "null";

        private sealed record Setting(string Queue, string Option, double? Value, Location Location);
    }
}
