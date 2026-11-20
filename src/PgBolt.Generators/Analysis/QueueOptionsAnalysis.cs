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
    /// BOLT010 — разные константные ClusterMaxConcurrency / Weight одной очереди в вызовах PgBoltServerOptions.Queue одной
    /// компиляции; BOLT011 — недопустимое константное значение опции очереди или PgBoltOptions.QueueSettingsCacheDuration.
    /// Ранняя проверка только констант: неконстантные значения, неконстантные имена и вызовы в разных сборках проверяет
    /// валидация при старте. Не зависит от PgBolt.Abstractions: без PgBolt.Server правила очередей молчат, а BOLT011 по
    /// PgBoltOptions работает.
    /// </summary>
    internal sealed class QueueOptionsAnalysis
    {
        /// <summary>Опции очереди на весь кластер: одно имя очереди — одно значение по умолчанию во всех вызовах Queue.</summary>
        private static readonly string[] ClusterOptions = ["ClusterMaxConcurrency", "Weight"];

        /// <summary>Тиков в единице: имя параметра конструктора и фабрик TimeSpan (days, hours, …) → длительность единицы.</summary>
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

        /// <summary>Последняя константа, присвоенная Reserved в той же лямбде, что и assignment; иначе null.</summary>
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

        /// <summary>Значения ClusterMaxConcurrency и Weight вызова Queue с константным именем; неизвестные значения не собираются.</summary>
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
        /// Присваивания configure: пусто — configure не передан, null или default; null — значение неизвестно (не лямбда или
        /// лямбда не только из присваиваний свойствам своего параметра с правой частью без этого параметра).
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

        /// <summary>Длительность константного TimeSpan в тиках; null — значение не распознано (в том числе литерал null).</summary>
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
        /// Сумма аргументов в тиках по именам параметров (days, hours, …); у фабрик с параметром value (FromSeconds(double value))
        /// единица — из имени метода.
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

        /// <summary>Числовая константа или литерал null (number = null) после неявных преобразований (int → int?); иначе false.</summary>
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

        /// <summary>Числовая константа; null — значение не числовая константа (в том числе литерал null).</summary>
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
