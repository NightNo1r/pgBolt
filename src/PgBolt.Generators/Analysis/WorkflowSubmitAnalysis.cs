using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace PgBolt.Generators.Analysis
{
    /// <summary>
    /// BOLT009 (часть 5 §5.4): результат IJobClient.Workflow(), сохранённый в локальную переменную или использованный цепочкой,
    /// ни одно значение потока которого (сам workflow, шаги от вызовов над значениями потока, параметр лямбды-тела batch,
    /// переданной вызову из потока, список шагов EnqueueMany и его элементы, локальные с ними, выражения ?:, ??, switch с ветвью
    /// из потока, присваивание, x?.M() и получатель ?. над значением потока, локальные из шаблонов is / switch над значением)
    /// не дошло до SubmitAsync в том же методе. Тело batch, переданное группой методов, не прослеживается (см. BatchBodyCall). Ложное срабатывание хуже пропуска: любое использование значения
    /// потока, кроме распознанных форм (получатель вызова, ?. или чтения свойства, сохранение в обычную локальную, ветвь ?: / ?? /
    /// switch, проверка типа и непозиционный шаблон, отброшенное выражение-statement), считается уходом из анализируемого потока
    /// и подавляет диагностику — возврат, аргумент, поле, кортеж, коллекция, делегат, Deconstruct и т. п.
    /// Анализ нечувствителен к порядку и ветвлениям — эвристика, а не поток управления.
    /// </summary>
    internal sealed class WorkflowSubmitAnalysis
    {
        private readonly INamedTypeSymbol _client;
        private readonly INamedTypeSymbol _workflow;
        private readonly INamedTypeSymbol _node;

        private WorkflowSubmitAnalysis(INamedTypeSymbol client, INamedTypeSymbol workflow, INamedTypeSymbol node)
        {
            _client = client;
            _workflow = workflow;
            _node = node;
        }

        public static WorkflowSubmitAnalysis? TryCreate(Compilation compilation)
        {
            var client = compilation.GetTypeByMetadataName(KnownNames.JobClientMetadata);
            var workflow = compilation.GetTypeByMetadataName(KnownNames.WorkflowMetadata);
            var node = compilation.GetTypeByMetadataName(KnownNames.NodeStepMetadata);
            return client is null || workflow is null || node is null ? null : new WorkflowSubmitAnalysis(client, workflow, node);
        }

        /// <summary>
        /// Дешёвый фильтр: разбор блока (AnalyzeBlock) только если в нём встретился вызов IJobClient.Workflow — иначе тела
        /// методов без workflow не обходятся вовсе.
        /// </summary>
        public void OnBlockStart(OperationBlockStartAnalysisContext start)
        {
            var hasWorkflow = false;
            start.RegisterOperationAction(ctx =>
            {
                if (IsWorkflowCall((IInvocationOperation)ctx.Operation))
                    Volatile.Write(ref hasWorkflow, true);
            }, OperationKind.Invocation);
            start.RegisterOperationBlockEndAction(ctx =>
            {
                if (Volatile.Read(ref hasWorkflow))
                    AnalyzeBlock(ctx);
            });
        }

        public void AnalyzeBlock(OperationBlockAnalysisContext ctx)
        {
            foreach (var block in ctx.OperationBlocks)
            {
                var all = block.Descendants().ToList();
                var roots = all.OfType<IInvocationOperation>().Where(IsWorkflowCall).ToList();
                if (roots.Count == 0)
                    continue;
                var candidates = all.Where(CanJoin).ToList();   // один раз на блок, а не на каждый Workflow()
                foreach (var root in roots)
                {
                    if ((StoredLocals(root).Any() || IsReceiver(root)) && !Escapes(root, candidates))
                        ctx.ReportDiagnostic(Diagnostic.Create(Diagnostics.WorkflowNotSubmitted, root.Syntax.GetLocation()));
                }
            }
        }

        /// <summary>Виды операций, которые могут войти в поток (ветви switch в Escapes).</summary>
        private static bool CanJoin(IOperation op) =>
            op is IInvocationOperation or ILocalReferenceOperation or IParameterReferenceOperation or IPropertyReferenceOperation
                or IConditionalOperation or ICoalesceOperation or ISwitchExpressionOperation or IConditionalAccessOperation
                or IConditionalAccessInstanceOperation or ISimpleAssignmentOperation;

        /// <summary>IJobClient.Workflow или его реализация: перегрузки с тем же именем, не возвращающие IWorkflow, — не корни.</summary>
        private bool IsWorkflowCall(IInvocationOperation invocation) =>
            invocation.TargetMethod.Name == "Workflow"
            && SymbolEqualityComparer.Default.Equals(invocation.TargetMethod.ReturnType, _workflow)
            && KnownNames.Implements(invocation.TargetMethod.ContainingType, _client);

        private bool IsFlowType(ITypeSymbol? type) =>
            type is INamedTypeSymbol named
            && (SymbolEqualityComparer.Default.Equals(named, _workflow) || KnownNames.Implements(named, _node));

        /// <summary>Обобщённый тип с единственным аргументом — типом потока: IReadOnlyList&lt;IJobStep&gt; из EnqueueMany.</summary>
        private bool IsFlowList(ITypeSymbol? type) =>
            type is INamedTypeSymbol { IsGenericType: true, TypeArguments.Length: 1 } named && IsFlowType(named.TypeArguments[0]);

        /// <summary>Замыкание потока от root; true — хотя бы одно значение потока отправлено или ушло из анализируемого потока.</summary>
        private bool Escapes(IInvocationOperation root, List<IOperation> candidates)
        {
            var flow = new HashSet<IOperation> { root };
            var locals = new HashSet<ILocalSymbol>(SymbolEqualityComparer.Default);
            bool InFlow(IOperation? op) => op is not null && flow.Contains(Unwrap(op));
            for (var changed = true; changed;)
            {
                changed = false;
                foreach (var op in candidates)
                {
                    if (flow.Contains(op))
                        continue;
                    var joins = op switch
                    {
                        IInvocationOperation call => InFlow(call.Instance)
                            && (IsFlowType(call.TargetMethod.ReturnType) || IsFlowList(call.TargetMethod.ReturnType)),
                        ILocalReferenceOperation local => locals.Contains(local.Local),
                        IParameterReferenceOperation parameter => InFlow(BatchBodyCall(parameter)),
                        IPropertyReferenceOperation property => InFlow(property.Instance) && IsFlowType(property.Type),
                        IConditionalOperation conditional => InFlow(conditional.WhenTrue) || InFlow(conditional.WhenFalse),
                        ICoalesceOperation coalesce => InFlow(coalesce.Value) || InFlow(coalesce.WhenNull),
                        ISwitchExpressionOperation switchExpression => switchExpression.Arms.Any(arm => InFlow(arm.Value)),
                        IConditionalAccessOperation access => InFlow(access.WhenNotNull),
                        IConditionalAccessInstanceOperation => ConditionalAccessOf(op) is { } access && InFlow(access.Operation),
                        ISimpleAssignmentOperation assignment => InFlow(assignment.Value),
                        _ => false,
                    };
                    if (joins)
                    {
                        flow.Add(op);
                        changed = true;
                    }
                }
                foreach (var local in flow.ToList().SelectMany(StoredLocals))
                {
                    if (locals.Add(local))
                        changed = true;
                }
            }

            return flow.Any(Leaves);
        }

        /// <summary>
        /// true — значение потока отправлено (получатель SubmitAsync) или ушло в форму, которую анализ не распознаёт
        /// (консервативно: уход подавляет BOLT009).
        /// </summary>
        private static bool Leaves(IOperation op)
        {
            if (PatternsOn(op) is { } patterns)
                return patterns.Any(IsPositional);  // позиционный подшаблон вызывает Deconstruct со значением

            return Parent(op) switch
            {
                IInvocationOperation call when Is(call.Instance, op) => call.TargetMethod.Name == "SubmitAsync",
                IPropertyReferenceOperation property when Is(property.Instance, op) => false,  // чтение Id и т. п.
                IConditionalAccessOperation => false,  // получатель ?. (его использование — у IConditionalAccessInstance) или ветвь
                IIsTypeOperation => false,
                ISimpleAssignmentOperation assignment => !Is(assignment.Target, op) && !IsPlainLocal(assignment.Target),
                IVariableInitializerOperation { Parent: IVariableDeclaratorOperation declarator } => declarator.Symbol.RefKind != RefKind.None,
                IConditionalOperation or ICoalesceOperation => false,  // ветвь: само выражение в потоке
                ISwitchExpressionArmOperation arm => !Is(arm.Value, op),
                IExpressionStatementOperation => false,
                _ => true,
            };
        }

        /// <summary>
        /// Вызов, аргументом которого передана лямбда с этим параметром (b в wf.StartBatch(b => …)), иначе null: параметр тела
        /// batch входит в поток вместе с вызовом StartBatch / ContinueWithBatch. Тело, переданное группой методов (локальная
        /// функция или метод), не распознаётся: шаги, созданные в нём и отправленные после вызова через захваченную локальную,
        /// дают ложный BOLT009 — редкий случай, обходится лямбдой.
        /// </summary>
        private static IInvocationOperation? BatchBodyCall(IParameterReferenceOperation reference)
        {
            for (var ancestor = reference.Parent; ancestor is not null; ancestor = ancestor.Parent)
            {
                if (ancestor is IAnonymousFunctionOperation lambda
                    && SymbolEqualityComparer.Default.Equals(lambda.Symbol, reference.Parameter.ContainingSymbol))
                {
                    return lambda.Parent is IDelegateCreationOperation creation
                        && Parent(creation) is IArgumentOperation { Parent: IInvocationOperation call } ? call : null;
                }
            }
            return null;
        }

        private static bool Is(IOperation? candidate, IOperation op) => candidate is not null && Unwrap(candidate) == op;

        private static bool IsPlainLocal(IOperation target) => target is ILocalReferenceOperation { Local.RefKind: RefKind.None };

        private static bool IsReceiver(IOperation op) => Parent(op) switch
        {
            IInvocationOperation call => Is(call.Instance, op),
            IConditionalAccessOperation access => Is(access.Operation, op),
            _ => false,
        };

        /// <summary>
        /// Локальные, в которые сохранено значение: инициализатор объявления, присваивание локальной, объявление верхнего уровня
        /// в шаблоне is / case / ветви switch над значением (вложенные подшаблоны — чтение свойств).
        /// </summary>
        private static IEnumerable<ILocalSymbol> StoredLocals(IOperation op)
        {
            if (PatternsOn(op) is { } patterns)
                return patterns.SelectMany(DeclaredLocals);
            return Parent(op) switch
            {
                IVariableInitializerOperation { Parent: IVariableDeclaratorOperation declarator } => [declarator.Symbol],
                ISimpleAssignmentOperation { Target: ILocalReferenceOperation local } assignment when Is(assignment.Value, op) => [local.Local],
                _ => [],
            };
        }

        /// <summary>Шаблоны, проверяемые над самим значением: x is P, switch (x) { case P: }, x switch { P => … }; иначе null.</summary>
        private static IEnumerable<IPatternOperation>? PatternsOn(IOperation op) => Parent(op) switch
        {
            IIsPatternOperation isPattern when Is(isPattern.Value, op) => [isPattern.Pattern],
            ISwitchOperation statement when Is(statement.Value, op) =>
                statement.Cases.SelectMany(c => c.Clauses).OfType<IPatternCaseClauseOperation>().Select(clause => clause.Pattern),
            ISwitchExpressionOperation expression when Is(expression.Value, op) => expression.Arms.Select(arm => arm.Pattern),
            _ => null,
        };

        /// <summary>Локальные, связанные с самим значением: объявление шаблона и обе стороны and.</summary>
        private static IEnumerable<ILocalSymbol> DeclaredLocals(IPatternOperation pattern) => pattern switch
        {
            IDeclarationPatternOperation { DeclaredSymbol: ILocalSymbol local } => [local],
            IRecursivePatternOperation { DeclaredSymbol: ILocalSymbol local } => [local],
            IBinaryPatternOperation binary => DeclaredLocals(binary.LeftPattern).Concat(DeclaredLocals(binary.RightPattern)),
            _ => [],
        };

        private static bool IsPositional(IPatternOperation pattern) =>
            pattern.DescendantsAndSelf().OfType<IRecursivePatternOperation>().Any(recursive => !recursive.DeconstructionSubpatterns.IsEmpty);

        /// <summary>Условный доступ, к которому относится x в x?.M(): ближайший предок, в чью ветку WhenNotNull мы поднялись.</summary>
        private static IConditionalAccessOperation? ConditionalAccessOf(IOperation instance)
        {
            for (var (child, parent) = (instance, instance.Parent); parent is not null; (child, parent) = (parent, parent.Parent))
            {
                if (parent is IConditionalAccessOperation access && access.WhenNotNull == child)
                    return access;
            }
            return null;
        }

        private static IOperation Unwrap(IOperation op)
        {
            while (op is IConversionOperation { OperatorMethod: null } conversion)  // пользовательское преобразование — уход
                op = conversion.Operand;
            return op;
        }

        private static IOperation? Parent(IOperation op)
        {
            var parent = op.Parent;
            while (parent is IConversionOperation { OperatorMethod: null })
                parent = parent.Parent;
            return parent;
        }
    }
}
