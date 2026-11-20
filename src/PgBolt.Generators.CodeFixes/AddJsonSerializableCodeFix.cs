using System.Collections.Immutable;
using System.Composition;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Formatting;
using Microsoft.CodeAnalysis.Simplification;

namespace PgBolt.Generators.CodeFixes
{
    [ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(AddJsonSerializableCodeFix)), Shared]
    public sealed class AddJsonSerializableCodeFix : CodeFixProvider
    {
        public override ImmutableArray<string> FixableDiagnosticIds => ImmutableArray.Create("BOLT001");

        public override FixAllProvider? GetFixAllProvider() => null;

        public override Task RegisterCodeFixesAsync(CodeFixContext context)
        {
            var diagnostic = context.Diagnostics[0];
            if (!diagnostic.Properties.TryGetValue("ArgsType", out var argsType) || argsType is null
                || !diagnostic.Properties.TryGetValue("ContextType", out var contextType) || contextType is null)
                return Task.CompletedTask;

            var project = context.Document.Project;
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Add [JsonSerializable(typeof({argsType.Replace("global::", "")}))] to the PgBolt JSON context",
                    ct => AddAttributeAsync(project, contextType, argsType, ct),
                    equivalenceKey: "BOLT001:" + argsType),
                diagnostic);
            return Task.CompletedTask;
        }

        private static async Task<Solution> AddAttributeAsync(Project project, string contextMetadataName, string argsType, CancellationToken ct)
        {
            var solution = project.Solution;
            var compilation = await project.GetCompilationAsync(ct).ConfigureAwait(false);
            var syntaxRef = compilation?.GetTypeByMetadataName(contextMetadataName)?.DeclaringSyntaxReferences.FirstOrDefault();
            if (syntaxRef is null)
                return solution;
            var document = solution.GetDocument(syntaxRef.SyntaxTree);
            if (document is null)
                return solution;
            var root = await document.GetSyntaxRootAsync(ct).ConfigureAwait(false);
            if (root is null || await syntaxRef.GetSyntaxAsync(ct).ConfigureAwait(false) is not ClassDeclarationSyntax declaration)
                return solution;

            var endOfLine = declaration.AttributeLists.LastOrDefault()?.GetTrailingTrivia()
                                .FirstOrDefault(t => t.IsKind(SyntaxKind.EndOfLineTrivia)) is { } eol && eol.IsKind(SyntaxKind.EndOfLineTrivia)
                ? eol
                : SyntaxFactory.ElasticCarriageReturnLineFeed;

            var attributeList = SyntaxFactory.AttributeList(SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Attribute(
                        SyntaxFactory.ParseName("global::System.Text.Json.Serialization.JsonSerializable"),
                        SyntaxFactory.ParseAttributeArgumentList($"(typeof({argsType}))"))))
                .WithAdditionalAnnotations(Simplifier.Annotation, Formatter.Annotation);

            ClassDeclarationSyntax updated;
            if (declaration.AttributeLists.Count == 0)
            {
                var leading = declaration.GetLeadingTrivia();
                updated = declaration.WithoutLeadingTrivia()
                    .AddAttributeLists(attributeList.WithLeadingTrivia(leading).WithTrailingTrivia(endOfLine));
            }
            else
            {
                updated = declaration.AddAttributeLists(attributeList.WithTrailingTrivia(endOfLine));
            }

            return solution.WithDocumentSyntaxRoot(document.Id, root.ReplaceNode(declaration, updated));
        }
    }
}
