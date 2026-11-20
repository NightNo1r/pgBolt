using System;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal sealed record JsonContextModel(string? ContextFqn, EquatableArray<string> Types)
    {
        public bool Contains(string typeFqn) => Types.Contains(typeFqn);

        public static JsonContextModel From(Compilation compilation)
        {
            foreach (var attr in compilation.Assembly.GetAttributes())
            {
                if (attr.AttributeClass?.ToDisplayString() != KnownNames.JsonContextAttribute
                    || attr.ConstructorArguments.Length != 1
                    || attr.ConstructorArguments[0].Value is not INamedTypeSymbol context)
                    continue;
                var types = context.GetAttributes()
                    .Where(a => a.AttributeClass?.ToDisplayString() == KnownNames.JsonSerializableAttribute && a.ConstructorArguments.Length > 0)
                    .Select(a => a.ConstructorArguments[0].Value)
                    .OfType<ITypeSymbol>()
                    .Select(t => t.ToDisplayString(KnownNames.Fq))
                    .OrderBy(t => t, StringComparer.Ordinal)
                    .ToArray();
                return new JsonContextModel(context.ToDisplayString(KnownNames.Fq), new EquatableArray<string>(types));
            }
            return new JsonContextModel(null, new EquatableArray<string>(Array.Empty<string>()));
        }
    }
}
