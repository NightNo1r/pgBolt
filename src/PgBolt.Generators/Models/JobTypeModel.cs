using System.Linq;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators
{
    internal sealed record JobTypeModel(
        string TypeFqn, string JobName, string Queue, int MaxAttempts, short Priority, EquatableArray<int>? RetryDelays, int TimeoutSeconds)
    {
        public static JobTypeModel? FromType(ITypeSymbol type)
        {
            foreach (var attr in type.GetAttributes())
            {
                if (attr.AttributeClass?.ToDisplayString() != KnownNames.JobAttribute)
                    continue;
                if (attr.ConstructorArguments.Length != 1 || attr.ConstructorArguments[0].Value is not string name)
                    return null;

                var queue = "default";
                var maxAttempts = 10;
                short priority = 0;
                EquatableArray<int>? delays = null;
                var timeoutSeconds = 0;
                foreach (var arg in attr.NamedArguments)
                {
                    switch (arg.Key)
                    {
                        case "Queue" when arg.Value.Value is string q:
                            queue = q;
                            break;
                        case "MaxAttempts" when arg.Value.Value is int m:
                            maxAttempts = m;
                            break;
                        case "Priority" when arg.Value.Value is short p:
                            priority = p;
                            break;
                        case "RetryDelaysSeconds" when !arg.Value.IsNull:
                            delays = new EquatableArray<int>(arg.Value.Values.Select(v => (int)v.Value!).ToArray());
                            break;
                        case "TimeoutSeconds" when arg.Value.Value is int t:
                            timeoutSeconds = t;
                            break;
                    }
                }
                return new JobTypeModel(type.ToDisplayString(KnownNames.Fq), name, queue, maxAttempts, priority, delays, timeoutSeconds);
            }
            return null;
        }
    }
}
