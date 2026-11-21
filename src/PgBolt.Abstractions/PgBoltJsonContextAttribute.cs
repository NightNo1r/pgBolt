namespace PgBolt;

/// <summary>Identifies the assembly's JsonSerializerContext declaring the job argument types (§7).</summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class PgBoltJsonContextAttribute(Type contextType) : Attribute
{
    public Type ContextType { get; } = contextType;
}
