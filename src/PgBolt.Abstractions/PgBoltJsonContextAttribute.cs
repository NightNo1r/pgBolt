namespace PgBolt;

/// <summary>Указывает JsonSerializerContext сборки, в котором объявлены args-типы задач (§7).</summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class PgBoltJsonContextAttribute(Type contextType) : Attribute
{
    public Type ContextType { get; } = contextType;
}
