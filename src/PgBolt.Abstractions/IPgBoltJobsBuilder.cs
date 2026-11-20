using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization.Metadata;

namespace PgBolt;

/// <summary>Приёмник регистраций из сгенерированного PgBoltJobRegistry.Register.</summary>
public interface IPgBoltJobsBuilder
{
    IPgBoltJobsBuilder AddExecutor<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TJob>(
        JobDescriptor descriptor, JsonTypeInfo<TArgs> argsTypeInfo)
        where TJob : class, IJob<TArgs>;
}
