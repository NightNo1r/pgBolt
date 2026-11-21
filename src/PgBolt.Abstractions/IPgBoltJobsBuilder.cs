using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization.Metadata;

namespace PgBolt;

/// <summary>Accepts registrations from the generated PgBoltJobRegistry.Register method.</summary>
public interface IPgBoltJobsBuilder
{
    IPgBoltJobsBuilder AddExecutor<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TJob>(
        JobDescriptor descriptor, JsonTypeInfo<TArgs> argsTypeInfo)
        where TJob : class, IJob<TArgs>;
}
