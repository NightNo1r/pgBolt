using System.Text.Json.Serialization.Metadata;

namespace PgBolt.Postgres.Client;

internal static class JobMetadataAccess
{
    public static (JobDescriptor Descriptor, JsonTypeInfo<TArgs> TypeInfo) Get<TArgs>() =>
        JobMetadata<TArgs>.Descriptor is { } d && JobMetadata<TArgs>.TypeInfo is { } ti
            ? (d, ti)
            : throw new InvalidOperationException(
                $"No PgBolt metadata for '{typeof(TArgs).FullName}'. Mark the type with [Job] and use it from an assembly " +
                "that references the PgBolt source generator and declares [assembly: PgBoltJsonContext(...)] including this type.");
}
