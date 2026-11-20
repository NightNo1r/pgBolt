using System.Text.Json.Serialization.Metadata;

namespace PgBolt;

/// <summary>Метаданные [Job]-типа, заполняемые сгенерированным кодом (без reflection).</summary>
public static class JobMetadata<TArgs>
{
    public static JobDescriptor? Descriptor { get; private set; }
    public static JsonTypeInfo<TArgs>? TypeInfo { get; private set; }

    internal static void Merge(JobDescriptor descriptor, JsonTypeInfo<TArgs>? typeInfo)
    {
        if (Descriptor is null)
            Descriptor = descriptor;
        else if (!Descriptor.SameAs(descriptor))
            throw new InvalidOperationException(
                $"Conflicting [Job] metadata for {typeof(TArgs).FullName}: '{Descriptor}' vs '{descriptor}'.");

        if (typeInfo is not null && TypeInfo is null)
            TypeInfo = typeInfo;
    }
}

public static class JobMetadataRegistry
{
    private static readonly Lock Gate = new();

    public static void Register<TArgs>(JobDescriptor descriptor, JsonTypeInfo<TArgs>? typeInfo)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        lock (Gate)
            JobMetadata<TArgs>.Merge(descriptor, typeInfo);
    }
}
