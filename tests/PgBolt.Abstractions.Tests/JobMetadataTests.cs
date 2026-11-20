using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace PgBolt.Abstractions.Tests;

public sealed record MetaArgsA(int X);
public sealed record MetaArgsB(int X);
public sealed record MetaArgsC(int X);

[JsonSerializable(typeof(MetaArgsA))]
[JsonSerializable(typeof(MetaArgsB))]
[JsonSerializable(typeof(MetaArgsC))]
internal sealed partial class MetaJson : JsonSerializerContext;

public sealed class JobMetadataTests
{
    [Fact]
    public void Register_sets_descriptor_and_type_info()
    {
        var d = new JobDescriptor("meta.a", "q", 3, 0, null);
        JobMetadataRegistry.Register(d, MetaJson.Default.MetaArgsA);
        Assert.True(JobMetadata<MetaArgsA>.Descriptor!.SameAs(d));
        Assert.Same(MetaJson.Default.MetaArgsA, JobMetadata<MetaArgsA>.TypeInfo);
    }

    [Fact]
    public void Null_type_info_never_overwrites_existing()
    {
        var d = new JobDescriptor("meta.b", "q", 3, 0, [5]);
        JobMetadataRegistry.Register(d, MetaJson.Default.MetaArgsB);
        JobMetadataRegistry.Register<MetaArgsB>(d with { RetryDelaysSeconds = [5] }, null);
        Assert.Same(MetaJson.Default.MetaArgsB, JobMetadata<MetaArgsB>.TypeInfo);
    }

    [Fact]
    public void Type_info_is_filled_by_later_registration()
    {
        var d = new JobDescriptor("meta.c", "q", 3, 0, null);
        JobMetadataRegistry.Register<MetaArgsC>(d, null);
        Assert.Null(JobMetadata<MetaArgsC>.TypeInfo);
        JobMetadataRegistry.Register(d, MetaJson.Default.MetaArgsC);
        Assert.Same(MetaJson.Default.MetaArgsC, JobMetadata<MetaArgsC>.TypeInfo);
    }

    [Fact]
    public void Conflicting_descriptor_throws()
    {
        JobMetadataRegistry.Register<ConflictArgs>(new JobDescriptor("meta.d", "q", 3, 0, null), null);
        var e = Assert.Throws<InvalidOperationException>(() =>
            JobMetadataRegistry.Register<ConflictArgs>(new JobDescriptor("meta.d", "other", 3, 0, null), null));
        Assert.Contains(nameof(ConflictArgs), e.Message);
    }

    private sealed record ConflictArgs(int X);

    [Fact]
    public void Conflicting_timeout_throws()
    {
        JobMetadataRegistry.Register<TimeoutConflictArgs>(new JobDescriptor("meta.e", "q", 3, 0, null) { TimeoutSeconds = 10 }, null);
        JobMetadataRegistry.Register<TimeoutConflictArgs>(new JobDescriptor("meta.e", "q", 3, 0, null) { TimeoutSeconds = 10 }, null);
        var e = Assert.Throws<InvalidOperationException>(() =>
            JobMetadataRegistry.Register<TimeoutConflictArgs>(new JobDescriptor("meta.e", "q", 3, 0, null), null));
        Assert.Contains(nameof(TimeoutConflictArgs), e.Message);
    }

    private sealed record TimeoutConflictArgs(int X);
}
