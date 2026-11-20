using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres.Jobs;

namespace PgBolt.Postgres.Tests;

public sealed record RegistryArgs(string Text, int Count);

public sealed class RegistryProbe
{
    public List<RegistryArgs> Seen { get; } = [];
}

public sealed class RegistryJob(RegistryProbe probe) : IJob<RegistryArgs>
{
    public Task ExecuteAsync(RegistryArgs args, JobContext context, CancellationToken cancellationToken)
    {
        probe.Seen.Add(args);
        return Task.CompletedTask;
    }
}

public sealed class OtherRegistryJob : IJob<RegistryArgs>
{
    public Task ExecuteAsync(RegistryArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed record ThrowingArgs(string Mode);

/// <summary>Исполнитель, который сам бросает — его исключения invoker не оборачивает.</summary>
public sealed class ThrowingRegistryJob : IJob<ThrowingArgs>
{
    public Task ExecuteAsync(ThrowingArgs args, JobContext context, CancellationToken cancellationToken) =>
        args.Mode == "json"
            ? throw new JsonException("job's own json error")
            : throw new InvalidOperationException("job failure");
}

[JsonConverter(typeof(UnsupportedArgsConverter))]
public sealed record UnsupportedArgs(int X);

/// <summary>Конвертер, имитирующий NotSupportedException от System.Text.Json при чтении.</summary>
public sealed class UnsupportedArgsConverter : JsonConverter<UnsupportedArgs>
{
    public override UnsupportedArgs Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        throw new NotSupportedException("UnsupportedArgs cannot be deserialized.");

    public override void Write(Utf8JsonWriter writer, UnsupportedArgs value, JsonSerializerOptions options) =>
        throw new NotSupportedException("UnsupportedArgs cannot be serialized.");
}

public sealed class UnsupportedRegistryJob : IJob<UnsupportedArgs>
{
    public Task ExecuteAsync(UnsupportedArgs args, JobContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

[JsonSerializable(typeof(RegistryArgs))]
[JsonSerializable(typeof(ThrowingArgs))]
[JsonSerializable(typeof(UnsupportedArgs))]
internal sealed partial class RegistryJson : JsonSerializerContext;

public sealed class JobRegistryTests
{
    private static readonly JobDescriptor Descriptor = new("registry.job", "reg", 4, 1, [3]);

    private static (ServiceCollection Services, JobRegistry Registry, JobsBuilder Builder) Create()
    {
        var services = new ServiceCollection();
        var registry = new JobRegistry();
        return (services, registry, new JobsBuilder(services, registry));
    }

    [Fact]
    public async Task Registered_invoker_deserializes_and_runs_scoped_job()
    {
        var (services, registry, builder) = Create();
        services.AddSingleton<RegistryProbe>();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        await using var sp = services.BuildServiceProvider();

        Assert.True(registry.TryGet("registry.job", out var reg));
        Assert.Equal((typeof(RegistryArgs), typeof(RegistryJob)), (reg.ArgsType, reg.JobType));
        await using var scope = sp.CreateAsyncScope();
        var ctx = new JobContext(JobId.New(), "registry.job", 1, 4, null, null, NullLogger.Instance);
        await reg.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes("""{"Text":"hi","Count":2}"""), ctx, CancellationToken.None);

        Assert.Equal(new[] { new RegistryArgs("hi", 2) }, sp.GetRequiredService<RegistryProbe>().Seen);
        Assert.Contains(services, d => d.ServiceType == typeof(RegistryJob) && d.Lifetime == ServiceLifetime.Scoped);
    }

    [Fact]
    public void Repeated_registration_is_idempotent()
    {
        var (services, registry, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        Assert.Single(registry.All);
        Assert.Single(services, d => d.ServiceType == typeof(RegistryJob));
    }

    [Fact]
    public void Conflicting_executor_for_same_name_throws()
    {
        var (_, _, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        Assert.Throws<InvalidOperationException>(() =>
            builder.AddExecutor<RegistryArgs, OtherRegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs));
    }

    [Fact]
    public void Registration_fills_job_metadata()
    {
        var (_, _, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        Assert.True(JobMetadata<RegistryArgs>.Descriptor!.SameAs(Descriptor));
        Assert.NotNull(JobMetadata<RegistryArgs>.TypeInfo);
    }

    [Fact]
    public void Job_type_records_carry_json_schema_and_defaults()
    {
        var (_, registry, builder) = Create();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);

        var record = Assert.Single(registry.ToJobTypeRecords());
        Assert.Equal(("registry.job", "reg", 4, (short)1), (record.Name, record.DefaultQueue, record.DefaultMaxAttempts, record.DefaultPriority));
        Assert.Equal(new[] { 3 }, record.DefaultRetryDelaysSeconds);
        using var schema = JsonDocument.Parse(record.ArgsSchemaJson);
        Assert.True(schema.RootElement.GetProperty("properties").TryGetProperty("Text", out _));
    }

    private static JobContext Ctx(string name) => new(JobId.New(), name, 1, 4, null, null, NullLogger.Instance);

    [Fact]
    public async Task Null_arguments_throw_job_arguments_exception()
    {
        var (services, registry, builder) = Create();
        services.AddSingleton<RegistryProbe>();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        await using var sp = services.BuildServiceProvider();
        registry.TryGet("registry.job", out var reg);

        var e = await Assert.ThrowsAsync<JobArgumentsException>(() => reg!.Invoke(sp, "null"u8.ToArray(), Ctx("registry.job"), CancellationToken.None));
        Assert.Contains("registry.job", e.Message);
        Assert.Empty(sp.GetRequiredService<RegistryProbe>().Seen);
    }

    [Theory]
    [InlineData("""{"Text":""")]                  // обрезанный JSON
    [InlineData("""not json""")]                  // не JSON
    [InlineData("""{"Text":1,"Count":2}""")]      // несовпадение типа поля
    public async Task Malformed_arguments_throw_job_arguments_exception_with_json_inner(string json)
    {
        var (services, registry, builder) = Create();
        services.AddSingleton<RegistryProbe>();
        builder.AddExecutor<RegistryArgs, RegistryJob>(Descriptor, RegistryJson.Default.RegistryArgs);
        await using var sp = services.BuildServiceProvider();
        registry.TryGet("registry.job", out var reg);

        var e = await Assert.ThrowsAsync<JobArgumentsException>(() => reg!.Invoke(sp, Encoding.UTF8.GetBytes(json), Ctx("registry.job"), CancellationToken.None));
        Assert.IsType<JsonException>(e.InnerException, exactMatch: false);
        Assert.Contains("registry.job", e.Message);
        Assert.Empty(sp.GetRequiredService<RegistryProbe>().Seen);
    }

    [Fact]
    public async Task Not_supported_during_deserialization_throws_job_arguments_exception()
    {
        var (services, registry, builder) = Create();
        builder.AddExecutor<UnsupportedArgs, UnsupportedRegistryJob>(
            new JobDescriptor("registry.unsupported", "reg", 4, 1, null), RegistryJson.Default.UnsupportedArgs);
        await using var sp = services.BuildServiceProvider();
        registry.TryGet("registry.unsupported", out var reg);

        var e = await Assert.ThrowsAsync<JobArgumentsException>(() => reg!.Invoke(sp, """{"X":1}"""u8.ToArray(), Ctx("registry.unsupported"), CancellationToken.None));
        Assert.IsType<NotSupportedException>(e.InnerException, exactMatch: false);
        Assert.Contains("registry.unsupported", e.Message);
    }

    [Theory]
    [InlineData("json", typeof(JsonException))]
    [InlineData("other", typeof(InvalidOperationException))]
    public async Task Exceptions_of_the_job_itself_are_not_wrapped(string mode, Type expected)
    {
        var (services, registry, builder) = Create();
        builder.AddExecutor<ThrowingArgs, ThrowingRegistryJob>(
            new JobDescriptor("registry.throwing", "reg", 4, 1, null), RegistryJson.Default.ThrowingArgs);
        await using var sp = services.BuildServiceProvider();
        await using var scope = sp.CreateAsyncScope();
        registry.TryGet("registry.throwing", out var reg);

        var e = await Record.ExceptionAsync(() =>
            reg!.Invoke(scope.ServiceProvider, Encoding.UTF8.GetBytes($$"""{"Mode":"{{mode}}"}"""), Ctx("registry.throwing"), CancellationToken.None));
        Assert.NotNull(e);
        Assert.IsType(expected, e);
    }
}
