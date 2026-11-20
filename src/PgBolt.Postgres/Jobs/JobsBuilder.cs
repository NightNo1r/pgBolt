using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace PgBolt.Postgres.Jobs;

internal sealed class JobsBuilder(IServiceCollection services, JobRegistry registry) : IPgBoltJobsBuilder
{
    public IPgBoltJobsBuilder AddExecutor<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TJob>(
        JobDescriptor descriptor, JsonTypeInfo<TArgs> argsTypeInfo)
        where TJob : class, IJob<TArgs>
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(argsTypeInfo);

        if (registry.TryAdd(new JobExecutorRegistration(descriptor, typeof(TArgs), typeof(TJob), argsTypeInfo, Invoke)))
            services.TryAddScoped<TJob>();
        JobMetadataRegistry.Register(descriptor, argsTypeInfo);
        return this;

        Task Invoke(IServiceProvider sp, ReadOnlyMemory<byte> args, JobContext context, CancellationToken ct)
        {
            TArgs value;
            try
            {
                value = JsonSerializer.Deserialize(args.Span, argsTypeInfo)
                        ?? throw new JobArgumentsException($"Arguments of job '{descriptor.Name}' deserialized to null.");
            }
            catch (Exception e) when (e is JsonException or NotSupportedException)
            {
                // Часть 3 §5.1 п. 6: ошибка аргументов — окончательный Failed, а не retry. Исключения самого
                // исполнителя сюда не попадают: он вызывается вне try.
                throw new JobArgumentsException($"Arguments of job '{descriptor.Name}' cannot be deserialized: {e.Message}", e);
            }
            return sp.GetRequiredService<TJob>().ExecuteAsync(value, context, ct);
        }
    }
}
