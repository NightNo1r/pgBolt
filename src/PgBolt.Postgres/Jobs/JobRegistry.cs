using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Schema;
using System.Text.Json.Serialization.Metadata;
using PgBolt.Postgres.Storage;

namespace PgBolt.Postgres.Jobs;

public delegate Task JobInvoker(IServiceProvider services, ReadOnlyMemory<byte> argsUtf8, JobContext context, CancellationToken ct);

public sealed record JobExecutorRegistration(
    JobDescriptor Descriptor, Type ArgsType, Type JobType, JsonTypeInfo ArgsTypeInfo, JobInvoker Invoke);

/// <summary>Исполнители, зарегистрированные сгенерированными PgBoltJobRegistry.Register (§7).</summary>
public sealed class JobRegistry
{
    private readonly Dictionary<string, JobExecutorRegistration> _byName = new(StringComparer.Ordinal);

    public IReadOnlyCollection<JobExecutorRegistration> All => _byName.Values;

    public bool TryGet(string name, [MaybeNullWhen(false)] out JobExecutorRegistration registration) =>
        _byName.TryGetValue(name, out registration);

    /// <summary>Записи для job_types: схема аргументов строится JsonSchemaExporter из JsonTypeInfo (§7).</summary>
    public IReadOnlyList<JobTypeRecord> ToJobTypeRecords() =>
        _byName.Values
            .OrderBy(r => r.Descriptor.Name, StringComparer.Ordinal)
            .Select(r => new JobTypeRecord(
                r.Descriptor.Name,
                JsonSchemaExporter.GetJsonSchemaAsNode(r.ArgsTypeInfo).ToJsonString(),
                r.Descriptor.Queue,
                r.Descriptor.MaxAttempts,
                r.Descriptor.Priority,
                r.Descriptor.RetryDelaysSeconds))
            .ToList();

    /// <returns>true — добавлено; false — такая же регистрация уже есть.</returns>
    internal bool TryAdd(JobExecutorRegistration registration)
    {
        if (!_byName.TryGetValue(registration.Descriptor.Name, out var existing))
        {
            _byName.Add(registration.Descriptor.Name, registration);
            return true;
        }
        if (existing.ArgsType == registration.ArgsType && existing.JobType == registration.JobType
            && existing.Descriptor.SameAs(registration.Descriptor))
            return false;
        throw new InvalidOperationException(
            $"Job '{registration.Descriptor.Name}' is already registered with executor {existing.JobType.FullName}; " +
            $"cannot register {registration.JobType.FullName}.");
    }
}
