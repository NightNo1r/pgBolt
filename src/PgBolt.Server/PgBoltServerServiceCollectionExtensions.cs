using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Storage;
using PgBolt.Server;
using PgBolt.Server.Runtime;

namespace Microsoft.Extensions.DependencyInjection;

public static class PgBoltServerServiceCollectionExtensions
{
    /// <summary>Добавляет виртуальный сервер (§3.1). Требует AddPgBolt; первый вызов регистрирует единственный hosted service.</summary>
    public static IServiceCollection AddPgBoltServer(this IServiceCollection services, string name, Action<PgBoltServerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);
        if (!services.Any(d => d.ServiceType == typeof(PgBoltOptions)))
            throw new InvalidOperationException("AddPgBoltServer requires PgBolt core: call services.AddPgBolt(...) first.");
        if (!IsValidName(name))
            throw new ArgumentException(
                $"PgBolt server name '{name}' is invalid: use 1-64 characters from [A-Za-z0-9._-].", nameof(name));

        var existing = services
            .Where(d => d.ServiceType == typeof(ServerDefinition) && !d.IsKeyedService)
            .Select(d => d.ImplementationInstance)
            .OfType<ServerDefinition>()
            .ToList();
        if (existing.Any(s => s.Name == name))
            throw new InvalidOperationException($"PgBolt server '{name}' is already registered.");

        var options = new PgBoltServerOptions();
        configure(options);
        options.Validate(name);
        services.AddSingleton(new ServerDefinition(name, options));
        foreach (var register in options.FilterRegistrations)
            register(services);

        if (existing.Count == 0)
        {
            services.TryAddSingleton(sp => new MonoClock(sp.GetRequiredService<TimeProvider>()));
            services.TryAddSingleton<IServerStore>(sp => new PgBoltServerStore(sp.GetRequiredService<PgBoltStore>()));
            services.TryAddSingleton(new PgBoltProcessingOptions());
            services.TryAddSingleton(sp => new QueueSettingsCache(sp.GetRequiredService<IServerStore>(),
                sp.GetRequiredService<PgBoltProcessingOptions>(), sp.GetRequiredService<MonoClock>(),
                (sp.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance).CreateLogger("PgBolt.Processing"),
                sp.GetRequiredService<PgBoltMetrics>()));
            services.TryAddSingleton<IQueueSettingsRefresher>(sp => sp.GetRequiredService<QueueSettingsCache>());
            services.TryAddSingleton(sp => new ClusterQueueStats(sp.GetRequiredService<MonoClock>(), sp.GetRequiredService<PgBoltProcessingOptions>()));
            services.TryAddSingleton(sp => new ClusterTableStats(sp.GetRequiredService<MonoClock>(), sp.GetRequiredService<PgBoltProcessingOptions>()));
            services.TryAddSingleton(sp => new PgBoltMetrics(sp.GetRequiredService<PgBoltInstrumentation>(), sp.GetRequiredService<ClusterQueueStats>(),
                sp.GetRequiredService<ClusterTableStats>()));
            services.AddHostedService(sp => new PgBoltProcessingService(sp));
        }
        return services;
    }

    /// <summary>Глобальный фильтр для всех серверов процесса; выполняется раньше фильтров сервера (§3.6).</summary>
    public static IServiceCollection AddPgBoltJobFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(
        this IServiceCollection services) where T : class, IJobFilter
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton(new GlobalJobFilter(typeof(T)));
        services.TryAddScoped<T>();
        return services;
    }

    /// <summary>Общие настройки обработки процесса (§3.1); порядок относительно AddPgBoltServer не важен.</summary>
    public static IServiceCollection ConfigurePgBoltProcessing(this IServiceCollection services, Action<PgBoltProcessingOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        var options = services
            .Where(d => d.ServiceType == typeof(PgBoltProcessingOptions) && !d.IsKeyedService)
            .Select(d => d.ImplementationInstance)
            .OfType<PgBoltProcessingOptions>()
            .FirstOrDefault();
        if (options is null)
        {
            options = new PgBoltProcessingOptions();
            services.AddSingleton(options);
        }
        configure(options);
        options.Validate();
        return services;
    }

    private static bool IsValidName(string name)
    {
        if (name.Length is < 1 or > 64)
            return false;
        foreach (var c in name)
            if (!char.IsAsciiLetterOrDigit(c) && c is not ('.' or '_' or '-'))
                return false;
        return true;
    }
}
