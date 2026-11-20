using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt;
using PgBolt.Postgres;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Diagnostics;
using PgBolt.Postgres.Hosting;
using PgBolt.Postgres.Jobs;
using PgBolt.Postgres.Recurring;
using PgBolt.Postgres.Schema;
using PgBolt.Postgres.Storage;

namespace Microsoft.Extensions.DependencyInjection;

public static class PgBoltServiceCollectionExtensions
{
    public static IServiceCollection AddPgBolt(this IServiceCollection services, Action<PgBoltOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(d => d.ServiceType == typeof(PgBoltOptions)))
            throw new InvalidOperationException("AddPgBolt has already been called for this service collection.");

        var options = new PgBoltOptions();
        configure(options);
        options.Validate();

        var registry = new JobRegistry();
        var builder = new JobsBuilder(services, registry);
        foreach (var register in options.JobRegistrations)
            register(builder);

        services.AddSingleton(options);
        services.AddSingleton(registry);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton(sp => new PgBoltInstrumentation(sp.GetService<IMeterFactory>(), sp.GetRequiredService<TimeProvider>(),
            sp.GetService<ILoggerFactory>()));
        services.TryAddSingleton(sp => new PgBoltClientMetrics(sp.GetRequiredService<PgBoltInstrumentation>()));
        services.AddSingleton(_ => options.DataSource is { } ds
            ? new PgBoltDataSource(ds, owned: false)
            : new PgBoltDataSource(PgBoltDataSourceFactory.Create(options.ConnectionString!, options.UsePreparedStatements), owned: true));
        services.TryAddSingleton<ICompletionSignal>(_ => new InProcessCompletionSignal());
        services.AddSingleton(sp => new PgBoltStore(sp.GetRequiredService<PgBoltDataSource>().Value, options.Schema, options.UsePreparedStatements)
        {
            Completions = sp.GetRequiredService<ICompletionSignal>(),
        });
        services.AddSingleton(sp => new MigrationRunner(sp.GetRequiredService<PgBoltDataSource>().Value, options.Schema));
        services.AddSingleton(sp => new PartitionManager(sp.GetRequiredService<PgBoltDataSource>().Value, options.Schema,
            options.JobsFillFactor, options.PartitionLockTimeout));
        services.AddSingleton(sp => new RetentionRunner(sp.GetRequiredService<PgBoltDataSource>().Value, options, RetentionTuning.Default));
        services.AddSingleton<IPgBoltMaintenance>(sp => new PgBoltMaintenance(options,
            sp.GetRequiredService<MigrationRunner>(), sp.GetRequiredService<PartitionManager>(), sp.GetRequiredService<PgBoltStore>(),
            sp.GetRequiredService<RetentionRunner>()));
        services.TryAddSingleton<IWakeupSignal>(_ => new InProcessWakeupSignal());
        services.AddSingleton(sp => new JobTypesCache(sp.GetRequiredService<PgBoltStore>(), options.JobTypesCacheTtl, sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IJobClient>(sp => new PgBoltJobClient(sp.GetRequiredService<PgBoltStore>(),
            sp.GetRequiredService<JobTypesCache>(), sp.GetRequiredService<IWakeupSignal>(), sp.GetRequiredService<PgBoltInstrumentation>(),
            sp.GetRequiredService<PgBoltClientMetrics>()));
        services.AddSingleton<IRecurringJobManager>(sp => new RecurringJobManager(sp.GetRequiredService<PgBoltStore>(),
            sp.GetRequiredService<JobTypesCache>(), sp.GetRequiredService<IWakeupSignal>(), sp.GetRequiredService<PgBoltInstrumentation>(),
            sp.GetRequiredService<PgBoltClientMetrics>()));
        services.AddSingleton<IQueueSettingsManager>(sp => new QueueSettingsManager(sp.GetRequiredService<PgBoltStore>(),
            new QueueSettingsReadCache(options.QueueSettingsCacheDuration, sp.GetRequiredService<TimeProvider>()), sp));
        services.AddSingleton(sp => new RecurringDeclarations(options, sp.GetService<IHostEnvironment>()));
        services.AddSingleton(sp => new PgBoltInitializer(options, sp.GetRequiredService<PgBoltDataSource>().Value,
            sp.GetRequiredService<MigrationRunner>(), sp.GetRequiredService<PartitionManager>(),
            sp.GetRequiredService<PgBoltStore>(), sp.GetRequiredService<RecurringDeclarations>(),
            sp.GetService<ILogger<PgBoltInitializer>>() ?? NullLogger<PgBoltInitializer>.Instance));
        services.AddHostedService(sp => sp.GetRequiredService<PgBoltInitializer>());
        return services;
    }
}
