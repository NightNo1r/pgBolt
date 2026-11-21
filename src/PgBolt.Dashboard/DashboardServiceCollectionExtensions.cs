using Microsoft.Extensions.DependencyInjection.Extensions;
using PgBolt.Dashboard;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Endpoints;
using PgBolt.Postgres;

namespace Microsoft.Extensions.DependencyInjection;

public static class DashboardServiceCollectionExtensions
{
    /// <summary>Registers dashboard services. Register <c>AddPgBolt</c> before mapping the routes with <c>app.MapPgBoltDashboard()</c>.</summary>
    public static IServiceCollection AddPgBoltDashboard(this IServiceCollection services, Action<PgBoltDashboardOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(d => d.ServiceType == typeof(PgBoltDashboardOptions)))
            throw new InvalidOperationException("AddPgBoltDashboard has already been called for this service collection.");

        var options = new PgBoltDashboardOptions();
        configure?.Invoke(options);
        options.Validate();

        services.AddSingleton(options);
        services.TryAddSingleton(DashboardAssets.Load());
        services.AddSingleton(sp =>
        {
            var core = sp.GetRequiredService<PgBoltOptions>();
            return new DashboardStore(sp.GetRequiredService<PgBoltDataSource>().Value, core.Schema, core.UsePreparedStatements, options,
                sp.GetRequiredService<TimeProvider>());
        });
        return services;
    }
}
