using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class GlobalProbeFilter : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
}

public sealed class ServerProbeFilter : IJobFilter
{
    public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
}

public sealed class ServerRegistrationTests
{
    private static ServiceCollection WithCore()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IServerStore>(new FakeServerStore());
        services.AddPgBolt(o => o.ConnectionString = "Host=unused");
        return services;
    }

    [Fact]
    public async Task Several_servers_share_one_hosted_service()
    {
        var services = WithCore();
        services.AddPgBoltServer("a", s => s.Queue("qa"));
        services.AddPgBoltServer("b", s => s.Queue("qb"));
        await using var sp = services.BuildServiceProvider();

        Assert.Single(sp.GetServices<IHostedService>().OfType<PgBoltProcessingService>());
        Assert.Equal(["a", "b"], sp.GetServices<ServerDefinition>().Select(d => d.Name));
        Assert.IsType<FakeServerStore>(sp.GetRequiredService<IServerStore>());   // регистрация до AddPgBoltServer сохраняется
        Assert.NotNull(sp.GetRequiredService<MonoClock>());
        Assert.NotNull(sp.GetRequiredService<PgBoltProcessingOptions>());
        Assert.Same(sp.GetRequiredService<ClusterQueueStats>(), sp.GetRequiredService<ClusterQueueStats>());
        Assert.Same(sp.GetRequiredService<ClusterTableStats>(), sp.GetRequiredService<ClusterTableStats>());
        Assert.Same(sp.GetRequiredService<PgBoltMetrics>(), sp.GetRequiredService<PgBoltMetrics>());
    }

    [Fact]
    public void Server_without_core_registration_throws()
    {
        var e = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddPgBoltServer("a", s => s.Queue("q")));
        Assert.Contains("AddPgBolt", e.Message);
    }

    [Fact]
    public void Duplicate_server_name_throws()
    {
        var services = WithCore();
        services.AddPgBoltServer("orders", s => s.Queue("q"));

        var e = Assert.Throws<InvalidOperationException>(() => services.AddPgBoltServer("orders", s => s.Queue("q2")));
        Assert.Contains("orders", e.Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("has space")]
    [InlineData("имя")]
    [InlineData("a/b")]
    public void Invalid_server_name_throws(string name)
    {
        Assert.Throws<ArgumentException>(() => WithCore().AddPgBoltServer(name, s => s.Queue("q")));
    }

    [Fact]
    public void Name_longer_than_64_chars_throws_and_64_is_accepted()
    {
        WithCore().AddPgBoltServer(new string('a', 64), s => s.Queue("q"));
        Assert.Throws<ArgumentException>(() => WithCore().AddPgBoltServer(new string('a', 65), s => s.Queue("q")));
    }

    [Fact]
    public void Invalid_options_are_rejected_at_registration()
    {
        var e = Assert.Throws<InvalidOperationException>(() => WithCore().AddPgBoltServer("empty", _ => { }));
        Assert.Contains("empty", e.Message);
    }

    [Fact]
    public void Filters_are_registered_as_scoped_and_globals_are_listed_in_order()
    {
        var services = WithCore();
        services.AddPgBoltJobFilter<GlobalProbeFilter>();
        services.AddPgBoltServer("a", s => s.Queue("q").AddFilter<ServerProbeFilter>());

        Assert.Contains(services, d => d.ServiceType == typeof(GlobalProbeFilter) && d.Lifetime == ServiceLifetime.Scoped);
        Assert.Contains(services, d => d.ServiceType == typeof(ServerProbeFilter) && d.Lifetime == ServiceLifetime.Scoped);
        var global = Assert.Single(services, d => d.ServiceType == typeof(GlobalJobFilter));
        Assert.Equal(typeof(GlobalProbeFilter), ((GlobalJobFilter)global.ImplementationInstance!).FilterType);
    }

    [Fact]
    public async Task Processing_options_can_be_configured_before_or_after_servers()
    {
        var before = WithCore();
        before.ConfigurePgBoltProcessing(p => p.PromoterInterval = TimeSpan.FromSeconds(3));
        before.AddPgBoltServer("a", s => s.Queue("q"));
        var after = WithCore();
        after.AddPgBoltServer("a", s => s.Queue("q"));
        after.ConfigurePgBoltProcessing(p => p.RunMaintenance = false);

        await using var sp1 = before.BuildServiceProvider();
        await using var sp2 = after.BuildServiceProvider();

        Assert.Equal(TimeSpan.FromSeconds(3), sp1.GetRequiredService<PgBoltProcessingOptions>().PromoterInterval);
        Assert.False(sp2.GetRequiredService<PgBoltProcessingOptions>().RunMaintenance);
        Assert.Single(after, d => d.ServiceType == typeof(PgBoltProcessingOptions));
    }

    [Fact]
    public void Invalid_processing_options_throw()
    {
        Assert.Throws<InvalidOperationException>(() =>
            WithCore().ConfigurePgBoltProcessing(p => p.SweeperInterval = TimeSpan.Zero));
    }

    [Theory]
    [InlineData(nameof(PgBoltProcessingOptions.StatsFlushInterval), 0)]
    [InlineData(nameof(PgBoltProcessingOptions.StatsFlushInterval), 2 * 24 * 3600)]
    [InlineData(nameof(PgBoltProcessingOptions.StatsSnapshotInterval), 0)]
    [InlineData(nameof(PgBoltProcessingOptions.StatsRetention), 0)]
    [InlineData(nameof(PgBoltProcessingOptions.StatsRetention), 3651 * 24 * 3600)]
    public void Invalid_stats_options_throw(string option, int seconds)
    {
        Assert.Throws<InvalidOperationException>(() => WithCore().ConfigurePgBoltProcessing(p =>
            typeof(PgBoltProcessingOptions).GetProperty(option)!.SetValue(p, TimeSpan.FromSeconds(seconds))));
    }
}
