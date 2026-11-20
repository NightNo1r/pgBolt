using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Tests.Support;
using PgBolt.Dashboard.Views;
using PgBolt.Testing;

namespace PgBolt.Dashboard.Tests.Data;

/// <summary>Prepared-режим — только напрямую (PgBouncer тестов: max_prepared_statements = 0).</summary>
public sealed class OverviewStorePreparedTests(PostgresFixture pg) : DashboardTestBase(pg, ConnectionMode.Direct)
{
    private static IEnumerable<ThroughputPoint> WithData(IEnumerable<ThroughputPoint> points) =>
        points.Where(p => p.Succeeded != 0 || p.Failed != 0 || p.Retried != 0);

    [Fact]
    public async Task Prepared_statements_return_same_results_as_unprepared()
    {
        await OverviewStoreTests.SeedOverviewAsync(Db, await OverviewStoreTests.DbMinuteAsync(Db));
        await OverviewStoreTests.SeedMaintenanceAsync(Db);
        await PublishTypeAsync("a.job");
        await PublishTypeAsync("b.job", "q2");
        var prepared = new DashboardStore(Db.DataSource, Db.Schema, usePreparedStatements: true, new PgBoltDashboardOptions(), Time);

        for (var i = 0; i < 2; i++)
        {
            var overview = await prepared.GetOverviewAsync(Ct);
            var expectedOverview = await Store.GetOverviewAsync(Ct);
            Assert.Equivalent(expectedOverview.Totals, overview.Totals);
            Assert.Equal(expectedOverview.LiveServers, overview.LiveServers);
            Assert.Equal(expectedOverview.Freshness, overview.Freshness);
            Assert.Equal(WithData(expectedOverview.LastHour), WithData(overview.LastHour));

            Assert.Equal(WithData(await Store.GetThroughputDayAsync(Ct)), WithData(await prepared.GetThroughputDayAsync(Ct)));
            Assert.Equivalent(await Store.ListQueuesAsync(Ct), await prepared.ListQueuesAsync(Ct), strict: true);
            Assert.Equivalent(await Store.GetServersAsync(Ct), await prepared.GetServersAsync(Ct), strict: true);
            Assert.Equivalent(await Store.ListJobTypesAsync(Ct), await prepared.ListJobTypesAsync(Ct), strict: true);
            Assert.Equivalent(await Store.GetJobTypeAsync("b.job", Ct), await prepared.GetJobTypeAsync("b.job", Ct), strict: true);
        }
    }
}
