using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests.Env;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class EnvironmentInfoTests(PostgresFixture pg)
{
    [Fact]
    public async Task Collects_versions_and_settings()
    {
        var options = OptionsParser.Parse(["vacuum", "--pg-setting", "autovacuum_naptime=10s"]).Options!;

        var info = (await EnvironmentInfo.CollectAsync(pg.DirectConnectionString, console: null, options, "public",
            TestContext.Current.CancellationToken)).ToDictionary(p => p.Key, p => p.Value);

        Assert.StartsWith("17", info["PostgreSQL"]);
        Assert.Equal(EnvironmentInfo.NoData, info["PgBouncer"]);
        Assert.Equal("400", info["max_connections"]);
        Assert.Equal("bench", info["--pg-profile"]);
        Assert.Equal("autovacuum_naptime=10s", info["--pg-setting"]);
        Assert.Equal(Environment.Version.ToString(), info[".NET"]);
        Assert.Equal(EnvironmentInfo.NoData, info["reloptions queue"]);
    }

    [Fact]
    public async Task ReportsExternalSettings()
    {
        var options = OptionsParser.Parse(["sustained", "--connection", pg.DirectConnectionString]).Options!;
        var info = (await EnvironmentInfo.CollectAsync(pg.DirectConnectionString, null, options, "public",
            TestContext.Current.CancellationToken)).ToDictionary(p => p.Key, p => p.Value);

        Assert.Equal("external", info["--pg-profile"]);
        foreach (var key in new[] { "fsync", "full_page_writes", "synchronous_commit", "track_io_timing",
                     "shared_preload_libraries" })
        {
            Assert.Equal(await LoadEnvironmentTests.ScalarAsync<string>(pg.DirectConnectionString, $"SHOW {key}"), info[key]);
        }
    }
}
