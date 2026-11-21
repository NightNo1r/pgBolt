using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;

namespace PgBolt.LoadTest.Tests.Cli;

public sealed class PostgresCommandTests
{
    private static LoadTestOptions Vacuum(params string[] args) =>
        OptionsParser.Parse(["vacuum", .. args]).Options!;

    private static string[] Pairs(IReadOnlyList<string> command)
    {
        Assert.All(command.Where((_, i) => i % 2 == 0), flag => Assert.Equal("-c", flag));
        return command.Where((_, i) => i % 2 == 1).ToArray();
    }

    [Fact]
    public void Defaults_are_dash_c_pairs()
    {
        var pairs = Pairs(PostgresCommand.Build(Vacuum(), trackPlanning: false));

        Assert.Equal(
            [
                "shared_buffers=1GB",
                "max_wal_size=4GB",
                "shared_preload_libraries=pg_stat_statements",
                "track_io_timing=on",
                "pg_stat_statements.track_planning=off",
            ],
            pairs);
    }

    [Fact]
    public void Track_planning_is_on_only_when_requested()
    {
        Assert.Contains("pg_stat_statements.track_planning=on", Pairs(PostgresCommand.Build(Vacuum(), trackPlanning: true)));
        Assert.DoesNotContain("pg_stat_statements.track_planning=on", Pairs(PostgresCommand.Build(Vacuum(), trackPlanning: false)));
    }

    [Theory]
    [InlineData("sustained")]
    [InlineData("vacuum")]
    [InlineData("partitions")]
    public void StockUsesServerDefaults(string scenario)
    {
        var parsed = OptionsParser.Parse([scenario, "--pg-profile", "stock"]);
        Assert.Null(parsed.Error);

        // PostgreSQL выбирает параметры производительности, стенд включает только диагностику.
        var settings = Pairs(PostgresCommand.Build(parsed.Options!, trackPlanning: true));
        Assert.Equal(["shared_preload_libraries=pg_stat_statements", "track_io_timing=on",
            "pg_stat_statements.track_planning=on", "pg_stat_statements.track=all"], settings);
    }

    [Fact]
    public void BenchMatchesOriginalDefaults()
    {
        var parsed = OptionsParser.Parse(["vacuum", "--pg-profile", "bench"]);
        Assert.Null(parsed.Error);
        Assert.Equal(PostgresCommand.Build(Vacuum(), false), PostgresCommand.Build(parsed.Options!, false));
    }

    [Fact]
    public void Pg_setting_becomes_dash_c_pair_and_overrides_default()
    {
        var command = PostgresCommand.Build(
            Vacuum("--pg-setting", "autovacuum_naptime=10s,max_wal_size=2GB"), trackPlanning: false);
        var pairs = Pairs(command);

        Assert.Contains("autovacuum_naptime=10s", pairs);
        Assert.Single(pairs, p => p.StartsWith("max_wal_size=", StringComparison.Ordinal));
        Assert.Contains("max_wal_size=2GB", pairs);
        Assert.DoesNotContain("max_wal_size=4GB", pairs);
    }
}
