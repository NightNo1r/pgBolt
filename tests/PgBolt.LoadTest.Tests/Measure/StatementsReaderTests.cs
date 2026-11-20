using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Tests.Env;
using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests.Measure;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class StatementsReaderTests(PostgresFixture pg)
{
    [Fact]
    public async Task Statements_reader_without_extension_returns_no_data()
    {
        var reader = new StatementsReader(pg.DirectConnectionString);

        await reader.ResetAsync();
        var summary = await reader.ReadAsync();

        Assert.Null(summary.Classes);
    }

    [Fact]
    public async Task Without_track_planning_only_plan_time_has_no_data()
    {
        var ct = TestContext.Current.CancellationToken;
        var options = OptionsParser.Parse(["sustained"]).Options!;
        await using var env = await LoadEnvironment.StartAsync(options, trackPlanning: false, ct);
        var reader = new StatementsReader(env.DirectConnectionString);

        await reader.ResetAsync();
        // Псевдоним acked — фрагмент класса ack.
        await LoadEnvironmentTests.ScalarAsync<int>(env.DirectConnectionString, "SELECT 1 AS acked");
        var summary = await reader.ReadAsync();

        var ack = summary.Classes!["ack"];
        Assert.Equal(1, ack.Calls);
        Assert.True(ack.MeanExecTime > 0);
        Assert.Equal(ack.MeanExecTime, ack.TotalExecTime);
        Assert.Null(ack.MeanPlanTime);
        Assert.Null(ack.TotalPlanTime);
    }

    [Fact]
    public async Task Reset_without_execute_privilege_gives_no_data()
    {
        await using var db = await TestDb.CreateInOwnDatabaseAsync(pg);
        var role = "r_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = db.Admin.CreateCommand(
            $"CREATE EXTENSION pg_stat_statements; CREATE ROLE {role} LOGIN PASSWORD 'p'"))
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        try
        {
            var asRole = new NpgsqlConnectionStringBuilder(db.ConnectionString)
            {
                Username = role, Password = "p", Pooling = false,
            }.ConnectionString;
            var reader = new StatementsReader(asRole);

            await reader.ResetAsync();
            var summary = await reader.ReadAsync();

            Assert.Null(summary.Classes);
        }
        finally
        {
            await using var cmd = db.Admin.CreateCommand($"DROP ROLE {role}");
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }
}
