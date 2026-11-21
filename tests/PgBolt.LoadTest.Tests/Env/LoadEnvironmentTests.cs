using Npgsql;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Env;

namespace PgBolt.LoadTest.Tests.Env;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class LoadEnvironmentTests
{
    internal static async Task<T> ScalarAsync<T>(string connectionString, string sql)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        return (T)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    [Theory]
    [InlineData("bench", 2)]
    [InlineData("stock", 0)]
    public async Task OwnContainersApplyProfile(string profile, int overrides)
    {
        var ct = TestContext.Current.CancellationToken;
        var options = OptionsParser.Parse(["sustained", "--pg-profile", profile]).Options!;
        await using var env = await LoadEnvironment.StartAsync(options, trackPlanning: false, ct);

        Assert.Equal(1, await ScalarAsync<int>(env.DirectConnectionString, "SELECT 1"));
        Assert.Equal(1, await ScalarAsync<int>(env.BouncerConnectionString!, "SELECT 1"));
        // Источник настройки показывает, кто выбрал размер памяти и WAL именно в этом контейнере.
        Assert.Equal(overrides, await ScalarAsync<int>(env.DirectConnectionString, """
            SELECT count(*)::int FROM pg_settings
            WHERE name IN ('shared_buffers', 'max_wal_size') AND source = 'command line'
            """));
        Assert.NotEmpty(await env.Console!.ShowPoolsAsync(ct));
        Assert.Equal(200, await env.Console.MaxPreparedStatementsAsync(ct));
        Assert.Equal("public", await ScalarAsync<string>(env.DirectConnectionString,
            "SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pg_stat_statements'"));
        // Образ Testcontainers по умолчанию выключает fsync и synchronous_commit; стенду нужны рабочие значения.
        Assert.Equal("on", await ScalarAsync<string>(env.DirectConnectionString, "SHOW fsync"));
        Assert.Equal("on", await ScalarAsync<string>(env.DirectConnectionString, "SHOW full_page_writes"));
        Assert.Equal("on", await ScalarAsync<string>(env.DirectConnectionString, "SHOW synchronous_commit"));
    }

    [Fact]
    public async Task Schema_is_created_and_dropped()
    {
        var ct = TestContext.Current.CancellationToken;
        var options = OptionsParser.Parse(["sustained"]).Options!;
        await using var env = await LoadEnvironment.StartAsync(options, trackPlanning: false, ct);
        const string exists = "SELECT count(*) FROM pg_namespace WHERE nspname = '{0}'";

        var schema = await env.CreateSchemaNameAsync();
        Assert.NotEqual(schema, await env.CreateSchemaNameAsync());
        Assert.Equal(1L, await ScalarAsync<long>(env.DirectConnectionString, string.Format(exists, schema)));

        await env.DropSchemaAsync(schema);
        Assert.Equal(0L, await ScalarAsync<long>(env.DirectConnectionString, string.Format(exists, schema)));
    }
}
