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

    [Fact]
    public async Task Own_containers_accept_direct_and_bouncer_connections()
    {
        var ct = TestContext.Current.CancellationToken;
        var options = OptionsParser.Parse(["sustained"]).Options!;
        await using var env = await LoadEnvironment.StartAsync(options, trackPlanning: false, ct);

        Assert.Equal(1, await ScalarAsync<int>(env.DirectConnectionString, "SELECT 1"));
        Assert.Equal(1, await ScalarAsync<int>(env.BouncerConnectionString!, "SELECT 1"));
        Assert.NotEmpty(await env.Console!.ShowPoolsAsync(ct));
        Assert.Equal(200, await env.Console.MaxPreparedStatementsAsync(ct));
        Assert.Equal("public", await ScalarAsync<string>(env.DirectConnectionString,
            "SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pg_stat_statements'"));
        // Образ Testcontainers по умолчанию выключает fsync и synchronous_commit; стенду нужны рабочие значения.
        Assert.Equal("on", await ScalarAsync<string>(env.DirectConnectionString, "SHOW fsync"));
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
