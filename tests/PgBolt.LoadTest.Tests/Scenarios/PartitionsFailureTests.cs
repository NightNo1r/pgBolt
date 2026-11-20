using Npgsql;
using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests.Scenarios;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class PartitionsFailureTests(PostgresFixture pg) : IDisposable
{
    private readonly string _out = Path.Combine(Path.GetTempPath(), "pgbolt-partitions-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_out)) Directory.Delete(_out, recursive: true);
    }

    [Fact]
    public async Task Schema_creation_failure_does_not_stop_matrix()
    {
        var ct = TestContext.Current.CancellationToken;
        var role = "r_" + Guid.NewGuid().ToString("N")[..12];
        await ExecuteAsync($"CREATE ROLE {role} LOGIN PASSWORD 'p'", ct);
        try
        {
            // У роли нет права CREATE на базу: схема каждой комбинации не создаётся, а настройки сервера читаются.
            var asRole = new NpgsqlConnectionStringBuilder(pg.DirectConnectionString)
            {
                Username = role, Password = "p", Pooling = false,
            }.ConnectionString;

            var code = await Program.RunAsync(
                ["partitions", "--connection", asRole, "--mode", "direct", "--partitions", "15,20", "--indexes", "current",
                 "--out", _out],
                ct);

            Assert.Equal(2, code);
            var summary = await File.ReadAllTextAsync(Assert.Single(Directory.GetFiles(_out, "*-partitions-summary.md")), ct);
            Assert.Matches(@"(?m)^\| direct \| current \| 15 \|.*\| PostgresException: 42501", summary);
            Assert.Matches(@"(?m)^\| direct \| current \| 20 \|.*\| PostgresException: 42501", summary);
        }
        finally
        {
            await ExecuteAsync($"DROP ROLE {role}", CancellationToken.None);
        }
    }

    private async Task ExecuteAsync(string sql, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(pg.DirectConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
