using System.Text.RegularExpressions;
using PgBolt.LoadTest.Tests.Env;
using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests.Scenarios;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class StandCancelTests(PostgresFixture pg) : IDisposable
{
    private readonly string _out = Path.Combine(Path.GetTempPath(), "pgbolt-cancel-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_out)) Directory.Delete(_out, recursive: true);
    }

    [Fact]
    public async Task Cancelled_run_drops_schema_and_writes_partial_report()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        var code = await Program.RunAsync(
            ["sustained", "--connection", pg.DirectConnectionString, "--mode", "direct", "--rate", "500",
             "--warmup", "1m", "--duration", "60s", "--out", _out],
            cts.Token);

        Assert.Equal(2, code);
        var text = await File.ReadAllTextAsync(Assert.Single(Directory.GetFiles(_out, "*.md")),
            TestContext.Current.CancellationToken);
        Assert.Contains("aborted", text);
        var schema = Regex.Match(text, "load_[0-9a-f]{12}").Value;
        Assert.NotEqual("", schema);
        Assert.Equal(0L, await LoadEnvironmentTests.ScalarAsync<long>(pg.DirectConnectionString,
            $"SELECT count(*) FROM pg_namespace WHERE nspname = '{schema}'"));
    }
}
