using Microsoft.Extensions.Logging;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed class ListLoggerTests
{
    [Fact]
    public void Captures_event_id_and_scopes()
    {
        var root = new ListLogger();
        var logger = root.CreateLogger("cat");

        logger.Log(LogLevel.Information, new EventId(42, "Answer"), "x", null, (s, _) => s);
        Assert.Equal(42, Assert.Single(root.Entries).EventId.Id);
        Assert.Equal("Answer", root.Entries.Single().EventId.Name);
        Assert.Empty(root.Entries.Single().Scopes);

        var scope = logger.BeginScope("a")!;
        Assert.Equal(1, root.OpenScopes);
        logger.LogInformation("inside");
        Assert.Equal(["a"], root.Entries.Last().Scopes);

        scope.Dispose();
        scope.Dispose();
        Assert.Equal(0, root.OpenScopes);
        logger.LogInformation("outside");
        Assert.Empty(root.Entries.Last().Scopes);
    }

    [Fact]
    public void OnBeginScope_receives_state_and_throwing_hook_leaves_no_open_scope()
    {
        var root = new ListLogger();
        var logger = root.CreateLogger("cat");
        object? seen = null;
        root.OnBeginScope = state => seen = state;

        using (logger.BeginScope("s1"))
            Assert.Equal("s1", seen);

        root.OnBeginScope = _ => throw new InvalidOperationException("boom");
        Assert.Throws<InvalidOperationException>(() => logger.BeginScope("s2"));
        Assert.Equal(0, root.OpenScopes);
        logger.LogInformation("after");
        Assert.Empty(root.Entries.Last().Scopes);
    }
}
