using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class MaintenanceTasksTests(PostgresFixture pg, ConnectionMode mode) : StoreTestBase(pg, mode)
{
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private Task MakeDueAsync(string name) =>
        Db.ExecAsync("UPDATE {schema}.maintenance_tasks SET next_run_at = now() - interval '1 second' WHERE name = @n", ("n", name));

    private Task<bool> RowAsync(string sql, string name) =>
        Db.ScalarAsync<bool>($"SELECT {sql} FROM {{schema}}.maintenance_tasks WHERE name = @n", ("n", name));

    [Fact]
    public async Task Claim_without_row_creates_and_claims_it()
    {
        var claimedAt = await Store.TryClaimMaintenanceAsync("retention", Hour, Ct);

        Assert.NotNull(claimedAt);
        Assert.Equal(DateTimeKind.Utc, claimedAt.Value.Kind);
        Assert.True(await RowAsync("next_run_at BETWEEN now() + interval '59 minutes' AND now() + interval '61 minutes' AND last_run_at IS NOT NULL", "retention"));
        Assert.Null(await Store.TryClaimMaintenanceAsync("retention", Hour, Ct));
    }

    [Fact]
    public async Task Due_row_is_claimed_again_with_new_token()
    {
        var first = await Store.TryClaimMaintenanceAsync("servers", Hour, Ct);
        await MakeDueAsync("servers");

        var second = await Store.TryClaimMaintenanceAsync("servers", Hour, Ct);

        Assert.NotNull(second);
        Assert.NotEqual(first, second);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Parallel_claims_have_exactly_one_winner(bool rowExists)
    {
        if (rowExists)
        {
            await Store.TryClaimMaintenanceAsync("partitions", Hour, Ct);
            await MakeDueAsync("partitions");
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Store.TryClaimMaintenanceAsync("partitions", Hour, Ct)));

        Assert.Single(results, r => r is not null);
    }

    [Fact]
    public async Task Complete_writes_result_only_for_current_claim()
    {
        var stale = (await Store.TryClaimMaintenanceAsync("retention", Hour, Ct))!.Value;
        await MakeDueAsync("retention");
        var current = (await Store.TryClaimMaintenanceAsync("retention", Hour, Ct))!.Value;

        Assert.False(await Store.CompleteMaintenanceAsync("retention", stale, "old", TimeSpan.FromMinutes(1), Ct));
        Assert.True(await RowAsync("last_finished_at IS NULL AND last_error IS NULL", "retention"));

        Assert.True(await Store.CompleteMaintenanceAsync("retention", current, null, null, Ct));
        Assert.True(await RowAsync("last_finished_at IS NOT NULL AND last_error IS NULL AND next_run_at > now() + interval '59 minutes'", "retention"));
    }

    [Fact]
    public async Task Complete_with_retry_delay_shortens_next_run()
    {
        var claimedAt = (await Store.TryClaimMaintenanceAsync("partitions", Hour, Ct))!.Value;

        Assert.True(await Store.CompleteMaintenanceAsync("partitions", claimedAt, "lock_timeout", TimeSpan.FromMinutes(1), Ct));

        Assert.True(await RowAsync("last_error = 'lock_timeout' AND next_run_at <= now() + interval '1 minute'", "partitions"));
    }

    [Fact]
    public async Task Remove_dead_servers_deletes_only_stale_rows()
    {
        await Store.RegisterServerAsync(ServerA, "a", "h", ["q"], 1, Ct);
        await Store.RegisterServerAsync(ServerB, "b", "h", ["q"], 1, Ct);
        await Db.ExecAsync("UPDATE {schema}.servers SET heartbeat_at = now() - interval '31 minutes' WHERE id = @id", ("id", ServerA));

        Assert.Equal(1, await Store.RemoveDeadServersAsync(TimeSpan.FromMinutes(30), Ct));

        Assert.Equal(new[] { ServerB }, await Db.ScalarAsync<Guid[]>("SELECT array_agg(id) FROM {schema}.servers"));
    }

    [Fact]
    public async Task Remove_dead_servers_rejects_non_positive_threshold() =>
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Store.RemoveDeadServersAsync(TimeSpan.Zero, Ct));
}

public sealed class MaintenanceTasksTests_Direct(PostgresFixture pg) : MaintenanceTasksTests(pg, ConnectionMode.Direct);
public sealed class MaintenanceTasksTests_Bouncer(PostgresFixture pg) : MaintenanceTasksTests(pg, ConnectionMode.Bouncer);
