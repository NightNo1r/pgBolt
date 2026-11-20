using Microsoft.Extensions.DependencyInjection;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class OptionsTests
{
    private static readonly Dictionary<string, Action<PgBoltDashboardOptions>> Invalid = new()
    {
        ["QueryTimeout below 1 ms"] = o => o.QueryTimeout = TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond - 1),
        ["QueryTimeout above 1 day"] = o => o.QueryTimeout = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1),
        ["ScanBudget zero"] = o => o.ScanBudget = TimeSpan.Zero,
        ["ScanBudget above 1 day"] = o => o.ScanBudget = TimeSpan.FromDays(2),
        ["RefreshInterval below 1 s"] = o => o.RefreshInterval = TimeSpan.FromMilliseconds(999),
        ["RefreshInterval above 1 day"] = o => o.RefreshInterval = TimeSpan.FromDays(2),
        ["ServerStaleAfter negative"] = o => o.ServerStaleAfter = TimeSpan.FromSeconds(-1),
        ["ServerStaleAfter above 1 day"] = o => o.ServerStaleAfter = TimeSpan.FromDays(2),
        ["StatsStaleAfter zero"] = o => o.StatsStaleAfter = TimeSpan.Zero,
        ["StatsStaleAfter above 1 day"] = o => o.StatsStaleAfter = TimeSpan.FromDays(2),
        ["ActionsPolicy empty"] = o => o.ActionsPolicy = "",
        ["ActionsPolicy blank"] = o => o.ActionsPolicy = "  ",
    };

    public static TheoryData<string> InvalidCases => [.. Invalid.Keys];

    [Fact]
    public void Defaults_match_spec()
    {
        var o = new PgBoltDashboardOptions();

        Assert.Equal(TimeSpan.FromSeconds(5), o.QueryTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), o.ScanBudget);
        Assert.Equal(TimeSpan.FromSeconds(5), o.RefreshInterval);
        Assert.Equal(TimeSpan.FromMinutes(1), o.ServerStaleAfter);
        Assert.Equal(TimeSpan.FromMinutes(1), o.StatsStaleAfter);
        Assert.Null(o.ActionsPolicy);
        Assert.False(o.ReadOnly);
        Assert.False(o.AllowAnonymousRemote);
    }

    [Theory]
    [MemberData(nameof(InvalidCases))]
    public void Invalid_values_are_rejected(string name) =>
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddPgBoltDashboard(Invalid[name]));

    [Fact]
    public void Boundary_values_are_accepted() =>
        new ServiceCollection().AddPgBoltDashboard(o =>
        {
            o.QueryTimeout = TimeSpan.FromMilliseconds(1);
            o.ScanBudget = TimeSpan.FromDays(1);
            o.RefreshInterval = TimeSpan.FromSeconds(1);
            o.ServerStaleAfter = TimeSpan.FromDays(1);
            o.StatsStaleAfter = TimeSpan.FromTicks(1);
            o.ActionsPolicy = "Operators";
        });

    [Fact]
    public void Second_call_throws()
    {
        var services = new ServiceCollection().AddPgBoltDashboard();

        Assert.Throws<InvalidOperationException>(() => services.AddPgBoltDashboard());
    }
}
