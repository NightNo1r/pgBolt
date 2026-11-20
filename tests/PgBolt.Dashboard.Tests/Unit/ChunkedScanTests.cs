using Microsoft.Extensions.Time.Testing;
using PgBolt.Dashboard.Data;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class ChunkedScanTests
{
    private const int PageSize = 50;
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Portion<Guid> Portion(int scanned, int matches) =>
        new(Enumerable.Range(0, matches).Select(_ => Guid.NewGuid()).ToList(), scanned, new ListKey(Guid.NewGuid()));

    /// <summary>Скрипт порций: каждый вызов продвигает время на 1 с и запоминает (after, need).</summary>
    private sealed class Script(FakeTimeProvider time, params Portion<Guid>[] portions)
    {
        public List<(ListKey? After, int Need)> Calls { get; } = [];

        public Task<Portion<Guid>> NextAsync(ListKey? after, int need, CancellationToken ct)
        {
            Calls.Add((after, need));
            time.Advance(TimeSpan.FromSeconds(1));
            return Task.FromResult(portions[Calls.Count - 1]);
        }
    }

    private static Task<ScanResult<Guid>> RunAsync(Script script, FakeTimeProvider time, TimeSpan budget, ListKey? after = null) =>
        ChunkedScan.RunAsync(after, PageSize, script.NextAsync, x => new ListKey(x), budget, time, Ct);

    [Fact]
    public async Task Fills_page_from_several_portions()
    {
        var time = new FakeTimeProvider();
        var third = Portion(ChunkedScan.PortionSize, 20);
        var script = new Script(time, Portion(ChunkedScan.PortionSize, 20), Portion(ChunkedScan.PortionSize, 20), third);

        // 3 с — наименьший бюджет, при котором третья порция (1 с на порцию) успевает заполнить страницу.
        var result = await RunAsync(script, time, TimeSpan.FromSeconds(3));

        Assert.Equal(PageSize, result.Rows.Count);
        Assert.Equal(new ListKey(third.Matches[9]), result.Next);
        Assert.False(result.Incomplete);
        Assert.Equal([51, 31, 11], script.Calls.Select(c => c.Need));
    }

    [Fact]
    public void Text_shape_has_no_scan_page() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => DashboardStore.ToPage(
            new ScanResult<Guid>([], new ListKey(Guid.Empty, Text: "x"), Incomplete: true), CursorShape.Text));

    [Fact]
    public async Task Extra_match_means_more_data()
    {
        var time = new FakeTimeProvider();
        var portion = Portion(ChunkedScan.PortionSize, 51);

        var result = await RunAsync(new Script(time, portion), time, Budget);

        Assert.Equal(portion.Matches.Take(PageSize), result.Rows);
        Assert.Equal(new ListKey(portion.Matches[49]), result.Next);
        Assert.False(result.Incomplete);
    }

    [Fact]
    public async Task Short_portion_ends_data()
    {
        var time = new FakeTimeProvider();

        var result = await RunAsync(new Script(time, Portion(1500, 3)), time, Budget);

        Assert.Equal(3, result.Rows.Count);
        Assert.Null(result.Next);
        Assert.False(result.Incomplete);
    }

    [Fact]
    public async Task Budget_exhausted_returns_incomplete_page_with_last_scanned_key()
    {
        var time = new FakeTimeProvider();
        var second = Portion(ChunkedScan.PortionSize, 1);
        var script = new Script(time, Portion(ChunkedScan.PortionSize, 1), second, Portion(ChunkedScan.PortionSize, 1));

        var result = await RunAsync(script, time, Budget);

        Assert.Equal(2, result.Rows.Count);
        Assert.True(result.Incomplete);
        Assert.Equal(second.LastScanned, result.Next);
        Assert.Equal(2, script.Calls.Count);
    }

    [Fact]
    public async Task Cursor_advances_when_budget_ends_without_matches()
    {
        var time = new FakeTimeProvider();
        var after = new ListKey(Guid.NewGuid());
        var second = Portion(ChunkedScan.PortionSize, 0);
        var script = new Script(time, Portion(ChunkedScan.PortionSize, 0), second, Portion(ChunkedScan.PortionSize, 0));

        var result = await RunAsync(script, time, Budget, after);

        Assert.Empty(result.Rows);
        Assert.True(result.Incomplete);
        Assert.Equal(second.LastScanned, result.Next);
        Assert.NotEqual(after, result.Next);
        Assert.Equal(after, script.Calls[0].After);
    }

    [Fact]
    public async Task Budget_is_checked_between_portions_only()
    {
        var time = new FakeTimeProvider();
        var script = new Script(time, Portion(ChunkedScan.PortionSize, 1), Portion(ChunkedScan.PortionSize, 1));

        var result = await RunAsync(script, time, TimeSpan.FromMilliseconds(500));

        Assert.Single(script.Calls);
        Assert.Single(result.Rows);
        Assert.True(result.Incomplete);
    }

    [Fact]
    public void To_page_reports_scanned_to_only_for_incomplete_scan()
    {
        var at = new DateTime(2026, 10, 5, 14, 20, 0, DateTimeKind.Utc);
        var key = new ListKey(Guid.CreateVersion7(), at);

        var incomplete = DashboardStore.ToPage(new ScanResult<int>([1], key, Incomplete: true), CursorShape.AtId);
        var full = DashboardStore.ToPage(new ScanResult<int>([1], key, Incomplete: false), CursorShape.AtId);
        var end = DashboardStore.ToPage(new ScanResult<int>([1], null, Incomplete: false), CursorShape.AtId);

        Assert.Equal(new DateTimeOffset(at), incomplete.ScannedTo);
        Assert.Equal(key, Cursor.Decode(incomplete.Next, CursorShape.AtId));
        Assert.Null(full.ScannedTo);
        Assert.Equal(key, Cursor.Decode(full.Next, CursorShape.AtId));
        Assert.Null(end.Next);
    }

    [Fact]
    public void To_page_takes_scanned_to_from_uuid_v7_for_id_shape()
    {
        var id = Guid.CreateVersion7(new DateTimeOffset(2026, 10, 5, 14, 20, 0, 123, TimeSpan.Zero));

        var page = DashboardStore.ToPage(new ScanResult<int>([], new ListKey(id), Incomplete: true), CursorShape.Id);

        Assert.Equal(new DateTimeOffset(2026, 10, 5, 14, 20, 0, 123, TimeSpan.Zero), page.ScannedTo);
    }

    [Fact]
    public void Limit_page_treats_extra_row_as_more_data()
    {
        var ids = Enumerable.Range(0, PageSize + 1).Select(_ => Guid.NewGuid()).ToList();

        var more = DashboardStore.LimitPage(ids, x => new ListKey(x), CursorShape.Id);
        var last = DashboardStore.LimitPage(ids[..PageSize], x => new ListKey(x), CursorShape.Id);

        Assert.Equal(ids.Take(PageSize), more.Rows);
        Assert.Equal(new ListKey(ids[PageSize - 1]), Cursor.Decode(more.Next, CursorShape.Id));
        Assert.Equal(PageSize, last.Rows.Count);
        Assert.Null(last.Next);
        Assert.Null(more.ScannedTo);
    }
}
