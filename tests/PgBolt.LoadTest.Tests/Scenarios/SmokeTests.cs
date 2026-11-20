using System.Text.RegularExpressions;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Scenarios;

[Trait("Category", "Load")]
[Collection("Load")]
public sealed class SmokeTests : IDisposable
{
    private readonly string _out = Path.Combine(Path.GetTempPath(), "pgbolt-smoke-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_out)) Directory.Delete(_out, recursive: true);
    }

    private async Task<string> RunSustainedAsync(string mode)
    {
        var code = await Program.RunAsync(
            ["sustained", "--mode", mode, "--rate", "500", "--warmup", "5s", "--duration", "20s", "--out", _out],
            TestContext.Current.CancellationToken);

        Assert.InRange(code, 0, 1);
        var report = Assert.Single(Directory.GetFiles(_out, "*.md"));
        Assert.True(File.Exists(Path.ChangeExtension(report, ".csv")));
        return report;
    }

    [Fact]
    public async Task Sustained_direct_short()
    {
        var text = await File.ReadAllTextAsync(await RunSustainedAsync("direct"), TestContext.Current.CancellationToken);

        foreach (var cls in StatementClasses.Required)
        {
            var row = Regex.Match(text, $@"^\| {cls} \| (\d+) \|", RegexOptions.Multiline);
            Assert.True(row.Success, $"нет строки класса {cls}");
            Assert.True(long.Parse(row.Groups[1].Value) > 0, $"у класса {cls} calls = 0");
        }

        // После остановки продюсера бакет не копит токены: окна дренажа (после последнего окна замера) не
        // показывают отброса больше секунды потока. В окнах замера отброс законен при обратном давлении.
        var windows = text[text.IndexOf("## Пропускная способность", StringComparison.Ordinal)..text.IndexOf("## Enqueue", StringComparison.Ordinal)]
            .Split('\n').Where(l => Regex.IsMatch(l, @"^\| \d+ \|")).Select(l => l.Split('|', StringSplitOptions.TrimEntries)).ToList();
        var drain = windows.Skip(windows.FindLastIndex(cells => cells[3] == "да") + 1).ToList();
        Assert.NotEmpty(drain);
        Assert.All(drain, cells => Assert.True(long.Parse(cells[12]) < 500, $"окно {cells[1]}: отброшено {cells[12]}"));
    }

    [Fact]
    public async Task Sustained_bouncer_short()
    {
        var lines = await File.ReadAllLinesAsync(
            Path.ChangeExtension(await RunSustainedAsync("bouncer"), ".csv"), TestContext.Current.CancellationToken);

        var header = lines[0].Split(',');
        int[] pools = [.. new[] { "max_wait_us", "cl_waiting", "sv_active", "sv_idle" }.Select(c => Array.IndexOf(header, c))];
        Assert.DoesNotContain(-1, pools);
        Assert.Contains(lines.Skip(1), line => pools.All(i => line.Split(',')[i] != ""));
    }

    [Fact]
    public async Task Ramp_direct_short()
    {
        var code = await Program.RunAsync(
            ["ramp", "--mode", "direct", "--ramp-start", "500", "--ramp-step", "500", "--ramp-step-duration", "60s",
             "--ramp-max", "1000", "--out", _out],
            TestContext.Current.CancellationToken);

        Assert.Equal(0, code);
        var text = await File.ReadAllTextAsync(Assert.Single(Directory.GetFiles(_out, "*.md")), TestContext.Current.CancellationToken);
        // Хотя бы одна ступень пройдена: в строке число потока, а не «нет».
        Assert.Matches(@"\| Максимальный устойчивый rate \| \d+/с", text);
        Assert.Matches(ExtraExecutionsCounted, text);
    }

    [Fact]
    public async Task Vacuum_direct_short()
    {
        var ct = TestContext.Current.CancellationToken;
        var code = await Program.RunAsync(
            ["vacuum", "--mode", "direct", "--rate", "500", "--warmup", "5s", "--phase1", "20s", "--hold-xmin", "10s",
             "--phase3", "20s", "--queue-reloptions", "autovacuum_vacuum_cost_delay=0", "--out", _out],
            ct);

        Assert.InRange(code, 0, 1);
        var text = await File.ReadAllTextAsync(Assert.Single(Directory.GetFiles(_out, "*-vacuum-direct.md")), ct);
        var slo = Section(text, "SLO");
        Assert.Equal([6, 7, 8, 9, 9],
            Regex.Matches(slo, @"(?m)^\| (\d+) \|").Select(m => int.Parse(m.Groups[1].Value)));
        // Фаза 1 короче 3 мин: последняя минута перекрылась бы с отрезком 60–120 с.
        Assert.Matches(@"(?m)^\| 7 \|.*\| нет данных \| нет данных \|", slo);
        Assert.Matches(@"(?m)^\| reloptions queue \| [^|]*autovacuum_vacuum_cost_delay=0", text);
        Assert.Matches(@"(?m)^\| (да|нет) \| \d+\.\d \| \d+\.\d \|$", Section(text, "Входы выбора параметров autovacuum"));
        Assert.Matches(ExtraExecutionsCounted, text);
        Assert.Matches(@"(?m)^\| Размер БД в конце прогона, МБ \| \d+\.\d \|$", text);
    }

    /// <summary>Сценарий без дренажа считает лишние выполнения: в строке число, а не «нет данных».</summary>
    private const string ExtraExecutionsCounted = @"(?m)^\| Повторные \(лишние\) выполнения \| \d+ \|$";

    private static string Section(string text, string title)
    {
        var start = text.IndexOf($"## {title}\n", StringComparison.Ordinal);
        Assert.True(start >= 0, $"нет раздела {title}");
        var end = text.IndexOf("\n## ", start, StringComparison.Ordinal);
        return text[start..(end < 0 ? text.Length : end)];
    }

    [Fact]
    public async Task Partitions_direct_short()
    {
        var ct = TestContext.Current.CancellationToken;
        var code = await Program.RunAsync(
            ["partitions", "--mode", "direct", "--partitions", "15,20", "--indexes", "current", "--rate", "500",
             "--warmup", "5s", "--duration", "20s", "--out", _out],
            ct);

        Assert.InRange(code, 0, 1);
        var combinations = Directory.GetFiles(_out, "*-partitions-direct-p*-current.md");
        Assert.Equal(2, combinations.Length);
        Assert.Matches(ExtraExecutionsCounted, await File.ReadAllTextAsync(combinations[0], ct));
        var summary = await File.ReadAllTextAsync(Assert.Single(Directory.GetFiles(_out, "*-partitions-summary.md")), ct);
        Assert.Matches(@"\| Ёмкость таблицы блокировок \| \d+ \|", summary);
        Assert.Matches(@"(?m)^\| direct \| current \| 15 \|", summary);
        Assert.Matches(@"(?m)^\| direct \| current \| 20 \|", summary);
    }

    [Fact]
    public async Task Partitions_bouncer_prepared_short()
    {
        var code = await Program.RunAsync(
            ["partitions", "--mode", "bouncer-prepared", "--partitions", "15", "--indexes", "current", "--rate", "500",
             "--warmup", "5s", "--duration", "20s", "--out", _out],
            TestContext.Current.CancellationToken);

        Assert.InRange(code, 0, 1);
        Assert.Single(Directory.GetFiles(_out, "*-partitions-bouncer-prepared-p15-current.md"));
    }
}
