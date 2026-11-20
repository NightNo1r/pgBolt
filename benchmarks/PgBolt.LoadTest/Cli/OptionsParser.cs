using System.Globalization;

namespace PgBolt.LoadTest.Cli;

/// <summary>Результат разбора: либо параметры, либо текст ошибки.</summary>
internal sealed record ParseResult(LoadTestOptions? Options, string? Error);

/// <summary>Разбор командной строки вида <c>&lt;сценарий&gt; [--флаг значение]...</c>.</summary>
internal static class OptionsParser
{
    private static readonly TimeSpan MinRampStepDuration = TimeSpan.FromSeconds(30);

    private static readonly Dictionary<string, Scenario> Scenarios = new()
    {
        ["sustained"] = Scenario.Sustained,
        ["ramp"] = Scenario.Ramp,
        ["partitions"] = Scenario.Partitions,
        ["vacuum"] = Scenario.Vacuum,
    };

    private static readonly Dictionary<string, LoadMode[]> Modes = new()
    {
        ["direct"] = [LoadMode.Direct],
        ["bouncer"] = [LoadMode.Bouncer],
        ["both"] = [LoadMode.Direct, LoadMode.Bouncer],
        ["direct-prepared"] = [LoadMode.DirectPrepared],
        ["bouncer-prepared"] = [LoadMode.BouncerPrepared],
        ["all"] = [LoadMode.Direct, LoadMode.Bouncer, LoadMode.DirectPrepared, LoadMode.BouncerPrepared],
    };

    private static readonly Scenario[] AllScenarios = [.. Scenarios.Values];
    private static readonly Scenario[] WithRate = [Scenario.Sustained, Scenario.Partitions, Scenario.Vacuum];

    /// <summary>Флаг → сценарии, в которых он допустим.</summary>
    private static readonly Dictionary<string, Scenario[]> Flags = new()
    {
        ["--mode"] = AllScenarios,
        ["--connection"] = AllScenarios,
        ["--bouncer-connection"] = AllScenarios,
        ["--out"] = AllScenarios,
        ["--rate"] = WithRate,
        ["--warmup"] = WithRate,
        ["--duration"] = [Scenario.Sustained, Scenario.Partitions],
        ["--ramp-start"] = [Scenario.Ramp],
        ["--ramp-step"] = [Scenario.Ramp],
        ["--ramp-step-duration"] = [Scenario.Ramp],
        ["--ramp-max"] = [Scenario.Ramp],
        ["--partitions"] = [Scenario.Partitions],
        ["--indexes"] = [Scenario.Partitions],
        ["--phase1"] = [Scenario.Vacuum],
        ["--hold-xmin"] = [Scenario.Vacuum],
        ["--phase3"] = [Scenario.Vacuum],
        ["--queue-reloptions"] = [Scenario.Vacuum],
        ["--pg-setting"] = [Scenario.Vacuum],
    };

    public static ParseResult Parse(IReadOnlyList<string> args)
    {
        try
        {
            return new ParseResult(Build(args), null);
        }
        catch (FormatException e)
        {
            return new ParseResult(null, e.Message);
        }
    }

    private static LoadTestOptions Build(IReadOnlyList<string> args)
    {
        if (args.Count == 0 || !Scenarios.TryGetValue(args[0], out var scenario))
            throw new FormatException($"Первым аргументом нужен сценарий: {string.Join('|', Scenarios.Keys)}.");

        var values = ReadFlags(args, scenario);
        var defaults = new LoadTestOptions { Scenario = scenario, Modes = [] };
        var connection = values.GetValueOrDefault("--connection");
        var bouncer = values.GetValueOrDefault("--bouncer-connection");
        if (bouncer is not null && connection is null)
            throw new FormatException("--bouncer-connection допустим только вместе с --connection.");
        if (connection is not null && values.ContainsKey("--pg-setting"))
            throw new FormatException("--pg-setting нельзя вместе с --connection: параметры задаются только своему контейнеру.");

        var warmup = scenario switch
        {
            Scenario.Partitions => TimeSpan.FromSeconds(30),
            Scenario.Vacuum => TimeSpan.FromMinutes(2),
            _ => defaults.Warmup,
        };
        var duration = scenario == Scenario.Partitions ? TimeSpan.FromMinutes(2) : defaults.Duration;

        var options = defaults with
        {
            Modes = ParseModes(values.GetValueOrDefault("--mode"), scenario, connection, bouncer),
            Connection = connection,
            BouncerConnection = bouncer,
            Out = values.GetValueOrDefault("--out") ?? defaults.Out,
            Rate = Number(values, "--rate", defaults.Rate),
            Warmup = Duration(values, "--warmup", warmup, multipleOf10: false),
            Duration = Duration(values, "--duration", duration),
            RampStart = Number(values, "--ramp-start", defaults.RampStart),
            RampStep = Number(values, "--ramp-step", defaults.RampStep),
            RampStepDuration = Duration(values, "--ramp-step-duration", defaults.RampStepDuration, MinRampStepDuration),
            RampMax = Number(values, "--ramp-max", defaults.RampMax),
            Partitions = values.TryGetValue("--partitions", out var p)
                ? [.. p.Split(',').Select(x => Number("--partitions", x))]
                : defaults.Partitions,
            Indexes = values.TryGetValue("--indexes", out var i)
                ? [.. i.Split(',').Select(ParseIndexSet)]
                : defaults.Indexes,
            Phase1 = Duration(values, "--phase1", defaults.Phase1),
            HoldXmin = Duration(values, "--hold-xmin", defaults.HoldXmin),
            Phase3 = Duration(values, "--phase3", defaults.Phase3),
            QueueReloptions = Pairs(values, "--queue-reloptions"),
            PgSettings = Pairs(values, "--pg-setting"),
        };
        if (options.RampStart > options.RampMax)
            throw new FormatException($"--ramp-start ({options.RampStart}) больше --ramp-max ({options.RampMax}).");
        return options;
    }

    private static Dictionary<string, string> ReadFlags(IReadOnlyList<string> args, Scenario scenario)
    {
        var values = new Dictionary<string, string>();
        for (var i = 1; i < args.Count; i += 2)
        {
            var flag = args[i];
            if (!Flags.TryGetValue(flag, out var allowed))
                throw new FormatException($"Неизвестный флаг {flag}.");
            if (!allowed.Contains(scenario))
                throw new FormatException($"Флаг {flag} не применим к сценарию {args[0]}.");
            if (i + 1 >= args.Count)
                throw new FormatException($"У флага {flag} нет значения.");
            values[flag] = args[i + 1];
        }

        return values;
    }

    private static LoadMode[] ParseModes(string? raw, Scenario scenario, string? connection, string? bouncer)
    {
        var partitions = scenario == Scenario.Partitions;
        if (raw is null)
        {
            return (connection, bouncer, partitions) switch
            {
                (null, _, true) => Modes["all"],
                (null, _, false) => Modes["both"],
                (_, null, true) => [LoadMode.Direct, LoadMode.DirectPrepared],
                (_, null, false) => [LoadMode.Direct],
                (_, _, true) => [LoadMode.Direct, LoadMode.Bouncer, LoadMode.DirectPrepared],
                _ => Modes["both"],
            };
        }

        if (!Modes.TryGetValue(raw, out var modes))
            throw new FormatException($"Неизвестный режим {raw}: {string.Join('|', Modes.Keys)}.");
        if (!partitions && modes.Any(m => m is LoadMode.DirectPrepared or LoadMode.BouncerPrepared))
            throw new FormatException($"Режим {raw} допустим только в сценарии partitions.");
        if (connection is not null && bouncer is null && modes.Any(m => m is LoadMode.Bouncer or LoadMode.BouncerPrepared))
            throw new FormatException($"Режим {raw} с --connection требует --bouncer-connection.");
        return modes;
    }

    private static TimeSpan Duration(
        Dictionary<string, string> values, string flag, TimeSpan fallback,
        TimeSpan? min = null, bool multipleOf10 = true)
    {
        if (!values.TryGetValue(flag, out var raw))
            return fallback;

        var span = ParseDuration(flag, raw);
        if (multipleOf10 && span.TotalSeconds % 10 != 0)
            throw new FormatException($"{flag}: длительность должна быть кратна 10 с, получено {raw}.");
        if (min is { } m && span < m)
            throw new FormatException($"{flag}: длительность не короче {m.TotalSeconds:0} с, получено {raw}.");
        return span;
    }

    private static TimeSpan ParseDuration(string flag, string raw)
    {
        var unit = raw.Length > 1 ? raw[^1] : ' ';
        // Единица проверяется первой: у пустой строки нечего отрезать.
        if (unit is not ('s' or 'm' or 'h') ||
            !int.TryParse(raw[..^1], NumberStyles.None, CultureInfo.InvariantCulture, out var n) || n <= 0)
            throw new FormatException($"{flag}: ожидается длительность вида 30s, 2m, 1h, получено {raw}.");

        return unit switch
        {
            's' => TimeSpan.FromSeconds(n),
            'm' => TimeSpan.FromMinutes(n),
            _ => TimeSpan.FromHours(n),
        };
    }

    private static int Number(Dictionary<string, string> values, string flag, int fallback) =>
        values.TryGetValue(flag, out var raw) ? Number(flag, raw) : fallback;

    private static int Number(string flag, string raw) =>
        int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n > 0
            ? n
            : throw new FormatException($"{flag}: ожидается положительное целое, получено {raw}.");

    private static IndexSet ParseIndexSet(string raw) => raw switch
    {
        "current" => IndexSet.Current,
        "extra" => IndexSet.Extra,
        _ => throw new FormatException($"--indexes: ожидается current|extra, получено {raw}."),
    };

    private static Dictionary<string, string> Pairs(Dictionary<string, string> values, string flag)
    {
        var result = new Dictionary<string, string>();
        if (!values.TryGetValue(flag, out var raw))
            return result;

        foreach (var pair in raw.Split(','))
        {
            var eq = pair.IndexOf('=');
            if (eq <= 0)
                throw new FormatException($"{flag}: ожидается список k=v, получено {pair}.");
            result[pair[..eq]] = pair[(eq + 1)..];
        }

        return result;
    }
}
