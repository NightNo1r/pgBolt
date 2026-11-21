namespace PgBolt.LoadTest.Cli;

internal enum QueueProfile { Standard, Limited }

internal enum Scenario { Sustained, Ramp, Partitions, Vacuum }

/// <summary>Database connection mode for the load. <c>*Prepared</c> enables Npgsql automatic statement preparation.</summary>
internal enum LoadMode { Direct, Bouncer, DirectPrepared, BouncerPrepared }

/// <summary>Index set for the <c>jobs</c> table in the <c>partitions</c> scenario.</summary>
internal enum IndexSet { Current, Extra }

/// <summary>Stock PostgreSQL settings or the settings used in the original load measurements.</summary>
internal enum PostgresProfile { Bench, Stock }

/// <summary>Run options. Property defaults apply to scenarios without overrides. <see cref="OptionsParser"/> supplies remaining
/// defaults.</summary>
internal sealed record LoadTestOptions
{
    public required Scenario Scenario { get; init; }
    public required IReadOnlyList<LoadMode> Modes { get; init; }
    public string? Connection { get; init; }
    public string? BouncerConnection { get; init; }
    public QueueProfile Queues { get; init; }
    public PostgresProfile PgProfile { get; init; } = PostgresProfile.Bench;
    public string Out { get; init; } = "benchmarks/PgBolt.LoadTest/results";
    public int Rate { get; init; } = 10_000;
    public TimeSpan Warmup { get; init; } = TimeSpan.FromMinutes(1);
    public TimeSpan Duration { get; init; } = TimeSpan.FromMinutes(10);
    public int RampStart { get; init; } = 2_000;
    public int RampStep { get; init; } = 2_000;
    public TimeSpan RampStepDuration { get; init; } = TimeSpan.FromSeconds(60);
    public int RampMax { get; init; } = 40_000;
    public IReadOnlyList<int> Partitions { get; init; } = [15, 60, 200, 400];
    public IReadOnlyList<IndexSet> Indexes { get; init; } = [IndexSet.Current, IndexSet.Extra];
    public TimeSpan Phase1 { get; init; } = TimeSpan.FromMinutes(20);
    public TimeSpan HoldXmin { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan Phase3 { get; init; } = TimeSpan.FromMinutes(10);
    public IReadOnlyDictionary<string, string> QueueReloptions { get; init; } = new Dictionary<string, string>();
    public IReadOnlyDictionary<string, string> PgSettings { get; init; } = new Dictionary<string, string>();
}
