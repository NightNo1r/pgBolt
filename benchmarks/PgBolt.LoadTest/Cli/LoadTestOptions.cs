namespace PgBolt.LoadTest.Cli;

internal enum QueueProfile { Standard, Limited }

internal enum Scenario { Sustained, Ramp, Partitions, Vacuum }

/// <summary>Способ подключения нагрузки к БД; <c>*Prepared</c> — с авто-подготовкой statement'ов Npgsql.</summary>
internal enum LoadMode { Direct, Bouncer, DirectPrepared, BouncerPrepared }

/// <summary>Набор индексов таблицы <c>jobs</c> в сценарии <c>partitions</c>.</summary>
internal enum IndexSet { Current, Extra }

/// <summary>Штатные параметры PostgreSQL или параметры исходных замеров стенда.</summary>
internal enum PostgresProfile { Bench, Stock }

/// <summary>
/// Параметры прогона. Значения свойств по умолчанию — для сценариев, где они не переопределены;
/// остальные значения по умолчанию подставляет <see cref="OptionsParser"/>.
/// </summary>
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
