using System.Text.Json;
using Npgsql;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres;

public sealed class PgBoltOptions
{
    private readonly List<Action<IPgBoltJobsBuilder>> _jobRegistrations = [];
    private readonly List<RecurringDeclaration> _recurring = [];

    public NpgsqlDataSource? DataSource { get; set; }
    public string? ConnectionString { get; set; }
    public string Schema { get; set; } = "pgbolt";
    public bool AutoMigrate { get; set; } = true;
    public bool UsePreparedStatements { get; set; }
    /// <summary>Fillfactor for jobs partitions (HOT update contract, §4.2).</summary>
    public int JobsFillFactor { get; set; } = 80;
    public int PartitionDaysAhead { get; set; } = 7;
    public TimeSpan PartitionLockTimeout { get; set; } = TimeSpan.FromSeconds(2);

    internal static readonly TimeSpan MaxRetention = TimeSpan.FromDays(3650);

    /// <summary>
    /// Retention for completed jobs (Succeeded, Failed, Cancelled), measured from their UTC creation day in daily partitions and rounded up to whole
    /// days (Part 2 §5.1–§5.3). Unfinished jobs are always retained. A job created on day D and completed on D + k is retained for Rd − k + 1 days
    /// after completion, or until the next retention run if k &gt; Rd. The first retention run after an upgrade removes all expired partitions.
    /// After compaction, a partition pinned by live jobs occupies space for its live rows. With more than
    /// <see cref="RetentionCompactionMaxLiveJobs"/> live jobs, the full partition remains on disk until the count falls. Set null in code to retain
    /// jobs indefinitely, adding one partition per day. Omitting the configuration key selects the default. Use the same setting on all pods and
    /// applications sharing the schema. The shortest retention among processes running maintenance takes effect. PgBolt owns partition names
    /// (jobs_YYYYMMDD, jobs_YYYYMMDD_rN, job_events_YYYYMMDD). Retention drops detached tables with these names even when set to null. Rename
    /// archived partitions or move them to another schema (Part 2 §5.2).
    /// </summary>
    public TimeSpan? JobsRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Retention for job_events, measured from the event's UTC day in daily partitions and rounded up to whole days. Set null in code for indefinite
    /// retention. The first run after an upgrade drops all expired partitions. Use the same setting on every pod sharing the schema. To archive
    /// partitions, rename them or move them to another schema as described in <see cref="JobsRetention"/>.
    /// </summary>
    public TimeSpan? EventsRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// lock_timeout for each retention attempt to lock a parent or partition (Part 2 §5.3). Hot-path operations may queue behind an attempt for this
    /// long.
    /// </summary>
    public TimeSpan RetentionLockTimeout { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Maximum number of live jobs in a pinned jobs partition eligible for compaction (Part 2 §5.3). Set to 0 to disable compaction. Like TRUNCATE,
    /// compaction is unsafe for earlier REPEATABLE READ or SERIALIZABLE snapshots. Set to 0 if reports or exports read jobs in REPEATABLE READ
    /// transactions. PgBolt skips compaction when jobs belongs to a logical replication publication. Use the same setting on every pod sharing the
    /// schema.
    /// </summary>
    public int RetentionCompactionMaxLiveJobs { get; set; } = 1000;
    public TimeSpan JobTypesCacheTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Lifetime of this process's <see cref="IQueueSettingsManager"/> read cache. Null sends every read to the database. Local manager writes and
    /// <see cref="IQueueSettingsManager.InvalidateCache"/> clear the cache. Other processes' changes appear after the TTL expires. Servers and
    /// dequeue operations bypass this cache. An explicit TTL must be positive and at most one day.
    /// </summary>
    public TimeSpan? QueueSettingsCacheDuration { get; set; }

    internal IReadOnlyList<Action<IPgBoltJobsBuilder>> JobRegistrations => _jobRegistrations;

    /// <summary>Registers an assembly's executors, e.g. o.AddJobs(My.Jobs.PgBoltJobRegistry.Register).</summary>
    public PgBoltOptions AddJobs(Action<IPgBoltJobsBuilder> register)
    {
        ArgumentNullException.ThrowIfNull(register);
        _jobRegistrations.Add(register);
        return this;
    }

    /// <summary>
    /// Owner of code-defined schedules (Part 4 §4.3). Every process with the same owner declares the same set. Defaults to
    /// IHostEnvironment.ApplicationName. Set the owner name here and enable synchronization with AddRecurring or ClearRecurring().
    /// </summary>
    /// <remarks>
    /// <para>
    /// Synchronization reconciles the owner's rows with the declared set at startup (§6). Processes with AddPgBoltServer also reconcile every
    /// PgBoltProcessingOptions.RecurringSyncInterval. A process without a server synchronizes only at startup, so an older pod can revert the set.
    /// </para>
    /// <para>
    /// During a rolling deployment, old and new pods can alternate between their sets until the old pods exit. Deleting and reinserting a row loses
    /// its cursor and paused state. An occurrence near the version change can be missed or repeated. An old pod restoring a schedule removed by the
    /// new version can also clear its pause.
    /// </para>
    /// <para>
    /// When an owner is retired, its schedules keep firing or record a row error every minute. IRecurringJobManager cannot remove them. Deploy the
    /// service one last time with ClearRecurring(), or run <c>DELETE FROM {schema}.recurring_jobs WHERE owner = '&lt;owner&gt;';</c>. Any surviving
    /// owner process will reinsert the rows at its next synchronization.
    /// </para>
    /// </remarks>
    public string? RecurringOwner { get; set; }

    internal IReadOnlyList<RecurringDeclaration> Recurring => _recurring;
    internal bool RecurringCleared { get; private set; }

    /// <summary>
    /// Declares a recurring schedule synchronized at startup and periodically (Part 4 §6). All values except job arguments are validated
    /// immediately.
    /// </summary>
    /// <remarks>
    /// The generator registers TArgs metadata at the call site. Arguments are serialized at the first synchronization. Removing the last
    /// AddRecurring without calling ClearRecurring() leaves the owner's rows in place because a process without declarations skips synchronization.
    /// See <see cref="RecurringOwner"/> for rolling deployments, processes without servers, and retiring owners.
    /// </remarks>
    public PgBoltOptions AddRecurring<TArgs>(string id, string cron, TArgs args, RecurringJobOptions? options = null)
    {
        RecurringDefinition.Validate(id, cron, options);
        _recurring.Add(new RecurringDeclaration(id, cron, options, () =>
        {
            var (descriptor, typeInfo) = JobMetadataAccess.Get<TArgs>();
            return (descriptor.Name, JsonSerializer.Serialize(args, typeInfo));
        }));
        return this;
    }

    /// <summary>Declares a recurring schedule by job name. The type is not checked against job_types (Part 4 §4.3).</summary>
    /// <remarks>Removing the last AddRecurring call and managing the set follow the rules for <see cref="AddRecurring{TArgs}"/>.</remarks>
    public PgBoltOptions AddRecurring(string id, string jobName, string cron, JsonElement args, RecurringJobOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobName);
        RecurringDefinition.Validate(id, cron, options);
        var json = args.GetRawText();
        _recurring.Add(new RecurringDeclaration(id, cron, options, () => (jobName, json)));
        return this;
    }

    /// <summary>Declares an empty set and removes the owner's rows. Mutually exclusive with AddRecurring (Part 4 §4.3).</summary>
    /// <remarks>
    /// Use this to remove the final AddRecurring declaration or retire an owner (<see cref="RecurringOwner"/>). After the ClearRecurring()
    /// deployment finishes and the owner's rows are gone, the next version can omit the call.
    /// </remarks>
    public PgBoltOptions ClearRecurring()
    {
        RecurringCleared = true;
        return this;
    }

    internal void Validate()
    {
        if ((DataSource is null) == string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException("PgBolt: set exactly one of DataSource or ConnectionString.");
        SchemaName.Validate(Schema);
        if (JobsFillFactor is < 10 or > 100)
            throw new InvalidOperationException("PgBolt: JobsFillFactor must be in [10, 100].");
        if (PartitionDaysAhead < 1)
            throw new InvalidOperationException("PgBolt: PartitionDaysAhead must be >= 1.");
        if (PartitionLockTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException("PgBolt: PartitionLockTimeout must be positive.");
        ValidateRetention(JobsRetention, nameof(JobsRetention));
        ValidateRetention(EventsRetention, nameof(EventsRetention));
        if (RetentionLockTimeout <= TimeSpan.Zero || RetentionLockTimeout > TimeSpan.FromMinutes(1))
            throw new InvalidOperationException("PgBolt: RetentionLockTimeout must be in (0, 1 min].");
        if (RetentionCompactionMaxLiveJobs is < 0 or > 10_000)
            throw new InvalidOperationException("PgBolt: RetentionCompactionMaxLiveJobs must be in [0, 10000].");
        if (JobTypesCacheTtl <= TimeSpan.Zero)
            throw new InvalidOperationException("PgBolt: JobTypesCacheTtl must be positive.");
        if (QueueSettingsCacheDuration is { } cache && (cache <= TimeSpan.Zero || cache > TimeSpan.FromDays(1)))
            throw new InvalidOperationException("PgBolt: QueueSettingsCacheDuration must be null or in (0, 1 day].");
        if (RecurringCleared && _recurring.Count > 0)
            throw new InvalidOperationException("PgBolt: ClearRecurring() cannot be combined with AddRecurring.");
        if (_recurring.GroupBy(r => r.Id, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1) is { } duplicate)
            throw new InvalidOperationException($"PgBolt: recurring job '{duplicate.Key}' is declared more than once.");
        if (RecurringOwner is { } owner && (string.IsNullOrWhiteSpace(owner) || owner.Length > RecurringDefinition.MaxIdLength))
            throw new InvalidOperationException(
                $"PgBolt: RecurringOwner must be non-blank and at most {RecurringDefinition.MaxIdLength} characters.");
    }

    private static void ValidateRetention(TimeSpan? retention, string name)
    {
        if (retention is { } r && (r <= TimeSpan.Zero || r > MaxRetention))
            throw new InvalidOperationException($"PgBolt: {name} must be null or in (0, 3650 days].");
    }
}
