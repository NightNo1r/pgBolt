using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Tests;

public sealed class OptionsTests
{
    [Theory]
    [InlineData("pgbolt")]
    [InlineData("_x1")]
    [InlineData("a23456789012345678901234567890123456789012345678901234567890123")]
    public void Valid_schema_names_pass(string name) => Assert.Equal(name, SchemaName.Validate(name));

    [Theory]
    [InlineData("")]
    [InlineData("Upper")]
    [InlineData("1abc")]
    [InlineData("a-b")]
    [InlineData("a;drop")]
    [InlineData("a234567890123456789012345678901234567890123456789012345678901234")]
    public void Invalid_schema_names_throw(string name) =>
        Assert.Throws<ArgumentException>(() => SchemaName.Validate(name));

    [Fact]
    public void Defaults_match_spec()
    {
        var o = new PgBoltOptions();
        Assert.Equal("pgbolt", o.Schema);
        Assert.True(o.AutoMigrate);
        Assert.False(o.UsePreparedStatements);
        Assert.Equal(80, o.JobsFillFactor);
        Assert.Equal(7, o.PartitionDaysAhead);
        Assert.Equal(TimeSpan.FromSeconds(2), o.PartitionLockTimeout);
        Assert.Equal(TimeSpan.FromDays(7), o.JobsRetention);
        Assert.Equal(TimeSpan.FromDays(7), o.EventsRetention);
        Assert.Equal(TimeSpan.FromMilliseconds(100), o.RetentionLockTimeout);
        Assert.Equal(1000, o.RetentionCompactionMaxLiveJobs);
        Assert.Null(o.QueueSettingsCacheDuration);
    }

    [Fact]
    public void Validate_requires_exactly_one_connection_source()
    {
        Assert.Throws<InvalidOperationException>(() => new PgBoltOptions().Validate());
        new PgBoltOptions { ConnectionString = "Host=x" }.Validate();
    }

    [Theory]
    [InlineData(9)]
    [InlineData(101)]
    public void Validate_rejects_fillfactor_out_of_range(int ff) =>
        Assert.Throws<InvalidOperationException>(() => new PgBoltOptions { ConnectionString = "Host=x", JobsFillFactor = ff }.Validate());

    [Theory]                                                           // часть 4 §4.3: если задан — непустой, не длиннее 200
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t \n")]
    public void Validate_rejects_blank_recurring_owner(string owner)
    {
        var e = Assert.Throws<InvalidOperationException>(() => new PgBoltOptions { ConnectionString = "Host=x", RecurringOwner = owner }.Validate());
        Assert.Contains("RecurringOwner", e.Message);
    }

    [Theory]                                                           // часть 4 §4.3: пустой ApplicationName — не владелец по умолчанию
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t \n")]
    public void Blank_application_name_is_not_a_default_owner(string applicationName)
    {
        var recurring = new RecurringDeclarations(new PgBoltOptions(), new TestHostEnvironment(applicationName));
        var e = Assert.Throws<InvalidOperationException>(() => recurring.Owner);
        Assert.Contains("RecurringOwner", e.Message);
        Assert.Equal("orders", new RecurringDeclarations(new PgBoltOptions(), new TestHostEnvironment("orders")).Owner);
    }

    [Fact]
    public void Validate_checks_recurring_owner_length()
    {
        new PgBoltOptions { ConnectionString = "Host=x", RecurringOwner = new string('o', 200) }.Validate();
        Assert.Throws<InvalidOperationException>(() =>
            new PgBoltOptions { ConnectionString = "Host=x", RecurringOwner = new string('o', 201) }.Validate());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_rejects_non_positive_job_types_cache_ttl(int seconds) =>
        Assert.Throws<InvalidOperationException>(() => new PgBoltOptions { ConnectionString = "Host=x", JobTypesCacheTtl = TimeSpan.FromSeconds(seconds) }.Validate());

    [Fact]
    public void AddJobs_collects_registrations()
    {
        var o = new PgBoltOptions();
        o.AddJobs(_ => { }).AddJobs(_ => { });
        Assert.Equal(2, o.JobRegistrations.Count);
    }

    public static TheoryData<string, TimeSpan> InvalidRetention => new()
    {
        { "JobsRetention", TimeSpan.Zero },
        { "JobsRetention", TimeSpan.FromSeconds(-1) },
        { "JobsRetention", TimeSpan.FromDays(3650) + TimeSpan.FromTicks(1) },
        { "EventsRetention", TimeSpan.Zero },
        { "EventsRetention", TimeSpan.FromDays(3651) },
    };

    [Theory]
    [MemberData(nameof(InvalidRetention))]
    public void Validate_rejects_retention_out_of_range(string field, TimeSpan value)
    {
        var o = new PgBoltOptions { ConnectionString = "Host=x" };
        if (field == "JobsRetention") o.JobsRetention = value; else o.EventsRetention = value;
        var e = Assert.Throws<InvalidOperationException>(o.Validate);
        Assert.Contains(field, e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_accepts_null_retention_and_bounds()
    {
        new PgBoltOptions { ConnectionString = "Host=x", JobsRetention = null, EventsRetention = null }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", JobsRetention = TimeSpan.FromDays(3650), EventsRetention = TimeSpan.FromTicks(1) }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", RetentionLockTimeout = TimeSpan.FromMinutes(1), RetentionCompactionMaxLiveJobs = 0 }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", RetentionCompactionMaxLiveJobs = 10_000 }.Validate();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(60_001)]
    public void Validate_rejects_retention_lock_timeout_out_of_range(int ms) =>
        Assert.Contains("RetentionLockTimeout", Assert.Throws<InvalidOperationException>(() =>
            new PgBoltOptions { ConnectionString = "Host=x", RetentionLockTimeout = TimeSpan.FromMilliseconds(ms) }.Validate()).Message);

    [Theory]
    [InlineData(-1)]
    [InlineData(10_001)]
    public void Validate_rejects_compaction_limit_out_of_range(int limit) =>
        Assert.Contains("RetentionCompactionMaxLiveJobs", Assert.Throws<InvalidOperationException>(() =>
            new PgBoltOptions { ConnectionString = "Host=x", RetentionCompactionMaxLiveJobs = limit }.Validate()).Message);

    public static TheoryData<TimeSpan> InvalidQueueSettingsCacheDuration =>
        [TimeSpan.Zero, TimeSpan.FromSeconds(-1), TimeSpan.FromDays(1) + TimeSpan.FromTicks(1)];

    [Theory]
    [MemberData(nameof(InvalidQueueSettingsCacheDuration))]
    public void Validate_rejects_queue_settings_cache_duration_out_of_range(TimeSpan duration) =>
        Assert.Contains("QueueSettingsCacheDuration", Assert.Throws<InvalidOperationException>(() =>
            new PgBoltOptions { ConnectionString = "Host=x", QueueSettingsCacheDuration = duration }.Validate()).Message);

    [Fact]
    public void Validate_accepts_null_and_positive_queue_settings_cache_duration()
    {
        new PgBoltOptions { ConnectionString = "Host=x", QueueSettingsCacheDuration = null }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", QueueSettingsCacheDuration = TimeSpan.FromMilliseconds(1) }.Validate();
        new PgBoltOptions { ConnectionString = "Host=x", QueueSettingsCacheDuration = TimeSpan.FromDays(1) }.Validate();
    }
}
