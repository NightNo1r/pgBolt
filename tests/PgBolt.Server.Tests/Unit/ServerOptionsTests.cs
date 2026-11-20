namespace PgBolt.Server.Tests.Unit;

public sealed class ServerOptionsTests
{
    private const string Server = "orders";

    private static PgBoltServerOptions Valid() => new PgBoltServerOptions { Workers = 4 }.Queue("default");

    private static void AssertInvalid(PgBoltServerOptions options, string field)
    {
        var e = Assert.Throws<InvalidOperationException>(() => options.Validate(Server));
        Assert.Contains($"'{Server}'", e.Message, StringComparison.Ordinal);
        Assert.Contains(field, e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Defaults_match_spec()
    {
        var o = new PgBoltServerOptions();
        Assert.Equal(Environment.ProcessorCount * 2, o.Workers);
        Assert.Equal(TimeSpan.FromSeconds(60), o.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(15), o.HeartbeatInterval);
        Assert.Equal(TimeSpan.FromSeconds(25), o.ShutdownTimeout);
        Assert.Null(o.DefaultJobTimeout);
        Assert.Equal(100, o.AckBatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(20), o.AckFlushInterval);
        Assert.Empty(o.Queues);
        Assert.Empty(o.FilterTypes);

        var q = Assert.Single(new PgBoltServerOptions().Queue("q").Queues);
        Assert.Equal("q", q.Name);
        Assert.Equal(0, q.Reserved);
        Assert.Null(q.MaxConcurrency);
        Assert.Equal(TimeSpan.FromMilliseconds(50), q.MinPollInterval);
        Assert.Equal(TimeSpan.FromSeconds(10), q.MaxPollInterval);

        var p = new PgBoltProcessingOptions();
        Assert.True(p.RunMaintenance);
        Assert.Equal(TimeSpan.FromSeconds(1), p.PromoterInterval);
        Assert.Equal(TimeSpan.FromSeconds(15), p.SweeperInterval);
        Assert.True(p.RunScheduler);
        Assert.Equal(TimeSpan.FromSeconds(1), p.SchedulerInterval);
        Assert.Null(p.MisfireThreshold);
        Assert.Equal(TimeSpan.FromMinutes(1), p.RecurringSyncInterval);
        Assert.Equal(TimeSpan.FromSeconds(1), p.ResolverInterval);
    }

    [Fact]
    public void Resolver_interval_is_validated_like_other_intervals()
    {
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { ResolverInterval = TimeSpan.Zero }.Validate());
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { ResolverInterval = TimeSpan.FromDays(2) }.Validate());
        new PgBoltProcessingOptions { ResolverInterval = TimeSpan.FromSeconds(10) }.Validate();
    }

    [Fact]
    public void Queue_settings_refresh_interval_is_validated_like_other_intervals()
    {
        Assert.Equal(TimeSpan.FromSeconds(10), new PgBoltProcessingOptions().QueueSettingsRefreshInterval);
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { QueueSettingsRefreshInterval = TimeSpan.Zero }.Validate());
        Assert.Throws<InvalidOperationException>(() =>
            new PgBoltProcessingOptions { QueueSettingsRefreshInterval = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1) }.Validate());
        new PgBoltProcessingOptions { QueueSettingsRefreshInterval = TimeSpan.FromDays(1) }.Validate();
    }

    [Fact]
    public void Explicit_misfire_threshold_must_cover_five_scheduler_intervals()
    {
        new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.FromSeconds(15) }.Validate();   // умолчание подстраивается
        Assert.Throws<InvalidOperationException>(() =>
            new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.FromSeconds(15), MisfireThreshold = TimeSpan.FromMinutes(1) }.Validate());
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { SchedulerInterval = TimeSpan.Zero }.Validate());
        Assert.Throws<InvalidOperationException>(() => new PgBoltProcessingOptions { RecurringSyncInterval = TimeSpan.Zero }.Validate());
    }

    [Fact]
    public void Valid_options_pass() => Valid().Validate(Server);

    [Fact]
    public void Queues_keep_declaration_order_and_configuration()
    {
        var o = new PgBoltServerOptions()
            .Queue("critical", q => q.Reserved = 5)
            .Queue("default")
            .Queue("emails", q => { q.MaxConcurrency = 4; q.MaxPollInterval = TimeSpan.FromSeconds(2); });

        Assert.Equal(["critical", "default", "emails"], o.Queues.Select(q => q.Name));
        Assert.Equal(5, o.Queues[0].Reserved);
        Assert.Equal(4, o.Queues[2].MaxConcurrency);
        Assert.Equal(TimeSpan.FromSeconds(2), o.Queues[2].MaxPollInterval);
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void Queue_rejects_blank_name(string name) =>
        Assert.Throws<ArgumentException>(() => new PgBoltServerOptions().Queue(name));

    [Fact]
    public void Queue_rejects_null_name() =>
        Assert.Throws<ArgumentNullException>(() => new PgBoltServerOptions().Queue(null!));

    [Fact]
    public void Capacity_is_workers_plus_reserved()
    {
        var o = new PgBoltServerOptions { Workers = 20 }
            .Queue("critical", q => q.Reserved = 5)
            .Queue("default")
            .Queue("bulk", q => q.Reserved = 2);
        Assert.Equal(27, o.Capacity);
    }

    [Fact]
    public void AddFilter_keeps_registration_order()
    {
        var o = new PgBoltServerOptions().AddFilter<FilterA>().AddFilter<FilterB>();
        Assert.Equal([typeof(FilterA), typeof(FilterB)], o.FilterTypes);
    }

    [Fact]
    public void Negative_workers_are_rejected() => AssertInvalid(Valid().With(o => o.Workers = -1), "Workers");

    [Fact]
    public void No_queues_are_rejected() => AssertInvalid(new PgBoltServerOptions { Workers = 1 }, "Queue");

    [Fact]
    public void Duplicate_queue_is_rejected()
    {
        var o = Valid().Queue("other").Queue("default");
        var e = Assert.Throws<InvalidOperationException>(() => o.Validate(Server));
        Assert.Contains($"'{Server}'", e.Message, StringComparison.Ordinal);
        Assert.Contains("'default'", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Zero_workers_without_reserved_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 0 }.Queue("a").Queue("b"), "Workers");

    [Fact]
    public void Zero_workers_with_reserved_is_valid()
    {
        var o = new PgBoltServerOptions { Workers = 0 }.Queue("a", q => q.Reserved = 1);
        o.Validate(Server);
        Assert.Equal(1, o.Capacity);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_lease_duration_is_rejected(int seconds) =>
        AssertInvalid(Valid().With(o => o.LeaseDuration = TimeSpan.FromSeconds(seconds)), "LeaseDuration");

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_heartbeat_interval_is_rejected(int seconds) =>
        AssertInvalid(Valid().With(o => o.HeartbeatInterval = TimeSpan.FromSeconds(seconds)), "HeartbeatInterval");

    [Fact]
    public void Heartbeat_interval_above_a_third_of_lease_is_rejected() =>
        AssertInvalid(Valid().With(o =>
        {
            o.LeaseDuration = TimeSpan.FromSeconds(30);
            o.HeartbeatInterval = TimeSpan.FromSeconds(10) + TimeSpan.FromTicks(1);
        }), "HeartbeatInterval");

    [Fact]
    public void Heartbeat_interval_exactly_a_third_of_lease_is_valid() =>
        Valid().With(o =>
        {
            o.LeaseDuration = TimeSpan.FromSeconds(30);
            o.HeartbeatInterval = TimeSpan.FromSeconds(10);
        }).Validate(Server);

    [Fact]
    public void Negative_shutdown_timeout_is_rejected() =>
        AssertInvalid(Valid().With(o => o.ShutdownTimeout = TimeSpan.FromTicks(-1)), "ShutdownTimeout");

    [Fact]
    public void Zero_shutdown_timeout_is_valid() => Valid().With(o => o.ShutdownTimeout = TimeSpan.Zero).Validate(Server);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_default_job_timeout_is_rejected(int seconds) =>
        AssertInvalid(Valid().With(o => o.DefaultJobTimeout = TimeSpan.FromSeconds(seconds)), "DefaultJobTimeout");

    [Fact]
    public void Positive_default_job_timeout_is_valid() =>
        Valid().With(o => o.DefaultJobTimeout = TimeSpan.FromMinutes(5)).Validate(Server);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_ack_batch_size_is_rejected(int size) =>
        AssertInvalid(Valid().With(o => o.AckBatchSize = size), "AckBatchSize");

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_ack_flush_interval_is_rejected(int ms) =>
        AssertInvalid(Valid().With(o => o.AckFlushInterval = TimeSpan.FromMilliseconds(ms)), "AckFlushInterval");

    [Fact]
    public void Negative_reserved_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.Reserved = -1), "Queue('q').Reserved");

    [Fact]
    public void Zero_max_concurrency_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.MaxConcurrency = 0), "Queue('q').MaxConcurrency");

    [Fact]
    public void Max_concurrency_below_reserved_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => { q.Reserved = 3; q.MaxConcurrency = 2; }),
            "Queue('q').MaxConcurrency");

    [Fact]
    public void Zero_cluster_max_concurrency_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.ClusterMaxConcurrency = 0),
            "Queue('q').ClusterMaxConcurrency must be null or >= 1.");

    [Theory]
    [InlineData(0)]
    [InlineData(1001)]
    public void Weight_out_of_range_is_rejected(int weight) =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.Weight = weight),
            "Queue('q').Weight must be null or in [1, 1000].");

    [Theory]
    [InlineData(1)]
    [InlineData(1000)]
    public void Weight_bounds_are_valid(int weight) =>
        new PgBoltServerOptions { Workers = 1 }.Queue("q", q => { q.Weight = weight; q.ClusterMaxConcurrency = 1; }).Validate(Server);

    [Fact]
    public void Max_concurrency_equal_to_reserved_is_valid() =>
        new PgBoltServerOptions { Workers = 1 }.Queue("q", q => { q.Reserved = 3; q.MaxConcurrency = 3; }).Validate(Server);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_min_poll_interval_is_rejected(int ms) =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q => q.MinPollInterval = TimeSpan.FromMilliseconds(ms)),
            "Queue('q').MinPollInterval");

    [Fact]
    public void Max_poll_interval_below_min_is_rejected() =>
        AssertInvalid(new PgBoltServerOptions { Workers = 1 }.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromSeconds(1);
            q.MaxPollInterval = TimeSpan.FromMilliseconds(999);
        }), "Queue('q').MaxPollInterval");

    [Fact]
    public void Max_poll_interval_equal_to_min_is_valid() =>
        new PgBoltServerOptions { Workers = 1 }.Queue("q", q =>
        {
            q.MinPollInterval = TimeSpan.FromSeconds(1);
            q.MaxPollInterval = TimeSpan.FromSeconds(1);
        }).Validate(Server);

    [Fact]
    public void Processing_defaults_are_valid() => new PgBoltProcessingOptions().Validate();

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_promoter_interval_is_rejected(int seconds)
    {
        var e = Assert.Throws<InvalidOperationException>(() =>
            new PgBoltProcessingOptions { PromoterInterval = TimeSpan.FromSeconds(seconds) }.Validate());
        Assert.Contains("PromoterInterval", e.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_sweeper_interval_is_rejected(int seconds)
    {
        var e = Assert.Throws<InvalidOperationException>(() =>
            new PgBoltProcessingOptions { SweeperInterval = TimeSpan.FromSeconds(seconds) }.Validate());
        Assert.Contains("SweeperInterval", e.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("LeaseDuration")]
    [InlineData("ShutdownTimeout")]
    [InlineData("AckFlushInterval")]
    [InlineData("Queue('q').MaxPollInterval")]
    public void Durations_above_one_day_are_rejected(string field)
    {
        var tooLong = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1);
        var o = new PgBoltServerOptions { Workers = 1 }.Queue("q", q =>
        {
            if (field == "Queue('q').MaxPollInterval")
                q.MaxPollInterval = tooLong;
        });
        switch (field)
        {
            case "LeaseDuration": o.LeaseDuration = tooLong; break;
            case "ShutdownTimeout": o.ShutdownTimeout = tooLong; break;
            case "AckFlushInterval": o.AckFlushInterval = tooLong; break;
        }

        AssertInvalid(o, field);
    }

    [Fact]
    public void Default_job_timeout_above_timer_limit_is_rejected() =>
        AssertInvalid(Valid().With(o => o.DefaultJobTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1) + TimeSpan.FromTicks(1)),
            "DefaultJobTimeout");

    [Fact]
    public void Maximum_durations_are_valid()
    {
        Assert.Equal(TimeSpan.FromDays(1), PgBoltServerOptions.MaxInterval);
        Assert.Equal(TimeSpan.FromMilliseconds(uint.MaxValue - 1), PgBoltServerOptions.MaxJobTimeout);

        new PgBoltServerOptions
        {
            Workers = 1,
            LeaseDuration = TimeSpan.FromDays(1),
            HeartbeatInterval = TimeSpan.FromHours(8),     // = LeaseDuration / 3: её граница следует из границы LeaseDuration
            ShutdownTimeout = TimeSpan.FromDays(1),
            AckFlushInterval = TimeSpan.FromDays(1),
            DefaultJobTimeout = PgBoltServerOptions.MaxJobTimeout,
        }.Queue("q", q => q.MaxPollInterval = TimeSpan.FromDays(1)).Validate(Server);

        new PgBoltProcessingOptions { PromoterInterval = TimeSpan.FromDays(1), SweeperInterval = TimeSpan.FromDays(1) }.Validate();
    }

    [Theory]
    [InlineData("PromoterInterval")]
    [InlineData("SweeperInterval")]
    public void Processing_intervals_above_one_day_are_rejected(string field)
    {
        var tooLong = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1);
        var options = field == "PromoterInterval"
            ? new PgBoltProcessingOptions { PromoterInterval = tooLong }
            : new PgBoltProcessingOptions { SweeperInterval = tooLong };

        var e = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.Contains(field, e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Maintenance_defaults_match_spec()
    {
        var p = new PgBoltProcessingOptions();
        Assert.Equal(TimeSpan.FromMinutes(1), p.MaintenancePollInterval);
        Assert.Equal(TimeSpan.FromHours(1), p.PartitionsInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), p.ServersCleanupInterval);
        Assert.Equal(TimeSpan.FromHours(1), p.RetentionInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), p.DeadServerTimeout);
    }

    [Theory]
    [InlineData("MaintenancePollInterval")]
    [InlineData("PartitionsInterval")]
    [InlineData("ServersCleanupInterval")]
    [InlineData("RetentionInterval")]
    [InlineData("DeadServerTimeout")]
    public void Maintenance_durations_must_be_positive_and_at_most_one_day(string field)
    {
        foreach (var bad in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1), TimeSpan.FromDays(1) + TimeSpan.FromTicks(1) })
        {
            var e = Assert.Throws<InvalidOperationException>(() => WithMaintenance(field, bad).Validate());
            Assert.Contains(field, e.Message, StringComparison.Ordinal);
        }
        WithMaintenance(field, TimeSpan.FromDays(1)).Validate();
    }

    private static PgBoltProcessingOptions WithMaintenance(string field, TimeSpan value)
    {
        var p = new PgBoltProcessingOptions();
        switch (field)
        {
            case "MaintenancePollInterval": p.MaintenancePollInterval = value; break;
            case "PartitionsInterval": p.PartitionsInterval = value; break;
            case "ServersCleanupInterval": p.ServersCleanupInterval = value; break;
            case "RetentionInterval": p.RetentionInterval = value; break;
            case "DeadServerTimeout": p.DeadServerTimeout = value; break;
        }
        return p;
    }

    private sealed class FilterA : IJobFilter
    {
        public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
    }

    private sealed class FilterB : IJobFilter
    {
        public ValueTask InvokeAsync(JobExecutingContext context, JobExecutionDelegate next) => next();
    }
}

internal static class ServerOptionsTestExtensions
{
    public static PgBoltServerOptions With(this PgBoltServerOptions options, Action<PgBoltServerOptions> change)
    {
        change(options);
        return options;
    }
}
