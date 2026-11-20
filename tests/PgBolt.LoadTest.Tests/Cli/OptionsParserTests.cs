using PgBolt.LoadTest.Cli;

namespace PgBolt.LoadTest.Tests.Cli;

public sealed class OptionsParserTests
{
    private static LoadTestOptions Ok(params string[] args)
    {
        var result = OptionsParser.Parse(args);
        Assert.Null(result.Error);
        return Assert.IsType<LoadTestOptions>(result.Options);
    }

    private static string Join(IEnumerable<LoadMode> modes) => string.Join(',', modes);

    private static string Fails(params string[] args)
    {
        var result = OptionsParser.Parse(args);
        Assert.Null(result.Options);
        return Assert.IsType<string>(result.Error);
    }

    [Fact]
    public void LimitedQueueOnlyForPartitions()
    {
        Assert.Null(OptionsParser.Parse(["partitions", "--queue-profile", "limited"]).Error);
        Assert.NotNull(OptionsParser.Parse(["sustained", "--queue-profile", "limited"]).Error);
        Assert.NotNull(OptionsParser.Parse(["partitions", "--queue-profile", "other"]).Error);
    }

    [Fact]
    public void Sustained_defaults()
    {
        var o = Ok("sustained");

        Assert.Equal(Scenario.Sustained, o.Scenario);
        Assert.Equal([LoadMode.Direct, LoadMode.Bouncer], o.Modes);
        Assert.Null(o.Connection);
        Assert.Null(o.BouncerConnection);
        Assert.Equal("benchmarks/PgBolt.LoadTest/results", o.Out);
        Assert.Equal(10_000, o.Rate);
        Assert.Equal(TimeSpan.FromMinutes(1), o.Warmup);
        Assert.Equal(TimeSpan.FromMinutes(10), o.Duration);
    }

    [Fact]
    public void Ramp_defaults()
    {
        var o = Ok("ramp");

        Assert.Equal(Scenario.Ramp, o.Scenario);
        Assert.Equal([LoadMode.Direct, LoadMode.Bouncer], o.Modes);
        Assert.Equal(2_000, o.RampStart);
        Assert.Equal(2_000, o.RampStep);
        Assert.Equal(TimeSpan.FromSeconds(60), o.RampStepDuration);
        Assert.Equal(40_000, o.RampMax);
    }

    [Fact]
    public void Partitions_defaults()
    {
        var o = Ok("partitions");

        Assert.Equal(Scenario.Partitions, o.Scenario);
        Assert.Equal([LoadMode.Direct, LoadMode.Bouncer, LoadMode.DirectPrepared, LoadMode.BouncerPrepared], o.Modes);
        Assert.Equal(10_000, o.Rate);
        Assert.Equal(TimeSpan.FromSeconds(30), o.Warmup);
        Assert.Equal(TimeSpan.FromMinutes(2), o.Duration);
        Assert.Equal([15, 60, 200, 400], o.Partitions);
        Assert.Equal([IndexSet.Current, IndexSet.Extra], o.Indexes);
    }

    [Fact]
    public void Vacuum_defaults()
    {
        var o = Ok("vacuum");

        Assert.Equal(Scenario.Vacuum, o.Scenario);
        Assert.Equal([LoadMode.Direct, LoadMode.Bouncer], o.Modes);
        Assert.Equal(10_000, o.Rate);
        Assert.Equal(TimeSpan.FromMinutes(2), o.Warmup);
        Assert.Equal(TimeSpan.FromMinutes(20), o.Phase1);
        Assert.Equal(TimeSpan.FromMinutes(5), o.HoldXmin);
        Assert.Equal(TimeSpan.FromMinutes(10), o.Phase3);
        Assert.Empty(o.QueueReloptions);
        Assert.Empty(o.PgSettings);
    }

    [Theory]
    [InlineData("sustained", "Direct")]
    [InlineData("vacuum", "Direct")]
    [InlineData("partitions", "Direct,DirectPrepared")]
    public void External_db_without_bouncer_defaults_to_direct_modes(string scenario, string expected)
    {
        var o = Ok(scenario, "--connection", "Host=x");

        Assert.Equal("Host=x", o.Connection);
        Assert.Equal(expected, Join(o.Modes));
    }

    [Theory]
    [InlineData("sustained", "Direct,Bouncer")]
    [InlineData("partitions", "Direct,Bouncer,DirectPrepared")]
    public void External_db_with_bouncer_defaults_to_both(string scenario, string expected)
    {
        var o = Ok(scenario, "--connection", "Host=x", "--bouncer-connection", "Host=y");

        Assert.Equal("Host=y", o.BouncerConnection);
        Assert.Equal(expected, Join(o.Modes));
    }

    [Theory]
    [InlineData("direct", "Direct")]
    [InlineData("bouncer", "Bouncer")]
    [InlineData("both", "Direct,Bouncer")]
    public void Explicit_mode(string value, string expected) =>
        Assert.Equal(expected, Join(Ok("sustained", "--mode", value).Modes));

    [Theory]
    [InlineData("direct-prepared", "DirectPrepared")]
    [InlineData("bouncer-prepared", "BouncerPrepared")]
    [InlineData("all", "Direct,Bouncer,DirectPrepared,BouncerPrepared")]
    public void Prepared_modes_are_allowed_in_partitions(string value, string expected) =>
        Assert.Equal(expected, Join(Ok("partitions", "--mode", value).Modes));

    [Theory]
    [InlineData("all")]
    [InlineData("direct-prepared")]
    [InlineData("bouncer-prepared")]
    public void Prepared_modes_outside_partitions_fail(string value) =>
        Fails("sustained", "--mode", value);

    [Fact]
    public void Unknown_mode_fails() => Fails("sustained", "--mode", "fast");

    [Fact]
    public void Flag_outside_its_scenario_fails()
    {
        Fails("sustained", "--hold-xmin", "1m");
        Fails("ramp", "--rate", "100");
        Fails("vacuum", "--duration", "1m");
        Fails("sustained", "--partitions", "15");
    }

    [Fact]
    public void Unknown_flag_or_scenario_fails()
    {
        Fails("sustained", "--nope", "1");
        Fails("nope");
        Fails();
    }

    [Fact]
    public void Flag_without_value_fails() => Fails("sustained", "--rate");

    [Fact]
    public void Bouncer_connection_without_connection_fails() =>
        Fails("sustained", "--bouncer-connection", "Host=y");

    [Fact]
    public void Pg_setting_with_connection_fails() =>
        Fails("vacuum", "--connection", "Host=x", "--pg-setting", "autovacuum_naptime=10s");

    [Fact]
    public void AcceptsSustainedPgSettings()
    {
        var options = Ok("sustained", "--pg-setting", "autovacuum_naptime=10s");
        Assert.Equal("10s", options.PgSettings["autovacuum_naptime"]);
    }

    [Fact]
    public void RejectsInvalidProfile()
    {
        Assert.Contains("stock", Fails("sustained", "--pg-profile", "fast"));
        Assert.Contains("--connection", Fails("sustained", "--connection", "Host=x", "--pg-profile", "bench"));
        Assert.Contains("--connection", Fails("sustained", "--connection", "Host=x", "--pg-setting", "work_mem=1MB"));
    }

    [Theory]
    [InlineData("bouncer")]
    [InlineData("both")]
    public void Bouncer_mode_with_connection_requires_bouncer_connection(string mode) =>
        Fails("sustained", "--connection", "Host=x", "--mode", mode);

    [Theory]
    [InlineData("5s", 5)]
    [InlineData("2m", 120)]
    [InlineData("1h", 3600)]
    public void Durations_are_parsed(string value, int seconds) =>
        Assert.Equal(TimeSpan.FromSeconds(seconds), Ok("sustained", "--warmup", value).Warmup);

    [Fact]
    public void Warmup_does_not_need_to_be_multiple_of_10_seconds() =>
        Assert.Equal(TimeSpan.FromSeconds(5), Ok("sustained", "--warmup", "5s").Warmup);

    [Theory]
    [InlineData("--duration")]
    [InlineData("--phase1")]
    [InlineData("--hold-xmin")]
    [InlineData("--phase3")]
    [InlineData("--ramp-step-duration")]
    public void Durations_not_multiple_of_10_seconds_fail(string flag)
    {
        var scenario = flag switch
        {
            "--duration" => "sustained",
            "--ramp-step-duration" => "ramp",
            _ => "vacuum",
        };

        Fails(scenario, flag, "45s");
    }

    [Fact]
    public void Duration_of_15_seconds_fails() => Fails("sustained", "--duration", "15s");

    [Fact]
    public void Ramp_step_shorter_than_30_seconds_fails() => Fails("ramp", "--ramp-step-duration", "20s");

    [Fact]
    public void Ramp_start_above_max_fails() => Fails("ramp", "--ramp-start", "1500", "--ramp-max", "1000");

    [Fact]
    public void Ramp_step_of_30_seconds_is_allowed() =>
        Assert.Equal(TimeSpan.FromSeconds(30), Ok("ramp", "--ramp-step-duration", "30s").RampStepDuration);

    [Theory]
    [InlineData("10")]
    [InlineData("s")]
    [InlineData("0s")]
    [InlineData("1d")]
    [InlineData("")]
    public void Malformed_duration_fails(string value) => Fails("sustained", "--warmup", value);

    [Fact]
    public void Short_smoke_options_are_parsed()
    {
        var o = Ok("vacuum", "--mode", "direct", "--rate", "500", "--warmup", "5s",
            "--phase1", "20s", "--hold-xmin", "10s", "--phase3", "20s");

        Assert.Equal([LoadMode.Direct], o.Modes);
        Assert.Equal(500, o.Rate);
        Assert.Equal(TimeSpan.FromSeconds(5), o.Warmup);
        Assert.Equal(TimeSpan.FromSeconds(20), o.Phase1);
        Assert.Equal(TimeSpan.FromSeconds(10), o.HoldXmin);
        Assert.Equal(TimeSpan.FromSeconds(20), o.Phase3);
    }

    [Fact]
    public void Partitions_and_indexes_are_lists()
    {
        var o = Ok("partitions", "--partitions", "15,20", "--indexes", "current");

        Assert.Equal([15, 20], o.Partitions);
        Assert.Equal([IndexSet.Current], o.Indexes);
    }

    [Theory]
    [InlineData("--partitions", "15,x")]
    [InlineData("--partitions", "0")]
    [InlineData("--indexes", "current,wide")]
    public void Malformed_list_fails(string flag, string value) => Fails("partitions", flag, value);

    [Theory]
    [InlineData("0")]
    [InlineData("-5")]
    [InlineData("many")]
    public void Non_positive_or_non_numeric_rate_fails(string value) => Fails("sustained", "--rate", value);

    [Fact]
    public void Ramp_numbers_are_parsed()
    {
        var o = Ok("ramp", "--ramp-start", "500", "--ramp-step", "500", "--ramp-max", "1000");

        Assert.Equal(500, o.RampStart);
        Assert.Equal(500, o.RampStep);
        Assert.Equal(1000, o.RampMax);
    }

    [Fact]
    public void Out_overrides_default() => Assert.Equal("/tmp/r", Ok("sustained", "--out", "/tmp/r").Out);

    [Fact]
    public void Queue_reloptions_and_pg_setting_become_dictionaries()
    {
        var o = Ok("vacuum",
            "--queue-reloptions", "autovacuum_vacuum_cost_delay=0",
            "--pg-setting", "autovacuum_naptime=10s,max_wal_size=2GB");

        Assert.Equal(new Dictionary<string, string> { ["autovacuum_vacuum_cost_delay"] = "0" }, o.QueueReloptions);
        Assert.Equal(
            new Dictionary<string, string> { ["autovacuum_naptime"] = "10s", ["max_wal_size"] = "2GB" },
            o.PgSettings);
    }

    [Fact]
    public void Setting_without_equals_sign_fails() => Fails("vacuum", "--pg-setting", "autovacuum_naptime");
}
