using Microsoft.CodeAnalysis.Testing;
using PgBolt.Generators.Analysis;
using static PgBolt.Generators.Tests.AnalyzerVerifier;

namespace PgBolt.Generators.Tests;

public sealed class QueueOptionsAnalyzerTests
{
    private static string Server(string body) => $$"""
        using System;
        using PgBolt.Server;

        public static class Setup
        {
            public static void Configure(QueueOptions q) => q.Weight = 5;

            public static void ConfigureCluster(QueueOptions q) => q.ClusterMaxConcurrency = 5;

            public static void M(PgBoltServerOptions s)
            {
                {{body}}
            }
        }
        """;

    private static string Postgres(string body) => $$"""
        using System;
        using PgBolt.Postgres;

        public static class Setup
        {
            public static void M(PgBoltOptions o)
            {
                {{body}}
            }
        }
        """;

    /// <summary>BOLT010 на вызове #<paramref name="at"/>, дополнительная локация — другой вызов #<paramref name="other"/>.</summary>
    private static DiagnosticResult Conflict(int at, int other, string option, string value, string otherValue) =>
        Diagnostic(Diagnostics.ConflictingQueueSetting).WithLocation(at).WithLocation(other).WithArguments("a", option, value, otherValue);

    private static DiagnosticResult Invalid(string option, string value, string reason) =>
        Diagnostic(Diagnostics.InvalidOptionValue).WithLocation(0).WithArguments(option, value, reason);

    [Theory]
    [InlineData("ClusterMaxConcurrency")]
    [InlineData("Weight")]
    public Task Different_constants_BOLT010(string option) => VerifyAsync(Server($$"""
        s.Queue("a", q => {|#0:q.{{option}} = 5|});
        s.Queue("a", q => {|#1:q.{{option}} = 3|});
        """),
        Conflict(0, 1, option, "5", "3"),
        Conflict(1, 0, option, "3", "5"));

    [Theory]
    [InlineData("ClusterMaxConcurrency", """s.Queue({|#1:"a"|}, q => q.Reserved = 1);""")]
    [InlineData("Weight", """s.Queue({|#1:"a"|}, q => q.Reserved = 1);""")]
    [InlineData("ClusterMaxConcurrency", """s.Queue({|#1:"a"|});""")]
    [InlineData("Weight", """s.Queue({|#1:"a"|});""")]
    [InlineData("ClusterMaxConcurrency", """s.Queue({|#1:"a"|}, null);""")]
    [InlineData("Weight", """s.Queue({|#1:"a"|}, null);""")]
    [InlineData("ClusterMaxConcurrency", """s.Queue("a", q => {|#1:q.ClusterMaxConcurrency = null|});""")]
    [InlineData("Weight", """s.Queue("a", q => {|#1:q.Weight = null|});""")]
    public Task Constant_against_null_BOLT010(string option, string nullCall) => VerifyAsync(Server($$"""
        s.Queue("a", q => {|#0:q.{{option}} = 5|});
        {{nullCall}}
        """),
        Conflict(0, 1, option, "5", "null"),
        Conflict(1, 0, option, "null", "5"));

    [Theory]
    [InlineData("ClusterMaxConcurrency")]
    [InlineData("Weight")]
    public Task Last_of_several_assignments_wins_BOLT010(string option) => VerifyAsync(Server($$"""
        s.Queue("a", q => { q.{{option}} = 3; {|#0:q.{{option}} = 5|}; });
        s.Queue("a", q => {|#1:q.{{option}} = 3|});
        """),
        Conflict(0, 1, option, "5", "3"),
        Conflict(1, 0, option, "3", "5"));

    [Theory]
    [InlineData("ClusterMaxConcurrency")]
    [InlineData("Weight")]
    public Task Equal_values_and_other_queues_are_silent(string option) => VerifyAsync(Server($$"""
        s.Queue("a", q => q.{{option}} = 5);
        s.Queue("a", q => { q.Reserved = 1; q.{{option}} = 5; });
        s.Queue("b", q => q.{{option}} = 3);
        """));

    [Theory]
    [InlineData("Configure", "Weight", "5")]
    [InlineData("Configure", "Weight", "3")]
    [InlineData("ConfigureCluster", "ClusterMaxConcurrency", "5")]
    [InlineData("ConfigureCluster", "ClusterMaxConcurrency", "3")]
    public Task Method_group_is_unknown(string method, string option, string value) => VerifyAsync(Server($$"""
        s.Queue("a", {{method}});
        s.Queue("a", q => q.{{option}} = {{value}});
        """));

    [Theory]
    [InlineData("ClusterMaxConcurrency", """Action<QueueOptions> c = q => q.ClusterMaxConcurrency = 5; s.Queue("a", c);""")]
    [InlineData("Weight", """Action<QueueOptions> c = q => q.Weight = 5; s.Queue("a", c);""")]
    [InlineData("ClusterMaxConcurrency", """s.Queue("a", q => { if (q.Reserved == 0) q.ClusterMaxConcurrency = 5; });""")]
    [InlineData("Weight", """s.Queue("a", q => { if (q.Reserved == 0) q.Weight = 5; });""")]
    [InlineData("ClusterMaxConcurrency", """s.Queue("a", q => { q.ClusterMaxConcurrency = 5; Console.WriteLine(); });""")]
    [InlineData("Weight", """s.Queue("a", q => { q.Weight = 5; Console.WriteLine(); });""")]
    [InlineData("ClusterMaxConcurrency", """int v = 5; s.Queue("a", q => q.ClusterMaxConcurrency = v);""")]
    [InlineData("Weight", """int v = 5; s.Queue("a", q => q.Weight = v);""")]
    [InlineData("ClusterMaxConcurrency", """s.Queue("a", q => { q.Reserved = q.Reserved + 1; q.ClusterMaxConcurrency = 5; });""")]
    [InlineData("Weight", """s.Queue("a", q => { q.Reserved = q.Reserved + 1; q.Weight = 5; });""")]
    public Task Unknown_value_is_silent(string option, string unknownCall) => VerifyAsync(Server($$"""
        {{unknownCall}}
        s.Queue("a", q => q.{{option}} = 3);
        """));

    [Fact]
    public Task Only_weight_differs_one_diagnostic_per_call_BOLT010() => VerifyAsync(Server("""
        s.Queue("a", q => { q.ClusterMaxConcurrency = 2; {|#0:q.Weight = 5|}; });
        s.Queue("a", q => { q.ClusterMaxConcurrency = 2; {|#1:q.Weight = 3|}; });
        """),
        Conflict(0, 1, "Weight", "5", "3"),
        Conflict(1, 0, "Weight", "3", "5"));

    [Fact]
    public Task Both_options_differ_two_diagnostics_per_call_BOLT010() => VerifyAsync(Server("""
        s.Queue("a", q => { {|#0:q.ClusterMaxConcurrency = 2|}; {|#1:q.Weight = 5|}; });
        s.Queue("a", q => { {|#2:q.ClusterMaxConcurrency = 4|}; {|#3:q.Weight = 3|}; });
        """),
        Conflict(0, 2, "ClusterMaxConcurrency", "2", "4"),
        Conflict(2, 0, "ClusterMaxConcurrency", "4", "2"),
        Conflict(1, 3, "Weight", "5", "3"),
        Conflict(3, 1, "Weight", "3", "5"));

    [Theory]
    [InlineData("Reserved", "-1", "must be >= 0")]
    [InlineData("MaxConcurrency", "0", "must be >= 1")]
    [InlineData("ClusterMaxConcurrency", "0", "must be >= 1")]
    [InlineData("ClusterMaxConcurrency", "-1", "must be >= 1")]
    [InlineData("Weight", "0", "must be in [1, 1000]")]
    [InlineData("Weight", "1001", "must be in [1, 1000]")]
    public Task Invalid_queue_option_BOLT011(string option, string value, string reason) => VerifyAsync(Server($$"""
        s.Queue("a", q => {|#0:q.{{option}} = {{value}}|});
        """),
        Invalid(option, value, reason));

    [Theory]
    [InlineData("Reserved", "0")]
    [InlineData("MaxConcurrency", "1")]
    [InlineData("MaxConcurrency", "null")]
    [InlineData("ClusterMaxConcurrency", "1")]
    [InlineData("Weight", "1")]
    [InlineData("Weight", "1000")]
    public Task Valid_queue_option_is_silent(string option, string value) => VerifyAsync(Server($$"""
        s.Queue("a", q => q.{{option}} = {{value}});
        """));

    [Fact]
    public Task Max_concurrency_below_reserved_in_same_lambda_BOLT011() => VerifyAsync(Server("""
        s.Queue("a", q => { q.Reserved = 3; {|#0:q.MaxConcurrency = 2|}; });
        s.Queue("b", q => { q.Reserved = 3; q.MaxConcurrency = 3; });
        s.Queue("c", q => q.MaxConcurrency = 2);
        """),
        Invalid("MaxConcurrency", "2", "must be >= Reserved (3)"));

    [Theory]
    [InlineData("TimeSpan.Zero")]
    [InlineData("TimeSpan.FromSeconds(0)")]
    [InlineData("TimeSpan.FromSeconds(-1)")]
    [InlineData("new TimeSpan(0, 0, -1)")]
    public Task Non_positive_cache_duration_BOLT011(string value) => VerifyAsync(Postgres($$"""
        {|#0:o.QueueSettingsCacheDuration = {{value}}|};
        """),
        Invalid("QueueSettingsCacheDuration", value, "must be > 0 (use null to disable the cache)"));

    [Theory]
    [InlineData("null")]
    [InlineData("TimeSpan.FromSeconds(1)")]
    [InlineData("new TimeSpan(0, 0, 1)")]
    public Task Positive_or_null_cache_duration_is_silent(string value) => VerifyAsync(Postgres($$"""
        o.QueueSettingsCacheDuration = {{value}};
        """));

    [Fact]
    public Task Without_server_assembly_queue_rules_are_silent() => VerifyAsync(Server("""
        s.Queue("a", q => q.Weight = 5);
        s.Queue("a", q => q.Weight = 3);
        """), server: false);

    [Fact]
    public Task Without_server_assembly_cache_duration_is_still_checked_BOLT011() => VerifyAsync(Postgres("""
        {|#0:o.QueueSettingsCacheDuration = TimeSpan.Zero|};
        """), server: false,
        Invalid("QueueSettingsCacheDuration", "TimeSpan.Zero", "must be > 0 (use null to disable the cache)"));
}
