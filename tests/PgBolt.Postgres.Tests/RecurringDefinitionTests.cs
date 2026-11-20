using PgBolt.Postgres.Recurring;

namespace PgBolt.Postgres.Tests;

public sealed class RecurringDefinitionTests
{
    private static RecurringDefinition Create(string cron = "0 3 * * *", RecurringJobOptions? options = null, string id = "daily") =>
        RecurringDefinition.Create(id, "rec.job", """{"N":1}""", cron, options);

    [Fact]
    public void Defaults_are_normalized()
    {
        var d = Create();
        Assert.Equal(("daily", "rec.job", """{"N":1}""", "0 3 * * *", "UTC"), (d.Id, d.Type, d.ArgsJson, d.Cron, d.TimeZone));
        Assert.Equal((MisfirePolicy.FireOnce, TimeSpan.FromDays(1), OverlapPolicy.Skip), (d.Misfire, d.CatchUpWindow, d.Overlap));
        Assert.Null(d.Queue);
        Assert.Null(d.Priority);
        Assert.Null(d.MaxAttempts);
        Assert.Null(d.RetryDelaysSeconds);
    }

    [Fact]
    public void Options_are_normalized_to_stored_form()
    {
        var d = Create(options: new RecurringJobOptions
        {
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Russian Standard Time"),
            CatchUpWindow = TimeSpan.FromDays(1) + TimeSpan.FromTicks(5),      // доли микросекунды усекаются (точность interval)
            RetryDelays = [TimeSpan.FromSeconds(1.5), TimeSpan.Zero],          // секунды вверх, как EnqueueOptionsResolver
            Queue = "q", Priority = 3, MaxAttempts = 2, Misfire = MisfirePolicy.CatchUp, Overlap = OverlapPolicy.Allow,
        });

        Assert.Equal("Europe/Moscow", d.TimeZone);
        Assert.Equal(TimeSpan.FromDays(1), d.CatchUpWindow);
        Assert.Equal([2, 0], d.RetryDelaysSeconds!);
        Assert.Equal(("q", (short?)3, (int?)2), (d.Queue, d.Priority, d.MaxAttempts));
        Assert.Equal(d, RecurringDefinition.Create(d.Id, d.Type, d.ArgsJson, d.Cron, new RecurringJobOptions   // идемпотентно
        {
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow"), CatchUpWindow = d.CatchUpWindow,
            RetryDelays = [TimeSpan.FromSeconds(2), TimeSpan.Zero], Queue = "q", Priority = 3, MaxAttempts = 2,
            Misfire = MisfirePolicy.CatchUp, Overlap = OverlapPolicy.Allow,
        }));
    }

    [Theory]
    [InlineData("")]
    [InlineData("bad cron")]
    [InlineData("* * * *")]
    [InlineData("0 0 30 2 *")]          // нет будущих срабатываний
    public void Invalid_cron_is_rejected(string cron) =>
        Assert.Throws<ArgumentException>(() => Create(cron));

    [Fact]
    public void Id_length_is_checked()
    {
        Assert.Throws<ArgumentException>(() => Create(id: ""));
        Assert.Throws<ArgumentException>(() => Create(id: new string('x', 201)));
        Assert.Equal(200, Create(id: new string('x', 200)).Id.Length);
    }

    public static TheoryData<RecurringJobOptions> InvalidOptions() =>
    [
        new RecurringJobOptions { CatchUpWindow = TimeSpan.Zero },
        new RecurringJobOptions { CatchUpWindow = TimeSpan.FromTicks(5) },                 // после усечения — 0
        new RecurringJobOptions { CatchUpWindow = TimeSpan.FromDays(367) },
        new RecurringJobOptions { Queue = "" },
        new RecurringJobOptions { MaxAttempts = 0 },
        new RecurringJobOptions { RetryDelays = [TimeSpan.FromSeconds(-1)] },
        new RecurringJobOptions { Misfire = (MisfirePolicy)7 },
        new RecurringJobOptions { Overlap = (OverlapPolicy)7 },
    ];

    [Theory]
    [MemberData(nameof(InvalidOptions))]
    public void Invalid_options_are_rejected(RecurringJobOptions options) =>
        Assert.Throws<ArgumentException>(() => Create(options: options));

    [Fact]
    public void Validate_checks_everything_but_type_and_args()
    {
        RecurringDefinition.Validate("daily", "0 3 * * *", null);
        Assert.Throws<ArgumentException>(() => RecurringDefinition.Validate("daily", "bad", null));
    }
}
