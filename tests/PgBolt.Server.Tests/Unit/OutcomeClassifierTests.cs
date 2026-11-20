using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class OutcomeClassifierTests
{
    private static readonly RetryDecider Retry =
        new(RetryPolicy.Custom((attempt, _) => TimeSpan.FromSeconds(10 * attempt)), NullLogger.Instance);

    private static Execution Exec(int attempt = 1, int maxAttempts = 3, int[]? retryDelays = null, params CancelReason[] reasons)
    {
        var execution = new Execution(Leased.Job(attempt: attempt, maxAttempts: maxAttempts, retryDelays: retryDelays), "q", 0, ExecutionState.Running);
        foreach (var reason in reasons)
            execution.Cancel(reason);
        return execution;
    }

    private static JobOutcome Classify(Execution execution, Exception? error, RetryDecider retry, TimeSpan? timeout) =>
        OutcomeClassifier.Classify(execution, execution.FirstReason, error, retry, timeout);

    [Fact]
    public void Success_is_ack() =>
        Assert.IsType<JobOutcome.Ack>(Classify(Exec(), null, Retry, null));

    [Fact]
    public void Success_after_cancellation_is_still_ack() =>
        Assert.IsType<JobOutcome.Ack>(Classify(Exec(reasons: CancelReason.CancelRequested), null, Retry, null));

    [Theory]
    [InlineData("cancelled")]
    [InlineData("invalid")]
    [InlineData("aborted")]
    public void Cancel_requested_first_confirms_cancel_for_any_exception(string kind)
    {
        Exception error = kind switch
        {
            "cancelled" => new OperationCanceledException("stopped by user"),
            "invalid" => new InvalidOperationException("stopped by user"),
            _ => new JobAbortedException("stopped by user"),
        };
        var outcome = Classify(Exec(reasons: [CancelReason.CancelRequested, CancelReason.Shutdown]), error, Retry, null);
        Assert.Equal(new JobOutcome.ConfirmCancel("stopped by user"), outcome);
    }

    [Fact]
    public void Shutdown_first_with_operation_cancelled_releases_as_ran()
    {
        var outcome = Classify(Exec(reasons: CancelReason.Shutdown), new TaskCanceledException(), Retry, null);
        Assert.Equal(new JobOutcome.Release(Ran: true), outcome);
    }

    [Fact]
    public void Shutdown_first_with_other_exception_is_retried()
    {
        var outcome = Classify(Exec(reasons: CancelReason.Shutdown), new IOException("disk"), Retry, null);
        var retry = Assert.IsType<JobOutcome.Retry>(outcome);
        Assert.Equal((TimeSpan.FromSeconds(10), JobEventKind.Retry, "disk"), (retry.Delay, retry.Kind, retry.Message));
    }

    [Fact]
    public void Shutdown_after_cancel_requested_confirms_cancel()
    {
        var outcome = Classify(Exec(reasons: [CancelReason.CancelRequested, CancelReason.Shutdown]),
            new OperationCanceledException("cancelled"), Retry, null);
        Assert.IsType<JobOutcome.ConfirmCancel>(outcome);
    }

    [Fact]
    public void Aborted_and_argument_errors_fail_without_retry()
    {
        var aborted = Classify(Exec(), new JobAbortedException("pointless"), Retry, null);
        var args = Classify(Exec(), new JobArgumentsException("bad args", new JsonException("x")), Retry, null);

        var a = Assert.IsType<JobOutcome.Fail>(aborted);
        Assert.Equal((JobEventKind.Failed, "pointless"), (a.Kind, a.Message));
        Assert.Contains(nameof(JobAbortedException), a.Exception);
        Assert.Equal((JobEventKind.Failed, "bad args"), (((JobOutcome.Fail)args).Kind, ((JobOutcome.Fail)args).Message));
    }

    [Fact]
    public void Other_exception_is_retried_with_policy_delay_and_details()
    {
        var error = new InvalidOperationException("boom");
        var retry = Assert.IsType<JobOutcome.Retry>(Classify(Exec(attempt: 2), error, Retry, null));
        Assert.Equal((TimeSpan.FromSeconds(20), JobEventKind.Retry, "boom", error.ToString()),
            (retry.Delay, retry.Kind, retry.Message, retry.Exception));
    }

    [Fact]
    public void Job_retry_delays_take_precedence_over_policy()
    {
        var retry = Assert.IsType<JobOutcome.Retry>(Classify(Exec(retryDelays: [7]), new Exception("x"), Retry, null));
        Assert.Equal(TimeSpan.FromSeconds(7), retry.Delay);
    }

    [Fact]
    public void Last_attempt_fails()
    {
        var fail = Assert.IsType<JobOutcome.Fail>(Classify(Exec(attempt: 3, maxAttempts: 3), new Exception("x"), Retry, null));
        Assert.Equal((JobEventKind.Failed, "x"), (fail.Kind, fail.Message));
    }

    [Fact]
    public void Null_delay_from_policy_fails()
    {
        var never = new RetryDecider(RetryPolicy.Custom((_, _) => null), NullLogger.Instance);
        Assert.IsType<JobOutcome.Fail>(Classify(Exec(), new Exception("x"), never, null));
    }

    [Fact]
    public void Timeout_first_is_retried_with_timeout_message()
    {
        var outcome = Classify(Exec(reasons: CancelReason.Timeout), new TaskCanceledException(), Retry, TimeSpan.FromMilliseconds(2500));
        Assert.Equal("timeout after 2.5 s", Assert.IsType<JobOutcome.Retry>(outcome).Message);
    }

    [Fact]
    public void Heartbeat_lost_then_shutdown_spends_attempt()
    {
        var execution = Exec(reasons: [CancelReason.HeartbeatLost, CancelReason.Shutdown]);
        var retry = Assert.IsType<JobOutcome.Retry>(Classify(execution, new OperationCanceledException(), Retry, null));
        Assert.Equal((JobEventKind.Retry, "cancelled: lease renewal not confirmed"), (retry.Kind, retry.Message));
    }

    [Fact]
    public void Heartbeat_lost_on_last_attempt_fails()
    {
        var execution = Exec(attempt: 3, maxAttempts: 3, reasons: [CancelReason.HeartbeatLost, CancelReason.Shutdown]);
        var fail = Assert.IsType<JobOutcome.Fail>(Classify(execution, new OperationCanceledException(), Retry, null));
        Assert.Equal((JobEventKind.Failed, "cancelled: lease renewal not confirmed"), (fail.Kind, fail.Message));
    }

    [Fact]
    public void No_executor_retries_with_no_executor_kind_then_fails()
    {
        JobExecutorNotFoundException? seen = null;
        var decider = new RetryDecider(RetryPolicy.Custom((_, e) =>
        {
            seen = (JobExecutorNotFoundException)e;
            return TimeSpan.FromMinutes(1);
        }), NullLogger.Instance);

        var retry = Assert.IsType<JobOutcome.Retry>(OutcomeClassifier.NoExecutor(Exec(), new JobExecutorNotFoundException("test.job"), decider));
        Assert.Equal((TimeSpan.FromMinutes(1), JobEventKind.NoExecutor), (retry.Delay, retry.Kind));
        Assert.Equal("test.job", seen!.JobName);
        Assert.Contains("test.job", retry.Message);

        var fail = Assert.IsType<JobOutcome.Fail>(OutcomeClassifier.NoExecutor(Exec(attempt: 3, maxAttempts: 3),
            new JobExecutorNotFoundException("test.job"), decider));
        Assert.Equal(JobEventKind.NoExecutor, fail.Kind);
    }

    [Fact]
    public void Truncate_limits_to_8192_utf8_bytes_and_replaces_lone_surrogates()
    {
        Assert.Equal(8192, OutcomeClassifier.Truncate(new string('a', 10_000)).Length);
        Assert.Equal("short", OutcomeClassifier.Truncate("short"));
        Assert.Equal("", OutcomeClassifier.Truncate(null));

        var cyrillic = OutcomeClassifier.Truncate(new string('я', 5000));   // 2 байта на символ: 10 000 байт
        Assert.Equal(4096, cyrillic.Length);
        Assert.Equal(8192, Encoding.UTF8.GetByteCount(cyrillic));

        var text = new string('a', 8190) + "😀" + "tail";   // эмодзи — 4 байта: 8190 + 4 > 8192
        var cut = OutcomeClassifier.Truncate(text);
        Assert.Equal(8190, cut.Length);
        Assert.False(char.IsHighSurrogate(cut[^1]));

        var exact = new string('a', 8188) + "😀" + "tail";  // 8188 + 4 = 8192: пара помещается целиком
        Assert.Equal(8190, OutcomeClassifier.Truncate(exact).Length);

        // Одиночные суррогаты исходного текста (невалидный UTF-16) → U+FFFD; валидная пара остаётся.
        Assert.Equal("a\uFFFDb", OutcomeClassifier.Truncate("a\uD800b"));
        Assert.Equal("\uFFFDx😀\uFFFD", OutcomeClassifier.Truncate("\uDC00x😀\uD83D"));
        var strict = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        Assert.Equal(5, strict.GetByteCount(OutcomeClassifier.Truncate("a\uD800b")));   // строгий энкодер (как у Npgsql) не бросает

        var lone = OutcomeClassifier.Truncate(new string('\uD800', 3000));   // 3000 × U+FFFD = 9000 байт
        Assert.Equal(new string('\uFFFD', 2730), lone);                      // 2730 × 3 = 8190 ≤ 8192
    }

    [Fact]
    public void Long_messages_are_truncated_in_outcomes()
    {
        var retry = Assert.IsType<JobOutcome.Retry>(Classify(Exec(), new Exception(new string('m', 9000)), Retry, null));
        Assert.Equal(8192, retry.Message!.Length);
        Assert.Equal(8192, retry.Exception!.Length);
    }
}
