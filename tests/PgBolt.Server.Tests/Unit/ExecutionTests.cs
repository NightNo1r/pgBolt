using PgBolt.Postgres.Storage;
using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class ExecutionTests
{
    /// <summary>Theory-параметры — строки: internal-enum нельзя сделать параметром public-метода теста.</summary>
    private static ExecutionState S(string name) => Enum.Parse<ExecutionState>(name);

    private static Execution Create(ExecutionState state = ExecutionState.Running, long validUntil = 1_000) =>
        new(Leased.Job(), "q", validUntil, state);

    /// <summary>Переводит свежую запись в нужное состояние только разрешёнными переходами.</summary>
    private static Execution In(ExecutionState state)
    {
        var e = Create();
        switch (state)
        {
            case ExecutionState.Running:
                break;
            case ExecutionState.OutcomePending:
                Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
                break;
            case ExecutionState.OutcomeInFlight:
                Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.OutcomePending));
                Assert.True(e.TryTransition(ExecutionState.OutcomePending, ExecutionState.OutcomeInFlight));
                break;
            case ExecutionState.Removed:
                Assert.True(e.TryTransition(ExecutionState.Running, ExecutionState.Removed));
                break;
        }
        Assert.Equal(state, e.State);
        return e;
    }

    [Fact]
    public void Constructor_sets_identity_and_initial_state()
    {
        var job = Leased.Job();
        var e = new Execution(job, "emails", 42, ExecutionState.OutcomePending);

        Assert.Same(job, e.Job);
        Assert.Equal("emails", e.Queue);
        Assert.Equal(new LeaseRef(job.Id, job.LeaseId), e.Lease);
        Assert.Equal(42, e.ValidUntil);
        Assert.Equal(ExecutionState.OutcomePending, e.State);
        Assert.Equal(CancelReason.None, e.FirstReason);
        Assert.False(e.Token.IsCancellationRequested);
        Assert.Null(e.Outcome);
        Assert.Equal(0, e.OutcomeFirstAttemptAt);
    }

    [Theory]
    [InlineData("OutcomeInFlight")]
    [InlineData("Removed")]
    public void Constructor_rejects_states_an_execution_cannot_start_in(string state) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Create(S(state)));

    [Theory]
    [InlineData("Running", "OutcomePending")]
    [InlineData("OutcomePending", "OutcomeInFlight")]
    [InlineData("OutcomeInFlight", "OutcomePending")]
    [InlineData("Running", "Removed")]
    [InlineData("OutcomePending", "Removed")]
    [InlineData("OutcomeInFlight", "Removed")]
    public void Allowed_transition_succeeds_from_the_expected_state(string from, string to)
    {
        var e = In(S(from));
        Assert.True(e.TryTransition(S(from), S(to)));
        Assert.Equal(S(to), e.State);
    }

    [Theory]
    [InlineData("Running", "OutcomePending", "OutcomePending")]
    [InlineData("OutcomePending", "OutcomeInFlight", "Running")]
    [InlineData("OutcomePending", "Removed", "OutcomeInFlight")]
    [InlineData("Running", "Removed", "Removed")]
    public void Allowed_transition_from_another_state_returns_false_and_keeps_state(string from, string to, string actual)
    {
        var e = In(S(actual));
        Assert.False(e.TryTransition(S(from), S(to)));
        Assert.Equal(S(actual), e.State);
    }

    [Theory]
    [InlineData("Running", "OutcomeInFlight")]
    [InlineData("Running", "Running")]
    [InlineData("OutcomePending", "Running")]
    [InlineData("OutcomeInFlight", "Running")]
    [InlineData("Removed", "Running")]
    [InlineData("Removed", "OutcomePending")]
    [InlineData("Removed", "Removed")]
    public void Transition_outside_the_spec_throws(string from, string to)
    {
        var e = In(S(from));
        Assert.Throws<InvalidOperationException>(() => e.TryTransition(S(from), S(to)));
        Assert.Equal(S(from), e.State);
    }

    [Fact]
    public async Task Sink_and_heartbeat_race_on_outcome_pending_has_exactly_one_winner()
    {
        const int threads = 8;
        for (var round = 0; round < 500; round++)
        {
            var e = In(ExecutionState.OutcomePending);
            using var start = new Barrier(threads);
            var wins = new int[threads];

            var tasks = Enumerable.Range(0, threads).Select(i => Task.Factory.StartNew(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                var to = i % 2 == 0 ? ExecutionState.OutcomeInFlight : ExecutionState.Removed;
                if (e.TryTransition(ExecutionState.OutcomePending, to))
                    wins[i] = 1;
            }, TestContext.Current.CancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
            await Task.WhenAll(tasks);

            Assert.Equal(1, wins.Sum());
            var winner = Array.IndexOf(wins, 1);
            Assert.Equal(winner % 2 == 0 ? ExecutionState.OutcomeInFlight : ExecutionState.Removed, e.State);
        }
    }

    [Fact]
    public void First_cancel_reason_wins_and_token_is_cancelled()
    {
        var e = Create();

        Assert.True(e.Cancel(CancelReason.Timeout));
        Assert.True(e.Token.IsCancellationRequested);
        Assert.False(e.Cancel(CancelReason.Shutdown));
        Assert.False(e.Cancel(CancelReason.Timeout));

        Assert.Equal(CancelReason.Timeout, e.FirstReason);
        Assert.True(e.Token.IsCancellationRequested);
    }

    [Fact]
    public void Cancel_with_none_is_rejected()
    {
        var e = Create();
        Assert.Throws<ArgumentOutOfRangeException>(() => e.Cancel(CancelReason.None));
        Assert.False(e.Token.IsCancellationRequested);
    }

    [Fact]
    public void Cancel_does_not_depend_on_state()
    {
        var e = In(ExecutionState.Removed);
        Assert.True(e.Cancel(CancelReason.LeaseLost));
        Assert.True(e.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task Concurrent_cancels_record_exactly_one_first_reason()
    {
        CancelReason[] reasons = [CancelReason.Shutdown, CancelReason.CancelRequested, CancelReason.HeartbeatLost,
            CancelReason.LeaseLost, CancelReason.Timeout];
        for (var round = 0; round < 500; round++)
        {
            var e = Create();
            using var start = new Barrier(reasons.Length);
            var firsts = new bool[reasons.Length];

            await Task.WhenAll(reasons.Select((reason, i) => Task.Factory.StartNew(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                firsts[i] = e.Cancel(reason);
            }, TestContext.Current.CancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default)));

            Assert.Single(firsts, f => f);
            Assert.Equal(reasons[Array.IndexOf(firsts, true)], e.FirstReason);
            Assert.True(e.Token.IsCancellationRequested);
        }
    }

    [Fact]
    public void Valid_until_only_grows()
    {
        var e = Create(validUntil: 100);

        e.ExtendValidUntil(90);
        Assert.Equal(100, e.ValidUntil);
        e.ExtendValidUntil(150);
        Assert.Equal(150, e.ValidUntil);
        e.ExtendValidUntil(120);
        Assert.Equal(150, e.ValidUntil);
    }

    [Fact]
    public void Concurrent_extensions_keep_the_maximum()
    {
        var e = Create(validUntil: 0);
        Parallel.For(1, 10_001, i => e.ExtendValidUntil(i));
        Assert.Equal(10_000, e.ValidUntil);
    }
}
