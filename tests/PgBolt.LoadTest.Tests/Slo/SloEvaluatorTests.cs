using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Measure;
using PgBolt.LoadTest.Slo;
using static PgBolt.LoadTest.Tests.Slo.SloTestData;

namespace PgBolt.LoadTest.Tests.Slo;

public sealed class SloEvaluatorTests
{
    private const string StandMark = "ограничено стендом";

    // У SLO 4 и SLO 9 по два вердикта: первый — проверка по срезам или окнам, второй — сравнение минут.
    private static SloVerdict Part(IReadOnlyList<SloVerdict> verdicts, int number, int part) =>
        verdicts.Where(v => v.Number == number).ElementAt(part);

    private static RunData WithBacklog(RunData data, Func<Sample, bool> match, long backlog) =>
        data with { Samples = Replace(data.Samples, match, s => s with { ReadyBacklog = backlog }) };

    [Fact]
    public void Good_run_passes_all()
    {
        var verdicts = SloEvaluator.Sustained(Sustained());

        Assert.Equal([1, 2, 3, 4, 4, 5, 6], verdicts.Select(v => v.Number));
        Assert.All(verdicts, v => Assert.Equal(Verdict.Pass, v.Result));
        Assert.All(verdicts, v => Assert.Null(v.Mark));
    }

    [Theory]
    [InlineData(95_000, "Pass")]
    [InlineData(94_050, "Fail")]
    public void Slo1_completed_threshold(long completed, string expected)
    {
        var data = Sustained();
        data = data with { Windows = Replace(data.Windows, w => w.Index == 30, w => w with { Completed = completed }) };

        var slo1 = Single(SloEvaluator.Sustained(data), 1);

        Assert.Equal(expected, slo1.Result.ToString());
        Assert.Null(slo1.Mark);
    }

    [Fact]
    public void Underloaded_window_fails_slo1_with_enqueue_mark()
    {
        Assert.True(SloEvaluator.IsUnderloaded(Window(0, enqueued: 97_900), Rate));
        Assert.False(SloEvaluator.IsUnderloaded(Window(0, enqueued: 98_000), Rate));

        var data = Sustained();
        data = data with { Windows = Replace(data.Windows, w => w.Index == 30, w => w with { Enqueued = 97_900 }) };

        var slo1 = Single(SloEvaluator.Sustained(data), 1);

        Assert.Equal(Verdict.Fail, slo1.Result);
        Assert.Equal("enqueue", slo1.Mark);
    }

    [Theory]
    [InlineData(50, 500, 2_000, "Pass")]
    [InlineData(50.5, 500, 2_000, "Fail")]
    [InlineData(50, 505, 2_000, "Fail")]
    [InlineData(50, 500, 2_020, "Fail")]
    public void Slo2_thresholds(double p50, double p99, double p999, string expected)
    {
        var data = Sustained() with { Immediate = new LatencySummary(1_000, p50, p50, p99, p999, p999) };

        Assert.Equal(expected, Single(SloEvaluator.Sustained(data), 2).Result.ToString());
    }

    [Theory]
    [InlineData(1_500, "Pass")]
    [InlineData(1_515, "Fail")]
    public void Slo3_threshold(double p99, string expected)
    {
        var data = Sustained() with { Delayed = new LatencySummary(1_000, 100, 200, p99, p99, p99) };

        Assert.Equal(expected, Single(SloEvaluator.Sustained(data), 3).Result.ToString());
    }

    [Theory]
    [InlineData(10_000, "Pass")]
    [InlineData(10_100, "Fail")]
    public void Slo4_each_sample_threshold(long backlog, string expected)
    {
        var data = WithBacklog(Sustained(), s => s.At == TimeSpan.FromSeconds(300), backlog);

        Assert.Equal(expected, Part(SloEvaluator.Sustained(data), 4, 0).Result.ToString());
    }

    [Theory]
    [InlineData(10_000, "Pass")]
    [InlineData(10_100, "Fail")]
    public void Slo4_growth_threshold(long lastMinuteBacklog, string expected)
    {
        var data = WithBacklog(Sustained(), _ => true, 0);
        data = WithBacklog(data, s => s.At >= TimeSpan.FromSeconds(600), lastMinuteBacklog);

        Assert.Equal(expected, Part(SloEvaluator.Sustained(data), 4, 1).Result.ToString());
    }

    [Fact]
    public void Slo4_second_part_uses_minute_means()
    {
        var data = WithBacklog(Sustained(), _ => true, 0);
        // Середина окна замера в сравнение минут не входит.
        data = WithBacklog(data, s => s.At >= TimeSpan.FromSeconds(120) && s.At < TimeSpan.FromSeconds(600), 50_000);
        // Последняя минута: каждый второй срез выше первой минуты + rate, среднее — 7 500.
        data = WithBacklog(data, s => s.At >= TimeSpan.FromSeconds(600) && (int)s.At.TotalSeconds % 2 == 0, 15_000);

        var verdicts = SloEvaluator.Sustained(data);

        Assert.Equal(Verdict.Fail, Part(verdicts, 4, 0).Result);
        Assert.Equal(Verdict.Pass, Part(verdicts, 4, 1).Result);
    }

    [Fact]
    public void Slo4_first_minute_starts_at_measure_start()
    {
        var data = WithBacklog(Sustained(), _ => true, 5_000);
        data = WithBacklog(data, s => s.At >= TimeSpan.FromSeconds(600), 14_000);
        // Прогрев с пустым backlog: если бы первая минута начиналась с него, рост 14 000 превысил бы rate.
        data = data with { Samples = [.. Samples(0, 60).Select(s => s with { ReadyBacklog = 0 }), .. data.Samples] };

        Assert.Equal(Verdict.Pass, Part(SloEvaluator.Sustained(data), 4, 1).Result);
    }

    [Fact]
    public void Slo4_growth_on_measure_shorter_than_two_minutes_is_no_data()
    {
        var data = Sustained() with { MeasureEnd = TimeSpan.FromSeconds(170) };

        Assert.Equal(Verdict.NoData, Part(SloEvaluator.Sustained(data), 4, 1).Result);
    }

    [Fact]
    public void Slo5_reports_excluded_failed_calls()
    {
        var data = Sustained() with { Correctness = Correct with { ExcludedFailedCalls = 7 } };

        var slo5 = Single(SloEvaluator.Sustained(data), 5);

        Assert.Equal(Verdict.Pass, slo5.Result);
        Assert.Contains("7", slo5.Value);
    }

    [Theory]
    [InlineData(1, true)]
    [InlineData(0, false)]
    public void Slo5_nonempty_queue_after_drain_fails(long queueRows, bool drainedInTime)
    {
        var data = Sustained() with
        {
            Correctness = Correct with { QueueRowsAfterDrain = queueRows, DrainedInTime = drainedInTime },
        };

        Assert.Equal(Verdict.Fail, Single(SloEvaluator.Sustained(data), 5).Result);
    }

    [Fact]
    public void Slo5_requires_two_runs_for_fail_first()
    {
        // Задача с FailFirstAttempt выполнена один раз: счётчик даёт пропуск, а jobs.attempt = 1 — расхождение.
        var missing = Sustained() with { Correctness = Correct with { MissingExecutions = 1 } };
        var attempt = Sustained() with { Correctness = Correct with { AttemptMismatches = 1 } };

        Assert.Equal(Verdict.Fail, Single(SloEvaluator.Sustained(missing), 5).Result);
        Assert.Equal(Verdict.Fail, Single(SloEvaluator.Sustained(attempt), 5).Result);
    }

    [Theory]
    [InlineData(1, 0, 0, 0)]
    [InlineData(0, 1, 0, 0)]
    [InlineData(0, 0, 1, 0)]
    [InlineData(0, 0, 0, 1)]
    public void Slo5_other_discrepancies_fail(long duplicated, long notSucceeded, long dropped, long storageErrors)
    {
        var data = Sustained() with
        {
            Correctness = Correct with
            {
                DuplicatedExecutions = duplicated, JobsNotSucceeded = notSucceeded,
                OutcomeDropped = dropped, StorageErrors = storageErrors,
            },
        };

        Assert.Equal(Verdict.Fail, Single(SloEvaluator.Sustained(data), 5).Result);
    }

    [Fact]
    public void Warmup_windows_are_not_evaluated()
    {
        var data = Sustained();
        data = data with
        {
            Windows = [.. Windows(0, 60).Select(w => w with { Enqueued = 0, Completed = 0 }), .. data.Windows],
            Samples = [.. Samples(0, 60).Select(s => s with { ReadyBacklog = 50_000 }), .. data.Samples],
        };

        var verdicts = SloEvaluator.Sustained(data);

        Assert.Equal(Verdict.Pass, Single(verdicts, 1).Result);
        Assert.Equal(Verdict.Pass, Part(verdicts, 4, 0).Result);
        Assert.Equal(Verdict.Pass, Part(verdicts, 4, 1).Result);
    }

    [Fact]
    public void Drain_windows_are_not_evaluated()
    {
        var data = Sustained();
        data = data with
        {
            Windows = [.. data.Windows, .. Windows(660, 780).Select(w => w with { Enqueued = 0, Completed = 10, CpuFraction = 0.99 })],
            Samples = [.. data.Samples, .. Samples(660, 780).Select(s => s with { ReadyBacklog = 50_000 })],
        };

        var verdicts = SloEvaluator.Sustained(data);

        Assert.Equal(Verdict.Pass, Single(verdicts, 1).Result);
        Assert.Equal(Verdict.Pass, Part(verdicts, 4, 0).Result);
        Assert.Equal(Verdict.Pass, Part(verdicts, 4, 1).Result);
        Assert.All(verdicts, v => Assert.Null(v.Mark));
        Assert.False(SloEvaluator.IsCpuBound(data));
    }

    [Fact]
    public void Window_ending_after_measure_end_is_not_evaluated()
    {
        var data = Sustained() with { MeasureEnd = TimeSpan.FromSeconds(655) };
        data = data with { Windows = Replace(data.Windows, w => w.Index == 65, w => w with { Completed = 0 }) };

        Assert.Equal(Verdict.Pass, Single(SloEvaluator.Sustained(data), 1).Result);
    }

    [Theory]
    [InlineData(100_000, "Pass")]
    [InlineData(101_000, "Fail")]
    public void Slo6_bouncer_maxwait(long maxWaitMicros, string expected)
    {
        var data = Sustained(LoadMode.Bouncer);
        data = data with
        {
            Samples = Replace(data.Samples, s => s.At == TimeSpan.FromSeconds(300),
                s => s with { MaxWaitMicros = maxWaitMicros, Connections = 100 }),
        };
        data = data with { Samples = Replace(data.Samples, s => s.At == TimeSpan.FromSeconds(301), s => s with { MaxWaitMicros = null }) };

        Assert.Equal(expected, Single(SloEvaluator.Sustained(data), 6).Result.ToString());
    }

    [Theory]
    [InlineData(48, "Pass")]
    [InlineData(49, "Fail")]
    public void Slo6_direct_connections(int connections, string expected)
    {
        var data = Sustained();
        data = data with
        {
            Samples = Replace(data.Samples, s => s.At == TimeSpan.FromSeconds(300),
                s => s with { Connections = connections, MaxWaitMicros = 10_000_000 }),
        };

        Assert.Equal(expected, Single(SloEvaluator.Sustained(data), 6).Result.ToString());
    }

    [Fact]
    public void Cpu_bound_measure_windows_mark_verdicts()
    {
        var warmupOnly = Sustained();
        warmupOnly = warmupOnly with { Windows = [.. Windows(0, 60).Select(w => w with { CpuFraction = 0.99 }), .. warmupOnly.Windows] };

        Assert.False(SloEvaluator.IsCpuBound(warmupOnly));
        Assert.All(SloEvaluator.Sustained(warmupOnly), v => Assert.Null(v.Mark));

        // 7 из 60 окон замера (11,7 %) выше 80 %; одно из них недогружено; сверки корректности нет.
        var bound = Sustained() with { Correctness = null };
        bound = bound with { Windows = Replace(bound.Windows, w => w.Index is >= 10 and < 17, w => w with { CpuFraction = 0.9 }) };
        bound = bound with { Windows = Replace(bound.Windows, w => w.Index == 10, w => w with { Enqueued = 0 }) };

        var verdicts = SloEvaluator.Sustained(bound);

        Assert.True(SloEvaluator.IsCpuBound(bound));
        Assert.Equal("enqueue, " + StandMark, Single(verdicts, 1).Mark);
        Assert.Equal(Verdict.NoData, Single(verdicts, 5).Result);
        Assert.All(verdicts.Where(v => v.Number != 1), v => Assert.Equal(StandMark, v.Mark));

        // 13 из 120 окон фаз 1–3.
        var vacuum = Vacuum();
        vacuum = vacuum with { Windows = Replace(vacuum.Windows, w => w.Index is >= 72 and < 85, w => w with { CpuFraction = 0.9 }) };

        Assert.All(SloEvaluator.Vacuum(vacuum), v => Assert.Equal(StandMark, v.Mark));
    }

    [Fact]
    public void Empty_inputs_give_no_data()
    {
        var empty = new RunData(Rate, [], [], Empty, Empty, null, LoadMode.Bouncer, null,
            TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(660));

        Assert.All(SloEvaluator.Sustained(empty), v => Assert.Equal(Verdict.NoData, v.Result));
        Assert.All(SloEvaluator.Sustained(empty with { Mode = LoadMode.Direct }), v => Assert.Equal(Verdict.NoData, v.Result));

        var nullSamples = Sustained(LoadMode.Bouncer) with
        {
            Samples = Samples(60, 660).Select(s => new Sample(s.At, null, null, null, null, null)).ToList(),
        };
        var verdicts = SloEvaluator.Sustained(nullSamples);
        Assert.Equal(Verdict.NoData, Part(verdicts, 4, 0).Result);
        Assert.Equal(Verdict.NoData, Part(verdicts, 4, 1).Result);
        Assert.Equal(Verdict.NoData, Single(verdicts, 6).Result);

        var vacuum = Vacuum() with { Windows = [], Samples = [] };
        Assert.All(SloEvaluator.Vacuum(vacuum), v => Assert.Equal(Verdict.NoData, v.Result));
    }

    [Fact]
    public void Vacuum_evaluates_slo6_not_slo5()
    {
        var data = Vacuum();

        var verdicts = SloEvaluator.Vacuum(data);

        Assert.Equal([6, 7, 8, 9, 9], verdicts.Select(v => v.Number));
        Assert.All(verdicts, v => Assert.Equal(Verdict.Pass, v.Result));

        // SLO 6 — по срезам фаз 1–3: прогрев не входит, фаза 2 входит.
        var warmup = data with { Samples = Replace(data.Samples, s => s.At == TimeSpan.FromSeconds(50), s => s with { Connections = 100 }) };
        var phase2 = data with { Samples = Replace(data.Samples, s => s.At == TimeSpan.FromSeconds(500), s => s with { Connections = 49 }) };

        Assert.Equal(Verdict.Pass, Single(SloEvaluator.Vacuum(warmup), 6).Result);
        Assert.Equal(Verdict.Fail, Single(SloEvaluator.Vacuum(phase2), 6).Result);
    }

    [Theory]
    [InlineData(200, "Pass")]
    [InlineData(202, "Fail")]
    public void Slo7_compares_last_minute_to_60_120s(long lastMinuteBytes, string expected)
    {
        var data = Vacuum();
        long Size(TimeSpan at) => at.TotalSeconds switch
        {
            < 180 => 10, // первая минута фазы 1 в сравнение не входит
            < 240 => 100,
            >= 360 and < 420 => lastMinuteBytes,
            _ => 1_000_000,
        };
        data = data with { Samples = data.Samples.Select(s => s with { QueueTotalBytes = Size(s.At) }).ToList() };

        Assert.Equal(expected, Single(SloEvaluator.Vacuum(data), 7).Result.ToString());
    }

    [Fact]
    public void Slo7_short_phase_no_data()
    {
        var phases = VacuumPhases with { Phase2Start = TimeSpan.FromSeconds(290) };

        Assert.Equal(Verdict.NoData, Single(SloEvaluator.Vacuum(Vacuum(phases)), 7).Result);
        Assert.Equal(Verdict.Pass, Single(SloEvaluator.Vacuum(Vacuum(VacuumPhases with { Phase2Start = TimeSpan.FromSeconds(300) })), 7).Result);
    }

    [Fact]
    public void Slo8_per_window_percentiles()
    {
        var data = Vacuum();
        var slowImmediate = GoodImmediate with { P99Ms = 600 };
        var lateDelayed = GoodDelayed with { P99Ms = 1_600 };

        // Общие перцентили прогона в порядке; нарушение — только в одном окне фазы 1.
        var immediate = data with { Windows = Replace(data.Windows, w => w.Index == 20, w => w with { Immediate = slowImmediate }) };
        var delayed = data with { Windows = Replace(data.Windows, w => w.Index == 20, w => w with { Delayed = lateDelayed }) };
        // То же нарушение в фазе 2 на SLO 8 не влияет.
        var phase2 = data with { Windows = Replace(data.Windows, w => w.Index == 50, w => w with { Immediate = slowImmediate }) };

        Assert.Equal(Verdict.Fail, Single(SloEvaluator.Vacuum(immediate), 8).Result);
        Assert.Equal(Verdict.Fail, Single(SloEvaluator.Vacuum(delayed), 8).Result);
        Assert.Equal(Verdict.Pass, Single(SloEvaluator.Vacuum(phase2), 8).Result);
    }

    [Fact]
    public void Slo8_underloaded_window_fails_with_enqueue_mark()
    {
        var data = Vacuum();
        data = data with { Windows = Replace(data.Windows, w => w.Index == 20, w => w with { Enqueued = 0 }) };

        var slo8 = Single(SloEvaluator.Vacuum(data), 8);

        Assert.Equal(Verdict.Fail, slo8.Result);
        Assert.Equal("enqueue", slo8.Mark);
    }

    [Theory]
    // Фаза 3 начинается в 720 с, конец — 1 320 с; T = начало окна после последнего плохого.
    [InlineData(1_010, "Pass", "Pass")] // T = 1 020 = фаза 3 + 5 мин
    [InlineData(1_020, "Fail", "Pass")] // T = 1 030
    [InlineData(1_200, "Fail", "NoData")] // T = 1 210, до конца фазы 110 с < 2 мин
    [InlineData(1_310, "Fail", "NoData")] // последнее окно фазы 3 плохое: не восстановилось
    public void Slo9_recovery_moment(double lastBadWindowStart, string recovery, string growth)
    {
        var data = Vacuum();
        data = data with
        {
            Windows = Replace(data.Windows,
                w => w.Start >= TimeSpan.FromSeconds(720) && w.Start <= TimeSpan.FromSeconds(lastBadWindowStart),
                w => w with { Completed = 50_000 }),
        };

        var verdicts = SloEvaluator.Vacuum(data);

        Assert.Equal(recovery, Part(verdicts, 9, 0).Result.ToString());
        Assert.Equal(growth, Part(verdicts, 9, 1).Result.ToString());
    }

    [Fact]
    public void Slo9_window_with_backlog_or_p99_violation_is_not_recovered()
    {
        var data = Vacuum();
        var backlog = WithBacklog(data, s => s.At == TimeSpan.FromSeconds(1_025), 10_001);
        var p99 = data with
        {
            Windows = Replace(data.Windows, w => w.Start == TimeSpan.FromSeconds(1_020),
                w => w with { Immediate = GoodImmediate with { P99Ms = 501 } }),
        };
        var underloaded = data with
        {
            Windows = Replace(data.Windows, w => w.Start == TimeSpan.FromSeconds(1_020), w => w with { Enqueued = 0 }),
        };

        Assert.Equal(Verdict.Fail, Part(SloEvaluator.Vacuum(backlog), 9, 0).Result);
        Assert.Equal(Verdict.Fail, Part(SloEvaluator.Vacuum(p99), 9, 0).Result);
        Assert.Equal(Verdict.Fail, Part(SloEvaluator.Vacuum(underloaded), 9, 0).Result);
    }

    [Theory]
    [InlineData(1_020)] // недогруз до T
    [InlineData(1_310)] // недогруз в последнем окне: не восстановилось
    public void Slo9_underload_before_recovery_marks_enqueue(double underloadedWindowStart)
    {
        var data = Vacuum();
        data = data with
        {
            Windows = Replace(data.Windows, w => w.Start == TimeSpan.FromSeconds(underloadedWindowStart),
                w => w with { Enqueued = 0 }),
        };
        var cpuBound = data with { Windows = Replace(data.Windows, _ => true, w => w with { CpuFraction = 0.9 }) };

        Assert.Equal("enqueue", Part(SloEvaluator.Vacuum(data), 9, 0).Mark);
        Assert.Equal($"enqueue, {StandMark}", Part(SloEvaluator.Vacuum(cpuBound), 9, 0).Mark);
    }

    [Fact]
    public void Slo9_underload_outside_phase3_does_not_mark_enqueue()
    {
        var data = Vacuum();
        // Недогруз только в фазе 2: на момент восстановления T не влияет.
        data = data with
        {
            Windows = Replace(data.Windows, w => w.Start == TimeSpan.FromSeconds(600), w => w with { Enqueued = 0 }),
        };

        Assert.Null(Part(SloEvaluator.Vacuum(data), 9, 0).Mark);
    }

    [Fact]
    public void Partitions_evaluates_slo_1_and_2_with_stand_mark()
    {
        var data = Sustained();
        data = data with
        {
            Windows = Replace(data.Windows, w => w.Index is >= 10 and < 17, w => w with { CpuFraction = 0.9 }),
            Correctness = null,
        };

        var verdicts = SloEvaluator.Partitions(data);

        Assert.Equal([1, 2], verdicts.Select(v => v.Number));
        Assert.All(verdicts, v => Assert.Equal(Verdict.Pass, v.Result));
        Assert.All(verdicts, v => Assert.Equal(StandMark, v.Mark));
    }
}
