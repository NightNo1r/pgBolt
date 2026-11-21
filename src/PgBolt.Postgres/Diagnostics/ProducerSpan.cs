using System.Diagnostics;
using static PgBolt.Postgres.Diagnostics.PgBoltTelemetry;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>
/// Producer span for one client API call. <see cref="ActivitySource"/> listeners run synchronously and can throw. Guards each
/// <see cref="Activity"/> access, reports failures through <see cref="PgBoltInstrumentation.ReportFailure"/>, and restores the previous
/// <see cref="Activity.Current"/> without changing the outcome of the call.
/// </summary>
internal readonly struct ProducerSpan : IDisposable
{
    private readonly PgBoltInstrumentation _instrumentation;
    private readonly Activity? _activity;
    private readonly Activity? _saved;

    private ProducerSpan(PgBoltInstrumentation instrumentation, Activity? activity, Activity? saved, string? traceParent)
    {
        _instrumentation = instrumentation;
        _activity = activity;
        _saved = saved;
        TraceParent = traceParent;
    }

    /// <summary>
    /// Value for <c>trace_parent</c>. Uses this span's W3C ID if available, then the restored current <see cref="Activity"/>'s W3C ID, otherwise
    /// <c>null</c>.
    /// </summary>
    public string? TraceParent { get; }

    public static ProducerSpan Start(PgBoltInstrumentation instrumentation, string operation, string? queue,
        string? jobType, int messageCount, string? recurringId)
    {
        var saved = Activity.Current;
        // Без слушателя span не создаётся, и имя строить незачем.
        if (!Source.HasListeners())
            return new ProducerSpan(instrumentation, null, saved, CurrentTraceParent());

        Activity? activity = null;
        try
        {
            var name = jobType is null ? operation : $"{operation} {jobType}";
            activity = Source.StartActivity(name, ActivityKind.Producer);
            if (activity is not null)
            {
                activity.SetTag(Attr.MessagingSystem, PgBoltTelemetry.System);
                activity.SetTag(Attr.MessagingOperationType, "send");
                if (queue is not null)
                    activity.SetTag(Attr.MessagingDestinationName, queue);
                if (jobType is not null)
                    activity.SetTag(Attr.JobType, jobType);
                if (messageCount > 1)
                    activity.SetTag(Attr.MessagingBatchMessageCount, messageCount);
                if (recurringId is not null)
                    activity.SetTag(Attr.RecurringId, recurringId);
            }
        }
        catch (Exception e)
        {
            // Бросивший ActivityStarted уже сделал недостартовавший span текущим. Current восстанавливается до отчёта о сбое,
            // чтобы логгер и его слушатели видели внешний span, а не оборвавшийся.
            Activity.Current = saved;
            instrumentation.ReportFailure(e);
            activity = null;
        }

        var traceParent = activity is { IdFormat: ActivityIdFormat.W3C } ? activity.Id : CurrentTraceParent();
        return new ProducerSpan(instrumentation, activity, saved, traceParent);
    }

    /// <summary>The queue is known after the span starts. A <c>null</c> queue leaves the tag unset.</summary>
    public void SetQueue(string? queue)
    {
        if (queue is null || _activity is null)
            return;
        try { _activity.SetTag(Attr.MessagingDestinationName, queue); }
        catch (Exception e) { _instrumentation.ReportFailure(e); }
    }

    public void Fail(Exception error)
    {
        if (_activity is null)
            return;
        try
        {
            _activity.SetStatus(ActivityStatusCode.Error);
            _activity.SetTag(Attr.ErrorType, error.GetType().FullName);
            _activity.AddException(error);
        }
        catch (Exception e) { _instrumentation.ReportFailure(e); }
    }

    public void Dispose()
    {
        if (_activity is not null)
            GuardedActivity.Stop(_activity, _saved, _instrumentation);
    }
}
