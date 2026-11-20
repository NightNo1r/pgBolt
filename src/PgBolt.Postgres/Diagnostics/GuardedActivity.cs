using System.Diagnostics;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>Остановка <see cref="Activity"/> под защитой: слушатель <see cref="ActivitySource"/> может бросить из <c>ActivityStopped</c>.</summary>
internal static class GuardedActivity
{
    /// <summary>
    /// Останавливает span, затем возвращает <see cref="Activity.Current"/> к <paramref name="restore"/>, и только после этого
    /// сообщает о сбое остановки: логгер и его слушатели должны видеть внешний span, а не оборвавшийся.
    /// </summary>
    public static void Stop(Activity span, Activity? restore, PgBoltInstrumentation instrumentation)
    {
        Exception? failure = null;
        try { span.Stop(); }
        catch (Exception e) { failure = e; }
        Activity.Current = restore;
        if (failure is not null)
            instrumentation.ReportFailure(failure);
    }
}
