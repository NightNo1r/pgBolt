using System.Collections.Concurrent;
using System.Diagnostics;

namespace PgBolt.Testing;

/// <summary>
/// Слушатель одного <see cref="ActivitySource"/>: включает полную запись span'ов только для него и собирает начатые и
/// завершённые Activity (можно читать из любого потока). <see cref="Listener"/> позволяет тесту подменить обработчики,
/// например на бросающие.
/// </summary>
public sealed class ActivityCapture : IDisposable
{
    private readonly ConcurrentQueue<Activity> _started = new();
    private readonly ConcurrentQueue<Activity> _stopped = new();

    /// <param name="sourceName">Имя источника, который слушаем.</param>
    /// <param name="filter">Какие Activity собирать; по умолчанию все.</param>
    public ActivityCapture(string sourceName, Func<Activity, bool>? filter = null)
    {
        Listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = a => Collect(_started, a),
            ActivityStopped = a => Collect(_stopped, a),
        };
        ActivitySource.AddActivityListener(Listener);

        void Collect(ConcurrentQueue<Activity> queue, Activity activity)
        {
            if (filter?.Invoke(activity) ?? true)
                queue.Enqueue(activity);
        }
    }

    public ActivityListener Listener { get; }

    /// <summary>Снимок начатых Activity.</summary>
    public IReadOnlyList<Activity> Started => [.. _started];

    /// <summary>Снимок завершённых Activity.</summary>
    public IReadOnlyList<Activity> Stopped => [.. _stopped];

    public void Dispose() => Listener.Dispose();
}
