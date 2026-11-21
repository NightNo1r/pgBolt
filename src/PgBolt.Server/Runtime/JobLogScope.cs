using System.Collections;
using System.Globalization;
using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>
/// Logging scope for one job attempt. Providers using <c>IExternalScopeProvider</c> expose structured fields to runtime
/// and job logs. Text providers use <see cref="ToString"/>.
/// </summary>
internal sealed class JobLogScope(LeasedJob job, string queue) : IReadOnlyList<KeyValuePair<string, object?>>
{
    public int Count => 4;

    public KeyValuePair<string, object?> this[int index] => index switch
    {
        0 => new("JobId", job.Id),
        1 => new("JobType", job.Type),
        2 => new("Queue", queue),
        3 => new("Attempt", job.Attempt),
        _ => throw new ArgumentOutOfRangeException(nameof(index)),
    };

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        for (var i = 0; i < Count; i++)
            yield return this[i];
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"JobId:{job.Id} JobType:{job.Type} Queue:{queue} Attempt:{job.Attempt}");
}
