using System.Text.Json.Serialization;
using PgBolt.Postgres.Client;
using PgBolt.Postgres.Storage;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public sealed record RecArgs(int N);

[JsonSerializable(typeof(RecArgs))]
internal sealed partial class RecJson : JsonSerializerContext;

internal static class RecurringTestSupport
{
    /// <summary>Тип задачи расписаний в тестах: очередь rq, 3 попытки, приоритет 1.</summary>
    public static readonly JobDescriptor Descriptor = new("rec.job", "rq", 3, 1, null);

    public static void EnsureRegistered() => JobMetadataRegistry.Register(Descriptor, RecJson.Default.RecArgs);

    public static Task PublishAsync(PgBoltStore store, CancellationToken ct) =>
        store.PublishJobTypesAsync([new JobTypeRecord("rec.job", "{}", "rq", 3, 1, null)], "test", ct);

    public static RecurringJobManager Manager(PgBoltStore store, IWakeupSignal wakeup) => ClientTelemetry.Manager(store, wakeup);

    /// <summary>
    /// Тик, удерживаемый хуком TickBeforeCommit (§10): строки захвачены FOR UPDATE, задачи вставлены, курсоры обновлены,
    /// COMMIT ещё не выполнен. Хук снимается сразу после захвата — следующие тики не удерживаются.
    /// </summary>
    public static async Task<HeldTick> HoldTickAsync(PgBoltStore store, CancellationToken ct)
    {
        var claimed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.TickBeforeCommit = async c =>
        {
            claimed.TrySetResult();
            await release.Task.WaitAsync(c);
        };
        try
        {
            var tick = store.TickRecurringAsync(100, 1000, TimeSpan.FromMinutes(1), ct);
            if (await Task.WhenAny(claimed.Task, tick).WaitAsync(TimeSpan.FromSeconds(10), ct) != claimed.Task)
                throw new InvalidOperationException($"The tick committed without holding rows: {await tick}.");
            return new HeldTick(tick, release);
        }
        finally
        {
            store.TickBeforeCommit = null;
        }
    }

    /// <summary>Ждёт сессию, вставшую в ожидание блокировки строки recurring_jobs схемы теста (pg_stat_activity по тексту statement'а).</summary>
    public static Task WaitForRowLockWaiterAsync(TestDb db, CancellationToken ct) => db.WaitForLockWaitersAsync("recurring_jobs", ct: ct);
}

/// <summary>Удерживаемый тик: ReleaseAsync отпускает COMMIT и возвращает результат тика.</summary>
internal sealed record HeldTick(Task<RecurringTickResult> Tick, TaskCompletionSource Release)
{
    public Task<RecurringTickResult> ReleaseAsync()
    {
        Release.TrySetResult();
        return Tick;
    }
}
