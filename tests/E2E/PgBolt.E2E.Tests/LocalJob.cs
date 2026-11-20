using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization;
using PgBolt.E2E.Contracts;
using PgBolt.Server;

namespace PgBolt.E2E.Tests;

[Job("e2e.local", Queue = "e2e")]
public sealed record LocalArgs(int N);

public static class LocalProbe
{
    public static ConcurrentQueue<int> Values { get; } = new();
}

public sealed class LocalJob : IJob<LocalArgs>
{
    public Task ExecuteAsync(LocalArgs args, JobContext context, CancellationToken cancellationToken)
    {
        LocalProbe.Values.Enqueue(args.N);
        return Task.CompletedTask;
    }
}

[Job("e2e.limited", Queue = "e2e-limited")]
public sealed record LimitedArgs(string Run, int N);

/// <summary>Одновременные выполнения LimitedJob по меткам прогона: тесты процесса идут параллельно и не делят счётчики.</summary>
public static class LimitedProbe
{
    private static readonly ConcurrentDictionary<string, Counter> Runs = new();

    public static Counter For(string run) => Runs.GetOrAdd(run, static _ => new Counter());

    /// <summary>Очередь e2e-limited для всех вызовов Queue проекта: значения ClusterMaxConcurrency расходиться не должны.</summary>
    public static void Queue(QueueOptions q)
    {
        q.ClusterMaxConcurrency = 3;
        q.MaxPollInterval = TimeSpan.FromMilliseconds(200);   // очередь у лимита опрашивается с бэкоффом до этого интервала
    }

    public sealed class Counter
    {
        private int _current, _max, _completed;

        public int Current => Volatile.Read(ref _current);
        public int Max => Volatile.Read(ref _max);
        public int Completed => Volatile.Read(ref _completed);

        public async Task RunAsync(Func<Task> body)
        {
            var now = Interlocked.Increment(ref _current);
            int seen;
            while ((seen = Volatile.Read(ref _max)) < now && Interlocked.CompareExchange(ref _max, now, seen) != seen)
            {
            }
            try
            {
                await body();
                Interlocked.Increment(ref _completed);
            }
            finally
            {
                Interlocked.Decrement(ref _current);
            }
        }
    }
}

public sealed class LimitedJob : IJob<LimitedArgs>
{
    public Task ExecuteAsync(LimitedArgs args, JobContext context, CancellationToken cancellationToken) =>
        LimitedProbe.For(args.Run).RunAsync(() => Task.Delay(300, cancellationToken));
}

[Job("e2e.trace", Queue = "e2e")]
public sealed record TraceArgs(string Run);

/// <summary>Задача с собственным span'ом: проверяет, что код задачи продолжает трейс того, кто её поставил.</summary>
public sealed class TraceJob : IJob<TraceArgs>
{
    public static ActivitySource Source { get; } = new("E2E");

    public Task ExecuteAsync(TraceArgs args, JobContext context, CancellationToken cancellationToken)
    {
        using var work = Source.StartActivity("e2e.work");
        return Task.CompletedTask;
    }
}

/// <summary>Сборка вызывает EnqueueAsync для PingArgs/PongArgs/LocalArgs/LimitedArgs/TraceArgs — все они обязаны быть в её контексте (BOLT001).</summary>
[JsonSerializable(typeof(LocalArgs))]
[JsonSerializable(typeof(LimitedArgs))]
[JsonSerializable(typeof(TraceArgs))]
[JsonSerializable(typeof(PingArgs))]
[JsonSerializable(typeof(PongArgs))]
internal sealed partial class E2EJson : JsonSerializerContext;
