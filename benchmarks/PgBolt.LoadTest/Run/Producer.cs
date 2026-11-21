using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgBolt.LoadTest.Cli;
using PgBolt.LoadTest.Load;
using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Run;

/// <summary>
/// Load producer with its own serverless PgBolt host and a <see cref="ProducerPipeline"/> sending batches via <c>EnqueueManyAsync</c>. A failed
/// call is counted without stopping generation. Its batch's <c>Seq</c> values go to <see cref="RunContext.FailedCallSeqs"/>.
/// </summary>
internal sealed class Producer : IAsyncDisposable
{
    public const string ApplicationName = "pgbolt-load-producer";

    // JSON LoadArgs без Padding занимает ~110 байт.
    private static readonly string Padding = new('x', 90);

    private readonly IHost _host;
    private readonly IJobClient _client;
    private readonly RunContext _ctx;
    private readonly ProducerPipeline _pipeline;
    private readonly LatencyRecorder _callLatency = new();
    private readonly CancellationTokenSource _generation = new();
    private Task _run = Task.CompletedTask;
    private Task? _stop;
    private long _enqueued;
    private long _calls;
    private long _errors;

    private Producer(IHost host, RunContext ctx, int rate)
    {
        _host = host;
        _client = host.Services.GetRequiredService<IJobClient>();
        _ctx = ctx;
        _pipeline = new ProducerPipeline(new ProfileB(new Random(), ctx.Queues), new TokenBucket(rate, rate, TimeProvider.System),
            new BatchAssembler(), SendAsync, () => RunClock.NowMicros, () => ProfileB.PickDelaySeconds(Random.Shared),
            TimeProvider.System, ctx.Counter.EnsureChunk);
    }

    /// <summary>Jobs issued since generation started.</summary>
    public long Issued => _pipeline.Stats.Generated;

    /// <param name="connectionString">Connection string for the run mode, direct or through PgBouncer.</param>
    /// <param name="rate">Initial target rate in jobs per second.</param>
    public static async Task<Producer> StartAsync(string connectionString, string schema, LoadMode mode, RunContext ctx,
        int rate, CancellationToken ct)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddPgBolt(o => HostCluster.Configure(o, connectionString, 8, ApplicationName, schema, mode));
        var host = builder.Build();
        try
        {
            await host.StartAsync(ct);
        }
        catch
        {
            await ((IAsyncDisposable)host).DisposeAsync();
            throw;
        }

        var producer = new Producer(host, ctx, rate);
        producer._run = Task.Run(() => producer._pipeline.RunAsync(producer._generation.Token), CancellationToken.None);
        return producer;
    }

    /// <summary>Changes the target rate and token capacity together. Capacity equals one second at the new rate.</summary>
    public void SetRate(int rate) => _pipeline.SetRate(rate);

    /// <summary>Returns statistics since the last call and resets the window.</summary>
    public ProducerWindow TakeWindow()
    {
        var pipeline = _pipeline.TakeWindow();
        return new ProducerWindow(Interlocked.Exchange(ref _enqueued, 0), Interlocked.Exchange(ref _calls, 0),
            Interlocked.Exchange(ref _errors, 0), _callLatency.CloseWindow().P99Ms, pipeline.MaxGeneratorWait,
            pipeline.Dropped);
    }

    /// <summary>Stops generation and flushes partial batches. Propagates sender failures.</summary>
    public Task StopAsync() => _stop ??= StopCoreAsync();

    private async Task StopCoreAsync()
    {
        await _generation.CancelAsync();
        await _run;
        await _pipeline.StopAsync();
    }

    private async Task SendAsync(SendRequest request, CancellationToken ct)
    {
        var args = new LoadArgs[request.Jobs.Count];
        for (var i = 0; i < args.Length; i++)
        {
            var job = request.Jobs[i];
            args[i] = new LoadArgs(job.Seq, request.EnqueuedAtMicros, request.RunAtMicros, WorkMillis: 1,
                job.FailFirstAttempt, Padding);
            if (job.FailFirstAttempt) _ctx.FailFirstSeqs.Add(job.Seq);
        }

        var options = new EnqueueOptions
        {
            Queue = request.Key.Queue,
            Delay = request.Key.Delayed ? TimeSpan.FromSeconds(request.DelaySeconds) : null,
        };
        var start = Stopwatch.GetTimestamp();
        try
        {
            await _client.EnqueueManyAsync(args, options, ct: ct);
            Interlocked.Add(ref _enqueued, args.Length);
        }
        catch (Exception)
        {
            // Строки могли закоммититься (таймаут после commit) или нет: такие Seq не сверяются.
            Interlocked.Increment(ref _errors);
            foreach (var a in args) _ctx.FailedCallSeqs.Add(a.Seq);
        }
        finally
        {
            _callLatency.Record((long)Stopwatch.GetElapsedTime(start).TotalMicroseconds);
            Interlocked.Increment(ref _calls);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopAsync();
        }
        catch (Exception)
        {
            // Падение отправителя уже получил вызвавший StopAsync; без него освобождение идёт после другой
            // ошибки прогона, и она важнее.
        }

        await _host.StopAsync(CancellationToken.None);
        await ((IAsyncDisposable)_host).DisposeAsync();
        _generation.Dispose();
    }
}
