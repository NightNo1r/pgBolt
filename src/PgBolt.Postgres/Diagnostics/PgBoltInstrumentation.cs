using System.Diagnostics.Metrics;
using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace PgBolt.Postgres.Diagnostics;

/// <summary>
/// Владеет <c>Meter</c> контейнера и сообщает о сбоях телеметрии. Meter берётся у <see cref="IMeterFactory"/>
/// контейнера (измерения нескольких контейнеров в одном процессе не смешиваются); без фабрики создаётся собственный
/// и освобождается вместе с контейнером.
/// </summary>
internal sealed partial class PgBoltInstrumentation : IDisposable
{
    private const long Never = long.MinValue;
    private static readonly TimeSpan ReportInterval = TimeSpan.FromMinutes(1);

    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly bool _ownsMeter;
    private long _lastFailure = Never;
    private long _lastCallbackFailure = Never;

    public PgBoltInstrumentation(IMeterFactory? meterFactory, TimeProvider time, ILoggerFactory? loggers)
    {
        _time = time;
        _logger = loggers?.CreateLogger("PgBolt.Telemetry") ?? NullLogger.Instance;
        var options = new MeterOptions(PgBoltDiagnostics.MeterName) { Version = Version };
        _ownsMeter = meterFactory is null;
        Meter = meterFactory?.Create(options) ?? new Meter(options);
    }

    /// <summary>Информационная версия сборки <c>PgBolt.Postgres</c>.</summary>
    public static string Version { get; } =
        typeof(PgBoltInstrumentation).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.0.0";

    public Meter Meter { get; }

    /// <summary>Сбой точки телеметрии: пишет событие в лог не чаще раза в минуту, сбой логгера глотает.</summary>
    public void ReportFailure(Exception error) => Report(ref _lastFailure, Log.TelemetryFailed, error);

    /// <summary>Сбой observable-callback'а: то же, что <see cref="ReportFailure"/>, но со своим ограничителем.</summary>
    public void ReportCallbackFailure(Exception error) => Report(ref _lastCallbackFailure, Log.MetricCallbackFailed, error);

    public void Dispose()
    {
        if (_ownsMeter)
            Meter.Dispose();
    }

    private void Report(ref long last, Action<ILogger, Exception> log, Exception error)
    {
        var seen = Volatile.Read(ref last);
        var now = _time.GetTimestamp();
        if (seen != Never && _time.GetElapsedTime(seen, now) < ReportInterval
            || Interlocked.CompareExchange(ref last, now, seen) != seen)
            return;
        try { log(_logger, error); }
        catch { /* сбой логгер-провайдера не должен ломать вызывающего */ }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 8001, EventName = nameof(TelemetryFailed), Level = LogLevel.Warning,
            Message = "PgBolt: telemetry failed; the signal is skipped.")]
        internal static partial void TelemetryFailed(ILogger logger, Exception e);

        [LoggerMessage(EventId = 8002, EventName = nameof(MetricCallbackFailed), Level = LogLevel.Warning,
            Message = "PgBolt: metric callback failed; the measurement is skipped.")]
        internal static partial void MetricCallbackFailed(ILogger logger, Exception e);
    }
}
