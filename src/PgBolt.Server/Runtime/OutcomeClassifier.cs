using System.Globalization;
using System.Text;

namespace PgBolt.Server.Runtime;

/// <summary>Классификация исхода попытки по ошибке и причине отмены (успех при завершении самим исполнителем обрабатывает CompletionSink).</summary>
internal static class OutcomeClassifier
{
    private const int MaxTextBytes = 8192;

    public static JobOutcome Classify(Execution execution, CancelReason reason, Exception? error, RetryDecider retry, TimeSpan? timeout)
    {
        if (error is null)
            return new JobOutcome.Ack();

        if (reason == CancelReason.CancelRequested)
            return new JobOutcome.ConfirmCancel(Truncate(error.Message));
        if (reason == CancelReason.Shutdown && error is OperationCanceledException)
            return new JobOutcome.Release(Ran: true);
        if (error is JobAbortedException or JobArgumentsException)
            return new JobOutcome.Fail(JobEventKind.Failed, Truncate(error.Message), Truncate(error.ToString()));

        var message = reason switch
        {
            CancelReason.Timeout when timeout is { } t =>
                string.Create(CultureInfo.InvariantCulture, $"timeout after {t.TotalSeconds:0.###} s"),
            CancelReason.HeartbeatLost => "cancelled: lease renewal not confirmed",
            _ => error.Message,
        };
        return RetryOrFail(execution, error, retry, message, JobEventKind.Retry, JobEventKind.Failed);
    }

    public static JobOutcome NoExecutor(Execution execution, JobExecutorNotFoundException error, RetryDecider retry) =>
        RetryOrFail(execution, error, retry, error.Message, JobEventKind.NoExecutor, JobEventKind.NoExecutor);

    /// <summary>
    /// Обрезает до 8 КБ — 8192 байт UTF-8, по границе кодовой точки: суррогатная пара не разрывается, одиночные
    /// суррогаты исходного текста заменяются на U+FFFD (строгий UTF-8 энкодер Npgsql отверг бы их при каждой отправке).
    /// </summary>
    public static string Truncate(string? text)
    {
        if (text is null)
            return string.Empty;
        text = ReplaceLoneSurrogates(text);
        if (text.Length <= MaxTextBytes / 3 || Encoding.UTF8.GetByteCount(text) <= MaxTextBytes)
            return text;
        var bytes = 0;
        var length = 0;
        while (length < text.Length)
        {
            var rune = Rune.GetRuneAt(text, length);   // после замены текст — валидный UTF-16
            if (bytes + rune.Utf8SequenceLength > MaxTextBytes)
                break;
            bytes += rune.Utf8SequenceLength;
            length += rune.Utf16SequenceLength;
        }
        return text[..length];
    }

    /// <summary>Одиночный суррогат → U+FFFD (3 байта UTF-8, 1 символ UTF-16). Без одиночных суррогатов — та же строка.</summary>
    private static string ReplaceLoneSurrogates(string text)
    {
        var i = text.AsSpan().IndexOfAnyInRange('\uD800', '\uDFFF');
        if (i < 0)
            return text;
        StringBuilder? builder = null;
        var copied = 0;
        while (i < text.Length)
        {
            if (!char.IsSurrogate(text[i]))
            {
                i++;
                continue;
            }
            if (Rune.TryGetRuneAt(text, i, out var pair))
            {
                i += pair.Utf16SequenceLength;          // валидная пара — остаётся
                continue;
            }
            builder ??= new StringBuilder(text.Length);
            builder.Append(text, copied, i - copied).Append('\uFFFD');
            i++;
            copied = i;
        }
        return builder is null ? text : builder.Append(text, copied, text.Length - copied).ToString();
    }

    private static JobOutcome RetryOrFail(Execution execution, Exception error, RetryDecider retry, string message,
        JobEventKind retryKind, JobEventKind failKind)
    {
        var job = execution.Job;
        var text = Truncate(message);
        var details = Truncate(error.ToString());
        return job.Attempt < job.MaxAttempts && retry.GetDelay(job.Attempt, job.RetryDelaysSeconds, error) is { } delay
            ? new JobOutcome.Retry(delay, retryKind, text, details)
            : new JobOutcome.Fail(failKind, text, details);
    }
}
