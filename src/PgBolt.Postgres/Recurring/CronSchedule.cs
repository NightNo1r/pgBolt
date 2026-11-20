using Cronos;

namespace PgBolt.Postgres.Recurring;

/// <summary>
/// Cron-выражение в таймзоне (часть 4 §2 «Cron», §5.4). Формат выбирается по строке одинаково в API, синхронизации и тике:
/// макрос @… — Standard (макросы разбираются одинаково в обоих форматах), иначе 5 полей — Standard, 6 — IncludeSeconds.
/// </summary>
internal sealed class CronSchedule
{
    private readonly CronExpression _expression;

    private CronSchedule(CronExpression expression, TimeZoneInfo zone)
    {
        _expression = expression;
        Zone = zone;
    }

    public TimeZoneInfo Zone { get; }

    /// <summary>FormatException (включая CronFormatException) — cron не разбирается; TimeZoneNotFoundException — нет таймзоны на поде.</summary>
    public static CronSchedule Parse(string cron, string timeZoneId)
    {
        ArgumentNullException.ThrowIfNull(cron);
        return new CronSchedule(CronExpression.Parse(cron.Trim(), FormatOf(cron)), TimeZoneInfo.FindSystemTimeZoneById(timeZoneId));
    }

    public static CronFormat FormatOf(string cron)
    {
        var trimmed = cron.Trim();
        if (trimmed.StartsWith('@'))
            return CronFormat.Standard;
        return trimmed.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length switch
        {
            5 => CronFormat.Standard,
            6 => CronFormat.IncludeSeconds,
            var n => throw new FormatException($"Cron expression '{cron}' has {n} fields; expected 5, 6 or a @macro."),
        };
    }

    /// <summary>IANA id таймзоны (хранимая форма, §4.2).</summary>
    public static string IanaId(TimeZoneInfo zone)
    {
        if (zone.HasIanaId)
            return zone.Id;
        return TimeZoneInfo.TryConvertWindowsIdToIanaId(zone.Id, out var iana)
            ? iana
            : throw new ArgumentException($"Time zone '{zone.Id}' has no IANA id.");
    }

    /// <summary>
    /// after(t) §5.3: первое срабатывание строго после t; null — срабатываний больше нет. after(t) &gt; t и не убывает по t
    /// (см. <see cref="Repair"/>); InvalidOperationException — Cronos не дал корректного ответа (ошибка строки тика, §5.2).
    /// </summary>
    public DateTime? After(DateTime utc)
    {
        utc = AsUtc(utc);
        var next = _expression.GetNextOccurrence(utc, Zone, inclusive: false);
        return next is { } n && n <= utc ? Repair(utc) : next;
    }

    /// <summary>
    /// Срабатывания в [fromUtc, toUtc]: обе границы включены (§5.4). Цепочка <see cref="After"/>, а не GetOccurrences Cronos:
    /// на той же ошибке Cronos GetOccurrences повторяет одно срабатывание бесконечно. Срабатывания — на целых секундах (не
    /// чаще тика), поэтому первое ≥ from — это after(from − 1 тик).
    /// </summary>
    public IEnumerable<DateTime> Between(DateTime fromUtc, DateTime toUtc)
    {
        var to = AsUtc(toUtc);
        for (var next = After(AsUtc(fromUtc).AddTicks(-1)); next is { } at && at <= to; next = After(at))
            yield return at;
    }

    /// <summary>
    /// Cronos 0.13 при смене стандартного смещения зоны (не DST: Europe/Moscow 2011-03-27 02:00 MSK+3 → 03:00 MSK+4) в часе перед
    /// переходом возвращает after(t) ≤ t (для <c>0 * * * *</c> — 22:00Z для любого t из [22:00Z, 23:00Z)), и тик ставил бы одно
    /// срабатывание до ~3600 раз подряд. Срабатывания — на целых секундах, поэтому первое &gt; t — это первое ≥ s, где s — целая
    /// секунда после t. Точка s «исправна», если Cronos отвечает срабатыванием ≥ s (или null). От s с удвоением шага ищется
    /// исправная точка, затем бинарным поиском — ближайшая исправная после последней неисправной; ответ — срабатывание от неё.
    /// Не больше ~2 × <see cref="MaxRepairDoublings"/> вызовов, только на неисправном участке.
    /// </summary>
    private DateTime? Repair(DateTime utc)
    {
        var start = new DateTime(utc.Ticks - utc.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc).AddSeconds(1);
        if (FirstAtOrAfter(start) is var first && Sane(start, first))
            return first;

        var lo = start;                                                    // неисправна
        for (var doubling = 0; doubling < MaxRepairDoublings; doubling++)
        {
            var hi = start.AddSeconds(2L << doubling);
            if (!Sane(hi, FirstAtOrAfter(hi)))
            {
                lo = hi;
                continue;
            }
            while (hi - lo > OneSecond)                                     // lo неисправна, hi исправна, обе — целые секунды
            {
                var mid = lo.AddSeconds((long)(hi - lo).TotalSeconds / 2);
                if (Sane(mid, FirstAtOrAfter(mid)))
                    hi = mid;
                else
                    lo = mid;
            }
            return FirstAtOrAfter(hi);
        }
        throw new InvalidOperationException(
            $"Cronos returned a non-increasing occurrence for cron '{_expression}' in time zone '{Zone.Id}' after {utc:O}.");
    }

    private const int MaxRepairDoublings = 20;                             // 2^20 с ≈ 12 сут от t
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    private DateTime? FirstAtOrAfter(DateTime utc) => _expression.GetNextOccurrence(utc, Zone, inclusive: true);

    private static bool Sane(DateTime from, DateTime? occurrence) => occurrence is not { } o || o >= from;

    /// <summary>timestamptz из Npgsql приходит с Kind = Utc; Cronos требует его явно.</summary>
    private static DateTime AsUtc(DateTime value) =>
        value.Kind == DateTimeKind.Utc ? value : DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
