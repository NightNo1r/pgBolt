namespace PgBolt;

/// <summary>Обработка срабатываний, пропущенных за простой или паузу (часть 4 §5.3). Значения хранятся в recurring_jobs.misfire.</summary>
public enum MisfirePolicy
{
    /// <summary>Одно срабатывание за все пропущенные; ScheduledAt — последнее из них.</summary>
    FireOnce = 0,

    /// <summary>Только если последнее пропущенное не старше MisfireThreshold; иначе ничего.</summary>
    Skip = 1,

    /// <summary>Каждое пропущенное срабатывание не старше CatchUpWindow.</summary>
    CatchUp = 2,
}

/// <summary>Поведение при живом предыдущем запуске (часть 4 §5.3). Значения хранятся в recurring_jobs.overlap.</summary>
public enum OverlapPolicy
{
    /// <summary>Не ставить, пока жив предыдущий запуск (ключ pgbolt:recurring:{id}); при CatchUp — ждать.</summary>
    Skip = 0,

    /// <summary>Ставить всегда.</summary>
    Allow = 1,
}

public sealed record RecurringJobOptions
{
    /// <summary>Таймзона cron; null — UTC. Хранится как IANA id.</summary>
    /// <remarks>
    /// <para>
    /// Переходы DST — семантика Cronos (часть 4 §5.4): весной время, попавшее в пропуск, срабатывает в момент перехода
    /// (<c>30 2 * * *</c> в America/New_York 2026-03-08 — в 03:00 EDT); осенью выражение с фиксированными часом и минутой
    /// срабатывает один раз, интервальное (<c>*/15</c> в часах или минутах) — в обоих проходах повторяющегося часа.
    /// </para>
    /// <para>
    /// Исторические смены базового смещения зоны (не DST: Europe/Moscow 2011 и 2014, Pacific/Apia 2011) эту семантику не
    /// гарантируют: повторяющийся час срабатывает один раз, время в пропуске может сработать раньше перехода. Срабатывания
    /// не повторяются; если ошибку Cronos на таком участке исправить не удалось, тик пишет ошибку строки (last_error).
    /// </para>
    /// </remarks>
    public TimeZoneInfo? TimeZone { get; init; }

    public MisfirePolicy Misfire { get; init; } = MisfirePolicy.FireOnce;

    /// <summary>CatchUp догоняет срабатывания не старше окна; (0, 366 суток].</summary>
    public TimeSpan CatchUpWindow { get; init; } = TimeSpan.FromDays(1);

    public OverlapPolicy Overlap { get; init; } = OverlapPolicy.Skip;

    /// <summary>null — умолчание задачи из job_types на момент срабатывания (как и остальные поля ниже).</summary>
    public string? Queue { get; init; }

    public short? Priority { get; init; }
    public int? MaxAttempts { get; init; }
    public IReadOnlyList<TimeSpan>? RetryDelays { get; init; }
}
