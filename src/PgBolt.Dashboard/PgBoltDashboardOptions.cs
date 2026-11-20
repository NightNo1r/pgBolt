namespace PgBolt.Dashboard;

/// <summary>Настройки dashboard (<c>services.AddPgBoltDashboard(o => …)</c>).</summary>
public sealed class PgBoltDashboardOptions
{
    private static readonly TimeSpan MaxInterval = TimeSpan.FromDays(1);

    /// <summary>Политика авторизации для действий и запуска задач; null — действия разрешены всем, кто видит dashboard.</summary>
    public string? ActionsPolicy { get; set; }

    /// <summary>Действия не рендерятся, POST → 403.</summary>
    public bool ReadOnly { get; set; }

    /// <summary>Разрешить нелокальный доступ без авторизации на группе dashboard.</summary>
    /// <remarks>
    /// Без авторизации на группе и при false пропускаются только локальные запросы: <c>RemoteIpAddress</c> loopback и нет
    /// заголовков <c>Forwarded</c>, <c>X-Forwarded-For</c>, <c>X-Real-IP</c>. Ограничение: sidecar service mesh (Envoy / Istio:
    /// 127.0.0.6, 127.0.0.1), не добавляющий заголовков, неотличим от локального клиента — в mesh авторизация на группе
    /// (<c>RequireAuthorization</c>) обязательна.
    /// </remarks>
    public bool AllowAnonymousRemote { get; set; }

    /// <summary><c>SET LOCAL statement_timeout</c> каждого statement'а dashboard.</summary>
    public TimeSpan QueryTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Бюджет обхода порциями (списки Succeeded / Failed / Cancelled и др.).</summary>
    public TimeSpan ScanBudget { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Период polling обзора за час и счётчиков очередей.</summary>
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Сервер без heartbeat дольше — «не отвечает».</summary>
    public TimeSpan ServerStaleAfter { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Срок задачи <c>stats</c> (<c>next_run_at</c>) просрочен дольше (сверх допуска по интервалу) — баннер устаревшей статистики.</summary>
    public TimeSpan StatsStaleAfter { get; set; } = TimeSpan.FromMinutes(1);

    internal void Validate()
    {
        if (QueryTimeout < TimeSpan.FromMilliseconds(1) || QueryTimeout > MaxInterval)
            throw new InvalidOperationException("PgBolt dashboard: QueryTimeout must be in [1 ms, 1 day].");
        if (RefreshInterval < TimeSpan.FromSeconds(1) || RefreshInterval > MaxInterval)
            throw new InvalidOperationException("PgBolt dashboard: RefreshInterval must be in [1 s, 1 day].");
        ValidateInterval(ScanBudget, nameof(ScanBudget));
        ValidateInterval(ServerStaleAfter, nameof(ServerStaleAfter));
        ValidateInterval(StatsStaleAfter, nameof(StatsStaleAfter));
        if (ActionsPolicy is not null && string.IsNullOrWhiteSpace(ActionsPolicy))
            throw new InvalidOperationException("PgBolt dashboard: ActionsPolicy must be null or a non-blank policy name.");
    }

    private static void ValidateInterval(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value > MaxInterval)
            throw new InvalidOperationException($"PgBolt dashboard: {name} must be in (0, 1 day].");
    }
}
