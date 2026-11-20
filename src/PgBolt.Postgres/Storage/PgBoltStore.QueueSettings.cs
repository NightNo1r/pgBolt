using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    // Один statement: prev читает прежние значения, блокируя существующие строки в порядке queue, ins/upd пишут новые,
    // итоговый SELECT отдаёт только свежие расхождения с чужой публикацией. Редкий 40P01 при одновременной вставке новых
    // строк несколькими процессами повторяет DbRetry.
    private const string SyncQueueDefaultsTemplate = """
        WITH u AS (
            SELECT * FROM unnest(@queues::text[], @limits::int[], @weights::int[]) AS u(queue, lim, weight)
        ),
        prev AS (
            SELECT s.queue, s.default_max_concurrency, s.default_weight, s.default_published_by, s.default_updated_at
            FROM {schema}.queue_settings s JOIN u ON u.queue = s.queue
            ORDER BY s.queue FOR UPDATE OF s
        ),
        ins AS (
            INSERT INTO {schema}.queue_settings AS s (queue, default_max_concurrency, default_weight, default_published_by, default_updated_at)
            SELECT queue, lim, weight, @published_by, now() FROM u WHERE lim IS NOT NULL OR weight IS NOT NULL ORDER BY queue
            ON CONFLICT (queue) DO UPDATE SET default_max_concurrency = EXCLUDED.default_max_concurrency,
                default_weight = EXCLUDED.default_weight, default_published_by = EXCLUDED.default_published_by,
                default_updated_at = EXCLUDED.default_updated_at
        ),
        upd AS (
            UPDATE {schema}.queue_settings s SET default_max_concurrency = NULL, default_weight = NULL,
                default_published_by = @published_by, default_updated_at = now()
            FROM u WHERE s.queue = u.queue AND u.lim IS NULL AND u.weight IS NULL
        )
        SELECT p.queue, p.default_max_concurrency, p.default_weight, p.default_published_by
        FROM prev p JOIN u ON u.queue = p.queue
        WHERE p.default_published_by IS NOT NULL AND p.default_published_by <> @published_by
          AND p.default_updated_at > now() - interval '5 minutes'
          AND (p.default_max_concurrency IS DISTINCT FROM u.lim OR p.default_weight IS DISTINCT FROM u.weight)
        """;

    /// <summary>Эффективный лимит строки queue_settings: переопределение поверх значения из кода; NULL — без лимита.</summary>
    private const string EffectiveLimit =
        "CASE WHEN override_concurrency_set THEN override_max_concurrency ELSE default_max_concurrency END";

    private const string ActiveQueueSettingsTemplate = $$"""
        SELECT queue, lim, weight FROM (
            SELECT queue, {{EffectiveLimit}} AS lim,
                   CASE WHEN override_weight_set THEN override_weight ELSE default_weight END AS weight
            FROM {schema}.queue_settings
        ) s
        WHERE lim IS NOT NULL OR weight IS NOT NULL
        ORDER BY queue
        """;

    private const string QueueSettingsColumns = """
        queue, default_max_concurrency, default_weight, default_published_by,
        override_concurrency_set, override_max_concurrency, override_concurrency_updated_by, override_concurrency_updated_at,
        override_weight_set, override_weight, override_weight_updated_by, override_weight_updated_at
        """;

    private const string ListQueueSettingsTemplate = "SELECT " + QueueSettingsColumns + " FROM {schema}.queue_settings ORDER BY queue";

    private const string GetQueueSettingsTemplate = "SELECT " + QueueSettingsColumns + " FROM {schema}.queue_settings WHERE queue = @queue";

    private const string SetConcurrencyOverrideTemplate = """
        INSERT INTO {schema}.queue_settings (queue, override_concurrency_set, override_max_concurrency,
                                             override_concurrency_updated_by, override_concurrency_updated_at)
        VALUES (@queue, true, @value, @by, now())
        ON CONFLICT (queue) DO UPDATE SET override_concurrency_set = true, override_max_concurrency = EXCLUDED.override_max_concurrency,
            override_concurrency_updated_by = EXCLUDED.override_concurrency_updated_by,
            override_concurrency_updated_at = EXCLUDED.override_concurrency_updated_at
        """;

    private const string ResetConcurrencyOverrideTemplate = """
        UPDATE {schema}.queue_settings SET override_concurrency_set = false, override_max_concurrency = NULL,
            override_concurrency_updated_by = NULL, override_concurrency_updated_at = NULL
        WHERE queue = @queue
        """;

    private const string SetWeightOverrideTemplate = """
        INSERT INTO {schema}.queue_settings (queue, override_weight_set, override_weight, override_weight_updated_by, override_weight_updated_at)
        VALUES (@queue, true, @value, @by, now())
        ON CONFLICT (queue) DO UPDATE SET override_weight_set = true, override_weight = EXCLUDED.override_weight,
            override_weight_updated_by = EXCLUDED.override_weight_updated_by, override_weight_updated_at = EXCLUDED.override_weight_updated_at
        """;

    private const string ResetWeightOverrideTemplate = """
        UPDATE {schema}.queue_settings SET override_weight_set = false, override_weight = NULL,
            override_weight_updated_by = NULL, override_weight_updated_at = NULL
        WHERE queue = @queue
        """;

    private string SyncQueueDefaultsSql => field ??= Render(SyncQueueDefaultsTemplate);
    private string ActiveQueueSettingsSql => field ??= Render(ActiveQueueSettingsTemplate);
    private string ListQueueSettingsSql => field ??= Render(ListQueueSettingsTemplate);
    private string GetQueueSettingsSql => field ??= Render(GetQueueSettingsTemplate);
    private string SetConcurrencyOverrideSql => field ??= Render(SetConcurrencyOverrideTemplate);
    private string ResetConcurrencyOverrideSql => field ??= Render(ResetConcurrencyOverrideTemplate);
    private string SetWeightOverrideSql => field ??= Render(SetWeightOverrideTemplate);
    private string ResetWeightOverrideSql => field ??= Render(ResetWeightOverrideTemplate);

    /// <summary>
    /// Публикует значения очередей из кода. <paramref name="defaults"/> уникальны по <see cref="QueueDefault.Queue"/>;
    /// порядок блокировок задаёт ORDER BY в SQL. Возвращает строки, которые недавно (за 5 минут) записал другой процесс с
    /// иными значениями; переопределения не трогает.
    /// </summary>
    public Task<IReadOnlyList<QueueDefaultConflict>> SyncQueueDefaultsAsync(IReadOnlyList<QueueDefault> defaults, string publishedBy, CancellationToken ct)
    {
        if (defaults.Count == 0)
            return Task.FromResult<IReadOnlyList<QueueDefaultConflict>>([]);

        var queues = defaults.Select(d => d.Queue).ToArray();
        var limits = defaults.Select(d => d.MaxConcurrency).ToArray();
        var weights = defaults.Select(d => d.Weight).ToArray();
        return QueryAsync<IReadOnlyList<QueueDefaultConflict>>(SyncQueueDefaultsSql, ps =>
            {
                P.Add(ps, "queues", queues);
                P.Add(ps, "limits", limits, NpgsqlDbType.Array | NpgsqlDbType.Integer);
                P.Add(ps, "weights", weights, NpgsqlDbType.Array | NpgsqlDbType.Integer);
                P.Add(ps, "published_by", publishedBy);
            },
            async (r, c) => await r.ReadAllAsync(static r => new QueueDefaultConflict(
                r.GetString(0), r.GetNullableValue<int>(1), r.GetNullableValue<int>(2), r.GetString(3)), c), ct);
    }

    /// <summary>Очереди с эффективным (переопределение поверх значения из кода) лимитом или весом.</summary>
    public Task<IReadOnlyList<ActiveQueueSetting>> GetActiveQueueSettingsAsync(CancellationToken ct) =>
        QueryAsync<IReadOnlyList<ActiveQueueSetting>>(ActiveQueueSettingsSql, static _ => { },
            async (r, c) => await r.ReadAllAsync(static r => new ActiveQueueSetting(
                r.GetString(0), r.GetNullableValue<int>(1), r.GetNullableValue<int>(2)), c), ct);

    internal Task<IReadOnlyList<QueueSettings>> ListQueueSettingsAsync(CancellationToken ct) =>
        QueryAsync<IReadOnlyList<QueueSettings>>(ListQueueSettingsSql, static _ => { },
            async (r, c) => await r.ReadAllAsync(ReadQueueSettings, c), ct);

    internal Task<QueueSettings?> GetQueueSettingsAsync(string queue, CancellationToken ct) =>
        QueryAsync(GetQueueSettingsSql, ps => P.Add(ps, "queue", queue),
            async (r, c) => await r.ReadAsync(c) ? ReadQueueSettings(r) : null, ct);

    internal Task SetConcurrencyOverrideAsync(string queue, int? maxConcurrency, string? updatedBy, CancellationToken ct) =>
        NonQueryAsync(SetConcurrencyOverrideSql, ps => BindOverride(ps, queue, maxConcurrency, updatedBy), ct);

    internal Task ResetConcurrencyOverrideAsync(string queue, CancellationToken ct) =>
        NonQueryAsync(ResetConcurrencyOverrideSql, ps => P.Add(ps, "queue", queue), ct);

    internal Task SetWeightOverrideAsync(string queue, int? weight, string? updatedBy, CancellationToken ct) =>
        NonQueryAsync(SetWeightOverrideSql, ps => BindOverride(ps, queue, weight, updatedBy), ct);

    internal Task ResetWeightOverrideAsync(string queue, CancellationToken ct) =>
        NonQueryAsync(ResetWeightOverrideSql, ps => P.Add(ps, "queue", queue), ct);

    private static void BindOverride(NpgsqlParameterCollection ps, string queue, int? value, string? updatedBy)
    {
        P.Add(ps, "queue", queue);
        P.AddValue(ps, "value", value, NpgsqlDbType.Integer);
        P.AddNullable(ps, "by", updatedBy, NpgsqlDbType.Text);
    }

    private static QueueSettings ReadQueueSettings(NpgsqlDataReader r) => new(
        r.GetString(0), r.GetNullableValue<int>(1), r.GetNullableValue<int>(2), r.GetNullable<string>(3),
        r.GetBoolean(4), r.GetNullableValue<int>(5), r.GetNullable<string>(6), r.GetNullableValue<DateTimeOffset>(7),
        r.GetBoolean(8), r.GetNullableValue<int>(9), r.GetNullable<string>(10), r.GetNullableValue<DateTimeOffset>(11));
}
