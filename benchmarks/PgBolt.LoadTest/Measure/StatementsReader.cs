using Npgsql;
using PgBolt.LoadTest.Env;

namespace PgBolt.LoadTest.Measure;

/// <summary>Итоги класса statement'ов, времена — в миллисекундах; <c>null</c> — нет данных.</summary>
internal sealed record StatementClassStats(
    long Calls, double? MeanPlanTime, double? MeanExecTime, double? TotalPlanTime, double TotalExecTime);

/// <summary>Сводка <c>pg_stat_statements</c> по классам; <see cref="Classes"/> = <c>null</c> — расширение недоступно.</summary>
internal sealed record StatementsSummary(IReadOnlyDictionary<string, StatementClassStats>? Classes)
{
    public static readonly StatementsSummary NoData = new((IReadOnlyDictionary<string, StatementClassStats>?)null);
}

/// <summary>
/// Сброс и чтение <c>pg_stat_statements</c> базы прогона. Без расширения сводка — «нет данных»; время
/// планирования копится только при <c>pg_stat_statements.track_planning = on</c>, иначе его колонки — «нет данных».
/// Если сброс не удался (у пользователя внешней БД нет права на <c>pg_stat_statements_reset</c>), статистика
/// включала бы время до окна замера, поэтому сводка — тоже «нет данных».
/// </summary>
internal sealed class StatementsReader(string connectionString)
{
    private bool _resetFailed;

    public async Task ResetAsync()
    {
        await using var conn = await StandConnection.OpenAsync(connectionString, "statements", CancellationToken.None);
        if (await ExtensionSchemaAsync(conn) is not { } schema)
            return;
        try
        {
            await ScalarAsync(conn, $"SELECT {schema}.pg_stat_statements_reset()");
        }
        catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.ObjectNotInPrerequisiteState
                                              or PostgresErrorCodes.InsufficientPrivilege)
        {
            // Библиотека не загружена через shared_preload_libraries или нет права EXECUTE.
            _resetFailed = true;
        }
    }

    public async Task<StatementsSummary> ReadAsync()
    {
        if (_resetFailed)
            return StatementsSummary.NoData;
        await using var conn = await StandConnection.OpenAsync(connectionString, "statements", CancellationToken.None);
        if (await ExtensionSchemaAsync(conn) is not { } schema)
            return StatementsSummary.NoData;

        var trackPlanning = await ScalarAsync(conn,
            "SELECT current_setting('pg_stat_statements.track_planning', true) = 'on'") is true;
        var totals = new Dictionary<string, (long Calls, long Plans, double Plan, double Exec)>();
        await using var cmd = new NpgsqlCommand($"""
            SELECT query, calls, plans, total_plan_time, total_exec_time FROM {schema}.pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            """, conn);
        try
        {
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                if (StatementClasses.Classify(r.GetString(0)) is not { } cls)
                    continue;
                var t = totals.GetValueOrDefault(cls);
                totals[cls] = (t.Calls + r.GetInt64(1), t.Plans + r.GetInt64(2), t.Plan + r.GetDouble(3), t.Exec + r.GetDouble(4));
            }
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.ObjectNotInPrerequisiteState)
        {
            // Расширение создано, но библиотека не загружена через shared_preload_libraries.
            return StatementsSummary.NoData;
        }

        return new StatementsSummary(totals.ToDictionary(
            t => t.Key, t => Stats(t.Value.Calls, t.Value.Plans, t.Value.Plan, t.Value.Exec, trackPlanning)));
    }

    /// <summary>
    /// Итоги класса. Среднее планирования — на число планирований (<c>plans</c>): у generic plan их намного
    /// меньше, чем выполнений. При нуле делителя среднего нет (деление дало бы NaN).
    /// </summary>
    internal static StatementClassStats Stats(long calls, long plans, double totalPlan, double totalExec,
        bool trackPlanning) =>
        new(calls,
            trackPlanning && plans > 0 ? totalPlan / plans : null,
            calls > 0 ? totalExec / calls : null,
            trackPlanning ? totalPlan : null,
            totalExec);

    private static async Task<string?> ExtensionSchemaAsync(NpgsqlConnection conn) =>
        await ScalarAsync(conn, """
            SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pg_stat_statements'
            """) as string;

    private static async Task<object?> ScalarAsync(NpgsqlConnection conn, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        return await cmd.ExecuteScalarAsync();
    }
}
