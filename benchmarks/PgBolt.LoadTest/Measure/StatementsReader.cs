using Npgsql;
using PgBolt.LoadTest.Env;

namespace PgBolt.LoadTest.Measure;

/// <summary>Totals per statement class, with times in milliseconds. <c>null</c> denotes missing data.</summary>
internal sealed record StatementClassStats(
    long Calls, double? MeanPlanTime, double? MeanExecTime, double? TotalPlanTime, double TotalExecTime);

/// <summary>Summary of <c>pg_stat_statements</c> by class. <see cref="Classes"/> is <c>null</c> if the extension is unavailable.</summary>
internal sealed record StatementsSummary(IReadOnlyDictionary<string, StatementClassStats>? Classes)
{
    // Ноль планирований допустим для готового generic plan; выключенный track_planning даёт null.
    public List<string> MissingStats(IEnumerable<string> expected) => expected
        .Where(name => Classes?.GetValueOrDefault(name) is not { Calls: > 0, TotalPlanTime: not null }).ToList();

    public static readonly StatementsSummary NoData = new((IReadOnlyDictionary<string, StatementClassStats>?)null);
}

/// <summary>
/// Resets and reads <c>pg_stat_statements</c> for the run database. Missing extensions produce no data. Planning time is available only when
/// <c>pg_stat_statements.track_planning = on</c>. If reset fails, e.g. an external DB user lacks permission for <c>pg_stat_statements_reset</c>,
/// omit the summary because it would include statistics from before the measurement window.
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
    /// Class totals. Mean planning time uses the number of plans (<c>plans</c>), which can be much lower than executions for a generic plan. A zero
    /// denominator yields no mean instead of NaN.
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
