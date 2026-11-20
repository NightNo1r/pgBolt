using Npgsql;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Slo;

namespace PgBolt.LoadTest.Run;

/// <summary>Проверка корректности после остановки продюсера и дренажа.</summary>
internal static class CorrectnessCheck
{
    /// <param name="issued">Выдано задач продюсером после его остановки.</param>
    /// <returns><see cref="CorrectnessData.DrainedInTime"/> = <c>true</c>: итог дренажа выставляет сценарий.</returns>
    public static async Task<CorrectnessData> RunAsync(string connectionString, string schema, RunContext ctx,
        long issued, MeterCounters counters, CancellationToken ct)
    {
        var (missing, duplicated, excluded) = ExecutionReconciler.Reconcile(
            ctx.Counter, issued, ctx.FailFirstSeqs.ToHashSet(), ctx.FailedCallSeqs.ToHashSet());

        await using var conn = await StandConnection.OpenAsync(connectionString, "correctness", ct);
        // state 2 — Succeeded; attempt — 2 у задач с FailFirstAttempt, 1 у остальных.
        await using var cmd = new NpgsqlCommand($"""
            SELECT (SELECT count(*) FROM {schema}.jobs WHERE state <> 2),
                   (SELECT count(*) FROM {schema}.queue),
                   (SELECT count(*) FILTER (WHERE attempt <> CASE WHEN (args->>'FailFirstAttempt')::boolean THEN 2 ELSE 1 END)
                    FROM {schema}.jobs)
            """, conn);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        var pgbolt = counters.Snapshot();
        return new CorrectnessData(missing, duplicated, excluded, r.GetInt64(0), r.GetInt64(1), DrainedInTime: true,
            r.GetInt64(2), pgbolt.OutcomeDropped, pgbolt.StorageErrorsTotal);
    }
}
