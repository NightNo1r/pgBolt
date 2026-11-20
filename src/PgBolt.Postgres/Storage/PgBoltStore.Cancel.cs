using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string CancelMarkTemplate = """
        UPDATE {schema}.queue SET cancel_requested = true WHERE job_id = @id RETURNING lease_owner
        """;

    private const string CancelNowTemplate = """
        WITH d AS (DELETE FROM {schema}.queue WHERE job_id = @id RETURNING job_id, attempt, notify),
        j AS (UPDATE {schema}.jobs SET state = 4, attempt = d.attempt, finished_at = now(), notify = jobs.notify OR d.notify
              FROM d WHERE jobs.id = d.job_id AND jobs.id = @id
              RETURNING jobs.id, jobs.batch_id, d.notify),
        s AS (INSERT INTO {schema}.completions (node_id, kind, batch_id)
              SELECT id, 4, batch_id FROM j WHERE notify
              RETURNING 1),
        ev AS (INSERT INTO {schema}.job_events (job_id, at, kind, attempt, message)
               SELECT job_id, now(), @kind_cancelled, attempt, @reason FROM d)
        SELECT count(*) FROM s
        """;

    private const string JobStateTemplate = "SELECT state FROM {schema}.jobs WHERE id = @id";

    private string CancelMarkSql => field ??= Render(CancelMarkTemplate);
    private string CancelNowSql => field ??= Render(CancelNowTemplate);
    private string JobStateSql => field ??= Render(JobStateTemplate);

    public Task<CancelResult> CancelAsync(Guid jobId, string? reason, CancellationToken ct) =>
        RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, c);

            bool found;
            Guid? owner = null;
            await using (var mark = new NpgsqlCommand(CancelMarkSql, conn, tx))
            {
                P.Add(mark.Parameters, "id", jobId);
                await PrepareAsync(mark, c);
                await using var r = await mark.ExecuteReaderAsync(c);
                found = await r.ReadAsync(c);
                if (found)
                    owner = r.GetNullableValue<Guid>(0);
            }

            CancelResult result;
            long signals = 0;
            if (!found)
            {
                await using var state = new NpgsqlCommand(JobStateSql, conn, tx);
                P.Add(state.Parameters, "id", jobId);
                result = await state.ExecuteScalarAsync(c) switch
                {
                    short s when s is 2 or 3 or 4 => CancelResult.AlreadyFinished,
                    _ => CancelResult.NotFound,
                };
            }
            else if (owner is null)
            {
                // Без lease: Pending, Scheduled и Awaiting (часть 5 §6.7).
                await using var now = new NpgsqlCommand(CancelNowSql, conn, tx);
                P.Add(now.Parameters, "id", jobId);
                P.Add(now.Parameters, "kind_cancelled", (short)JobEventKind.Cancelled);
                P.AddNullable(now.Parameters, "reason", reason, NpgsqlDbType.Text);
                await PrepareAsync(now, c);
                signals = (long)(await now.ExecuteScalarAsync(c))!;
                result = CancelResult.Cancelled;
            }
            else
            {
                result = CancelResult.CancellationRequested;
            }

            await tx.CommitAsync(c);
            SignalCompletions(signals);
            return result;
        }, ct);
}
