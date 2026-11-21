using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

public sealed partial class PgBoltStore
{
    private const string JobTypeMismatchTemplate = """
        SELECT t.name
        FROM {schema}.job_types t
        JOIN unnest(@names::text[], @schemas::jsonb[]) AS u(name, args_schema) ON u.name = t.name
        WHERE t.published_by <> @published_by
          AND t.last_seen_at > now() - interval '5 minutes'
          AND t.args_schema <> u.args_schema
        """;

    private const string JobTypeUpsertTemplate = """
        INSERT INTO {schema}.job_types (name, args_schema, default_queue, default_max_attempts, default_priority,
                                        default_retry_delays, published_by, last_seen_at)
        VALUES (@name, @args_schema, @default_queue, @default_max_attempts, @default_priority,
                @default_retry_delays, @published_by, now())
        ON CONFLICT (name) DO UPDATE SET
            args_schema = EXCLUDED.args_schema,
            default_queue = EXCLUDED.default_queue,
            default_max_attempts = EXCLUDED.default_max_attempts,
            default_priority = EXCLUDED.default_priority,
            default_retry_delays = EXCLUDED.default_retry_delays,
            published_by = EXCLUDED.published_by,
            last_seen_at = now()
        """;

    private const string GetJobTypeTemplate = """
        SELECT name, args_schema, default_queue, default_max_attempts, default_priority, default_retry_delays
        FROM {schema}.job_types WHERE name = @name
        """;

    private const string JobTypeNamesByQueuesTemplate = """
        SELECT name FROM {schema}.job_types WHERE default_queue = ANY(@queues) ORDER BY name
        """;

    private string JobTypeMismatchSql => field ??= Render(JobTypeMismatchTemplate);
    private string JobTypeUpsertSql => field ??= Render(JobTypeUpsertTemplate);
    private string GetJobTypeSql => field ??= Render(GetJobTypeTemplate);
    private string JobTypeNamesByQueuesSql => field ??= Render(JobTypeNamesByQueuesTemplate);

    public Task<IReadOnlyList<string>> PublishJobTypesAsync(IReadOnlyList<JobTypeRecord> types, string publishedBy, CancellationToken ct)
    {
        if (types.Count == 0)
            return Task.FromResult<IReadOnlyList<string>>([]);

        return RunAsync<IReadOnlyList<string>>(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var tx = await conn.BeginTransactionAsync(c);

            List<string> mismatches;
            await using (var check = new NpgsqlCommand(JobTypeMismatchSql, conn, tx))
            {
                P.Add(check.Parameters, "names", types.Select(t => t.Name).ToArray());
                P.Add(check.Parameters, "schemas", types.Select(t => t.ArgsSchemaJson).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
                P.Add(check.Parameters, "published_by", publishedBy);
                await using var r = await check.ExecuteReaderAsync(c);
                mismatches = await r.ReadAllAsync(static r => r.GetString(0), c);
            }

            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                foreach (var t in types)
                {
                    var cmd = new NpgsqlBatchCommand(JobTypeUpsertSql);
                    P.Add(cmd.Parameters, "name", t.Name);
                    P.Add(cmd.Parameters, "args_schema", t.ArgsSchemaJson, NpgsqlDbType.Jsonb);
                    P.Add(cmd.Parameters, "default_queue", t.DefaultQueue);
                    P.Add(cmd.Parameters, "default_max_attempts", t.DefaultMaxAttempts);
                    P.Add(cmd.Parameters, "default_priority", t.DefaultPriority);
                    P.AddNullable(cmd.Parameters, "default_retry_delays", t.DefaultRetryDelaysSeconds, NpgsqlDbType.Array | NpgsqlDbType.Integer);
                    P.Add(cmd.Parameters, "published_by", publishedBy);
                    batch.BatchCommands.Add(cmd);
                }
                await batch.ExecuteNonQueryAsync(c);
            }

            await tx.CommitAsync(c);
            return mismatches;
        }, ct);
    }

    public Task<JobTypeRecord?> GetJobTypeAsync(string name, CancellationToken ct) =>
        QueryAsync(GetJobTypeSql, ps => P.Add(ps, "name", name), async (r, c) =>
            await r.ReadAsync(c)
                ? new JobTypeRecord(r.GetString(0), r.GetString(1), r.GetString(2), r.GetInt32(3), r.GetInt16(4), r.GetNullable<int[]>(5))
                : null, ct);

    /// <summary>Names from job_types whose default_queue is in queues, used for the startup NoExecutor warning (Part 3 §4.4, step 4).</summary>
    public Task<IReadOnlyList<string>> GetJobTypeNamesByQueuesAsync(IReadOnlyList<string> queues, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(queues);
        if (queues.Count == 0)
            return Task.FromResult<IReadOnlyList<string>>([]);
        var array = queues.ToArray();
        return QueryAsync<IReadOnlyList<string>>(JobTypeNamesByQueuesSql, ps => P.Add(ps, "queues", array),
            async (r, c) => await r.ReadAllAsync(static r => r.GetString(0), c), ct);
    }
}
