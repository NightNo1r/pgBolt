using Npgsql;

namespace PgBolt.Postgres.Schema;

public sealed class MigrationRunner(NpgsqlDataSource dataSource, string schema)
{
    private readonly string _schema = SchemaName.Validate(schema);

    /// <summary>Применяет недостающие миграции в одной транзакции под advisory xact lock. Возвращает число применённых.</summary>
    public async Task<int> MigrateAsync(CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await ExecAsync(conn, tx, PgBoltMigrations.LockKeySql, ct, ("lock_key", _schema));
        await ExecAsync(conn, tx, PgBoltMigrations.Render(PgBoltMigrations.BootstrapSql, _schema), ct);

        var applied = new HashSet<int>();
        await using (var cmd = new NpgsqlCommand(PgBoltMigrations.Render("SELECT version FROM {schema}.schema_version", _schema), conn, tx))
        await using (var r = await cmd.ExecuteReaderAsync(ct))
            while (await r.ReadAsync(ct))
                applied.Add(r.GetInt32(0));

        var count = 0;
        foreach (var m in PgBoltMigrations.All.Where(m => !applied.Contains(m.Version)))
        {
            try
            {
                await ExecAsync(conn, tx, PgBoltMigrations.Render(m.Sql, _schema), ct);
            }
            catch (PostgresException e)
            {
                throw new InvalidOperationException($"PgBolt migration V{m.Version:D3} ({m.Name}) failed in schema '{_schema}': {e.MessageText}", e);
            }
            await ExecAsync(conn, tx, PgBoltMigrations.Render("INSERT INTO {schema}.schema_version (version, applied_at) VALUES (@v, now())", _schema), ct, ("v", m.Version));
            count++;
        }

        await tx.CommitAsync(ct);
        return count;
    }

    public async Task<int?> GetCurrentVersionAsync(CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using (var check = new NpgsqlCommand("SELECT to_regclass(@t) IS NOT NULL", conn))
        {
            check.Parameters.AddWithValue("t", $"{_schema}.schema_version");
            if (!(bool)(await check.ExecuteScalarAsync(ct))!)
                return null;
        }
        await using var cmd = new NpgsqlCommand(PgBoltMigrations.Render("SELECT max(version) FROM {schema}.schema_version", _schema), conn);
        return await cmd.ExecuteScalarAsync(ct) is int v ? v : null;
    }

    private static async Task ExecAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, CancellationToken ct, params (string Name, object Value)[] ps)
    {
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        foreach (var (name, value) in ps)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
