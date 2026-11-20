using Npgsql;
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Storage;

/// <summary>SQL горячего пути. Каждая операция — один round trip; один partial-файл на операцию.</summary>
public sealed partial class PgBoltStore
{
    private const int MaxStatementAttempts = DbRetry.MaxAttempts;

    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _prepare;
    private readonly bool _retryOnConflict;

    public PgBoltStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements)
        : this(dataSource, schema, usePreparedStatements, retryOnConflict: true)
    {
    }

    /// <param name="retryOnConflict">false — только для тестов, проверяющих отсутствие deadlock.</param>
    internal PgBoltStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements, bool retryOnConflict)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        Schema = SchemaName.Validate(schema);
        _prepare = usePreparedStatements;
        _retryOnConflict = retryOnConflict;
    }

    public string Schema { get; }

    private string Render(string template) => template.Replace("{schema}", Schema, StringComparison.Ordinal);

    private async Task PrepareAsync(NpgsqlCommand cmd, CancellationToken ct)
    {
        if (_prepare)
            await cmd.PrepareAsync(ct);
    }

    private Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> op, CancellationToken ct) =>
        _retryOnConflict ? DbRetry.RunAsync(op, ct) : op(ct);

    private Task<T> QueryAsync<T>(string sql, Action<NpgsqlParameterCollection> bind,
        Func<NpgsqlDataReader, CancellationToken, Task<T>> read, CancellationToken ct) =>
        RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var cmd = new NpgsqlCommand(sql, conn);
            bind(cmd.Parameters);
            await PrepareAsync(cmd, c);
            await using var reader = await cmd.ExecuteReaderAsync(c);
            return await read(reader, c);
        }, ct);

    /// <summary>In-process сигнал resolver'у (часть 5 §6.3); null — не подавать (хранилище без DI, тесты).</summary>
    internal ICompletionSignal? Completions { get; init; }

    /// <summary>§6.3: statement финализации записал сигналы — разбудить resolver процесса.</summary>
    private void SignalCompletions(long signals)
    {
        if (signals > 0)
            Completions?.Signal();
    }

    /// <summary>
    /// QueryAsync autocommit-statement'а финализации (§6.3): делегат возвращает результат и число записанных сигналов, сигнал
    /// подаётся после завершения QueryAsync. Внутри делегата ридер открыт, а коммит неявной транзакции наступает только на Sync
    /// после строк результата: resolver, разбуженный раньше, сигнала не увидит и прождёт ResolverInterval.
    /// </summary>
    private async Task<T> QueryAndSignalAsync<T>(string sql, Action<NpgsqlParameterCollection> bind,
        Func<NpgsqlDataReader, CancellationToken, Task<(T Result, long Signals)>> read, CancellationToken ct)
    {
        var (result, signals) = await QueryAsync(sql, bind, read, ct);
        SignalCompletions(signals);
        return result;
    }

    private Task<int> NonQueryAsync(string sql, Action<NpgsqlParameterCollection> bind, CancellationToken ct) =>
        RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var cmd = new NpgsqlCommand(sql, conn);
            bind(cmd.Parameters);
            await PrepareAsync(cmd, c);
            return await cmd.ExecuteNonQueryAsync(c);
        }, ct);
}
