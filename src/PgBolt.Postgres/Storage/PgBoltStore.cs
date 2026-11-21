using Npgsql;
using PgBolt.Postgres.Client;

namespace PgBolt.Postgres.Storage;

/// <summary>Hot-path SQL, organized by operation in partial files. Each operation takes one round trip.</summary>
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

    /// <param name="dataSource">PostgreSQL data source.</param>
    /// <param name="schema">Schema containing the PgBolt tables.</param>
    /// <param name="usePreparedStatements">Whether to prepare storage statements.</param>
    /// <param name="retryOnConflict">Set to false only in tests that verify deadlock avoidance.</param>
    internal PgBoltStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements, bool retryOnConflict)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        Schema = SchemaName.Validate(schema);
        _prepare = usePreparedStatements;
        _retryOnConflict = retryOnConflict;
    }

    public string Schema { get; }

    // Стенд считает отправленные команды и пакеты, включая повторные попытки.
    internal Action? DequeueExecuting { get; set; }

    private string Render(string template) => template.Replace("{schema}", Schema, StringComparison.Ordinal);

    private async Task PrepareAsync(NpgsqlCommand cmd, CancellationToken ct)
    {
        if (_prepare)
            await cmd.PrepareAsync(ct);
    }

    private Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> op, CancellationToken ct) =>
        _retryOnConflict ? DbRetry.RunAsync(op, ct) : op(ct);

    private Task<T> QueryAsync<T>(string sql, Action<NpgsqlParameterCollection> bind,
        Func<NpgsqlDataReader, CancellationToken, Task<T>> read, CancellationToken ct, Action? executing = null) =>
        RunAsync(async c =>
        {
            await using var conn = await _dataSource.OpenConnectionAsync(c);
            await using var cmd = new NpgsqlCommand(sql, conn);
            bind(cmd.Parameters);
            await PrepareAsync(cmd, c);
            executing?.Invoke();
            await using var reader = await cmd.ExecuteReaderAsync(c);
            return await read(reader, c);
        }, ct);

    /// <summary>In-process resolver wakeup (Part 5 §6.3). Null disables signaling for stores without DI and in tests.</summary>
    internal ICompletionSignal? Completions { get; init; }

    /// <summary>Wakes the local resolver when a finalization statement writes signals (§6.3).</summary>
    private void SignalCompletions(long signals)
    {
        if (signals > 0)
            Completions?.Signal();
    }

    /// <summary>
    /// Runs an autocommit finalization statement with QueryAsync (§6.3). The delegate returns the result and the number of signals written. Wake the
    /// resolver only after QueryAsync completes. The reader remains open inside the delegate, and the implicit transaction commits at Sync after
    /// result rows. A resolver woken earlier would miss the signal and wait for ResolverInterval.
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
