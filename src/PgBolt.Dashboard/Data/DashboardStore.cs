using System.Data;
using System.Globalization;
using Npgsql;
using PgBolt.Dashboard.Views;
using PgBolt.Postgres;

namespace PgBolt.Dashboard.Data;

/// <summary>
/// Dashboard reads (§6). Each statement uses its own READ COMMITTED, READ ONLY transaction with SET LOCAL
/// statement_timeout. Calls PrepareAsync only when UsePreparedStatements is enabled. Each screen has its own partial
/// file.
/// </summary>
internal sealed partial class DashboardStore(NpgsqlDataSource dataSource, string schema, bool usePreparedStatements,
    PgBoltDashboardOptions options, TimeProvider time)
{
    internal const int PageSize = 50;

    private readonly string _schema = SchemaName.Validate(schema);
    private readonly string _timeoutSql = string.Create(CultureInfo.InvariantCulture,
        $"SET LOCAL statement_timeout = {(long)Math.Ceiling(options.QueryTimeout.TotalMilliseconds)}");

    /// <summary>Test hook invoked before each statement with the SQL and parameter binding, so tests can EXPLAIN screen queries.</summary>
    internal Func<string, Action<NpgsqlParameterCollection>, CancellationToken, Task>? OnStatement { get; set; }

    private string Render(string template) => template.Replace("{schema}", _schema, StringComparison.Ordinal);

    internal async Task<T> ReadAsync<T>(string sql, Action<NpgsqlParameterCollection> bind,
        Func<NpgsqlDataReader, CancellationToken, Task<T>> read, CancellationToken ct)
    {
        if (OnStatement is { } observe)
            await observe(sql, bind, ct);
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        await using var batch = new NpgsqlBatch(conn, tx)
        {
            BatchCommands = { new("SET TRANSACTION READ ONLY"), new(_timeoutSql), new(sql) },
        };
        bind(batch.BatchCommands[2].Parameters);
        if (usePreparedStatements)
            await batch.PrepareAsync(ct);
        T result;
        await using (var r = await batch.ExecuteReaderAsync(ct))
            result = await read(r, ct);          // ридер уже на результате третьей команды: SET не возвращают строк
        await tx.CommitAsync(ct);
        return result;
    }

    /// <summary>Scans from the after cursor in chunks within ScanBudget to produce a list page (§6.3).</summary>
    private async Task<Page<T>> ScanPageAsync<T>(string? after, CursorShape shape, Func<ListKey?, int, CancellationToken, Task<Portion<T>>> next,
        Func<T, ListKey> keyOf, CancellationToken ct) =>
        ToPage(await ChunkedScan.RunAsync(Cursor.Decode(after, shape), PageSize, next, keyOf, options.ScanBudget, time, ct), shape);

    /// <summary>A chunked scan page. ScannedTo gives the continuation key timestamp only when the budget runs out.</summary>
    internal static Page<T> ToPage<T>(ScanResult<T> result, CursorShape shape) =>
        new(result.Rows, result.Next is { } next ? Cursor.Encode(next, shape) : null,
            result is { Incomplete: true, Next: { } key } ? KeyTime(key, shape) : null);

    /// <summary>Builds a page from up to PageSize + 1 fetched rows. An extra row indicates more data. Next is the last key returned to the caller.</summary>
    internal static Page<T> LimitPage<T>(List<T> fetched, Func<T, ListKey> keyOf, CursorShape shape)
    {
        if (fetched.Count <= PageSize)
            return new Page<T>(fetched, null, null);
        var rows = fetched.GetRange(0, PageSize);
        return new Page<T>(rows, Cursor.Encode(keyOf(rows[^1]), shape), null);
    }

    private static DateTimeOffset KeyTime(ListKey key, CursorShape shape) => shape switch
    {
        CursorShape.Id => UuidV7.GetTimestamp(key.Id),
        CursorShape.AtId or CursorShape.PriorityAtId => new DateTimeOffset(key.At!.Value),
        _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "Scan pages have no Text cursor."),
    };
}
