using Npgsql;

namespace PgBolt.Postgres;

/// <summary>
/// AOT-compatible data source for ConnectionString, using the slim builder with array support. No Reset On Close is always enabled for
/// compatibility with PgBouncer transaction pooling. PgBolt keeps no session state, and DISCARD ALL would remove server-side statements that
/// PgBouncer still tracks. Disabling UsePreparedStatements also disables automatic preparation (Max Auto Prepare=0).
/// </summary>
internal static class PgBoltDataSourceFactory
{
    public static NpgsqlDataSource Create(string connectionString, bool usePreparedStatements)
    {
        var csb = new NpgsqlConnectionStringBuilder(connectionString) { NoResetOnClose = true };
        if (!usePreparedStatements)
            csb.MaxAutoPrepare = 0;
        var builder = new NpgsqlSlimDataSourceBuilder(csb.ConnectionString);
        builder.EnableArrays();
        builder.EnableTransportSecurity();
        return builder.Build();
    }
}

internal sealed class PgBoltDataSource(NpgsqlDataSource value, bool owned) : IAsyncDisposable
{
    public NpgsqlDataSource Value { get; } = value;

    public ValueTask DisposeAsync() => owned ? Value.DisposeAsync() : ValueTask.CompletedTask;
}
