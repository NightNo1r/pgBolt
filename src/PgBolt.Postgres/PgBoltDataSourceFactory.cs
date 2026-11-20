using Npgsql;

namespace PgBolt.Postgres;

/// <summary>
/// AOT-совместимый источник данных для ConnectionString: slim builder с массивами. Чтобы строка подключения работала и
/// через PgBouncer в режиме transaction, сброс сессии при возврате соединения в пул выключен всегда (No Reset On Close):
/// PgBolt не держит состояния сессии, а DISCARD ALL удалил бы на сервере подготовленные statement'ы, которые PgBouncer
/// считает живыми. Без UsePreparedStatements выключена и авто-подготовка (Max Auto Prepare=0).
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
