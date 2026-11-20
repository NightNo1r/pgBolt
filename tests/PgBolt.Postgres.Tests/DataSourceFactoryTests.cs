using Npgsql;

namespace PgBolt.Postgres.Tests;

public sealed class DataSourceFactoryTests
{
    private const string ConnectionString = "Host=localhost;Database=pgbolt;Username=u;Max Auto Prepare=20";

    [Theory]
    [InlineData(true, 20)]
    [InlineData(false, 0)]
    public void Reset_on_close_is_skipped_and_auto_prepare_follows_prepared_statements(bool prepared, int maxAutoPrepare)
    {
        using var ds = PgBoltDataSourceFactory.Create(ConnectionString, usePreparedStatements: prepared);
        var csb = new NpgsqlConnectionStringBuilder(ds.ConnectionString);

        // Сброс при возврате в пул (DISCARD ALL) стёр бы подготовленные statement'ы, которые PgBouncer считает живыми.
        Assert.True(csb.NoResetOnClose);
        Assert.Equal(maxAutoPrepare, csb.MaxAutoPrepare);
    }
}
