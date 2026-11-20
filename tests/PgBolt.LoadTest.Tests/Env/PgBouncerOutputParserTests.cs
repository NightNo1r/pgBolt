using PgBolt.LoadTest.Env;

namespace PgBolt.LoadTest.Tests.Env;

public sealed class PgBouncerOutputParserTests
{
    // Вывод psql -At с edoburu/pgbouncer:v1.24.1-p1 после одного запроса через пул pgbolt.
    private const string ShowPools = """
        pgbolt|postgres|0|0|0|0|0|0|0|1|0|0|0|0|0|transaction|
        pgbouncer|pgbouncer|1|0|0|0|0|0|0|0|0|0|0|0|0|statement|

        """;

    // Фрагмент SHOW CONFIG того же образа (key|value|default|changeable).
    private const string ShowConfig = """
        admin_users|postgres||yes
        max_client_conn|1000|100|yes
        max_prepared_statements|200|200|yes
        pool_mode|transaction|session|yes

        """;

    [Fact]
    public void Parses_show_pools_of_1_24()
    {
        var pools = PgBouncerOutputParser.ParsePools(ShowPools);

        Assert.Equal(
            [
                new PoolRow("pgbolt", "postgres", 0, 0, 0, 1, 0),
                new PoolRow("pgbouncer", "pgbouncer", 1, 0, 0, 0, 0),
            ],
            pools);
    }

    [Fact]
    public void Parses_show_config_max_prepared_statements()
    {
        Assert.Equal(200, PgBouncerOutputParser.ParseMaxPreparedStatements(ShowConfig));
        Assert.Null(PgBouncerOutputParser.ParseMaxPreparedStatements("admin_users|postgres||yes\n"));
    }

    [Fact]
    public void Max_wait_combines_seconds_and_micros()
    {
        var pool = Assert.Single(PgBouncerOutputParser.ParsePools(
            "pgbolt|postgres|40|7|0|0|40|0|0|0|0|0|0|1|250000|transaction|\n"));

        Assert.Equal(new PoolRow("pgbolt", "postgres", 40, 7, 40, 0, 1_250_000), pool);
    }
}
