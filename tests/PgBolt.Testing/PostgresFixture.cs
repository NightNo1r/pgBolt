using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.PostgreSql;
using Xunit;

namespace PgBolt.Testing;

public enum ConnectionMode { Direct, Bouncer }

/// <summary>Один PostgreSQL и один PgBouncer (transaction mode) на тестовую сборку.</summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private const string Database = "pgbolt";
    private const string User = "postgres";
    private const string Password = "postgres";

    private const string BouncerIni = """
        [databases]
        pgbolt = host=pg port=5432 dbname=pgbolt user=postgres password=postgres

        [pgbouncer]
        listen_addr = 0.0.0.0
        listen_port = 6432
        auth_type = plain
        auth_file = /etc/pgbouncer/userlist.txt
        pool_mode = transaction
        max_prepared_statements = 0
        max_client_conn = 1000
        default_pool_size = 80
        ignore_startup_parameters = extra_float_digits
        """;

    private INetwork _network = null!;
    private PostgreSqlContainer _postgres = null!;
    private IContainer _bouncer = null!;

    public string DirectConnectionString { get; private set; } = "";
    public string BouncerConnectionString { get; private set; } = "";

    public string ConnectionString(ConnectionMode mode) =>
        mode == ConnectionMode.Direct ? DirectConnectionString : BouncerConnectionString;

    public async ValueTask InitializeAsync()
    {
        _network = new NetworkBuilder().Build();
        await _network.CreateAsync();

        _postgres = new PostgreSqlBuilder("postgres:17-alpine")
            .WithDatabase(Database)
            .WithUsername(User)
            .WithPassword(Password)
            .WithNetwork(_network)
            .WithNetworkAliases("pg")
            .WithCommand("-c", "max_connections=400", "-c", "max_locks_per_transaction=256", "-c", "max_prepared_transactions=10")
            .Build();
        await _postgres.StartAsync();
        DirectConnectionString = _postgres.GetConnectionString();

        _bouncer = new ContainerBuilder("edoburu/pgbouncer:v1.24.1-p1")
            .WithNetwork(_network)
            .WithResourceMapping(Encoding.UTF8.GetBytes(BouncerIni), "/etc/pgbouncer/pgbouncer.ini")
            .WithResourceMapping(Encoding.UTF8.GetBytes($"\"{User}\" \"{Password}\"\n"), "/etc/pgbouncer/userlist.txt")
            .WithPortBinding(6432, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(6432))
            .Build();
        await _bouncer.StartAsync();
        BouncerConnectionString =
            $"Host={_bouncer.Hostname};Port={_bouncer.GetMappedPublicPort(6432)};Database={Database};" +
            $"Username={User};Password={Password};No Reset On Close=true;Max Auto Prepare=0";
    }

    public async ValueTask DisposeAsync()
    {
        await _bouncer.DisposeAsync();
        await _postgres.DisposeAsync();
        await _network.DisposeAsync();
    }
}
