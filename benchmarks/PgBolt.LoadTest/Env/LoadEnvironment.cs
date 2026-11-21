using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Npgsql;
using PgBolt.LoadTest.Cli;
using Testcontainers.PostgreSql;

namespace PgBolt.LoadTest.Env;

/// <summary>
/// Run database and PgBouncer in containers on a shared Docker network, or use external <c>--connection</c> and <c>--bouncer-connection</c>
/// strings. An external PgBouncer provides no admin console channel.
/// </summary>
internal sealed class LoadEnvironment : IAsyncDisposable
{
    private const string Database = "pgbolt";
    private const string User = "postgres";
    private const string Password = "postgres";
    private const string BouncerAlias = "pgbouncer";
    private const int BouncerPort = 5432;

    private INetwork? _network;
    private PostgreSqlContainer? _postgres;
    private IContainer? _bouncer;

    private LoadEnvironment(string direct, string? bouncer)
    {
        DirectConnectionString = direct;
        BouncerConnectionString = bouncer;
    }

    public string DirectConnectionString { get; private set; }
    public string? BouncerConnectionString { get; private set; }
    public PgBouncerConsole? Console { get; private set; }

    public static async Task<LoadEnvironment> StartAsync(LoadTestOptions options, bool trackPlanning, CancellationToken ct)
    {
        if (options.Connection is { } external)
            return new LoadEnvironment(external, options.BouncerConnection);

        var env = new LoadEnvironment("", null);
        try
        {
            await env.StartContainersAsync(options, trackPlanning, ct);
            return env;
        }
        catch
        {
            await env.DisposeAsync();
            throw;
        }
    }

    private async Task StartContainersAsync(LoadTestOptions options, bool trackPlanning, CancellationToken ct)
    {
        _network = new NetworkBuilder().Build();
        await _network.CreateAsync(ct);

        // Сборщик Testcontainers по умолчанию выключает fsync, full_page_writes и synchronous_commit; стенд меряет
        // с рабочими значениями. Более поздний -c перекрывает ранний, поэтому --pg-setting идёт последним.
        _postgres = new PostgreSqlBuilder("postgres:17-alpine")
            .WithDatabase(Database)
            .WithUsername(User)
            .WithPassword(Password)
            .WithNetwork(_network)
            .WithNetworkAliases("pg")
            .WithCommand(["-c", "fsync=on", "-c", "full_page_writes=on", "-c", "synchronous_commit=on",
                .. PostgresCommand.Build(options, trackPlanning)])
            .Build();
        await _postgres.StartAsync(ct);
        DirectConnectionString = _postgres.GetConnectionString();

        // AUTH_TYPE=scram-sha-256: с md5 по умолчанию образ пишет в userlist.txt md5-хэш, с которым PgBouncer
        // не проходит SCRAM к PostgreSQL 17. Один max_prepared_statements для всех режимов позволяет прогонять
        // bouncer и bouncer-prepared на одном контейнере: безымянные statement'ы PgBouncer не отслеживает.
        _bouncer = new ContainerBuilder("edoburu/pgbouncer:v1.24.1-p1")
            .WithNetwork(_network)
            .WithNetworkAliases(BouncerAlias)
            .WithEnvironment(new Dictionary<string, string>
            {
                ["DB_HOST"] = "pg",
                ["DB_NAME"] = Database,
                ["DB_USER"] = User,
                ["DB_PASSWORD"] = Password,
                ["AUTH_TYPE"] = "scram-sha-256",
                ["POOL_MODE"] = "transaction",
                ["DEFAULT_POOL_SIZE"] = "40",
                ["MAX_CLIENT_CONN"] = "1000",
                ["MAX_PREPARED_STATEMENTS"] = "200",
                ["ADMIN_USERS"] = User,
                ["STATS_USERS"] = User,
            })
            .WithPortBinding(BouncerPort, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(BouncerPort))
            .Build();
        await _bouncer.StartAsync(ct);
        BouncerConnectionString = new NpgsqlConnectionStringBuilder
        {
            Host = _bouncer.Hostname,
            Port = _bouncer.GetMappedPublicPort(BouncerPort),
            Database = Database,
            Username = User,
            Password = Password,
        }.ConnectionString;
        Console = new PgBouncerConsole(_postgres, BouncerAlias, BouncerPort, User, Password);

        // В public, а не в схеме прогона: DROP SCHEMA ... CASCADE удалил бы расширение.
        await ExecuteAsync("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public", ct);
    }

    /// <summary>Creates an empty schema with a unique name for one run.</summary>
    public async Task<string> CreateSchemaNameAsync()
    {
        var schema = $"load_{Guid.NewGuid():N}"[..17];
        await ExecuteAsync($"CREATE SCHEMA {schema}", CancellationToken.None);
        return schema;
    }

    public Task DropSchemaAsync(string schema) =>
        ExecuteAsync($"DROP SCHEMA IF EXISTS {schema} CASCADE", CancellationToken.None);

    private async Task ExecuteAsync(string sql, CancellationToken ct)
    {
        await using var conn = await StandConnection.OpenAsync(DirectConnectionString, "schema", ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_bouncer is not null) await _bouncer.DisposeAsync();
        if (_postgres is not null) await _postgres.DisposeAsync();
        if (_network is not null) await _network.DisposeAsync();
    }
}
