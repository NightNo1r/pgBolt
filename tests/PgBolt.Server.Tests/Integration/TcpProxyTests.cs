using System.Net;
using System.Net.Sockets;
using System.Text;
using Npgsql;
using PgBolt.Testing;

namespace PgBolt.Server.Tests.Integration;

/// <summary>TcpProxy против локального echo-сервера (без PostgreSQL).</summary>
public sealed class TcpProxyTests : IAsyncLifetime
{
    private readonly TcpListener _echo = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private Task _echoLoop = Task.CompletedTask;
    private TcpProxy _proxy = null!;
    private int _received;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Io = TimeSpan.FromSeconds(5);

    public ValueTask InitializeAsync()
    {
        _echo.Start();
        _echoLoop = EchoLoopAsync();
        _proxy = TcpProxy.Start("127.0.0.1", ((IPEndPoint)_echo.LocalEndpoint).Port);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _proxy.DisposeAsync();
        await _stop.CancelAsync();
        _echo.Stop();
        await _echoLoop;
        _stop.Dispose();
    }

    [Fact]
    public async Task Bytes_pass_through_in_both_directions()
    {
        using var client = await ConnectAsync();
        var stream = client.GetStream();

        await stream.WriteAsync("ping"u8.ToArray(), Ct);

        Assert.Equal("ping", await ReadAsync(stream, 4).WaitAsync(Io, Ct));
    }

    [Fact]
    public async Task Pause_holds_bytes_without_breaking_the_connection_and_resume_delivers_them()
    {
        using var client = await ConnectAsync();
        var stream = client.GetStream();
        await stream.WriteAsync("warm"u8.ToArray(), Ct);
        Assert.Equal("warm", await ReadAsync(stream, 4).WaitAsync(Io, Ct));

        _proxy.Pause();
        await stream.WriteAsync("held"u8.ToArray(), Ct);
        var read = ReadAsync(stream, 4);
        await Task.Delay(500, Ct); // явно: за 500 мс паузы байты не прошли

        Assert.False(read.IsCompleted, "bytes must not pass while the proxy is paused");
        Assert.True(_proxy.IsPaused);
        Assert.Equal(1, _proxy.ActiveConnections);
        Assert.Equal(4, Volatile.Read(ref _received));

        _proxy.Resume();

        Assert.Equal("held", await read.WaitAsync(Io, Ct));
    }

    [Fact]
    public async Task Side_closing_during_pause_drops_the_connection_and_the_held_bytes()
    {
        using (var client = await ConnectAsync())
        {
            var stream = client.GetStream();
            await stream.WriteAsync("warm"u8.ToArray(), Ct);
            Assert.Equal("warm", await ReadAsync(stream, 4).WaitAsync(Io, Ct));

            _proxy.Pause();
            await stream.WriteAsync("lost"u8.ToArray(), Ct);
        }

        await WaitAsync(() => _proxy.ActiveConnections == 0, "proxy tears down the connection closed during pause");
        _proxy.Resume();
        await Task.Delay(300, Ct); // явно: после Resume удержанные байты не дописываются

        Assert.Equal(4, Volatile.Read(ref _received));
    }

    [Fact]
    public async Task Connection_opened_during_pause_is_reset_and_connections_after_resume_work()
    {
        _proxy.Pause();
        Assert.True(await IsResetOnConnectAsync(), "a connection opened during pause must be reset");

        _proxy.Resume();
        using var after = await ConnectAsync();
        var stream = after.GetStream();
        await stream.WriteAsync("back"u8.ToArray(), Ct);

        Assert.Equal("back", await ReadAsync(stream, 4).WaitAsync(Io, Ct));
        Assert.Equal(4, Volatile.Read(ref _received));
    }

    [Fact]
    public async Task DropAll_breaks_existing_connections_and_new_ones_work()
    {
        using var first = await ConnectAsync();
        var s1 = first.GetStream();
        await s1.WriteAsync("one1"u8.ToArray(), Ct);
        Assert.Equal("one1", await ReadAsync(s1, 4).WaitAsync(Io, Ct));

        _proxy.DropAll();

        Assert.True(await IsClosedAsync(s1), "DropAll must break the existing connection");
        using var second = await ConnectAsync();
        var s2 = second.GetStream();
        await s2.WriteAsync("two2"u8.ToArray(), Ct);
        Assert.Equal("two2", await ReadAsync(s2, 4).WaitAsync(Io, Ct));
    }

    [Fact]
    public void ConnectionStringVia_rewrites_only_host_and_port()
    {
        var via = _proxy.ConnectionStringVia(
            "Host=db.example;Port=6432;Database=pgbolt;Username=u;Password=p;No Reset On Close=true;Max Auto Prepare=0");
        var b = new NpgsqlConnectionStringBuilder(via);

        Assert.Equal("127.0.0.1", b.Host);
        Assert.Equal(_proxy.Port, b.Port);
        Assert.Equal("pgbolt", b.Database);
        Assert.Equal("u", b.Username);
        Assert.True(b.NoResetOnClose);
        Assert.Equal(0, b.MaxAutoPrepare);
    }

    private async Task<TcpClient> ConnectAsync()
    {
        var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, _proxy.Port, Ct);
        return client;
    }

    private static async Task<string> ReadAsync(NetworkStream stream, int count)
    {
        var buffer = new byte[count];
        await stream.ReadExactlyAsync(buffer, Ct);
        return Encoding.ASCII.GetString(buffer);
    }

    /// <summary>RST прокси доходит либо уже при подключении, либо при первом чтении.</summary>
    private async Task<bool> IsResetOnConnectAsync()
    {
        TcpClient client;
        try
        {
            client = await ConnectAsync();
        }
        catch (SocketException)
        {
            return true;
        }
        using (client)
            return await IsClosedAsync(client.GetStream());
    }

    private static async Task<bool> IsClosedAsync(NetworkStream stream)
    {
        var buffer = new byte[1];
        try
        {
            return await stream.ReadAsync(buffer, Ct).AsTask().WaitAsync(Io, Ct) == 0;
        }
        catch (IOException)
        {
            return true; // RST от прокси
        }
    }

    private static async Task WaitAsync(Func<bool> condition, string because)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (!condition())
        {
            if (System.Diagnostics.Stopwatch.GetElapsedTime(started) > Io)
                Assert.Fail($"Timed out waiting for: {because}");
            await Task.Delay(20, Ct);
        }
    }

    private async Task EchoLoopAsync()
    {
        var sessions = new List<Task>();
        while (true)
        {
            TcpClient accepted;
            try
            {
                accepted = await _echo.AcceptTcpClientAsync(_stop.Token);
            }
            catch (Exception e) when (e is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                break; // echo-сервер остановлен
            }
            sessions.Add(EchoAsync(accepted));
        }
        await Task.WhenAll(sessions);
    }

    private async Task EchoAsync(TcpClient accepted)
    {
        using (accepted)
        {
            var stream = accepted.GetStream();
            var buffer = new byte[1024];
            try
            {
                int n;
                while ((n = await stream.ReadAsync(buffer, _stop.Token)) > 0)
                {
                    Interlocked.Add(ref _received, n);
                    await stream.WriteAsync(buffer.AsMemory(0, n), _stop.Token);
                }
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
            {
                return; // соединение оборвано прокси или тестом
            }
        }
    }
}

/// <summary>
/// TcpProxy против Npgsql: команда, отменённая на паузе, рвёт соединение сразу и не выполняется после Resume.
/// Основа сценариев LeaseFailureTests: таймаут вызова runtime срабатывает во время сбоя, а запрос, от которого runtime
/// отказался, не применяется на сервере после восстановления.
/// </summary>
public abstract class TcpProxyNpgsqlTests(PostgresFixture pg, ConnectionMode mode) : ServerIntegrationTestBase(pg, mode)
{
    [Fact]
    public async Task Command_cancelled_during_pause_breaks_its_connection_at_once_and_does_not_run_after_resume()
    {
        await Db.ExecAsync("CREATE TABLE {schema}.proxy_probe (n int)");
        var proxy = StartProxy();
        var csb = new NpgsqlConnectionStringBuilder(proxy.ConnectionStringVia(Db.ConnectionString))
        {
            CancellationTimeout = -1,
            Pooling = false,
        };
        await using var conn = new NpgsqlConnection(csb.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new NpgsqlCommand(Db.Sql("INSERT INTO {schema}.proxy_probe VALUES (1)"), conn);

        proxy.Pause();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var insert = cmd.ExecuteNonQueryAsync(timeout.Token);

        // Прокси остаётся на паузе, пока вызов не завершится: отмена не должна ждать сети. Удержанный CancelRequest
        // держал бы её до Resume или до Timeout подключения (15 с).
        var error = await Record.ExceptionAsync(() => insert.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        await WaitUntilAsync(() => proxy.ActiveConnections == 0, TimeSpan.FromSeconds(5),
            "proxy tears down the connection of the cancelled command");

        proxy.Resume();
        await Task.Delay(TimeSpan.FromMilliseconds(500), Ct); // явно: после Resume удержанный INSERT не дописывается

        Assert.Equal(0, await Db.ScalarAsync<long>("SELECT count(*) FROM {schema}.proxy_probe"));
    }
}

public sealed class TcpProxyNpgsqlTests_Direct(PostgresFixture pg) : TcpProxyNpgsqlTests(pg, ConnectionMode.Direct);

public sealed class TcpProxyNpgsqlTests_Bouncer(PostgresFixture pg) : TcpProxyNpgsqlTests(pg, ConnectionMode.Bouncer);
