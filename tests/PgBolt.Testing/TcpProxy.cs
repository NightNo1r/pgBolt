using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Npgsql;

namespace PgBolt.Testing;

/// <summary>
/// Управляемый TCP-прокси 127.0.0.1:случайный порт → target. <see cref="Pause"/> перестаёт пересылать байты в обе
/// стороны, не разрывая соединений: прочитанное держится в памяти и уходит после <see cref="Resume"/> (фрагмент, запись
/// которого уже началась в момент паузы, может пройти). Если во время паузы одна из сторон закрыла соединение, оно
/// рвётся целиком, а удержанные байты отбрасываются — как пакеты, потерянные вместе с соединением: отменённый клиентом
/// запрос не выполнится на сервере после восстановления. <see cref="DropAll"/> рвёт все текущие соединения (RST).
/// Новое соединение во время паузы принимается и сразу рвётся (RST): база недоступна, новые соединения не проходят.
/// </summary>
/// <remarks>
/// Разрыв новых соединений нужен для отмены запросов Npgsql: отменяя команду, Npgsql сначала синхронно отправляет
/// PostgreSQL CancelRequest через новое соединение и только потом рвёт своё (Cancellation Timeout = -1 отменяет лишь
/// ожидание ответа). Если бы прокси удерживал и это соединение, отмена — а с ней и таймаут вызова — ждала бы Resume,
/// соединение с отменённым запросом оставалось бы живым, и после Resume запрос выполнился бы на сервере.
/// </remarks>
public sealed class TcpProxy : IAsyncDisposable
{
    private readonly string _targetHost;
    private readonly int _targetPort;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<Link, byte> _links = new();
    private readonly Lock _pauseGate = new();
    private readonly Task _acceptLoop;
    private TaskCompletionSource _resumed = NewResumed(completed: true);
    private volatile bool _paused;

    private TcpProxy(string targetHost, int targetPort)
    {
        _targetHost = targetHost;
        _targetPort = targetPort;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = AcceptLoopAsync();
    }

    public int Port { get; }
    public bool IsPaused => _paused;
    public int ActiveConnections => _links.Count;

    public static TcpProxy Start(string targetHost, int targetPort) => new(targetHost, targetPort);

    /// <summary>Прокси к Host/Port строки подключения Npgsql.</summary>
    public static TcpProxy For(string connectionString)
    {
        var csb = new NpgsqlConnectionStringBuilder(connectionString);
        var host = csb.Host ?? throw new ArgumentException("Connection string has no Host.", nameof(connectionString));
        return Start(host, csb.Port);
    }

    /// <summary>Та же строка подключения, но через прокси: меняются только Host и Port.</summary>
    public string ConnectionStringVia(string connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString) { Host = "127.0.0.1", Port = Port }.ConnectionString;

    public void Pause()
    {
        lock (_pauseGate)
        {
            if (_paused)
                return;
            _resumed = NewResumed(completed: false);
            _paused = true;
        }
    }

    public void Resume()
    {
        TaskCompletionSource resumed;
        lock (_pauseGate)
        {
            if (!_paused)
                return;
            _paused = false;
            resumed = _resumed;
        }
        resumed.TrySetResult();
    }

    public void DropAll()
    {
        foreach (var link in _links.Keys)
            link.Abort();
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        Resume();
        DropAll();
        await _acceptLoop;
        _stop.Dispose();
    }

    private static TaskCompletionSource NewResumed(bool completed)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (completed)
            tcs.SetResult();
        return tcs;
    }

    private Task WaitResumedAsync(CancellationToken ct)
    {
        Task resumed;
        lock (_pauseGate)
            resumed = _resumed.Task;
        return resumed.WaitAsync(ct);
    }

    private async Task AcceptLoopAsync()
    {
        while (true)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stop.Token);
            }
            catch (Exception e) when (e is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                return; // прокси остановлен
            }
            _ = ConnectAsync(client);
        }
    }

    private async Task ConnectAsync(TcpClient client)
    {
        var target = new TcpClient();
        try
        {
            await target.ConnectAsync(_targetHost, _targetPort, _stop.Token);
        }
        catch (Exception e) when (e is SocketException or OperationCanceledException or ObjectDisposedException)
        {
            Link.Kill(client);
            Link.Kill(target);
            return;
        }
        if (_paused)
        {
            // Во время паузы база недоступна (см. remarks); проверка после connect покрывает и паузу, начавшуюся в нём.
            Link.Kill(client);
            Link.Kill(target);
            return;
        }

        client.NoDelay = true;
        target.NoDelay = true;
        var link = new Link(this, client, target);
        _links.TryAdd(link, 0);
        if (_stop.IsCancellationRequested)
        {
            link.Abort();
            return;
        }
        await link.RunAsync();
    }

    private sealed class Link
    {
        private readonly TcpProxy _proxy;
        private readonly TcpClient _client;
        private readonly TcpClient _target;
        private readonly NetworkStream _clientStream;
        private readonly NetworkStream _targetStream;
        private readonly CancellationTokenSource _cts = new();
        private int _aborted;

        public Link(TcpProxy proxy, TcpClient client, TcpClient target)
        {
            _proxy = proxy;
            _client = client;
            _target = target;
            _clientStream = client.GetStream();
            _targetStream = target.GetStream();
        }

        public Task RunAsync()
        {
            var up = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
            var down = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
            return Task.WhenAll(
                ReadLoopAsync(_clientStream, up.Writer),
                WriteLoopAsync(up.Reader, _targetStream),
                ReadLoopAsync(_targetStream, down.Writer),
                WriteLoopAsync(down.Reader, _clientStream));
        }

        /// <summary>Читает непрерывно (и во время паузы), чтобы заметить закрытие стороны.</summary>
        private async Task ReadLoopAsync(NetworkStream source, ChannelWriter<byte[]> pending)
        {
            var buffer = new byte[16 * 1024];
            try
            {
                while (true)
                {
                    var n = await source.ReadAsync(buffer, _cts.Token);
                    if (n == 0)
                        break;
                    pending.TryWrite(buffer.AsSpan(0, n).ToArray());
                }
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
            {
                Abort();
                return;
            }

            if (_proxy.IsPaused)
                Abort();                // сторона закрылась во время паузы: удержанное теряется вместе с соединением
            else
                pending.TryComplete();  // дописать остаток, затем закрыть обе стороны (WriteLoopAsync)
        }

        private async Task WriteLoopAsync(ChannelReader<byte[]> pending, NetworkStream destination)
        {
            try
            {
                await foreach (var chunk in pending.ReadAllAsync(_cts.Token))
                {
                    await _proxy.WaitResumedAsync(_cts.Token);
                    await destination.WriteAsync(chunk, _cts.Token);
                }
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
            {
                Abort();
                return;
            }
            Abort(); // источник закрылся и остаток дописан
        }

        public void Abort()
        {
            if (Interlocked.Exchange(ref _aborted, 1) == 1)
                return;
            _proxy._links.TryRemove(this, out _);
            _cts.Cancel();
            Kill(_client);
            Kill(_target);
        }

        public static void Kill(TcpClient connection)
        {
            if (connection.Client is { Connected: true } socket)
                socket.LingerState = new LingerOption(true, 0); // RST вместо FIN
            connection.Dispose();
        }
    }
}
