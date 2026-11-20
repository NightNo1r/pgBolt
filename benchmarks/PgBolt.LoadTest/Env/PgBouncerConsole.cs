using System.Globalization;
using DotNet.Testcontainers.Containers;

namespace PgBolt.LoadTest.Env;

/// <summary>
/// Админ-консоль своего PgBouncer. Она принимает только simple query protocol, а Npgsql шлёт команды через
/// extended protocol и при открытии загружает типы, поэтому команды идут через <c>psql</c> в контейнере
/// PostgreSQL: он в той же сети, а <c>psql</c> есть в образе.
/// </summary>
internal sealed class PgBouncerConsole(IContainer postgres, string host, int port, string user, string password)
{
    public async Task<IReadOnlyList<PoolRow>> ShowPoolsAsync(CancellationToken ct) =>
        PgBouncerOutputParser.ParsePools(await PsqlAsync("SHOW POOLS", ct));

    public async Task<int?> MaxPreparedStatementsAsync(CancellationToken ct) =>
        PgBouncerOutputParser.ParseMaxPreparedStatements(await PsqlAsync("SHOW CONFIG", ct));

    /// <summary>Строка вида <c>PgBouncer 1.24.1</c>.</summary>
    public async Task<string> VersionAsync(CancellationToken ct) => (await PsqlAsync("SHOW VERSION", ct)).Trim();

    /// <summary>
    /// Зависший PgBouncer не должен блокировать срез и копить процессы в контейнере: psql завершает
    /// <c>timeout</c> внутри контейнера, отмена вызова — страховка, если не завершился сам exec.
    /// </summary>
    private const int PsqlTimeoutSeconds = 10;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(PsqlTimeoutSeconds + 5);

    private async Task<string> PsqlAsync(string command, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(Timeout);
        ExecResult result;
        try
        {
            result = await postgres.ExecAsync(
            [
                "env", $"PGPASSWORD={password}", "PGCONNECT_TIMEOUT=5",
                "timeout", PsqlTimeoutSeconds.ToString(CultureInfo.InvariantCulture),
                "psql", "-At", "-h", host, "-p", port.ToString(CultureInfo.InvariantCulture),
                "-U", user, "-d", "pgbouncer", "-c", command,
            ], timeout.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException($"psql \"{command}\" did not finish in {Timeout.TotalSeconds} s.");
        }
        return result.ExitCode == 0
            ? result.Stdout
            : throw new InvalidOperationException($"psql \"{command}\" exited with {result.ExitCode}: {result.Stderr}");
    }
}
