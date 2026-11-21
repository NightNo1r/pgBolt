using System.Globalization;
using DotNet.Testcontainers.Containers;

namespace PgBolt.LoadTest.Env;

/// <summary>
/// Admin console for the local PgBouncer. It accepts only the simple query protocol. Npgsql uses the extended protocol and loads types on
/// connection, so commands run through <c>psql</c> in the PostgreSQL container. It shares the network and its image includes <c>psql</c>.
/// </summary>
internal sealed class PgBouncerConsole(IContainer postgres, string host, int port, string user, string password)
{
    public async Task<IReadOnlyList<PoolRow>> ShowPoolsAsync(CancellationToken ct) =>
        PgBouncerOutputParser.ParsePools(await PsqlAsync("SHOW POOLS", ct));

    public async Task<int?> MaxPreparedStatementsAsync(CancellationToken ct) =>
        PgBouncerOutputParser.ParseMaxPreparedStatements(await PsqlAsync("SHOW CONFIG", ct));

    /// <summary>A version string such as <c>PgBouncer 1.24.1</c>.</summary>
    public async Task<string> VersionAsync(CancellationToken ct) => (await PsqlAsync("SHOW VERSION", ct)).Trim();

    /// <summary>
    /// Runs psql under <c>timeout</c> inside the container so a stalled PgBouncer cannot block sampling or leave processes behind. Cancelling the
    /// call also bounds a stalled container exec.
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
