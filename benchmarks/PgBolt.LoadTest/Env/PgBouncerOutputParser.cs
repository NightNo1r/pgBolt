using System.Globalization;

namespace PgBolt.LoadTest.Env;

/// <summary>A <c>SHOW POOLS</c> row.</summary>
/// <param name="MaxWaitMicros">
/// Wait of the oldest queued client, combining <c>maxwait</c> in seconds and the <c>maxwait_us</c> fractional microseconds.
/// </param>
internal sealed record PoolRow(
    string Database, string User, int ClActive, int ClWaiting, int SvActive, int SvIdle, long MaxWaitMicros);

/// <summary>
/// Parses PgBouncer admin console output from <c>psql -At</c>: no header and columns separated by <c>|</c>. Column order matches the harness's
/// pinned PgBouncer 1.24 image.
/// </summary>
internal static class PgBouncerOutputParser
{
    // database|user|cl_active|cl_waiting|cl_active_cancel_req|cl_waiting_cancel_req|sv_active|sv_active_cancel|
    // sv_being_canceled|sv_idle|sv_used|sv_tested|sv_login|maxwait|maxwait_us|pool_mode|load_balance_hosts
    public static IReadOnlyList<PoolRow> ParsePools(string psqlOutput) =>
    [
        .. Rows(psqlOutput).Select(c => new PoolRow(
            c[0], c[1], Int(c[2]), Int(c[3]), Int(c[6]), Int(c[9]), Long(c[13]) * 1_000_000 + Long(c[14]))),
    ];

    /// <summary>Value of <c>max_prepared_statements</c> from <c>SHOW CONFIG</c> (key|value|default|changeable).</summary>
    public static int? ParseMaxPreparedStatements(string psqlOutput) =>
        Rows(psqlOutput).FirstOrDefault(c => c[0] == "max_prepared_statements") is { } row ? Int(row[1]) : null;

    private static IEnumerable<string[]> Rows(string output) =>
        output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(l => l.Split('|'));

    private static int Int(string s) => int.Parse(s, CultureInfo.InvariantCulture);

    private static long Long(string s) => long.Parse(s, CultureInfo.InvariantCulture);
}
