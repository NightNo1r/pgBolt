using System.Globalization;

namespace PgBolt.LoadTest.Env;

/// <summary>Строка <c>SHOW POOLS</c>.</summary>
/// <param name="MaxWaitMicros">Ожидание самого старого клиента в очереди пула: <c>maxwait</c> (секунды) и <c>maxwait_us</c> (микросекундная часть).</param>
internal sealed record PoolRow(
    string Database, string User, int ClActive, int ClWaiting, int SvActive, int SvIdle, long MaxWaitMicros);

/// <summary>
/// Разбор вывода <c>psql -At</c> админ-консоли PgBouncer: строки без заголовка, колонки через <c>|</c>.
/// Порядок колонок — PgBouncer 1.24, образ стенда закреплён.
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

    /// <summary>Значение <c>max_prepared_statements</c> из <c>SHOW CONFIG</c> (key|value|default|changeable).</summary>
    public static int? ParseMaxPreparedStatements(string psqlOutput) =>
        Rows(psqlOutput).FirstOrDefault(c => c[0] == "max_prepared_statements") is { } row ? Int(row[1]) : null;

    private static IEnumerable<string[]> Rows(string output) =>
        output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(l => l.Split('|'));

    private static int Int(string s) => int.Parse(s, CultureInfo.InvariantCulture);

    private static long Long(string s) => long.Parse(s, CultureInfo.InvariantCulture);
}
