using Npgsql;

namespace PgBolt.LoadTest.Env;

/// <summary>
/// Служебные соединения стенда (срез, проба блокировок, удержание xmin, подготовка схемы) помечаются общим
/// префиксом <c>application_name</c>: срез не считает их backend'ами PgBolt. Хосты и продюсер называются
/// <c>pgbolt-load-*</c> и с префиксом не пересекаются.
/// </summary>
internal static class StandConnection
{
    public const string ApplicationNamePrefix = "pgbolt-loadtest-";

    public static NpgsqlConnectionStringBuilder For(string connectionString, string purpose) =>
        new(connectionString) { ApplicationName = ApplicationNamePrefix + purpose };

    /// <summary>Открытое служебное соединение; при сбое открытия оно освобождается.</summary>
    public static async Task<NpgsqlConnection> OpenAsync(string connectionString, string purpose, CancellationToken ct)
    {
        var conn = new NpgsqlConnection(For(connectionString, purpose).ConnectionString);
        try
        {
            await conn.OpenAsync(ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }
}
