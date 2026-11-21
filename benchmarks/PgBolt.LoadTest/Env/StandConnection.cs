using Npgsql;

namespace PgBolt.LoadTest.Env;

/// <summary>
/// Harness connections for sampling, probing locks, holding xmin, and preparing the schema share an <c>application_name</c> prefix. Sampling
/// excludes them from PgBolt backend counts. Host and producer connections use <c>pgbolt-load-*</c>, a distinct prefix.
/// </summary>
internal static class StandConnection
{
    public const string ApplicationNamePrefix = "pgbolt-loadtest-";

    public static NpgsqlConnectionStringBuilder For(string connectionString, string purpose) =>
        new(connectionString) { ApplicationName = ApplicationNamePrefix + purpose };

    /// <summary>Opens a harness connection, disposing it if opening fails.</summary>
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
