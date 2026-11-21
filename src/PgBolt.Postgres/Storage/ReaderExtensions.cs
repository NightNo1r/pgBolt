using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

internal static class ReaderExtensions
{
    public static async Task<List<T>> ReadAllAsync<T>(this NpgsqlDataReader reader, Func<NpgsqlDataReader, T> map, CancellationToken ct)
    {
        var list = new List<T>();
        while (await reader.ReadAsync(ct))
            list.Add(map(reader));
        return list;
    }

    public static T? GetNullable<T>(this NpgsqlDataReader reader, int ordinal) where T : class =>
        reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);

    public static T? GetNullableValue<T>(this NpgsqlDataReader reader, int ordinal) where T : struct =>
        reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);

    /// <summary>
    /// Converts interval to TimeSpan like Npgsql, using 24-hour days. Returns null for ambiguous intervals containing months or values outside the
    /// TimeSpan range. Handles rows written outside the API (Part 4 §5.2).
    /// </summary>
    public static TimeSpan? ToTimeSpanOrNull(this NpgsqlInterval value)
    {
        if (value.Months != 0)
            return null;
        try
        {
            return TimeSpan.FromTicks(checked(value.Days * TimeSpan.TicksPerDay + value.Time * TimeSpan.TicksPerMicrosecond));
        }
        catch (OverflowException)
        {
            return null;
        }
    }
}
