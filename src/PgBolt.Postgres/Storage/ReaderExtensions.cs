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
    /// interval → TimeSpan так же, как чтение TimeSpan в Npgsql (день — 24 ч), но без исключения: null, если в interval есть
    /// месяцы (длительность неоднозначна) или значение не помещается в TimeSpan. Для строк, записанных в обход API (часть 4 §5.2).
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
