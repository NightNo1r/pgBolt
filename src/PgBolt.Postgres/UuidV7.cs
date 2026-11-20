using System.Buffers.Binary;

namespace PgBolt.Postgres;

/// <summary>Работа с 48-битным unix-ms префиксом UUIDv7 в порядке байтов PostgreSQL (big-endian).</summary>
internal static class UuidV7
{
    public static Guid LowerBound(DateOnly day) => LowerBound(new DateTimeOffset(day, TimeOnly.MinValue, TimeSpan.Zero));

    /// <summary>Наименьший UUIDv7 миллисекунды <paramref name="at"/>: 48-битный префикс unix-ms, остальные байты — нули.</summary>
    public static Guid LowerBound(DateTimeOffset at)
    {
        var ms = at.ToUnixTimeMilliseconds();
        Span<byte> bytes = stackalloc byte[16];   // нули: SkipLocalsInit не включён
        BinaryPrimitives.WriteInt64BigEndian(bytes, ms << 16);
        return new Guid(bytes, bigEndian: true);
    }

    public static DateTimeOffset GetTimestamp(Guid id)
    {
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        return DateTimeOffset.FromUnixTimeMilliseconds((long)(BinaryPrimitives.ReadUInt64BigEndian(bytes) >> 16));
    }

    /// <summary>Нижняя граница поиска job_events (§4.3): начало суток UTC минус одни сутки.</summary>
    public static DateTimeOffset EventsLowerBound(Guid id) =>
        new DateTimeOffset(GetTimestamp(id).UtcDateTime.Date, TimeSpan.Zero).AddDays(-1);
}
