using System.Buffers.Binary;

namespace PgBolt.Postgres;

/// <summary>Handles UUIDv7's 48-bit Unix millisecond prefix in PostgreSQL's big-endian byte order.</summary>
internal static class UuidV7
{
    public static Guid LowerBound(DateOnly day) => LowerBound(new DateTimeOffset(day, TimeOnly.MinValue, TimeSpan.Zero));

    /// <summary>
    /// Lowest UUIDv7 boundary for millisecond <paramref name="at"/>, with the 48-bit Unix millisecond prefix and remaining bytes zeroed.
    /// </summary>
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

    /// <summary>Lower bound for job_events lookup (§4.3): the start of the UTC day minus one day.</summary>
    public static DateTimeOffset EventsLowerBound(Guid id) =>
        new DateTimeOffset(GetTimestamp(id).UtcDateTime.Date, TimeSpan.Zero).AddDays(-1);
}
