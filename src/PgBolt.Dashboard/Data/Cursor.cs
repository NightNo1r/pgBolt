using System.Buffers.Binary;
using System.Buffers.Text;
using System.Text;
using PgBolt.Dashboard.Endpoints;

namespace PgBolt.Dashboard.Data;

/// <summary>Cursor key fields by shape: Id, (At, Id), (Priority, At, Id), or Text.</summary>
internal enum CursorShape : byte { Id, AtId, PriorityAtId, Text }

/// <summary>
/// Opaque keyset cursor (§6). The base64url payload contains version and shape bytes followed by big-endian fields:
/// Priority (int16), At (int64 UTC ticks), Id (16 bytes), or Text (remaining UTF-8 bytes, up to 800).
/// </summary>
internal static class Cursor
{
    private const byte Version = 1;
    private const int HeaderLength = 2;
    private const int MaxTextBytes = 800;
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static string Encode(ListKey key, CursorShape shape)
    {
        if (shape == CursorShape.Text)
            return Base64Url.EncodeToString([Version, (byte)shape, .. Encoding.UTF8.GetBytes(key.Text!)]);

        var bytes = new byte[HeaderLength + FieldsLength(shape)];
        bytes[0] = Version;
        bytes[1] = (byte)shape;
        var fields = bytes.AsSpan(HeaderLength);
        if (shape == CursorShape.PriorityAtId)
        {
            BinaryPrimitives.WriteInt16BigEndian(fields, key.Priority!.Value);
            fields = fields[2..];
        }
        if (shape != CursorShape.Id)
        {
            BinaryPrimitives.WriteInt64BigEndian(fields, key.At!.Value.Ticks);
            fields = fields[8..];
        }
        key.Id.TryWriteBytes(fields, bigEndian: true, out _);
        return Base64Url.EncodeToString(bytes);
    }

    /// <summary>A null or empty string represents no cursor. Invalid data or an unexpected shape throws <see cref="DashboardBadRequestException"/>.</summary>
    public static ListKey? Decode(string? value, CursorShape shape)
    {
        if (string.IsNullOrEmpty(value))
            return null;
        try
        {
            return Parse(Base64Url.DecodeFromChars(value), shape);
        }
        catch (Exception e) when (e is FormatException or ArgumentException)
        {
            throw Invalid();
        }
    }

    private static ListKey Parse(ReadOnlySpan<byte> bytes, CursorShape shape)
    {
        if (bytes.Length < HeaderLength || bytes[0] != Version || bytes[1] != (byte)shape)
            throw Invalid();
        var fields = bytes[HeaderLength..];
        if (shape == CursorShape.Text)
            return fields.Length <= MaxTextBytes ? new ListKey(Guid.Empty, Text: StrictUtf8.GetString(fields)) : throw Invalid();
        if (fields.Length != FieldsLength(shape))
            throw Invalid();

        short? priority = null;
        DateTime? at = null;
        if (shape == CursorShape.PriorityAtId)
        {
            priority = BinaryPrimitives.ReadInt16BigEndian(fields);
            fields = fields[2..];
        }
        if (shape != CursorShape.Id)
        {
            at = new DateTime(BinaryPrimitives.ReadInt64BigEndian(fields), DateTimeKind.Utc);
            fields = fields[8..];
        }
        return new ListKey(new Guid(fields, bigEndian: true), at, priority);
    }

    private static int FieldsLength(CursorShape shape) => shape switch
    {
        CursorShape.Id => 16,
        CursorShape.AtId => 8 + 16,
        CursorShape.PriorityAtId => 2 + 8 + 16,
        _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, null),
    };

    private static DashboardBadRequestException Invalid() => new("invalid cursor");
}
