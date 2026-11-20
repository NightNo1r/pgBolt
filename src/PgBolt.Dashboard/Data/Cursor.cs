using System.Buffers.Binary;
using System.Buffers.Text;
using System.Text;
using PgBolt.Dashboard.Endpoints;

namespace PgBolt.Dashboard.Data;

/// <summary>Поля ключа в курсоре: Id; (At, Id); (Priority, At, Id); Text.</summary>
internal enum CursorShape : byte { Id, AtId, PriorityAtId, Text }

/// <summary>
/// Непрозрачный курсор keyset-пагинации (§6): base64url от байта версии, байта формы и полей формы в big-endian —
/// Priority (int16), At (тики int64, UTC), Id (16 байт), Text (UTF-8 до конца, не больше 800 байт).
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

    /// <summary>null или "" — без курсора; ошибка разбора или другая форма — <see cref="DashboardBadRequestException"/>.</summary>
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
