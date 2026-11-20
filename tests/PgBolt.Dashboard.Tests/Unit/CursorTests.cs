using System.Buffers.Text;
using PgBolt.Dashboard.Data;
using PgBolt.Dashboard.Endpoints;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class CursorTests
{
    private static readonly Guid Id = Guid.CreateVersion7();
    private static readonly DateTime At = new DateTime(2026, 10, 5, 14, 20, 33, DateTimeKind.Utc).AddTicks(1_234_560);

    [Fact]
    public void Round_trip_for_every_shape()
    {
        (CursorShape Shape, ListKey Key)[] cases =
        [
            (CursorShape.Id, new ListKey(Id)),
            (CursorShape.AtId, new ListKey(Id, At)),
            (CursorShape.PriorityAtId, new ListKey(Id, At, Priority: -5)),
            (CursorShape.PriorityAtId, new ListKey(Id, At, Priority: short.MaxValue)),
            (CursorShape.Text, new ListKey(Guid.Empty, Text: "отчёт/ежедневный" + new string('я', 184))),
        ];

        foreach (var (shape, key) in cases)
        {
            var encoded = Cursor.Encode(key, shape);

            Assert.Matches("^[A-Za-z0-9_-]+$", encoded);
            var decoded = Cursor.Decode(encoded, shape);
            Assert.Equal(key, decoded);
            if (decoded!.Value.At is { } at)
                Assert.Equal(DateTimeKind.Utc, at.Kind);
        }
    }

    public static TheoryData<string> InvalidCursors()
    {
        var atId = Base64Url.DecodeFromChars(Cursor.Encode(new ListKey(Id, At), CursorShape.AtId));
        return new TheoryData<string>
        {
            "!!",
            Base64Url.EncodeToString("garbage!"u8),
            Cursor.Encode(new ListKey(Id), CursorShape.Id),
            Base64Url.EncodeToString(atId[..^1]),
            Base64Url.EncodeToString([.. atId, 0]),
            Base64Url.EncodeToString([2, .. atId[1..]]),
        };
    }

    [Theory]
    [MemberData(nameof(InvalidCursors))]
    public void Invalid_cursor_is_bad_request(string value) =>
        Assert.Throws<DashboardBadRequestException>(() => Cursor.Decode(value, CursorShape.AtId));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Missing_cursor_is_null(string? value) => Assert.Null(Cursor.Decode(value, CursorShape.AtId));
}
