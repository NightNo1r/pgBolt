using System.Text.Json;
using System.Text.Json.Nodes;
using PgBolt.Dashboard.Forms;
using PgBolt.Dashboard.Tests.Support;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class ArgsValidatorTests
{
    private static readonly IReadOnlyList<FormField> Fields = SchemaForm.TryBuild(TestSchemas.OrderJson)!;

    private static JsonObject Valid() => JsonNode.Parse("""
        {"Name":"n","Count":2,"Price":1.5,"Urgent":true,"Due":"2026-10-05T12:00:00Z","Ref":"0199b3a0-0000-7000-8000-000000000001",
         "Tint":"Red","Ship":{"City":"Москва","Zip":null},"Limit":null}
        """)!.AsObject();

    private static IReadOnlyList<FieldError> Validate(Action<JsonObject> change)
    {
        var args = Valid();
        change(args);
        return ArgsValidator.Validate(Fields, JsonSerializer.SerializeToElement(args));
    }

    private static string ErrorPath(Action<JsonObject> change) => Assert.Single(Validate(change)).Path;

    [Fact]
    public void Valid_document_has_no_errors() => Assert.Empty(Validate(_ => { }));

    [Fact]
    public void Missing_required_field() => Assert.Equal("Name", ErrorPath(a => a.Remove("Name")));

    [Fact]
    public void Wrong_type() => Assert.Equal("Count", ErrorPath(a => a["Count"] = "x"));

    [Fact]
    public void Value_outside_enum() => Assert.Equal("Tint", ErrorPath(a => a["Tint"] = "Blue"));

    [Fact]
    public void Bad_date_time() => Assert.Equal("Due", ErrorPath(a => a["Due"] = "2026-13-01"));

    [Fact]
    public void Bad_uuid() => Assert.Equal("Ref", ErrorPath(a => a["Ref"] = "nope"));

    [Theory]
    [InlineData("10/05/2026 12:00")]
    [InlineData("5 Oct 2026")]
    public void Date_time_outside_iso_is_rejected(string value) => Assert.Equal("Due", ErrorPath(a => a["Due"] = value));

    [Theory]
    [InlineData("{0199b3a0-0000-7000-8000-000000000001}")]
    [InlineData("0199b3a0000070008000000000000001")]
    public void Uuid_outside_hyphenated_form_is_rejected(string value) => Assert.Equal("Ref", ErrorPath(a => a["Ref"] = value));

    [Fact]
    public void Null_in_not_nullable_field() => Assert.Equal("Name", ErrorPath(a => a["Name"] = null));

    [Fact]
    public void Null_in_required_nullable_field_is_valid() => Assert.Empty(Validate(a => a["Limit"] = null));

    [Fact]
    public void Nested_field_has_dotted_path() => Assert.Equal("Ship.City", ErrorPath(a => a["Ship"]!.AsObject().Remove("City")));
}
