using System.Text.Json;
using PgBolt.Dashboard.Forms;
using PgBolt.Dashboard.Tests.Support;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class Node { public Node? Next { get; set; } }

public sealed record WithArray(int[] Items);

public sealed record WithList(List<Address> Items);

public sealed record WithDictionary(Dictionary<string, int> Items);

public sealed record WithElement(JsonElement Payload);

public sealed class SchemaFormTests
{
    private static FormField Field(IReadOnlyList<FormField> fields, string name) => fields.Single(f => f.Name == name);

    [Fact]
    public void Supported_subset_builds_fields()
    {
        var fields = SchemaForm.TryBuild(TestSchemas.OrderJson);

        Assert.NotNull(fields);
        Assert.Equal(["Name", "Count", "Price", "Urgent", "Due", "Ref", "Tint", "Ship", "Limit"], fields.Select(f => f.Name));
        Assert.All(fields, f => Assert.True(f.Required, f.Name));
        Assert.Equal((FieldKind.String, false), (Field(fields, "Name").Kind, Field(fields, "Name").Nullable));
        Assert.Equal(FieldKind.Integer, Field(fields, "Count").Kind);
        Assert.Equal(FieldKind.Number, Field(fields, "Price").Kind);
        Assert.Equal(FieldKind.Boolean, Field(fields, "Urgent").Kind);
        Assert.Equal(FieldKind.DateTime, Field(fields, "Due").Kind);
        Assert.Equal(FieldKind.Uuid, Field(fields, "Ref").Kind);
        var tint = Field(fields, "Tint");
        Assert.Equal(FieldKind.Enum, tint.Kind);
        Assert.Equal(["Red", "Green"], tint.EnumValues);
        var ship = Field(fields, "Ship");
        Assert.Equal((FieldKind.Object, false), (ship.Kind, ship.Nullable));
        var city = Field(ship.Children, "City");
        Assert.Equal(("Ship.City", FieldKind.String, false, true), (city.Path, city.Kind, city.Nullable, city.Required));
        var zip = Field(ship.Children, "Zip");
        Assert.Equal(("Ship.Zip", FieldKind.String, true, true), (zip.Path, zip.Kind, zip.Nullable, zip.Required));
        var limit = Field(fields, "Limit");
        Assert.Equal((FieldKind.Integer, true), (limit.Kind, limit.Nullable));
    }

    [Fact]
    public void Web_number_handling_maps_to_numbers()
    {
        var fields = SchemaForm.TryBuild(TestSchemas.Of<Order>(TestSchemas.Web));

        Assert.NotNull(fields);
        Assert.Equal((FieldKind.Integer, false), (Field(fields, "count").Kind, Field(fields, "count").Nullable));
        Assert.Equal(FieldKind.Number, Field(fields, "price").Kind);
        Assert.Equal((FieldKind.Integer, true), (Field(fields, "limit").Kind, Field(fields, "limit").Nullable));
        Assert.Equal(FieldKind.String, Field(fields, "name").Kind);
    }

    public static TheoryData<string> Unsupported => new()
    {
        TestSchemas.Of<WithArray>(),
        TestSchemas.Of<WithList>(),
        TestSchemas.Of<WithDictionary>(),
        TestSchemas.Of<WithElement>(),
        TestSchemas.Of<Node>(),
        """{"type":"object","properties":{"X":{"anyOf":[{"type":"string"},{"type":"integer"}]}}}""",
    };

    [Theory]
    [MemberData(nameof(Unsupported))]
    public void Unsupported_schemas_fall_back_to_json(string schema) => Assert.Null(SchemaForm.TryBuild(schema));

    [Fact]
    public void Annotations_are_ignored()
    {
        var fields = SchemaForm.TryBuild("""
            {"type":"object","title":"T","description":"d","$comment":"c",
             "properties":{"Name":{"type":"string","description":"имя","title":"N","default":"x"}},"required":["Name"]}
            """);

        Assert.NotNull(fields);
        Assert.Equal(FieldKind.String, Assert.Single(fields).Kind);
    }
}
