using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using PgBolt.Dashboard.Forms;
using PgBolt.Dashboard.Tests.Support;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed record WithOptionalAddress(Address? Ship);

public sealed class FormArgsTests
{
    private static readonly IReadOnlyList<FormField> Fields = SchemaForm.TryBuild(TestSchemas.Of<WithOptionalAddress>())!;

    private static FormCollection Form(string city, string zip) =>
        new(new Dictionary<string, StringValues> { ["args.Ship.City"] = city, ["args.Ship.Zip"] = zip });

    [Fact]
    public void Nullable_object_with_empty_fields_is_null()
    {
        var (args, errors) = FormArgs.Read(Fields, Form("", ""), bestEffort: false);

        Assert.Empty(errors);
        Assert.Equal("""{"Ship":null}""", args.ToJsonString());
    }

    [Fact]
    public void Nullable_object_with_a_value_is_an_object()
    {
        var (args, errors) = FormArgs.Read(Fields, Form("Москва", ""), bestEffort: false);

        Assert.Empty(errors);
        Assert.Equal("Москва", (string?)args["Ship"]!["City"]);
        Assert.Null(args["Ship"]!["Zip"]);
    }
}
