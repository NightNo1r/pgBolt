using System.Text.Json;

namespace PgBolt.Dashboard.Forms;

/// <summary>
/// Validates required fields, types, enums and formats against the supported schema subset before enqueueing (§8).
/// Extra properties are ignored here. Final validation runs at execution and can raise JobArgumentsException.
/// </summary>
internal static class ArgsValidator
{
    public static IReadOnlyList<FieldError> Validate(IReadOnlyList<FormField> fields, JsonElement args)
    {
        var errors = new List<FieldError>();
        if (args.ValueKind != JsonValueKind.Object)
            errors.Add(new("", "expected an object"));
        else
            ValidateObject(fields, args, errors);
        return errors;
    }

    private static void ValidateObject(IReadOnlyList<FormField> fields, JsonElement obj, List<FieldError> errors)
    {
        foreach (var field in fields)
        {
            if (!obj.TryGetProperty(field.Name, out var value))
            {
                if (field.Required)
                    errors.Add(new(field.Path, "required field"));
                continue;
            }
            if (value.ValueKind == JsonValueKind.Null)
            {
                if (!field.Nullable)
                    errors.Add(new(field.Path, "cannot be null"));
                continue;
            }
            if (field.Kind == FieldKind.Object && value.ValueKind == JsonValueKind.Object)
            {
                ValidateObject(field.Children, value, errors);
                continue;
            }
            if (Error(field, value) is { } message)
                errors.Add(new(field.Path, message));
        }
    }

    private static string? Error(FormField field, JsonElement value)
    {
        var text = value.ValueKind == JsonValueKind.String ? value.GetString() : null;
        return field.Kind switch
        {
            FieldKind.String when text is null => "expected a string",
            // Разбор System.Text.Json: те же форматы, что примет десериализация args при выполнении.
            FieldKind.DateTime when text is null || !value.TryGetDateTimeOffset(out _) => "expected an ISO 8601 timestamp",
            FieldKind.Uuid when text is null || !value.TryGetGuid(out _) => "expected a UUID",
            FieldKind.Integer when value.ValueKind != JsonValueKind.Number || !value.TryGetInt64(out _) => "expected an integer",
            FieldKind.Number when value.ValueKind != JsonValueKind.Number => "expected a number",
            FieldKind.Boolean when value.ValueKind is not (JsonValueKind.True or JsonValueKind.False) => "expected true or false",
            FieldKind.Enum when text is null || !field.EnumValues.Contains(text) => "value is not in the list",
            FieldKind.Object => "expected an object",
            _ => null,
        };
    }
}
