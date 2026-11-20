using System.Text.Json;

namespace PgBolt.Dashboard.Forms;

/// <summary>
/// Проверка args по подмножеству схемы перед вызовом (§8): обязательность, тип, enum, формат. Лишние свойства не проверяются —
/// окончательная проверка при выполнении (JobArgumentsException, core).
/// </summary>
internal static class ArgsValidator
{
    public static IReadOnlyList<FieldError> Validate(IReadOnlyList<FormField> fields, JsonElement args)
    {
        var errors = new List<FieldError>();
        if (args.ValueKind != JsonValueKind.Object)
            errors.Add(new("", "ожидается объект"));
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
                    errors.Add(new(field.Path, "обязательное поле"));
                continue;
            }
            if (value.ValueKind == JsonValueKind.Null)
            {
                if (!field.Nullable)
                    errors.Add(new(field.Path, "не может быть null"));
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
            FieldKind.String when text is null => "ожидается строка",
            // Разбор System.Text.Json: те же форматы, что примет десериализация args при выполнении.
            FieldKind.DateTime when text is null || !value.TryGetDateTimeOffset(out _) => "ожидается дата-время ISO 8601",
            FieldKind.Uuid when text is null || !value.TryGetGuid(out _) => "ожидается UUID",
            FieldKind.Integer when value.ValueKind != JsonValueKind.Number || !value.TryGetInt64(out _) => "ожидается целое число",
            FieldKind.Number when value.ValueKind != JsonValueKind.Number => "ожидается число",
            FieldKind.Boolean when value.ValueKind is not (JsonValueKind.True or JsonValueKind.False) => "ожидается true или false",
            FieldKind.Enum when text is null || !field.EnumValues.Contains(text) => "значение не из списка",
            FieldKind.Object => "ожидается объект",
            _ => null,
        };
    }
}
