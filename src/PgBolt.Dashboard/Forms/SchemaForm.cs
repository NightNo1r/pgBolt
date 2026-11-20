using System.Text.Json;
using System.Text.Json.Nodes;

namespace PgBolt.Dashboard.Forms;

/// <summary>
/// Поля формы «Поставить» из схемы args (вывод JsonSchemaExporter, §8 + отклонение 11). Схема с чем-либо сверх подмножества —
/// null: только режим JSON.
/// </summary>
internal static class SchemaForm
{
    private static readonly HashSet<string> Keywords =
        ["type", "properties", "required", "enum", "format", "pattern", "title", "description", "default", "$comment"];

    public static IReadOnlyList<FormField>? TryBuild(string argsSchemaJson)
    {
        JsonNode? root;
        try
        {
            root = JsonNode.Parse(argsSchemaJson);
        }
        catch (JsonException)
        {
            return null;
        }
        return root is JsonObject obj && Parse(obj, "", "", required: true) is { Kind: FieldKind.Object } field ? field.Children : null;
    }

    private static FormField? Parse(JsonNode? node, string path, string name, bool required)
    {
        if (node is not JsonObject schema || schema.Any(p => !Keywords.Contains(p.Key)))
            return null;
        if (!TryTypes(schema["type"], out var types))
            return null;
        var nullable = types.Remove("null");

        if (schema["enum"] is { } enumNode)
        {
            if (enumNode is not JsonArray values || values.Any(v => v is not null && v.GetValueKind() != JsonValueKind.String)
                || types.Any(t => t != "string") || schema.ContainsKey("properties"))
                return null;
            var options = values.Select(v => (string?)v).ToList();
            return new(path, name, FieldKind.Enum, required, nullable || options.Contains(null), options, []);
        }

        // pattern — только у integer / number, разрешённых из строки (JsonNumberHandling.AllowReadingFromString).
        var fromString = types.Count == 2 && types.Contains("string") && (types.Contains("integer") || types.Contains("number"));
        if (fromString)
            types.Remove("string");
        else if (schema.ContainsKey("pattern"))
            return null;
        if (types.Count != 1)
            return null;

        FieldKind? kind = types.Single() switch
        {
            "string" => (string?)schema["format"] switch
            {
                "date-time" => FieldKind.DateTime,
                "uuid" => FieldKind.Uuid,
                _ => FieldKind.String,
            },
            "integer" => FieldKind.Integer,
            "number" => FieldKind.Number,
            "boolean" => FieldKind.Boolean,
            "object" => FieldKind.Object,
            _ => null,
        };
        if (kind is null)
            return null;
        if (kind != FieldKind.Object)
            return schema.ContainsKey("properties") ? null : new(path, name, kind.Value, required, nullable, [], []);

        if (schema["properties"] is not JsonObject properties || schema["required"] is not (null or JsonArray))
            return null;
        var requiredNames = (schema["required"] as JsonArray)?.Select(v => (string?)v).ToHashSet() ?? [];
        var children = new List<FormField>();
        foreach (var (childName, childSchema) in properties)
        {
            var child = Parse(childSchema, path.Length == 0 ? childName : $"{path}.{childName}", childName, requiredNames.Contains(childName));
            if (child is null)
                return null;
            children.Add(child);
        }
        return new(path, name, FieldKind.Object, required, nullable, [], children);
    }

    /// <summary>type — строка или массив строк; без type — пустое множество (допустимо только у enum).</summary>
    private static bool TryTypes(JsonNode? node, out HashSet<string> types)
    {
        types = [];
        switch (node)
        {
            case null:
                return true;
            case JsonValue value when value.GetValueKind() == JsonValueKind.String:
                types.Add((string)value!);
                return true;
            case JsonArray array when array.All(t => t?.GetValueKind() == JsonValueKind.String):
                types.UnionWith(array.Select(t => (string)t!));
                return true;
            default:
                return false;
        }
    }
}
