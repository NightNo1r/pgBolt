using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;

namespace PgBolt.Dashboard.Forms;

/// <summary>
/// Reads args.{Path} form fields. A blank nullable field becomes null even if required. An optional blank field is
/// omitted and a required non-nullable field reports an error. A nullable object with all fields blank becomes null.
/// bestEffort preserves unparsed values as strings for JSON prefill. Dates and UUIDs stay strings for ArgsValidator to
/// check.
/// </summary>
internal static class FormArgs
{
    public static (JsonObject Args, IReadOnlyList<FieldError> Errors) Read(IReadOnlyList<FormField> fields, IFormCollection form, bool bestEffort)
    {
        var errors = new List<FieldError>();
        return (ReadObject(fields, form, bestEffort, errors), errors);
    }

    private static JsonObject ReadObject(IReadOnlyList<FormField> fields, IFormCollection form, bool bestEffort, List<FieldError> errors)
    {
        var obj = new JsonObject();
        foreach (var field in fields)
        {
            var text = form["args." + field.Path].ToString();
            if (field.Kind == FieldKind.Object)
                obj[field.Name] = field.Nullable && IsEmpty(field, form) ? null : ReadObject(field.Children, form, bestEffort, errors);
            else if (field.Kind == FieldKind.Boolean)
                obj[field.Name] = text == "true";
            else if (text.Length == 0)
            {
                if (field.Nullable)
                    obj[field.Name] = null;
                else if (field.Required && !bestEffort)
                    errors.Add(new(field.Path, "required field"));
            }
            else
            {
                var (value, error) = Parse(field.Kind, text);
                if (error is null || bestEffort)
                    obj[field.Name] = value ?? JsonValue.Create(text);
                else
                    errors.Add(new(field.Path, error));
            }
        }
        return obj;
    }

    /// <summary>Tests for an empty field or an object containing only empty fields. An unchecked box counts as empty.</summary>
    private static bool IsEmpty(FormField field, IFormCollection form) => field.Kind == FieldKind.Object
        ? field.Children.All(c => IsEmpty(c, form))
        : form["args." + field.Path].ToString().Length == 0;

    /// <summary>The field value or its parse error message.</summary>
    private static (JsonNode? Value, string? Error) Parse(FieldKind kind, string text) => kind switch
    {
        FieldKind.Integer => long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l)
            ? (JsonValue.Create(l), null) : (null, "expected an integer"),
        FieldKind.Number => decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d)
            ? (JsonValue.Create(d), null) : (null, "expected a number"),
        _ => (JsonValue.Create(text), null),
    };
}
