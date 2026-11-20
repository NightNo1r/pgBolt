using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;

namespace PgBolt.Dashboard.Forms;

/// <summary>
/// args из полей формы args.{Path}. Пустое значение: nullable-поле — null (в том числе обязательное), необязательное — не пишется,
/// обязательное — ошибка; nullable-объект, все поля которого пусты, — null. bestEffort (префилл JSON): неразобранное пишется
/// строкой, ошибок нет. Дата-время и UUID пишутся строкой — формат проверяет ArgsValidator.
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
                    errors.Add(new(field.Path, "обязательное поле"));
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

    /// <summary>Пустое поле или объект из пустых полей (флажок — снят).</summary>
    private static bool IsEmpty(FormField field, IFormCollection form) => field.Kind == FieldKind.Object
        ? field.Children.All(c => IsEmpty(c, form))
        : form["args." + field.Path].ToString().Length == 0;

    /// <summary>Значение поля или сообщение об ошибке разбора.</summary>
    private static (JsonNode? Value, string? Error) Parse(FieldKind kind, string text) => kind switch
    {
        FieldKind.Integer => long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l)
            ? (JsonValue.Create(l), null) : (null, "ожидается целое число"),
        FieldKind.Number => decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d)
            ? (JsonValue.Create(d), null) : (null, "ожидается число"),
        _ => (JsonValue.Create(text), null),
    };
}
