using System.Text.Json;
using System.Text.Json.Schema;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace PgBolt.Dashboard.Tests.Support;

public enum Color { Red, Green }

public sealed record Address(string City, string? Zip);

public sealed record Order(string Name, int Count, double Price, bool Urgent, DateTimeOffset Due, Guid Ref,
    [property: JsonConverter(typeof(JsonStringEnumConverter<Color>))] Color Tint, Address Ship, int? Limit);

/// <summary>Схемы args — вывод JsonSchemaExporter (как публикует core).</summary>
public static class TestSchemas
{
    public static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    public static string Of<T>(JsonSerializerOptions? options = null) =>
        JsonSchemaExporter.GetJsonSchemaAsNode(options ?? JsonSerializerOptions.Default, typeof(T)).ToJsonString();

    public static string OrderJson => Of<Order>();

    public const string OrderRef = "0199b3a0-0000-7000-8000-000000000001";

    /// <summary>Валидная форма «Поставить» для <see cref="Order"/> (form-urlencoded).</summary>
    public static List<KeyValuePair<string, string>> OrderForm() =>
    [
        new("args.Name", "n"), new("args.Count", "2"), new("args.Price", "1.5"), new("args.Urgent", "true"),
        new("args.Due", "2026-10-05T12:00:00Z"), new("args.Ref", OrderRef), new("args.Tint", "Green"),
        new("args.Ship.City", "Москва"), new("args.Ship.Zip", ""), new("args.Limit", ""),
        new("queue", "orders"), new("priority", ""), new("delay", ""), new("runAt", ""), new("uniqueKey", ""), new("maxAttempts", ""),
    ];

    /// <summary>Форма с заменой (или добавлением; null — удалением) значения ключа.</summary>
    public static List<KeyValuePair<string, string>> With(this List<KeyValuePair<string, string>> form, string key, string? value)
    {
        var result = form.Where(p => p.Key != key).ToList();
        if (value is not null)
            result.Add(new(key, value));
        return result;
    }
}
