using System.ComponentModel;

namespace PgBolt.Dashboard.Forms;

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: вид поля формы «Поставить» (§8).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum FieldKind { String, DateTime, Uuid, Integer, Number, Boolean, Enum, Object }

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: поле формы из схемы args; Path — путь через точку (Parent.Child).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record FormField(string Path, string Name, FieldKind Kind, bool Required, bool Nullable, IReadOnlyList<string?> EnumValues,
    IReadOnlyList<FormField> Children);

/// <summary>Внутренний тип dashboard, не входит в контракт пакета: ошибка у поля формы.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record FieldError(string Path, string Message);
