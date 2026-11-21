using System.ComponentModel;

namespace PgBolt.Dashboard.Forms;

/// <summary>Internal dashboard model for an enqueue form field kind (§8).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum FieldKind { String, DateTime, Uuid, Integer, Number, Boolean, Enum, Object }

/// <summary>Internal enqueue form field derived from the argument schema. Path uses dotted notation (Parent.Child).</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record FormField(string Path, string Name, FieldKind Kind, bool Required, bool Nullable, IReadOnlyList<string?> EnumValues,
    IReadOnlyList<FormField> Children);

/// <summary>Internal dashboard validation error for a form field.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record FieldError(string Path, string Message);
