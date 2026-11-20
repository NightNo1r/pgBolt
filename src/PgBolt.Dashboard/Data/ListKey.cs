namespace PgBolt.Dashboard.Data;

/// <summary>Ключ сортировки списка (§6.3): какие поля заданы — определяет <see cref="CursorShape"/>.</summary>
internal readonly record struct ListKey(Guid Id, DateTime? At = null, short? Priority = null, string? Text = null);
