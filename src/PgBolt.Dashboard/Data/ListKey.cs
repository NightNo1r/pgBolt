namespace PgBolt.Dashboard.Data;

/// <summary>List sort key (§6.3). <see cref="CursorShape"/> determines which fields are populated.</summary>
internal readonly record struct ListKey(Guid Id, DateTime? At = null, short? Priority = null, string? Text = null);
