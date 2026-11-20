using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>
/// Внутренний тип dashboard, не входит в контракт пакета: страница keyset-списка. ScannedTo != null — неполная страница
/// (исчерпан бюджет обхода), Next — «продолжить поиск».
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record Page<T>(IReadOnlyList<T> Rows, string? Next, DateTimeOffset? ScannedTo);
