using System.ComponentModel;

namespace PgBolt.Dashboard.Views;

/// <summary>
/// Internal keyset page. A non-null ScannedTo marks a partial page after the scan budget expired. Next resumes the
/// search.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record Page<T>(IReadOnlyList<T> Rows, string? Next, DateTimeOffset? ScannedTo);
