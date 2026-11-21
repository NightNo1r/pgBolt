namespace PgBolt;

/// <summary>
/// Indicates that retrying a job would be futile. The job ends in Failed without retrying (Part 3 §3.5, §5.2). Applications can derive their own
/// exception types.
/// </summary>
public class JobAbortedException(string message, Exception? inner = null) : Exception(message, inner);
