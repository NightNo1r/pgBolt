namespace PgBolt;

/// <summary>
/// The invoker could not deserialize the job arguments before calling the executor. Causes include malformed JSON, an unsupported type, or null
/// instead of an object. The job ends in Failed (Part 3 §5.1, §5.2). Filters can inspect this exception.
/// </summary>
public sealed class JobArgumentsException(string message, Exception? inner = null) : Exception(message, inner);
