namespace PgBolt;

/// <summary>
/// Задача сообщает, что повтор бессмысленен: исход — окончательный Failed без retry (часть 3 §3.5, §5.2).
/// Не sealed: приложение может вводить собственные подтипы.
/// </summary>
public class JobAbortedException(string message, Exception? inner = null) : Exception(message, inner);
