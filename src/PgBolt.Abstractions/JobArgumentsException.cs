namespace PgBolt;

/// <summary>
/// Аргументы задачи не удалось десериализовать (битый JSON, неподдерживаемый тип, null вместо объекта).
/// Бросается invoker'ом до вызова исполнителя; исход — окончательный Failed (часть 3 §5.1, §5.2). Публичен для фильтров.
/// </summary>
public sealed class JobArgumentsException(string message, Exception? inner = null) : Exception(message, inner);
