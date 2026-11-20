namespace PgBolt.Dashboard.Endpoints;

/// <summary>Невалидный курсор или параметр запроса → 400.</summary>
internal sealed class DashboardBadRequestException(string message) : Exception(message);

/// <summary>Несуществующая задача, batch или расписание → 404.</summary>
internal sealed class DashboardNotFoundException(string message) : Exception(message);
