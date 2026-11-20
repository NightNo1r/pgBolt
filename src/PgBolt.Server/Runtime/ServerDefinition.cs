namespace PgBolt.Server.Runtime;

/// <summary>Описание виртуального сервера в DI: одно на вызов AddPgBoltServer (§3.1).</summary>
internal sealed record ServerDefinition(string Name, PgBoltServerOptions Options);

/// <summary>Глобальный фильтр процесса (AddPgBoltJobFilter), в порядке регистрации (§3.6).</summary>
internal sealed record GlobalJobFilter(Type FilterType);
