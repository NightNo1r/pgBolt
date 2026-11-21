namespace PgBolt.Server.Runtime;

/// <summary>Virtual server definition registered in DI, one per AddPgBoltServer call (§3.1).</summary>
internal sealed record ServerDefinition(string Name, PgBoltServerOptions Options);

/// <summary>Process-wide job filter (AddPgBoltJobFilter), in registration order (§3.6).</summary>
internal sealed record GlobalJobFilter(Type FilterType);
