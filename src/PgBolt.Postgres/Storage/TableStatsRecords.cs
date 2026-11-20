namespace PgBolt.Postgres.Storage;

/// <summary>Статистика таблицы схемы: число мёртвых строк по pg_stat_user_tables и полный размер с индексами и TOAST.</summary>
internal sealed record TableStatsRow(string Table, long DeadTuples, long SizeBytes);
