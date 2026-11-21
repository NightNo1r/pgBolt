namespace PgBolt.Postgres.Storage;

/// <summary>Schema table statistics: dead tuples from pg_stat_user_tables and total size including indexes and TOAST.</summary>
internal sealed record TableStatsRow(string Table, long DeadTuples, long SizeBytes);
