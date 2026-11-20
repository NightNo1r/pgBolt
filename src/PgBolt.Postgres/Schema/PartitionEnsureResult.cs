namespace PgBolt.Postgres.Schema;

/// <param name="Created">Сколько партиций создано (jobs + job_events).</param>
/// <param name="TimedOut">Не удалось взять блокировку за lock_timeout — повторить позже (§4.4).</param>
/// <param name="TodayPresent">Партиции jobs и job_events на текущие сутки UTC сервера существуют.</param>
public sealed record PartitionEnsureResult(int Created, bool TimedOut, bool TodayPresent);
