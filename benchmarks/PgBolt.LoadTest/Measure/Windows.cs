namespace PgBolt.LoadTest.Measure;

/// <summary>
/// Итоги окна 10 с. <see cref="Start"/> — от начала прогона; <see cref="Enqueued"/> — задачи пачек, вызов
/// <c>EnqueueManyAsync</c> которых успешно завершился в этом окне; <see cref="CpuFraction"/> — загрузка CPU
/// процесса стенда в долях всех доступных ядер.
/// </summary>
internal sealed record WindowStats(
    int Index, TimeSpan Start, long Completed, long Enqueued, long EnqueueCalls, long EnqueueErrors,
    double EnqueueP99Ms, double CpuFraction, TimeSpan MaxGeneratorWait, long DroppedTokens,
    LatencySummary Immediate, LatencySummary Delayed);

/// <summary>
/// Посекундный срез состояния БД и PgBouncer. <c>null</c> — пропуск отсчёта: запрос не выполнился
/// или источник недоступен.
/// </summary>
/// <param name="ReadyBacklog">Готовые не взятые задачи <c>queue</c>, ограниченный счёт: rate + 1 — «больше порога».</param>
/// <param name="QueueTotalBytes"><c>pg_total_relation_size</c> таблицы <c>queue</c>.</param>
/// <param name="MaxWaitMicros">Максимальный <c>maxwait</c> пулов PgBouncer.</param>
/// <param name="ClWaiting"><c>cl_waiting</c> пулов PgBouncer.</param>
/// <param name="Connections">Соединения хостов и продюсера в <c>pg_stat_activity</c>.</param>
/// <param name="Completed">Выполнено задач с начала прогона.</param>
/// <param name="Delayed">Отложенные задачи <c>queue</c>; считается раз в 10 с, в остальных срезах <c>null</c>.</param>
/// <param name="Leased">Задачи <c>queue</c> в lease; считается вместе с <see cref="Delayed"/>.</param>
/// <param name="QueueIndexBytes"><c>pg_relation_size</c> каждого индекса <c>queue</c> по имени.</param>
/// <param name="LockManagerWaits">Backend'ы PgBolt, ждущие LWLock <c>LockManager</c>.</param>
/// <param name="Vacuums">Идущие vacuum по отношениям схемы прогона.</param>
/// <param name="QueryTime">Время ежесекундных запросов среза.</param>
/// <param name="CountsTime">Время 10-секундного счёта по <c>queue</c>.</param>
/// <param name="DatabaseBytes"><c>pg_database_size</c> БД прогона.</param>
internal sealed record Sample(
    TimeSpan At, long? ReadyBacklog, long? QueueTotalBytes, long? MaxWaitMicros, int? ClWaiting, int? Connections,
    long? Completed = null, long? Delayed = null, long? Leased = null,
    TableSample? Queue = null, TableSample? Completions = null,
    IReadOnlyDictionary<string, long>? QueueIndexBytes = null, long? WalBytes = null,
    CheckpointerSample? Checkpointer = null, int? LockManagerWaits = null, int? SvActive = null, int? SvIdle = null,
    int? AutovacuumWorkers = null, IReadOnlyList<VacuumProgress>? Vacuums = null,
    TimeSpan? QueryTime = null, TimeSpan? CountsTime = null, long? DatabaseBytes = null);

/// <summary>Строка <c>pg_stat_user_tables</c>.</summary>
internal sealed record TableSample(long LiveTuples, long DeadTuples, long AutovacuumCount, DateTime? LastAutovacuum);

/// <summary><c>pg_stat_checkpointer</c>; времена — в миллисекундах.</summary>
internal sealed record CheckpointerSample(
    long NumTimed, long NumRequested, double WriteTimeMs, double SyncTimeMs, long BuffersWritten);

/// <summary>Строка <c>pg_stat_progress_vacuum</c>.</summary>
internal sealed record VacuumProgress(string Relation, string Phase);
