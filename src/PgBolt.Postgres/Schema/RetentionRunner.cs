using System.Data;
using System.Globalization;
using Npgsql;

namespace PgBolt.Postgres.Schema;

/// <summary>
/// Runs retention on jobs and job_events (Part 2 §5.2–§5.5). Clears earlier remnants, detaches groups under LOCK TABLE ONLY on the parent with
/// hold timeouts (transaction 1), and compacts pinned jobs partitions (RetentionRunner.Jobs.cs). Drops tables in separate transactions only if
/// each OID still matches (transaction 2).
/// </summary>
internal sealed partial class RetentionRunner
{
    internal const int MinCompactionGain = 100;
    private const string IdleInTransactionTimeout = "25P03";

    private readonly NpgsqlDataSource _dataSource;
    private readonly PgBoltOptions _options;
    private readonly RetentionTuning _tuning;
    private readonly string _schema;

    public RetentionRunner(NpgsqlDataSource dataSource, PgBoltOptions options, RetentionTuning tuning)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _tuning = tuning ?? throw new ArgumentNullException(nameof(tuning));
        _schema = SchemaName.Validate(options.Schema);
    }

    private int MaxLive => _options.RetentionCompactionMaxLiveJobs;

    private string LockTimeoutSql => $"SET LOCAL lock_timeout = '{Ms(_options.RetentionLockTimeout)}ms'";

    /// <summary>Processes remnants, jobs, then job_events, and counts attached jobs partitions (§5.5).</summary>
    public async Task<RetentionResult> ApplyAsync(CancellationToken ct = default)
    {
        var listing = await PartitionCatalog.ListAsync(_dataSource, _schema, ct);
        await HookAsync(RetentionStep.Listed, "", [], ct);
        var jobsHorizon = PartitionCatalog.Horizon(listing.Today, _options.JobsRetention);
        var eventsHorizon = PartitionCatalog.Horizon(listing.Today, _options.EventsRetention);
        var acc = new Accumulator();

        // §5.2: остатки — при любом значении retention, без проверок задач; единственная проверка — в транзакции 2.
        foreach (var t in listing.Tables
                     .Where(t => t.Kind is CatalogKind.DetachedJobs or CatalogKind.DetachedEvents or CatalogKind.CompactionRemnant)
                     .OrderBy(t => t.Day))
        {
            var horizon = t.IsJobs ? jobsHorizon : eventsHorizon;
            var overdue = horizon is { } h ? Math.Max(0, PartitionCatalog.Overdue(h, t.Day)) : 0;
            acc.RecordDrop(t, overdue, await DropAsync(t.Name, t.Oid, ct), countRemnant: true);
        }

        if (jobsHorizon is { } jh)
            await ApplyJobsAsync(Candidates(listing.Tables, CatalogKind.JobsPartition, jh), acc, ct);
        if (eventsHorizon is { } eh)
            await ApplyEventsAsync(Candidates(listing.Tables, CatalogKind.EventsPartition, eh), acc, ct);

        // Часть 5 §7.1: строки batches — по JobsRetention, сверху вниз; при null не удаляются.
        var deletedBatches = _options.JobsRetention is { } retention ? await DeleteBatchesAsync(retention, ct) : 0;
        return acc.ToResult(await CountAttachedJobsAsync(ct), deletedBatches);
    }

    // ---- job_events (§5.4) ----------------------------------------------------------------------

    private async Task ApplyEventsAsync(List<Candidate> candidates, Accumulator acc, CancellationToken ct)
    {
        foreach (var chunk in candidates.Chunk(_tuning.PartitionsPerTransaction))
        {
            var group = await ExcludeHeldAsync("job_events", chunk, acc, ct);
            if (group.Count == 0)
                continue;
            var result = await DetachGroupAsync("job_events", group, ct);
            if (result.Outcome is LockedOutcome.LockTimeout or LockedOutcome.HoldTimeout)
            {
                acc.DeferGroup("job_events", group, result.Outcome);
                return;   // §5.4: остальные кандидаты job_events в этом запуске не обрабатываются
            }
            await DropDetachedAsync(result, acc, ct);
        }
    }

    // ---- общие шаги §5.3 п. 3, 5, 6 -------------------------------------------------------------

    private static List<Candidate> Candidates(IEnumerable<CatalogTable> tables, CatalogKind kind, DateOnly horizon) =>
        [.. tables.Where(t => t.Kind == kind && t.Day <= horizon)
                  .OrderBy(t => t.Day)
                  .Select(t => new Candidate(t, PartitionCatalog.Overdue(horizon, t.Day)))];

    /// <summary>Excludes partitions locked independently of the parent from the group and reports Deferred(LockTimeout) (§5.3, step 3).</summary>
    private async Task<List<Candidate>> ExcludeHeldAsync(string parent, IReadOnlyList<Candidate> chunk, Accumulator acc, CancellationToken ct)
    {
        var held = await HeldAsync(parent, [.. chunk.Select(c => c.Table.Oid)], ct);
        var group = new List<Candidate>(chunk.Count);
        foreach (var c in chunk)
        {
            if (held.Contains(c.Table.Oid))
                acc.Deferred.Add(new DeferredPartition(parent, c.Day, DeferReason.LockTimeout, c.Overdue, null));
            else
                group.Add(c);
        }
        if (group.Count > 0)
            await HookAsync(RetentionStep.LocksChecked, parent, [.. group.Select(c => c.Day)], ct);
        return group;
    }

    /// <remarks>
    /// Matches a holder by pid or virtualtransaction. A prepared transaction (2PC) has a NULL pid, hence IS DISTINCT FROM when excluding this
    /// session, but has a unique virtualtransaction. Matching only virtualtransaction would miss a session that changed transactions between reads:
    /// pg_locks collects fast-path locks before the main lock table, so the parent and partition can show different vxids for the same pid.
    /// </remarks>
    private async Task<HashSet<long>> HeldAsync(string parent, long[] oids, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand($"""
            WITH l AS MATERIALIZED (
                SELECT pid, virtualtransaction, relation FROM pg_locks
                WHERE locktype = 'relation' AND granted AND pid IS DISTINCT FROM pg_backend_pid()
                  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
            )
            SELECT DISTINCT l.relation::bigint FROM l
            WHERE l.relation = ANY($1::bigint[]::oid[])
              AND NOT EXISTS (SELECT 1 FROM l p
                              WHERE (p.pid = l.pid OR p.virtualtransaction = l.virtualtransaction)
                                AND p.relation = '{_schema}.{parent}'::regclass)
            """);
        cmd.Parameters.Add(Arg(oids));
        var held = new HashSet<long>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            held.Add(reader.GetInt64(0));
        return held;
    }

    /// <summary>
    /// Transaction 1 detaches a group (§5.3, step 5 and §5.4). For jobs, has_live rechecks live rows under lock. Such partitions are omitted from
    /// the result and left for the next run. Detached tables proceed to transaction 2.
    /// </summary>
    private async Task<GroupResult> DetachGroupAsync(string parent, IReadOnlyList<Candidate> group, CancellationToken ct)
    {
        var jobs = parent == "jobs";
        var check = jobs
            ? Cmd($"""
                SELECT p.oid::bigint,
                       to_regclass(p.rel) = p.oid
                         AND EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p.oid AND i.inhparent = '{_schema}.jobs'::regclass),
                       EXISTS (SELECT 1 FROM {_schema}.queue q WHERE q.job_id >= p.lo AND q.job_id < p.hi)
                       OR EXISTS (SELECT 1 FROM {_schema}.queue q
                                  WHERE q.await_condition IN (1, 2) AND q.parent_id >= p.lo AND q.parent_id < p.hi)
                       OR EXISTS (SELECT 1 FROM {_schema}.batches b
                                  WHERE b.state = 1 AND b.condition IN (1, 2) AND b.parent_id >= p.lo AND b.parent_id < p.hi)
                FROM unnest($1::bigint[]::oid[], $2::text[], $3::uuid[], $4::uuid[]) AS p(oid, rel, lo, hi)
                """,
                Arg(group.Select(c => c.Table.Oid).ToArray()), Arg(group.Select(c => Qualified(c.Table.Name)).ToArray()),
                Arg(group.Select(c => c.Lo).ToArray()), Arg(group.Select(c => c.Hi).ToArray()))
            : Cmd($"""
                SELECT p.oid::bigint,
                       to_regclass(p.rel) = p.oid
                         AND EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p.oid AND i.inhparent = '{_schema}.job_events'::regclass),
                       false
                FROM unnest($1::bigint[]::oid[], $2::text[]) AS p(oid, rel)
                """,
                Arg(group.Select(c => c.Table.Oid).ToArray()), Arg(group.Select(c => Qualified(c.Table.Name)).ToArray()));

        var byOid = group.ToDictionary(c => c.Table.Oid);
        var detach = new List<Candidate>();
        var notAttached = new List<Candidate>();
        var days = group.Select(c => c.Day).ToArray();
        var outcome = await RunLockedAsync(parent, check, days, async (reader, t) =>
        {
            while (await reader.ReadAsync(t))
            {
                var candidate = byOid[reader.GetInt64(0)];
                var attached = !reader.IsDBNull(1) && reader.GetBoolean(1);
                if (!attached)
                    notAttached.Add(candidate);
                else if (!reader.GetBoolean(2))
                    detach.Add(candidate);
            }
        }, () => [.. detach.Select(c => new NpgsqlBatchCommand($"ALTER TABLE {_schema}.{parent} DETACH PARTITION {Qualified(c.Table.Name)}"))], ct);

        if (outcome == LockedOutcome.Committed)
            await HookAsync(RetentionStep.Committed, parent, days, ct);
        return outcome switch
        {
            LockedOutcome.Committed => new GroupResult(outcome, detach, notAttached),
            LockedOutcome.RolledBack => new GroupResult(outcome, [], notAttached),
            _ => new GroupResult(outcome, [], []),
        };
    }

    private async Task DropDetachedAsync(GroupResult result, Accumulator acc, CancellationToken ct)
    {
        foreach (var c in result.Detached.Concat(result.NotAttached))
            acc.RecordDrop(Detached(c.Table), c.Overdue, await DropAsync(c.Table.Name, c.Table.Oid, ct), countRemnant: true);
    }

    private static CatalogTable Detached(CatalogTable t) =>
        t with { Kind = t.IsJobs ? CatalogKind.DetachedJobs : CatalogKind.DetachedEvents };

    /// <summary>
    /// Transaction 1 (§5.3, step 5): BeginTransactionAsync, batch A (lock_timeout, LOCK TABLE ONLY on the parent, hold timeouts, verification), then
    /// batch B (DDL) and CommitAsync or a rollback. An empty batch B triggers RollbackAsync immediately after batch A.
    /// </summary>
    private async Task<LockedOutcome> RunLockedAsync(string parent, NpgsqlBatchCommand check, IReadOnlyList<DateOnly> days,
        Func<NpgsqlDataReader, CancellationToken, Task> readCheck, Func<List<NpgsqlBatchCommand>> batchB, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            await using (var a = new NpgsqlBatch(conn, tx))
            {
                a.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                a.BatchCommands.Add(new NpgsqlBatchCommand($"LOCK TABLE ONLY {_schema}.{parent} IN ACCESS EXCLUSIVE MODE"));
                a.BatchCommands.Add(new NpgsqlBatchCommand(
                    $"SET LOCAL statement_timeout = '{Ms(_tuning.HoldStatementTimeout + _options.RetentionLockTimeout)}ms'"));
                a.BatchCommands.Add(new NpgsqlBatchCommand($"SET LOCAL idle_in_transaction_session_timeout = '{Ms(_tuning.HoldIdleTimeout)}ms'"));
                a.BatchCommands.Add(check);
                await using var reader = await ExecuteToRowsAsync(a, ct);
                await readCheck(reader, ct);
            }
            await HookAsync(RetentionStep.BatchA, parent, days, ct);

            var commands = batchB();
            if (commands.Count == 0)
            {
                await tx.RollbackAsync(ct);
                return LockedOutcome.RolledBack;
            }
            await using (var b = new NpgsqlBatch(conn, tx))
            {
                if (_tuning.BatchBTestPrefix is { } prefix)
                    b.BatchCommands.Add(new NpgsqlBatchCommand(prefix));
                foreach (var command in commands)
                    b.BatchCommands.Add(command);
                await b.ExecuteNonQueryAsync(ct);
            }
            await HookAsync(RetentionStep.BatchB, parent, days, ct);
            await tx.CommitAsync(ct);
            return LockedOutcome.Committed;
        }
        catch (Exception e) when (IsCancellation(e, ct))
        {
            await SafeRollbackAsync(conn, tx);
            if (e is NpgsqlException)
                throw new OperationCanceledException(ct);
            throw;
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await SafeRollbackAsync(conn, tx);
            return LockedOutcome.LockTimeout;
        }
        catch (NpgsqlException e) when (IsHoldTimeout(e, conn))
        {
            await SafeRollbackAsync(conn, tx);
            return LockedOutcome.HoldTimeout;
        }
        catch
        {
            await SafeRollbackAsync(conn, tx);
            throw;
        }
    }

    /// <summary>
    /// Transaction 2 (§5.3, step 6) drops only an unattached table whose name still identifies the same OID. The parent remains unlocked.
    /// </summary>
    /// <remarks>
    /// Overlapping runs (§5.5) can both pass the unlocked OID check. The second DROP waits, then PostgreSQL resolves the name again after the first
    /// commit and returns 42P01 for DROP TABLE without IF EXISTS. Classify this as Skipped to avoid counting the same day in both pods'
    /// DroppedJobsPartitions or DroppedEventsPartitions. Use only SQLSTATE, independent of NOTICE, lc_messages, client_min_messages, and event
    /// triggers.
    /// </remarks>
    private async Task<DropResult> DropAsync(string name, long oid, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            bool droppable;
            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                batch.BatchCommands.Add(Cmd("""
                    SELECT to_regclass($1) = $2::oid AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = $2::oid)
                    """, Arg(Qualified(name)), Arg(oid)));
                await using var reader = await ExecuteToRowsAsync(batch, ct);
                droppable = await ReadTrueAsync(reader, ct);
            }
            if (!droppable)
            {
                await tx.RollbackAsync(ct);
                return new DropResult(DropStatus.Skipped, null);
            }
            await using (var drop = new NpgsqlCommand($"DROP TABLE {Qualified(name)}", conn, tx))
                await drop.ExecuteNonQueryAsync(ct);
            await tx.CommitAsync(ct);
            return new DropResult(DropStatus.Dropped, null);
        }
        catch (Exception e) when (IsCancellation(e, ct))
        {
            await SafeRollbackAsync(conn, tx);
            if (e is NpgsqlException)
                throw new OperationCanceledException(ct);
            throw;
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            await SafeRollbackAsync(conn, tx);
            return new DropResult(DropStatus.LockTimeout, null);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            // Таблицу удалил другой под, пока этот DROP ждал блокировку (§5.5): пропуск, в результат не попадает.
            await SafeRollbackAsync(conn, tx);
            return new DropResult(DropStatus.Skipped, null);
        }
        catch (NpgsqlException e)
        {
            await SafeRollbackAsync(conn, tx);
            return new DropResult(DropStatus.Failed, e.Message);
        }
    }

    private async Task<int> CountAttachedJobsAsync(CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand($"SELECT count(*)::int FROM pg_inherits WHERE inhparent = '{_schema}.jobs'::regclass");
        return (int)(await cmd.ExecuteScalarAsync(ct))!;
    }

    // ---- помощники ------------------------------------------------------------------------------

    private Task HookAsync(RetentionStep step, string parent, IReadOnlyList<DateOnly> days, CancellationToken ct) =>
        _tuning.Hook is { } hook ? hook(new RetentionHookContext(step, parent, days), ct) : Task.CompletedTask;

    private string Qualified(string table) => $"{_schema}.{table}";

    private static int Ms(TimeSpan t) => (int)Math.Ceiling(t.TotalMilliseconds);

    private static NpgsqlBatchCommand Cmd(string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlBatchCommand(sql);
        foreach (var p in parameters)
            command.Parameters.Add(p);
        return command;
    }

    /// <summary>Checks that the partition name still identifies the listed OID. NULL means the table is gone.</summary>
    private NpgsqlBatchCommand OidCheckCmd(CatalogTable table) =>
        Cmd("SELECT to_regclass($1) = $2::oid", Arg(Qualified(table.Name)), Arg(table.Oid));

    /// <summary>True if the first row of the result set contains true. A missing row or NULL returns false.</summary>
    private static async Task<bool> ReadTrueAsync(NpgsqlDataReader reader, CancellationToken ct) =>
        await reader.ReadAsync(ct) && !reader.IsDBNull(0) && reader.GetBoolean(0);

    /// <summary>Creates a positional parameter ($N in insertion order).</summary>
    private static NpgsqlParameter Arg<T>(T value) => new NpgsqlParameter<T> { TypedValue = value };

    /// <summary>Positions the reader on the first result set. SET and LOCK commands in the batch return no rows.</summary>
    private static async Task<NpgsqlDataReader> ExecuteToRowsAsync(NpgsqlBatch batch, CancellationToken ct)
    {
        var reader = await batch.ExecuteReaderAsync(ct);
        while (reader.FieldCount == 0 && await reader.NextResultAsync(ct))
        {
        }
        return reader;
    }

    /// <summary>
    /// Rolls back only on an open connection, without a cancellation token (§5.3, step 5). The server rolls back on disconnect, and Npgsql rolls
    /// back unfinished transactions on disposal. A rollback error must preserve the original failure.
    /// </summary>
    private static async Task SafeRollbackAsync(NpgsqlConnection conn, NpgsqlTransaction tx)
    {
        if (conn.FullState != ConnectionState.Open)
            return;
        try
        {
            await tx.RollbackAsync(CancellationToken.None);
        }
        catch (Exception e) when (e is NpgsqlException or InvalidOperationException)
        {
            // Соединение разорвано во время отката: транзакцию откатит сервер.
        }
    }

    /// <summary>
    /// Handles cancellation first in each catch chain (§5.3, Error Handling). Rolls back without a token, rethrows OperationCanceledException, and
    /// converts an interrupted command's NpgsqlException into OperationCanceledException(ct).
    /// </summary>
    private static bool IsCancellation(Exception e, CancellationToken ct) =>
        ct.IsCancellationRequested && e is OperationCanceledException or NpgsqlException;

    /// <summary>
    /// Recognizes statement_timeout (57014), idle_in_transaction_session_timeout (25P03), or any broken connection (§5.3, Error Handling). Checks
    /// connection state because PgBouncer may return its own PostgresException (e.g. 08P01) or disconnect without SQLSTATE.
    /// </summary>
    private static bool IsHoldTimeout(NpgsqlException e, NpgsqlConnection conn) =>
        e is PostgresException { SqlState: PostgresErrorCodes.QueryCanceled or IdleInTransactionTimeout }
        || (conn.FullState & ConnectionState.Broken) != 0 || conn.FullState == ConnectionState.Closed;

    private enum LockedOutcome { Committed, RolledBack, LockTimeout, HoldTimeout }

    private enum DropStatus { Dropped, Skipped, LockTimeout, Failed }

    private readonly record struct DropResult(DropStatus Status, string? Error);

    private sealed record GroupResult(LockedOutcome Outcome, List<Candidate> Detached, List<Candidate> NotAttached);

    private sealed record Candidate(CatalogTable Table, int Overdue)
    {
        public DateOnly Day => Table.Day;
        public Guid Lo => UuidV7.LowerBound(Day);
        public Guid Hi => UuidV7.LowerBound(Day.AddDays(1));
    }

    private sealed class Accumulator
    {
        public List<DateOnly> DroppedJobs { get; } = [];
        public List<DateOnly> DroppedEvents { get; } = [];
        public int RemnantsDropped { get; private set; }
        public List<PinnedPartition> Pinned { get; } = [];
        public List<DeferredPartition> Deferred { get; } = [];

        public void DeferGroup(string table, IEnumerable<Candidate> group, LockedOutcome outcome)
        {
            var reason = outcome == LockedOutcome.LockTimeout ? DeferReason.LockTimeout : DeferReason.HoldTimeout;
            foreach (var c in group)
                Deferred.Add(new DeferredPartition(table, c.Day, reason, c.Overdue, null));
        }

        /// <param name="table">Table considered for removal.</param>
        /// <param name="overdue">Days since the table became eligible for retention.</param>
        /// <param name="drop">Result of attempting to drop the table.</param>
        /// <param name="countRemnant">False for a remnant of this run's compaction, which is excluded from DroppedCompactionRemnants (§5.1).</param>
        public void RecordDrop(CatalogTable table, int overdue, DropResult drop, bool countRemnant)
        {
            switch (drop.Status)
            {
                case DropStatus.Dropped when table.Kind == CatalogKind.CompactionRemnant:
                    if (countRemnant)
                        RemnantsDropped++;
                    break;
                case DropStatus.Dropped:
                    (table.IsJobs ? DroppedJobs : DroppedEvents).Add(table.Day);
                    break;
                case DropStatus.LockTimeout:
                    Deferred.Add(new DeferredPartition(table.Table, table.Day, DeferReason.DropLockTimeout, overdue, null));
                    break;
                case DropStatus.Failed:
                    Deferred.Add(new DeferredPartition(table.Table, table.Day, DeferReason.DropFailed, overdue, null, drop.Error));
                    break;
            }
        }

        public RetentionResult ToResult(int attachedJobs, int deletedBatches) =>
            new([.. DroppedJobs], [.. DroppedEvents], RemnantsDropped, [.. Pinned], [.. Deferred], attachedJobs, deletedBatches);
    }
}
