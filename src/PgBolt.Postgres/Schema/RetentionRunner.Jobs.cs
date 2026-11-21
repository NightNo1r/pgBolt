using System.Globalization;
using Npgsql;

namespace PgBolt.Postgres.Schema;

internal sealed partial class RetentionRunner
{
    /// <summary>
    /// Prechecks all attached candidates, checks publications, detaches groups, then compacts partitions individually (§5.3). LockTimeout or
    /// HoldTimeout in transaction 1 stops the jobs stage (step 2).
    /// </summary>
    private async Task ApplyJobsAsync(List<Candidate> candidates, Accumulator acc, CancellationToken ct)
    {
        var detach = new List<Candidate>();
        var compact = new List<Compaction>();
        foreach (var c in candidates)
        {
            var p = await PrecheckAsync(c, ct);
            switch (p.Kind)
            {
                case PrecheckKind.Detach:
                    detach.Add(c);
                    break;
                case PrecheckKind.Compact:
                    compact.Add(new Compaction(c, p.Live));
                    break;
                case PrecheckKind.Pinned:
                    acc.Pinned.Add(new PinnedPartition(c.Day, c.Overdue, p.Live.Length, Compacted: false));
                    break;
                case PrecheckKind.TooManyLive:
                    acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LiveJobs, c.Overdue, MaxLive + 1));
                    break;
                case PrecheckKind.LockTimeout:
                    acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LockTimeout, c.Overdue, null));
                    break;
                case PrecheckKind.Gone:
                    break;
            }
        }
        await HookAsync(RetentionStep.Prechecked, "jobs", [.. candidates.Select(c => c.Day)], ct);

        if (compact.Count > 0 && await PublishedAsync(ct))
        {
            foreach (var x in compact)
                acc.Deferred.Add(new DeferredPartition("jobs", x.Candidate.Day, DeferReason.Replicated, x.Candidate.Overdue, x.Live.Length));
            compact.Clear();
        }

        foreach (var chunk in detach.Chunk(_tuning.PartitionsPerTransaction))
        {
            var group = await ExcludeHeldAsync("jobs", chunk, acc, ct);
            if (group.Count == 0)
                continue;
            var result = await DetachGroupAsync("jobs", group, ct);
            if (result.Outcome is LockedOutcome.LockTimeout or LockedOutcome.HoldTimeout)
            {
                acc.DeferGroup("jobs", group, result.Outcome);
                return;   // компактирования в этом запуске тоже не выполняются
            }
            await DropDetachedAsync(result, acc, ct);
        }

        foreach (var x in compact)
            if (!await CompactAsync(x.Candidate, x.Live, acc, ct))
                return;
    }

    private enum PrecheckKind { Detach, Compact, Pinned, TooManyLive, LockTimeout, Gone }

    private enum WarmUpResult { Done, LockTimeout, Gone }

    private sealed record Precheck(PrecheckKind Kind, Guid[] Live);

    private sealed record Compaction(Candidate Candidate, Guid[] Live);

    /// <summary>
    /// Uses a short transaction per partition with lock_timeout (§5.3, step 1). Finds live rows from source (a) in Part 5 §7.1, then checks the OID
    /// and reads the queue primary key and source (b) in one statement with another OID check. A sequential scan stops at row L + max(L, 100) to
    /// assess compaction. If the name no longer identifies the listed OID, returns Gone without reading the partition. Rechecks after counting
    /// because a lookup by name can wait on the old OID and read its replacement. A mismatch returns Gone. SQLSTATE 55P03 means LockTimeout. A 42P01
    /// for the partition means Gone, but a 42P01 for queue propagates.
    /// </summary>
    private async Task<Precheck> PrecheckAsync(Candidate c, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            var live = new HashSet<Guid>();
            bool same;
            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                batch.BatchCommands.Add(OidCheckCmd(c.Table));
                batch.BatchCommands.Add(Cmd(AwaitingParentsSql(batches: false, "$1", "$2", "$3"), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1)));
                batch.BatchCommands.Add(Cmd(AwaitingParentsSql(batches: true, "$1", "$2", "$3"), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1)));
                await using var reader = await ExecuteToRowsAsync(batch, ct);
                same = await ReadTrueAsync(reader, ct);
                for (var source = 0; source < 2; source++)       // (а): queue и batches
                {
                    await reader.NextResultAsync(ct);
                    while (await reader.ReadAsync(ct))
                        live.Add(reader.GetGuid(0));
                }
            }
            if (!same)
            {
                await tx.RollbackAsync(ct);
                return new Precheck(PrecheckKind.Gone, []);
            }
            await HookAsync(RetentionStep.PrecheckOidChecked, "jobs", [c.Day], ct);

            // Живые queue и часть 5 §7.1 (б) — одним statement'ом, вторым пакетом: (б) читает партицию кандидата по имени
            // (не родителя jobs: его держит DETACH другого пода, и предпроверка ждала бы родителя), поэтому только после
            // сверки oid. Общий снимок: requeue (вставка queue и jobs.state = 0) и уход из queue в Failed / Cancelled — каждый
            // одним коммитом, поэтому член существующего batch в снимке либо строка queue, либо в (б); член, ставший живым
            // по (б) после этого снимка, был в нём строкой queue (§7.1). Чтение по имени могло дождаться блокировки старого
            // oid и прочитать новую таблицу — сверка повторяется после него, как у count.
            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                batch.BatchCommands.Add(Cmd(QueueAndBatchMembersSql(c.Table.Name), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1)));
                batch.BatchCommands.Add(OidCheckCmd(c.Table));
                try
                {
                    await using var reader = await ExecuteToRowsAsync(batch, ct);
                    while (await reader.ReadAsync(ct))
                        live.Add(reader.GetGuid(0));
                    await reader.NextResultAsync(ct);
                    same = await ReadTrueAsync(reader, ct);
                }
                catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable && !ct.IsCancellationRequested)
                {
                    // 42P01 даёт только чтение партиции (to_regclass возвращает NULL): её удалил другой под, пока оно ждало.
                    await SafeRollbackAsync(conn, tx);
                    return new Precheck(PrecheckKind.Gone, []);
                }
            }
            if (!same)
            {
                await tx.RollbackAsync(ct);
                return new Precheck(PrecheckKind.Gone, []);
            }

            var kind = PrecheckKind.Detach;
            if (live.Count > MaxLive)
            {
                kind = PrecheckKind.TooManyLive;
            }
            else if (live.Count > 0)
            {
                var need = (long)live.Count + Math.Max(live.Count, MinCompactionGain);
                long rows;
                await using (var batch = new NpgsqlBatch(conn, tx))
                {
                    // Повторная сверка — после count: пока count ждал блокировку старого oid, другой под мог сжать партицию,
                    // и PostgreSQL после получения блокировки заново разрешил имя. Прочитанная таблица уже под ACCESS SHARE
                    // до конца транзакции (её не переименовать и не удалить), поэтому имя, указывающее на oid из списка,
                    // означает, что прочитана именно она.
                    batch.BatchCommands.Add(Cmd($"SELECT count(*) FROM (SELECT 1 FROM {Qualified(c.Table.Name)} LIMIT $1) x", Arg(need)));
                    batch.BatchCommands.Add(OidCheckCmd(c.Table));
                    try
                    {
                        await using var reader = await ExecuteToRowsAsync(batch, ct);
                        await reader.ReadAsync(ct);
                        rows = reader.GetInt64(0);
                        await reader.NextResultAsync(ct);
                        same = await ReadTrueAsync(reader, ct);
                    }
                    catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable && !ct.IsCancellationRequested)
                    {
                        // 42P01 даёт только count (to_regclass возвращает NULL): партицию удалил другой под, пока count ждал.
                        await SafeRollbackAsync(conn, tx);
                        return new Precheck(PrecheckKind.Gone, []);
                    }
                }
                if (!same)
                {
                    await tx.RollbackAsync(ct);
                    return new Precheck(PrecheckKind.Gone, []);
                }
                kind = rows == need ? PrecheckKind.Compact : PrecheckKind.Pinned;
            }
            await tx.CommitAsync(ct);
            return new Precheck(kind, [.. live]);
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
            return new Precheck(PrecheckKind.LockTimeout, []);
        }
    }

    /// <summary>Detects any publication covering jobs or its partitions, including FOR ALL TABLES (§5.3, Publications).</summary>
    private async Task<bool> PublishedAsync(CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT EXISTS (SELECT 1 FROM pg_publication WHERE puballtables)
                OR EXISTS (SELECT 1 FROM pg_publication_tables WHERE schemaname = $1 AND tablename ~ '^jobs(_[0-9]{8})?$')
            """);
        cmd.Parameters.Add(Arg(_schema));
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// Reading the row as text accesses every column and detoasts args, forcing a heap read instead of an index-only scan (§5.3, step 4).
    /// </summary>
    internal string WarmUpSql(string partition) =>
        $"SELECT sum(octet_length(j::text)) FROM {Qualified(partition)} j WHERE j.id = ANY($1)";

    private async Task<WarmUpResult> WarmUpAsync(Candidate c, Guid[] ids, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            await using (var batch = new NpgsqlBatch(conn, tx))
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand(LockTimeoutSql));
                batch.BatchCommands.Add(Cmd(WarmUpSql(c.Table.Name), Arg(ids)));
                await batch.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
            return WarmUpResult.Done;
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
            return WarmUpResult.LockTimeout;
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            await SafeRollbackAsync(conn, tx);
            return WarmUpResult.Gone;
        }
    }

    /// <summary>
    /// Compacts one partition (§5.3, steps 3–6). Check pg_locks and warm the data. Transaction 1 rechecks attachment by OID and live rows under
    /// lock, detaches and renames the old partition, creates a replacement with LIKE, copies live @ids, and attaches it. Transaction 2 drops the
    /// remnant. Under lock, recheck live queue rows and source (a) but omit (b) (Part 5 §7.1). Union @ids with the precheck set, so this compaction
    /// transaction never performs a detach-only operation.
    /// </summary>
    /// <returns>False if LockTimeout or HoldTimeout in transaction 1 stops the jobs stage.</returns>
    private async Task<bool> CompactAsync(Candidate c, Guid[] live, Accumulator acc, CancellationToken ct)
    {
        if ((await ExcludeHeldAsync("jobs", [c], acc, ct)).Count == 0)
            return true;
        switch (await WarmUpAsync(c, live, ct))
        {
            case WarmUpResult.LockTimeout:
                acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LockTimeout, c.Overdue, null));
                return true;
            case WarmUpResult.Gone:
                return true;
        }
        var days = new[] { c.Day };
        await HookAsync(RetentionStep.WarmedUp, "jobs", days, ct);

        var name = c.Table.Name;
        var remnant = $"{name}_r{c.Table.Oid.ToString(CultureInfo.InvariantCulture)}";
        var attached = false;
        var recheck = new HashSet<Guid>();
        var check = Cmd($"""
            SELECT to_regclass($1) = $2::oid
                     AND EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = $2::oid AND inhparent = '{_schema}.jobs'::regclass),
                   ARRAY(SELECT job_id FROM {_schema}.queue WHERE job_id >= $3 AND job_id < $4 LIMIT $5),
                   ARRAY({AwaitingParentsSql(batches: false, "$3", "$4", "$5")}),
                   ARRAY({AwaitingParentsSql(batches: true, "$3", "$4", "$5")})
            """, Arg(Qualified(name)), Arg(c.Table.Oid), Arg(c.Lo), Arg(c.Hi), Arg(MaxLive + 1));

        var outcome = await RunLockedAsync("jobs", check, days, async (reader, t) =>
        {
            if (!await reader.ReadAsync(t))
                return;
            attached = !reader.IsDBNull(0) && reader.GetBoolean(0);
            for (var i = 1; i <= 3; i++)
                recheck.UnionWith(reader.GetFieldValue<Guid[]>(i));
        }, () =>
        {
            if (!attached)
                return [];
            // §7.1: @ids — объединение предпроверки и повторной проверки. Набор предпроверки на этом пути непуст, поэтому
            // отсоединения нет даже при пустой повторной проверке: (б) под блокировкой не повторяется, а член, ставший живым
            // по (б) после предпроверки (отмена, окончательный fail), был в ней строкой queue и переносится через объединение.
            Guid[] ids = [.. live.Union(recheck)];
            if (ids.Length > MaxLive)
                return [];
            return CompactionCommands(c, remnant, ids);
        }, ct);

        switch (outcome)
        {
            case LockedOutcome.LockTimeout or LockedOutcome.HoldTimeout:
                acc.DeferGroup("jobs", [c], outcome);
                return false;
            case LockedOutcome.RolledBack when !attached:
                // обработал другой под: удалится, только если отсоединена и имя указывает на тот же oid (п. 6)
                acc.RecordDrop(Detached(c.Table), c.Overdue, await DropAsync(name, c.Table.Oid, ct), countRemnant: true);
                return true;
            case LockedOutcome.RolledBack:
                acc.Deferred.Add(new DeferredPartition("jobs", c.Day, DeferReason.LiveJobs, c.Overdue, MaxLive + 1));
                return true;
        }

        await HookAsync(RetentionStep.Committed, "jobs", days, ct);
        acc.Pinned.Add(new PinnedPartition(c.Day, c.Overdue, live.Length, Compacted: true));   // §5.1: LiveJobs — по предпроверке
        acc.RecordDrop(c.Table with { Name = remnant, Kind = CatalogKind.CompactionRemnant }, c.Overdue,
            await DropAsync(remnant, c.Table.Oid, ct), countRemnant: false);
        return true;
    }

    /// <summary>
    /// Compaction batch B (§5.3, step 5). SELECT * is safe because PgBolt alone creates partitions with LIKE, preserving logical column order across
    /// parent, old, and new tables (§11). ATTACH checks at most T rows, so no range CHECK is needed.
    /// </summary>
    private List<NpgsqlBatchCommand> CompactionCommands(Candidate c, string remnant, Guid[] ids)
    {
        var partition = Qualified(c.Table.Name);
        return
        [
            new NpgsqlBatchCommand($"ALTER TABLE {_schema}.jobs DETACH PARTITION {partition}"),
            new NpgsqlBatchCommand($"ALTER TABLE {partition} RENAME TO {remnant}"),
            new NpgsqlBatchCommand(
                $"CREATE TABLE {partition} (LIKE {_schema}.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = {_options.JobsFillFactor})"),
            Cmd($"INSERT INTO {partition} SELECT * FROM {Qualified(remnant)} WHERE id = ANY($1)", Arg(ids)),
            new NpgsqlBatchCommand($"ALTER TABLE {_schema}.jobs ATTACH PARTITION {partition} FOR VALUES FROM ('{c.Lo}') TO ('{c.Hi}')"),
        ];
    }
}
