using System.Data;
using System.Diagnostics;
using Npgsql;
using PgBolt.LoadTest.Env;
using PgBolt.LoadTest.Load;

namespace PgBolt.LoadTest.Measure;

/// <summary>
/// Посекундный срез БД и PgBouncer на отдельном прямом соединении, не из пулов хостов. Каждый запрос среза
/// выполняется отдельно: неудачный даёт пропуск только своих значений. Соединение открывается один раз в начале
/// попытки; если запрос его разорвал, остальные запросы попытки неудачны, а новое соединение — на следующей
/// попытке. Так при недоступной БД попытка стоит одного таймаута открытия, а не по таймауту на запрос.
/// Попытка без единого удачного запроса к БД не записывается; после 30 таких подряд <see cref="Exhausted"/> —
/// соединение стенда потеряно, срез останавливается.
/// </summary>
/// <param name="schema">Схема прогона: имя — уже проверенный идентификатор, подставляется в текст SQL.</param>
/// <param name="console">Админ-консоль своего PgBouncer; <c>null</c> — данных PgBouncer нет.</param>
/// <param name="applicationNames"><c>application_name</c> соединений хостов и продюсера.</param>
/// <param name="rate">Текущий целевой поток: готовые задачи считаются до rate + 1.</param>
/// <param name="completed">Выполнено задач с начала прогона.</param>
internal sealed class Sampler(
    string connectionString, string schema, PgBouncerConsole? console, IReadOnlyCollection<string> applicationNames,
    Func<int> rate, Func<long> completed)
{
    // 10-секундный счёт при раздутой куче queue читает её целиком и может идти дольше ежесекундных запросов.
    private const int CountsTimeoutSeconds = 30;

    private readonly string _connectionString = new NpgsqlConnectionStringBuilder(
        StandConnection.For(connectionString, "sampler").ConnectionString)
    {
        Timeout = 5,
        CommandTimeout = 5,
    }.ConnectionString;

    private readonly SamplerSchedule _schedule = new(TimeSpan.FromSeconds(1));
    private readonly List<Sample> _samples = [];
    private NpgsqlConnection? _conn;
    private long _failedQueries;

    public IReadOnlyList<Sample> Samples
    {
        get { lock (_samples) return [.. _samples]; }
    }

    public long SkippedTicks
    {
        get { lock (_schedule) return _schedule.SkippedTicks; }
    }

    /// <summary>Попытки, в которых не выполнился ни один запрос к БД.</summary>
    public long FailedAttempts
    {
        get { lock (_schedule) return _schedule.FailedAttempts; }
    }

    /// <summary>
    /// Неудачные запросы среза, включая <c>SHOW POOLS</c>. Неудачное открытие соединения или его обрыв — один
    /// неудачный запрос на попытку: запросы, пропущенные после этого в той же попытке, не считаются.
    /// </summary>
    public long FailedQueries => Interlocked.Read(ref _failedQueries);

    public bool Exhausted
    {
        get { lock (_schedule) return _schedule.Exhausted; }
    }

    /// <summary>Снимает срезы до отмены <paramref name="ct"/> или до <see cref="Exhausted"/>.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_schedule.Period);
        var running = Task.CompletedTask;
        try
        {
            while (!Exhausted && await timer.WaitForNextTickAsync(ct))
            {
                var now = TimeSpan.FromMicroseconds(RunClock.NowMicros);
                TickDecision decision;
                lock (_schedule) decision = _schedule.OnTick(now);
                if (decision != TickDecision.Skip)
                    running = TakeAsync(now, decision == TickDecision.SampleWithCounts, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }

        try
        {
            await running;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        if (_conn is not null) await _conn.DisposeAsync();
    }

    private async Task TakeAsync(TimeSpan at, bool withCounts, CancellationToken ct)
    {
        long? backlog = null, queueBytes = null, delayed = null, leased = null, wal = null, databaseBytes = null;
        int? connections = null, lockWaits = null, autovacuumWorkers = null;
        TableSample? queue = null, completions = null;
        IReadOnlyDictionary<string, long>? indexBytes = null;
        CheckpointerSample? checkpointer = null;
        IReadOnlyList<VacuumProgress>? vacuums = null;
        var completedNow = completed();

        if (_conn is null && !await TryOpenAsync(ct))
        {
            lock (_schedule) _schedule.OnCompleted(anyQueryOk: false);
            return;
        }

        var watch = Stopwatch.StartNew();
        var anyOk = await TryAsync($"""
            SELECT count(*) FROM (SELECT 1 FROM {schema}.queue WHERE ready AND lease_owner IS NULL LIMIT @limit) s
            """, ps => ps.AddWithValue("limit", rate() + 1L), r => backlog = r.GetInt64(0), ct);

        anyOk |= await TryAsync("""
            SELECT count(*) FILTER (WHERE application_name = ANY(@apps)),
                   count(*) FILTER (WHERE backend_type = 'client backend' AND datname = current_database()
                                      AND application_name NOT LIKE @stand
                                      AND wait_event_type = 'LWLock' AND wait_event = 'LockManager'),
                   count(*) FILTER (WHERE backend_type = 'autovacuum worker')
            FROM pg_stat_activity
            """, ps =>
        {
            ps.AddWithValue("apps", applicationNames.ToArray());
            ps.AddWithValue("stand", StandConnection.ApplicationNamePrefix + "%");
        }, r =>
        {
            connections = (int)r.GetInt64(0);
            lockWaits = (int)r.GetInt64(1);
            autovacuumWorkers = (int)r.GetInt64(2);
        }, ct);

        anyOk |= await TryAsync("""
            SELECT relname, n_live_tup, n_dead_tup, autovacuum_count, last_autovacuum FROM pg_stat_user_tables
            WHERE schemaname = @schema AND relname IN ('queue', 'completions')
            """, ps => ps.AddWithValue("schema", schema), r =>
        {
            var table = new TableSample(r.GetInt64(1), r.GetInt64(2), r.GetInt64(3),
                r.IsDBNull(4) ? null : r.GetDateTime(4));
            if (r.GetString(0) == "queue") queue = table;
            else completions = table;
        }, ct);

        var indexRows = new Dictionary<string, long>();
        if (await TryAsync($"""
                SELECT pg_total_relation_size(q.oid), c.relname, pg_relation_size(c.oid)
                FROM (SELECT '{schema}.queue'::regclass AS oid) q
                LEFT JOIN pg_index i ON i.indrelid = q.oid
                LEFT JOIN pg_class c ON c.oid = i.indexrelid
                """, null, r =>
            {
                queueBytes = r.GetInt64(0);
                if (!r.IsDBNull(1)) indexRows[r.GetString(1)] = r.GetInt64(2);
            }, ct))
        {
            indexBytes = indexRows;
            anyOk = true;
        }

        anyOk |= await TryAsync("SELECT wal_bytes::bigint FROM pg_stat_wal", null, r => wal = r.GetInt64(0), ct);

        anyOk |= await TryAsync("SELECT pg_database_size(current_database())", null,
            r => databaseBytes = r.GetInt64(0), ct);

        anyOk |= await TryAsync("""
            SELECT num_timed, num_requested, write_time, sync_time, buffers_written FROM pg_stat_checkpointer
            """, null, r => checkpointer = new CheckpointerSample(
                r.GetInt64(0), r.GetInt64(1), r.GetDouble(2), r.GetDouble(3), r.GetInt64(4)), ct);

        var vacuumRows = new List<VacuumProgress>();
        if (await TryAsync("""
                SELECT c.relname, p.phase FROM pg_stat_progress_vacuum p
                JOIN pg_class c ON c.oid = p.relid JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE p.datname = current_database() AND n.nspname = @schema
                """, ps => ps.AddWithValue("schema", schema),
                r => vacuumRows.Add(new VacuumProgress(r.GetString(0), r.GetString(1))), ct))
        {
            vacuums = vacuumRows;
            anyOk = true;
        }
        var queryTime = watch.Elapsed;

        TimeSpan? countsTime = null;
        if (withCounts)
        {
            watch.Restart();
            // Счёт по частичным индексам: в фазах удержания xmin куча queue раздута мёртвыми версиями.
            if (await TryAsync($"""
                    SELECT (SELECT count(*) FROM {schema}.queue WHERE NOT ready),
                           (SELECT count(*) FROM {schema}.queue WHERE lease_owner IS NOT NULL)
                    """, null, r => (delayed, leased) = (r.GetInt64(0), r.GetInt64(1)), ct, CountsTimeoutSeconds))
            {
                anyOk = true;
                countsTime = watch.Elapsed;
            }
        }

        int? clWaiting = null, svActive = null, svIdle = null;
        long? maxWait = null;
        if (console is not null)
        {
            try
            {
                var pools = (await console.ShowPoolsAsync(ct)).Where(p => p.Database != "pgbouncer").ToList();
                clWaiting = pools.Sum(p => p.ClWaiting);
                svActive = pools.Sum(p => p.SvActive);
                svIdle = pools.Sum(p => p.SvIdle);
                maxWait = pools.Count == 0 ? 0 : pools.Max(p => p.MaxWaitMicros);
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
                Interlocked.Increment(ref _failedQueries);
            }
        }

        if (anyOk)
        {
            var sample = new Sample(at, backlog, queueBytes, maxWait, clWaiting, connections,
                completedNow, delayed, leased, queue, completions, indexBytes, wal, checkpointer, lockWaits,
                svActive, svIdle, autovacuumWorkers, vacuums, queryTime, countsTime, databaseBytes);
            lock (_samples) _samples.Add(sample);
        }
        lock (_schedule) _schedule.OnCompleted(anyOk);
    }

    /// <summary>
    /// Выполняет запрос и читает все строки; <c>false</c> — запрос не выполнился. Ошибка сервера (например,
    /// «out of shared memory») соединение не разрывает, и следующие запросы попытки идут по нему же.
    /// </summary>
    private async Task<bool> TryAsync(string sql, Action<NpgsqlParameterCollection>? bind,
        Action<NpgsqlDataReader> read, CancellationToken ct, int? timeoutSeconds = null)
    {
        if (_conn is null)
            return false;   // соединение оборвалось раньше в этой попытке, обрыв уже посчитан

        try
        {
            await using var cmd = new NpgsqlCommand(sql, _conn);
            if (timeoutSeconds is { } timeout) cmd.CommandTimeout = timeout;
            bind?.Invoke(cmd.Parameters);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
                read(r);
            return true;
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
            Interlocked.Increment(ref _failedQueries);
            if (_conn.State != ConnectionState.Open)
            {
                await _conn.DisposeAsync();
                _conn = null;
            }
            return false;
        }
    }

    private async Task<bool> TryOpenAsync(CancellationToken ct)
    {
        _conn = new NpgsqlConnection(_connectionString);
        try
        {
            await _conn.OpenAsync(ct);
            return true;
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
            Interlocked.Increment(ref _failedQueries);
            await _conn.DisposeAsync();
            _conn = null;
            return false;
        }
    }
}
