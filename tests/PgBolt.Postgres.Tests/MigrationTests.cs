using System.Text.RegularExpressions;
using Npgsql;
using PgBolt.Postgres.Schema;
using PgBolt.Testing;

namespace PgBolt.Postgres.Tests;

public abstract class MigrationTests(PostgresFixture pg, ConnectionMode mode) : IAsyncLifetime
{
    private TestDb _db = null!;

    public async ValueTask InitializeAsync() => _db = await TestDb.CreateAsync(pg, mode, migrate: false);
    public async ValueTask DisposeAsync() => await _db.DisposeAsync();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private MigrationRunner Runner() => new(_db.DataSource, _db.Schema);

    [Fact]
    public async Task Migrate_creates_all_tables_once()
    {
        Assert.Equal(PgBoltMigrations.All.Count, await Runner().MigrateAsync(Ct));
        Assert.Equal(0, await Runner().MigrateAsync(Ct));
        foreach (var table in new[] { "queue", "jobs", "job_events", "job_types", "servers", "schema_version", "maintenance_tasks", "recurring_jobs", "batches", "completions", "stats_minutes", "queue_stats", "queue_settings" })
            Assert.True(await _db.ScalarAsync<bool>("SELECT to_regclass(@t) IS NOT NULL", ("t", $"{_db.Schema}.{table}")), table);
        Assert.Equal(PgBoltMigrations.LatestVersion, await Runner().GetCurrentVersionAsync(Ct));
    }

    private async Task<string> ColumnsAsync(string table)
    {
        await using var cmd = _db.Admin.CreateCommand("""
            SELECT string_agg(attname || ':' || format_type(atttypid, atttypmod) || ':' || attnotnull, ',' ORDER BY attnum)
            FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND NOT attisdropped
            """);
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.{table}");
        return (string)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private Task<string> IndexesAsync(string table, string pkey) =>
        _db.ScalarAsync<string>(
            "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes WHERE schemaname = @s AND tablename = @t AND indexname <> @p",
            ("s", _db.Schema), ("t", table), ("p", pkey));

    [Fact]
    public async Task Queue_and_jobs_have_workflow_columns_and_indexes()   // часть 5 §4
    {
        await Runner().MigrateAsync(Ct);

        Assert.EndsWith(
            ",cancel_requested:boolean:true,parent_id:uuid:false,await_condition:smallint:false,batch_id:uuid:false,notify:boolean:true",
            await ColumnsAsync("queue"));
        Assert.EndsWith(
            ",recurring_id:text:false,scheduled_at:timestamp with time zone:false,continuation:smallint:false,notify:boolean:true",
            await ColumnsAsync("jobs"));
        Assert.Equal("queue_await_jobs,queue_awaiting,queue_awaiting_list,queue_delayed,queue_expiry,queue_fetch,queue_leased,queue_unique",
            await IndexesAsync("queue", "queue_pkey"));
        Assert.Equal("jobs_batch,jobs_parent,jobs_recurring", await IndexesAsync("jobs", "jobs_pkey"));
        Assert.True(await _db.ScalarAsync<bool>("""
            SELECT pg_get_indexdef(to_regclass(@a)) LIKE '%(parent_id, job_id) WHERE (await_condition IS NOT NULL)'
               AND pg_get_indexdef(to_regclass(@j)) LIKE '%(batch_id, id) WHERE (batch_id IS NOT NULL)'
            """, ("a", $"{_db.Schema}.queue_awaiting"), ("j", $"{_db.Schema}.jobs_batch")));
        Assert.Equal("n_distinct=-0.05", await AttributeOptionsAsync("queue", "parent_id"));
    }

    /// <summary>pg_attribute.attoptions колонки: закреплённый n_distinct поиска детей по parent_id (§4, §6.4).</summary>
    private Task<string> AttributeOptionsAsync(string table, string column) =>
        _db.ScalarAsync<string>(
            "SELECT coalesce(array_to_string(attoptions, ','), '') FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attname = @c",
            ("t", $"{_db.Schema}.{table}"), ("c", column));

    [Fact]
    public async Task Batches_and_completions_have_spec_columns_and_indexes()   // часть 5 §4
    {
        await Runner().MigrateAsync(Ct);

        Assert.Equal(
            "id:uuid:true,parent_batch_id:uuid:false,parent_id:uuid:false,condition:smallint:false,state:smallint:true," +
            "total:integer:true,succeeded:integer:true,failed:integer:true,cancelled:integer:true,cancel_requested:boolean:true," +
            "created_at:timestamp with time zone:true,finished_at:timestamp with time zone:false",
            await ColumnsAsync("batches"));
        Assert.Equal("batches_await_jobs,batches_awaiting,batches_finished,batches_nested,batches_open,batches_parent",
            await IndexesAsync("batches", "batches_pkey"));
        Assert.Equal("n_distinct=-0.05", await AttributeOptionsAsync("batches", "parent_id"));
        Assert.Equal("n_distinct=-0.05", await AttributeOptionsAsync("batches", "parent_batch_id"));
        Assert.Equal(
            "id:bigint:true,node_id:uuid:true,kind:smallint:true,batch_id:uuid:false,counted:boolean:true,stage:smallint:true," +
            "cursor:uuid:false,retry_after:timestamp with time zone:false,created_at:timestamp with time zone:true",
            await ColumnsAsync("completions"));
        Assert.Equal("completions_cancel,completions_count,completions_walk", await IndexesAsync("completions", "completions_pkey"));
        Assert.Contains("autovacuum_vacuum_threshold=1000",
            await _db.ScalarAsync<string[]>("SELECT reloptions FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.completions")));
    }

    [Fact]
    public async Task Jobs_partitions_inherit_workflow_columns_and_batch_index()
    {
        await Runner().MigrateAsync(Ct);
        await new PartitionManager(_db.Admin, _db.Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(1, Ct);

        Assert.Equal(0L, await _db.ScalarAsync<long>("""
            SELECT count(*) FROM pg_inherits i
            WHERE i.inhparent = to_regclass(@t)
              AND ((SELECT count(*) FROM pg_attribute a
                    WHERE a.attrelid = i.inhrelid AND a.attname IN ('continuation', 'notify') AND NOT a.attisdropped) <> 2
                   OR NOT EXISTS (SELECT 1 FROM pg_index x JOIN pg_inherits p ON p.inhrelid = x.indexrelid
                                  WHERE x.indrelid = i.inhrelid AND p.inhparent = to_regclass(@b)))
            """, ("t", $"{_db.Schema}.jobs"), ("b", $"{_db.Schema}.jobs_batch")));
        Assert.Equal(2L, await _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
    }

    [Fact]
    public async Task Maintenance_tasks_has_spec_columns_and_no_rows()
    {
        await Runner().MigrateAsync(Ct);
        Assert.Equal(1, PgBoltMigrations.LatestVersion);
        await using var cmd = _db.Admin.CreateCommand("""
            SELECT string_agg(attname || ':' || format_type(atttypid, atttypmod) || ':' || attnotnull, ',' ORDER BY attnum)
            FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND NOT attisdropped
            """);
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.maintenance_tasks");
        Assert.Equal(
            "name:text:true,next_run_at:timestamp with time zone:true,last_run_at:timestamp with time zone:false," +
            "last_finished_at:timestamp with time zone:false,last_error:text:false",
            (string)(await cmd.ExecuteScalarAsync(Ct))!);
        Assert.Equal(0L, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.maintenance_tasks"));
    }

    [Fact]
    public async Task Recurring_jobs_has_spec_columns_and_indexes()
    {
        await Runner().MigrateAsync(Ct);
        await using var cmd = _db.Admin.CreateCommand("""
            SELECT string_agg(attname || ':' || format_type(atttypid, atttypmod) || ':' || attnotnull, ',' ORDER BY attnum)
            FROM pg_attribute WHERE attrelid = to_regclass(@t) AND attnum > 0 AND NOT attisdropped
            """);
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.recurring_jobs");
        Assert.Equal(
            "id:text:true,type:text:true,args:jsonb:true,cron:text:true,time_zone:text:true,misfire:smallint:true," +
            "catch_up_window:interval:true,overlap:smallint:true,queue:text:false,priority:smallint:false," +
            "max_attempts:integer:false,retry_delays:integer[]:false,owner:text:false,paused:boolean:true," +
            "next_fire_at:timestamp with time zone:false,wake_at:timestamp with time zone:false," +
            "last_fire_at:timestamp with time zone:false,last_job_id:uuid:false,last_error:text:false," +
            "created_at:timestamp with time zone:true,updated_at:timestamp with time zone:true",
            (string)(await cmd.ExecuteScalarAsync(Ct))!);
        Assert.Equal("recurring_due,recurring_owner", await _db.ScalarAsync<string>(
            "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes WHERE schemaname = @s AND tablename = 'recurring_jobs' AND indexname <> 'recurring_jobs_pkey'",
            ("s", _db.Schema)));
    }

    [Fact]
    public async Task Jobs_partitions_have_recurring_columns()
    {
        await Runner().MigrateAsync(Ct);
        await new PartitionManager(_db.Admin, _db.Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(1, Ct);

        var missing = await _db.ScalarAsync<long>("""
            SELECT count(*) FROM pg_inherits i
            WHERE i.inhparent = to_regclass(@t)
              AND (SELECT count(*) FROM pg_attribute a
                   WHERE a.attrelid = i.inhrelid AND a.attname IN ('recurring_id', 'scheduled_at') AND NOT a.attisdropped) <> 2
            """, ("t", $"{_db.Schema}.jobs"));

        Assert.Equal(0L, missing);
        Assert.Equal(2L, await _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
    }

    [Fact]
    public async Task Dashboard_tables_and_indexes_match_spec()   // часть 6 §3
    {
        await Runner().MigrateAsync(Ct);

        Assert.Equal(
            "bucket:timestamp with time zone:true,server_id:uuid:true,queue:text:true,succeeded:integer:true," +
            "failed:integer:true,retried:integer:true", await ColumnsAsync("stats_minutes"));
        Assert.Equal("stats_minutes_pkey", await _db.ScalarAsync<string>(
            "SELECT string_agg(indexname, ',') FROM pg_indexes WHERE schemaname = @s AND tablename = 'stats_minutes'", ("s", _db.Schema)));
        Assert.Contains("(bucket, server_id, queue)", await _db.ScalarAsync<string>(
            "SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.stats_minutes_pkey")));
        Assert.Equal(
            "queue:text:true,scheduled:bigint:true,enqueued:bigint:true,processing:bigint:true,awaiting:bigint:true," +
            "measured_at:timestamp with time zone:true,oldest_ready_seconds:double precision:false", await ColumnsAsync("queue_stats"));
        Assert.Equal("job_events_cancelled,job_events_failed,job_events_job_id_idx", await IndexesAsync("job_events", ""));
        foreach (var (index, definition) in new[]
        {
            ("queue_awaiting_list", "(job_id) WHERE (await_condition IS NOT NULL)"),
            ("job_events_failed", "(at, job_id) WHERE (kind = ANY (ARRAY[2, 3, 7]))"),
            ("job_events_cancelled", "(at, job_id) WHERE (kind = 4)"),
            ("jobs_parent", "(parent_id, id) WHERE (continuation = ANY (ARRAY[1, 2, 3, 4]))"),
            ("jobs_recurring", "(recurring_id, id) WHERE (recurring_id IS NOT NULL)"),
            ("batches_parent", "(parent_id, id) WHERE (parent_id IS NOT NULL)"),
        })
            Assert.EndsWith(definition, await _db.ScalarAsync<string>("SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.{index}")));
    }

    [Fact]
    public async Task Partitions_inherit_dashboard_indexes()   // §3.2: ATTACH / LIKE родителя
    {
        await Runner().MigrateAsync(Ct);
        await new PartitionManager(_db.Admin, _db.Schema, 80, TimeSpan.FromSeconds(5)).EnsureAsync(1, Ct);

        foreach (var (parent, index) in new[] { ("jobs", "jobs_parent"), ("jobs", "jobs_recurring"),
                     ("job_events", "job_events_failed"), ("job_events", "job_events_cancelled") })
            Assert.Equal(0L, await _db.ScalarAsync<long>("""
                SELECT count(*) FROM pg_inherits i
                WHERE i.inhparent = to_regclass(@t)
                  AND NOT EXISTS (SELECT 1 FROM pg_index x JOIN pg_inherits p ON p.inhrelid = x.indexrelid
                                  WHERE x.indrelid = i.inhrelid AND p.inhparent = to_regclass(@x))
                """, ("t", $"{_db.Schema}.{parent}"), ("x", $"{_db.Schema}.{index}")));
    }

    [Fact]
    public void Script_contains_single_merged_migration()
    {
        var script = PgBoltMigrations.GetScript("my_schema");
        Assert.Contains("-- V001 initial", script);
        Assert.DoesNotContain("-- V002", script);
        Assert.Contains("CREATE TABLE my_schema.maintenance_tasks", script);
        Assert.Contains("CREATE TABLE my_schema.recurring_jobs", script);
        Assert.Contains("CREATE TABLE my_schema.batches", script);
        Assert.Contains("CREATE TABLE my_schema.completions", script);
        Assert.Contains("CREATE TABLE my_schema.stats_minutes", script);
        Assert.Contains("CREATE TABLE my_schema.queue_stats", script);
        Assert.Contains("VALUES (1, now())", script);
    }

    [Fact]
    public async Task Parallel_migrations_apply_each_version_exactly_once()
    {
        var applied = await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => Runner().MigrateAsync(Ct)));
        Assert.Equal(PgBoltMigrations.All.Count, applied.Sum());
        Assert.Equal((long)PgBoltMigrations.All.Count, await _db.ScalarAsync<long>("SELECT count(*) FROM {schema}.schema_version"));
    }

    [Fact]
    public async Task Version_is_null_before_migration()
    {
        Assert.Null(await Runner().GetCurrentVersionAsync(Ct));
    }

    [Fact]
    public async Task Parents_are_partitioned_and_have_no_partitions()
    {
        await Runner().MigrateAsync(Ct);
        Assert.Equal("p", await _db.ScalarAsync<string>("SELECT relkind::text FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
        Assert.Equal("p", await _db.ScalarAsync<string>("SELECT relkind::text FROM pg_class WHERE oid = to_regclass(@t)", ("t", $"{_db.Schema}.job_events")));
        Assert.Equal(0L, await _db.ScalarAsync<long>("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(@t)", ("t", $"{_db.Schema}.jobs")));
    }

    [Fact]
    public async Task Hot_contract_no_jobs_index_uses_mutable_columns()
    {
        await Runner().MigrateAsync(Ct);
        await using var cmd = _db.Admin.CreateCommand("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = to_regclass(@t)");
        cmd.Parameters.AddWithValue("t", $"{_db.Schema}.jobs");
        await using var r = await cmd.ExecuteReaderAsync(Ct);
        var mutable = new Regex(@"\b(state|attempt|started_at|finished_at|server_id)\b");
        var count = 0;
        while (await r.ReadAsync(Ct))
        {
            count++;
            Assert.DoesNotMatch(mutable, r.GetString(0));
        }
        Assert.Equal(4, count); // PK, jobs_batch, jobs_parent, jobs_recurring
    }

    [Fact]
    public async Task Queue_indexes_match_spec_predicates()
    {
        await Runner().MigrateAsync(Ct);
        var fetch = await _db.ScalarAsync<string>("SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.queue_fetch"));
        Assert.Contains("(queue, priority DESC, run_at)", fetch);
        Assert.Contains("WHERE ((lease_owner IS NULL) AND ready)", fetch);
        var delayed = await _db.ScalarAsync<string>("SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.queue_delayed"));
        Assert.Contains("WHERE (NOT ready)", delayed);
    }

    [Fact]
    public async Task Queue_settings_columns_and_checks()
    {
        await Runner().MigrateAsync(Ct);

        Assert.Equal(
            "queue:text:true,default_max_concurrency:integer:false,default_weight:integer:false,default_published_by:text:false," +
            "default_updated_at:timestamp with time zone:false,override_concurrency_set:boolean:true,override_max_concurrency:integer:false," +
            "override_concurrency_updated_by:text:false,override_concurrency_updated_at:timestamp with time zone:false," +
            "override_weight_set:boolean:true,override_weight:integer:false,override_weight_updated_by:text:false," +
            "override_weight_updated_at:timestamp with time zone:false",
            await ColumnsAsync("queue_settings"));
        foreach (var (columns, values) in new[]
        {
            ("default_max_concurrency", "0"),
            ("default_weight", "1001"),
            ("override_concurrency_set, override_max_concurrency", "true, -1"),
            ("override_weight_set, override_weight", "true, 0"),
            ("override_max_concurrency", "3"),   // значение без override_concurrency_set
        })
        {
            var e = await Assert.ThrowsAsync<PostgresException>(() =>
                _db.ExecAsync($"INSERT INTO {{schema}}.queue_settings (queue, {columns}) VALUES ('q', {values})"));
            Assert.Equal(PostgresErrorCodes.CheckViolation, e.SqlState);
        }
    }

    [Fact]
    public async Task Queue_leased_index_is_keyed_by_queue()
    {
        await Runner().MigrateAsync(Ct);

        Assert.EndsWith("(queue) WHERE (lease_owner IS NOT NULL)", await _db.ScalarAsync<string>(
            "SELECT pg_get_indexdef(to_regclass(@i))", ("i", $"{_db.Schema}.queue_leased")));
    }

    [Fact]
    public void Exported_script_substitutes_schema()
    {
        var script = PgBoltMigrations.GetScript("my_schema");
        Assert.DoesNotContain("{schema}", script);
        Assert.Contains("CREATE TABLE my_schema.queue", script);
        Assert.Contains("INSERT INTO my_schema.schema_version", script);
        Assert.StartsWith("BEGIN;", script);
        Assert.EndsWith("COMMIT;\n", script);
    }
}

public sealed class MigrationTests_Direct(PostgresFixture pg) : MigrationTests(pg, ConnectionMode.Direct);
public sealed class MigrationTests_Bouncer(PostgresFixture pg) : MigrationTests(pg, ConnectionMode.Bouncer);
