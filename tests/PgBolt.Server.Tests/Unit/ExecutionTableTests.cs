using PgBolt.Server.Runtime;
using PgBolt.Server.Tests.Fakes;

namespace PgBolt.Server.Tests.Unit;

public sealed class ExecutionTableTests
{
    private static Execution Running(Guid? jobId = null) => new(Leased.Job(jobId), "q", 0, ExecutionState.Running);

    [Fact]
    public void Add_get_remove()
    {
        var table = new ExecutionTable();
        var e = Running();

        table.Add(e);
        Assert.Equal(1, table.Count);
        Assert.True(table.TryGet(e.Lease.LeaseId, out var found));
        Assert.Same(e, found);

        Assert.True(table.Remove(e));
        Assert.False(table.Remove(e));
        Assert.Equal(0, table.Count);
        Assert.False(table.TryGet(e.Lease.LeaseId, out _));
    }

    [Fact]
    public void Entries_enumerates_every_record_including_removed_state()   // поиск рубежа lease без Snapshot()
    {
        var table = new ExecutionTable();
        var running = Running();
        var removed = Running();
        removed.TryTransition(ExecutionState.Running, ExecutionState.Removed);
        table.Add(running);
        table.Add(removed);

        Assert.Equivalent(new[] { running, removed }, table.Entries().ToArray());
        Assert.Equal([running], table.Snapshot());   // Snapshot по-прежнему отбрасывает Removed
    }

    [Fact]
    public void Duplicate_lease_is_rejected()
    {
        var table = new ExecutionTable();
        var e = Running();
        table.Add(e);

        var twin = new Execution(e.Job, "q", 0, ExecutionState.Running);
        Assert.Throws<InvalidOperationException>(() => table.Add(twin));
        Assert.False(table.Remove(twin));             // удаляется только тот же объект
        Assert.True(table.TryGet(e.Lease.LeaseId, out var found));
        Assert.Same(e, found);
    }

    [Fact]
    public void Same_job_taken_again_with_a_new_lease_is_a_separate_entry()
    {
        var table = new ExecutionTable();
        var jobId = Guid.CreateVersion7();
        var stale = Running(jobId);
        var fresh = Running(jobId);
        Assert.NotEqual(stale.Lease.LeaseId, fresh.Lease.LeaseId);

        table.Add(stale);
        table.Add(fresh);
        Assert.Equal(2, table.Count);
        Assert.True(table.TryGet(stale.Lease.LeaseId, out var a));
        Assert.True(table.TryGet(fresh.Lease.LeaseId, out var b));
        Assert.Same(stale, a);
        Assert.Same(fresh, b);

        Assert.True(table.Remove(stale));
        Assert.True(table.TryGet(fresh.Lease.LeaseId, out var left));
        Assert.Same(fresh, left);
        Assert.Equal([fresh.Lease], table.Snapshot().Select(x => x.Lease));
    }

    [Fact]
    public void Snapshot_skips_removed_and_is_a_copy()
    {
        var table = new ExecutionTable();
        var running = Running();
        var pending = new Execution(Leased.Job(), "q", 0, ExecutionState.OutcomePending);
        var removed = Running();
        Assert.True(removed.TryTransition(ExecutionState.Running, ExecutionState.Removed));
        table.Add(running);
        table.Add(pending);
        table.Add(removed);

        var snapshot = table.Snapshot();
        table.Add(Running());

        Assert.Equal(2, snapshot.Count);
        Assert.Contains(running, snapshot);
        Assert.Contains(pending, snapshot);
        Assert.Equal(4, table.Count);
    }
}
