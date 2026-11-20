using Microsoft.Extensions.Logging.Abstractions;

namespace PgBolt.Abstractions.Tests;

public sealed class ContractsTests
{
    [Fact]
    public void JobId_New_is_uuid_v7()
    {
        var id = JobId.New();
        Assert.Equal(7, id.Value.Version);
        Assert.Equal(id.Value.ToString(), id.ToString());
    }

    [Fact]
    public void JobAttribute_has_spec_defaults()
    {
        var a = new JobAttribute("orders.send-receipt");
        Assert.Equal("orders.send-receipt", a.Name);
        Assert.Equal("default", a.Queue);
        Assert.Equal(10, a.MaxAttempts);
        Assert.Equal(0, a.Priority);
        Assert.Null(a.RetryDelaysSeconds);
    }

    [Fact]
    public void JobEventKind_codes_match_spec()
    {
        Assert.Equal(1, (short)JobEventKind.Retry);
        Assert.Equal(2, (short)JobEventKind.Failed);
        Assert.Equal(3, (short)JobEventKind.LeaseExpired);
        Assert.Equal(4, (short)JobEventKind.Cancelled);
        Assert.Equal(5, (short)JobEventKind.Requeued);
        Assert.Equal(6, (short)JobEventKind.Triggered);
        Assert.Equal(7, (short)JobEventKind.NoExecutor);
    }

    [Fact]
    public void JobDescriptor_SameAs_compares_retry_delays_by_value()
    {
        var a = new JobDescriptor("x", "q", 3, 1, [1, 2]);
        var b = new JobDescriptor("x", "q", 3, 1, [1, 2]);
        var c = new JobDescriptor("x", "q", 3, 1, null);
        Assert.True(a.SameAs(b));
        Assert.False(a.SameAs(c));
    }

    [Fact]
    public void JobContext_exposes_constructor_values()
    {
        var id = JobId.New();
        var parent = JobId.New();
        var batch = BatchId.New();
        var ctx = new JobContext(id, "n", 2, 5, parent, batch, NullLogger.Instance);
        Assert.Equal((id, "n", 2, 5, parent, batch), (ctx.JobId, ctx.JobName, ctx.Attempt, ctx.MaxAttempts, ctx.ParentId, ctx.BatchId));
        Assert.Same(NullLogger.Instance, ctx.Logger);
    }

    [Fact]
    public void UnknownJobException_carries_name()
    {
        var e = new UnknownJobException("a.b");
        Assert.Equal("a.b", e.JobName);
        Assert.Contains("a.b", e.Message);
    }

    [Fact]
    public void JobAttribute_TimeoutSeconds_defaults_to_zero_and_is_settable()
    {
        Assert.Equal(0, new JobAttribute("a.job").TimeoutSeconds);
        Assert.Equal(30, new JobAttribute("a.job") { TimeoutSeconds = 30 }.TimeoutSeconds);
    }

    [Fact]
    public void JobDescriptor_TimeoutSeconds_defaults_to_zero_and_participates_in_SameAs()
    {
        var a = new JobDescriptor("x", "q", 3, 1, [1, 2]);
        var b = a with { TimeoutSeconds = 30 };

        Assert.Equal(0, a.TimeoutSeconds);
        Assert.Equal(30, b.TimeoutSeconds);
        Assert.False(a.SameAs(b));
        Assert.False(b.SameAs(a));
        Assert.True(b.SameAs(new JobDescriptor("x", "q", 3, 1, [1, 2]) { TimeoutSeconds = 30 }));
        Assert.Contains("TimeoutSeconds = 30", b.ToString());
    }

    [Fact]
    public void JobAbortedException_carries_message_and_inner_and_is_not_sealed()
    {
        var inner = new InvalidOperationException("root");
        var e = new JobAbortedException("retry is pointless", inner);

        Assert.Equal("retry is pointless", e.Message);
        Assert.Same(inner, e.InnerException);
        Assert.Null(new JobAbortedException("m").InnerException);
        Assert.False(typeof(JobAbortedException).IsSealed);
    }

    [Fact]
    public void JobArgumentsException_carries_message_and_inner()
    {
        var inner = new FormatException("bad json");
        var e = new JobArgumentsException("cannot deserialize", inner);

        Assert.Equal("cannot deserialize", e.Message);
        Assert.Same(inner, e.InnerException);
        Assert.Null(new JobArgumentsException("m").InnerException);
        Assert.True(typeof(JobArgumentsException).IsSealed);
    }
}
