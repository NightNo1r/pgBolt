using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;

namespace PgBolt.Abstractions.Tests;

public sealed class WorkflowContractsTests
{
    [Fact]
    public void BatchId_is_a_value_with_uuid_v7_factory()
    {
        var a = BatchId.New();
        Assert.Equal(7, a.Value.Version);
        Assert.Equal(a, new BatchId(a.Value));
        Assert.Equal(a.Value.ToString(), a.ToString());
    }

    [Fact]
    public void Enums_follow_stored_codes()   // §2.4: batches.state 0–4
    {
        Assert.Equal([0, 1, 2, 3, 4], Enum.GetValues<BatchState>().Select(s => (int)s));
        Assert.Equal([BatchState.Active, BatchState.Awaiting, BatchState.Succeeded, BatchState.Failed, BatchState.Cancelled],
            Enum.GetValues<BatchState>());
        Assert.Equal([ContinuationCondition.OnSuccess, ContinuationCondition.OnAnyFinished], Enum.GetValues<ContinuationCondition>());
        Assert.Equal([BatchCancelResult.NotFound, BatchCancelResult.AlreadyFinished, BatchCancelResult.CancellationRequested],
            Enum.GetValues<BatchCancelResult>());
    }

    [Fact]
    public void Exceptions_carry_ids()
    {
        var parent = Guid.CreateVersion7();
        var missing = new ParentNotFoundException(parent);
        Assert.IsAssignableFrom<InvalidOperationException>(missing);
        Assert.Equal(parent, missing.ParentId);
        Assert.Contains(parent.ToString(), missing.Message);

        var existing = JobId.New();
        var conflict = new UniqueKeyConflictException("k", existing);
        Assert.IsAssignableFrom<InvalidOperationException>(conflict);
        Assert.Equal("k", conflict.UniqueKey);
        Assert.Equal(existing, conflict.ExistingId);
        Assert.Contains("'k'", conflict.Message);
        Assert.Null(new UniqueKeyConflictException("k", null).ExistingId);
    }

    [Fact]
    public void JobContext_exposes_workflow_links()
    {
        var batch = BatchId.New();
        var parentBatch = BatchId.New();
        var ctx = new JobContext(JobId.New(), "n", 1, 3, null, batch, NullLogger.Instance, parentBatchId: parentBatch);
        Assert.Equal(batch, ctx.BatchId);
        Assert.Equal(parentBatch, ctx.ParentBatchId);
        Assert.Null(new JobContext(JobId.New(), "n", 1, 3, null, null, NullLogger.Instance).ParentBatchId);
    }

    [Fact]
    public void JobInfo_workflow_fields_default_to_null()
    {
        var info = new JobInfo(JobId.New(), "n", "q", 0, JobState.Awaiting, 0, 3, DateTimeOffset.UtcNow, null, null, null,
            null, null, false, default(JsonElement), []);
        Assert.Null(info.ParentId);
        Assert.Null(info.ParentBatchId);
        Assert.Null(info.BatchId);
        Assert.Null(info.Continuation);
    }

    [Fact]
    public void Options_and_results_are_value_records()
    {
        Assert.Equal(new StepOptions { Queue = "q", UniqueKey = "k" }, new StepOptions { Queue = "q", UniqueKey = "k" });
        Assert.Null(new WorkflowOptions().Transaction);
        var result = new WorkflowResult([JobId.New()], [BatchId.New()]);
        Assert.Single(result.Jobs);
        Assert.Single(result.Batches);
        var info = new BatchInfo(BatchId.New(), BatchState.Active, 3, 1, 0, 0, null, JobId.New(), null,
            ContinuationCondition.OnAnyFinished, false, DateTimeOffset.UtcNow, null);
        Assert.Equal((3, 1, 0, 0), (info.Total, info.Succeeded, info.Failed, info.Cancelled));
        Assert.Null(info.OuterBatchId);
        Assert.Equal(ContinuationCondition.OnAnyFinished, info.Condition);
    }
}
