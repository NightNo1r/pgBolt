namespace PgBolt;

public readonly record struct EnqueueResult(JobId? Id, bool Created);

public enum CancelResult { NotFound, AlreadyFinished, Cancelled, CancellationRequested }

public enum RequeueResult { NotFound, NotTerminal, Conflict, Requeued }
