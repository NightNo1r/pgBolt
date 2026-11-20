namespace PgBolt.E2E.Contracts;

[Job("e2e.ping", Queue = "e2e")]
public sealed record PingArgs(string Message);

[Job("e2e.pong", Queue = "e2e", MaxAttempts = 3)]
public sealed record PongArgs(int N);
