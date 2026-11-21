namespace PgBolt;

/// <summary>Identifies a job with a client-generated UUIDv7.</summary>
public readonly record struct JobId(Guid Value)
{
    public static JobId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString();
}
