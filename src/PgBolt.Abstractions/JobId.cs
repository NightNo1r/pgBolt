namespace PgBolt;

/// <summary>Идентификатор задачи (UUIDv7, генерируется на клиенте).</summary>
public readonly record struct JobId(Guid Value)
{
    public static JobId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString();
}
