namespace PgBolt;

/// <summary>Идентификатор batch (UUIDv7, генерируется на клиенте при создании шага, часть 5 §5.2).</summary>
public readonly record struct BatchId(Guid Value)
{
    public static BatchId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString();
}
