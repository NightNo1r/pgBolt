namespace PgBolt.Postgres.Client;

/// <summary>
/// Пробуждение воркеров после enqueue/trigger/promote (§5.10, часть 3 §8.2). Сигнал не гарантирован — воркер
/// страхуется polling'ом. Точка расширения (например, Redis pub/sub между подами).
/// </summary>
public interface IWakeupSignal
{
    void Signal(string queue);

    /// <summary>callback вызывается синхронно из Signal; должен быть дешёвым и не блокировать.</summary>
    /// <returns>Подписка; Dispose отписывает (идемпотентно).</returns>
    IDisposable Subscribe(string queue, Action callback);
}
