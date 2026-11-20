namespace PgBolt.Postgres.Client;

/// <summary>
/// Пробуждение resolver'а процесса после записи сигналов завершения (часть 5 §6.4): финализация с подписчиками,
/// CancelBatchAsync, синтетические сигналы SubmitAsync. Сигнал не гарантирован — resolver страхуется интервалом.
/// </summary>
public interface ICompletionSignal
{
    void Signal();

    /// <summary>callback вызывается синхронно из Signal; должен быть дешёвым и не блокировать.</summary>
    /// <returns>Подписка; Dispose отписывает (идемпотентно).</returns>
    IDisposable Subscribe(Action callback);
}
