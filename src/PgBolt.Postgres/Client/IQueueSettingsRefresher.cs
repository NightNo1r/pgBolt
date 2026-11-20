namespace PgBolt.Postgres.Client;

/// <summary>Внеочередное обновление настроек очередей на серверах этого процесса (после записи менеджера).</summary>
internal interface IQueueSettingsRefresher
{
    void RefreshNow();
}
