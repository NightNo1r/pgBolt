namespace PgBolt.Postgres.Client;

/// <summary>Requests an immediate refresh of queue settings on local servers after a manager write.</summary>
internal interface IQueueSettingsRefresher
{
    void RefreshNow();
}
