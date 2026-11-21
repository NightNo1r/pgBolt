# PgBolt

PgBolt runs .NET background jobs on PostgreSQL. Use it to enqueue work in the same database transaction as application data, schedule recurring jobs, and run job chains or batches.

| Package | Use |
| --- | --- |
| PgBolt.Abstractions | Typed contracts, source generator and analyzers |
| PgBolt.Postgres | Job client, storage and migrations |
| PgBolt.Server | Hosted workers and queue scheduling |
| PgBolt.Dashboard | ASP.NET Core dashboard |
| PgBolt.OpenTelemetry | OpenTelemetry integration |

Applications target .NET 10. Workers deliver jobs at least once, so handlers should tolerate retries. The first release supports new database schemas.

[Integration guide and working example](https://github.com/NightNo1r/pgBolt#quickstart) · [Russian guide](https://github.com/NightNo1r/pgBolt/blob/main/README.ru.md#quickstart) · [Operational limits](https://github.com/NightNo1r/pgBolt#operations)
