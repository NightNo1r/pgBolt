using PgBolt.Testing;

[assembly: AssemblyFixture(typeof(PostgresFixture))]
[assembly: PgBolt.PgBoltJsonContext(typeof(PgBolt.E2E.Tests.E2EJson))]
