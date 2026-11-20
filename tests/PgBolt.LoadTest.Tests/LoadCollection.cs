using PgBolt.Testing;

namespace PgBolt.LoadTest.Tests;

/// <summary>
/// Одна коллекция на все тесты стенда с БД. Стенд держит глобальное состояние процесса (часы прогона,
/// счётчики метрик, учёт CPU), поэтому два одновременных прогона портят друг другу измерения; коллекция
/// выполняется отдельно от параллельной фазы. Фикстура создаётся только при запуске её тестов, так что
/// прогон без Docker (Category!=Load) контейнеры не поднимает.
/// </summary>
[CollectionDefinition("Load", DisableParallelization = true)]
public sealed class LoadCollection : ICollectionFixture<PostgresFixture>;
