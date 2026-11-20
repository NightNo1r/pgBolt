using System.Runtime.CompilerServices;

namespace PgBolt.Generators.Tests;

public static class ModuleInit
{
    [ModuleInitializer]
    public static void Init() => VerifySourceGenerators.Initialize();
}
