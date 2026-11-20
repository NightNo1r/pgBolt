using System.Runtime.CompilerServices;
using VerifyXunit;

namespace PgBolt.Dashboard.Tests;

public static class ModuleInit
{
    [ModuleInitializer]
    public static void Init() => Verifier.UseProjectRelativeDirectory("Snapshots");
}
