using PgBolt.Postgres.Storage;

namespace PgBolt.Testing;

/// <summary>Проходы resolver'а в тестах без цикла ProcessingMaintenance.</summary>
public static class ResolverPasses
{
    /// <summary>Т1, Т2 и (cancelMembers) CancelMembers, пока захватываются сигналы и выполняются порции отмены; не больше 200 проходов.</summary>
    public static async Task SettleAsync(PgBoltStore store, bool cancelMembers, CancellationToken ct)
    {
        for (var i = 0; i < 200; i++)
        {
            var counted = await store.CountCompletionsAsync(500, ct);
            var walked = await store.ResolveAsync(500, 5000, TimeSpan.FromSeconds(30), ct);
            var portions = cancelMembers ? (await store.CancelMembersAsync(1000, 5000, 100, TimeSpan.FromSeconds(30), ct)).Portions : 0;
            if (counted.Captured == 0 && walked.Captured == 0 && portions == 0)
                return;
        }
        throw new InvalidOperationException("The resolver did not settle within 200 passes.");
    }
}
