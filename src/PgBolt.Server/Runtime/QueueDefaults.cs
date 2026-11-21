using PgBolt.Postgres.Storage;

namespace PgBolt.Server.Runtime;

/// <summary>Queue defaults from code across all servers in the process, published to the store at startup.</summary>
internal static class QueueDefaults
{
    /// <summary>
    /// One entry per queue using ordinal comparison. Different values for the same queue across servers cause startup to
    /// fail.
    /// </summary>
    public static IReadOnlyList<QueueDefault> Collect(IReadOnlyList<ServerDefinition> servers)
    {
        var byQueue = new SortedDictionary<string, (string Server, QueueOptions Options)>(StringComparer.Ordinal);
        foreach (var server in servers)
            foreach (var queue in server.Options.Queues)
            {
                if (!byQueue.TryGetValue(queue.Name, out var first))
                {
                    byQueue[queue.Name] = (server.Name, queue);
                    continue;
                }

                Compare(queue.Name, nameof(QueueOptions.ClusterMaxConcurrency), first.Server, first.Options.ClusterMaxConcurrency,
                    server.Name, queue.ClusterMaxConcurrency);
                Compare(queue.Name, nameof(QueueOptions.Weight), first.Server, first.Options.Weight, server.Name, queue.Weight);
            }

        return [.. byQueue.Select(p => new QueueDefault(p.Key, p.Value.Options.ClusterMaxConcurrency, p.Value.Options.Weight))];
    }

    private static void Compare(string queue, string option, string serverA, int? valueA, string serverB, int? valueB)
    {
        if (valueA != valueB)
            throw new InvalidOperationException(
                $"PgBolt: queue '{queue}' has different {option} on servers '{serverA}' ({Show(valueA)}) and '{serverB}' ({Show(valueB)}).");
    }

    internal static string Show(int? value) => value?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "null";
}
