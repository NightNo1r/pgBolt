using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;

namespace PgBolt.Dashboard.Endpoints;

internal sealed record DashboardAsset(string Hash, string ContentType, byte[] Content);

/// <summary>Embedded-ресурсы Assets/, адресуемые хешем содержимого.</summary>
// htmx.min.js — htmx 2.0.4, sha256 e209dda5c8235479f3166defc7750e1dbcd5a5c1808b7792fc2e6733768fb447
internal sealed class DashboardAssets
{
    private const string Prefix = "PgBolt.Dashboard.Assets.";

    private readonly Dictionary<string, DashboardAsset> _byFile;

    private DashboardAssets(Dictionary<string, DashboardAsset> byFile) => _byFile = byFile;

    public static DashboardAssets Load()
    {
        var assembly = typeof(DashboardAssets).Assembly;
        var byFile = new Dictionary<string, DashboardAsset>(StringComparer.Ordinal);
        foreach (var name in assembly.GetManifestResourceNames().Where(n => n.StartsWith(Prefix, StringComparison.Ordinal)))
        {
            using var stream = assembly.GetManifestResourceStream(name)!;
            var content = new byte[stream.Length];
            stream.ReadExactly(content);
            var file = name[Prefix.Length..];
            byFile[file] = new DashboardAsset(Convert.ToHexStringLower(SHA256.HashData(content))[..16], ContentType(file), content);
        }
        return new DashboardAssets(byFile);
    }

    public string RelativeUrl(string file) => $"assets/{_byFile[file].Hash}/{file}";

    public bool TryGet(string hash, string file, [MaybeNullWhen(false)] out DashboardAsset asset) =>
        _byFile.TryGetValue(file, out asset) && asset.Hash == hash;

    private static string ContentType(string file) => Path.GetExtension(file) switch
    {
        ".js" => "text/javascript; charset=utf-8",
        ".css" => "text/css; charset=utf-8",
        _ => throw new InvalidOperationException($"PgBolt dashboard: unexpected asset '{file}'."),
    };
}
