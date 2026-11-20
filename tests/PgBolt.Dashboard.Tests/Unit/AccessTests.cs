using System.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using PgBolt.Dashboard.Security;

namespace PgBolt.Dashboard.Tests.Unit;

public sealed class AccessTests
{
    private static readonly Dictionary<string, (object[] Metadata, bool Fallback, bool Expected)> Authorization = new()
    {
        ["Authorize"] = ([new AuthorizeAttribute()], false, true),
        ["Authorize policy"] = ([new AuthorizeAttribute("P")], false, true),
        ["AuthorizationPolicy"] = ([new AuthorizationPolicyBuilder().RequireAuthenticatedUser().Build()], false, true),
        ["IAuthorizationRequirementData"] = ([new RequirementData()], false, true),
        ["empty"] = ([], false, false),
        ["empty with fallback"] = ([], true, true),
        ["Authorize and AllowAnonymous"] = ([new AuthorizeAttribute(), new AllowAnonymousAttribute()], false, false),
        ["AllowAnonymous with fallback"] = ([new AllowAnonymousAttribute()], true, false),
    };

    public static TheoryData<string> AuthorizationCases => [.. Authorization.Keys];

    [Theory]
    [InlineData("127.0.0.1", null, null, true)]
    [InlineData("::1", null, null, true)]
    [InlineData("::ffff:127.0.0.1", null, null, true)]
    [InlineData("10.0.0.5", null, null, false)]
    [InlineData(null, null, null, false)]
    [InlineData("127.0.0.1", "X-Forwarded-For", "1.2.3.4", false)]
    [InlineData("127.0.0.1", "Forwarded", "for=1.2.3.4", false)]
    [InlineData("127.0.0.1", "X-Real-IP", "1.2.3.4", false)]
    public void IsLocal_requires_loopback_without_proxy_headers(string? remote, string? header, string? value, bool expected)
    {
        var http = new DefaultHttpContext();
        http.Connection.RemoteIpAddress = remote is null ? null : IPAddress.Parse(remote);
        if (header is not null)
            http.Request.Headers[header] = value;

        Assert.Equal(expected, DashboardAccess.IsLocal(http));
    }

    [Theory]
    [MemberData(nameof(AuthorizationCases))]
    public void Authorization_is_detected_from_metadata_or_fallback(string name)
    {
        var (metadata, fallback, expected) = Authorization[name];

        Assert.Equal(expected, DashboardAccess.HasAuthorization(new EndpointMetadataCollection(metadata), fallback));
    }

    [Theory]
    [InlineData(false, "same-origin", null, "app.example:8443", false)]
    [InlineData(true, "same-origin", null, "app.example:8443", true)]
    [InlineData(true, "same-site", null, "app.example:8443", false)]
    [InlineData(true, "cross-site", null, "app.example:8443", false)]
    [InlineData(true, "none", null, "app.example:8443", false)]
    [InlineData(true, "cross-site", "https://app.example:8443", "app.example:8443", false)]
    [InlineData(true, null, "https://app.example:8443", "app.example:8443", true)]
    [InlineData(true, null, "https://APP.example:8443", "app.example:8443", true)]
    [InlineData(true, null, "http://app.example:8443", "app.example:8443", false)]
    [InlineData(true, null, "https://evil.example:8443", "app.example:8443", false)]
    [InlineData(true, null, "https://app.example", "app.example:8443", false)]
    [InlineData(true, null, "null", "app.example:8443", false)]
    [InlineData(true, null, null, "app.example:8443", false)]
    [InlineData(true, null, "https://app.example:443", "app.example", true)]
    public void Post_requires_htmx_header_and_same_origin(bool htmx, string? fetchSite, string? origin, string host, bool expected)
    {
        var http = new DefaultHttpContext();
        http.Request.Scheme = "https";
        http.Request.Host = new HostString(host);
        if (htmx)
            http.Request.Headers["HX-Request"] = "true";
        if (fetchSite is not null)
            http.Request.Headers["Sec-Fetch-Site"] = fetchSite;
        if (origin is not null)
            http.Request.Headers.Origin = origin;

        Assert.Equal(expected, DashboardAccess.IsAllowedPost(http.Request));
    }

    private sealed class RequirementData : IAuthorizationRequirementData
    {
        public IEnumerable<IAuthorizationRequirement> GetRequirements() => [];
    }
}
