using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PgBolt.Dashboard.Tests.Support;

/// <summary>Схема «Test» по умолчанию: X-Test-User — имя, X-Test-Roles — роли через запятую; без X-Test-User — NoResult (401).</summary>
public static class TestAuthentication
{
    public const string Scheme = "Test", UserHeader = "X-Test-User", RolesHeader = "X-Test-Roles";

    public static IServiceCollection Add(IServiceCollection services, Action<AuthorizationOptions>? authorization = null)
    {
        services.AddAuthentication(Scheme).AddScheme<AuthenticationSchemeOptions, Handler>(Scheme, null);
        services.AddAuthorization(authorization ?? (_ => { }));
        return services;
    }

    private sealed class Handler(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            var user = Request.Headers[UserHeader].ToString();
            if (user.Length == 0)
                return Task.FromResult(AuthenticateResult.NoResult());
            var roles = Request.Headers[RolesHeader].ToString().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var identity = new ClaimsIdentity([new Claim(ClaimTypes.Name, user), .. roles.Select(r => new Claim(ClaimTypes.Role, r))], Scheme.Name);
            return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), Scheme.Name)));
        }
    }
}
