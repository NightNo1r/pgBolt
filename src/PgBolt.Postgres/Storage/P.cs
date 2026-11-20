using Npgsql;
using NpgsqlTypes;

namespace PgBolt.Postgres.Storage;

/// <summary>Типизированные параметры (NpgsqlParameter&lt;T&gt;) без boxing.</summary>
internal static class P
{
    public static void Add<T>(NpgsqlParameterCollection ps, string name, T value) =>
        ps.Add(new NpgsqlParameter<T>(name, value));

    public static void Add<T>(NpgsqlParameterCollection ps, string name, T value, NpgsqlDbType type) =>
        ps.Add(new NpgsqlParameter<T>(name, type) { TypedValue = value });

    public static void AddNullable<T>(NpgsqlParameterCollection ps, string name, T? value, NpgsqlDbType type) where T : class =>
        ps.Add(value is null
            ? new NpgsqlParameter(name, type) { Value = DBNull.Value }
            : new NpgsqlParameter<T>(name, type) { TypedValue = value });

    public static void AddValue<T>(NpgsqlParameterCollection ps, string name, T? value, NpgsqlDbType type) where T : struct =>
        ps.Add(value is { } v
            ? new NpgsqlParameter<T>(name, type) { TypedValue = v }
            : new NpgsqlParameter(name, type) { Value = DBNull.Value });
}
