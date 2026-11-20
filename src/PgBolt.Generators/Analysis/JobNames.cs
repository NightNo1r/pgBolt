namespace PgBolt.Generators.Analysis
{
    internal static class JobNames
    {
        public static bool IsValid(string? name)
        {
            if (name is null || name.Length is 0 or > 128)
                return false;
            foreach (var c in name)
                if (!(c is >= 'a' and <= 'z' || c is >= '0' and <= '9' || c is '.' or '_' or '-'))
                    return false;
            return true;
        }
    }
}
