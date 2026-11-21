using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace PgBolt.Generators.Analysis
{
    internal static class Diagnostics
    {
        private const string Category = "PgBolt";

        public static readonly DiagnosticDescriptor MissingJsonType = new(
            "BOLT001", "Job arguments type is missing in the JSON context",
            "Job arguments type '{0}' is not declared in JSON context '{1}'; add [JsonSerializable(typeof({0}))]",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor DuplicateName = new(
            "BOLT002", "Duplicate job name",
            "Job name '{0}' is declared by more than one type in this compilation",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true, customTags: WellKnownDiagnosticTags.CompilationEnd);

        public static readonly DiagnosticDescriptor ArgsNotJob = new(
            "BOLT003", "Job executor arguments type is not a [Job]",
            "'{0}' implements IJob<{1}>, but '{1}' is not marked with [Job]",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor MultipleExecutors = new(
            "BOLT004", "Multiple executors for one job",
            "Arguments type '{0}' has more than one executor in this assembly",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true, customTags: WellKnownDiagnosticTags.CompilationEnd);

        public static readonly DiagnosticDescriptor InvalidName = new(
            "BOLT005", "Invalid job name",
            "Invalid job name '{0}': allowed characters are [a-z0-9._-], length 1-128",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor MissingJsonContext = new(
            "BOLT006", "Missing PgBolt JSON context",
            "This assembly has job executors or enqueue calls but no [assembly: PgBoltJsonContext(typeof(...))]",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true, customTags: WellKnownDiagnosticTags.CompilationEnd);

        /// <summary>Largest TimeoutSeconds supported by .NET timers (2³² − 2 ms, or 4294967.294 s).</summary>
        public const int MaxTimeoutSeconds = 4294967;

        public static readonly DiagnosticDescriptor InvalidTimeout = new(
            "BOLT007", "Invalid job timeout",
            "TimeoutSeconds must be >= 0 (0 = no timeout), but is {0}",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor TimeoutAboveLimit = new(
            "BOLT008", "Job timeout exceeds the runtime limit",
            "TimeoutSeconds {0} exceeds the .NET timer limit of 4294967 s (about 49.7 days); the server caps the job timeout at this limit",
            Category, DiagnosticSeverity.Warning, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor WorkflowNotSubmitted = new(
            "BOLT009", "Workflow is never submitted",
            "The workflow created here is never submitted: none of its steps reaches SubmitAsync in this method, is returned, passed as an argument or stored in a field or property",
            Category, DiagnosticSeverity.Warning, isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor ConflictingQueueSetting = new(
            "BOLT010", "Conflicting queue setting",
            "Queue '{0}' sets {1} = {2} here and {3} at another call",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true, customTags: WellKnownDiagnosticTags.CompilationEnd);

        public static readonly DiagnosticDescriptor InvalidOptionValue = new(
            "BOLT011", "Invalid option value",
            "{0} = {1} is invalid: {2}",
            Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

        public static readonly ImmutableArray<DiagnosticDescriptor> All = ImmutableArray.Create(
            MissingJsonType, DuplicateName, ArgsNotJob, MultipleExecutors, InvalidName, MissingJsonContext, InvalidTimeout,
            TimeoutAboveLimit, WorkflowNotSubmitted, ConflictingQueueSetting, InvalidOptionValue);
    }
}
