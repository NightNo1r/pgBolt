using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace PgBolt.Testing;

/// <param name="Scopes">Снимок стека scope'ов на момент записи (от внешнего к внутреннему); у записей <see cref="ListLogger"/> не <c>null</c>.</param>
public sealed record LogEntry(string Category, LogLevel Level, string Message, Exception? Exception,
    EventId EventId = default, IReadOnlyList<object> Scopes = null!);

/// <summary>
/// Собирает записи лога для проверок; потокобезопасен. Годится как ILogger, ILoggerFactory и ILoggerProvider:
/// логгеры из CreateLogger пишут в те же Entries со своей категорией.
/// </summary>
public class ListLogger : ILogger, ILoggerFactory, ILoggerProvider
{
    private readonly ConcurrentQueue<LogEntry> _entries;
    private readonly ListLogger _root;
    private readonly string _category;
    private readonly ScopeState _scopes;

    public ListLogger()
    {
        _entries = new ConcurrentQueue<LogEntry>();
        _root = this;
        _category = "";
        _scopes = new ScopeState();
    }

    private ListLogger(ListLogger root, string category) => (_entries, _root, _category, _scopes) = (root._entries, root, category, root._scopes);

    public IReadOnlyCollection<LogEntry> Entries => _entries;

    /// <summary>Вызывается синхронно на каждой записи, в том числе логгеров из CreateLogger; задаётся у корневого.</summary>
    public Action<LogEntry>? OnEntry { get; set; }

    /// <summary>
    /// Вызывается синхронно в BeginScope любого логгера до записи в стек и до увеличения <see cref="OpenScopes"/>;
    /// бросающий хук моделирует логгер-провайдер, который бросает на BeginScope. Задаётся у корневого.
    /// </summary>
    public Action<object>? OnBeginScope { get; set; }

    /// <summary>
    /// Число открытых и не снятых scope'ов всех логгеров из CreateLogger. Стек scope'ов лежит в AsyncLocal, и его изменение
    /// внутри RunAsync вызывающему не возвращается, поэтому «scope закрыт» проверяется этим счётчиком.
    /// </summary>
    public int OpenScopes => Volatile.Read(ref _scopes.Open);

    public IEnumerable<LogEntry> At(LogLevel level) => _entries.Where(e => e.Level == level);

    public ILogger CreateLogger(string categoryName) => new ListLogger(_root, categoryName);

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        _root.OnBeginScope?.Invoke(state);
        var previous = _scopes.Stack.Value ?? ImmutableList<object>.Empty;
        _scopes.Stack.Value = previous.Add(state);
        Interlocked.Increment(ref _scopes.Open);
        return new Scope(_scopes, previous);
    }

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        var entry = new LogEntry(_category, logLevel, formatter(state, exception), exception, eventId,
            _scopes.Stack.Value ?? ImmutableList<object>.Empty);
        _entries.Enqueue(entry);
        _root.OnEntry?.Invoke(entry);
    }

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }

    /// <summary>Стек и счётчик scope'ов общие для корневого логгера и всех созданных из него.</summary>
    private sealed class ScopeState
    {
        public readonly AsyncLocal<ImmutableList<object>> Stack = new();
        public int Open;
    }

    private sealed class Scope(ScopeState state, ImmutableList<object> previous) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            state.Stack.Value = previous;
            Interlocked.Decrement(ref state.Open);
        }
    }
}

/// <summary>ListLogger для внедрения как ILogger&lt;T&gt;.</summary>
public sealed class ListLogger<T> : ListLogger, ILogger<T>;
