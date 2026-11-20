using PgBolt.Server.Runtime;

namespace PgBolt.Server.Tests.Unit;

/// <summary>
/// Модель цикла диспетчера поверх настоящего <see cref="SlotAllocator"/>: итерация — освобождение слотов, задачи которых
/// закончились, затем план → dequeue (<see cref="Jobs"/> задач по записи) → <see cref="SlotAllocator.Take"/> на каждую задачу.
/// Задача держит слот <see cref="Duration"/> итераций. После каждой итерации (граница плана) счётчики взятых общих слотов
/// по очередям актуальны.
/// </summary>
internal sealed class PoolSimulation
{
    private readonly List<(int Until, string Queue, bool FromReserved)> _running = [];
    private readonly Action<int>? _beforeIteration;
    private readonly SlotAllocator _slots;
    private readonly string[] _names;

    public PoolSimulation(int workers, Action<PgBoltServerOptions> queues, ISharedPoolDistribution distribution)
    {
        var o = new PgBoltServerOptions { Workers = workers };
        queues(o);
        _names = [.. o.Queues.Select(q => q.Name)];
        _slots = new SlotAllocator(o.Workers, o.Queues, distribution);
        SharedTaken = new long[_names.Length];
    }

    /// <param name="slots">Готовый allocator, очереди которого объявлены в порядке <paramref name="names"/>.</param>
    /// <param name="beforeIteration">Вызывается в начале итерации k (с 1), до освобождения слотов и плана.</param>
    public PoolSimulation(SlotAllocator slots, IReadOnlyList<string> names, Action<int>? beforeIteration = null)
    {
        _slots = slots;
        _names = [.. names];
        SharedTaken = new long[_names.Length];
        _beforeIteration = beforeIteration;
    }

    /// <summary>Сколько задач вернёт dequeue очереди по записи размера n; по умолчанию — backlog (n).</summary>
    public Func<int, int, int> Jobs { get; set; } = (_, n) => n;

    /// <summary>Готовность очереди к опросу; по умолчанию готовы все.</summary>
    public Func<int, bool> Due { get; set; } = _ => true;

    /// <summary>Сколько итераций задача держит слот.</summary>
    public int Duration { get; set; } = 1;

    public int Iteration { get; private set; }

    /// <summary>Взятые общие слоты по очередям с начала моделирования.</summary>
    public long[] SharedTaken { get; }

    /// <summary>Очереди в порядке взятия общих слотов.</summary>
    public List<int> SharedOrder { get; } = [];

    /// <summary>Записи плана последней итерации.</summary>
    public List<(int Queue, int N)> LastPlan { get; } = [];

    /// <summary>
    /// Пачечное освобождение, как у задач, стартовавших вместе: до плана освобождается только первая закончившаяся задача,
    /// остальные — после выдачи первой записи плана, до следующего Next, или в конце плана без записей.
    /// </summary>
    public bool ReleaseMidPlan { get; set; }

    public void Step()
    {
        Iteration++;
        _beforeIteration?.Invoke(Iteration);
        var finished = _running.Where(job => job.Until <= Iteration).ToList();
        _running.RemoveAll(job => job.Until <= Iteration);
        var late = ReleaseMidPlan ? finished.Skip(1).ToList() : [];
        foreach (var job in ReleaseMidPlan ? finished.Take(1) : finished)
            _slots.Release(job.Queue, job.FromReserved);

        LastPlan.Clear();
        foreach (var (name, n) in _slots.Plan(name => Due(Array.IndexOf(_names, name))))
        {
            ReleaseAll(late);
            var queue = Array.IndexOf(_names, name);
            LastPlan.Add((queue, n));
            for (var i = Math.Min(n, Jobs(queue, n)); i > 0; i--)
            {
                var fromReserved = _slots.Take(name);
                _running.Add((Iteration + Duration, name, fromReserved));
                if (!fromReserved)
                {
                    SharedTaken[queue]++;
                    SharedOrder.Add(queue);
                }
            }
        }
        ReleaseAll(late);
    }

    private void ReleaseAll(List<(int Until, string Queue, bool FromReserved)> jobs)
    {
        foreach (var job in jobs)
            _slots.Release(job.Queue, job.FromReserved);
        jobs.Clear();
    }
}
