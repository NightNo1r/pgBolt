using System.Diagnostics;
using Xunit;

namespace PgBolt.Testing;

/// <summary>
/// Ожидание условия, которое выставит асинхронное продолжение после <c>FakeTimeProvider.Advance</c> или завершения
/// задачи фейка. Единственное допустимое реальное ожидание в unit-тестах: опрос раз в 1 мс, по умолчанию не дольше 5 с.
/// Логика runtime от него не зависит — время тестов по-прежнему двигает только FakeTimeProvider.
/// </summary>
public static class Eventually
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

    public static async Task True(Func<bool> condition, string because, TimeSpan? timeout = null)
    {
        var limit = timeout ?? DefaultTimeout;
        var watch = Stopwatch.StartNew();
        while (!condition())
        {
            if (watch.Elapsed > limit)
                Assert.Fail($"Condition was not met within {limit.TotalSeconds} s: {because}");
            await Task.Delay(1, TestContext.Current.CancellationToken);
        }
    }
}
