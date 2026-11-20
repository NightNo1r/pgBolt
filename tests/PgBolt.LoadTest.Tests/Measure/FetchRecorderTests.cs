using PgBolt.LoadTest.Measure;

namespace PgBolt.LoadTest.Tests.Measure;

public sealed class FetchRecorderTests
{
    [Fact]
    public void CountsOnlyMeasurementWindow()
    {
        var recorder = new FetchRecorder();
        recorder.Fetch(TimeSpan.FromSeconds(1), 1, 1);
        recorder.Begin();
        // Первая попытка могла начаться до границы. Обе учитываются вместе с результатом вызова.
        recorder.Fetch(TimeSpan.FromMilliseconds(5), null, 2);
        recorder.Fetch(TimeSpan.FromMilliseconds(2), 0, 1);
        recorder.QueueLock(3000);
        recorder.Fetch(TimeSpan.FromMilliseconds(4), 12, 1);
        var result = recorder.End();
        Assert.Equal((3L, 1L, 1L, 4L, 1L),
            (result.Duration.Count, result.Empty, result.Failed, result.Sent, result.QueueLock.Count));
        Assert.InRange(result.Duration.P99Ms, 5, 5.01);
        Assert.InRange(result.QueueLock.P99Ms, 3, 3.01);
    }
}
