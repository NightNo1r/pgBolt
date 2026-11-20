namespace PgBolt.LoadTest.Tests;

public sealed class ProgramTests
{
    [Fact]
    public async Task Parse_error_exits_with_code_2()
    {
        var originalError = Console.Error;
        var stderr = new StringWriter();
        Console.SetError(stderr);
        try
        {
            var code = await Program.RunAsync(["sustained", "--hold-xmin", "1m"], TestContext.Current.CancellationToken);

            Assert.Equal(2, code);
            Assert.Contains("--hold-xmin", stderr.ToString());
        }
        finally
        {
            Console.SetError(originalError);
        }
    }
}
