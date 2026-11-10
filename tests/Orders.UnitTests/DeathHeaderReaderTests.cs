using System.Text;
using Orders.Contracts;
using Orders.Worker;

namespace Orders.UnitTests;

public class DeathHeaderReaderTests
{
    private static Dictionary<string, object?> Death(string queue, string reason, long count) => new()
    {
        ["queue"] = Encoding.UTF8.GetBytes(queue),
        ["reason"] = Encoding.UTF8.GetBytes(reason),
        ["count"] = count
    };

    private static Dictionary<string, object?> HeadersWith(params Dictionary<string, object?>[] deaths) => new()
    {
        ["x-death"] = deaths.Cast<object?>().ToList()
    };

    [Fact]
    public void CountRejections_NullHeaders_ReturnsZero()
    {
        var count = DeathHeaderReader.CountRejections(null, RabbitTopology.ProcessQueue);

        Assert.Equal(0, count);
    }

    [Fact]
    public void CountRejections_NoXDeathHeader_ReturnsZero()
    {
        var headers = new Dictionary<string, object?> { ["outro"] = "valor" };

        var count = DeathHeaderReader.CountRejections(headers, RabbitTopology.ProcessQueue);

        Assert.Equal(0, count);
    }

    [Fact]
    public void CountRejections_RejectedInProcessQueue_ReturnsCount()
    {
        var headers = HeadersWith(Death(RabbitTopology.ProcessQueue, "rejected", 2));

        var count = DeathHeaderReader.CountRejections(headers, RabbitTopology.ProcessQueue);

        Assert.Equal(2, count);
    }

    [Fact]
    public void CountRejections_IgnoresRetryQueueExpirations()
    {
        var headers = HeadersWith(
            Death(RabbitTopology.RetryQueue, "expired", 5),
            Death(RabbitTopology.ProcessQueue, "rejected", 2));

        var count = DeathHeaderReader.CountRejections(headers, RabbitTopology.ProcessQueue);

        Assert.Equal(2, count);
    }

    [Fact]
    public void CountRejections_OnlyRetryQueueEntries_ReturnsZero()
    {
        var headers = HeadersWith(Death(RabbitTopology.RetryQueue, "expired", 2));

        var count = DeathHeaderReader.CountRejections(headers, RabbitTopology.ProcessQueue);

        Assert.Equal(0, count);
    }
}