using System.Text;

namespace Orders.Worker;

public static class DeathHeaderReader
{
    public static long CountRejections(IDictionary<string, object?>? headers, string queue)
    {
        if (headers is null ||
            !headers.TryGetValue("x-death", out var raw) ||
            raw is not IList<object?> deaths)
        {
            return 0;
        }

        foreach (var item in deaths)
        {
            if (item is not IDictionary<string, object?> death)
            {
                continue;
            }

            var deathQueue = Encoding.UTF8.GetString((byte[])death["queue"]!);
            var reason = Encoding.UTF8.GetString((byte[])death["reason"]!);

            if (deathQueue == queue && reason == "rejected")
            {
                return (long)death["count"]!;
            }
        }

        return 0;
    }
}