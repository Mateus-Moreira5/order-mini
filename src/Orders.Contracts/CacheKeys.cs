using System;

namespace Orders.Contracts;

public static class CacheKeys
{
    public static string Order(Guid id) => $"order:{id}";
}