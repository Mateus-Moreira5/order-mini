using Microsoft.EntityFrameworkCore;

namespace Orders.Data;

public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(o => o.Id);
            entity.Property(o => o.Customer).HasMaxLength(200).IsRequired();
            entity.Property(o => o.Amount).HasPrecision(18, 2);
            entity.Property(o => o.Status).HasConversion<string>().HasMaxLength(20);
            entity.Property(o => o.FailureMode).HasMaxLength(20).IsRequired();
            entity.Property(o => o.LastError).HasMaxLength(1000);
        });
    }
}