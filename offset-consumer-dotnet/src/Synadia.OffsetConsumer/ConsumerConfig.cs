namespace Synadia.OffsetConsumer;

public record ConsumerConfig
{
    public required string Stream { get; init; }
    public required string ConsumerName { get; init; }
    public bool AutoCommit { get; init; } = false;
    public TimeSpan AutoCommitInterval { get; init; } = TimeSpan.FromSeconds(5);
    public IReadOnlyList<string>? FilterSubjects { get; init; } = null;
    public Func<IOffsetConsumer, Exception, Task>? ErrorHandler { get; init; } = null;
    public StartPosition StartPosition { get; init; } = StartPosition.Beginning;
}
