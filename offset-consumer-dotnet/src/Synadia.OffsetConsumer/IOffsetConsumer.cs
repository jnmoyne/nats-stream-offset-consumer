using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Synadia.OffsetConsumer;

public interface IOffsetConsumer : IAsyncDisposable
{
    IAsyncEnumerable<INatsJSMsg<byte[]>> ConsumeAsync(CancellationToken cancellationToken = default);
    Task CommitSyncAsync(CancellationToken cancellationToken = default);
    Task CommitAsync(CancellationToken cancellationToken = default);
    Task SeekAsync(ulong seq, CancellationToken cancellationToken = default);
    Task SeekToBeginningAsync(CancellationToken cancellationToken = default);
    Task SeekToEndAsync(CancellationToken cancellationToken = default);
    Task DeleteConsumerAsync(CancellationToken cancellationToken = default);
}
