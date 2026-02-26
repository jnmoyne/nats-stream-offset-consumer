using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

namespace Synadia.OffsetConsumer;

/// <summary>
/// JSON-serializable record persisted in the offset-consumers KV bucket.
/// </summary>
internal record OffsetEntry
{
    [JsonPropertyName("seq")]
    public ulong Seq { get; init; }

    [JsonPropertyName("committed_at")]
    public string CommittedAt { get; init; } = string.Empty;
}

/// <summary>
/// Provides Kafka-style offset-based consumption on a NATS JetStream stream.
/// All public methods are thread-safe. Implements IAsyncDisposable for cleanup.
/// </summary>
public sealed class OffsetConsumer : IOffsetConsumer
{
    private const string KvBucketName = "offset-consumers";

    private readonly INatsJSContext _js;
    private readonly INatsKVStore _store;
    private readonly ConsumerConfig _config;
    private readonly string _kvKey;

    private ulong _lastRevision;
    private ulong _startSeq;
    private ulong _lastDeliveredSeq;
    private ulong _lastCommittedSeq;
    private bool _asyncCommitInFlight;
    private bool _closed;
    private bool _consuming;

    private CancellationTokenSource? _consumeCts;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private Timer? _autoCommitTimer;

    private OffsetConsumer(
        INatsJSContext js,
        INatsKVStore store,
        ConsumerConfig config,
        string kvKey,
        ulong lastRevision,
        ulong startSeq,
        ulong lastCommittedSeq)
    {
        _js = js;
        _store = store;
        _config = config;
        _kvKey = kvKey;
        _lastRevision = lastRevision;
        _startSeq = startSeq;
        _lastDeliveredSeq = startSeq;
        _lastCommittedSeq = lastCommittedSeq;
    }

    /// <summary>
    /// Creates a new offset consumer for the given stream and consumer name.
    /// Ensures the offset-consumers KV bucket exists, reads any stored offset,
    /// and resolves the start sequence. Does NOT start consuming — call ConsumeAsync() for that.
    /// </summary>
    /// <param name="js">JetStream context</param>
    /// <param name="config">Consumer configuration</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A new IOffsetConsumer instance</returns>
    /// <exception cref="ArgumentException">If ConsumerName is empty or contains '.'</exception>
    public static async Task<IOffsetConsumer> CreateAsync(
        INatsJSContext js,
        ConsumerConfig config,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(config.ConsumerName) || config.ConsumerName.Contains('.'))
        {
            throw new ArgumentException(
                "Consumer name must not be empty and must not contain '.'",
                nameof(config));
        }

        if (string.IsNullOrEmpty(config.Stream))
        {
            throw new ArgumentException(
                "Stream name is required",
                nameof(config));
        }

        // Look up the stream to verify it exists and get replica count
        var stream = await js.GetStreamAsync(config.Stream, cancellationToken: cancellationToken);
        var replicas = stream.Info.Config.NumReplicas;
        if (replicas < 1)
        {
            replicas = 1;
        }

        // Create KV context from the JS context's underlying connection
        var kvCtx = new NatsKVContext(js.Connection);

        // Ensure the offset-consumers KV bucket exists
        var store = await EnsureBucketAsync(kvCtx, replicas, cancellationToken);

        var kvKey = $"{config.Stream}.{config.ConsumerName}";

        ulong lastRevision = 0;
        ulong startSeq = 0;
        ulong lastCommittedSeq = 0;

        // Read stored offset
        try
        {
            var entry = await store.GetEntryAsync<byte[]>(kvKey, cancellationToken: cancellationToken);
            if (entry.Value is not null)
            {
                var offset = JsonSerializer.Deserialize<OffsetEntry>(entry.Value);
                if (offset is not null)
                {
                    lastRevision = entry.Revision;
                    startSeq = offset.Seq;
                    lastCommittedSeq = offset.Seq;
                }
            }
        }
        catch (NatsKVKeyNotFoundException)
        {
            // No stored offset — use configured start position
        }

        var consumer = new OffsetConsumer(js, store, config, kvKey, lastRevision, startSeq, lastCommittedSeq);

        // Start auto-commit timer if enabled
        if (config.AutoCommit)
        {
            consumer.StartAutoCommitTimer();
        }

        return consumer;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<NatsJSMsg<byte[]>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_closed)
            {
                throw new ObjectDisposedException(nameof(OffsetConsumer));
            }

            if (_consuming)
            {
                throw new InvalidOperationException("Already consuming. Cancel the previous ConsumeAsync before starting a new one.");
            }

            _consuming = true;
            _consumeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        var linkedCt = _consumeCts.Token;

        INatsJSConsumer orderedConsumer;
        try
        {
            orderedConsumer = await CreateOrderedConsumerAsync(linkedCt);
        }
        catch
        {
            await _lock.WaitAsync(CancellationToken.None);
            try
            {
                _consuming = false;
                _consumeCts?.Dispose();
                _consumeCts = null;
            }
            finally
            {
                _lock.Release();
            }

            throw;
        }

        Exception? errorToReport = null;

        try
        {
            await foreach (var msg in orderedConsumer.ConsumeAsync<byte[]>(cancellationToken: linkedCt))
            {
                if (msg.Metadata is { } metadata)
                {
                    await _lock.WaitAsync(CancellationToken.None);
                    try
                    {
                        _lastDeliveredSeq = metadata.Sequence.Stream;
                    }
                    finally
                    {
                        _lock.Release();
                    }
                }

                yield return msg;
            }
        }
        finally
        {
            await _lock.WaitAsync(CancellationToken.None);
            try
            {
                _consuming = false;
                _consumeCts?.Dispose();
                _consumeCts = null;
            }
            finally
            {
                _lock.Release();
            }

            // Report error via error handler if one occurred
            if (errorToReport is not null && _config.ErrorHandler is not null)
            {
                await _config.ErrorHandler(this, errorToReport);
            }
        }
    }

    /// <inheritdoc />
    public async Task CommitSyncAsync(CancellationToken cancellationToken = default)
    {
        ulong seq;
        ulong lastRev;

        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_closed)
            {
                throw new ObjectDisposedException(nameof(OffsetConsumer));
            }

            seq = _lastDeliveredSeq;
            lastRev = _lastRevision;
        }
        finally
        {
            _lock.Release();
        }

        if (seq == 0)
        {
            return; // nothing to commit
        }

        var data = MarshalOffset(seq);
        var newRev = await KvUpdateAsync(data, lastRev, cancellationToken);

        await _lock.WaitAsync(CancellationToken.None);
        try
        {
            _lastRevision = newRev;
            _lastCommittedSeq = seq;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_closed)
            {
                throw new ObjectDisposedException(nameof(OffsetConsumer));
            }

            if (_asyncCommitInFlight)
            {
                throw new InvalidOperationException("A previous async commit has not completed.");
            }

            _asyncCommitInFlight = true;
        }
        finally
        {
            _lock.Release();
        }

        try
        {
            await CommitSyncAsync(cancellationToken);
        }
        finally
        {
            await _lock.WaitAsync(CancellationToken.None);
            try
            {
                _asyncCommitInFlight = false;
            }
            finally
            {
                _lock.Release();
            }
        }
    }

    /// <inheritdoc />
    public async Task SeekAsync(ulong seq, CancellationToken cancellationToken = default)
    {
        CancellationTokenSource? oldCts;
        ulong lastRev;

        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_closed)
            {
                throw new ObjectDisposedException(nameof(OffsetConsumer));
            }

            oldCts = _consumeCts;
            lastRev = _lastRevision;
        }
        finally
        {
            _lock.Release();
        }

        // Stop current consumption
        if (oldCts is not null)
        {
            await oldCts.CancelAsync();
        }

        // Persist new offset via CAS
        var data = MarshalOffset(seq);
        var newRev = await KvUpdateAsync(data, lastRev, cancellationToken);

        // Update internal state
        await _lock.WaitAsync(CancellationToken.None);
        try
        {
            _lastRevision = newRev;
            _startSeq = seq;
            _lastDeliveredSeq = seq;
            _lastCommittedSeq = seq;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public Task SeekToBeginningAsync(CancellationToken cancellationToken = default)
    {
        return SeekAsync(0, cancellationToken);
    }

    /// <inheritdoc />
    public async Task SeekToEndAsync(CancellationToken cancellationToken = default)
    {
        var stream = await _js.GetStreamAsync(_config.Stream, cancellationToken: cancellationToken);
        var lastSeq = stream.Info.State.LastSeq;
        await SeekAsync(lastSeq, cancellationToken);
    }

    /// <inheritdoc />
    public async Task DeleteConsumerAsync(CancellationToken cancellationToken = default)
    {
        await _store.DeleteAsync(_kvKey, cancellationToken: cancellationToken);

        await _lock.WaitAsync(CancellationToken.None);
        try
        {
            _lastRevision = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _lock.WaitAsync(CancellationToken.None);
        try
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
        }
        finally
        {
            _lock.Release();
        }

        // Stop auto-commit timer
        if (_autoCommitTimer is not null)
        {
            await _autoCommitTimer.DisposeAsync();
            _autoCommitTimer = null;
        }

        // Stop current consumption
        if (_consumeCts is not null)
        {
            await _consumeCts.CancelAsync();
            _consumeCts.Dispose();
            _consumeCts = null;
        }

        // Final commit if auto-commit is enabled and there are uncommitted messages
        if (_config.AutoCommit && _lastDeliveredSeq > _lastCommittedSeq)
        {
            try
            {
                await CommitSyncAsync();
            }
            catch
            {
                // Best-effort final commit on dispose
            }
        }

        _lock.Dispose();
    }

    // ──────────────────────── Private Helpers ────────────────────────

    private async Task<INatsJSConsumer> CreateOrderedConsumerAsync(CancellationToken cancellationToken)
    {
        var opts = new NatsJSOrderedConsumerOpts();

        if (_startSeq > 0)
        {
            opts = opts with
            {
                DeliverPolicy = ConsumerConfigDeliverPolicy.ByStartSequence,
                OptStartSeq = _startSeq + 1,
            };
        }
        else if (_config.StartPosition == StartPosition.End)
        {
            opts = opts with
            {
                DeliverPolicy = ConsumerConfigDeliverPolicy.New,
            };
        }
        else
        {
            opts = opts with
            {
                DeliverPolicy = ConsumerConfigDeliverPolicy.All,
            };
        }

        if (_config.FilterSubjects is { Count: > 0 })
        {
            opts = opts with
            {
                FilterSubjects = _config.FilterSubjects.ToList(),
            };
        }

        return await _js.CreateOrderedConsumerAsync(
            _config.Stream,
            opts,
            cancellationToken);
    }

    private static async Task<INatsKVStore> EnsureBucketAsync(
        NatsKVContext kvCtx,
        int replicas,
        CancellationToken cancellationToken)
    {
        var kvConfig = new NatsKVConfig(KvBucketName)
        {
            History = 1,
            NumberOfReplicas = replicas,
        };

        try
        {
            return await kvCtx.CreateStoreAsync(kvConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.ErrCode == 10058)
        {
            // Bucket already exists — get the existing store
            return await kvCtx.GetStoreAsync(KvBucketName, cancellationToken);
        }
    }

    private static byte[] MarshalOffset(ulong seq)
    {
        var entry = new OffsetEntry
        {
            Seq = seq,
            CommittedAt = DateTime.UtcNow.ToString("o"),
        };
        return JsonSerializer.SerializeToUtf8Bytes(entry);
    }

    /// <summary>
    /// Performs a CAS update on the KV store. If lastRevision is 0, creates the key;
    /// otherwise updates with the expected revision.
    /// Returns the new revision on success, or throws CASConflictException on mismatch.
    /// </summary>
    private async Task<ulong> KvUpdateAsync(byte[] value, ulong lastRevision, CancellationToken cancellationToken)
    {
        try
        {
            if (lastRevision == 0)
            {
                return await _store.CreateAsync(_kvKey, value, cancellationToken);
            }
            else
            {
                return await _store.UpdateAsync(_kvKey, value, lastRevision, cancellationToken);
            }
        }
        catch (NatsKVWrongLastRevisionException ex)
        {
            throw new CASConflictException(ex);
        }
        catch (NatsKVCreateException ex)
        {
            throw new CASConflictException(ex);
        }
    }

    private void StartAutoCommitTimer()
    {
        _autoCommitTimer = new Timer(
            async _ =>
            {
                try
                {
                    await _lock.WaitAsync(CancellationToken.None);
                    bool shouldCommit;
                    try
                    {
                        shouldCommit = !_closed && _lastDeliveredSeq > _lastCommittedSeq;
                    }
                    finally
                    {
                        _lock.Release();
                    }

                    if (shouldCommit)
                    {
                        await CommitSyncAsync();
                    }
                }
                catch (Exception ex)
                {
                    if (_config.ErrorHandler is not null)
                    {
                        await _config.ErrorHandler(this, ex);
                    }
                }
            },
            null,
            _config.AutoCommitInterval,
            _config.AutoCommitInterval);
    }
}
