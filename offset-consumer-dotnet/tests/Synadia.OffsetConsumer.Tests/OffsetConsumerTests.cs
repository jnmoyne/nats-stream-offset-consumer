using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Synadia.OffsetConsumer;
using Xunit;

namespace Synadia.OffsetConsumer.Tests;

public class OffsetConsumerTests : IDisposable
{
    private const string TestStream = "test-stream";
    private const int NumMessages = 100;

    private readonly NatsServerHelper _server;

    public OffsetConsumerTests()
    {
        _server = NatsServerHelper.Start();
    }

    public void Dispose()
    {
        _server.Dispose();
    }

    private async Task<NatsConnection> ConnectAsync()
    {
        var conn = new NatsConnection(new NatsOpts { Url = _server.Url });
        await conn.ConnectAsync();
        return conn;
    }

    private async Task CreateStreamAndPublish(INatsJSContext js)
    {
        await js.CreateStreamAsync(new StreamConfig(TestStream, ["test.>"]) { Storage = StreamConfigStorage.Memory });

        for (var i = 1; i <= NumMessages; i++)
        {
            await js.PublishAsync($"test.{i}", System.Text.Encoding.UTF8.GetBytes($"msg-{i}"));
        }
    }

    private string UniqueName(string prefix) => $"{prefix}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

    [Fact]
    public async Task BasicConsume_ReceivesMessages()
    {
        await using var nc = await ConnectAsync();
        var js = new NatsJSContext(nc);
        await CreateStreamAndPublish(js);
        var name = UniqueName("test-basic");

        await using var consumer = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
        {
            Stream = TestStream,
            ConsumerName = name,
        });

        var count = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        try
        {
            await foreach (var msg in consumer.ConsumeAsync(cts.Token))
            {
                count++;
                if (count >= NumMessages)
                    break;
            }
        }
        catch (OperationCanceledException) { }

        Assert.True(count > 0, "Expected to receive messages, got 0");
    }

    [Fact]
    public async Task CommitSyncAndResume_ResumesFromCommittedPosition()
    {
        await using var nc = await ConnectAsync();
        var js = new NatsJSContext(nc);
        await CreateStreamAndPublish(js);
        var name = UniqueName("test-resume");

        // Phase 1: consume and commit
        ulong committedSeq = 0;
        {
            await using var c1 = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
            {
                Stream = TestStream,
                ConsumerName = name,
            });

            using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await foreach (var msg in c1.ConsumeAsync(cts1.Token))
                {
                    if (msg.Metadata is { } md)
                        committedSeq = md.Sequence.Stream;
                    if (committedSeq >= 10)
                        break;
                }
            }
            catch (OperationCanceledException) { }

            Assert.True(committedSeq > 0, "No messages received in phase 1");
            await c1.CommitSyncAsync();
        }

        // Publish more messages for the resumed consumer
        for (var i = NumMessages + 1; i <= NumMessages + 10; i++)
        {
            await js.PublishAsync($"test.{i}", System.Text.Encoding.UTF8.GetBytes($"msg-{i}"));
        }

        // Phase 2: reopen and verify resume
        await using var c2 = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
        {
            Stream = TestStream,
            ConsumerName = name,
        });

        ulong firstResumeSeq = 0;
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await foreach (var msg in c2.ConsumeAsync(cts2.Token))
            {
                if (msg.Metadata is { } md)
                {
                    firstResumeSeq = md.Sequence.Stream;
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }

        Assert.Equal(committedSeq + 1, firstResumeSeq);
    }

    [Fact]
    public async Task CommitAsync_Succeeds()
    {
        await using var nc = await ConnectAsync();
        var js = new NatsJSContext(nc);
        await CreateStreamAndPublish(js);
        var name = UniqueName("test-async");

        await using var consumer = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
        {
            Stream = TestStream,
            ConsumerName = name,
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var count = 0;
        try
        {
            await foreach (var msg in consumer.ConsumeAsync(cts.Token))
            {
                count++;
                if (count >= 10)
                    break;
            }
        }
        catch (OperationCanceledException) { }

        Assert.True(count > 0);
        await consumer.CommitAsync();
    }

    [Fact]
    public async Task Seek_RepositionsConsumer()
    {
        await using var nc = await ConnectAsync();
        var js = new NatsJSContext(nc);
        await CreateStreamAndPublish(js);
        var name = UniqueName("test-seek");

        // Phase 1: consume, commit, dispose
        {
            await using var c1 = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
            {
                Stream = TestStream,
                ConsumerName = name,
            });

            using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await foreach (var msg in c1.ConsumeAsync(cts1.Token))
                {
                    break; // just consume one
                }
            }
            catch (OperationCanceledException) { }

            await c1.CommitSyncAsync();
        }

        // Phase 2: seek to 50
        await using var c2 = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
        {
            Stream = TestStream,
            ConsumerName = name,
        });

        await c2.SeekAsync(50);

        ulong firstSeq = 0;
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await foreach (var msg in c2.ConsumeAsync(cts2.Token))
            {
                if (msg.Metadata is { } md)
                {
                    firstSeq = md.Sequence.Stream;
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }

        Assert.Equal(51UL, firstSeq);
    }

    [Fact]
    public async Task DeleteConsumer_RemovesStoredOffset()
    {
        await using var nc = await ConnectAsync();
        var js = new NatsJSContext(nc);
        await CreateStreamAndPublish(js);
        var name = UniqueName("test-delete");

        // Create and commit
        {
            await using var c1 = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
            {
                Stream = TestStream,
                ConsumerName = name,
            });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await foreach (var msg in c1.ConsumeAsync(cts.Token))
                {
                    break;
                }
            }
            catch (OperationCanceledException) { }

            await c1.CommitSyncAsync();
        }

        // Delete
        {
            await using var c2 = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
            {
                Stream = TestStream,
                ConsumerName = name,
            });
            await c2.DeleteConsumerAsync();
        }

        // Verify starts from beginning
        await using var c3 = await OffsetConsumer.CreateAsync(js, new ConsumerConfig
        {
            Stream = TestStream,
            ConsumerName = name,
        });

        ulong firstSeq = 0;
        using var cts3 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await foreach (var msg in c3.ConsumeAsync(cts3.Token))
            {
                if (msg.Metadata is { } md)
                {
                    firstSeq = md.Sequence.Stream;
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }

        Assert.Equal(1UL, firstSeq);
    }

    [Fact]
    public async Task InvalidConsumerName_ThrowsArgumentException()
    {
        await using var nc = await ConnectAsync();
        var js = new NatsJSContext(nc);
        await CreateStreamAndPublish(js);

        // Empty name
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await OffsetConsumer.CreateAsync(js, new ConsumerConfig
            {
                Stream = TestStream,
                ConsumerName = "",
            }));

        // Name with dot
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await OffsetConsumer.CreateAsync(js, new ConsumerConfig
            {
                Stream = TestStream,
                ConsumerName = "has.dot",
            }));
    }
}
