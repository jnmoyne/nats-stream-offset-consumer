using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Synadia.OffsetConsumer.Tests;

/// <summary>
/// Manages a nats-server process for integration tests.
/// Starts a JetStream-enabled nats-server on a random port.
/// </summary>
public sealed class NatsServerHelper : IDisposable
{
    private readonly Process _process;
    private readonly string _storeDir;

    public int Port { get; }
    public string Url => $"nats://localhost:{Port}";

    private NatsServerHelper(Process process, int port, string storeDir)
    {
        _process = process;
        Port = port;
        _storeDir = storeDir;
    }

    public static NatsServerHelper Start()
    {
        var port = GetFreePort();
        var storeDir = Path.Combine(Path.GetTempPath(), $"nats-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(storeDir);

        var psi = new ProcessStartInfo
        {
            FileName = "nats-server",
            Arguments = $"-p {port} -js -sd {storeDir}",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };

        var process = Process.Start(psi)
            ?? throw new InvalidOperationException("Failed to start nats-server");

        // Wait for server to be ready
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var tcp = new TcpClient();
                tcp.Connect("127.0.0.1", port);
                break;
            }
            catch
            {
                Thread.Sleep(100);
            }
        }

        return new NatsServerHelper(process, port, storeDir);
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public void Dispose()
    {
        try
        {
            if (!_process.HasExited)
            {
                _process.Kill();
                _process.WaitForExit(5000);
            }
            _process.Dispose();
        }
        catch
        {
            // best effort
        }

        try
        {
            if (Directory.Exists(_storeDir))
            {
                Directory.Delete(_storeDir, true);
            }
        }
        catch
        {
            // best effort
        }
    }
}
