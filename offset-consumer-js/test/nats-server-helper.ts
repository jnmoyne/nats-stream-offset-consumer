import { spawn, ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as net from "node:net";

/**
 * Helper to start and manage a JetStream-enabled nats-server for tests.
 */
export interface NatsServer {
  port: number;
  url: string;
  stop: () => Promise<void>;
}

function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address() as net.AddressInfo;
      const port = addr.port;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

function waitForPort(port: number, timeoutMs = 10000): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;

    function tryConnect() {
      const sock = new net.Socket();
      sock.once("connect", () => {
        sock.destroy();
        resolve();
      });
      sock.once("error", () => {
        sock.destroy();
        if (Date.now() > deadline) {
          reject(new Error(`nats-server did not start within ${timeoutMs}ms`));
        } else {
          setTimeout(tryConnect, 100);
        }
      });
      sock.connect(port, "127.0.0.1");
    }

    tryConnect();
  });
}

export async function startNatsServer(): Promise<NatsServer> {
  const port = await getFreePort();
  const storeDir = mkdtempSync(join(tmpdir(), "nats-test-"));

  const proc: ChildProcess = spawn("nats-server", [
    "-p", String(port),
    "-js",
    "-sd", storeDir,
  ], {
    stdio: ["ignore", "ignore", "ignore"],
  });

  // Wait for it to be ready
  await waitForPort(port);

  return {
    port,
    url: `nats://127.0.0.1:${port}`,
    stop: async () => {
      proc.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        proc.once("exit", () => resolve());
        setTimeout(() => {
          proc.kill("SIGKILL");
          resolve();
        }, 5000);
      });
      try {
        rmSync(storeDir, { recursive: true, force: true });
      } catch {
        // best effort
      }
    },
  };
}
