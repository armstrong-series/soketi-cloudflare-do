import { spawn } from "node:child_process";
import { createHmac } from "node:crypto";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";

import Echo from "laravel-echo";
import Pusher from "pusher";
import PusherJS from "pusher-js";

const cwd = new URL("..", import.meta.url);
const appId = "app-id";
const appKey = "app-key";
const appSecret = "app-secret";
const encryptionMasterKeyBase64 = "nxzvbGF+f8FGhk/jOaZvgMle1tqxzF/VfUZLBLhhaH0=";
const remoteMode = process.env.WRANGLER_REMOTE === "1";

globalThis.self ??= globalThis;

const PusherJSEncrypted = await import(
  "pusher-js/worker/with-encryption/index.js"
);

const PusherBrowserClient =
  PusherJS.Pusher ?? PusherJS.default?.Pusher ?? PusherJS;
const PusherEncryptedBrowserClient =
  PusherJSEncrypted.default ??
  PusherJSEncrypted.Pusher ??
  PusherJSEncrypted.default?.Pusher ??
  PusherJSEncrypted;

globalThis.Pusher = PusherBrowserClient;

function findOpenPort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();

    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => {
          reject(new Error("Could not determine a free local port."));
        });
        return;
      }

      server.close(error => {
        if (error) {
          reject(error);
          return;
        }

        resolve(address.port);
      });
    });
  });
}

function sign(data) {
  return `${appKey}:${createHmac("sha256", appSecret).update(data).digest("hex")}`;
}

function newBackend() {
  return new Pusher({
    appId,
    key: appKey,
    secret: appSecret,
    host: runtime.wsHost,
    port: runtime.wsPort,
    useTLS: false,
    encryptionMasterKeyBase64,
  });
}

function baseClientOptions(options = {}) {
  return {
    cluster: "mt1",
    wsHost: runtime.wsHost,
    wsPort: runtime.wsPort,
    httpHost: runtime.wsHost,
    httpPort: runtime.wsPort,
    forceTLS: false,
    enabledTransports: ["ws"],
    disableStats: true,
    ...options,
  };
}

function newClient(options = {}) {
  return new PusherBrowserClient(appKey, baseClientOptions(options));
}

function newEncryptedClient(options = {}) {
  return new PusherEncryptedBrowserClient(
    appKey,
    baseClientOptions({
      encryptionMasterKeyBase64,
      ...options,
    }),
  );
}

async function waitForReady() {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    try {
      const response = await fetch(`${runtime.baseUrl}/ready`);
      if (response.ok) {
        return;
      }
    } catch {
      //
    }

    await delay(1000);
  }

  throw new Error("Timed out waiting for wrangler dev to become ready.");
}

async function withTimeout(promise, label, ms = 15000) {
  const timeout = delay(ms).then(() => {
    throw new Error(`${label} timed out after ${ms}ms`);
  });

  return Promise.race([promise, timeout]);
}

async function testPublicChannel() {
  const backend = newBackend();
  const client = newClient();

  await withTimeout(
    new Promise((resolve, reject) => {
      client.connection.bind("error", reject);
      client.connection.bind("connected", () => {
        const channel = client.subscribe("orders");

        channel.bind("pusher:subscription_succeeded", async () => {
          try {
            await backend.trigger("orders", "greeting", { message: "hello" });
          } catch (error) {
            reject(error);
          }
        });

        channel.bind("greeting", data => {
          if (data.message === "hello") {
            client.disconnect();
            resolve(undefined);
          }
        });
      });
    }),
    "public channel test",
  );
}

async function testBatchEvents() {
  const backend = newBackend();
  const client = newClient();
  const channelName = "orders-batch";

  await withTimeout(
    new Promise((resolve, reject) => {
      let receivedMessages = 0;

      client.connection.bind("error", reject);
      client.connection.bind("connected", () => {
        const channel = client.subscribe(channelName);

        channel.bind("pusher:subscription_succeeded", async () => {
          try {
            await backend.triggerBatch([
              {
                name: "greeting",
                channel: channelName,
                data: { message: "hello", weirdVariable: "abc/d" },
              },
              {
                name: "greeting",
                channel: channelName,
                data: { message: "hello", weirdVariable: "abc/d" },
              },
              {
                name: "greeting",
                channel: channelName,
                data: { message: "hello", weirdVariable: "abc/d" },
              },
            ]);
          } catch (error) {
            reject(error);
          }
        });

        channel.bind("greeting", data => {
          if (data.message !== "hello" || data.weirdVariable !== "abc/d") {
            reject(new Error(`Unexpected batch event payload: ${JSON.stringify(data)}`));
            return;
          }

          receivedMessages += 1;
          if (receivedMessages === 3) {
            client.disconnect();
            resolve(undefined);
          }
        });
      });
    }),
    "batch events test",
  );
}

async function testPrivateChannel() {
  const backend = newBackend();
  const client = newClient({
    authorizer: channel => ({
      authorize(socketId, callback) {
        callback(false, backend.authorizeChannel(socketId, channel.name));
      },
    }),
  });

  await withTimeout(
    new Promise((resolve, reject) => {
      client.connection.bind("error", reject);
      client.connection.bind("connected", () => {
        const channel = client.subscribe("private-orders");

        channel.bind("pusher:subscription_succeeded", async () => {
          try {
            await backend.trigger("private-orders", "private-greeting", {
              message: "secret hello",
            });
          } catch (error) {
            reject(error);
          }
        });

        channel.bind("private-greeting", data => {
          if (data.message === "secret hello") {
            client.disconnect();
            resolve(undefined);
          }
        });
      });
    }),
    "private channel test",
  );
}

async function testEchoPresence() {
  const backend = newBackend();

  const echo = new Echo({
    broadcaster: "pusher",
    key: appKey,
    Pusher: PusherBrowserClient,
    cluster: "mt1",
    wsHost: runtime.wsHost,
    wsPort: runtime.wsPort,
    forceTLS: false,
    enabledTransports: ["ws"],
    disableStats: true,
    authorizer: channel => ({
      authorize(socketId, callback) {
        const member = {
          user_id: 1,
          user_info: {
            id: 1,
            name: "John",
          },
        };

        callback(
          false,
          backend.authorizeChannel(socketId, channel.name, member),
        );
      },
    }),
  });

  await withTimeout(
    new Promise((resolve, reject) => {
      let sawHere = false;

      echo
        .join("chat.room-1")
        .here(users => {
          if (users.length !== 1 || users[0].name !== "John") {
            reject(new Error(`Unexpected presence snapshot: ${JSON.stringify(users)}`));
            return;
          }

          sawHere = true;

          backend
            .trigger("presence-chat.room-1", "chat.message", { message: "echo hello" })
            .catch(reject);
        })
        .listen(".chat.message", event => {
          if (!sawHere) {
            reject(new Error("Received event before presence snapshot."));
            return;
          }

          if (event.message === "echo hello") {
            echo.leave("chat.room-1");
            resolve(undefined);
          }
        });
    }),
    "Laravel Echo presence test",
    20000,
  );
}

async function testEncryptedPrivateChannel() {
  const backend = newBackend();
  const client = newEncryptedClient({
    authorizer: channel => ({
      authorize(socketId, callback) {
        callback(false, backend.authorizeChannel(socketId, channel.name));
      },
    }),
  });
  const channelName = "private-encrypted-orders";

  await withTimeout(
    new Promise((resolve, reject) => {
      client.connection.bind("error", reject);
      client.connection.bind("connected", () => {
        const channel = client.subscribe(channelName);

        channel.bind("pusher:subscription_succeeded", async () => {
          try {
            await backend.trigger(channelName, "encrypted-greeting", {
              message: "encrypted hello",
            });
          } catch (error) {
            reject(error);
          }
        });

        channel.bind("encrypted-greeting", data => {
          if (data.message === "encrypted hello") {
            client.disconnect();
            resolve(undefined);
          }
        });
      });
    }),
    "encrypted private channel test",
    20000,
  );
}

async function testAuthenticatedUserEvents() {
  const backend = newBackend();
  const client = newClient({
    userAuthentication: {
      customHandler({ socketId }, callback) {
        callback(
          false,
          backend.authenticateUser(socketId, {
            id: "1",
            name: "John",
          }),
        );
      },
    },
  });

  await withTimeout(
    new Promise((resolve, reject) => {
      let delivered = false;

      client.connection.bind("error", reject);
      client.user.bind("my-event", data => {
        if (data.works !== true) {
          reject(new Error(`Unexpected user event payload: ${JSON.stringify(data)}`));
          return;
        }

        delivered = true;
        client.disconnect();
        resolve(undefined);
      });

      client.connection.bind("message", async payload => {
        if (
          payload.event === "pusher_internal:subscription_succeeded" &&
          payload.channel === "#server-to-user-1" &&
          !delivered
        ) {
          try {
            await backend.sendToUser("1", "my-event", { works: true });
          } catch (error) {
            reject(error);
          }
        }
      });

      client.connection.bind("connected", () => {
        client.signin();
      });
    }),
    "authenticated user event test",
    20000,
  );
}

async function testTerminateUserConnections() {
  const backend = newBackend();
  const client = newClient({
    userAuthentication: {
      customHandler({ socketId }, callback) {
        callback(
          false,
          backend.authenticateUser(socketId, {
            id: "1",
            name: "John",
          }),
        );
      },
    },
  });

  await withTimeout(
    new Promise((resolve, reject) => {
      let sawExpectedDisconnect = false;

      client.connection.bind("error", error => {
        if (error?.data?.code === 4009) {
          sawExpectedDisconnect = true;
          client.disconnect();
          resolve(undefined);
          return;
        }

        reject(error);
      });

      client.connection.bind("message", async payload => {
        if (payload.event === "pusher:signin_success") {
          try {
            await backend.terminateUserConnections("1");
          } catch (error) {
            reject(error);
          }
        }

        if (payload.event === "pusher:error" && payload.data?.code === 4009) {
          sawExpectedDisconnect = true;
        }
      });

      client.connection.bind("connected", () => {
        client.signin();
      });
    }),
    "terminate user connections test",
    20000,
  );
}

async function main() {
  const port = await findOpenPort();
  runtime.baseUrl = `http://127.0.0.1:${port}`;
  runtime.wsHost = "127.0.0.1";
  runtime.wsPort = port;
  const wranglerArgs = [
    "wrangler",
    "dev",
    "--ip",
    "127.0.0.1",
    "--port",
    String(port),
  ];

  if (remoteMode) {
    wranglerArgs.push("--remote");
  }

  wranglerArgs.push(
    "--var",
    `PUSHER_APP_ID:${appId}`,
    "--var",
    `PUSHER_APP_KEY:${appKey}`,
    "--var",
    `PUSHER_APP_SECRET:${appSecret}`,
    "--var",
    "PUSHER_ENABLE_USER_AUTHENTICATION:true",
  );

  const child = spawn(
    process.platform === "win32" ? "npx.cmd" : "npx",
    wranglerArgs,
    {
      cwd,
      stdio: "pipe",
      env: process.env,
    },
  );

  child.stdout.on("data", chunk => {
    process.stdout.write(chunk);
  });

  child.stderr.on("data", chunk => {
    process.stderr.write(chunk);
  });

  try {
    console.log(
      `Running integration checks in ${remoteMode ? "remote" : "local"} Wrangler mode...`,
    );
    await waitForReady();
    await testPublicChannel();
    await testBatchEvents();
    await testPrivateChannel();
    await testEchoPresence();
    await testEncryptedPrivateChannel();
    await testAuthenticatedUserEvents();
    await testTerminateUserConnections();
    console.log("All integration checks passed.");
  } finally {
    child.kill("SIGTERM");
    await delay(1000);
    if (!child.killed) {
      child.kill("SIGKILL");
    }
  }
}

const runtime = {
  baseUrl: "",
  wsHost: "",
  wsPort: 0,
};

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
