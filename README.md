# Cloudflare Durable Objects Broadcaster

This package is a Cloudflare-native Pusher-compatible websocket server aimed at Laravel Echo and Laravel's Pusher broadcaster.

## Deploy to Cloudflare

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/BenQoder/soketi/tree/durable-object/deploy/cloudflare-durable-objects)

This button targets the `durable-object` branch because that is where the Worker currently lives in this repository. Cloudflare will provision the Durable Object binding from `wrangler.jsonc`, then prompt for the app credentials declared in `.dev.vars.example`.

The current implementation is verified locally against:

- `pusher-js` public channel subscriptions
- `pusher-js` batch event delivery
- `pusher-js` private channel subscriptions
- `laravel-echo` presence channels
- `pusher-js` authenticated-user sign in, `sendToUser`, and termination
- `pusher-js/with-encryption` encrypted private channels
- signed Pusher HTTP publish requests through the official Node `pusher` client

## Current scope

- public channels
- batch event publishing via `/batch_events`
- private channels
- presence channels
- authenticated user sign-in via `pusher:signin`
- server-to-user delivery via `sendToUser`
- user connection termination via `/users/:userId/terminate_connections`
- encrypted private channel delivery
- signed Pusher HTTP publish endpoint
- Laravel Echo presence subscriptions
- Pusher-compatible websocket handshake for `pusher-js`

## Local development

```bash
cd deploy/cloudflare-durable-objects
npm install
cp .dev.vars.example .dev.vars
npm run dev
```

If `8787` is already in use, run `npx wrangler dev --ip 127.0.0.1 --port <free-port>` instead.

## Local verification

```bash
cd deploy/cloudflare-durable-objects
npm install
npm run test:integration
```

## Remote verification

```bash
cd deploy/cloudflare-durable-objects
npm install
npm run test:remote
```

Wrangler currently warns that `wrangler dev --remote` is being superseded by remote bindings in regular `wrangler dev`, but the remote preview flow still works and is useful for validating this Worker against Cloudflare's remote runtime.

## Environment variables

Use either:

- `PUSHER_APP_ID`, `PUSHER_APP_KEY`, `PUSHER_APP_SECRET`
- `PUSHER_ENABLE_USER_AUTHENTICATION=true` to require either `pusher:signin` or authenticated private / presence subscription within the timeout window
- `USER_AUTHENTICATION_TIMEOUT_MS=30000` to override the default auth timeout
- `APPS_JSON` for multiple apps

`APPS_JSON` example:

```json
[{"id":"app-id","key":"app-key","secret":"app-secret","enableUserAuthentication":true}]
```

## Laravel Echo example

```js
import Echo from "laravel-echo";
import Pusher from "pusher-js";

window.Pusher = Pusher;

window.Echo = new Echo({
  broadcaster: "pusher",
  key: import.meta.env.VITE_PUSHER_APP_KEY,
  cluster: "mt1",
  wsHost: "your-worker-domain.example.com",
  wssPort: 443,
  forceTLS: true,
  enabledTransports: ["ws", "wss"],
});
```

## Laravel broadcaster example

Point Laravel's Pusher config at the Worker hostname:

```env
BROADCAST_CONNECTION=pusher
PUSHER_APP_ID=app-id
PUSHER_APP_KEY=app-key
PUSHER_APP_SECRET=app-secret
PUSHER_HOST=your-worker-domain.example.com
PUSHER_PORT=443
PUSHER_SCHEME=https
PUSHER_APP_CLUSTER=mt1
```

## Not Yet Implemented

This is ready for the Laravel Echo + Laravel broadcaster path, but it is not full `soketi` parity yet. Still missing:

- operational extras like metrics, webhooks, and the broader `soketi` dashboard surface
- some wider `soketi` management endpoints and dashboard behavior outside the broadcaster/runtime core
