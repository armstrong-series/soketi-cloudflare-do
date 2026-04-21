import { DurableObject } from "cloudflare:workers";
import { createHash, createHmac } from "node:crypto";

type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

interface Env {
  APP_GATEWAY: DurableObjectNamespace<AppGateway>;
  APPS_JSON?: string;
  PUSHER_APP_ID?: string;
  PUSHER_APP_KEY?: string;
  PUSHER_APP_SECRET?: string;
  PUSHER_ENABLE_USER_AUTHENTICATION?: string;
  SOKETI_DEFAULT_APP_ID?: string;
  SOKETI_DEFAULT_APP_KEY?: string;
  SOKETI_DEFAULT_APP_SECRET?: string;
  SOKETI_DEFAULT_ENABLE_USER_AUTHENTICATION?: string;
  USER_AUTHENTICATION_TIMEOUT_MS?: string;
}

interface AppConfig {
  id: string;
  key: string;
  secret: string;
  enabled: boolean;
  enableClientMessages: boolean;
  enableUserAuthentication: boolean;
  maxConnections: number;
  maxPresenceMembersPerChannel: number;
  maxPresenceMemberSizeInKb: number;
  maxChannelNameLength: number;
  maxEventChannelsAtOnce: number;
  maxEventNameLength: number;
  maxEventPayloadInKb: number;
  maxEventBatchSize: number;
}

interface PresenceMember {
  user_id: string | number;
  user_info: Record<string, JsonValue>;
}

interface AuthenticatedUser {
  id: string;
  [key: string]: JsonValue;
}

interface SocketAttachment {
  socketId: string;
  subscribedChannels: string[];
  presence: Record<string, PresenceMember>;
  user: AuthenticatedUser | null;
  connectedAt: number;
  authDeadlineAt: number | null;
}

interface SocketRecord {
  socketId: string;
  websocket: WebSocket;
  subscribedChannels: Set<string>;
  presence: Map<string, PresenceMember>;
  user: AuthenticatedUser | null;
  connectedAt: number;
  authDeadlineAt: number | null;
}

interface PusherIncomingMessage {
  event: string;
  channel?: string;
  data?: any;
}

interface PusherBackendMessage {
  name: string;
  data: any;
  channel?: string;
  channels?: string[];
  socket_id?: string;
}

interface BatchEventsRequest {
  batch: PusherBackendMessage[];
}

const APP_ID_HEADER = "x-cf-pusher-app-id";
const VERIFIED_HEADER = "x-cf-pusher-verified";
const DEFAULT_USER_AUTHENTICATION_TIMEOUT_MS = 30_000;
const SERVER_TO_USER_PREFIX = "#server-to-user-";

function jsonResponse(
  data: unknown,
  status = 200,
  extraHeaders: HeadersInit = {},
): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "*",
      "content-type": "application/json",
      ...extraHeaders,
    },
  });
}

function textResponse(data: string, status = 200): Response {
  return new Response(data, {
    status,
    headers: {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "*",
      "content-type": "text/plain; charset=utf-8",
    },
  });
}

function withAppHeader(request: Request, appId: string, verified = false): Request {
  const headers = new Headers(request.headers);
  headers.set(APP_ID_HEADER, appId);

  if (verified) {
    headers.set(VERIFIED_HEADER, "1");
  }

  return new Request(request, { headers });
}

function parseBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    if (value === "true") {
      return true;
    }

    if (value === "false") {
      return false;
    }
  }

  return fallback;
}

function parseNumber(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function normalizeApp(raw: Partial<AppConfig> & Record<string, unknown>): AppConfig {
  return {
    id: String(raw.id ?? raw.AppId ?? "app-id"),
    key: String(raw.key ?? raw.AppKey ?? "app-key"),
    secret: String(raw.secret ?? raw.AppSecret ?? "app-secret"),
    enabled: parseBoolean(raw.enabled ?? raw.Enabled, true),
    enableClientMessages: parseBoolean(
      raw.enableClientMessages ?? raw.EnableClientMessages,
      false,
    ),
    enableUserAuthentication: parseBoolean(
      raw.enableUserAuthentication ??
        raw.EnableUserAuthentication ??
        raw.enable_user_authentication,
      false,
    ),
    maxConnections: parseNumber(raw.maxConnections ?? raw.MaxConnections, -1),
    maxPresenceMembersPerChannel: parseNumber(
      raw.maxPresenceMembersPerChannel ?? raw.MaxPresenceMembersPerChannel,
      100,
    ),
    maxPresenceMemberSizeInKb: parseNumber(
      raw.maxPresenceMemberSizeInKb ?? raw.MaxPresenceMemberSizeInKb,
      2,
    ),
    maxChannelNameLength: parseNumber(
      raw.maxChannelNameLength ?? raw.MaxChannelNameLength,
      200,
    ),
    maxEventChannelsAtOnce: parseNumber(
      raw.maxEventChannelsAtOnce ?? raw.MaxEventChannelsAtOnce,
      100,
    ),
    maxEventNameLength: parseNumber(
      raw.maxEventNameLength ?? raw.MaxEventNameLength,
      200,
    ),
    maxEventPayloadInKb: parseNumber(
      raw.maxEventPayloadInKb ?? raw.MaxEventPayloadInKb,
      100,
    ),
    maxEventBatchSize: parseNumber(
      raw.maxEventBatchSize ?? raw.MaxEventBatchSize,
      10,
    ),
  };
}

function loadApps(env: Env): AppConfig[] {
  const apps: AppConfig[] = [];

  if (env.APPS_JSON) {
    try {
      const parsed = JSON.parse(env.APPS_JSON) as unknown;
      if (Array.isArray(parsed)) {
        apps.push(...parsed.map(app => normalizeApp(app as Record<string, unknown>)));
      }
    } catch (error) {
      console.error("Failed to parse APPS_JSON", error);
    }
  }

  if (env.PUSHER_APP_ID && env.PUSHER_APP_KEY && env.PUSHER_APP_SECRET) {
    apps.push(
      normalizeApp({
        id: env.PUSHER_APP_ID,
        key: env.PUSHER_APP_KEY,
        secret: env.PUSHER_APP_SECRET,
        enableUserAuthentication: env.PUSHER_ENABLE_USER_AUTHENTICATION,
      }),
    );
  }

  if (
    env.SOKETI_DEFAULT_APP_ID &&
    env.SOKETI_DEFAULT_APP_KEY &&
    env.SOKETI_DEFAULT_APP_SECRET
  ) {
    apps.push(
      normalizeApp({
        id: env.SOKETI_DEFAULT_APP_ID,
        key: env.SOKETI_DEFAULT_APP_KEY,
        secret: env.SOKETI_DEFAULT_APP_SECRET,
        enableUserAuthentication: env.SOKETI_DEFAULT_ENABLE_USER_AUTHENTICATION,
      }),
    );
  }

  const deduped = new Map<string, AppConfig>();
  for (const app of apps) {
    deduped.set(app.id, app);
  }

  return [...deduped.values()];
}

function findAppById(env: Env, appId: string): AppConfig | null {
  return loadApps(env).find(app => app.id === appId) ?? null;
}

function findAppByKey(env: Env, key: string): AppConfig | null {
  return loadApps(env).find(app => app.key === key) ?? null;
}

function dataToBytes(data: unknown): number {
  const serialized = typeof data === "string" ? data : JSON.stringify(data);
  return new TextEncoder().encode(serialized).length;
}

function dataToKilobytes(data: unknown): number {
  return dataToBytes(data) / 1024;
}

function md5(value: string): string {
  return createHash("md5").update(value).digest("hex");
}

function sign(value: string, secret: string): string {
  return createHmac("sha256", secret).update(value).digest("hex");
}

function pusherAuth(app: AppConfig, value: string): string {
  return `${app.key}:${sign(value, app.secret)}`;
}

function orderedQueryString(
  searchParams: URLSearchParams,
  rawBody: string,
): string {
  const entries: Array<[string, string]> = [];

  for (const [key, value] of searchParams.entries()) {
    if (key === "auth_signature" || key === "body_md5") {
      continue;
    }

    entries.push([key, value]);
  }

  if (rawBody.length > 0 || searchParams.has("body_md5")) {
    entries.push(["body_md5", md5(rawBody)]);
  }

  entries.sort(([aKey, aValue], [bKey, bValue]) => {
    if (aKey === bKey) {
      return aValue.localeCompare(bValue);
    }

    return aKey.localeCompare(bKey);
  });

  return entries
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
}

async function verifyApiRequest(request: Request, app: AppConfig): Promise<boolean> {
  const url = new URL(request.url);
  const rawBody = request.method === "GET" ? "" : await request.clone().text();
  const authSignature = url.searchParams.get("auth_signature");
  const authKey = url.searchParams.get("auth_key");

  if (!authSignature || authKey !== app.key) {
    return false;
  }

  const expected = sign(
    `${request.method.toUpperCase()}\n${url.pathname}\n${orderedQueryString(url.searchParams, rawBody)}`,
    app.secret,
  );

  return expected === authSignature;
}

function isRestrictedChannelName(name: string): boolean {
  return /^#?[-a-zA-Z0-9_=@,.;]+$/.test(name) === false;
}

function isEncryptedChannel(channel: string): boolean {
  return channel.startsWith("private-encrypted-");
}

function isPrivateChannel(channel: string): boolean {
  return (
    channel.startsWith("private-") ||
    channel.startsWith("private-encrypted-") ||
    channel.startsWith("presence-")
  );
}

function isPresenceChannel(channel: string): boolean {
  return channel.startsWith("presence-");
}

function isClientEvent(event: string): boolean {
  return /^client-/.test(event);
}

function isServerToUserChannel(channel: string): boolean {
  return channel.startsWith(SERVER_TO_USER_PREFIX);
}

function serverToUserChannelFor(userId: string): string {
  return `${SERVER_TO_USER_PREFIX}${userId}`;
}

function userIdFromServerToUserChannel(channel: string): string | null {
  if (!isServerToUserChannel(channel)) {
    return null;
  }

  const userId = channel.slice(SERVER_TO_USER_PREFIX.length);
  return userId.length > 0 ? userId : null;
}

function generateSocketId(): string {
  const max = 10_000_000_000;
  const random = () => Math.floor(Math.random() * (max + 1));
  return `${random()}.${random()}`;
}

function websocketError(code: number, message: string): Response {
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);

  server.accept();
  server.send(
    JSON.stringify({
      event: "pusher:error",
      data: {
        code,
        message,
      },
    }),
  );
  server.close(code, message);

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

function createAttachment(
  socketId: string,
  subscribedChannels: Iterable<string>,
  presence: Map<string, PresenceMember>,
  user: AuthenticatedUser | null,
  connectedAt: number,
  authDeadlineAt: number | null,
): SocketAttachment {
  return {
    socketId,
    subscribedChannels: [...subscribedChannels],
    presence: Object.fromEntries(presence.entries()),
    user,
    connectedAt,
    authDeadlineAt,
  };
}

function parseChannelData(raw: unknown): PresenceMember | null {
  if (typeof raw !== "string" || raw.length === 0) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as PresenceMember;
    if (typeof parsed.user_id === "undefined" || !parsed.user_info) {
      return null;
    }

    return parsed;
  } catch {
    return null;
  }
}

function parseUserData(raw: unknown): AuthenticatedUser | null {
  if (typeof raw !== "string" || raw.length === 0) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as Record<string, JsonValue>;
    const id = parsed?.id;
    if (typeof id === "undefined" || id === null) {
      return null;
    }

    const normalizedId = String(id);
    if (normalizedId.length === 0) {
      return null;
    }

    return {
      ...parsed,
      id: normalizedId,
    };
  } catch {
    return null;
  }
}

function getPresenceUserSnapshot(member: PresenceMember) {
  return {
    user_id: member.user_id,
    user_info: member.user_info,
  };
}

function getUserAuthenticationTimeoutMs(env: Env): number {
  return Math.max(
    0,
    parseNumber(
      env.USER_AUTHENTICATION_TIMEOUT_MS,
      DEFAULT_USER_AUTHENTICATION_TIMEOUT_MS,
    ),
  );
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return textResponse("", 204);
    }

    if (url.pathname === "/" || url.pathname === "/ready") {
      return textResponse("OK");
    }

    const websocketMatch = url.pathname.match(/^\/app\/([^/]+)$/);
    if (websocketMatch) {
      const appKey = decodeURIComponent(websocketMatch[1]);
      const app = findAppByKey(env, appKey);

      if (!app) {
        return websocketError(4001, `App key ${appKey} does not exist.`);
      }

      if (!app.enabled) {
        return websocketError(4003, "The app is not enabled.");
      }

      return env.APP_GATEWAY.getByName(`app:${app.id}`).fetch(
        withAppHeader(request, app.id),
      );
    }

    const apiMatch = url.pathname.match(/^\/apps\/([^/]+)(\/.*)?$/);
    if (!apiMatch) {
      return jsonResponse({ error: "Not found", code: 404 }, 404);
    }

    const appId = decodeURIComponent(apiMatch[1]);
    const app = findAppById(env, appId);

    if (!app) {
      return jsonResponse({ error: `The app ${appId} could not be found.`, code: 404 }, 404);
    }

    if (!(await verifyApiRequest(request, app))) {
      return jsonResponse({ error: "The secret authentication failed", code: 401 }, 401);
    }

    return env.APP_GATEWAY.getByName(`app:${app.id}`).fetch(
      withAppHeader(request, app.id, true),
    );
  },
};

export class AppGateway extends DurableObject<Env> {
  private appId: string | null = null;
  private sockets = new Map<string, SocketRecord>();
  private channels = new Map<string, Set<string>>();
  private presence = new Map<
    string,
    Map<string, { member: PresenceMember; socketIds: Set<string> }>
  >();
  private users = new Map<string, Set<string>>();
  private authTimers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    this.ctx.blockConcurrencyWhile(async () => {
      this.appId = (await this.ctx.storage.get<string>("appId")) ?? null;
      this.rebuildStateFromWebSockets();
      await this.scheduleAuthenticationAlarm();
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (!this.appId) {
      const appId = request.headers.get(APP_ID_HEADER);
      if (appId) {
        this.appId = appId;
        await this.ctx.storage.put("appId", appId);
      }
    }

    const app = this.getApp();
    if (!app) {
      if (url.pathname.startsWith("/app/")) {
        return websocketError(4001, "App does not exist.");
      }

      return jsonResponse({ error: "App does not exist.", code: 404 }, 404);
    }

    if (url.pathname === "/" || url.pathname === "/ready") {
      return textResponse("OK");
    }

    if (url.pathname.startsWith("/app/")) {
      return this.handleWebSocket(request, app);
    }

    if (request.headers.get(VERIFIED_HEADER) !== "1") {
      return jsonResponse({ error: "Unauthorized internal request.", code: 401 }, 401);
    }

    if (request.method === "GET" && url.pathname.endsWith("/channels")) {
      return this.handleChannels(url);
    }

    if (request.method === "GET" && /\/channels\/[^/]+\/users$/.test(url.pathname)) {
      return this.handleChannelUsers(url);
    }

    if (request.method === "GET" && /\/channels\/[^/]+$/.test(url.pathname)) {
      return this.handleChannel(url);
    }

    if (request.method === "POST" && url.pathname.endsWith("/batch_events")) {
      return this.handleBatchEvents(request, app);
    }

    if (
      request.method === "POST" &&
      /\/users\/[^/]+\/terminate_connections$/.test(url.pathname)
    ) {
      return this.handleTerminateUserConnections(url);
    }

    if (request.method === "POST" && url.pathname.endsWith("/events")) {
      return this.handleEvents(request, app);
    }

    return jsonResponse({ error: "Not found", code: 404 }, 404);
  }

  async alarm(): Promise<void> {
    await this.closeExpiredUnauthenticatedSockets();
    await this.scheduleAuthenticationAlarm();
  }

  async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string) {
    const socket = this.getSocketRecord(ws);
    if (!socket) {
      ws.close(4009, "Connection not initialized.");
      return;
    }

    const text =
      typeof message === "string" ? message : new TextDecoder().decode(message);

    let payload: PusherIncomingMessage | null = null;

    try {
      payload = JSON.parse(text) as PusherIncomingMessage;
    } catch {
      return;
    }

    if (!payload) {
      return;
    }

    if (payload.event === "pusher:ping") {
      this.send(ws, { event: "pusher:pong" });
      return;
    }

    if (payload.event === "pusher:subscribe") {
      await this.subscribe(socket, payload);
      return;
    }

    if (payload.event === "pusher:unsubscribe") {
      if (payload.data?.channel) {
        await this.unsubscribe(socket, String(payload.data.channel));
      }
      return;
    }

    if (payload.event === "pusher:signin") {
      await this.handleSignin(socket, payload);
      return;
    }

    if (isClientEvent(payload.event)) {
      await this.handleClientEvent(socket, payload);
    }
  }

  async webSocketClose(ws: WebSocket) {
    const socket = this.getSocketRecord(ws);
    if (!socket) {
      return;
    }

    await this.removeSocket(socket);
  }

  private rebuildStateFromWebSockets() {
    for (const timer of this.authTimers.values()) {
      clearTimeout(timer);
    }

    this.authTimers.clear();
    this.sockets.clear();
    this.channels.clear();
    this.presence.clear();
    this.users.clear();

    for (const websocket of this.ctx.getWebSockets()) {
      const attachment = websocket.deserializeAttachment() as SocketAttachment | null;
      if (!attachment?.socketId) {
        continue;
      }

      const presence = new Map<string, PresenceMember>(
        Object.entries(attachment.presence ?? {}),
      );

      const record: SocketRecord = {
        socketId: attachment.socketId,
        websocket,
        subscribedChannels: new Set(attachment.subscribedChannels ?? []),
        presence,
        user: attachment.user ?? null,
        connectedAt: attachment.connectedAt ?? Date.now(),
        authDeadlineAt:
          typeof attachment.authDeadlineAt === "number"
            ? attachment.authDeadlineAt
            : null,
      };

      this.sockets.set(record.socketId, record);

      for (const channel of record.subscribedChannels) {
        this.getChannelSockets(channel).add(record.socketId);
      }

      for (const [channel, member] of presence.entries()) {
        const members = this.getPresenceMembers(channel);
        const memberKey = String(member.user_id);
        const existing =
          members.get(memberKey) ?? { member, socketIds: new Set<string>() };
        existing.socketIds.add(record.socketId);
        members.set(memberKey, existing);
      }

      if (record.user) {
        this.addUserSocket(record.user.id, record.socketId);
      }

      this.scheduleSocketAuthenticationTimeout(record);
    }
  }

  private getApp(): AppConfig | null {
    if (!this.appId) {
      return null;
    }

    return findAppById(this.env, this.appId);
  }

  private getSocketRecord(websocket: WebSocket): SocketRecord | null {
    const attachment = websocket.deserializeAttachment() as SocketAttachment | null;
    if (!attachment?.socketId) {
      return null;
    }

    return this.sockets.get(attachment.socketId) ?? null;
  }

  private getChannelSockets(channel: string): Set<string> {
    if (!this.channels.has(channel)) {
      this.channels.set(channel, new Set<string>());
    }

    return this.channels.get(channel)!;
  }

  private getPresenceMembers(
    channel: string,
  ): Map<string, { member: PresenceMember; socketIds: Set<string> }> {
    if (!this.presence.has(channel)) {
      this.presence.set(
        channel,
        new Map<string, { member: PresenceMember; socketIds: Set<string> }>(),
      );
    }

    return this.presence.get(channel)!;
  }

  private addUserSocket(userId: string, socketId: string) {
    if (!this.users.has(userId)) {
      this.users.set(userId, new Set<string>());
    }

    this.users.get(userId)!.add(socketId);
  }

  private removeUserSocket(userId: string, socketId: string) {
    const sockets = this.users.get(userId);
    if (!sockets) {
      return;
    }

    sockets.delete(socketId);
    if (sockets.size === 0) {
      this.users.delete(userId);
    }
  }

  private send(websocket: WebSocket, payload: Record<string, unknown>) {
    try {
      websocket.send(JSON.stringify(payload));
    } catch {
      //
    }
  }

  private persistSocket(socket: SocketRecord) {
    socket.websocket.serializeAttachment(
      createAttachment(
        socket.socketId,
        socket.subscribedChannels,
        socket.presence,
        socket.user,
        socket.connectedAt,
        socket.authDeadlineAt,
      ),
    );
  }

  private clearSocketAuthenticationTimeout(socketId: string) {
    const timer = this.authTimers.get(socketId);
    if (timer) {
      clearTimeout(timer);
      this.authTimers.delete(socketId);
    }
  }

  private scheduleSocketAuthenticationTimeout(socket: SocketRecord) {
    this.clearSocketAuthenticationTimeout(socket.socketId);

    if (socket.authDeadlineAt === null) {
      return;
    }

    const delay = Math.max(socket.authDeadlineAt - Date.now(), 0);
    const timer = setTimeout(() => {
      void this.enforceAuthenticationDeadline(socket.socketId);
    }, delay);

    this.authTimers.set(socket.socketId, timer);
  }

  private async enforceAuthenticationDeadline(socketId: string) {
    const socket = this.sockets.get(socketId);
    if (!socket || socket.authDeadlineAt === null) {
      return;
    }

    if (Date.now() < socket.authDeadlineAt) {
      this.scheduleSocketAuthenticationTimeout(socket);
      return;
    }

    await this.disconnectSocket(
      socket,
      4009,
      "Connection not authorized within timeout.",
    );
  }

  private async clearAuthenticationRequirement(socket: SocketRecord) {
    socket.authDeadlineAt = null;
    this.clearSocketAuthenticationTimeout(socket.socketId);
    this.persistSocket(socket);
    await this.scheduleAuthenticationAlarm();
  }

  private async scheduleAuthenticationAlarm() {
    const deadlines = [...this.sockets.values()]
      .map(socket => socket.authDeadlineAt)
      .filter((deadline): deadline is number => typeof deadline === "number");

    if (deadlines.length === 0) {
      await this.ctx.storage.deleteAlarm();
      return;
    }

    await this.ctx.storage.setAlarm(Math.min(...deadlines));
  }

  private async closeExpiredUnauthenticatedSockets() {
    const now = Date.now();
    const expiredSockets = [...this.sockets.values()].filter(
      socket =>
        socket.authDeadlineAt !== null &&
        socket.authDeadlineAt <= now,
    );

    for (const socket of expiredSockets) {
      await this.disconnectSocket(
        socket,
        4009,
        "Connection not authorized within timeout.",
      );
    }
  }

  private async disconnectSocket(
    socket: SocketRecord,
    code: number,
    message: string,
  ) {
    this.send(socket.websocket, {
      event: "pusher:error",
      data: {
        code,
        message,
      },
    });

    try {
      socket.websocket.close(code, message);
    } catch {
      //
    }

    await this.removeSocket(socket);
  }

  private async handleWebSocket(request: Request, app: AppConfig): Promise<Response> {
    const upgrade = request.headers.get("Upgrade");
    if (!upgrade || upgrade.toLowerCase() !== "websocket") {
      return new Response(null, {
        status: 426,
        statusText: "Expected Upgrade: websocket",
      });
    }

    if (app.maxConnections >= 0 && this.ctx.getWebSockets().length >= app.maxConnections) {
      return websocketError(
        4100,
        "The current concurrent connections quota has been reached.",
      );
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const socketId = generateSocketId();
    const connectedAt = Date.now();
    const authDeadlineAt = app.enableUserAuthentication
      ? connectedAt + getUserAuthenticationTimeoutMs(this.env)
      : null;

    server.serializeAttachment(
      createAttachment(
        socketId,
        [],
        new Map<string, PresenceMember>(),
        null,
        connectedAt,
        authDeadlineAt,
      ),
    );
    this.ctx.acceptWebSocket(server);

    const socket: SocketRecord = {
      socketId,
      websocket: server,
      subscribedChannels: new Set<string>(),
      presence: new Map<string, PresenceMember>(),
      user: null,
      connectedAt,
      authDeadlineAt,
    };

    this.sockets.set(socketId, socket);
    this.scheduleSocketAuthenticationTimeout(socket);
    await this.scheduleAuthenticationAlarm();

    this.send(server, {
      event: "pusher:connection_established",
      data: JSON.stringify({
        socket_id: socketId,
        activity_timeout: 30,
      }),
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private async subscribe(socket: SocketRecord, payload: PusherIncomingMessage) {
    const app = this.getApp();
    const data = payload.data ?? {};
    const channel = String(data.channel ?? "");

    if (!app) {
      await this.disconnectSocket(socket, 4001, "App not configured.");
      return;
    }

    if (!channel) {
      return;
    }

    if (isServerToUserChannel(channel)) {
      this.subscribeToUserChannel(socket, channel);
      return;
    }

    if (channel.length > app.maxChannelNameLength) {
      this.send(socket.websocket, {
        event: "pusher:subscription_error",
        channel,
        data: {
          type: "LimitReached",
          error: `The channel name is longer than the allowed ${app.maxChannelNameLength} characters.`,
          status: 4009,
        },
      });
      return;
    }

    if (isRestrictedChannelName(channel)) {
      this.send(socket.websocket, {
        event: "pusher:subscription_error",
        channel,
        data: {
          type: "BadEventName",
          error:
            "The channel name is not allowed. Read channel conventions: https://pusher.com/docs/channels/using_channels/channels/#channel-naming-conventions",
          status: 4009,
        },
      });
      return;
    }

    if (isPrivateChannel(channel)) {
      const auth = typeof data.auth === "string" ? data.auth : "";
      const expected = isPresenceChannel(channel)
        ? this.expectedPresenceAuth(app, socket.socketId, channel, data.channel_data)
        : this.expectedPrivateAuth(app, socket.socketId, channel);

      if (auth !== expected) {
        this.send(socket.websocket, {
          event: "pusher:subscription_error",
          channel,
          data: {
            type: "AuthError",
            error: "The connection is unauthorized.",
            status: 401,
          },
        });
        return;
      }
    }

    if (socket.subscribedChannels.has(channel)) {
      return;
    }

    if (isPresenceChannel(channel)) {
      const member = parseChannelData(data.channel_data);

      if (!member) {
        this.send(socket.websocket, {
          event: "pusher:subscription_error",
          channel,
          data: {
            type: "AuthError",
            error: "Presence channel_data is missing or invalid.",
            status: 401,
          },
        });
        return;
      }

      if (dataToKilobytes(member.user_info) > app.maxPresenceMemberSizeInKb) {
        this.send(socket.websocket, {
          event: "pusher:subscription_error",
          channel,
          data: {
            type: "LimitReached",
            error: `The maximum size for a channel member is ${app.maxPresenceMemberSizeInKb} KB.`,
            status: 4301,
          },
        });
        return;
      }

      const members = this.getPresenceMembers(channel);
      const memberKey = String(member.user_id);

      if (!members.has(memberKey) && members.size + 1 > app.maxPresenceMembersPerChannel) {
        this.send(socket.websocket, {
          event: "pusher:subscription_error",
          channel,
          data: {
            type: "LimitReached",
            error: "The maximum members per presence channel limit was reached",
            status: 4100,
          },
        });
        return;
      }

      const existing =
        members.get(memberKey) ?? { member, socketIds: new Set<string>() };
      const isNewMember = existing.socketIds.size === 0;
      existing.socketIds.add(socket.socketId);
      members.set(memberKey, existing);
      socket.presence.set(channel, member);
      socket.subscribedChannels.add(channel);
      this.getChannelSockets(channel).add(socket.socketId);

      if (socket.authDeadlineAt !== null) {
        await this.clearAuthenticationRequirement(socket);
      } else {
        this.persistSocket(socket);
      }

      if (isNewMember) {
        this.broadcastInternal(
          channel,
          {
            event: "pusher_internal:member_added",
            channel,
            data: JSON.stringify(getPresenceUserSnapshot(member)),
          },
          socket.socketId,
        );
      }

      const snapshot = this.getPresenceSnapshot(channel);

      this.send(socket.websocket, {
        event: "pusher_internal:subscription_succeeded",
        channel,
        data: JSON.stringify({
          presence: snapshot,
        }),
      });
      return;
    }

    socket.subscribedChannels.add(channel);
    this.getChannelSockets(channel).add(socket.socketId);

    if (isPrivateChannel(channel) && socket.authDeadlineAt !== null) {
      await this.clearAuthenticationRequirement(socket);
    } else {
      this.persistSocket(socket);
    }

    this.send(socket.websocket, {
      event: "pusher_internal:subscription_succeeded",
      channel,
    });
  }

  private subscribeToUserChannel(socket: SocketRecord, channel: string) {
    if (!socket.user || channel !== serverToUserChannelFor(socket.user.id)) {
      this.send(socket.websocket, {
        event: "pusher:subscription_error",
        channel,
        data: {
          type: "AuthError",
          error: "The connection is unauthorized.",
          status: 401,
        },
      });
      return;
    }

    this.send(socket.websocket, {
      event: "pusher_internal:subscription_succeeded",
      channel,
    });
  }

  private async unsubscribe(socket: SocketRecord, channel: string) {
    if (isServerToUserChannel(channel) || !socket.subscribedChannels.has(channel)) {
      return;
    }

    socket.subscribedChannels.delete(channel);
    this.getChannelSockets(channel).delete(socket.socketId);

    if (this.getChannelSockets(channel).size === 0) {
      this.channels.delete(channel);
    }

    if (socket.presence.has(channel)) {
      const member = socket.presence.get(channel)!;
      const members = this.getPresenceMembers(channel);
      const memberKey = String(member.user_id);
      const existing = members.get(memberKey);

      if (existing) {
        existing.socketIds.delete(socket.socketId);
        if (existing.socketIds.size === 0) {
          members.delete(memberKey);
          this.broadcastInternal(channel, {
            event: "pusher_internal:member_removed",
            channel,
            data: JSON.stringify(getPresenceUserSnapshot(member)),
          });
        }
      }

      if (members.size === 0) {
        this.presence.delete(channel);
      }

      socket.presence.delete(channel);
    }

    this.persistSocket(socket);
  }

  private async removeSocket(socket: SocketRecord) {
    this.clearSocketAuthenticationTimeout(socket.socketId);

    for (const channel of [...socket.subscribedChannels]) {
      await this.unsubscribe(socket, channel);
    }

    if (socket.user) {
      this.removeUserSocket(socket.user.id, socket.socketId);
    }

    this.sockets.delete(socket.socketId);
    await this.scheduleAuthenticationAlarm();
  }

  private getPresenceSnapshot(channel: string) {
    const members = this.getPresenceMembers(channel);
    const ids = [...members.keys()];

    return {
      ids,
      hash: Object.fromEntries(
        [...members.entries()].map(([userId, state]) => [userId, state.member.user_info]),
      ),
      count: ids.length,
    };
  }

  private broadcastInternal(
    channel: string,
    payload: Record<string, unknown>,
    exceptSocketId?: string,
  ) {
    const sockets = this.channels.get(channel);
    if (!sockets) {
      return;
    }

    for (const socketId of sockets) {
      if (socketId === exceptSocketId) {
        continue;
      }

      const socket = this.sockets.get(socketId);
      if (socket) {
        this.send(socket.websocket, payload);
      }
    }
  }

  private broadcastToUser(
    userId: string,
    payload: Record<string, unknown>,
    exceptSocketId?: string,
  ) {
    const sockets = this.users.get(userId);
    if (!sockets) {
      return;
    }

    for (const socketId of sockets) {
      if (socketId === exceptSocketId) {
        continue;
      }

      const socket = this.sockets.get(socketId);
      if (socket) {
        this.send(socket.websocket, payload);
      }
    }
  }

  private async handleClientEvent(socket: SocketRecord, payload: PusherIncomingMessage) {
    const app = this.getApp();
    const channel = String(payload.channel ?? "");

    if (!app || !channel) {
      return;
    }

    if (isEncryptedChannel(channel)) {
      this.send(socket.websocket, {
        event: "pusher:error",
        channel,
        data: {
          code: 4301,
          message: "Client events are not supported on encrypted channels.",
        },
      });
      return;
    }

    if (!app.enableClientMessages) {
      this.send(socket.websocket, {
        event: "pusher:error",
        channel,
        data: {
          code: 4301,
          message: "The app does not have client messaging enabled.",
        },
      });
      return;
    }

    if (!socket.subscribedChannels.has(channel)) {
      return;
    }

    const event = payload.event;
    if (event.length > app.maxEventNameLength) {
      this.send(socket.websocket, {
        event: "pusher:error",
        channel,
        data: {
          code: 4301,
          message: `Event name is too long. Maximum allowed size is ${app.maxEventNameLength}.`,
        },
      });
      return;
    }

    if (dataToKilobytes(payload.data) > app.maxEventPayloadInKb) {
      this.send(socket.websocket, {
        event: "pusher:error",
        channel,
        data: {
          code: 4301,
          message: `The event data should be less than ${app.maxEventPayloadInKb} KB.`,
        },
      });
      return;
    }

    const body: Record<string, unknown> = {
      event,
      channel,
      data: payload.data,
    };

    const member = socket.presence.get(channel);
    if (member) {
      body.user_id = member.user_id;
    }

    this.broadcastInternal(channel, body, socket.socketId);
  }

  private async handleSignin(socket: SocketRecord, payload: PusherIncomingMessage) {
    const app = this.getApp();
    if (!app || !app.enableUserAuthentication) {
      return;
    }

    const data = payload.data ?? {};
    const auth = typeof data.auth === "string" ? data.auth : "";
    const userData = typeof data.user_data === "string" ? data.user_data : "";

    if (auth !== this.expectedUserAuthentication(app, socket.socketId, userData)) {
      await this.disconnectSocket(socket, 4009, "Connection not authorized.");
      return;
    }

    const user = parseUserData(userData);
    if (!user) {
      await this.disconnectSocket(
        socket,
        4009,
        'The returned user data must contain the "id" field.',
      );
      return;
    }

    if (socket.user) {
      this.removeUserSocket(socket.user.id, socket.socketId);
    }

    socket.user = user;
    this.addUserSocket(user.id, socket.socketId);

    if (socket.authDeadlineAt !== null) {
      await this.clearAuthenticationRequirement(socket);
    } else {
      this.persistSocket(socket);
    }

    this.send(socket.websocket, {
      event: "pusher:signin_success",
      data: {
        auth,
        user_data: userData,
      },
    });
  }

  private handleChannels(url: URL): Response {
    const filter = url.searchParams.get("filter_by_prefix");
    const channels = Object.fromEntries(
      [...this.channels.entries()]
        .filter(([, sockets]) => sockets.size > 0)
        .filter(([channel]) => !filter || channel.startsWith(filter))
        .map(([channel, sockets]) => [
          channel,
          {
            subscription_count: sockets.size,
            occupied: true,
          },
        ]),
    );

    return jsonResponse({ channels });
  }

  private handleChannel(url: URL): Response {
    const channel = decodeURIComponent(url.pathname.split("/").pop()!);
    const socketCount = this.channels.get(channel)?.size ?? 0;
    const response: Record<string, unknown> = {
      subscription_count: socketCount,
      occupied: socketCount > 0,
    };

    if (isPresenceChannel(channel)) {
      response.user_count = this.presence.get(channel)?.size ?? 0;
    }

    return jsonResponse(response);
  }

  private handleChannelUsers(url: URL): Response {
    const segments = url.pathname.split("/");
    const channel = decodeURIComponent(segments[segments.length - 2] ?? "");

    if (!isPresenceChannel(channel)) {
      return jsonResponse({ error: "The channel must be a presence channel.", code: 400 }, 400);
    }

    const withUserInfo = url.searchParams.get("with_user_info") === "1";
    const users = [...(this.presence.get(channel)?.values() ?? [])].map(({ member }) =>
      withUserInfo
        ? { id: member.user_id, user_info: member.user_info }
        : { id: member.user_id },
    );

    return jsonResponse({ users });
  }

  private async handleEvents(request: Request, app: AppConfig): Promise<Response> {
    const rawBody = await request.text();
    let body: PusherBackendMessage;

    try {
      body = JSON.parse(rawBody) as PusherBackendMessage;
    } catch {
      return jsonResponse({ error: "The received data is incorrect", code: 400 }, 400);
    }

    const validationError = this.validateBackendMessage(body, app);
    if (validationError) {
      return validationError;
    }

    this.broadcastBackendMessage(body);
    return jsonResponse({ ok: true });
  }

  private async handleBatchEvents(request: Request, app: AppConfig): Promise<Response> {
    const rawBody = await request.text();
    let body: BatchEventsRequest;

    try {
      body = JSON.parse(rawBody) as BatchEventsRequest;
    } catch {
      return jsonResponse({ error: "The received data is incorrect", code: 400 }, 400);
    }

    if (!Array.isArray(body.batch)) {
      return jsonResponse({ error: "The received data is incorrect", code: 400 }, 400);
    }

    if (body.batch.length > app.maxEventBatchSize) {
      return jsonResponse(
        {
          error: `Cannot batch-send more than ${app.maxEventBatchSize} messages at once`,
          code: 400,
        },
        400,
      );
    }

    for (const message of body.batch) {
      const validationError = this.validateBackendMessage(message, app);
      if (validationError) {
        return validationError;
      }
    }

    for (const message of body.batch) {
      this.broadcastBackendMessage(message);
    }

    return jsonResponse({ ok: true });
  }

  private async handleTerminateUserConnections(url: URL): Promise<Response> {
    const match = url.pathname.match(/\/users\/([^/]+)\/terminate_connections$/);
    const userId = match ? decodeURIComponent(match[1]) : null;

    if (!userId) {
      return jsonResponse({ error: "Not found", code: 404 }, 404);
    }

    const socketIds = [...(this.users.get(userId) ?? new Set<string>())];
    for (const socketId of socketIds) {
      const socket = this.sockets.get(socketId);
      if (socket) {
        await this.disconnectSocket(socket, 4009, "You got disconnected by the app.");
      }
    }

    return jsonResponse({ ok: true });
  }

  private validateBackendMessage(
    body: PusherBackendMessage | null | undefined,
    app: AppConfig,
  ): Response | null {
    if (!body || (!body.channel && !body.channels) || !body.name || typeof body.data === "undefined") {
      return jsonResponse({ error: "The received data is incorrect", code: 400 }, 400);
    }

    const channels = body.channels ?? [body.channel!];
    if (channels.length === 0) {
      return jsonResponse({ error: "The received data is incorrect", code: 400 }, 400);
    }

    if (channels.length > app.maxEventChannelsAtOnce) {
      return jsonResponse(
        {
          error: `Cannot broadcast to more than ${app.maxEventChannelsAtOnce} channels at once`,
          code: 400,
        },
        400,
      );
    }

    if (channels.some(isEncryptedChannel) && channels.length > 1) {
      return jsonResponse(
        {
          error: "You cannot broadcast to multiple channels when using encrypted channels.",
          code: 400,
        },
        400,
      );
    }

    if (body.name.length > app.maxEventNameLength) {
      return jsonResponse(
        {
          error: `Event name is too long. Maximum allowed size is ${app.maxEventNameLength}.`,
          code: 400,
        },
        400,
      );
    }

    if (dataToKilobytes(body.data) > app.maxEventPayloadInKb) {
      return jsonResponse(
        {
          error: `The event data should be less than ${app.maxEventPayloadInKb} KB.`,
          code: 413,
        },
        413,
      );
    }

    return null;
  }

  private broadcastBackendMessage(body: PusherBackendMessage) {
    const channels = body.channels ?? [body.channel!];

    for (const channel of channels) {
      const payload = {
        event: body.name,
        channel,
        data: body.data,
      };

      if (isServerToUserChannel(channel)) {
        const userId = userIdFromServerToUserChannel(channel);
        if (userId) {
          this.broadcastToUser(userId, payload, body.socket_id);
        }
        continue;
      }

      this.broadcastInternal(channel, payload, body.socket_id);
    }
  }

  private expectedPrivateAuth(app: AppConfig, socketId: string, channel: string): string {
    return pusherAuth(app, `${socketId}:${channel}`);
  }

  private expectedPresenceAuth(
    app: AppConfig,
    socketId: string,
    channel: string,
    channelData: unknown,
  ): string {
    return pusherAuth(app, `${socketId}:${channel}:${String(channelData ?? "")}`);
  }

  private expectedUserAuthentication(
    app: AppConfig,
    socketId: string,
    userData: string,
  ): string {
    return pusherAuth(app, `${socketId}::user::${userData}`);
  }
}
