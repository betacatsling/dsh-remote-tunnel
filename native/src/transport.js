import { randomUUID } from "node:crypto";
import WebSocket from "ws";

const endpointPattern = /^(?:[A-Za-z][\w.-]*\/[\w.-]+|\$events(?:\/result)?)$/;
const problem = (message, status = 400) =>
  Object.assign(new Error(message), { status });
export class AsyncQueue {
  constructor() {
    this.values = [];
    this.waiters = [];
    this.ended = false;
    this.error = null;
  }
  push(value) {
    if (this.ended) return;
    const next = this.waiters.shift();
    if (next) next.resolve({ value, done: false });
    else this.values.push(value);
  }
  end(error) {
    if (this.ended) return;
    this.ended = true;
    this.error = error;
    for (const next of this.waiters.splice(0))
      error ? next.reject(error) : next.resolve({ done: true });
  }
  [Symbol.asyncIterator]() {
    return this;
  }
  async next() {
    if (this.values.length) return { value: this.values.shift(), done: false };
    if (this.error) throw this.error;
    if (this.ended) return { done: true };
    return new Promise((resolve, reject) =>
      this.waiters.push({ resolve, reject }),
    );
  }
  async return() {
    this.end();
    return { done: true };
  }
}
function loopback(value) {
  const u = new URL(value);
  if (
    u.protocol !== "http:" ||
    !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) ||
    u.username ||
    u.password
  )
    throw problem("Invalid tunnel address");
  return u;
}
/** A transport adapter only: all business methods and views stay in the shipped Harness. */
export class WorkspaceTransport {
  constructor(
    manager,
    hosts,
    { fetchImpl = fetch, WebSocketImpl = WebSocket } = {},
  ) {
    this.manager = manager;
    this.hosts = hosts;
    this.fetch = fetchImpl;
    this.WebSocket = WebSocketImpl;
    this.diagnostics = [];
    this.connections = new Map();
    this.connecting = new Map();
    this.sessionOwners = new Map();
    this.workspaceOwners = new Map();
    this.controllers = new Set();
    this.closed = false;
  }
  host(alias) {
    const host = this.hosts.find((h) => h.alias === alias);
    if (!host) throw problem("Unknown workspace connection", 404);
    return host;
  }
  async connect(alias) {
    this.host(alias);
    if (this.closed) throw problem("Transport closed", 503);
    if (this.connections.has(alias)) return this.connections.get(alias);
    if (!this.connecting.has(alias))
      this.connecting.set(
        alias,
        this.open(alias).finally(() => this.connecting.delete(alias)),
      );
    return this.connecting.get(alias);
  }
  async open(alias) {
    const host = this.host(alias);
    let state = this.manager.listStatesLocal().find((s) => s.alias === alias);
    let alive = false;
    if (state)
      try {
        process.kill(state.sshPid, 0);
        alive = true;
      } catch {}
    if (!alive) state = await this.manager.up(alias, {});
    const url = loopback(state.url);
    let auth = loopback(state.authUrl ?? state.url);
    if (auth.origin !== url.origin)
      throw problem("Tunnel authentication origin mismatch");
    let response = await this.fetch(auth, {
      redirect: "manual",
      signal: AbortSignal.timeout(15000),
    });
    if (response.status === 401 || !response.headers.getSetCookie().length) {
      const targets = await this.manager.resolveTargets(alias);
      const journal = await targets.scope.journal(
        targets.hostDef,
        this.manager.cfg,
        this.manager.ctxFor(alias),
        200,
      );
      const latest = [...journal.matchAll(/dsh web:\s*(http\S+)/g)].at(-1);
      const token = latest && new URL(latest[1]).searchParams.get("token");
      if (token) {
        auth = new URL("/", url);
        auth.searchParams.set("token", token);
        response = await this.fetch(auth, {
          redirect: "manual",
          signal: AbortSignal.timeout(15000),
        });
      }
    }
    const cookie = response.headers
      .getSetCookie()
      .map((v) => v.split(";")[0])
      .join("; ");
    if (response.status >= 400 || !cookie)
      throw problem("Workspace authentication failed", 502);
    const c = { alias, url: url.origin, cookie, workspace: null };
    const result = await this.raw(c, "workspace/create", {
      args: { request: { path: host.workspace } },
    });
    if (!result.ok) throw problem(result.error.message, 502);
    c.workspace = result.value.workspace;
    if (c.workspace.path !== host.workspace)
      throw problem("Workspace directory mismatch", 502);
    if (this.closed) throw problem("Transport closed", 503);
    this.connections.set(alias, c);
    this.workspaceOwners.set(c.workspace.workspaceId, alias);
    await this.summaries(alias, c);
    return c;
  }
  async raw(c, endpoint, payload, signal) {
    if (!endpointPattern.test(endpoint)) throw problem("Invalid API endpoint");
    const rpcId = randomUUID();
    const response = await this.fetch(c.url + "/api/" + endpoint, {
      method: "POST",
      headers: { cookie: c.cookie, "content-type": "application/json" },
      body: JSON.stringify({
        type: "client-request",
        rpcId,
        method: endpoint,
        payload,
      }),
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(60000)])
        : AbortSignal.timeout(60000),
    });
    if (!response.ok)
      throw problem(
        "Workspace API transport failed: HTTP " + response.status,
        502,
      );
    let value;
    if (
      response.headers.get("content-type")?.startsWith("multipart/form-data")
    ) {
      const form = await response.formData();
      value = JSON.parse(form.get("metadata"));
      value.result.binaryParts = [];
      for (const part of value.attachments ?? []) {
        const blob = form.get(part.part);
        if (part.codec !== "bytes" || !(blob instanceof Blob))
          throw problem("Invalid binary response", 502);
        value.result.binaryParts.push({
          path: part.path,
          data: Buffer.from(await blob.arrayBuffer()).toString("base64"),
        });
      }
    } else value = await response.json();
    if (value.rpcId !== rpcId || typeof value.result?.ok !== "boolean")
      throw problem("Workspace protocol mismatch", 502);
    return value.result;
  }
  async summaries(alias, c) {
    const result = await this.raw(c, "session/list", {
      args: { _request: {} },
    });
    if (!result.ok) throw problem(result.error.message, 502);
    const items = result.value.items.filter((s) => s.cwd === c.workspace.path);
    for (const item of items) this.sessionOwners.set(item.sessionId, alias);
    return items;
  }
  async catalog() {
    return {
      diagnostics: this.diagnostics,
      hosts: await Promise.all(
        this.hosts.map(async (host) => {
          try {
            const c = await this.connect(host.alias);
            const sessions = await this.summaries(host.alias, c);
            return {
              alias: host.alias,
              workspace: {
                ...c.workspace,
                sessionIds: sessions.map((s) => s.sessionId),
              },
              sessions,
            };
          } catch (error) {
            this.connections.delete(host.alias);
            return { alias: host.alias, error: error.message };
          }
        }),
      ),
    };
  }
  validate(alias, endpoint, args) {
    const check = (value, key) => {
      if (!value || typeof value !== "object") return;
      for (const [k, v] of Object.entries(value)) {
        if (typeof v === "string") {
          if (k === "workspaceId" && this.workspaceOwners.get(v) !== alias)
            throw problem("Workspace belongs to another connection", 404);
          if (
            [
              "sessionId",
              "agentId",
              "agent",
              "parentSessionId",
              "childSessionId",
              "workspaceFileScopeId",
            ].includes(k) &&
            endpoint !== "session/create" &&
            this.sessionOwners.get(v) !== alias
          )
            throw problem("Session belongs to another connection", 404);
        } else check(v, k);
      }
    };
    check(args);
    if (
      endpoint === "session/create" &&
      (args.request?.workspaceId !==
        this.connections.get(alias)?.workspace.workspaceId ||
        (args.request?.cwd && args.request.cwd !== this.host(alias).workspace))
    )
      throw problem("Session creation requires the configured workspace");
  }
  async call(alias, endpoint, payload, signal) {
    const c = await this.connect(alias);
    this.validate(alias, endpoint, payload.args);
    if (endpoint === "session/list")
      return { ok: true, value: { items: await this.summaries(alias, c) } };
    let result;
    try {
      result = await this.raw(c, endpoint, payload, signal);
    } catch (error) {
      this.connections.delete(alias);
      throw error;
    }
    if (result.ok && endpoint === "session/search")
      result.value.items = result.value.items.filter(
        (item) => this.sessionOwners.get(item.sessionId) === alias,
      );
    if (result.ok && result.value?.sessionId)
      this.sessionOwners.set(result.value.sessionId, alias);
    return result;
  }
  /** Opening/cancelling a logical stream only attaches/detaches observation. */
  async *stream(alias, endpoint, payload, signal, uplink) {
    const c = await this.connect(alias);
    this.validate(alias, endpoint, payload.args);
    if (!endpointPattern.test(endpoint)) throw problem("Invalid API endpoint");
    const queue = new AsyncQueue(),
      ws = new this.WebSocket(
        c.url.replace("http:", "ws:") + "/api/remote.mux",
        { headers: { cookie: c.cookie } },
      );
    const opened = Promise.withResolvers();
    const timeout = setTimeout(() => {
      opened.reject(problem("Stream connection timed out", 502));
      ws.terminate();
    }, 15000);
    timeout.unref?.();
    const stop = () => {
      queue.end();
      ws.close();
    };
    signal.addEventListener("abort", stop, { once: true });
    ws.on("open", () => {
      clearTimeout(timeout);
      ws.send(
        JSON.stringify({ type: "open", streamId: "stream", endpoint, payload }),
      );
      opened.resolve();
    });
    ws.on("message", (raw) => {
      try {
        const frame = JSON.parse(String(raw));
        if (frame.type === "item") queue.push(frame.value);
        else if (frame.type === "end") queue.end();
        else if (frame.type === "error")
          queue.end(Object.assign(new Error(frame.error.message), frame.error));
      } catch (error) {
        queue.end(error);
      }
    });
    ws.on("error", (error) => {
      clearTimeout(timeout);
      opened.reject(error);
      queue.end(error);
    });
    ws.on("close", () => {
      clearTimeout(timeout);
      opened.reject(new Error("Workspace stream closed"));
      queue.end(new Error("Workspace connection lost"));
    });
    try {
      await opened.promise;
      if (signal.aborted) return;
      if (uplink)
        void (async () => {
          try {
            for await (const value of uplink) {
              if (ws.readyState !== 1) return;
              ws.send(
                JSON.stringify({ type: "item", streamId: "stream", value }),
              );
            }
            if (ws.readyState === 1)
              ws.send(JSON.stringify({ type: "end", streamId: "stream" }));
          } catch (error) {
            queue.end(error);
          }
        })();
      for await (const value of queue) {
        if (endpoint === "workspace/follow" && value.type === "baseline") {
          const items = value.value.items.filter(
            (w) => w.workspaceId === c.workspace.workspaceId,
          );
          yield { ...value, value: { ...value.value, items } };
        } else if (
          endpoint === "workspace/follow" &&
          value.type === "upsert" &&
          value.workspace.workspaceId !== c.workspace.workspaceId
        )
          continue;
        else yield value;
      }
    } catch (error) {
      if (!signal.aborted) this.connections.delete(alias);
      throw error;
    } finally {
      clearTimeout(timeout);
      signal.removeEventListener("abort", stop);
      queue.end();
      if (ws.readyState === 1)
        ws.send(JSON.stringify({ type: "cancel", streamId: "stream" }));
      ws.close();
    }
  }
  async dispose() {
    this.closed = true;
    for (const controller of this.controllers) controller.abort();
    await this.manager.dispose();
  }
}

async function bodyOf(req) {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > 64 * 1024 * 1024) throw problem("Request too large", 413);
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
}
export function registerTransportRoutes(ctx, transport) {
  const uplinks = new Map();
  // Reuse the shipped upload/media routes, including their original byte protocol.
  ctx.on("connection/request", async (req, res, next) => {
    const url = new URL(req.url, "http://127.0.0.1");
    let alias;
    if (url.pathname === "/api/session/uploadFileBinary")
      alias = transport.sessionOwners.get(url.searchParams.get("sessionId"));
    else if (url.pathname === "/api/file") {
      const path = url.searchParams.get("path");
      const matches = transport.hosts.filter(
        (host) =>
          path &&
          (path === host.workspace ||
            path.startsWith(host.workspace.replace(/\/+$/, "") + "/")),
      );
      if (matches.length === 1) alias = matches[0].alias;
    }
    if (!alias) return next();
    let admission;
    try {
      admission = ctx.get("connection").admit(req);
    } catch {}
    if (!admission || "rejection" in admission) {
      res.writeHead(admission?.rejection ?? 401);
      res.end();
      return;
    }
    const c = await transport.connect(alias),
      abort = new AbortController();
    const close = () => abort.abort();
    res.on("close", close);
    try {
      const headers = { cookie: c.cookie };
      for (const key of ["content-type", "content-length", "range"])
        if (typeof req.headers[key] === "string")
          headers[key] = req.headers[key];
      const response = await transport.fetch(
        c.url + url.pathname + url.search,
        {
          method: req.method,
          headers,
          ...(["GET", "HEAD"].includes(req.method)
            ? {}
            : { body: req, duplex: "half" }),
          signal: abort.signal,
        },
      );
      const outgoing = {};
      for (const key of [
        "content-type",
        "content-length",
        "content-disposition",
        "content-security-policy",
        "x-content-type-options",
        "cache-control",
        "content-range",
        "accept-ranges",
      ])
        if (response.headers.has(key))
          outgoing[key] = response.headers.get(key);
      res.writeHead(response.status, outgoing);
      if (response.body)
        for await (const chunk of response.body) {
          if (abort.signal.aborted) break;
          res.write(chunk);
        }
      res.end();
    } catch (error) {
      if (!res.headersSent)
        res.writeHead(502, { "content-type": "text/plain" });
      if (!res.destroyed) res.end("Workspace file transport unavailable");
    } finally {
      res.removeListener("close", close);
      abort.abort();
    }
  });
  const dispose = ctx.get("webServer").register({
    kind: "prefix",
    path: "/native-remote",
    handler: (req, res) => {
      void (async () => {
        let streaming = false;
        try {
          let admission;
          try {
            admission = ctx.get("connection")?.admit(req);
          } catch {}
          if (!admission || "rejection" in admission)
            throw problem(
              "Harness authentication required",
              admission?.rejection ?? 401,
            );
          const url = new URL(req.url, "http://127.0.0.1"),
            action = url.pathname.slice("/native-remote/".length);
          if (req.method !== "POST" || url.searchParams.get("confirm") !== "1")
            throw problem("POST with confirm=1 required", 405);
          const body = await bodyOf(req);
          if (action === "stream" || action === "local-stream") {
            if (!endpointPattern.test(body.endpoint))
              throw problem("Invalid API endpoint");
            const abort = new AbortController(),
              uplink = new AsyncQueue();
            transport.controllers.add(abort);
            if (typeof body.streamId !== "string" || uplinks.has(body.streamId))
              throw problem("Invalid stream identity");
            uplinks.set(body.streamId, uplink);
            const close = () => abort.abort();
            res.on("close", close);
            try {
              const source =
                action === "stream"
                  ? transport.stream(
                      body.host,
                      body.endpoint,
                      body.payload,
                      abort.signal,
                      uplink,
                    )
                  : await ctx
                      .get("typertGateway")
                      .wireStream.open(
                        body.endpoint,
                        body.payload,
                        uplink,
                        admission.peer,
                        abort.signal,
                      );
              res.writeHead(200, {
                "content-type": "application/x-ndjson",
                "cache-control": "no-store",
              });
              streaming = true;
              // Headers must arrive before an otherwise idle snapshot stream produces an increment.
              res.flushHeaders?.();
              for await (const value of source) {
                if (abort.signal.aborted) break;
                if (!res.write(JSON.stringify({ type: "item", value }) + "\n"))
                  await new Promise((resolve) => {
                    res.once("drain", resolve);
                    abort.signal.addEventListener("abort", resolve, {
                      once: true,
                    });
                  });
              }
              if (!res.destroyed)
                res.end(JSON.stringify({ type: "end" }) + "\n");
            } finally {
              abort.abort();
              uplink.end();
              uplinks.delete(body.streamId);
              transport.controllers.delete(abort);
              res.removeListener("close", close);
            }
            return;
          }
          let value;
          if (action === "diagnostic") {
            transport.diagnostics.push(String(body.stage).slice(0, 500));
            transport.diagnostics = transport.diagnostics.slice(-20);
            value = { recorded: true };
          } else if (action === "catalog") {
            value = await transport.catalog();
            const local = await ctx
              .get("sessionController")
              .list({}, new AbortController().signal);
            const ids = new Set(local.items.map((s) => s.sessionId));
            for (const host of value.hosts)
              if (host.sessions?.some((s) => ids.has(s.sessionId))) {
                delete host.workspace;
                delete host.sessions;
                host.error =
                  "Session identity collision between workspace connections";
              }
          } else if (action === "rpc")
            value = await transport.call(
              body.host,
              body.endpoint,
              body.payload,
            );
          else if (action === "uplink") {
            const queue = uplinks.get(body.streamId);
            if (!queue) throw problem("Stream ended", 409);
            body.end ? queue.end() : queue.push(body.value);
            value = { accepted: true };
          } else throw problem("Unknown transport action", 404);
          res.writeHead(200, {
            "content-type": "application/json",
            "cache-control": "no-store",
          });
          res.end(JSON.stringify({ ok: true, value }));
        } catch (error) {
          if (res.destroyed) return;
          if (streaming)
            res.end(
              JSON.stringify({
                type: "error",
                error: {
                  code: error.code ?? "gateway/transport",
                  message: error.message,
                  details: error.details ?? {},
                },
              }) + "\n",
            );
          else {
            res.writeHead(error.status ?? 502, {
              "content-type": "application/json",
              "cache-control": "no-store",
            });
            res.end(JSON.stringify({ ok: false, error: error.message }));
          }
        }
      })();
    },
  });
  ctx.effect(() => dispose, "workspace-transport.routes");
}
