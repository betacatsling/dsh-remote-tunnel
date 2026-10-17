import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import { WorkspaceTransport, AsyncQueue } from "../native/src/transport.js";

function fixture() {
  const calls = [],
    manager = { dispose: async () => calls.push("dispose") };
  const transport = new WorkspaceTransport(manager, [
    { alias: "lab", workspace: "/workspace/research" },
  ]);
  const connection = {
    alias: "lab",
    workspace: { workspaceId: "workspace-lab", path: "/workspace/research" },
  };
  transport.connections.set("lab", connection);
  transport.workspaceOwners.set("workspace-lab", "lab");
  transport.sessionOwners.set("session-lab", "lab");
  transport.raw = async (c, e, p) => {
    calls.push([e, p]);
    return { ok: true, value: { sessionId: "created" } };
  };
  return { transport, calls };
}
test("workspace routing refuses another connection session before forwarding", async () => {
  const { transport, calls } = fixture();
  await assert.rejects(
    transport.call("lab", "session/prompt", {
      args: { request: { sessionId: "session-other" } },
    }),
    /another connection/,
  );
  assert.equal(calls.length, 0);
  await transport.call("lab", "session/prompt", {
    args: { request: { sessionId: "session-lab", requestId: "receipt" } },
  });
  assert.equal(calls[0][1].args.request.requestId, "receipt");
});
test("session creation is bound to the configured workspace and preserves the native response", async () => {
  const { transport } = fixture();
  await assert.rejects(
    transport.call("lab", "session/create", {
      args: { request: { cwd: "/workspace/other" } },
    }),
    /configured workspace/,
  );
  const value = await transport.call("lab", "session/create", {
    args: { request: { workspaceId: "workspace-lab" } },
  });
  assert.deepEqual(value, { ok: true, value: { sessionId: "created" } });
  assert.equal(transport.sessionOwners.get("created"), "lab");
});
test("client disposal releases observation and owned local transport without cancelling remote work", async () => {
  const { transport, calls } = fixture();
  const controller = new AbortController();
  transport.controllers.add(controller);
  await transport.dispose();
  assert.equal(controller.signal.aborted, true);
  assert.deepEqual(calls, ["dispose"]);
});
test("stream queues deliver frames in order and release pending readers", async () => {
  const q = new AsyncQueue();
  q.push({ seq: 1 });
  assert.deepEqual(await q.next(), { value: { seq: 1 }, done: false });
  const pending = q.next();
  q.end();
  assert.deepEqual(await pending, { done: true });
});

async function clientFixture({ question = false } = {}) {
  const requests = [],
    disposers = [],
    remote = {
      workspaceId: "workspace-lab",
      path: "/workspace/research",
      title: "research",
      sessionIds: ["session-lab"],
    };
  let registered;
  const controls = [],
    accepted = [],
    injectedDependencies = [],
    generationListeners = new Set(),
    workspaceReaders = new Map();
  let createdCount = 0,
    failWorkspaceOpen = false;
  const pushFrame = (controller, value) =>
    controller.enqueue(
      new TextEncoder().encode(JSON.stringify({ type: "item", value }) + "\n"),
    );
  const fetchImpl = async (url, init) => {
    const body = JSON.parse(init.body);
    requests.push({ url, body });
    const action = url.split("/").at(-1).split("?")[0];
    let value;
    if (action === "catalog")
      value = {
        hosts: [
          {
            alias: "lab",
            workspace: remote,
            sessions: [
              {
                sessionId: "session-lab",
                updatedAt: 2,
                cwd: remote.path,
                projections: {
                  kind: "sequenced",
                  asOfSeq: 1,
                  values: { title: "Saved session" },
                },
              },
            ],
          },
        ],
      };
    else if (action === "rpc") {
      if (body.endpoint === "session/create") {
        const sessionId = "session-created-" + ++createdCount;
        remote.sessionIds = [sessionId, ...remote.sessionIds];
        for (const controller of workspaceReaders.get("stream") || [])
          pushFrame(controller, { type: "upsert", workspace: remote });
        return Response.json({
          ok: true,
          value: { ok: true, value: { sessionId } },
        });
      }
      value = {
        ok: true,
        value:
          body.endpoint === "session/list"
            ? { items: [{ sessionId: "session-lab", updatedAt: 2 }] }
            : { accepted: true },
      };
    } else if (action === "local-stream" || action === "stream") {
      if (
        body.endpoint === "workspace/follow" &&
        action === "local-stream" &&
        failWorkspaceOpen
      ) {
        failWorkspaceOpen = false;
        throw new TypeError("network error opening stream");
      }
      let frame;
      if (body.endpoint === "workspace/follow")
        frame = {
          type: "baseline",
          value: {
            items:
              action === "local-stream"
                ? [{ workspaceId: "workspace-local", title: "local" }]
                : [remote],
            archivedSessionIds: [],
            pinnedSessionIds: [],
          },
        };
      else if (body.endpoint === "session/control") {
        const frames =
          action === "local-stream"
            ? [{ type: "baseline", value: { projections: {} } }]
            : [
                {
                  type: "baseline",
                  value: {
                    projections: {
                      "session-lab": { asOfSeq: 1, values: { title: null } },
                    },
                  },
                },
                {
                  type: "projection",
                  sessionId: "session-lab",
                  key: "title",
                  value: "Automatic title after first message",
                  seq: 2,
                },
              ];
        return new Response(
          new ReadableStream({
            start(controller) {
              for (const value of frames)
                controller.enqueue(
                  new TextEncoder().encode(
                    JSON.stringify({ type: "item", value }) + "\n",
                  ),
                );
              init.signal.addEventListener("abort", () => controller.close(), {
                once: true,
              });
            },
          }),
        );
      } else if (body.endpoint === "$events") {
        const frames = [
          {
            type: "ready",
            clientId: action === "stream" ? "client-lab" : "client-local",
            host: { home: "/workspace" },
          },
        ];
        if (action === "stream" && question)
          frames.push({
            type: "waterfall",
            event: "agent/user-questions",
            eventId: "question-original",
            agentId: "session-lab",
            request: { questions: [] },
          });
        return new Response(
          new ReadableStream({
            start(controller) {
              for (const value of frames)
                controller.enqueue(
                  new TextEncoder().encode(
                    JSON.stringify({ type: "item", value }) + "\n",
                  ),
                );
              init.signal.addEventListener("abort", () => controller.close(), {
                once: true,
              });
            },
          }),
        );
      } else
        frame = {
          type: "opening",
          records: [
            {
              event: {
                type: "user/message",
                seq: 1,
                data: { content: [{ type: "text", text: "hello" }] },
              },
            },
          ],
        };
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(
                JSON.stringify({ type: "item", value: frame }) + "\n",
              ),
            );
            let readers;
            if (body.endpoint === "workspace/follow") {
              readers = workspaceReaders.get(action);
              if (!readers) workspaceReaders.set(action, (readers = new Set()));
              readers.add(controller);
            }
            init.signal.addEventListener(
              "abort",
              () => {
                readers?.delete(controller);
                try {
                  controller.close();
                } catch {}
              },
              { once: true },
            );
          },
        }),
        { headers: { "content-type": "application/x-ndjson" } },
      );
    } else value = { recorded: true };
    return Response.json({ ok: true, value });
  };
  const originalCall = async (channel, endpoint) => ({
    ok: true,
    value:
      endpoint === "session/list"
        ? { items: [{ sessionId: "session-local", updatedAt: 1 }] }
        : { local: true },
  });
  const rpc = { call: originalCall };
  let reconnects = 0;
  const scoped = {
    connection: {
      rpc,
      generation: {
        getSnapshot: () => ({ id: 1 }),
        subscribe(fn) {
          generationListeners.add(fn);
          return () => generationListeners.delete(fn);
        },
      },
      reconnect() {
        reconnects++;
      },
    },
    remote: {},
    sessions: {
      handleControlFrame(frame) {
        accepted.push(frame);
      },
      list: {
        subscribe: () => () => {},
        getSnapshot: () => ({
          ids: ["session-local", "session-lab"],
          byId: {
            "session-lab": { id: "session-lab", retainedBy: { mainView: 1 } },
          },
        }),
      },
    },
    effect(fn) {
      disposers.push(fn());
    },
    get() {
      return undefined;
    },
  };
  scoped.inject = (deps, fn) => {
    injectedDependencies.push(Array.from(deps));
    if (deps.includes("fileUpload") || deps.includes("workspaces")) return;
    fn(scoped);
  };
  const source = readFileSync(
    new URL("../native/src/client.js", import.meta.url),
    "utf8",
  );
  vm.runInNewContext(source, {
    window: {
      __ModuleLoader__: {
        load(v) {
          registered = v.factory((name) =>
            name === "@deepseek-ai/dsh-api-session-controller/client"
              ? {
                  createSessionControlStream(remote, options) {
                    assert.equal(remote, scoped.remote);
                    const control = {
                      options,
                      started: false,
                      disposed: false,
                      restarts: 0,
                      start() {
                        this.started = true;
                      },
                      restart() {
                        this.restarts++;
                      },
                      async dispose() {
                        this.disposed = true;
                      },
                    };
                    controls.push(control);
                    return control;
                  },
                }
              : { RemoteStreamCarrierError: class extends Error {} },
          );
        },
      },
    },
    fetch: fetchImpl,
    crypto,
    AbortController,
    AbortSignal,
    ReadableStream,
    TextDecoder,
    TextEncoder,
    Uint8Array,
    atob,
    console,
    setTimeout,
    clearTimeout,
  });
  registered.apply(scoped);
  await new Promise((r) => setImmediate(r));
  return {
    rpc,
    requests,
    disposers,
    controls,
    accepted,
    injectedDependencies,
    loseLocalWorkspace({ failReopen = false } = {}) {
      failWorkspaceOpen = failReopen;
      for (const controller of workspaceReaders.get("local-stream") || [])
        controller.error(new TypeError("network error reading stream"));
    },
    replaceGeneration() {
      for (const fn of generationListeners) fn();
    },
    get reconnects() {
      return reconnects;
    },
  };
}
test("adapter reuses the original RPC for local calls and routes unchanged native requests by session identity", async () => {
  const f = await clientFixture();
  assert.equal(f.reconnects, 1);
  assert.deepEqual(await f.rpc.call("/api", "settings/read", { args: {} }), {
    ok: true,
    value: { local: true },
  });
  await f.rpc.call("/api", "session/prompt", {
    args: {
      request: {
        sessionId: "session-lab",
        requestId: "native-receipt",
        content: [{ type: "text", text: "hello" }],
      },
    },
  });
  const sent = f.requests.find((r) => r.body.endpoint === "session/prompt");
  assert.equal(sent.body.host, "lab");
  assert.equal(sent.body.payload.args.request.requestId, "native-receipt");
  f.disposers.forEach((d) => d?.());
});
test("native session list and workspace stream combine both sources without registering any UI slots", async () => {
  const f = await clientFixture();
  const list = await f.rpc.call("/api", "session/list", {
    args: { _request: {} },
  });
  assert.deepEqual(
    Array.from(list.value.items, (s) => s.sessionId),
    ["session-lab", "session-local"],
  );
  const abort = new AbortController();
  const frames = f.rpc
    .open("/api", "workspace/follow", { args: {} }, abort.signal)
    [Symbol.asyncIterator]();
  const first = await frames.next();
  assert.deepEqual(
    Array.from(first.value.value.items, (w) => w.title),
    ["local", "research"],
  );
  abort.abort();
  await frames.return();
  f.disposers.forEach((d) => d?.());
});
test("existing conversation receives the complete native journal unchanged", async () => {
  const f = await clientFixture(),
    abort = new AbortController();
  const frames = f.rpc
    .open(
      "/api",
      "session/follow",
      {
        args: {
          request: { address: { kind: "session", sessionId: "session-lab" } },
        },
      },
      abort.signal,
    )
    [Symbol.asyncIterator]();
  const first = await frames.next();
  assert.equal(first.value.type, "opening");
  assert.equal(first.value.records[0].event.data.content[0].text, "hello");
  abort.abort();
  await frames.return();
  f.disposers.forEach((d) => d?.());
});

test("shipped questions receive native frames and answers return to the owning Host", async () => {
  const f = await clientFixture({ question: true }),
    abort = new AbortController();
  const frames = f.rpc
    .open("/api", "$events", { args: {} }, abort.signal)
    [Symbol.asyncIterator]();
  const ready = await frames.next();
  assert.equal(ready.value.clientId, "client-local");
  const question = await frames.next();
  assert.equal(question.value.type, "waterfall");
  assert.equal(question.value.agentId, "session-lab");
  await f.rpc.call("/api", "$events/result", {
    args: {
      clientId: "client-local",
      eventId: question.value.eventId,
      outcome: { kind: "result", value: { answer: "confirmed" } },
    },
  });
  const reply = f.requests.find(
    (r) => r.body.endpoint === "$events/result",
  ).body;
  assert.equal(reply.host, "lab");
  assert.equal(reply.payload.args.clientId, "client-lab");
  assert.equal(reply.payload.args.eventId, "question-original");
  assert.equal(reply.payload.args.outcome.value.answer, "confirmed");
  abort.abort();
  await frames.return();
  f.disposers.forEach((d) => d?.());
});

test("local session streams use the composed local carrier when the shell has no custom stream hook", async () => {
  const f = await clientFixture(),
    abort = new AbortController();
  const frames = f.rpc
    .open(
      "/api",
      "session/follow",
      {
        args: {
          request: { address: { kind: "session", sessionId: "session-local" } },
        },
      },
      abort.signal,
    )
    [Symbol.asyncIterator]();
  assert.equal((await frames.next()).value.type, "opening");
  assert.equal(
    f.requests.find((r) => r.body.endpoint === "session/follow").url,
    "/native-remote/local-stream?confirm=1",
  );
  abort.abort();
  await frames.return();
  f.disposers.forEach((d) => d?.());
});

test("native binary RPC replies preserve attachment paths and bytes", async () => {
  const { transport } = fixture();
  transport.fetch = async (_url, init) => {
    const { rpcId } = JSON.parse(init.body),
      form = new FormData();
    form.append(
      "metadata",
      JSON.stringify({
        rpcId,
        result: { ok: true, value: { data: null } },
        attachments: [{ part: "bytes-0", path: ["data"], codec: "bytes" }],
      }),
    );
    form.append(
      "bytes-0",
      new Blob([new Uint8Array([0, 128, 255])]),
      "data.bin",
    );
    return new Response(form);
  };
  const result = await WorkspaceTransport.prototype.raw.call(
    transport,
    { url: "http://127.0.0.1", cookie: "test-only" },
    "files/readBytes",
    { args: {} },
  );
  assert.deepEqual(result.binaryParts, [
    { path: ["data"], data: Buffer.from([0, 128, 255]).toString("base64") },
  ]);
});

test("carrier replacement restores the shipped control stream and forwards live generated titles to its original Session sink", async () => {
  const f = await clientFixture();
  assert.ok(f.injectedDependencies[0].includes("remote.session"));
  assert.equal(f.controls.length, 1);
  const control = f.controls[0];
  assert.equal(control.started, true);
  const abort = new AbortController(),
    frames = f.rpc
      .open("/api", "session/control", { args: {} }, abort.signal)
      [Symbol.asyncIterator]();
  const baseline = (await frames.next()).value;
  assert.equal(baseline.type, "baseline");
  control.options.accept(baseline);
  control.options.accept((await frames.next()).value);
  const title = (await frames.next()).value;
  control.options.accept(title);
  assert.equal(f.accepted.at(-1), title);
  assert.equal(title.key, "title");
  assert.equal(title.value, "Automatic title after first message");
  assert.equal(title.seq, 2);
  f.replaceGeneration();
  assert.equal(control.restarts, 1);
  abort.abort();
  await frames.return();
  f.disposers.forEach((d) => d?.());
  assert.equal(control.disposed, true);
  f.replaceGeneration();
  assert.equal(control.restarts, 1);
});

test(
  "workspace plus creation preserves membership after local stream read and reopen failures",
  { timeout: 12000 },
  async () => {
    const f = await clientFixture(),
      abort = new AbortController();
    const frames = f.rpc
      .open("/api", "workspace/follow", { args: {} }, abort.signal)
      [Symbol.asyncIterator]();
    try {
      assert.equal((await frames.next()).value.type, "baseline");
      // The remote opening snapshot arrives as native increments.
      for (let i = 0; i < 3; i++) await frames.next();
      f.loseLocalWorkspace({ failReopen: true });
      const recovered = (await frames.next()).value;
      assert.equal(recovered.type, "upsert");
      assert.equal(recovered.workspace.workspaceId, "workspace-local");
      assert.equal((await frames.next()).value.type, "archived");
      assert.equal((await frames.next()).value.type, "pinned");
      const result = await f.rpc.call("/api", "session/create", {
        args: { request: { workspaceId: "workspace-lab" } },
      });
      const membership = (await frames.next()).value;
      assert.equal(membership.type, "upsert");
      assert.equal(membership.workspace.workspaceId, "workspace-lab");
      assert.ok(
        membership.workspace.sessionIds.includes(result.value.sessionId),
      );
      await f.rpc.call("/api", "session/prompt", {
        args: { request: { sessionId: result.value.sessionId } },
      });
      assert.equal(
        f.requests.find((r) => r.body.endpoint === "session/prompt").body.host,
        "lab",
      );
    } finally {
      abort.abort();
      await frames.return();
      f.disposers.forEach((d) => d?.());
    }
  },
);
