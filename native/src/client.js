/* Transport composition only. No UI slots, React components, or conversation storage. */
window.__ModuleLoader__.load({
  id: "dsh-native-remote",
  factory(require) {
    "use strict";
    let RemoteStreamCarrierError;
    try {
      RemoteStreamCarrierError =
        require("@deepseek-ai/dsh-api-gateway/client").RemoteStreamCarrierError;
    } catch (error) {
      void fetch("/native-remote/diagnostic?confirm=1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ stage: "module: " + error.message }),
      });
      throw error;
    }
    const GLOBAL_STREAMS = new Set([
      "$events",
      "workspace/follow",
      "session/control",
    ]);
    const SESSION_GLOBALS = new Set([
      "session/modelCatalog",
      "permissionPresets/catalog",
    ]);
    function queue() {
      const values = [],
        waiters = [];
      let ended = false,
        error;
      return {
        push(value) {
          if (ended) return;
          const waiter = waiters.shift();
          waiter ? waiter.resolve({ value, done: false }) : values.push(value);
        },
        end(reason) {
          if (ended) return;
          ended = true;
          error = reason;
          waiters
            .splice(0)
            .forEach((w) =>
              reason ? w.reject(reason) : w.resolve({ done: true }),
            );
        },
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          if (values.length)
            return Promise.resolve({ value: values.shift(), done: false });
          if (error) return Promise.reject(error);
          if (ended) return Promise.resolve({ done: true });
          return new Promise((resolve, reject) =>
            waiters.push({ resolve, reject }),
          );
        },
      };
    }
    async function api(action, body, signal) {
      const response = await fetch("/native-remote/" + action + "?confirm=1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body || {}),
        signal,
      });
      const result = await response.json();
      if (!response.ok || !result.ok)
        throw new RemoteStreamCarrierError(
          result.error || "Workspace transport unavailable",
        );
      return result.value;
    }
    async function* stream(action, host, endpoint, payload, signal, uplink) {
      const streamId = crypto.randomUUID();
      const abort = new AbortController(),
        combined = AbortSignal.any([signal, abort.signal]);
      let response;
      try {
        response = await fetch("/native-remote/" + action + "?confirm=1", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ host, endpoint, payload, streamId }),
          signal: combined,
        });
      } catch (error) {
        if (combined.aborted) throw error;
        throw new RemoteStreamCarrierError(error.message);
      }
      if (!response.ok)
        throw new RemoteStreamCarrierError(
          "Workspace stream unavailable: HTTP " + response.status,
        );
      void api("diagnostic", {
        stage: action + " " + endpoint + " headers",
      }).catch(() => {});
      const reader = response.body.getReader(),
        decoder = new TextDecoder();
      let buffer = "",
        first = true;
      if (uplink)
        void (async () => {
          try {
            for await (const value of uplink) {
              if (combined.aborted) return;
              await api("uplink", { streamId, value }, combined);
            }
            if (!combined.aborted)
              await api("uplink", { streamId, end: true }, combined);
          } catch (error) {
            if (!combined.aborted) abort.abort(error);
          }
        })();
      try {
        while (!combined.aborted) {
          let next;
          try {
            next = await reader.read();
          } catch (error) {
            if (combined.aborted) throw error;
            throw new RemoteStreamCarrierError(error.message);
          }
          if (next.done)
            throw new RemoteStreamCarrierError("Workspace stream ended");
          buffer += decoder.decode(next.value, { stream: true });
          let at;
          while ((at = buffer.indexOf("\n")) !== -1) {
            const line = buffer.slice(0, at);
            buffer = buffer.slice(at + 1);
            if (!line) continue;
            const frame = JSON.parse(line);
            if (first) {
              first = false;
              void api("diagnostic", {
                stage:
                  action +
                  " " +
                  endpoint +
                  " first " +
                  frame.type +
                  "/" +
                  frame.value?.type,
              }).catch(() => {});
            }
            if (frame.type === "item") yield frame.value;
            else if (frame.type === "end") return;
            else
              throw Object.assign(
                new RemoteStreamCarrierError(frame.error.message),
                { code: frame.error.code, details: frame.error.details },
              );
          }
        }
      } finally {
        abort.abort();
        await reader.cancel().catch(() => {});
      }
    }
    function apply(ctx) {
      const dependencies = [
        "connection",
        "remote",
        "remote.session",
        "sessions",
      ];
      ctx.inject(dependencies, (scoped) => {
        void api("diagnostic", { stage: "transport mounted" });
        const connection = scoped.connection,
          rpc = connection.rpc,
          originalCall = rpc.call.bind(rpc),
          originalOpen = rpc.open?.bind(rpc);
        const owners = new Map(),
          workspaceOwners = new Map(),
          hosts = new Map(),
          events = new Map();
        let disposed = false,
          installed = false;
        const abort = new AbortController();
        let control, unsubscribeGeneration;

        function remember(alias, workspace, sessions) {
          if (workspace) {
            workspaceOwners.set(workspace.workspaceId, alias);
            hosts.set(alias, { ...(hosts.get(alias) || {}), workspace });
          }
          if (sessions) {
            const entry = hosts.get(alias);
            if (entry) entry.sessions = sessions;
            for (const s of sessions) owners.set(s.sessionId, alias);
          }
        }
        function activeHost() {
          const rows = scoped.sessions.list.getSnapshot().byId;
          for (const row of Object.values(rows))
            if (row.retainedBy?.mainView > 0) return owners.get(row.id);
        }
        function target(endpoint, args) {
          let found;
          function visit(value) {
            if (typeof value === "string") {
              const owner = owners.get(value) || workspaceOwners.get(value);
              if (owner) {
                if (found && found !== owner)
                  throw new Error(
                    "A request cannot span workspace connections",
                  );
                found = owner;
              }
            } else if (value && typeof value === "object")
              Object.values(value).forEach(visit);
          }
          visit(args);
          if (!found && endpoint === "workspace/create")
            for (const [alias, host] of hosts)
              if (host.workspace?.path === args.request?.path) found = alias;
          return (
            found || (SESSION_GLOBALS.has(endpoint) ? activeHost() : undefined)
          );
        }
        async function remoteCall(alias, endpoint, payload, signal) {
          const result = await api(
            "rpc",
            { host: alias, endpoint, payload },
            signal,
          );
          for (const part of result.binaryParts || []) {
            let parent = result.value;
            for (const key of part.path.slice(0, -1)) parent = parent[key];
            const value = Uint8Array.from(atob(part.data), (c) =>
              c.charCodeAt(0),
            );
            if (part.path.length) parent[part.path.at(-1)] = value;
            else result.value = value;
          }
          delete result.binaryParts;
          if (result.ok) {
            const value = result.value;
            if (value?.sessionId) owners.set(value.sessionId, alias);
            if (value?.workspace) remember(alias, value.workspace);
            if (endpoint === "session/list") remember(alias, null, value.items);
          }
          if (result.ok && endpoint.startsWith("workspace/")) {
            let snapshot;
            try {
              snapshot = scoped.get("workspaces")?.list.getSnapshot();
            } catch {}
            for (const field of ["archivedSessionIds", "pinnedSessionIds"])
              if (result.value?.[field] && snapshot)
                result.value[field] = [
                  ...snapshot[field].filter((id) => owners.get(id) !== alias),
                  ...result.value[field],
                ];
            if (result.value?.workspaceIds && snapshot)
              result.value.workspaceIds = [
                ...snapshot.items
                  .filter((w) => workspaceOwners.get(w.workspaceId) !== alias)
                  .map((w) => w.workspaceId),
                ...result.value.workspaceIds,
              ];
          }
          return result;
        }
        async function call(channel, endpoint, payload, signal) {
          if (channel !== "/api")
            return originalCall(channel, endpoint, payload, signal);
          if (endpoint === "$events/result") {
            const route = events.get(payload.args?.eventId);
            if (route) {
              const result = await remoteCall(
                route.alias,
                endpoint,
                {
                  args: {
                    ...payload.args,
                    eventId: route.eventId,
                    clientId: route.clientId,
                  },
                },
                signal,
              );
              events.delete(payload.args.eventId);
              return result;
            }
          }
          const alias = target(endpoint, payload.args);
          if (alias) return remoteCall(alias, endpoint, payload, signal);
          if (endpoint === "session/list")
            void api("diagnostic", {
              stage: "session list intercepted",
            }).catch(() => {});
          const local = await originalCall(channel, endpoint, payload, signal);
          if (!local.ok) return local;
          if (endpoint === "session/search") {
            const results = await Promise.all(
              [...hosts.keys()].map((alias) =>
                remoteCall(alias, endpoint, payload, signal).catch(() => null),
              ),
            );
            return {
              ...local,
              value: {
                ...local.value,
                items: [
                  ...local.value.items,
                  ...results.flatMap((r) => (r?.ok ? r.value.items : [])),
                ],
                hasMore:
                  local.value.hasMore || results.some((r) => r?.value?.hasMore),
              },
            };
          }
          if (endpoint === "session/list") {
            const localIds = new Set(local.value.items.map((s) => s.sessionId));
            const extra = await Promise.all(
              [...hosts.keys()].map(async (alias) => {
                try {
                  return (
                    (await remoteCall(alias, endpoint, payload, signal)).value
                      ?.items || []
                  );
                } catch {
                  return hosts.get(alias).sessions || [];
                }
              }),
            );
            const items = extra.flat();
            void api("diagnostic", {
              stage: "session list merged remote=" + items.length,
            }).catch(() => {});
            if (items.some((s) => localIds.has(s.sessionId)))
              throw new Error("Workspace session identity collision");
            return {
              ...local,
              value: {
                ...local.value,
                items: [...local.value.items, ...items].sort(
                  (a, b) => b.updatedAt - a.updatedAt,
                ),
              },
            };
          }
          return local;
        }
        function cacheProjections() {
          const result = {};
          for (const host of hosts.values())
            for (const s of host.sessions || [])
              if (s.projections?.kind === "sequenced")
                result[s.sessionId] = {
                  asOfSeq: s.projections.asOfSeq,
                  values: s.projections.values,
                };
          return result;
        }
        async function* merged(endpoint, payload, signal, uplink) {
          const inbox = queue(),
            controller = new AbortController(),
            combined = AbortSignal.any([signal, controller.signal]),
            states = new Map();
          const stop = () => {
            inbox.end();
            controller.abort();
          };
          signal.addEventListener("abort", stop, { once: true });
          const pump = async (alias) => {
            do {
              try {
                for await (const value of stream(
                  alias ? "stream" : "local-stream",
                  alias,
                  endpoint,
                  payload,
                  combined,
                  alias ? undefined : uplink,
                ))
                  inbox.push({ alias, value });
                if (!alias && !combined.aborted)
                  throw new RemoteStreamCarrierError("Local stream ended");
              } catch (error) {
                if (combined.aborted) return;
                if (!alias && !(error instanceof RemoteStreamCarrierError)) {
                  inbox.end(error);
                  return;
                }
                console.warn("[workspace transport]", error.message);
                void api("diagnostic", {
                  stage: endpoint + ": " + error.message,
                }).catch(() => {});
              }
              // Keep the composed observation alive across local carrier loss too.
              // Ending it would permanently close the shipped Workspace consumer.
              if (combined.aborted) return;
              await new Promise((resolve) => {
                const timer = setTimeout(resolve, 2000);
                combined.addEventListener(
                  "abort",
                  () => {
                    clearTimeout(timer);
                    resolve();
                  },
                  { once: true },
                );
              });
            } while (!combined.aborted);
          };
          const jobs = [pump(null), ...[...hosts.keys()].map(pump)];
          let localReady = false;
          const deferred = [];
          const allSet = (field) => [
            ...new Set([...states.values()].flatMap((v) => v[field] || [])),
          ];
          try {
            for await (const item of inbox) {
              const { alias, value } = item;
              if (alias && !localReady) {
                deferred.push(item);
                continue;
              }
              if (
                !alias &&
                !localReady &&
                ((endpoint === "$events" && value.type === "ready") ||
                  (endpoint !== "$events" && value.type === "baseline"))
              ) {
                localReady = true;
                if (endpoint === "$events") yield value;
                else if (endpoint === "session/control")
                  yield {
                    ...value,
                    value: {
                      ...value.value,
                      projections: {
                        ...value.value.projections,
                        ...cacheProjections(),
                      },
                    },
                  };
                else {
                  states.set(null, value.value);
                  const remote = [...hosts.values()]
                    .map((h) => h.workspace)
                    .filter(Boolean);
                  yield {
                    ...value,
                    value: {
                      ...value.value,
                      items: [...value.value.items, ...remote],
                    },
                  };
                }
                for (const pending of deferred.splice(0)) inbox.push(pending);
                continue;
              }
              if (endpoint === "$events") {
                if (alias && value.type === "ready") {
                  states.set(alias, { clientId: value.clientId });
                  continue;
                }
                if (alias && value.type === "waterfall") {
                  void api("diagnostic", {
                    stage: "interaction " + value.event,
                  }).catch(() => {});
                  const eventId = "workspace:" + alias + ":" + value.eventId;
                  events.set(eventId, {
                    alias,
                    eventId: value.eventId,
                    clientId: states.get(alias)?.clientId,
                  });
                  owners.set(value.agentId, alias);
                  yield { ...value, eventId };
                  continue;
                }
                if (alias && value.type === "cancel") {
                  const eventId = "workspace:" + alias + ":" + value.eventId;
                  events.delete(eventId);
                  yield { ...value, eventId };
                  continue;
                }
                if (alias && value.type === "emit") {
                  if (value.event === "api-session/added") {
                    const summary = value.args[0];
                    if (summary.cwd !== hosts.get(alias).workspace.path)
                      continue;
                    owners.set(summary.sessionId, alias);
                  }
                  if (
                    !/^(api-session\/|commands\/change|goal\/|subagent)/.test(
                      value.event,
                    )
                  )
                    continue;
                }
                yield value;
              } else if (endpoint === "session/control") {
                if (value.type === "baseline") {
                  for (const [sessionId, p] of Object.entries(
                    value.value.projections,
                  )) {
                    if (alias) owners.set(sessionId, alias);
                    for (const [key, v] of Object.entries(p.values))
                      yield {
                        type: "projection",
                        sessionId,
                        key,
                        value: v,
                        seq: p.asOfSeq,
                      };
                  }
                } else yield value;
              } else {
                if (value.type === "baseline") {
                  // A physical reconnect is a fresh snapshot, but the logical
                  // stream has already opened. Reconcile it as native increments.
                  const previous = states.get(alias)?.items || [];
                  states.set(alias, value.value);
                  for (const workspace of previous)
                    if (
                      !value.value.items.some(
                        (w) => w.workspaceId === workspace.workspaceId,
                      )
                    )
                      yield {
                        type: "remove",
                        workspaceId: workspace.workspaceId,
                      };
                  for (const workspace of value.value.items) {
                    if (alias) remember(alias, workspace);
                    yield { type: "upsert", workspace };
                  }
                  yield {
                    type: "archived",
                    archivedSessionIds: allSet("archivedSessionIds"),
                  };
                  yield {
                    type: "pinned",
                    pinnedSessionIds: allSet("pinnedSessionIds"),
                  };
                } else if (value.type === "upsert") {
                  if (alias) remember(alias, value.workspace);
                  const state = states.get(alias);
                  if (state)
                    states.set(alias, {
                      ...state,
                      items: state.items.some(
                        (w) => w.workspaceId === value.workspace.workspaceId,
                      )
                        ? state.items.map((w) =>
                            w.workspaceId === value.workspace.workspaceId
                              ? value.workspace
                              : w,
                          )
                        : [...state.items, value.workspace],
                    });
                  yield value;
                } else if (value.type === "remove") {
                  const state = states.get(alias);
                  if (state)
                    states.set(alias, {
                      ...state,
                      items: state.items.filter(
                        (w) => w.workspaceId !== value.workspaceId,
                      ),
                    });
                  yield value;
                } else if (value.type === "order") {
                  const local =
                    states.get(null)?.items?.map((w) => w.workspaceId) || [];
                  yield {
                    ...value,
                    workspaceIds: alias
                      ? [
                          ...local,
                          ...[...hosts.values()].map(
                            (h) => h.workspace.workspaceId,
                          ),
                        ]
                      : [
                          ...value.workspaceIds,
                          ...[...hosts.values()].map(
                            (h) => h.workspace.workspaceId,
                          ),
                        ],
                  };
                } else if (
                  value.type === "archived" ||
                  value.type === "pinned"
                ) {
                  const field =
                    value.type === "archived"
                      ? "archivedSessionIds"
                      : "pinnedSessionIds";
                  states.set(alias, {
                    ...(states.get(alias) || {}),
                    [field]: value[field],
                  });
                  yield { ...value, [field]: allSet(field) };
                } else yield value;
              }
            }
          } finally {
            controller.abort();
            inbox.end();
            signal.removeEventListener("abort", stop);
            await Promise.allSettled(jobs);
          }
        }
        function open(channel, endpoint, payload, signal, uplink) {
          signal = AbortSignal.any([signal, abort.signal]);
          void api("diagnostic", { stage: "open " + endpoint }).catch(() => {});
          if (channel !== "/api")
            return originalOpen?.(channel, endpoint, payload, signal, uplink);
          if (GLOBAL_STREAMS.has(endpoint))
            return merged(endpoint, payload, signal, uplink);
          const alias = target(endpoint, payload.args);
          return alias
            ? stream("stream", alias, endpoint, payload, signal, uplink)
            : originalOpen
              ? originalOpen(channel, endpoint, payload, signal, uplink)
              : stream("local-stream", null, endpoint, payload, signal, uplink);
        }
        async function refreshModule(name) {
          const loader = scoped.get("loader");
          const entry =
            loader &&
            [...loader.entries()].find((value) => value.options.name === name);
          if (!entry) return;
          const {
            tearDownEntryFiber,
          } = require("@deepseek-ai/dsh-client-modules/client");
          await tearDownEntryFiber(entry);
          await entry.refresh();
          await entry.fiber?.await();
        }
        let lastCatalogStatus = "",
          catalogHost,
          refreshing = Promise.resolve();
        function syncCatalogHost() {
          const alias = activeHost();
          if (!installed || alias === catalogHost) return;
          catalogHost = alias;
          // The shipped model directory caches one Host catalog. Its public module
          // lifecycle invalidates that cache when the selected Host changes.
          refreshing = refreshing
            .then(() =>
              disposed
                ? undefined
                : refreshModule("@deepseek-ai/dsh-client-ui-model-selection"),
            )
            .catch(console.error);
        }

        const reportCatalog = () => {
          syncCatalogHost();
          const state = scoped.sessions.list.getSnapshot();
          const status =
            "catalog ids=" +
            state.ids.length +
            " imported=" +
            state.ids.filter((id) => owners.has(id)).length +
            " error=" +
            (state.error?.message || "none");
          if (status !== lastCatalogStatus) {
            lastCatalogStatus = status;
            void api("diagnostic", { stage: status }).catch(() => {});
          }
        };
        const unsubscribeCatalog =
          scoped.sessions.list.subscribe(reportCatalog);
        scoped.effect(
          () => () => {
            unsubscribeCatalog();
            disposed = true;
            abort.abort();
            unsubscribeGeneration?.();
            void control?.dispose();
            if (installed) {
              rpc.call = originalCall;
              if (originalOpen) rpc.open = originalOpen;
              else delete rpc.open;
              connection.reconnect();
            }
          },
          "workspace-transport.client",
        );
        void (async () => {
          let catalog;
          while (!disposed) {
            catalog = await api("catalog", {}, abort.signal);
            if (catalog.hosts.some((h) => h.workspace) || !catalog.hosts.length)
              break;
            await new Promise((resolve) => {
              const timer = setTimeout(resolve, 5000);
              abort.signal.addEventListener(
                "abort",
                () => {
                  clearTimeout(timer);
                  resolve();
                },
                { once: true },
              );
            });
          }
          if (disposed) return;
          for (const host of catalog.hosts)
            if (host.workspace)
              remember(host.alias, host.workspace, host.sessions);
          if (!hosts.size) return;
          rpc.call = call;
          rpc.open = open;
          installed = true;
          // The original control consumer may already have finished on the old
          // carrier. Reuse the shipped stream and its existing Session sink so
          // generated titles and other projections keep arriving after install.
          const {
            createSessionControlStream,
          } = require("@deepseek-ai/dsh-api-session-controller/client");
          control = createSessionControlStream(scoped.remote, {
            accept: (frame) => scoped.sessions.handleControlFrame(frame),
            failed: (error) =>
              console.error("[workspace transport] session control:", error),
          });
          unsubscribeGeneration = connection.generation?.subscribe(() => {
            if (connection.generation.getSnapshot() !== undefined)
              control.restart();
          });
          control.start();
          // Reopen the shipped Workspace Controller on the composed carrier.
          // Its existing UI dependents remount through the normal module lifecycle.
          await refreshModule("@deepseek-ai/dsh-api-workspace-controller");
          syncCatalogHost();
          void api("diagnostic", {
            stage:
              "transport installed: " +
              hosts.size +
              " open=" +
              String(scoped.connection.rpc.open === open) +
              " call=" +
              String(scoped.connection.rpc.call === call),
          });
          connection.reconnect();
        })().catch((error) => {
          if (!disposed) {
            console.error("[workspace transport]", error);
            void api("diagnostic", {
              stage: "install: " + error.message,
            }).catch(() => {});
          }
        });
      });
    }
    return { apply };
  },
});
