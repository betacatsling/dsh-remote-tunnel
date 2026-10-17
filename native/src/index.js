import Schema from "@deepseek-ai/schemastery";
import { join } from "node:path";
import { homedir } from "node:os";
import { TunnelManager } from "../../src/manager.js";
import { WorkspaceTransport, registerTransportRoutes } from "./transport.js";
export const name = "workspace-transport";
export const Config = Schema.object({
  hosts: Schema.array(
    Schema.object({ alias: Schema.string(), workspace: Schema.string() }),
  ),
});
export function apply(ctx, config) {
  const manager = new TunnelManager({
    home: join(
      process.env.DSH_HOME ?? join(homedir(), ".dsh"),
      "remote-tunnel",
    ),
    reporter: { out() {}, err() {}, event() {} },
  });
  const transport = new WorkspaceTransport(manager, config?.hosts ?? []);
  ctx.effect(() => () => transport.dispose(), "workspace-transport.lifecycle");
  ctx.inject(["clientModules"], (scoped) => {
    for (const id of ["dsh-native-remote", "dsh-remote-tunnel"])
      scoped.get("clientModules").rebuilt?.(id);
  });
  ctx.inject(["webServer", "connection", "typertGateway"], (scoped) =>
    registerTransportRoutes(scoped, transport),
  );
}
