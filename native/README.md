# Workspace transport composition

This companion connects an existing managed Host to the desktop's **existing** Workspace Controller, Session Controller and generated Remote API. It registers no UI slots and supplies no chat, composer, tool-card or sidebar renderer. A workspace keeps its ordinary title and sessions appear in the standard workspace tree.

The shipped conversation consumes the remote session's original journal and live stream. The shipped APIs handle prompt admission, cancellation, model/permission selection, user questions, approvals, forks, archive/pin operations and file/terminal tools. Only the transport chooses which Host owns an identity. Session execution and history remain on that Host; closing a local view cancels its observation stream, never the Agent.

Install from one source checkout, retaining the parent and companion directories together:

```sh
dsh plugin --profile desktop add --link <repo>
dsh plugin --profile desktop add --link <repo>/native
```

Installing the companion switches the parent to background mode, disabling its legacy frontend panels and composer dock.

The companion imports the parent manager to reuse SSH discovery, remote installation, systemd lifecycle, port registration, heartbeat and reconnection. Configure the alias in the parent tunnel bundle, then add its existing workspace to the desktop profile patch:

```yaml
- id: native-remote
  config:
    hosts:
      - alias: lab
        workspace: /workspace/project
```

Authentication stays in the desktop Host. The companion relays generated API values and transport frames without embedding the remote cookie or launch token. Local relay requests require the existing Harness admission plus explicit POST. Session and workspace ownership is validated before forwarding; conflicting local and remote session identities are rejected. The local Host's original APIs remain responsible for local operations. Workspace feeds and Session control/event streams combine both sources without copying conversation storage.

The adapter uses the public Connection RPC carrier, Gateway wire-stream API and public client-module lifecycle. It requires a Harness version exposing these extension points and matching Remote protocol versions on both Hosts. A host with no connection is retried without blocking local work. Reconnection does not resend prompts; request IDs and admission behavior belong to Harness's existing Session API.

Run the root test command for routing isolation, exact journal relay, lifecycle, and existing-UI composition tests. Desktop acceptance must also exercise the actual standard workspace tree, chat send/receive, permissions/questions, local-session switching and execution across transport loss.
