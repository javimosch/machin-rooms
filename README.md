# machin-rooms — a self-hostable real-time chat server, in one MFL binary

Open the page, pick a room and a name, and chat **live over WebSockets** — messages,
join/leave notices, and a running member count, broadcast to everyone in the room. A
tiny self-hosted team chat / live-presence widget for an SME — one static native binary,
no message broker, no Node. Built in pure [MFL](https://github.com/javimosch/machin).

```bash
./build.sh                      # -> ./machin-rooms   (needs machin v0.146.1+ and a C compiler)
./machin-rooms                  # http://localhost:48500  — open it in two tabs and chat
```

## Why this exists

machin-rooms is a **dogfood**: building it is what drove a **WebSocket server** into
machin. machin already had a WebSocket *client* (machin-wscat); this is the symmetric
*server* half, so machin now does both ends of real-time. The whole protocol is pure MFL:

- the **upgrade handshake** — `Sec-WebSocket-Accept = base64(sha1(key + GUID))`;
- the **RFC 6455 frame codec** — unmasked server frames out, masked client frames in
  (the 4-byte XOR unmask), ping/pong + close — built and parsed over `bytes` with the
  bitwise builtins, no new primitive;
- it rides machweb's new generic **`hijack`** response: the handler returns
  `ws(req, fn)`, machweb hands over the raw socket, and `fn` speaks frames.

All shipped in [machin v0.79.0](https://github.com/javimosch/machin/blob/main/CHANGELOG.md).
See `machin guide --skill web` for the WebSocket how-to.

## How it works

Fan-out is the idiomatic **lock-free hub goroutine**: one `hub_run()` owns the `[]Sub`
room registry; `join` / `leave` / `say` commands arrive over a channel and it broadcasts
each message to every subscriber in the room (each subscriber has its own unbounded
delivery channel, so the hub never blocks). 

Each connection runs **two goroutines** over the shared socket — read and write are
independent directions:
- the **reader** (the `ws` handler loop) turns inbound frames into `say` commands;
- a spawned **writer** drains the connection's outbound channel and writes each broadcast
  as a WebSocket frame.

No broker, no locks, no shared mutable state — just goroutines and channels.

## Endpoints

| Path | Purpose |
|---|---|
| `GET /` | the chat web client (room + name, live message list) |
| `GET /ws?room=<r>&name=<n>` | the WebSocket endpoint (what the browser connects to) |
| `GET /_health` | `{"ok":true,"service":"machin-rooms"}` |

Each broadcast is a small JSON frame — `{"type":"msg","room","name","text"}` for chat,
`{"type":"sys","room","text","count"}` for join/leave + the live member count.

## Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `MACHROOMS_PORT` | `48500` | listen port |
| `MACHROOMS_TITLE` | `machin-rooms` | page heading |

## Talk to it from code

Any WebSocket client works — it's standards-compliant (verified against the Python
`websockets` reference client):

```python
import asyncio, websockets
async def main():
    async with websockets.connect("ws://localhost:48500/ws?room=ops&name=bot") as ws:
        await ws.send("deploy finished")
        print(await ws.recv())
asyncio.run(main())
```

## v1 scope (honest edges)

- **No auth / no history.** Anyone who can reach it can join any room; messages aren't
  persisted (a fresh tab starts empty). machin has signed sessions + SSO (gate `/ws`) and
  SQLite/Redis (persist + replay scrollback) if you want them — see
  `machin guide --skill backend`.
- **Text frames** (chat). Binary frames are received (`ws_send_bytes` exists) but the app
  treats messages as text.
- Behind a proxy use `wss://` (the client picks `ws`/`wss` from the page's scheme) and
  allow upgrades — see [machin-deploy](https://github.com/javimosch/machin-deploy) and
  `machin guide --skill deploy`.

Built with [machin](https://github.com/javimosch/machin) · part of
[awesome-machin](https://github.com/javimosch/awesome-machin).
