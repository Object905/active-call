# Side-Path Audio Distribution (Monitor)

The monitor provides a way to mirror **both legs' audio** to an external
consumer **without touching the main media path**: active-call dials out a
WebSocket connection and pushes the call's audio in real time. It works
independently of `media_pass` (media pass-through) — the two can be combined.

## Monitor vs. media_pass

| | media_pass | monitor |
|---|---|---|
| Connection | active-call dials out, **bidirectional bridge** | active-call dials out, **one-way push only** |
| Effect on media path | Takes it over entirely (audio is processed by the external service and played back) | None (passive mirror) |
| Audio direction | Uplink sent out, downlink played back | Both legs sent out |
| Failure impact | Media path breaks | Monitoring stops; the call is unaffected |

## Architecture

```
caller audio ──► caller track processor chain ──► MonitorProcessor(leg=1) ──┐
                                                                            ├──► MonitorHub ──► forwarder ──► ws://external
downlink audio ──► server track processor chain ──► MonitorProcessor(leg=0) ┘   (TTS / media_pass / ambiance)
```

- `MonitorProcessor` is attached per track at registration time
  (`update_track_wrapper`); media_pass tracks are tapped by reaching into the
  processor chain directly (same technique the recorder uses).
- Each processor is owner-filtered: audio merely relayed through another
  track's chain (e.g. the caller's audio forwarded inside the media-pass
  track) is never published twice.
- With no consumer attached, publishing is a near no-op.

## Configuration

Set it in `CallOption` (playbooks supported as well):

```yaml
monitor:
  url: "ws://asr-server:9000/audio"   # dial-out address
  bufferSize: 3200                    # optional: target bytes per binary frame (PCM16)
  ptime: 100                          # optional: flush interval in ms for partial buffers
  headers:                            # optional: extra HTTP headers for the WS handshake
    Authorization: "Bearer xxx"
    X-Tenant-ID: "t1"
```

### Buffering semantics

| Configuration | Behavior |
|---|---|
| neither set | Pass-through: every tapped frame (~20 ms) is pushed immediately |
| `bufferSize` only | Emit exact-size chunks as soon as they fill; the tail waits for more audio |
| `ptime` only | Flush on the timer; partial frames are sent, **never padded with silence** |
| both | Whichever triggers first |

## Wire protocol

### 1. Handshake

active-call dials `url` with a `session_id` query parameter appended (gateways
can route on it before the upgrade):

```
ws://asr-server:9000/audio?session_id=s.xxxx
```

Custom `headers` are added to the handshake request.

### 2. Metadata frame (first message, text)

```json
{
  "event": "monitor_start",
  "sessionId": "s.xxxx",
  "callType": "sip",
  "startedAt": "2026-10-27T10:00:00+08:00",
  "format": "pcm16/mono",
  "sampleRate": 16000,
  "legs": [
    { "leg": 0, "direction": "agent",  "sampleRate": 16000 },
    { "leg": 1, "direction": "caller", "sampleRate": 16000 }
  ],
  "extras": { "X-Tenant-ID": "t1" }
}
```

`extras` carries the call's custom variables (extracted SIP headers, playbook
variables, …).

### 3. Audio frames (subsequent binary messages)

```
[1-byte leg][PCM16 mono]
```

- `leg = 0`: agent side (downlink: TTS / media-pass downlink / ambiance)
- `leg = 1`: caller side (uplink)
- Sample rate is always 16000 Hz (the internal processing rate)

## Failure semantics

- **Single attempt**: a failed handshake, rejected upgrade, dropped connection
  or failed send logs a warning and gives up for the rest of the call — no
  retries.
- Audio during a disconnection is dropped (no buffering, no replay).
- A slow consumer only drops its own frames (bounded broadcast queue ≈ 4 s);
  the media path is never back-pressured.
- The connection is closed when the call ends (cancel token).

## Example

### Playbook

Top-level key in the playbook frontmatter (sibling of `asr`/`tts`/…):

```yaml
---
monitor:
  url: "ws://asr-server:9000/audio"
  bufferSize: 3200
  ptime: 100
  headers:
    Authorization: "Bearer xxx"
---
# Normal prompt content follows
...
```

### Consumer (Python)

active-call dials in, so the consumer only runs a WS server and waits:

```python
# pip install websockets
import asyncio, json, websockets

async def handler(ws):
    meta = json.loads(await ws.recv())      # first message: monitor_start metadata
    print("session:", meta["sessionId"])
    async for msg in ws:
        if isinstance(msg, bytes):
            leg, pcm = msg[0], msg[1:]      # leg: 0=agent, 1=caller; pcm16/16k/mono
            feed_to_asr(leg, pcm)           # per-leg ASR / QA / …

async def main():
    async with websockets.serve(handler, "0.0.0.0", 9000):
        await asyncio.Future()
```

> `monitor` can be combined with `media_pass`: media_pass drives the dialog on
> the main path while monitor mirrors both legs to ASR/QA consumers.
