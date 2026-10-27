# 侧路音频分发（Monitor）

侧路监听提供了一种**不影响主媒体路径**的两路音频分发能力：active-call 主动拨出一个 WebSocket 连接，把通话双方的音频实时推送出去。无论通话是否启用了 `media_pass`（媒体透传），侧路分发都可以独立工作。

## 与 media_pass 的区别

| | media_pass | monitor 侧路 |
|---|---|---|
| 连接方向 | active-call 主动拨出，**双向桥接** | active-call 主动拨出，**纯单向推送** |
| 对媒体路径的影响 | 完全接管（音频经外部服务处理后再回来） | 无（只是旁路镜像） |
| 音频方向 | 上行送出 + 下行送回 | 两路都只送出 |
| 失败影响 | 媒体路径中断 | 仅监听停止，通话不受影响 |

## 架构

```
主叫音频 ──► caller track processor chain ──► MonitorProcessor(leg=1) ──┐
                                                                        ├──► MonitorHub ──► 推送任务 ──► ws://外部服务
下行音频 ──► server track processor chain ──► MonitorProcessor(leg=0) ──┘   (TTS / media_pass 均适用)
```

- 每个 track 注册时（`update_track_wrapper`）挂载 `MonitorProcessor`，media_pass track 通过直连 processor chain 的方式挂载（与录音机同一手法）
- `MonitorProcessor` 带 owner 过滤：media_pass chain 里透传的对端音频不会被重复分发
- 无监听消费者时，分发近似零开销

## 配置

在 `CallOption`（playbook 亦可）中配置：

```yaml
monitor:
  url: "ws://asr-server:9000/audio"   # 拨出地址
  bufferSize: 3200                    # 可选：每个二进制帧的目标字节数（PCM16）
  ptime: 100                          # 可选：部分缓冲的定时刷出间隔（毫秒）
  headers:                            # 可选：WS 握手附加 HTTP 头
    Authorization: "Bearer xxx"
    X-Tenant-ID: "t1"
```

### buffering 语义

| 配置 | 行为 |
|---|---|
| 都不设 | 透传：每个 tap 帧（约 20ms）立即推送 |
| 只设 `bufferSize` | 攒够即发，整块切分；尾部不足时等待后续音频 |
| 只设 `ptime` | 定时刷出，部分帧也发，**不补零**（监听不伪造静音） |
| 两者都设 | 先到者触发：整块到点即发，定时器刷出尾部 |

## 线路协议

### 1. 握手

active-call 向 `url` 发起 WebSocket 握手，URL 自动追加 `session_id` 查询参数（网关可在 upgrade 前完成会话路由）：

```
ws://asr-server:9000/audio?session_id=s.xxxx
```

自定义 `headers` 附加在握手请求上。

### 2. 元数据帧（首条 text 消息）

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

`extras` 为通话的自定义变量（SIP 提取头、playbook 变量等）。

### 3. 音频帧（后续 binary 消息）

```
[1 字节 leg][PCM16 mono]
```

- `leg = 0`：agent 侧（下行：TTS / media_pass 下行 / 背景音）
- `leg = 1`：主叫侧（上行）
- 采样率固定 16000 Hz（内部处理采样率）

## 失败语义

- **只尝试一次**：握手失败、升级被拒、连接中断、发送失败 —— 记录 warn 日志后本次通话放弃推送，不重试
- 断连期间的音频直接丢弃（不缓存、不回放）
- 消费端过慢时只丢自己的帧（内部 broadcast 有界队列 ≈ 4 秒），不会反压主媒体链路
- 通话结束（cancel token 触发）时主动关闭连接

## 使用示例

### Playbook

playbook frontmatter 中顶层配置（与 `asr`/`tts` 等同级）：

```yaml
---
monitor:
  url: "ws://asr-server:9000/audio"
  bufferSize: 3200
  ptime: 100
  headers:
    Authorization: "Bearer xxx"
---
# 在此之后是正常的 prompt 内容
...
```

### 消费端（Python）

active-call 主动拨入，消费端只需起一个 WS 服务等待连接：

```python
# pip install websockets
import asyncio, json, websockets

async def handler(ws):
    meta = json.loads(await ws.recv())      # 首条: monitor_start 元数据
    print("session:", meta["sessionId"])
    async for msg in ws:
        if isinstance(msg, bytes):
            leg, pcm = msg[0], msg[1:]      # leg: 0=agent, 1=caller; pcm16/16k/mono
            feed_to_asr(leg, pcm)           # 按腿送入 ASR / 质检等

async def main():
    async with websockets.serve(handler, "0.0.0.0", 9000):
        await asyncio.Future()
```

> `monitor` 与 `media_pass` 可同时使用：media_pass 负责对话主路径，monitor 把两路音频旁路给 ASR/质检等消费端。
