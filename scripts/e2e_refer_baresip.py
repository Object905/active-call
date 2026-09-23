#!/usr/bin/env python3
"""
End-to-end incoming-REFER test driven by a REAL SIP client: baresip.

baresip calls the active-call server (webhook + websocket accept), then sends
an in-dialog REFER (`/transfer`) to move the call to a sipbot `wait` target.
Asserts the full RFC 3515 flow on the baresip SIP trace:

    INVITE -> 200 OK -> REFER -> 202 -> NOTIFY(100 Trying)
           -> refer INVITE -> 200 OK -> NOTIFY(200 OK, terminated)
           -> BYE (parent dialog, after the target hangs up)

Usage:
    python3 scripts/e2e_refer_baresip.py [--release] [--sipbot PATH]
                                         [--binary PATH] [--keep-logs] [-v]

Requires: baresip (brew install baresip), sipbot, python3 + websockets.
"""

import argparse
import asyncio
import json
import queue
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def free_udp_port() -> int:
    import socket

    # Stay below the OS ephemeral range (macOS: 49152+) — see e2e_sip.py.
    for _ in range(20):
        port = random.randint(20000, 45000)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                return port
            except OSError:
                continue
    raise RuntimeError("no free UDP port found")


class WebhookServer:
    """Receives the server's invite notifications and queues them."""

    def __init__(self, port: int):
        self.port = port
        self.invites: "queue.Queue[dict]" = queue.Queue()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(length) or b"{}")
                if payload.get("event") == "invite":
                    outer.invites.put(payload)
                body = json.dumps({"status": "ok"}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # silence
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()


async def next_invite(webhook: WebhookServer, timeout: float = 15.0) -> dict:
    loop = asyncio.get_running_loop()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return await loop.run_in_executor(None, webhook.invites.get, True, 0.5)
        except queue.Empty:
            continue
    raise TimeoutError("no invite webhook received")


def build_server(release: bool) -> Path:
    binary = REPO / ("target/release/active-call" if release else "target/debug/active-call")
    if not binary.exists():
        profile = "--release" if release else ""
        print(f"building server ({'release' if release else 'debug'})...")
        subprocess.run(f"cargo build {profile}".split(), cwd=REPO, check=True)
    return binary


def start_server(binary: Path, workdir: Path, ports: dict) -> subprocess.Popen:
    config = f"""
addr = "0.0.0.0"
udp_port = {ports['sip']}
http_addr = "127.0.0.1:{ports['http']}"
accept_timeout = "120s"
media_cache_path = "{workdir}/media_cache"
rtp_start_port = {ports['rtp0']}
rtp_end_port = {ports['rtp1']}
graceful_shutdown = true
log_level = "info"

[handler]
type = "webhook"
url = "http://127.0.0.1:{ports['webhook']}/invite"
method = "POST"
"""
    conf_path = workdir / "e2e.toml"
    conf_path.write_text(config)
    log = open(workdir / "server.log", "w")
    return subprocess.Popen(
        [str(binary), "--conf", str(conf_path)],
        stdout=log,
        stderr=subprocess.STDOUT,
        cwd=workdir,
    )


def lan_ip() -> str:
    """baresip refuses loopback as a local address, so run the SIP legs on
    the LAN IP (the machine's primary interface)."""
    try:
        out = subprocess.run(
            ["ipconfig", "getifaddr", "en0"], capture_output=True, text=True
        )
        ip = out.stdout.strip()
        if ip:
            return ip
    except OSError:
        pass
    return socket.gethostbyname(socket.gethostname())


def make_baresip_conf(workdir: Path, lan: str) -> Path:
    """Create a headless baresip config dir (no audio devices needed)."""
    conf = workdir / "baresip"
    conf.mkdir(exist_ok=True)
    # Bootstrap the default template, then override for headless operation.
    subprocess.run(
        ["baresip", "-f", str(conf), "-t", "1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=30,
    )
    config = conf / "config"
    text = config.read_text() if config.exists() else ""
    text += (
        "\n# headless e2e overrides\n"
        "audio_player none,0\n"
        "audio_source none,0\n"
        "audio_alert none,0\n"
        "sip_listen 0.0.0.0:0\n"
    )
    config.write_text(text)
    # A local, unregistered account — calls are made by URI directly.
    (conf / "accounts").write_text(f"<sip:baresip@{lan}>;regint=0\n")
    return conf


class BaresipClient:
    """Drives baresip via stdin and watches the SIP trace on stderr."""

    def __init__(self, conf_dir: Path, log_path: Path):
        self.log_path = log_path
        self.log = open(log_path, "w")
        self.proc = subprocess.Popen(
            ["baresip", "-f", str(conf_dir), "-4", "-s", "-v"],
            stdin=subprocess.PIPE,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.trace: list[str] = []

    def cmd(self, command: str):
        assert self.proc.stdin is not None
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()

    async def wait_trace(self, needle: str, timeout: float = 20.0) -> bool:
        """Poll the SIP trace until `needle` appears (or timeout)."""
        deadline = time.monotonic() + timeout
        pos = 0
        while time.monotonic() < deadline:
            await asyncio.sleep(0.2)
            try:
                text = Path(self.log_path).read_text(errors="replace")
            except OSError:
                continue
            new = text[pos:]
            pos = len(text)
            for line in new.splitlines():
                self.trace.append(line)
                if needle in line:
                    return True
        return False

    def stop(self):
        try:
            if self.proc.stdin and not self.proc.stdin.closed:
                self.proc.stdin.write("/quit\n")
                self.proc.stdin.flush()
        except (OSError, AssertionError):
            pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def output(self) -> str:
        return Path(self.log_path).read_text(errors="replace")


async def wait_for_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        await asyncio.sleep(0.3)
    raise TimeoutError(f"server did not become ready at {url}")


async def run(args) -> bool:
    binary = Path(args.binary) if args.binary else build_server(args.release)
    if shutil.which("baresip") is None:
        print("baresip not found (brew install baresip); skipping")
        return True  # skip, not fail

    workdir = Path(tempfile.mkdtemp(prefix="active-call-baresip-e2e-"))
    ports = {
        "sip": free_udp_port(),
        "http": free_port(),
        "webhook": free_port(),
    }
    ports["rtp0"] = 31000 + random.randint(0, 200) * 50
    ports["rtp1"] = ports["rtp0"] + 2000

    webhook = WebhookServer(ports["webhook"])
    webhook.start()
    server = start_server(binary, workdir, ports)

    sipbot_bin = args.sipbot or shutil.which("sipbot") or "sipbot"
    lan = lan_ip()
    target_port = free_udp_port()
    target = subprocess.Popen(
        [
            sipbot_bin,
            "wait",
            "--addr",
            f"0.0.0.0:{target_port}",
            "--answer",
            str(REPO / "fixtures" / "sample.wav"),
            "--hangup",
            "6",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    ok = False
    try:
        await wait_for_http(f"http://127.0.0.1:{ports['http']}/list")
        print(
            f"server ready: sip={ports['sip']} http={ports['http']} "
            f"target={target_port} lan={lan} workdir={workdir}"
        )

        conf_dir = make_baresip_conf(workdir, lan)
        client = BaresipClient(conf_dir, workdir / "baresip.log")
        await asyncio.sleep(1.0)
        # Dial the server (baresip binds an ephemeral UDP port).
        client.cmd(f"/dial sip:e2e@{lan}:{ports['sip']}")

        invite = await next_invite(webhook, timeout=20)
        dialog_id = invite["dialogId"]
        print(f"invite received: {dialog_id}")

        # Accept the call over the websocket.
        import websockets

        ws = await websockets.connect(
            f"ws://127.0.0.1:{ports['http']}/call/sip?id={dialog_id}",
            open_timeout=10,
        )
        await ws.send(json.dumps({"command": "accept", "option": {}}))
        answer = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        assert answer.get("event") == "answer" or answer.get("sdp"), answer
        print("call accepted")

        # Wait for the dialog to be established (200 OK + ACK on the trace).
        established = await client.wait_trace("200 OK", timeout=15)
        print(f"established: {established}")
        if not established:
            raise RuntimeError("baresip call was never established")

        # Transfer: menu's transfer sends an in-dialog REFER with Refer-To.
        await asyncio.sleep(1.0)
        client.cmd(f"/transfer sip:target@{lan}:{target_port}")

        checks = {
            "refer_sent": ("REFER sip:", await client.wait_trace("REFER sip:", 15)),
            "refer_202": ("SIP/2.0 202", await client.wait_trace("SIP/2.0 202", 15)),
            "notify_trying": (
                "Subscription-State: active",
                await client.wait_trace("Subscription-State: active", 15),
            ),
            "refer_leg_200": (
                "answer",
                await client.wait_trace("answer", 15),
            ),
            "notify_final": (
                "Subscription-State: terminated",
                await client.wait_trace("Subscription-State: terminated", 15),
            ),
            "bye": ("BYE", await client.wait_trace("BYE", 25)),
        }
        for name, (needle, hit) in checks.items():
            print(f"  {name:<16} {'PASS' if hit else 'FAIL'}  ({needle})")

        # The final NOTIFY must report success.
        out = client.output()
        final_ok = "terminated" in out and (
            "SIP/2.0 200 OK" in out.split("Subscription-State: terminated")[-1][:400]
        )

        client.stop()
        ok = all(hit for _, hit in checks.values()) and final_ok
        if not ok:
            trace_tail = out[-4000:]
            print(f"baresip log tail:\n{trace_tail}")
        await ws.close()
    finally:
        for proc in (server, target):
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        webhook.stop()
        if args.keep_logs:
            print(f"logs kept in {workdir}")

    print(f"\n=> {'PASS' if ok else 'FAIL'}")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--sipbot", help="path to sipbot binary")
    parser.add_argument("--binary", help="path to active-call binary")
    parser.add_argument("--keep-logs", action="store_true", help="print log location")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    try:
        ok = asyncio.run(run(args))
    except Exception as e:  # noqa: BLE001
        print(f"FAIL {type(e).__name__}: {e}")
        return 1
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
