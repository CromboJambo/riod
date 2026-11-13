#!/usr/bin/env python3
"""riod-run: Resource-aware service launcher for riod-managed dinit services.

Usage: riod-run.py <service-name>

Reads a service profile from /etc/riod/services/<name>.toml, declares intent
and allocates resources via the riod daemon (if available), then runs the
command with heartbeats and cleanup on exit.

Fail-open: if riod is down or unresponsive, the command runs directly without
resource allocation. The service is never blocked by supervisor failure.

Profile format:
    name = "service-name"
    purpose = "inference"
    description = "What this service does"

    [resources]
    gpu_count = 1
    ram_mb = 4096
    cpu_percent = 50
    ttl_secs = 604800

    [command]
    binary = "/path/to/binary"
    args = ["--flag", "value"]
"""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    import tomllib
except ImportError:
    print("riod-run: Python 3.11+ required (tomllib)", file=sys.stderr)
    sys.exit(1)

RIOD_SOCKET = os.environ.get("RIOD_SOCKET", "/run/riod.sock")
SERVICES_DIR = os.environ.get("RIOD_SERVICES_DIR", "/etc/riod/services")


def log(msg: str):
    print(f"[riod-run] {msg}", file=sys.stderr, flush=True)


def riod_call(method: str, params: dict | None = None, timeout: float = 5.0) -> dict | None:
    """Make a JSON-RPC call to the riod daemon. Returns result dict or None on failure."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect(RIOD_SOCKET)
        req = json.dumps({
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": int(time.time() * 1000),
        }) + "\n"
        s.sendall(req.encode())
        data = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
            if data.endswith(b"\n"):
                break
        s.close()
        resp = json.loads(data.decode())
        if "error" in resp:
            log(f"riod error ({method}): {resp['error'].get('message', 'unknown')}")
            return None
        return resp.get("result", {}).get("data")
    except (socket.error, OSError, json.JSONDecodeError) as e:
        log(f"riod unavailable ({method}): {e}")
        return None


def load_profile(service_name: str) -> dict:
    """Load and validate a service profile TOML file."""
    profile_path = Path(SERVICES_DIR) / f"{service_name}.toml"
    if not profile_path.exists():
        log(f"profile not found: {profile_path}")
        sys.exit(1)

    with open(profile_path, "rb") as f:
        profile = tomllib.load(f)

    # Validate required fields
    for field in ("name", "purpose"):
        if field not in profile:
            log(f"profile missing required field: {field}")
            sys.exit(1)

    if "command" not in profile or "binary" not in profile["command"]:
        log("profile missing [command].binary")
        sys.exit(1)

    return profile


class HeartbeatThread(threading.Thread):
    """Background thread that sends heartbeats to riod for an active allocation."""

    def __init__(self, allocation_id: str, client_id: str, interval: float):
        super().__init__(daemon=True)
        self.allocation_id = allocation_id
        self.client_id = client_id
        self.interval = interval
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.wait(self.interval):
            result = riod_call("resources.heartbeat", {
                "allocation_id": self.allocation_id,
                "client_id": self.client_id,
            })
            if result is None:
                log("heartbeat failed (riod down?)")
                # Keep trying — don't kill the service because riod hiccuped

    def stop(self):
        self.stop_event.set()


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <service-name>", file=sys.stderr)
        sys.exit(1)

    service_name = sys.argv[1]
    profile = load_profile(service_name)
    purpose = profile["purpose"]
    resources = profile.get("resources", {})
    command = profile["command"]

    binary = command["binary"]
    args = command.get("args", [])
    full_cmd = [binary] + [str(a) for a in args]

    log(f"starting {service_name} (purpose={purpose})")

    # --- Resource allocation (fail-open) ---
    allocation_id = None
    client_id = f"dinit:{service_name}"
    heartbeat = None

    # Check if riod is reachable before attempting allocation
    status = riod_call("system.status", timeout=2.0)
    if status is not None:
        log("riod is reachable, requesting resources")

        # Declare intent (only use it for allocation if granted)
        intent_result = riod_call("intent.declare", {
            "client_id": client_id,
            "purpose": purpose,
            "description": profile.get("description", service_name),
        })
        intent_id = None
        if intent_result and intent_result.get("id"):
            status = intent_result.get("status")
            if status == "granted":
                intent_id = intent_result["id"]
                log(f"intent granted: {intent_id}")
            else:
                reason = status.get("denied", "unknown") if isinstance(status, dict) else str(status)
                log(f"intent not granted ({reason}) — allocating without intent")

        # Allocate resources
        alloc_params = {"client_id": client_id, "purpose": purpose}
        if intent_id:
            alloc_params["intent_id"] = intent_id

        gpu_count = resources.get("gpu_count", 0)
        if gpu_count > 0:
            alloc_params["gpu_count"] = gpu_count  # Auto-assign N GPUs
        else:
            alloc_params["gpus"] = []  # Explicitly CPU-only (empty list, not omitted)
        ram_mb = resources.get("ram_mb")
        if ram_mb:
            alloc_params["ram_mb"] = ram_mb
        cpu_percent = resources.get("cpu_percent")
        if cpu_percent:
            alloc_params["cpu_percent"] = cpu_percent

        ttl_secs = resources.get("ttl_secs", 3600)
        alloc_params["ttl_secs"] = ttl_secs

        alloc_result = riod_call("resources.allocate", alloc_params)
        # riod_call returns result.data: {"allocation": {...}, "intent_id": ..., "message": ...}
        allocation_obj = alloc_result.get("allocation", {}) if alloc_result else {}
        if allocation_obj and allocation_obj.get("id"):
            allocation_id = allocation_obj["id"]
            log(f"allocation granted: {allocation_id} (ttl={ttl_secs}s, gpus={allocation_obj.get('gpus', [])})")

            # Start heartbeat at 1/3 of TTL interval
            hb_interval = max(60.0, ttl_secs / 3.0)
            heartbeat = HeartbeatThread(allocation_id, client_id, hb_interval)
            heartbeat.start()
        else:
            log("allocation denied or failed — running without riod resource tracking")
    else:
        log("riod unreachable — running in fail-open mode (no allocation)")

    # --- Run the service command ---
    child = None

    def cleanup(signum=None, frame=None):
        nonlocal child, heartbeat
        log(f"cleanup triggered (signal={signum})")
        if heartbeat:
            heartbeat.stop()
        if child and child.poll() is None:
            log("terminating child process")
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                log("child did not exit, killing")
                child.kill()
                child.wait(timeout=5)
        if allocation_id:
            riod_call("resources.release", {
                "allocation_id": allocation_id,
                "client_id": client_id,
            })
            log(f"allocation released: {allocation_id}")

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    try:
        child = subprocess.Popen(full_cmd)
        exit_code = child.wait()
    except FileNotFoundError:
        log(f"command not found: {binary}")
        cleanup()
        sys.exit(127)
    except Exception as e:
        log(f"failed to start command: {e}")
        cleanup()
        sys.exit(1)

    # Normal exit path
    if heartbeat:
        heartbeat.stop()
    if allocation_id:
        riod_call("resources.release", {
            "allocation_id": allocation_id,
            "client_id": client_id,
        })
        log(f"allocation released: {allocation_id}")

    log(f"{service_name} exited with code {exit_code}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
