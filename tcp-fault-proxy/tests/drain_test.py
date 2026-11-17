#!/usr/bin/env python3
"""In-process drain-control test for the TCP fault proxy.

Runs the proxy's real handler/drain logic in this process (stdlib only, plus
the project's FastAPI/pydantic imports) with a deterministic gate in front of
the target connect.  This makes the "connection entered the proxy but is
still waiting for the target connect" phase reachable, which is not reliably
reproducible against a real listener.

Covered:
  1. baseline forwarding works before any drain;
  2. drain start closes new ingress immediately (client sees EOF/reset) and
     the target is not contacted for it;
  3. a connection already waiting for its target connect is still tracked,
     completes upload/half-close/receipt after the gate opens, and only then
     can_stop becomes true with outcome normal;
  4. repeated drain start does not extend the deadline nor reset counters;
  5. stop is refused (409) while draining, allowed once drained, and new
     connections are accepted again;
  6. wait timeout force-closes a connection stuck waiting for the target;
     it settles, is counted as force_closed, and can_stop becomes true;
  7. target connect failure is counted as fault;
  8. drain start with no live connections is immediately stoppable.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fastapi import HTTPException  # noqa: E402

import receiver.app as receiver_app  # noqa: E402
import proxy.app as proxy_app  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


async def wait_until(predicate, timeout: float = 5.0, interval: float = 0.01):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(interval)
    return predicate()


async def upload(host: str, port: int, size: int = 256 * 1024) -> dict:
    reader, writer = await asyncio.open_connection(host, port)
    block = bytes((i * 37 + 11) & 0xFF for i in range(4096))
    digest = hashlib.sha256()
    remaining = size
    while remaining > 0:
        chunk = block[: min(len(block), remaining)]
        writer.write(chunk)
        await writer.drain()
        digest.update(chunk)
        remaining -= len(chunk)
    if writer.transport.can_write_eof():
        writer.write_eof()
    raw = await asyncio.wait_for(reader.read(), timeout=15)
    writer.close()
    try:
        await writer.wait_closed()
    except (OSError, RuntimeError):
        pass
    receipt = json.loads(raw.decode().strip().splitlines()[-1])
    receipt["_expected_sha256"] = digest.hexdigest()
    return receipt


class Gate:
    """Blocks / fails proxy target connects deterministically."""

    def __init__(self) -> None:
        self.mode = "pass"  # pass | hold | fail
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.attempts = 0
        self._original = proxy_app._open_target

    async def open(self):
        self.attempts += 1
        if self.mode == "fail":
            raise ConnectionRefusedError("gate: target refused")
        if self.mode == "hold":
            self.entered.set()
            await self.release.wait()
        return await self._original()

    def reset(self) -> None:
        self.mode = "pass"
        self.entered = asyncio.Event()
        self.release = asyncio.Event()


async def main() -> int:
    target = await asyncio.start_server(receiver_app.handle_conn, "127.0.0.1", 0)
    target_port = target.sockets[0].getsockname()[1]

    proxy_app.TARGET_HOST = "127.0.0.1"
    proxy_app.TARGET_PORT = target_port
    gate = Gate()
    proxy_app._open_target = gate.open

    server = await asyncio.start_server(
        proxy_app.handle_client, "127.0.0.1", 0, limit=proxy_app.STREAM_LIMIT
    )
    proxy_port = server.sockets[0].getsockname()[1]

    def active():
        return list(proxy_app.registry.active.values())

    try:
        # 0. Idle state exposes last_result; stop is an idempotent no-op. -----
        snap = proxy_app.drain.snapshot()
        check("initial drain state idle", snap["state"] == "idle"
              and snap["accepting_connections"] is True)
        idle_stop = proxy_app.drain.stop()
        check("stop while idle is a no-op",
              idle_stop["state"] == "idle"
              and idle_stop["accepting_connections"] is True)

        # 1. Baseline before drain. ------------------------------------------
        receipt = await upload("127.0.0.1", proxy_port)
        check("baseline upload hash matches",
              receipt["sha256"] == receipt["_expected_sha256"])
        await wait_until(lambda: not active())
        check("baseline has no outstanding connections", not active())

        # 2+3. Drain with a connection waiting for its target connect. ------
        gate.mode = "hold"
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        await asyncio.wait_for(gate.entered.wait(), timeout=5)
        conn = await wait_until(lambda: active() and active()[0])
        check("connection tracked while target connect is pending",
              conn is not None and conn.phase == "connecting",
              f"phase={getattr(conn, 'phase', None)}")

        snap = proxy_app.drain.start(30)
        check("drain start closes the gate", snap["accepting_connections"] is False
              and snap["state"] == "draining")
        check("pre-drain waiting connection is tracked",
              snap["tracked_total"] == 1 and snap["outstanding_total"] == 1)
        check("can_stop false while connection is still connecting",
              snap["can_stop"] is False)
        deadline_wall = snap["deadline_at"]

        # New ingress: handshake may complete, but it must be closed at once
        # and must never reach the target.
        attempts_before = gate.attempts
        try:
            r2, w2 = await asyncio.open_connection("127.0.0.1", proxy_port)
            try:
                new_data = await asyncio.wait_for(r2.read(1), timeout=2)
            except (ConnectionResetError, asyncio.IncompleteReadError):
                new_data = b""
            w2.close()
        except ConnectionError:
            new_data = b""
        check("new ingress closed immediately during drain", new_data == b"")
        await asyncio.sleep(0.1)
        check("rejected connection never contacted the target",
              gate.attempts == attempts_before,
              f"attempts={gate.attempts} before={attempts_before}")
        check("rejected connection counted",
              proxy_app.drain.snapshot()["new_connections_rejected"] >= 1)

        # The pre-drain connection still completes after the target opens.
        gate.mode = "pass"
        gate.release.set()
        digest = hashlib.sha256()
        payload = b"drain-payload-" + bytes(range(256)) * 64
        writer.write(payload)
        digest.update(payload)
        await writer.drain()
        if writer.transport.can_write_eof():
            writer.write_eof()
        reply = await asyncio.wait_for(reader.read(), timeout=15)
        receipt = json.loads(reply.decode().strip().splitlines()[-1])
        check("pre-drain connection completes upload+receipt",
              receipt["sha256"] == digest.hexdigest()
              and receipt["bytes"] == len(payload))
        writer.close()
        try:
            await writer.wait_closed()
        except (OSError, RuntimeError):
            pass
        check("can_stop true only after pumps and sockets settle",
              await wait_until(lambda: proxy_app.drain.snapshot()["can_stop"]))
        snap = proxy_app.drain.snapshot()
        check("drained counts normal=1",
              snap["counts"] == {"normal": 1, "fault": 0, "force_closed": 0},
              str(snap["counts"]))
        check("drained connection shows pumps done and sockets closed",
              snap["connections"][0]["settled"] is True
              and all(snap["connections"][0]["pumps_done"].values())
              and snap["connections"][0]["client_socket_closed"] is True
              and snap["connections"][0]["remote_socket_closed"] is True)
        check("tracked connection is no longer active",
              not proxy_app.registry.active)
        check("drain still closes ingress after completion",
              proxy_app.drain.snapshot()["accepting_connections"] is False)

        # 4. Repeated start does not extend the deadline. --------------------
        repeated = proxy_app.drain.start(3600)
        check("repeated start is idempotent", repeated["state"] == "drained"
              and repeated["deadline_at"] == deadline_wall,
              f"deadline={repeated['deadline_at']} first={deadline_wall}")
        check("repeated start reopens nothing",
              repeated["accepting_connections"] is False)
        proxy_app.drain.stop()
        check("drain released before next scenario",
              proxy_app.drain.snapshot()["accepting_connections"] is True)

        # 5. Stop refused while draining (fresh drain with a held conn). ----
        gate.reset()
        gate.mode = "hold"
        reader2, writer2 = await asyncio.open_connection("127.0.0.1", proxy_port)
        await asyncio.wait_for(gate.entered.wait(), timeout=5)
        await wait_until(lambda: any(c.phase == "connecting" for c in active()))
        proxy_app.drain.start(2)
        try:
            proxy_app.drain.stop()
            refused = False
        except HTTPException as exc:
            refused = exc.status_code == 409
        check("stop refused while old connections still live", refused)

        # Timeout force-closes the held connection.
        await asyncio.wait_for(
            wait_until(lambda: proxy_app.drain.snapshot()["can_stop"], timeout=10),
            timeout=10,
        )
        snap = proxy_app.drain.snapshot()
        check("timed-out drain reports timed_out", snap["timed_out"] is True)
        check("force-closed waiting connection settles",
              snap["counts"]["force_closed"] == 1 and snap["outstanding_total"] == 0,
              str(snap["counts"]))
        check("force-closed reason recorded",
              snap["connections"][0]["force_reason"] == "drain deadline exceeded")
        try:
            tail = await asyncio.wait_for(reader2.read(), timeout=3)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            tail = b""
        check("client of force-closed connection sees closure", tail == b"")
        writer2.close()
        try:
            await writer2.wait_closed()
        except (OSError, RuntimeError):
            pass
        check("connect-cancelled handler is gone", not proxy_app.registry.active)

        gate.reset()

        # 6. Stop once drained reopens the gate and accepts again. -----------
        stopped = proxy_app.drain.stop()
        check("stop after drained opens the gate",
              stopped["state"] == "idle"
              and stopped["accepting_connections"] is True)
        check("stop keeps last drain result",
              stopped["last_result"]["counts"]["force_closed"] == 1)
        receipt = await upload("127.0.0.1", proxy_port)
        check("new connection accepted after stop",
              receipt["sha256"] == receipt["_expected_sha256"])
        await wait_until(lambda: not proxy_app.registry.active)

        # 7. Target connect failure counts as fault. -------------------------
        gate.mode = "fail"
        r3, w3 = await asyncio.open_connection("127.0.0.1", proxy_port)
        try:
            failed_read = await asyncio.wait_for(r3.read(1), timeout=5)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            failed_read = b""
        check("client closed on target connect failure", failed_read == b"")
        w3.close()
        try:
            await w3.wait_closed()
        except (OSError, RuntimeError):
            pass
        check("connect failure retires the connection",
              await wait_until(lambda: not proxy_app.registry.active))
        fail_snap = proxy_app.registry.closed[0]
        check("connect failure recorded as fault",
              fail_snap["outcome"] == "fault" and "connect target" in fail_snap["error"],
              str(fail_snap.get("error")))
        gate.reset()

        # 8. Empty drain is immediately stoppable. ---------------------------
        snap = proxy_app.drain.start(5)
        check("empty drain completes immediately",
              snap["state"] == "drained" and snap["can_stop"] is True
              and snap["tracked_total"] == 0)
        proxy_app.drain.stop()
        check("empty drain stop reopens ingress",
              proxy_app.drain.snapshot()["accepting_connections"] is True)
    finally:
        server.close()
        target.close()
        await server.wait_closed()
        await target.wait_closed()
        proxy_app.drain._cancel_timer()
        for conn in list(proxy_app.registry.active.values()):
            conn.mark_force_closed("test teardown")
        await asyncio.sleep(0.1)

    failed = [r for r in results if r[0] == FAIL]
    print("\n== summary ==")
    for status, name, detail in results:
        print(f"  [{status}] {name}" + (f" -- {detail}" if detail else ""))
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as exc:
        print(f"FATAL: {exc!r}")
        raise
