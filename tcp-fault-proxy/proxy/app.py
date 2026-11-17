"""Single-process TCP fault proxy built with FastAPI + asyncio.

The proxy accepts TCP connections on one port and always forwards them to a
fixed local receiver endpoint.  For every connection, each direction
(client->proxy->receiver and receiver->proxy->client) is pumped by its own
asyncio task.

Per direction the following can be configured (via the control API before a
new connection arrives, or manually on a live connection):

* pause after N cumulative bytes have been forwarded in that direction;
* resume a paused direction (or all paused directions of a connection);
* forcibly close the whole connection, interrupting paused directions and
  blocked writes immediately.

Drain control (used before restarting the receiver):

* ``POST /drain/start`` closes the ingress gate for *new* TCP connections
  immediately (accepted socket torn down, target never contacted) while every
  connection that had already entered proxy handling -- including one still
  waiting for its target connect -- is allowed to finish upload, half-close,
  and receipt.
* ``GET /drain`` reports progress and ``can_stop`` only once all tracked
  connections have both pump tasks and both sockets finished.
* When the configured wait expires, remaining tracked connections are
  force-closed; the final tally separates ``normal``, ``fault`` and
  ``force_closed`` endings.
* Repeated ``POST /drain/start`` is idempotent and never extends the original
  deadline.  Once ``can_stop`` is true, ``POST /drain/stop`` re-opens the gate.

Guarantees implemented in this file:

* A read chunk that crosses the N-byte threshold is split, so forwarding
  stops *exactly* at N bytes.  The unsent tail (backlog) and any FIN stay on
  the proxy side while the direction is paused.
* Backpressure: while paused no further bytes are read from the socket; the
  kernel receive buffer fills and TCP flow control throttles the sender.
  Nothing is spooled to disk and no whole packets are buffered.  User-space
  buffering per direction is bounded by the stream read limit (<=256 KiB).
* A FIN (half-close) is held back until the backlog of its own direction has
  been forwarded after resume; the reverse direction keeps flowing.
* A force-close aborts both transports, cancels both pump tasks and waits
  until they have exited and both sockets are closed before reporting
  completion.
* A drain tracks connections from handler entry (before the target connect
  starts) through final socket teardown.  The gate check and registration are
  done without an intervening ``await``, so a connection is either rejected
  or tracked as a pre-drain connection, never missed by both.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
import uuid
from collections import deque
from contextlib import suppress
from typing import Deque, Dict, Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger("tcp_fault_proxy")

# --------------------------------------------------------------------------- #
# Configuration / limits
# --------------------------------------------------------------------------- #

LISTEN_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
LISTEN_PORT = int(os.getenv("PROXY_PORT", "9000"))
TARGET_HOST = os.getenv("TARGET_HOST", "receiver")
TARGET_PORT = int(os.getenv("TARGET_PORT", "9100"))

# asyncio.StreamReader byte limit: a single read() can return at most this
# many bytes, and it is also the hard cap on user-space per-direction state
# (one reader buffer + at most one unsent split tail).
CHUNK = 64 * 1024  # 64 KiB per read
STREAM_LIMIT = 256 * 1024  # 256 KiB, required user-space cap
MAX_CLOSED_RECORDS = 200

# Default drain wait when the caller does not specify one.  Can be overridden
# per request via POST /drain/start.
DEFAULT_DRAIN_TIMEOUT = float(os.getenv("PROXY_DRAIN_TIMEOUT", "60"))
# How long a force-close waits for the supervisor to finish socket teardown.
FORCE_SETTLE_TIMEOUT = float(os.getenv("PROXY_FORCE_SETTLE_TIMEOUT", "5"))

Direction = Literal["c2r", "r2c"]
DIRECTIONS: tuple[Direction, Direction] = ("c2r", "r2c")
Outcome = Literal["normal", "fault", "force_closed"]


def _now() -> float:
    return time.monotonic()


# --------------------------------------------------------------------------- #
# Preset policy applied to every new connection
# --------------------------------------------------------------------------- #


class Preset(BaseModel):
    c2r_pause_after: Optional[int] = Field(
        default=None,
        ge=0,
        description="Pause client->receiver after this many bytes were "
        "forwarded. null/omitted disables it for new connections.",
    )
    r2c_pause_after: Optional[int] = Field(
        default=None,
        ge=0,
        description="Pause receiver->client after this many bytes were "
        "forwarded. null/omitted disables it for new connections.",
    )


preset = Preset()


# --------------------------------------------------------------------------- #
# Per-connection / per-direction state
# --------------------------------------------------------------------------- #


class DirState:
    """Bookkeeping for one direction of one connection."""

    def __init__(self, direction: Direction, pause_after: Optional[int]):
        self.direction: Direction = direction
        # Snapshot of the preset at connection creation time.
        self.pause_after: Optional[int] = pause_after
        self.forwarded: int = 0
        self.pending: bytes = b""  # unsent tail held while paused
        self.fin_pending: bool = False  # local peer half-closed, FIN held back
        self.eof_seen: bool = False
        self.finished: bool = False  # half-close delivered to remote peer
        self.closed: bool = False  # remote side fully closed (read returned)
        self.paused: bool = False
        self.auto_triggered: bool = False
        self.resume_event: asyncio.Event = asyncio.Event()
        self.paused_since: Optional[float] = None

    def snapshot(self) -> dict:
        return {
            "direction": self.direction,
            "pause_after": self.pause_after,
            "forwarded_bytes": self.forwarded,
            "pending_bytes": len(self.pending),
            "paused": self.paused,
            "auto_triggered": self.auto_triggered,
            "fin_pending": self.fin_pending,
            "eof_seen": self.eof_seen,
            "half_closed_delivered": self.finished,
            "peer_closed": self.closed,
            "paused_since": self.paused_since,
        }


class Conn:
    def __init__(
        self,
        conn_id: str,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        peer: tuple[str, int],
    ):
        self.id = conn_id
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.remote_reader: Optional[asyncio.StreamReader] = None
        self.remote_writer: Optional[asyncio.StreamWriter] = None
        self.peer = peer
        self.phase: str = "connecting"  # connecting | forwarding | connect-failed | closed
        self.created_at = _now()
        self.created_wall = time.time()
        self.closed = False
        self.closed_at: Optional[float] = None
        self.settled = False  # pumps done AND both sockets torn down
        self.settled_at: Optional[float] = None
        self.settled_wall: Optional[float] = None
        self.settled_event: asyncio.Event = asyncio.Event()
        self.error: Optional[str] = None
        self.force_closed = False
        self.force_reason: Optional[str] = None
        self.outcome: Optional[Outcome] = None
        self.drain_tracked = False
        self.client_socket_closed = False
        self.remote_socket_closed = False
        self.task: Optional[asyncio.Task] = None
        try:
            self.task = asyncio.current_task()
        except RuntimeError:  # pragma: no cover - only outside a running loop
            self.task = None
        self.tasks: Dict[Direction, Optional[asyncio.Task]] = {
            "c2r": None,
            "r2c": None,
        }
        self.dir: Dict[Direction, DirState] = {
            "c2r": DirState("c2r", preset.c2r_pause_after),
            "r2c": DirState("r2c", preset.r2c_pause_after),
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<Conn {self.id} phase={self.phase} settled={self.settled}>"

    # -- pause control ----------------------------------------------------- #

    def pause(self, direction: Direction) -> None:
        st = self.dir[direction]
        if st.paused or st.finished or st.closed:
            return
        st.paused = True
        st.paused_since = _now()
        st.resume_event.clear()

    def resume(self, direction: Direction) -> bool:
        st = self.dir[direction]
        if not st.paused:
            return False
        # Once resumed, the one-shot auto threshold must not fire again.
        st.pause_after = None
        st.paused = False
        st.paused_since = None
        st.resume_event.set()
        return True

    # -- force close ------------------------------------------------------- #

    def mark_force_closed(self, reason: str) -> bool:
        """Abort transports and cancel pumps (or the target connect).

        Returns True only for the call that actually initiated the teardown;
        a second concurrent force-close (for example a manual request racing
        the drain deadline) only waits for settlement.  A later reason never
        overwrites the first one: the earliest cause is what gets reported.
        """
        if self.force_closed:
            return False
        self.force_closed = True
        self.force_reason = reason
        self.closed = True
        for d in DIRECTIONS:
            self.dir[d].resume_event.set()
        for writer in (self.client_writer, self.remote_writer):
            if writer is None:
                continue
            with suppress(Exception):
                writer.transport.abort()
        if self.phase == "connecting":
            # The handler is blocked in open_connection(); cancel it so the
            # pending target connect does not outlive the drain deadline.
            if self.task is not None and self.task is not asyncio.current_task():
                self.task.cancel()
        else:
            for task in self.tasks.values():
                if task is not None and not task.done():
                    task.cancel()
        return True

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "peer": f"{self.peer[0]}:{self.peer[1]}",
            "phase": self.phase,
            "created_at": self.created_at,
            "created_at_wall": self.created_wall,
            "closed": self.closed,
            "closed_at": self.closed_at,
            "settled": self.settled,
            "settled_at": self.settled_at,
            "settled_at_wall": self.settled_wall,
            "force_closed": self.force_closed,
            "force_reason": self.force_reason,
            "outcome": self.outcome,
            "error": self.error,
            "client_socket_closed": self.client_socket_closed,
            "remote_socket_closed": self.remote_socket_closed,
            "pumps_done": {
                d: self.settled or (self.tasks[d] is not None and self.tasks[d].done())
                for d in DIRECTIONS
            },
            "directions": {d: self.dir[d].snapshot() for d in DIRECTIONS},
        }


class Registry:
    def __init__(self) -> None:
        self.active: Dict[str, Conn] = {}
        self.closed: Deque[dict] = deque(maxlen=MAX_CLOSED_RECORDS)

    def add(self, conn: Conn) -> None:
        self.active[conn.id] = conn

    def get_active(self, conn_id: str) -> Conn:
        conn = self.active.get(conn_id)
        if conn is None:
            raise HTTPException(status_code=404, detail=f"unknown connection {conn_id}")
        return conn

    def retire(self, conn: Conn) -> None:
        self.active.pop(conn.id, None)
        self.closed.appendleft(conn.snapshot())

    def list(self) -> list[dict]:
        # Newest first: active connections, then closed records.
        return [c.snapshot() for c in self.active.values()] + list(self.closed)


registry = Registry()


# --------------------------------------------------------------------------- #
# Pump: one direction of one connection
# --------------------------------------------------------------------------- #


async def _wait_resume(st: DirState, conn: Conn) -> None:
    """Block while the direction is paused or while the connection dies."""
    while st.paused and not conn.closed:
        try:
            await asyncio.wait_for(st.resume_event.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            continue


async def pump(conn: Conn, direction: Direction) -> None:
    """Forward exactly one direction, honouring the pause/backlog rules."""
    if direction == "c2r":
        reader, writer = conn.client_reader, conn.remote_writer
    else:
        reader, writer = conn.remote_reader, conn.client_writer
    st = conn.dir[direction]
    assert writer is not None and reader is not None

    try:
        while True:
            # While paused we do NOT touch the socket at all: no reads (which
            # would buffer more user-space data) and no FIN delivery.  The
            # kernel receive buffer fills and TCP backpressure reaches the
            # sender; the held backlog is at most one split read tail.
            if st.paused:
                await _wait_resume(st, conn)
                if conn.closed:
                    return

            # Flush the backlog that was held when the threshold tripped,
            # before reading anything new from the socket.
            if st.pending:
                data = st.pending
                st.pending = b""
                writer.write(data)
                # drain() raises ConnectionReset/aborted when force-close
                # aborts the transport, which cancels this task cleanly.
                await writer.drain()
                st.forwarded += len(data)
                if st.fin_pending:
                    _deliver_fin(writer, st)
                    return

            if st.eof_seen:
                # EOF arrived while paused; backlog (if any) is now flushed.
                if st.fin_pending:
                    _deliver_fin(writer, st)
                return

            data = await reader.read(CHUNK)
            if data == b"":
                st.eof_seen = True
                st.closed = True
                if st.paused:
                    # FIN must not cross the pause; wait, then flush backlog.
                    await _wait_resume(st, conn)
                    if conn.closed:
                        return
                    if st.pending:
                        tail = st.pending
                        st.pending = b""
                        writer.write(tail)
                        await writer.drain()
                        st.forwarded += len(tail)
                _deliver_fin(writer, st)
                return

            # Threshold handling: split the chunk so we stop exactly at N,
            # even across read-block boundaries.
            threshold = st.pause_after
            if (
                not st.auto_triggered
                and threshold is not None
                and st.forwarded + len(data) >= threshold
            ):
                cut = threshold - st.forwarded
                head, tail = data[:cut], data[cut:]
                writer.write(head)
                await writer.drain()
                st.forwarded += len(head)
                st.auto_triggered = True
                if tail:
                    # Held backlog: small, never spooled to disk.
                    st.pending = tail
                st.paused = True
                st.paused_since = _now()
                st.resume_event.clear()
                continue

            writer.write(data)
            await writer.drain()
            st.forwarded += len(data)
    except (asyncio.CancelledError, ConnectionError, RuntimeError):
        # Force-close (transport abort / task cancel) or peer reset: the
        # supervisor performs the cleanup.
        raise
    except Exception as exc:  # pragma: no cover - defensive
        conn.error = f"{direction} pump error: {exc!r}"
        raise


def _deliver_fin(writer: asyncio.StreamWriter, st: DirState) -> None:
    """Half-close the remote write side after the backlog is fully sent."""
    transport = writer.transport
    if transport.can_write_eof():
        writer.write_eof()
    st.fin_pending = False
    st.finished = True


# --------------------------------------------------------------------------- #
# Connection lifecycle
# --------------------------------------------------------------------------- #


async def _open_target():
    """Indirection so tests can hold/fail a target connect deterministically."""
    return await asyncio.open_connection(TARGET_HOST, TARGET_PORT, limit=STREAM_LIMIT)


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    peer = client_writer.get_extra_info("peername") or ("?", 0)

    # Gate check and registration are deliberately adjacent with no await in
    # between: either this connection is rejected now, or it is already in
    # registry.active before a concurrent drain start snapshots that dict.
    if drain.active:
        drain.note_rejected(peer)
        # New ingress must not even open a target connection.
        client_writer.transport.abort()
        with suppress(Exception):
            await client_writer.wait_closed()
        return

    conn_id = uuid.uuid4().hex[:12]
    conn = Conn(conn_id, client_reader, client_writer, peer)
    registry.add(conn)

    pump_tasks: list[asyncio.Task] = []
    try:
        try:
            remote_reader, remote_writer = await _open_target()
        except asyncio.CancelledError:
            # Drain deadline / manual force-close while waiting for the target.
            raise
        except OSError as exc:
            conn.error = (
                f"connect target {TARGET_HOST}:{TARGET_PORT} failed: {exc!r}"
            )
            conn.phase = "connect-failed"
            return

        conn.remote_reader = remote_reader
        conn.remote_writer = remote_writer
        conn.phase = "forwarding"
        if conn.force_closed:
            # Force-close raced with a successful target connect: keep the
            # connection but do not start pumping data.
            return

        conn.tasks["c2r"] = asyncio.create_task(pump(conn, "c2r"), name=f"{conn_id}-c2r")
        conn.tasks["r2c"] = asyncio.create_task(pump(conn, "r2c"), name=f"{conn_id}-r2c")
        pump_tasks = [t for t in conn.tasks.values() if t is not None]

        # The connection lives until both pump tasks have exited (normal
        # half-close completion, error, or force-close cancellation).
        results = await asyncio.gather(*pump_tasks, return_exceptions=True)
        for direction, result in zip(DIRECTIONS, results):
            if isinstance(result, asyncio.CancelledError):
                if not conn.force_closed and conn.error is None:
                    conn.error = f"{direction} pump cancelled before completion"
            elif isinstance(result, BaseException) and not conn.force_closed:
                if conn.error is None:
                    conn.error = f"{direction} pump error: {result!r}"
    finally:
        conn.closed = True
        conn.closed_at = _now()
        for writer in (client_writer, conn.remote_writer):
            if writer is None:
                continue
            try:
                writer.close()
            except Exception:
                pass
        for task in conn.tasks.values():
            if task is not None and not task.done():
                task.cancel()
        if pump_tasks:
            await asyncio.gather(*pump_tasks, return_exceptions=True)
        for attr, writer in (
            ("client_socket_closed", client_writer),
            ("remote_socket_closed", conn.remote_writer),
        ):
            if writer is None:
                # No target socket was ever created (rejected/connect failure).
                setattr(conn, attr, True)
                continue
            try:
                await writer.wait_closed()
            except (OSError, RuntimeError):
                pass
            setattr(conn, attr, True)
        conn.phase = "closed"
        conn.settled = True
        conn.settled_at = _now()
        conn.settled_wall = time.time()
        if conn.force_closed:
            conn.outcome = "force_closed"
        elif conn.error:
            conn.outcome = "fault"
        else:
            conn.outcome = "normal"
        registry.retire(conn)
        conn.settled_event.set()
        drain.on_settled(conn)


async def force_close_connection(conn: Conn, reason: str) -> bool:
    """Force-close one connection and wait for full cleanup (bounded)."""
    if conn.settled:
        return True
    conn.mark_force_closed(reason)
    try:
        await asyncio.wait_for(
            conn.settled_event.wait(), timeout=FORCE_SETTLE_TIMEOUT
        )
    except asyncio.TimeoutError:
        logger.warning(
            "connection %s did not settle within %.1fs after force-close (%s)",
            conn.id,
            FORCE_SETTLE_TIMEOUT,
            reason,
        )
        return False
    return True


# --------------------------------------------------------------------------- #
# Drain manager
# --------------------------------------------------------------------------- #


class DrainManager:
    """Ingress gate + progress tracking for a receiver restart.

    States: ``idle`` (accepting), ``draining`` (gate closed, tracked
    connections still running), ``drained`` (gate still closed, but every
    tracked connection has finished its pumps and sockets; safe to stop the
    receiver).  ``stop()`` moves a drained manager back to ``idle``.
    """

    def __init__(self) -> None:
        self.active = False
        self.state: str = "idle"
        self.started_at: Optional[float] = None  # monotonic
        self.started_wall: Optional[float] = None
        self.wait_timeout: Optional[float] = None
        self.deadline: Optional[float] = None  # monotonic
        self.deadline_wall: Optional[float] = None
        self.timed_out = False
        self.force_started_at: Optional[float] = None
        self.force_started_wall: Optional[float] = None
        self.tracked: Dict[str, Conn] = {}
        self.rejected = 0
        self.timer: Optional[asyncio.Task] = None
        self.completed_at: Optional[float] = None
        self.completed_wall: Optional[float] = None
        self.last_result: Optional[dict] = None

    # -- state transitions ------------------------------------------------- #

    def start(self, wait_timeout_seconds: float) -> dict:
        """Close the ingress gate; idempotent while a drain is in progress."""
        if self.active:
            # Repeated start: never move the original deadline, never reset
            # the rejected counter or tracked set.
            return self.snapshot()

        self.active = True
        self.state = "draining"
        self.started_at = _now()
        self.started_wall = time.time()
        self.wait_timeout = float(wait_timeout_seconds)
        self.deadline = self.started_at + self.wait_timeout
        self.deadline_wall = self.started_wall + self.wait_timeout
        self.timed_out = False
        self.force_started_at = None
        self.force_started_wall = None
        self.rejected = 0
        self.completed_at = None
        self.completed_wall = None
        self.last_result = None
        self.tracked = dict(registry.active)
        for conn in self.tracked.values():
            conn.drain_tracked = True

        # No await between closing the gate and taking the snapshot above, so
        # no handler can slip in a pre-drain connection unnoticed.
        self._maybe_finalize()
        if self.state == "draining":
            self.timer = asyncio.create_task(self._watch_deadline(), name="drain-deadline")
        return self.snapshot()

    def stop(self) -> dict:
        """Release a completed drain and accept new connections again."""
        if not self.active:
            return self.snapshot()
        if self.state != "drained":
            raise HTTPException(
                status_code=409,
                detail=(
                    "drain is still in progress; wait until can_stop is true "
                    "before releasing the gate"
                ),
            )
        self.last_result = self._summary()
        self.last_result["state"] = "drained"
        self.last_result["can_stop"] = True
        self.active = False
        self.state = "idle"
        self._cancel_timer()
        self.tracked = {}
        self.started_at = None
        self.started_wall = None
        self.wait_timeout = None
        self.deadline = None
        self.deadline_wall = None
        return self.snapshot()

    def note_rejected(self, peer: tuple[str, int]) -> None:
        self.rejected += 1

    def on_settled(self, conn: Conn) -> None:
        if self.active and conn.drain_tracked:
            self._maybe_finalize()

    # -- deadline ---------------------------------------------------------- #

    async def _watch_deadline(self) -> None:
        try:
            assert self.deadline is not None
            delay = self.deadline - _now()
            if delay > 0:
                await asyncio.sleep(delay)
            if not self.active or self.state != "draining":
                return
            self.timed_out = True
            self.force_started_at = _now()
            self.force_started_wall = time.time()
            targets = [c for c in list(self.tracked.values()) if not c.settled]
            if targets:
                await asyncio.gather(
                    *(
                        force_close_connection(c, "drain deadline exceeded")
                        for c in targets
                    ),
                    return_exceptions=True,
                )
            self._maybe_finalize()
            # A force-close that could not settle within its own bound leaves
            # the drain un-finalized; keep watching so a late settlement is
            # still reflected (can_stop stays false meanwhile).
            outstanding = [c for c in self.tracked.values() if not c.settled]
            if outstanding and self.active and self.state == "draining":
                await asyncio.gather(
                    *(c.settled_event.wait() for c in outstanding),
                    return_exceptions=True,
                )
                self._maybe_finalize()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            logger.exception("drain deadline watcher failed")

    def _cancel_timer(self) -> None:
        timer = self.timer
        if (
            timer is not None
            and timer is not asyncio.current_task()
            and not timer.done()
        ):
            timer.cancel()
        self.timer = None

    def _maybe_finalize(self) -> None:
        if not self.active or self.state != "draining":
            return
        if all(conn.settled for conn in self.tracked.values()):
            # Capture the completion time before cancelling the timer: when
            # this runs from the deadline watcher the current task is the
            # timer itself and must not be cancelled.
            self.state = "drained"
            self.completed_at = _now()
            self.completed_wall = time.time()
            self._cancel_timer()
            self.last_result = self._summary()
            self.last_result["state"] = "drained"
            self.last_result["can_stop"] = True

    # -- reporting --------------------------------------------------------- #

    def _summary(self) -> dict:
        settled = [c for c in self.tracked.values() if c.settled]
        counts = {"normal": 0, "fault": 0, "force_closed": 0}
        for conn in settled:
            if conn.outcome in counts:
                counts[conn.outcome] += 1
        outstanding = len(self.tracked) - len(settled)
        return {
            "started_at": self.started_wall,
            "completed_at": self.completed_wall,
            "wait_timeout_seconds": self.wait_timeout,
            "deadline_at": self.deadline_wall,
            "timed_out": self.timed_out,
            "force_close_started_at": self.force_started_wall,
            "tracked_total": len(self.tracked),
            "settled_total": len(settled),
            "outstanding_total": outstanding,
            "counts": counts,
            "new_connections_rejected": self.rejected,
            "connections": [c.snapshot() for c in self.tracked.values()],
        }

    def snapshot(self) -> dict:
        if not self.active:
            return {
                "state": "idle",
                "active": False,
                "accepting_connections": True,
                "can_stop": False,
                "last_result": self.last_result,
            }
        summary = self._summary()
        remaining: Optional[float]
        if self.state == "drained" or self.deadline is None:
            remaining = 0.0
        else:
            remaining = round(max(0.0, self.deadline - _now()), 3)
        summary.update(
            {
                "state": self.state,
                "active": True,
                "accepting_connections": False,
                "can_stop": self.state == "drained" and not summary["outstanding_total"],
                "remaining_seconds": remaining,
            }
        )
        return summary


drain = DrainManager()


# --------------------------------------------------------------------------- #
# Control API
# --------------------------------------------------------------------------- #


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.server = await asyncio.start_server(
        handle_client, LISTEN_HOST, LISTEN_PORT, limit=STREAM_LIMIT
    )
    try:
        yield
    finally:
        app.state.server.close()
        await app.state.server.wait_closed()
        drain._cancel_timer()
        # Drain any in-flight connection supervisors so the process exits
        # without leaving half-closed sockets behind.
        active = list(registry.active.values())
        for conn in active:
            conn.mark_force_closed("proxy shutdown")
        if active:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(
                        *(conn.settled_event.wait() for conn in active),
                        return_exceptions=True,
                    ),
                    timeout=FORCE_SETTLE_TIMEOUT,
                )


app = FastAPI(
    title="TCP 故障代理 (TCP Fault Proxy)",
    description=(
        "单进程 TCP 故障代理：固定转发至本地接收端。可预设新连接每个方向在累计转发 "
        "N 字节后暂停，查询连接、按连接恢复或强断；重启接收端前可先排空（停止接单、"
        "等待旧连接完成、超时强断并统计收尾）。详见各接口说明。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


class PresetResponse(BaseModel):
    preset: Preset


class ConnListResponse(BaseModel):
    active_count: int
    connections: list[dict]


class PauseBody(BaseModel):
    direction: Direction = Field(
        ..., description="c2r = 客户端→接收端（上传方向），r2c = 接收端→客户端（回执方向）"
    )


class ResumeBody(BaseModel):
    direction: Optional[Direction] = Field(
        default=None,
        description="要恢复的方向；省略或 null 表示恢复该连接两个方向中所有暂停的方向。",
    )


class DrainStartBody(BaseModel):
    wait_timeout_seconds: float = Field(
        default=DEFAULT_DRAIN_TIMEOUT,
        ge=0,
        description=(
            "排空最长等待秒数。超时后强断仍存活的旧连接；重复调用 start 不会延长"
            "首次设定的期限。"
        ),
    )


class OpResponse(BaseModel):
    ok: bool
    connection: dict


@app.get("/health", tags=["control"])
async def health() -> dict:
    return {
        "status": "ok",
        "listen": f"{LISTEN_HOST}:{LISTEN_PORT}",
        "target": f"{TARGET_HOST}:{TARGET_PORT}",
        "active_connections": len(registry.active),
        "accepting_connections": not drain.active,
        "drain_state": drain.state,
        "stream_limit_bytes": STREAM_LIMIT,
        "chunk_bytes": CHUNK,
    }


@app.get("/preset", response_model=PresetResponse, tags=["control"])
async def get_preset() -> PresetResponse:
    """查看当前预设：对之后新建连接生效（不影响已存在的连接）。"""
    return PresetResponse(preset=preset)


@app.put("/preset", response_model=PresetResponse, tags=["control"])
async def set_preset(body: Preset) -> PresetResponse:
    """预设新连接各方向累计转发多少字节后自动暂停；传 null 取消。"""
    preset.c2r_pause_after = body.c2r_pause_after
    preset.r2c_pause_after = body.r2c_pause_after
    return PresetResponse(preset=preset)


@app.get("/connections", response_model=ConnListResponse, tags=["control"])
async def list_connections() -> ConnListResponse:
    """列出全部连接（活动在前、已关闭在后），含每方向字节计数与暂停状态。"""
    return ConnListResponse(active_count=len(registry.active), connections=registry.list())


@app.get("/connections/{conn_id}", tags=["control"])
async def get_connection(conn_id: str) -> dict:
    return registry.get_active(conn_id).snapshot()


@app.post("/connections/{conn_id}/pause", response_model=OpResponse, tags=["control"])
async def pause_connection(conn_id: str, body: PauseBody) -> OpResponse:
    """手动暂停某连接的某个方向（除自动阈值外的测试手段）。"""
    conn = registry.get_active(conn_id)
    conn.pause(body.direction)
    return OpResponse(ok=True, connection=conn.snapshot())


@app.post("/connections/{conn_id}/resume", response_model=OpResponse, tags=["control"])
async def resume_connection(conn_id: str, body: ResumeBody) -> OpResponse:
    """恢复某连接的指定方向；不传 direction 则恢复该连接全部暂停方向。

    恢复后积压字节按原顺序继续转发，不丢、不重、不乱序；若 FIN 被暂扣，
    会在本方向积压发完后再向对端半关闭。
    """
    conn = registry.get_active(conn_id)
    targets = (body.direction,) if body.direction else DIRECTIONS
    for d in targets:
        conn.resume(d)
    return OpResponse(ok=True, connection=conn.snapshot())


@app.post("/connections/{conn_id}/force-close", response_model=OpResponse, tags=["control"])
async def force_close(conn_id: str) -> OpResponse:
    """强断连接：立即打断暂停、阻塞写或等待目标建连，并等待清理完成。

    abort() 会让正卡在 drain() 的写和卡在 read() 的读立刻抛异常；处于等待
    目标建连阶段的连接则取消建连任务。两个 pump 任务与两端 socket 都结束
    （supervisor 完成清理）后才返回。
    """
    conn = registry.get_active(conn_id)
    settled = await force_close_connection(conn, "manual force-close")
    if not settled:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail="force-close did not finish in time")
    return OpResponse(ok=True, connection=conn.snapshot())


# --------------------------------------------------------------------------- #
# Drain control
# --------------------------------------------------------------------------- #


@app.post("/drain/start", tags=["drain"])
async def start_drain(body: Optional[DrainStartBody] = None) -> dict:
    """停止接单并开始排空旧连接（用于重启接收端前）。

    * 调用生效后新接入的 TCP 连接立即关闭，且不会向接收端建连；
    * 调用前已进入代理处理的连接（含正在等待目标建连的连接）继续完成上传、
      半关闭与回执；
    * 重复调用不会延长首次设置的等待期限，也不会清空已统计的数据；
    * 等待超时后强断剩余连接，`GET /drain` 的 `counts` 分别给出
      `normal` / `fault` / `force_closed` 收尾数量；
    * `can_stop` 仅在全部被跟踪连接的双向 pump 与两端 socket 都结束后为 true；
    * 调用本身立即返回（等待与超时在后台进行），用 `GET /drain` 查进度。
    """
    timeout = body.wait_timeout_seconds if body is not None else DEFAULT_DRAIN_TIMEOUT
    return drain.start(timeout)


@app.get("/drain", tags=["drain"])
async def get_drain() -> dict:
    """查看排空进度：`can_stop` 为 true 才可安全停机重启接收端。

    只有所有被跟踪连接的双向任务和套接字都结束时才会显示可停机；
    空闲状态会保留上一次排空的 `last_result` 统计。
    """
    return drain.snapshot()


@app.post("/drain/stop", tags=["drain"])
async def stop_drain() -> dict:
    """解除排空并重新接纳新连接（仅当 `can_stop` 为 true 时允许）。

    排空尚未完成时返回 409，以免在旧连接仍存活时重新接单。
    """
    return drain.stop()
