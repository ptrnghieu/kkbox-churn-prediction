"""Streaming simulation control — start / pause / resume / stop + SSE events."""
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
from asyncio import Queue
from typing import AsyncGenerator

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Streaming"])

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
_PYTHON = sys.executable

# ── Shared state ──────────────────────────────────────────────────────────────

_state: dict = {
    "status": "idle",       # idle | running | paused | done | error
    "current_date": None,
    "dates_done": [],
    "producer_pid": None,
    "consumer_pid": None,
    "speed": 55,
}

_date_users: dict[str, list[str]] = {}   # date_str → [msno, ...]
_sse_queues: list[Queue] = []

TOTAL_DAYS = 31  # March 2017


# ── SSE helpers ───────────────────────────────────────────────────────────────

async def _broadcast(event_type: str, data: dict) -> None:
    payload = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    dead = []
    for q in _sse_queues:
        try:
            q.put_nowait(payload)
        except Exception:
            dead.append(q)
    for q in dead:
        try:
            _sse_queues.remove(q)
        except ValueError:
            pass


# ── Process monitor ───────────────────────────────────────────────────────────

async def _monitor(producer_proc: subprocess.Popen, consumer_proc: subprocess.Popen) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, producer_proc.wait)
    logger.info("Producer exited (code %d)", producer_proc.returncode)
    await loop.run_in_executor(None, consumer_proc.wait)
    logger.info("Consumer exited (code %d)", consumer_proc.returncode)
    if _state["status"] not in ("idle", "error"):
        _state["status"] = "done"
    await _broadcast("status", {
        "status": _state["status"],
        "current_date": _state["current_date"],
        "dates_done": len(_state["dates_done"]),
    })


# ── Schemas ───────────────────────────────────────────────────────────────────

class StartRequest(BaseModel):
    speed: int = 55  # seconds between days


class NotifyRequest(BaseModel):
    date: str
    msnos: list[str] = []


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/start")
async def stream_start(req: StartRequest) -> dict:
    if _state["status"] in ("running", "paused"):
        raise HTTPException(400, "Stream already active — call /stream/stop first")

    _state.update({
        "status": "running",
        "current_date": None,
        "dates_done": [],
        "speed": req.speed,
        "producer_pid": None,
        "consumer_pid": None,
    })
    _date_users.clear()

    group_id = f"kkbox-stream-{int(time.time())}"
    env = {**os.environ,
           "FEAST_REPO_PATH": os.path.join(_REPO_ROOT, "feature_store"),
           "KAFKA_BOOTSTRAP_SERVERS": "localhost:9093"}

    consumer_proc = subprocess.Popen(
        [_PYTHON,
         os.path.join(_REPO_ROOT, "data_pipeline/ingestion/kafka_consumer.py"),
         "--bootstrap-servers", "localhost:9093",
         "--group-id", group_id,
         "--notify-url", "http://localhost:8000/stream/notify",
         "--offset-reset", "latest"],
        env=env,
    )
    producer_proc = subprocess.Popen(
        [_PYTHON,
         os.path.join(_REPO_ROOT, "data_pipeline/ingestion/kafka_producer.py"),
         "--speed", str(req.speed)],
        env=env,
    )

    _state["producer_pid"] = producer_proc.pid
    _state["consumer_pid"] = consumer_proc.pid

    asyncio.create_task(_monitor(producer_proc, consumer_proc))
    await _broadcast("status", {"status": "running", "speed": req.speed})

    return {"status": "started",
            "producer_pid": producer_proc.pid,
            "consumer_pid": consumer_proc.pid}


@router.post("/pause")
async def stream_pause() -> dict:
    pid = _state.get("producer_pid")
    if not pid or _state["status"] != "running":
        raise HTTPException(400, "Stream is not running")
    try:
        os.kill(pid, signal.SIGSTOP)
    except ProcessLookupError:
        raise HTTPException(404, "Producer process not found")
    _state["status"] = "paused"
    await _broadcast("status", {"status": "paused", "current_date": _state["current_date"]})
    return {"status": "paused"}


@router.post("/resume")
async def stream_resume() -> dict:
    pid = _state.get("producer_pid")
    if not pid or _state["status"] != "paused":
        raise HTTPException(400, "Stream is not paused")
    try:
        os.kill(pid, signal.SIGCONT)
    except ProcessLookupError:
        raise HTTPException(404, "Producer process not found")
    _state["status"] = "running"
    await _broadcast("status", {"status": "running", "current_date": _state["current_date"]})
    return {"status": "resumed"}


@router.post("/stop")
async def stream_stop() -> dict:
    for key in ("producer_pid", "consumer_pid"):
        pid = _state.get(key)
        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    _state.update({"status": "idle", "producer_pid": None, "consumer_pid": None,
                   "current_date": None, "dates_done": []})
    _date_users.clear()
    await _broadcast("status", {"status": "idle"})
    return {"status": "stopped"}


@router.post("/notify")
async def stream_notify(req: NotifyRequest) -> dict:
    """Called by consumer after each date is flushed to BigQuery + Feast."""
    _state["current_date"] = req.date
    if req.date not in _state["dates_done"]:
        _state["dates_done"].append(req.date)
    if req.msnos:
        _date_users[req.date] = req.msnos
    await _broadcast("date_done", {
        "date": req.date,
        "user_count": len(req.msnos),
        "dates_done": len(_state["dates_done"]),
    })
    return {"ok": True}


@router.get("/status")
def stream_status() -> dict:
    return {
        "status": _state["status"],
        "current_date": _state["current_date"],
        "dates_done": len(_state["dates_done"]),
        "dates_list": _state["dates_done"],
        "speed": _state["speed"],
        "total_days": TOTAL_DAYS,
    }


@router.get("/users")
def stream_users(date: str, page: int = 1, limit: int = 20) -> dict:
    users = _date_users.get(date, [])
    total = len(users)
    start = (page - 1) * limit
    return {
        "date": date,
        "total": total,
        "page": page,
        "pages": max(1, (total + limit - 1) // limit),
        "users": users[start: start + limit],
    }


@router.get("/events")
async def stream_events() -> StreamingResponse:
    """SSE — pushes `status` and `date_done` events to the browser."""
    q: Queue = Queue(maxsize=200)
    _sse_queues.append(q)

    init = json.dumps({
        "status": _state["status"],
        "current_date": _state["current_date"],
        "dates_done": len(_state["dates_done"]),
    })

    async def generator() -> AsyncGenerator[str, None]:
        yield f"event: status\ndata: {init}\n\n"
        try:
            while True:
                try:
                    yield await asyncio.wait_for(q.get(), timeout=20.0)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            try:
                _sse_queues.remove(q)
            except ValueError:
                pass

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
