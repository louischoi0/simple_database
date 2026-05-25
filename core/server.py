from __future__ import annotations

import asyncio
import json
import os
import signal
import uuid
from dataclasses import dataclass, field
from typing import Any

from core.executor import QueryExecutionCtx, BtreePageExpandState, init_insert, BtreePageScanState, BuildIndexState
from core.executor import BtreeIndexPageScanState
from core.tx import generate_xid
from core.catalog import get_public_namespace, init_scan_index
from utils.logging import info
from core.catalog import init_table_access, create_index
from core.page_mgr import ref_heap_page
from core.heap import StructuredTuple
import traceback

import websockets
from websockets.server import WebSocketServerProtocol

_info = lambda *x: info("server", *x)

def make_response(request_id: str, status: str, data: Any) -> str:
    return json.dumps({"request_id": request_id, "status": status, "data": data})

def make_error(request_id: str, message: str) -> str:
    return make_response(request_id, "error", {"message": message})

@dataclass
class ClientRequest:
    request_id: str
    command: str
    payload: dict[str, Any]
    future: asyncio.Future

@dataclass
class WorkerResult:
    request_id: str
    status: str
    data: Any

def create_new_ctx(app) -> QueryExecutionCtx:
    xid = generate_xid()
    return QueryExecutionCtx(xid=xid, wal_writer=app.wal_writer, allocator=app.alloc)

#def begin_transaction(ctx) ->

def command_callback(app):
    app.cache_pool.autocommit()

async def execute_command(app, command: str, payload: dict[str, Any]) -> WorkerResult:
    if command == "ping":
        return WorkerResult(request_id="", status="ok", data={"pong": True})
    
    elif command == "bt_tuple_insert":
        ctx = create_new_ctx(app) 

        execution = init_insert(get_public_namespace(), payload["table_oid"], payload["data"])
        execution.exec(ctx)
        execution.on_finished()

        command_callback(app)
        return WorkerResult(request_id="", status="ok", data=execution.result)
    
    elif command == "bt_new_heap_page":
        ctx = create_new_ctx(app) 

        table_access = init_table_access(get_public_namespace(), payload["table_oid"], lockmode=2)
        execution = BtreePageExpandState(table_access, table_access.desc_pg_id, payload["new_min_key"])
        execution.exec(ctx)
        execution.on_finished()

        command_callback(app)
        return WorkerResult(request_id="", status="ok", data={})
    
    elif command == "select":
        ctx = create_new_ctx(app)

        table_access = init_table_access(get_public_namespace(), payload["table_oid"], lockmode=1)
        execution = BtreePageScanState(table_access)

        execution.exec(ctx)
        execution.on_finished()

        data = list( x.struct(table_access.schema) for x in  execution.result )
        return WorkerResult(request_id="", status="ok", data=data)
    
    elif command == "i_select":
        ctx = create_new_ctx(app)

        table_access = init_table_access(get_public_namespace(), payload["table_oid"], lockmode=1)
        table_access.index_entry_pg_id = payload["index_entry_pg_id"]

        from core.executor import Equal, IndexValueCol

        execution = BtreeIndexPageScanState(table_access, table_access.index_entry_pg_id, predicate=Equal(IndexValueCol, 4))
        execution.exec(ctx)
        execution.on_finished()

        return WorkerResult(request_id="", status="ok", data=list( x.structured_data for x in  execution.result))
    
    elif command == "create_index":
        ctx = create_new_ctx(app)
        table_access = init_table_access(get_public_namespace(), payload["table_oid"], lockmode=2)

        build_execution = BuildIndexState(table_access, payload["target_col"])
        build_execution.exec(ctx)
        create_index(ctx.allocator, get_public_namespace(), **payload)

        command_callback(app)

        return WorkerResult(request_id="", status="ok", data={})
    
    elif command == "scan_heap":
        ctx = create_new_ctx(app)
        table_access = init_table_access(get_public_namespace(), payload["table_oid"], lockmode=None)

        heap_page = ref_heap_page(payload["page_id"])
        read_datas = heap_page.raw_map(lambda buffer: StructuredTuple.parse(buffer).struct(table_access.schema))

        return WorkerResult(request_id="", status="ok", data=read_datas)

    elif command == "scan_index":
        ctx = create_new_ctx(app)

        table_access = init_scan_index(get_public_namespace(), payload["entry_pg_id"], lockmode=1)
        execution = BtreePageScanState(table_access)

        execution.exec(ctx)
        execution.on_finished()

        data = list( x.struct(table_access.schema) for x in  execution.result )
        return WorkerResult(request_id="", status="ok", data=data)
    
    return WorkerResult(
        request_id="",
        status="error",
        data={"message": f"Unknown command: {command}"},
    )


class Worker:
    def __init__(self, app, worker_id: int, request_queue: asyncio.Queue[ClientRequest]) -> None:
        self.app = app
        self.worker_id = worker_id
        self.request_queue = request_queue
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"worker-{self.worker_id}")
        _info("worker %d started", self.worker_id)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        _info("worker %d stopped", self.worker_id)

    async def _run(self) -> None:
        while True:
            request: ClientRequest = await self.request_queue.get()
            _info("Worker %d handling request %s", self.worker_id, request.request_id)

            try:
                result = await execute_command(self.app, request.command, request.payload)
                result.request_id = request.request_id

                if not request.future.done():
                    request.future.set_result(result)

            except Exception as exc:
                tb = traceback.format_exc()

                _info("Worker %d error on request %s\n%s", self.worker_id, request.request_id, tb)
                if not request.future.done():
                    request.future.set_exception(exc)

            finally:
                self.request_queue.task_done()

class WorkerPool:
    def __init__(self, app, size: int) -> None:
        self.size = size
        self.app = app
        self.request_queue: asyncio.Queue[ClientRequest] = asyncio.Queue()
        self._workers: list[Worker] = []

    def start(self) -> None:
        for i in range(self.size):
            w = Worker(app=self.app, worker_id=i, request_queue=self.request_queue)
            w.start()
            self._workers.append(w)
        _info("workerPool started with %d workers", self.size)

    async def stop(self) -> None:
        for w in self._workers:
            await w.stop()
        log.info("workerPool stopped")

    async def submit(self, request: ClientRequest) -> None:
        await self.request_queue.put(request)


class ServerHandler:

    def __init__(self, app, pool: WorkerPool) -> None:
        self.app = app
        self.pool = pool

    async def handle(self, ws: WebSocketServerProtocol) -> None:
        client_addr = ws.remote_address
        _info("client connected: %s", client_addr)

        try:
            async for raw in ws:
                await self._dispatch(ws, raw)
        except websockets.exceptions.ConnectionClosedOK:
            pass
        except websockets.exceptions.ConnectionClosedError as exc:
            _info("Client %s closed with error: %s", client_addr, exc)
        finally:
            _info("Client disconnected: %s", client_addr)

    async def _dispatch(self, ws: WebSocketServerProtocol, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            await ws.send(make_error("unknown", "Invalid JSON"))
            return

        request_id: str = msg.get("request_id") or str(uuid.uuid4())
        command: str    = msg.get("command", "")
        payload: dict   = msg.get("payload") or {}

        if not command:
            await ws.send(make_error(request_id, "Missing 'command' field"))
            return

        # 2) Future 생성 및 요청 제출
        loop = asyncio.get_event_loop()
        future: asyncio.Future[WorkerResult] = loop.create_future()

        request = ClientRequest(
            request_id=request_id,
            command=command,
            payload=payload,
            future=future,
        )
        await self.pool.submit(request)

        # 3) 결과 대기 후 응답 전송
        try:
            result: WorkerResult = await asyncio.wait_for(future, timeout=30.0)
            await ws.send(make_response(request_id, result.status, result.data))

        except asyncio.TimeoutError:
            await ws.send(make_error(request_id, "Request timed out"))

        except Exception as exc:
            await ws.send(make_error(request_id, str(exc)))

class DBServer:
    def __init__(
        self,
        app,
        host: str = "localhost",
        port: int = 5678,
        worker_count: int = 4,
    ) -> None:
        self.host = host
        self.port = port
        self.worker_count = worker_count
        self._pool: WorkerPool | None = None
        self._app = app

    async def run(self) -> None:
        self._pool = WorkerPool(app=self._app, size=self.worker_count)
        self._pool.start()

        handler = ServerHandler(app=self._app, pool=self._pool)

        stop_event = asyncio.Event()
        loop = asyncio.get_event_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

        async with websockets.serve(handler.handle, self.host, self.port):
            _info("dBServer listening on ws://%s:%d (workers=%d)",
                     self.host, self.port, self.worker_count)
            await stop_event.wait()

        _info("shutting down...")
        await self._pool.stop()
        _info("dBServer stopped.")


if __name__ == "__main__":
    host         = os.getenv("DB_HOST", "localhost")
    port         = int(os.getenv("DB_PORT", "5678"))
    worker_count = int(os.getenv("DB_WORKERS", "4"))

    asyncio.run(DBServer(host=host, port=port, worker_count=worker_count).run())


