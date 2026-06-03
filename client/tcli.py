import asyncio
import json
import uuid
import websockets
import sys

SERVER_URI = "ws://localhost:5678"

async def send_request(ws, request) -> dict:
    await ws.send(json.dumps(request))
    raw = await ws.recv()
    return json.loads(raw)

def create_request__query_create_index(table_oid):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "create_index", "payload": { "table_oid": table_oid, "target_col": "grade" } }

def create_request__query_select(table_oid):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "select", "payload": { "table_oid": table_oid } }

def create_request__query_get(table_oid, key):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "get", "payload": { "table_oid": table_oid, "key": int(key) } }

def create_request__bt_new_heap_page(table_oid, new_min_key):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "bt_new_heap_page", "payload": { "table_oid": table_oid, "new_min_key": new_min_key } }

def create_request__bt_insert_tuple(table_oid, min_key):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "bt_tuple_insert", "payload": { "table_oid": table_oid, "data": { "student_id": min_key, "name": "louis", "grade": min_key % 10 }} }

def create_request__query_scan_index(entry_pg_id):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "scan_index", "payload": { "entry_pg_id": entry_pg_id } }

def create_request__query_i_select(entry_pg_id, table_oid):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "i_select", "payload": { "index_entry_pg_id": entry_pg_id, "table_oid": table_oid } }

def create_request__query_scan_heap(page_id, table_oid=4001):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "scan_heap", "payload": { "page_id": int(page_id), "table_oid": int(table_oid) } }

def create_request__query_update_tuple_heap(page_id, pk, new_tuple, table_oid=4001):
    request_id = str(uuid.uuid4())
    new_tuple = {'student_id': 8944, 'name': 'louis', 'grade': 4999} 

    return {"request_id": request_id, "command": "update_heap_tuple", "payload": { "page_id": int(page_id), "owner_oid": int(table_oid), "pk": int(pk), "new_tuple": new_tuple } }

# ── commands ──────────────────────────────────────────────────────────────────

async def scan_heap(args):
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_scan_heap(*args)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)
        
async def update_heap_tuple(args):
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_update_tuple_heap(*args)
        res = await send_request(ws, request)
        print(res["data"])

async def get(args):
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_get(4001, args[0])
        res = await send_request(ws, request)
        print(res["data"])

async def select(args):
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_select(4001)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def i_select(args):
    index_entry_pg_id = int(args[0])
    oid = int(args[1])
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_i_select(index_entry_pg_id, oid)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def scan_index(args):
    entry_pg_id = int(args[0])
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_scan_index(entry_pg_id)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def create_index(args):
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_create_index(4001)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def insert_tp(args):
    min_key = int(args[0])
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__bt_insert_tuple(4001, min_key)
        res = await send_request(ws, request)
        print(res)

async def scenario_zero(args):
    count = int(args[0])

    async with websockets.connect(SERVER_URI) as ws:
        from random import randint
        for i in range(0, count):
            k = randint(0, 20000)
            print(k)
            request = create_request__bt_insert_tuple(4001, k)
            res = await send_request(ws, request)

            #from time import sleep
            #sleep(0.5)
            print(res)

# ── dispatch ──────────────────────────────────────────────────────────────────

COMMANDS = {
    "select":        (select,        ""),
    "get":           (get,  "<key>"),
    "i_select":      (i_select,      "<entry_pg_id> <table_oid>"),
    "scan_index":    (scan_index,    "<entry_pg_id>"),
    "scan_heap":     (scan_heap, "<page_id>"),
    "create_index":  (create_index,  ""),
    "insert_tp":     (insert_tp,     "<min_key>"),
    "zero": (scenario_zero, "<count>"),
    "update": (update_heap_tuple, "<count>"),
}

def print_usage():
    print("usage: client.py <command> [args...]")
    print()
    print("commands:")
    for name, (_, usage) in COMMANDS.items():
        print(f"  {name} {usage}")

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print_usage()
        sys.exit(0)

    cmd = sys.argv[1]
    args = sys.argv[2:]

    if cmd not in COMMANDS:
        print(f"unknown command: {cmd}")
        print_usage()
        sys.exit(1)

    func, _ = COMMANDS[cmd]
    asyncio.run(func(args))
