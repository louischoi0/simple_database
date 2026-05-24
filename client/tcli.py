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

def create_request__bt_new_heap_page(table_oid, new_min_key):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "bt_new_heap_page", "payload": { "table_oid": table_oid, "new_min_key": new_min_key } }

def create_request__bt_insert_tuple(table_oid, min_key):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "bt_tuple_insert", "payload": { "table_oid": table_oid,  "data": { "student_id": min_key, "name": "louis", "grade": min_key % 10 }} }

def create_request__query_scan_index(entry_pg_id):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "scan_index", "payload": { "entry_pg_id": entry_pg_id } }

def create_request__query_i_select(entry_pg_id, table_oid):
    request_id = str(uuid.uuid4())
    return {"request_id": request_id, "command": "i_select", "payload": { "index_entry_pg_id": entry_pg_id, "table_oid": table_oid } }

async def select():
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_select(4001)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def i_select(index_entry_pg_id, oid):
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_i_select(index_entry_pg_id, oid)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def scan_index(entry_pg_id):
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_scan_index(entry_pg_id)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def create_index():
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__query_create_index(4001)
        res = await send_request(ws, request)
        for i in res["data"]:
            print(i)

async def insert_tp():
    async with websockets.connect(SERVER_URI) as ws:
        request = create_request__bt_insert_tuple(4001, int(sys.argv[1]))
        res = await send_request(ws, request)
        print(res["data"])

async def scenario_zero():
    async with websockets.connect(SERVER_URI) as ws:
        """
        request = create_request__bt_new_heap_page(4001, 27)
        res = await send_request(ws, request)
        print(res)
        """

        for i in range(1, 70):
            request = create_request__bt_insert_tuple(4001, i)
            res = await send_request(ws, request)
            print(res)

async def main():
    async with websockets.connect(SERVER_URI) as ws:
        #request = create_request__bt_new_heap_page(4001, int(sys.argv[1]))
        request = create_request__bt_insert_tuple(4001, int(sys.argv[1]))
        res = await send_request(ws, request)
        print(res["data"])

if __name__ == "__main__":
    #asyncio.run(insert_tp())
    #asyncio.run(scenario_zero())
    #asyncio.run(select())
    #asyncio.run(create_index())
    #asyncio.run(scan_index(217))
    asyncio.run(i_select(217, 4001))
