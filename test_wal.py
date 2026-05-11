from core.executor import init_select, init_insert
from core.dbmaster import DBMaster
from core.catalog import get_public_namespace, Schema, Column, get_type_val
from core.wal import create_xlog_heap_insert_cmd
from core.heap import StructuredTuple

def test2():
    payload = { "a": "asdfsdafb", "c": "1111"}
    cmd = XLogHeapInsertCMD(0, 1, payload)

    buffer = cmd.ser()

    cmd2 = XLog.decode(buffer)

    assert cmd.cmd == cmd2.cmd

def test3(app):
    test_table_schema = Schema([
        Column(0, 0, 0, "student_id", get_type_val("int"), notnull=True, defval=None),
        Column(0, 0, 1, "name", get_type_val("varchar"), notnull=True, defval=None),
        Column(0, 0, 2, "grade", get_type_val("int"), notnull=True, defval=None),
        #Column(0, 3, "grade2", get_type_val("int"), notnull=True, defval=None),
    ])

    data_template = {
        "student_id": 0,
        "name": "student_",
        "grade": 1,
        #"grade2": 2,
    }

    tuple = StructuredTuple.load(test_table_schema, data_template)
    cmd = create_xlog_heap_insert_cmd(xid=7, rel_id=17, page_id=0, slot_index=0, tuple=tuple)
    app.wal_writer.write_xlog(cmd)

if __name__ == "__main__":
    app = DBMaster(2)
    app.activate()

    test3(app)

    #app.terminate()