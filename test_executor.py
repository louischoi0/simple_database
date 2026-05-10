from core.executor import init_select, init_insert
from core.dbmaster import DBMaster
from core.catalog import get_public_namespace, Schema, Column, get_type_val
from core.wal import _init_wal_system

if __name__ == "__main__":
    app = DBMaster(2)
    app.activate()

    test_table_schema = Schema([
        Column(0, 0, 0, "student_id", get_type_val("int"), notnull=True, defval=None),
        Column(0, 0, 1, "name", get_type_val("varchar"), notnull=True, defval=None),
        Column(0, 0, 2, "grade", get_type_val("int"), notnull=True, defval=None),
    ])

    d0 = { "student_id": 0, "name": "louis", "grade": 3}

    insert_query_state = init_insert(get_public_namespace(), 4001, d0)
    insert_query_state.exec()

    query_state = init_select(get_public_namespace(), 4001)

    print(query_state.exec())

    app.terminate()