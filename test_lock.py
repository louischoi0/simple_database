from core.dbmaster import DBMaster
from core.lock import monitor_lock_state, Lock


def test0(app):
    oid = 0
    mode = 1
    lock = Lock(oid, mode, 7)

    res = lock.acquire()
    assert res
    monitor_lock_state()
    res = lock.try_acquire()
    assert not res

    lock.release()
    monitor_lock_state()

if __name__ == "__main__":
    app = DBMaster(2)
    app.activate()

    test0(app)

    app.terminate()