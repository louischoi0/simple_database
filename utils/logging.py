from datetime import datetime
from threading import Lock
import time

global LOG_ENABLE
LOG_ENABLE = True

_log_lock = Lock()
_timer_lock = Lock()
_log_file_path = "app.log"

def set_log_disable():
    global LOG_ENABLE
    LOG_ENABLE = False

def set_log_file(path: str):
    global _log_file_path
    _log_file_path = path

def info(module, *msg):
    global LOG_ENABLE
    if not LOG_ENABLE:
        return
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"{now}:<{module.zfill(8)}>   {' '.join(map(lambda x: str(x), msg))}"
    with _log_lock:
        print(line)
        with open(_log_file_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

def timer(func):
    def wrapper(*args, **kwargs):
        nonlocal total
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        with _timer_lock:
            total += duration
            line = f"Execution time: {duration:.6f}   Total: {total:.6f}"
            print(line)
        return result
    total = 0
    return wrapper