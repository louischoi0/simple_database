from datetime import datetime
import time

global LOG_ENABLE
LOG_ENABLE = True


def set_log_disable():
    global LOG_ENABLE
    LOG_ENABLE = False

def info(module, msg):
    global LOG_ENABLE

    if not LOG_ENABLE: 
        return

    now= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{now}:<{module.zfill(8)}>   {msg}")


def timer(func):
    def wrapper(*args, **kwargs):
        nonlocal total
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        total += duration
        print(f"Execution time: {duration}   Total: {total}")
        return result

    total = 0
    return wrapper
