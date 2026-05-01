from datetime import datetime

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