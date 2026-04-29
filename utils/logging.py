from datetime import datetime

def info(module, msg):
    now= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{now} {module}: {msg}")