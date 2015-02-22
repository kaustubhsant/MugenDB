from datetime import time
import json

def savekeymap(keylocation=None):
    keymapfile = "KeyMap.txt"
    while True:
        with open(keymapfile,'w') as kf:
            json.dump(keylocation,kf)
        time.sleep(10)