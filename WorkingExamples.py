import json, threading
import data_store
ds = data_store.data_store()
v = '{"AGE":21, "ROLL": 11}'
try:
    threading.Thread(ds.Create('Jim', v)).start()
except Exception as e:
    print(e)
try:
    threading.Thread(ds.Read('Jim')).start()
except Exception as e:
    print(e)
try:
    threading.Thread(ds.Delete('Jim')).start()
except Exception as e:
    print(e)