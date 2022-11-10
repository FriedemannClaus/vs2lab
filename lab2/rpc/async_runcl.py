import logging

import async_rpc as rpc
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

def callback(res_list):
    print("Result: {}".format(res_list.value))

cl = rpc.Client()
cl.run()

base_list = rpc.DBList({'foo'})
cl.append("bar", base_list, callback)
cl.work_on_other_stuff()

cl.receiverThread.join()

print("\nClient process finished")
cl.stop()