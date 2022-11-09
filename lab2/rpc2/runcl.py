import rpc
import logging

from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

cl = rpc.Client()

base_list = rpc.DBList({'foo'})
#result_list = 
cl.append('bar', base_list, cl.call_back_func)

#print("Result: {}".format(result_list.value))

cl.stop()
