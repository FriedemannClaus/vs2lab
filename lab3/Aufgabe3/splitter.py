import pickle
import threading

import constpipe
import zmq

class Splitter(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        src = constpipe.SRC_IP   # set src host
        prt = constpipe.SPLITTER_PORT   # set splitter port

        context = zmq.Context()
        self.push_socket = context.socket(zmq.PUSH)  # create a push socket
        address = "tcp://" + src + ":" + prt  # how and where to connect
        self.push_socket.bind(address)  # bind socket to address

    def run(self):
        print("splitter started")
        # read txt file
        file = open("test.txt", "r", encoding="utf-8")
        Lines = file.readlines()

        for x in Lines:
            #print("string:" + x[:-1])
            self.push_socket.send(pickle.dumps(("splitter",x))) # send workload to mapper

        # tell mappers to exit infinity loop
        for x in range(3):
            self.push_socket.send(pickle.dumps(("splitter", b"abort")))
