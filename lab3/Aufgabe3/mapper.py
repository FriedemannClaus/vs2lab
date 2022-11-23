import pickle
import re
import time
import threading

import constpipe
import zmq

class Mapper(threading.Thread):
    def __init__(self, me):
        threading.Thread.__init__(self)
        self.me = me
        address = "tcp://" + constpipe.SRC_IP + ":" + constpipe.SPLITTER_PORT  # SPLITTER

        context = zmq.Context()

        # receive
        self.pull_socket = context.socket(zmq.PULL)  # create a pull socket
        self.pull_socket.connect(address)  # connect to splitter

        # send
        self.push_socket1 = context.socket(zmq.PUSH) # create a push socket for reducer1
        self.push_socket2 = context.socket(zmq.PUSH) # create a push socket for reducer2

        prt1 = constpipe.MAPPER1_PORT1 if me == '1' else (constpipe.MAPPER2_PORT1 if me  == '2'
        else constpipe.MAPPER3_PORT1)  # check port
        prt2 = constpipe.MAPPER1_PORT2 if me == '1' else (constpipe.MAPPER2_PORT2 if me  == '2'
        else constpipe.MAPPER3_PORT2)  # check port
        address1 = "tcp://" + constpipe.SRC_IP + ":" + prt1  # how and where to connect
        address2 = "tcp://" + constpipe.SRC_IP + ":" + prt2  # how and where to connect
        self.push_socket1.bind(address1)
        self.push_socket2.bind(address2)
        time.sleep(1)

    def run(self):
        print("mapper {} started".format(self.me))

        while True:
            work = pickle.loads(self.pull_socket.recv())  # receive work from a source

            #check if told mission abort
            if work[1] == b"abort":
                # send abort to reducer
                self.push_socket1.send(pickle.dumps((self.me, b"abort")))
                self.push_socket2.send(pickle.dumps((self.me, b"abort")))
                break

            print("{} received workload {} from {}".format(self.me, work[1], work[0]))
            string = work[1]
            replace_signs = ',.!?'
            for i in replace_signs:
                string = string.replace(i,"")

            words = string.split()
            for word in words:
                if re.search("^[a-mA-M]", word) is not None:
                    #print("word:" + word + " String startet mit <=M")
                    self.push_socket1.send(pickle.dumps((1,word)))  # send word to reducer
                else:
                    #print("word:" + word +  "string startet mit >M" )
                    self.push_socket2.send(pickle.dumps((1,word))) # send word to reducer
