import pickle
import time
import threading

import constpipe
import zmq

class Reducer(threading.Thread):
    def __init__(self, me):
        threading.Thread.__init__(self)
        self.me = me

        # specify address
        prt1 = constpipe.MAPPER1_PORT1 if me == '1' else constpipe.MAPPER1_PORT2
        prt2 = constpipe.MAPPER2_PORT1 if me == '1' else constpipe.MAPPER2_PORT2
        prt3 = constpipe.MAPPER3_PORT1 if me == '1' else constpipe.MAPPER3_PORT2
        address1 = "tcp://" + constpipe.SRC_IP + ":" + prt1  # 1st task src
        address2 = "tcp://" + constpipe.SRC_IP + ":" + prt2  # 2nd task src
        address3 = "tcp://" + constpipe.SRC_IP + ":" + prt3  # 2nd task src

        context = zmq.Context()
        self.pull_socket = context.socket(zmq.PULL)  # create a pull socket

        #connect sockets
        self.pull_socket.connect(address1)  # connect to task source 1
        self.pull_socket.connect(address2)  # connect to task source 2
        self.pull_socket.connect(address3)  # connect to task source 1
        time.sleep(1)
        self.dictionary = {}
        self.counter = 0

    def run(self):
        print("reducer {} started".format(self.me))
        mapper_counter = 3

        while True:
            work = pickle.loads(self.pull_socket.recv())  # receive work from a source
            #check if told mission abort
            if work[1] == b"abort":
                mapper_counter -=1
                if mapper_counter == 0:
                    break
            else:
                self.counter += 1
                if work[1] not in self.dictionary:
                    self.dictionary[work[1]] = 1
                else:
                    self.dictionary[work[1]] += 1
                print("Reducer {} received word: {} count {} current Total-wordcount: {}"
                .format(self.me, work[1], self.dictionary[work[1]],self.counter))
