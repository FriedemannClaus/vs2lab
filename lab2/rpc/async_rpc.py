import constRPC
import time
import logging
import threading
from context import lab_channel

class DBList:
    def __init__(self, basic_list):
        self.value = list(basic_list)

    def append(self, data):
        self.value = self.value + [data]
        return self


class Client():
    _logger = logging.getLogger("vs2lab.lab2.rpc.rpc_cl")

    def __init__(self):
        self.chan = lab_channel.Channel()
        self.client = self.chan.join('client')
        self.server = None

    def run(self):
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')

    def stop(self):
        self.chan.leave('client')

    def append(self, data, db_list, callback):
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send the message to the server
        self._logger.info("Sent the request to the server.")
        reply = None
        while reply[1] != "ACK":
            reply = self.chan.receive_from(self.server)
        self._logger.info("The server acknowledged the request.")
        self.receiverThread = self.ReceiverThread(self, callback)
        self.receiverThread.start()

    def printMsgs(self):
        # client is still active and does not block
        for i in range(1, 10):
            print("Client is still working on work " + str(i))
            time.sleep(1)

    class ReceiverThread(threading.Thread):
        def __init__(self, client, callback):
            threading.Thread.__init__(self)
            self.cl = client
            self.callback = callback

        def run(self):
            reply = self.cl.chan.receive_from(self.cl.server)  # fetch for the response
            self.cl._logger.info("Receiver-thread received the result from the server.")
            self.callback(reply[1])


class Server:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.server = self.chan.join('server')
        self.timeout = 3

    @staticmethod
    def append(data, db_list):
        assert isinstance(db_list, DBList)  # - Make sure we have a list
        return db_list.append(data)

    def run(self):
        self.chan.bind(self.server)
        while True:
            msgreq = self.chan.receive_from_any(
                self.timeout)  # wait for any request
            if msgreq is not None:
                client = msgreq[0]  # see who is the caller
                msgrpc = msgreq[1]  # fetch call & parameters
                # check what is being requested
                if constRPC.APPEND == msgrpc[0]:
                    self.chan.send_to({client}, "ACK")  # send ACK
                    result = self.append(msgrpc[1], msgrpc[2])  # do local call
                    time.sleep(10.0)  # simulate 10 sec working time
                    self.chan.send_to({client}, result)  # return response
                else:
                    pass  # unsupported request, simply ignore