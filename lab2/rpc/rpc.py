import threading
import time
import constRPC


from context import lab_channel


class DBList:
    def __init__(self, basic_list):
        self.value = list(basic_list)

    def append(self, data):
        self.value = self.value + [data]
        return self


class Client(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.chan = lab_channel.Channel()
        self.client = self.chan.join('client')
        self.server = None
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')
        self.result_list = []

    def run(self):
        print("thread started: waiting for response")
        msgrcv = self.chan.receive_from(self.server)  # wait for response
        self.result_list = msgrcv[1]
        print("thread done: response received")

    def stop(self):
        self.chan.leave('client')

    def call_back_func(self, res):
        """callbackfunction"""
        print("Result: {}".format(res.value))
        return

    def append(self, data, db_list, callback):
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server
        print("Client waiting for ack")
        reqack = self.chan.receive_from(self.server)  # wait for ack
        if reqack[1] == "ack":
            print("Client received ack")
            self.start() # start thread
            time.sleep(3)
            print("Client macht irgendwas")
            time.sleep(5)
            print("Client macht immer noch irgendwas")
            time.sleep(5)
            print("Client tut immer noch als würde er was machen")
            self.join()  # Wait for the background task to finish
            callback(self.result_list)

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
            msgreq = self.chan.receive_from_any(self.timeout)  # wait for any request
            if msgreq is not None:
                client = msgreq[0]  # see who is the caller
                msgrpc = msgreq[1]  # fetch call & parameters
                if constRPC.APPEND == msgrpc[0]:  # check what is being requested
                    self.chan.send_to({client}, "ack")  # sending acknowledge for the request
                    print("Server is waiting 10 seconds.")
                    time.sleep(10)  # wait 10 seconds after receiving ack
                    print("Server 10 seconds wait is over.")
                    result = self.append(msgrpc[1], msgrpc[2])  # do local call
                    self.chan.send_to({client}, result)  # return response
                else:
                    pass  # unsupported request, simply ignore
