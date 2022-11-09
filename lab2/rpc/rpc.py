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
        self.result_list = []

    def run(self):
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')
        print("Thread waiting for response")
        msgrcv = self.chan.receive_from(self.server)  # wait for response / final result
        result_list = msgrcv[1]
        print("Result: {}".format(result_list.value))

    def stop(self):
        self.chan.leave('client')

    def call_back_func(self, abc):
        """callbackfunction"""
        print("callback: bums" + abc)
        #print("Result: {}".format(abc.value))
        return

    def append(self, data, db_list, callback):
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server
        print("Client waiting for ack")
        reqack = self.chan.receive_from(self.server)  # wait for ack
        print (reqack[1])
        print("reqack ende")
        if reqack[1] == "ack":
            print("Client received ack")
            self.start() # start thread
            #msgrcv = self.chan.receive_from(self.server)  # wait for response / final result
            print("antwort erhalten")
            print("Client macht irgendwas")
            print("Client macht immer noch irgendwas")
            #return msgrcv[1]  # pass it to caller
            self.join()  # Wait for the background task to finish
            #callback(msgrcv[1])
            callback("bums")

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
