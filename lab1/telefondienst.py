"""
Client and server using classes
"""
import logging
import socket
import json

import const_cs
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)  # init loging channels for the lab

# pylint: disable=logging-not-lazy, line-too-long

class Server:
    """ The server """
    _logger = logging.getLogger("vs2lab.lab1.telefondienst.Server")
    _serving = True

    telefonbuch = {
        "Sascha": "063412299",
        "Max" : "063412298",
        "Alice" : "063412297",
        "Bob" :"063412296"
    }

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # prevents errors due to "addresses in use"
        self.sock.bind((const_cs.HOST, const_cs.PORT))
        self.sock.settimeout(3)  # time out in order not to block forever
        self._logger.info("Server bound to socket " + str(self.sock))

    def serve(self):
        """ Serve echo """
        self.sock.listen(1)
        while self._serving:  # as long as _serving (checked after connections or socket timeouts)
            try:
                # pylint: disable=unused-variable
                (connection, address) = self.sock.accept()  # returns new socket and address of client
                while True:  # forever
                    data = connection.recv(1024).decode('ascii')  # receive data from client
                    if not data:
                        break  # stop if client stopped
                    if data.startswith("GETALL"):
                        self._logger.info("Server received GETALL request")
                        connection.send(json.dumps(self.telefonbuch).encode('ascii'))
                    elif data.startswith("GET "):
                        self._logger.info("Server received GET request for " + data[4:])
                        if data[4:] in self.telefonbuch:
                            connection.send(self.telefonbuch.get(data[4:]).encode('ascii'))
                        else:
                            connection.send("name does not exist.".encode('ascii'))
                    else:
                        connection.send("request must start with GET or GETALL".encode('ascii'))
                connection.close()  # close the connection
            except socket.timeout:
                pass  # ignore timeouts
        self.sock.close()
        self._logger.info("Server down.")

    def close(self):
        """ Close socket """
        _serving = False
        #self.sock.close()
        self._logger.info("Server down.")

class Client:
    """ The client """
    logger = logging.getLogger("vs2lab.a1_layers.clientserver.Client")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((const_cs.HOST, const_cs.PORT))
        self.logger.info("Client connected to socket " + str(self.sock))

    def getall(self):
        """ getall server """
        self.sock.send(("GETALL").encode('ascii'))  # send encoded string as data
        response = json.loads(self.sock.recv(1024).decode('ascii'))
        for keys,values in response.items():
            print(keys + ": " + values)
        return response

    def get(self, key):
        """ get server """
        self.sock.send(("GET " + key).encode('ascii'))  # send encoded string as data
        response = self.sock.recv(1024).decode('ascii')  # receive the response and decode
        print(key + ": " + response)
        return response

    def close(self):
        """ Close socket """
        self.sock.close()
        self.logger.info("Client down.")