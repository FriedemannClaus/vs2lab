"""
Simple client server unit test
"""

import logging
import threading
import unittest

import telefondienst
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

class TestEchoService(unittest.TestCase):
    """The test"""
    _server = telefondienst.Server()  # create single server in class variable
    _server_thread = threading.Thread(target=_server.serve)  # define thread for running server

    @classmethod
    def setUpClass(cls):
        cls._server_thread.start()  # start server loop in a thread (called only once)

    def setUp(self):
        super().setUp()
        self.client = telefondienst.Client()  # create new client for each test

    def test_srv_get(self):  # each test_* function is a test
        """Test simple get"""
        msg = self.client.get("Sascha")
        self.assertEqual(msg, '063412299')

    def test_srv_getall(self):
        """Test simple get"""
        msg = self.client.getall()
        telefonbuch = {
        "Sascha": "063412299",
        "Max" : "063412298",
        "Alice" : "063412297",
        "Bob" :"063412296"
        }
        self.assertEqual(msg, telefonbuch)

    def tearDown(self):
        self.client.close()  # terminate client after each test

    @classmethod
    def tearDownClass(cls):
        cls._server._serving = False  # break out of server loop. pylint: disable=protected-access
        cls._server_thread.join()  # wait for server thread to terminate


if __name__ == '__main__':
    unittest.main()
