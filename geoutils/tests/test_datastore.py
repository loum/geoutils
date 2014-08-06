# pylint: disable=R0904,C0103
""":class:`geoutils.Datastore` tests.

"""
import unittest2
import subprocess
import signal
import os
import time

import geoutils


class TestDatastore(unittest2.TestCase):
    """:class:`geoutils.Datastore` test cases.
    """
    @classmethod
    def setUpClass(cls):
        """Attempt to start the Accumulo mock proxy server.
        """
        cls._accumulo_server_exists = False
        accumulo_exe = os.path.join(os.sep, 'usr', 'bin', 'accumulo')
        if (os.path.isfile(accumulo_exe) and
            os.access(accumulo_exe, os.X_OK)):
            cls._accumulo_server_exists = True

            conf = os.path.join('geoutils',
                                'tests',
                                'files',
                                'proxy.properties')
            args = [accumulo_exe, 'proxy', '-p', conf]
            cls._proc = subprocess.Popen(args)
            time.sleep(1)

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()

    def test_init(self):
        """Initialise a :class:`geoutils.Datastore` object.
        """
        msg = 'Object is not a geoutils.Datastore'
        self.assertIsInstance(self._ds, geoutils.Datastore, msg)

    def test_connect(self):
        """Attempt a connection to an Accumulo datastore.
        """
        self._ds.connect()
        received = self._ds.connection
        msg = 'geoutils.Datastore.connection attribute should be set'
        self.assertIsNotNone(received, msg)

    def test_connect_bad_credentials(self):
        """Attempt a connection to an Accumulo datastore: bad creds.
        """
        self._ds.password = 'banana'
        self._ds.connect()
        received = self._ds.connection
        msg = 'geoutils.Datastore.connection attribute SHOULD NOT be set'
        self.assertIsNone(received, msg)

    def test_init_table_no_connection_state(self):
        """Initialise the image library table: no connection state.
        """
        received = self._ds.init_table()
        msg = 'Table initialisation (no connection) should return False'
        self.assertFalse(received, msg)

    def test_init_table_with_connection_state(self):
        """Initialise the image library table: with connection state.
        """
        self._ds.connect()
        received = self._ds.init_table()
        msg = 'Table initialisation should return True'
        self.assertTrue(received, msg)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        if cls._accumulo_server_exists:
            cls._proc.send_signal(signal.SIGINT)

    @classmethod
    def tearDown(cls):
        cls._ds = None
        del cls._ds
