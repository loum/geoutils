# pylint: disable=R0904,C0103
""":class:`geoutils.MockServer` tests.

"""
import unittest2
import os

import geoutils


class TestMockServer(unittest2.TestCase):
    """:class:`geoutils.MockServer` test cases.
    """
    @classmethod
    def setUp(cls):
        cls._mock = geoutils.MockServer()

    def test_init(self):
        """Initialise a geoutils.NITF object.
        """
        msg = 'Object is not a geoutils.MockServer'
        self.assertIsInstance(self._mock, geoutils.MockServer, msg)

    def test_start_stop(self):
        """Start/stop the Accumulo mock server.
        """
        conf = os.path.join('geoutils',
                            'tests',
                            'files',
                            'proxy.properties')
        self._mock.properties_file = conf
        self._mock.start()
        msg = 'Mock server instance should be ACTIVE'
        self.assertTrue(self._mock.is_active, msg)

        self._mock.stop()
        msg = 'Mock server instance should be INACTIVE'
        self.assertFalse(self._mock.is_active, msg)

    @classmethod
    def tearDown(cls):
        cls._mock = None
        del cls._mock
