# pylint: disable=R0904,C0103
""":class:`geoutils.InitConfig` tests.

"""
import unittest2
import os

import geoutils


class TestInitConfig(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'config',
                                 'tests',
                                 'files',
                                 'geoutils.conf')

    def setUp(self):
        self._conf = geoutils.InitConfig()

    def test_init(self):
        """Initialise a InitConfig object.
        """
        msg = 'Object is not a geoutils.InitConfig'
        self.assertIsInstance(self._conf, geoutils.InitConfig, msg)

    def test_parse_config(self):
        """Parse comms items from the config.
        """
        self._conf.set_config_file(self._file)
        self._conf.parse_config()

        received = self._conf.accumulo_host
        expected = 'accumulo_host'
        msg = 'accumulo_proxy_server.host not as expected'
        self.assertEqual(received, expected, msg)

        received = self._conf.accumulo_port
        expected = 12345
        msg = 'accumulo_proxy_server.port not as expected'
        self.assertEqual(received, expected, msg)

        received = self._conf.accumulo_user
        expected = 'accumulo_user'
        msg = 'accumulo_proxy_server.user not as expected'
        self.assertEqual(received, expected, msg)

        received = self._conf.accumulo_password
        expected = 'accumulo_pass'
        msg = 'accumulo_proxy_server.password not as expected'
        self.assertEqual(received, expected, msg)

    def tearDown(self):
        self._conf = None
        del self._conf

    @classmethod
    def tearDownClass(cls):
        cls._file = None
        del cls._file
