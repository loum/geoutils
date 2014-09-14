# pylint: disable=R0904,C0103
""":class:`geoutils.IngestConfig` tests.

"""
import unittest2
import os

import geoutils


class TestIngestConfig(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'config',
                                 'tests',
                                 'files',
                                 'geoutils.conf')

    def setUp(self):
        self._conf = geoutils.IngestConfig()

    def test_init(self):
        """Initialise a IngestConfig object.
        """
        msg = 'Object is not a geoutils.IngestConfig'
        self.assertIsInstance(self._conf, geoutils.IngestConfig, msg)

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

        received = self._conf.namenode_host
        expected = 'jp2044lm-hdfs-nn01'
        msg = 'hdfs_namenode.host not as expected'
        self.assertEqual(received, expected, msg)

        received = self._conf.namenode_port
        expected = 50079
        msg = 'hdfs_namenode.port not as expected'
        self.assertEqual(received, expected, msg)

        received = self._conf.namenode_user
        expected = 'hdfs_user'
        msg = 'hdfs_namenode.user not as expected'
        self.assertEqual(received, expected, msg)

        received = self._conf.namenode_target_path
        expected = 'tmp'
        msg = 'hdfs_namenode.target_path not as expected'
        self.assertEqual(received, expected, msg)

    def tearDown(self):
        self._conf = None
        del self._conf

    @classmethod
    def tearDownClass(cls):
        cls._file = None
        del cls._file