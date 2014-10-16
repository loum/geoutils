# pylint: disable=R0904,C0103
""":class:`geoutils.StagerConfig` tests.

"""
import unittest2
import os

import geoutils


class TestStagerConfig(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'config',
                                 'tests',
                                 'files',
                                 'geoutils.conf')

    def setUp(self):
        self._conf = geoutils.StagerConfig()

    def test_init(self):
        """Initialise a StagerConfig object.
        """
        msg = 'Object is not a geoutils.StagerConfig'
        self.assertIsInstance(self._conf, geoutils.StagerConfig, msg)

    def test_parse_config(self):
        """Parse comms items from the config.
        """
        self._conf.set_config_file(self._file)
        self._conf.parse_config()

        received = self._conf.inbound_dir
        expected = '/var/tmp/geoingest'
        msg = 'ingest.inbound_dir not as expected'
        self.assertEqual(received, expected, msg)

    def tearDown(self):
        self._conf = None
        del self._conf

    @classmethod
    def tearDownClass(cls):
        cls._file = None
        del cls._file
