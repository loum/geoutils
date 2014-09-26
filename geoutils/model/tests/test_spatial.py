# pylint: disable=R0904,C0103
""":class:`geoutils.model.Spatial` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock


class TestModelSpatial(unittest2.TestCase):
    """:class:`geoutils.model.Spatial` test cases.
    """
    @classmethod
    def setUpClass(cls):
        """Attempt to start the Accumulo mock proxy server.
        """
        cls.maxDiff = None

        conf = os.path.join('geoutils',
                            'tests',
                            'files',
                            'proxy.properties')
        cls._mock = geolib_mock.MockServer(conf)
        cls._mock.start()

        cls._spatial_name = 'spatial'

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()
        cls._spatial = geoutils.model.Spatial(cls._ds.connect(),
                                             name=cls._spatial_name)

    def test_init(self):
        """Initialise a :class:`geoutils.model.Spatial` object.
        """
        msg = 'Object is not a geoutils.model.Spatial'
        self.assertIsInstance(self._spatial, geoutils.model.Spatial, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._spatial.name, 'spatial', msg)

    def test_gen_geohash(self):
        """Generate a base32 geohash string.
        """
        latitude = 42.6
        longitude = -5.6
        received = self._spatial.gen_geohash(latitude, longitude)
        expected = 'ezs42e44yx96'
        msg = 'Base32 geohash string error'
        self.assertEqual(received, expected, msg)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

        del cls._spatial_name

    @classmethod
    def tearDown(cls):
        cls._spatial = None
        del cls._spatial
        cls._ds = None
        del cls._ds
