# pylint: disable=R0904,C0103
""":class:`geoutils.index.Spatial` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock


class TestSpatial(unittest2.TestCase):
    """:class:`geoutils.index.Spatial` test cases.
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

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()
        cls._spatial = geoutils.index.Spatial()

    def test_init(self):
        """Initialise a :class:`geoutils.index.Spatial` object.
        """
        msg = 'Object is not a geoutils.index.Spatial'
        self.assertIsInstance(self._spatial, geoutils.index.Spatial, msg)

    def test_gen_geohash(self):
        """Generate a base32 geohash string.
        """
        latitude = 42.6
        longitude = -5.6
        received = self._spatial.gen_geohash(latitude, longitude)
        expected = 'ezs42e44yx96'
        msg = 'Base32 geohash string error'
        self.assertEqual(received, expected, msg)

    def test_get_stripe_token(self):
        """Generate a stripe_token.
        """
        # Set a new stripe value.
        old_stripes = self._spatial.stripes
        self._spatial.stripes = 100

        source = 'i_3001a'
        received = self._spatial.get_stripe_token(source)
        expected = '0021'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

        source = 'i_6130e'
        received = self._spatial.get_stripe_token(source)
        expected = '0031'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

        # Restore the default.
        self._spatial.stripes = old_stripes

        source = 'i_3001a'
        received = self._spatial.get_stripe_token(source)
        expected = '0000'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

        source = 'i_6130e'
        received = self._spatial.get_stripe_token(source)
        expected = '0000'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

    @classmethod
    def tearDown(cls):
        cls._spatial = None
        del cls._spatial
        cls._ds = None
        del cls._ds
