# pylint: disable=R0904,C0103
""":class:`geoutils.ModelBase` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock


class TestModelBase(unittest2.TestCase):
    """:class:`geoutils.ModelBase` test cases.
    """
    @classmethod
    def setUpClass(cls):
        """Attempt to start the Accumulo mock proxy server.
        """
        conf = os.path.join('geoutils',
                            'tests',
                            'files',
                            'proxy.properties')
        cls._mock = geolib_mock.MockServer(conf)
        cls._mock.start()

        cls._image_table_name = 'image_library'

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()
        cls._base = geoutils.ModelBase(cls._ds.connect())

    def test_init(self):
        """Initialise a :class:`geoutils.ModelBase` object.
        """
        msg = 'Object is not a geoutils.ModelBase'
        self.assertIsInstance(self._base, geoutils.ModelBase, msg)

    def test_query_no_data(self):
        """Query: no data.
        """
        self._ds.init_table(self._image_table_name)

        received = self._base.query(self._image_table_name)
        expected = []
        msg = 'Base query across empty image table should return []'
        self.assertListEqual(list(received), expected, msg)

        # Clean up.
        self._ds.delete_table(self._image_table_name)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()
        del cls._mock

        del cls._image_table_name

    @classmethod
    def tearDown(cls):
        cls._base = None
        del cls._base
        cls._ds = None
        del cls._ds
