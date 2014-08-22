# pylint: disable=R0904,C0103
""":class:`geoutils.model.Metadata` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock


class TestModelMetadata(unittest2.TestCase):
    """:class:`geoutils.model.Metadata` test cases.
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

        cls._meta_table_name = 'meta_library'
        cls._image_table_name = 'image_library'
        cls._thumb_table_name = 'thumb_library'

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()
        cls._meta = geoutils.model.Metadata(cls._ds.connect())

    def test_init(self):
        """Initialise a :class:`geoutils.model.Metadata` object.
        """
        msg = 'Object is not a geoutils.model.Metadata'
        self.assertIsInstance(self._meta, geoutils.model.Metadata, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._meta.name, 'meta_library', msg)

    def test_query_metadata_no_data(self):
        """Query the metadata component from an empty datastore.
        """
        self._ds.init_table(self._meta_table_name)

        received = self._meta.query_metadata(key='i_3001a')
        expected = 0
        msg = 'Scan across empty table should return 0 cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_metadata_with_data(self):
        """Attempt to query the metadata component from the datastore.
        """
        from geoutils.tests.files.ingest_data_01 import DATA

        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA)

        received = self._meta.query_metadata(key='i_3001a',
                                             display=False)
        expected = 77
        msg = 'Scan across table should return cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_coords(self):
        """Scan the metadata datastore table.
        """
        self._ds.init_table(self._meta_table_name)

        from geoutils.tests.files.ingest_data_01 import DATA
        self._ds.ingest(DATA)

        received = self._meta.query_coords(jsonify=True)
        results_file = os.path.join('geoutils',
                                    'tests',
                                    'results',
                                    'coords01.txt')
        results_fh = open(results_file)
        expected = results_fh.readline().rstrip()
        msg = 'Image coordinates scan should return results'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_coords_missing_geogcs(self):
        """Scan the metadata datastore table: missing GEOGCS.
        """
        self._ds.init_table(self._meta_table_name)

        from geoutils.tests.files.ingest_data_02 import DATA
        self._ds.ingest(DATA)

        received = self._meta.query_coords(jsonify=False)
        expected = []
        msg = 'Image coords scan should return no results: no GEOGCS'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

        del cls._meta_table_name
        del cls._image_table_name
        del cls._thumb_table_name

    @classmethod
    def tearDown(cls):
        cls._meta = None
        del cls._meta
        cls._ds = None
        del cls._ds
