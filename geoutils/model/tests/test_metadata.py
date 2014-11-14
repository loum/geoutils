# pylint: disable=R0904,C0103
""":class:`geoutils.model.Metadata` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock
from geoutils.tests.files.ingest_data_01 import DATA as DATA_01
from geoutils.tests.files.ingest_data_02 import DATA as DATA_02


class TestModelMetadata(unittest2.TestCase):
    """:class:`geoutils.model.Metadata` test cases.
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

        cls._meta_table_name = 'meta_library'
        cls._image_spatial_index_table_name = 'image_spatial_index'

    def setUp(self):
        self._ds = geoutils.Datastore()
        self._meta = geoutils.model.Metadata(connection=self._ds.connect(),
                                             name=self._meta_table_name)
        self._meta.coord_cols = [['coord=0'],
                                 ['coord=1'],
                                 ['coord=2'],
                                 ['coord=3']]

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
        expected = {}
        msg = 'Scan across empty table should return 0 cells'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_metadata_with_data(self):
        """Attempt to query the metadata component from the datastore.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)

        received = self._meta.query_metadata(key='i_3001a')
        from geoutils.tests.results.meta_01 import META
        expected = META
        msg = 'Scan across table should return cells'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_metadata_with_data_missing_row_id(self):
        """Query metadata from the datastore: missing row_id.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)

        received = self._meta.query_metadata(key='dodgy')
        from geoutils.tests.results.meta_01 import META
        expected = {}
        msg = 'Scan across metadata table against dodgy row_id error'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_metadata_with_data_jsonify(self):
        """Query the metadata component from the datastore: jsonify.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)

        received = self._meta.query_metadata(key='i_3001a',
                                             jsonify=True)
        results_file = os.path.join('geoutils',
                                    'tests',
                                    'results',
                                    'ingest_data_01.txt')
        results_fh = open(results_file)
        expected = results_fh.readline().rstrip()
        msg = 'JSON results from metadata scan error'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_coords(self):
        """Scan the metadata datastore table.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)

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

        self._ds.ingest(DATA_02)

        received = self._meta.query_coords(jsonify=False)
        expected = {}
        msg = 'Image coords scan should return no results: no GEOGCS'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_points(self):
        """Scan the metadata spatial index table.
        """
        self._ds.init_table(self._image_spatial_index_table_name)

        self._ds.ingest(DATA_01)

        point = (32.9831944444, 85.0001388889)
        received = self._meta.query_points(point)
        expected = {'center_point_match': ['i_3001a']}
        msg = 'Image points scan should return results'
        self.assertDictEqual(received, expected, msg)

        # Shift the grid up around 110KM.
        point = (34.0, 85.0001388889)
        received = self._meta.query_points(point)
        expected = {'center_point_match': []}
        msg = 'Image points scan should not return results'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_bbox_points(self):
        """Scan the metadata spatial index table: bbox.
        """
        self._ds.init_table(self._image_spatial_index_table_name)

        self._ds.ingest(DATA_01)

        point = (32.0, 84.0, 34.0, 86.0)
        received = self._meta.query_bbox_points(point)
        expected = {'center_point_match': ['i_3001a']}
        msg = 'Image points scan should return results'
        self.assertDictEqual(received, expected, msg)

        # Shift the boundary box.
        point = (12.0, 84.0, 14.0, 86.0)
        received = self._meta.query_bbox_points(point)
        expected = {'center_point_match': []}
        msg = 'Image points scan should not return results'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_scan_metadata_no_data(self):
        """Chained meta scans: no data.
        """
        self._ds.init_table(self._meta_table_name)

        kwargs = {'metadata=NITF_IREP': ['MONO'],
                  'metadata=NITF_FTITLE': ['Airfield', 'Checks an']}
        received = self._meta.scan_metadata(search_terms=kwargs)
        expected = {'metas': []}
        msg = 'Chained meta scans should return no results'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_scan_metadata(self):
        """Chained meta scans.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)
        self._ds.ingest(DATA_02)

        # Single result.
        kwargs = {'metadata=NITF_IREP': ['MONO'],
                  'metadata=NITF_FTITLE': ['Airfield', 'Checks an']}
        received = self._meta.scan_metadata(search_terms=kwargs)
        expected = {'metas': ['i_3001a']}
        msg = 'Chained meta scans should return single result'
        self.assertDictEqual(received, expected, msg)

        # Multiple results.
        kwargs = {'metadata=NITF_IREP': ['MONO']}
        received = self._meta.scan_metadata(search_terms=kwargs)
        expected = {'metas': ['i_3001a', 'i_6130e']}
        msg = 'Chained meta scans should return multiple results'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def tearDown(self):
        self._meta = None
        del self._meta
        self._ds = None
        del self._ds

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

        del cls._meta_table_name
        del cls._image_spatial_index_table_name
