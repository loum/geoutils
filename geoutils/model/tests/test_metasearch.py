# pylint: disable=R0904,C0103
""":class:`geoutils.model.Metasearch` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock


class TestModelMetasearch(unittest2.TestCase):
    """:class:`geoutils.model.Metasearch` test cases.
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

        cls._meta_search_name = 'meta_search'

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()
        cls._search = geoutils.model.Metasearch(cls._ds.connect(),
                                                name=cls._meta_search_name)

    def test_init(self):
        """Initialise a :class:`geoutils.model.Metasearch` object.
        """
        msg = 'Object is not a geoutils.model.Metasearch'
        self.assertIsInstance(self._search, geoutils.model.Metasearch, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._search.name, 'meta_search', msg)

    def test_query_metadata_no_data(self):
        """Query the metadata component from an empty datastore.
        """
        self._ds.init_table(self._meta_search_name)

        received = self._search.query_metadata(key='i_3001a')
        expected = {}
        msg = 'Scan across empty table should return 0 cells'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_search_name)

    def test_query_metadata_with_data(self):
        """Attempt to query the metadata component from the datastore.
        """
        from geoutils.tests.files.ingest_data_03 import DATA

        self._ds.init_table(self._meta_search_name)

        self._ds.ingest(DATA)

        search_terms = ['checks', 'Airfield']
        received = self._search.query_metadata(search_terms)
        print('xxx: %s' % received)
        #from geoutils.tests.results.meta_01 import META
        #expected = META
        #msg = 'Scan across table should return cells'
        #self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_search_name)

    def test_query_metadata_with_data_missing_row_id(self):
        """Query metadata from the datastore: missing row_id.
        """
        from geoutils.tests.files.ingest_data_03 import DATA

        self._ds.init_table(self._meta_search_name)

        self._ds.ingest(DATA)

        received = self._search.query_metadata(key='dodgy')
        from geoutils.tests.results.meta_01 import META
        expected = {}
        msg = 'Scan across metadata table against dodgy row_id error'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_search_name)

    def test_query_metadata_with_data_jsonify(self):
        """Query the metadata component from the datastore: jsonify.
        """
        from geoutils.tests.files.ingest_data_03 import DATA

        self._ds.init_table(self._meta_search_name)

        self._ds.ingest(DATA)

        received = self._search.query_metadata(key='i_3001a',
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
        self._ds.delete_table(self._meta_search_name)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

        del cls._meta_search_name

    @classmethod
    def tearDown(cls):
        cls._search = None
        del cls._search
        cls._ds = None
        del cls._ds
