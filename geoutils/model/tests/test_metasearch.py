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

        search_terms = ['checks', 'Airfield']
        received = self._search.query_metadata(search_terms)
        expected = []
        msg = 'Free text metadata should not return results'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_search_name)

    def test_query_metadata_free_text(self):
        """Query the metadata: free text.
        """
        from geoutils.tests.files.ingest_data_03 import DATA

        self._ds.init_table(self._meta_search_name)

        self._ds.ingest(DATA)

        search_terms = ['checks', 'Airfield']
        received = self._search.query_metadata(search_terms)
        expected = ['i_3001a']
        msg = 'Free text metadata should return results'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_search_name)

    def test_query_metadata_free_text_unmatched(self):
        """Query the metadata free text: (unmatched).
        """
        from geoutils.tests.files.ingest_data_03 import DATA

        self._ds.init_table(self._meta_search_name)

        self._ds.ingest(DATA)

        search_terms = ['banana', 'Airfield']
        received = self._search.query_metadata(search_terms)
        expected = []
        msg = 'Free text metadata (unmatched) should NO return results'
        self.assertListEqual(received, expected, msg)

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
