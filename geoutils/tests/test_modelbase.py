# pylint: disable=R0904,C0103,W0142
""":class:`geoutils.ModelBase` tests.

"""
import unittest2
import os

from geoutils.tests.files.ingest_data_01 import DATA as DATA_01
from geoutils.tests.files.ingest_data_02 import DATA as DATA_02
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

        cls._meta_table_name = 'meta_library'
        cls._image_table_name = 'image_library'
        cls._image_spatial_index_table_name = 'image_spatial_index'

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

    def test_query_with_cols(self):
        """Query: data exists.
        """
        self._ds.init_table(self._meta_table_name)

        # Load sample data.  One record for now but we
        # probably want to expand this to provide more data.
        self._ds.ingest(DATA_01)

        table = self._meta_table_name

        # Dodgy family identifier.
        cols = [['banana']]
        received = self._base.query(table, cols=cols)
        expected = []
        msg = 'Base query with dodgy family column should return []'
        self.assertListEqual(list(received), expected, msg)

        # OK family, dodgy qualifier identifiers.
        cols = [['geoxform=0', 'banana']]
        received = self._base.query(table, cols=cols)
        expected = []
        msg = 'Base query with dodgy qualifier column should return []'
        self.assertListEqual(list(received), expected, msg)

        # OK family, OK qualifier identifiers.
        cols = [['geoxform=0', '84.999999864233729']]
        received = self._base.query(table, cols=cols)
        expected = 1
        msg = 'Base query with dodgy qualifier column should return []'
        self.assertEqual(len(list(received)), expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_regex_scan(self):
        """Scan table for key/regex value.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)
        self._ds.ingest(DATA_02)

        table = self._meta_table_name

        # Valid regex search.
        kwargs = {'family': 'metadata=NITF_FTITLE',
                  'qualifiers': ['Airfield', 'Checks an']}
        results = self._base.regex_scan(table, **kwargs)
        received = []
        for result in results:
            received.append(result.row)
        expected = ['i_3001a']
        msg = 'Regex scan should return row_ids'
        self.assertListEqual(received, expected, msg)

        # Missing search parameter regex search.
        kwargs = {'family': 'metadata=NITF_FTITLE',
                  'qualifiers': ['banana', 'Checks an']}
        received = self._base.regex_scan(table, **kwargs)
        expected = []
        msg = 'Regex scan with missing term should not return row_ids'
        self.assertListEqual(list(received), expected, msg)

        # Unmatched family/qualifier identifier regex search.
        kwargs = {'family': 'metadata=NITF_FDT',
                  'qualifiers': ['banana', 'Checks an']}
        received = self._base.regex_scan(table, **kwargs)
        expected = []
        msg = 'Unmatched family/qualifier should not return row_ids'
        self.assertListEqual(list(received), expected, msg)

        # Common family/qualifier identifier across records regex search.
        kwargs = {'family': 'metadata=NITF_IREP',
                  'qualifiers': ['MONO']}
        results = self._base.regex_scan(table, **kwargs)
        received = []
        for result in results:
            received.append(result.row)
        expected = ['i_3001a', 'i_6130e']
        msg = 'Common family/qualifier should return multiple row_ids'
        self.assertListEqual(list(received), expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_regex_query(self):
        """RegEx query.
        """
        self._ds.init_table(self._image_spatial_index_table_name)

        # Load sample data.  One record for now but we
        # probably want to expand this to provide more data.
        self._ds.ingest(DATA_01)

        table = self._image_spatial_index_table_name

        # Direct geohash match.
        regexs = ['.*tvu7whrjnc16.*']
        # Accumulo timestamp will mess with results.  Just get the row.
        results = self._base.regex_query(table, regexs=regexs)
        received = []
        for result in results:
            received.append(result.row)
        expected = ['0000_tvu7whrjnc16_09222521218464775807']
        msg = 'RegEx query returned error'
        self.assertListEqual(list(received), expected, msg)

        # Reduced geohash precision search.
        regexs = ['.*tvu7whrjnc1.*']
        results = self._base.regex_query(table, regexs=regexs)
        received = []
        for result in results:
            received.append(result.row)
        expected = ['0000_tvu7whrjnc16_09222521218464775807']
        msg = 'RegEx query (reduced resolution) error'
        self.assertListEqual(received, expected, msg)

        # Missing geohash search.
        regexs = ['.*banana.*']
        received = self._base.regex_query(table, regexs=regexs)
        expected = []
        msg = 'RegEx query (missing geohash) error'
        self.assertListEqual(list(received), expected, msg)

        # Clean up.
        self._ds.delete_table(self._image_spatial_index_table_name)

    def test_regex_query_string_based_regex(self):
        """RegEx query: string based regex.
        """
        self._ds.init_table(self._image_spatial_index_table_name)

        # Load sample data.  One record for now but we
        # probably want to expand this to provide more data.
        self._ds.ingest(DATA_01)

        table = self._image_spatial_index_table_name

        # Direct geohash match.
        regex = '.*tvu7whrjnc16.*'
        results = self._base.regex_query(table, regexs=regex)
        received = []
        for result in results:
            received.append(result.row)
        expected = ['0000_tvu7whrjnc16_09222521218464775807']
        msg = 'RegEx query (string based) returned error'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_batch_regex_scan(self):
        """Batch scan across table for key/regex values.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)
        self._ds.ingest(DATA_02)

        table = self._meta_table_name

        # Valid regex search.
        kwargs = {'metadata=NITF_FTITLE': ['Airfield', 'Checks an']}
        received = self._base.batch_regex_scan(table, kwargs)
        expected = ['i_3001a']
        msg = 'Batch regex scan should return row_ids'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_batch_regex_scan_unmatched(self):
        """Batch scan across table for key/regex values: unmatched.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)
        self._ds.ingest(DATA_02)

        table = self._meta_table_name

        # Unmatched qualifier identifier regex search.
        kwargs = {'metadata=NITF_IREP': ['banana'],
                  'metadata=NITF_FTITLE': ['Airfield', 'Checks an']}
        received = self._base.batch_regex_scan(table, kwargs)
        expected = []
        msg = 'Batch regex scan should return row_ids'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_batch_regex_scan_multiple_matched(self):
        """Batch scan across table for key/regex values: multiple matched.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)
        self._ds.ingest(DATA_02)

        table = self._meta_table_name

        # Unmatched qualifier identifier regex search.
        kwargs = {'metadata=NITF_IREP': ['MONO']}
        received = self._base.batch_regex_scan(table, kwargs)
        expected = ['i_3001a', 'i_6130e']
        msg = 'Batch regex scan should return multiple row_ids'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_batch_regex_scan_single_qualifier_unmatched(self):
        """Batch scan across table for key/regex values: single unmatched.
        """
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA_01)
        self._ds.ingest(DATA_02)

        table = self._meta_table_name

        # Single unmatched qualifier identifier regex search.
        kwargs = {'metadata=NITF_IREP': ['banana'],
                  'metadata=NITF_FTITLE': ['Airfield', 'Checks an']}
        received = self._base.batch_regex_scan(table, kwargs)
        expected = []
        msg = 'Regex scan (single unmatched) should not return row_ids'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()
        del cls._mock

        del cls._meta_table_name
        del cls._image_table_name
        del cls._image_spatial_index_table_name

    @classmethod
    def tearDown(cls):
        cls._base = None
        del cls._base
        cls._ds = None
        del cls._ds
