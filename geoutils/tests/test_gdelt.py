# pylint: disable=R0904,C0103
""":class:`geoutils.Gdelt` tests.

"""
import unittest2

import geoutils
from geoutils.tests.files.test_gdelt_data import DATA
from geoutils.tests.results.gdelt_01 import GDELT_SCHEMA


class TestGdelt(unittest2.TestCase):
    """:class:`geoutils.Gdelt` test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None

    def setUp(self):
        self._gdelt = geoutils.Gdelt()

    def test_init(self):
        """Initialise a geoutils.Gdelt object.
        """
        msg = 'Object is not a geoutils.Gdelt'
        self.assertIsInstance(self._gdelt, geoutils.Gdelt, msg)

    def test_extract_gdelt(self):
        """Extract Event Geography line from GDELT.
        """
        data = DATA['gdelt_001']

        gdelt = geoutils.Gdelt()

        gdelt.extract_gdelt(data)

        # GlobalEventID.
        received = gdelt.event_id
        expected = '324845984'
        msg = 'Extracted GlobalEventID error'
        self.assertEqual(received, expected, msg)

        # Day.
        received = gdelt.event_day
        expected = '20041126'
        msg = 'Extracted Day error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_ADM1Code.
        received = gdelt.adm1_code
        expected = 'USDC'
        msg = 'Extracted Actor1Geo_ADM1Code error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_CountryCode.
        received = gdelt.country_code
        expected = 'US'
        msg = 'Extracted Actor1Geo_CountryCode error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_FeatureID.
        received = gdelt.feature_id
        expected = '531871'
        msg = 'Extracted Actor1Geo_FeatureID error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Fullname.
        received = gdelt.fullname
        expected = 'Washington, District of Columbia, United States'
        msg = 'Extracted Actor1Geo_Fullname error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Lat.
        received = gdelt.latitude
        expected = '38.8951'
        msg = 'Extracted Actor1Geo_Lat error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Long.
        received = gdelt.longitude
        expected = '-77.0364'
        msg = 'Extracted Actor1Geo_Long error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Type.
        received = gdelt.type
        expected = '3'
        msg = 'Extracted Actor1Geo_Type error'
        self.assertEqual(received, expected, msg)

        # DATEADDED.
        received = gdelt.date_added
        expected = '20141124'
        msg = 'Extracted DATEADDED error'
        self.assertEqual(received, expected, msg)

        # SOURCEURL.
        received = gdelt.source_url[:31] + '...'
        expected = 'http://www.news.com.au/national...'
        msg = 'Extracted SOURCEURL error'
        self.assertEqual(received, expected, msg)

    def test_extract_gdelt_via_init(self):
        """Extract Event Geography line from GDELT: via init.
        """
        data = DATA['gdelt_001']

        gdelt = geoutils.Gdelt(data)

        # GlobalEventID.
        received = gdelt.event_id
        expected = '324845984'
        msg = 'Extracted GlobalEventID error'
        self.assertEqual(received, expected, msg)

        # Day.
        received = gdelt.event_day
        expected = '20041126'
        msg = 'Extracted Day error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_ADM1Code.
        received = gdelt.adm1_code
        expected = 'USDC'
        msg = 'Extracted Actor1Geo_ADM1Code error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_CountryCode.
        received = gdelt.country_code
        expected = 'US'
        msg = 'Extracted Actor1Geo_CountryCode error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_FeatureID.
        received = gdelt.feature_id
        expected = '531871'
        msg = 'Extracted Actor1Geo_FeatureID error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Fullname.
        received = gdelt.fullname
        expected = 'Washington, District of Columbia, United States'
        msg = 'Extracted Actor1Geo_Fullname error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Lat.
        received = gdelt.latitude
        expected = '38.8951'
        msg = 'Extracted Actor1Geo_Lat error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Long.
        received = gdelt.longitude
        expected = '-77.0364'
        msg = 'Extracted Actor1Geo_Long error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Type.
        received = gdelt.type
        expected = '3'
        msg = 'Extracted Actor1Geo_Type error'
        self.assertEqual(received, expected, msg)

        # DATEADDED.
        received = gdelt.date_added
        expected = '20141124'
        msg = 'Extracted DATEADDED error'
        self.assertEqual(received, expected, msg)

        # SOURCEURL.
        received = gdelt.source_url[:31] + '...'
        expected = 'http://www.news.com.au/national...'
        msg = 'Extracted SOURCEURL error'
        self.assertEqual(received, expected, msg)

    def test_extract_gdelt_old_style(self):
        """Extract Event Geography line from GDELT: pre April 2013 format.
        """
        data = DATA['gdelt_002']

        gdelt = geoutils.Gdelt(data)

        # GlobalEventID.
        received = gdelt.event_id
        expected = '324845984'
        msg = 'Extracted GlobalEventID error'
        self.assertEqual(received, expected, msg)

        # Day.
        received = gdelt.event_day
        expected = '20041126'
        msg = 'Extracted Day error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_ADM1Code.
        received = gdelt.adm1_code
        expected = 'USDC'
        msg = 'Extracted Actor1Geo_ADM1Code error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_CountryCode.
        received = gdelt.country_code
        expected = 'US'
        msg = 'Extracted Actor1Geo_CountryCode error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_FeatureID.
        received = gdelt.feature_id
        expected = '531871'
        msg = 'Extracted Actor1Geo_FeatureID error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Fullname.
        received = gdelt.fullname
        expected = 'Washington, District of Columbia, United States'
        msg = 'Extracted Actor1Geo_Fullname error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Lat.
        received = gdelt.latitude
        expected = '38.8951'
        msg = 'Extracted Actor1Geo_Lat error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Long.
        received = gdelt.longitude
        expected = '-77.0364'
        msg = 'Extracted Actor1Geo_Long error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Type.
        received = gdelt.type
        expected = '3'
        msg = 'Extracted Actor1Geo_Type error'
        self.assertEqual(received, expected, msg)

        # DATEADDED.
        received = gdelt.date_added
        expected = '20141124'
        msg = 'Extracted DATEADDED error'
        self.assertEqual(received, expected, msg)

        # SOURCEURL.
        received = gdelt.source_url
        msg = 'Extracted SOURCEURL error'
        self.assertIsNone(received, msg)

    def test_extract_gdelt_error(self):
        """Extract Event Geography line from GDELT: error
        """
        data = DATA['gdelt_003']

        gdelt = geoutils.Gdelt(data)

        # GlobalEventID.
        received = gdelt.event_id
        expected = '324845987'
        msg = 'Extracted GlobalEventID error'
        self.assertEqual(received, expected, msg)

        # Day.
        received = gdelt.event_day
        expected = '20041126'
        msg = 'Extracted Day error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_ADM1Code.
        received = gdelt.adm1_code
        expected = 'PP20'
        msg = 'Extracted Actor1Geo_ADM1Code error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_CountryCode.
        received = gdelt.country_code
        expected = 'PP'
        msg = 'Extracted Actor1Geo_CountryCode error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_FeatureID.
        received = gdelt.feature_id
        expected = '-2198538'
        msg = 'Extracted Actor1Geo_FeatureID error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Fullname.
        received = gdelt.fullname
        expected = 'Port Moresby, National Capital, Papua New Guinea'
        msg = 'Extracted Actor1Geo_Fullname error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Lat.
        received = gdelt.latitude
        expected = '-9.46472'
        msg = 'Extracted Actor1Geo_Lat error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Long.
        received = gdelt.longitude
        expected = '147.193'
        msg = 'Extracted Actor1Geo_Long error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Type.
        received = gdelt.type
        expected = '4'
        msg = 'Extracted Actor1Geo_Type error'
        self.assertEqual(received, expected, msg)

        # DATEADDED.
        received = gdelt.date_added
        expected = '20141124'
        msg = 'Extracted DATEADDED error'
        self.assertEqual(received, expected, msg)

        # SOURCEURL.
        received = gdelt.source_url[:31] + '...'
        expected = 'http://www.jaunted.com/story/20...'
        msg = 'Extracted SOURCEURL error'
        self.assertEqual(received, expected, msg)

    def test_extract_gdelt_errored_empty_lat_lone(self):
        """Extract Event Geography line from GDELT: error empty lat/long
        """
        data = DATA['gdelt_004']

        gdelt = geoutils.Gdelt(data)

        # GlobalEventID.
        received = gdelt.event_id
        expected = '324846008'
        msg = 'Extracted GlobalEventID error'
        self.assertEqual(received, expected, msg)

        # Day.
        received = gdelt.event_day
        expected = '20131124'
        msg = 'Extracted Day error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_ADM1Code.
        received = gdelt.adm1_code
        expected = ''
        msg = 'Extracted Actor1Geo_ADM1Code error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_CountryCode.
        received = gdelt.country_code
        expected = ''
        msg = 'Extracted Actor1Geo_CountryCode error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_FeatureID.
        received = gdelt.feature_id
        expected = ''
        msg = 'Extracted Actor1Geo_FeatureID error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Fullname.
        received = gdelt.fullname
        expected = ''
        msg = 'Extracted Actor1Geo_Fullname error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Lat.
        received = gdelt.latitude
        expected = ''
        msg = 'Extracted Actor1Geo_Lat error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Long.
        received = gdelt.longitude
        expected = ''
        msg = 'Extracted Actor1Geo_Long error'
        self.assertEqual(received, expected, msg)

        # Actor1Geo_Type.
        received = gdelt.type
        expected = '0'
        msg = 'Extracted Actor1Geo_Type error'
        self.assertEqual(received, expected, msg)

        # DATEADDED.
        received = gdelt.date_added
        expected = '20141124'
        msg = 'Extracted DATEADDED error'
        self.assertEqual(received, expected, msg)

        # SOURCEURL.
        received = gdelt.source_url[:31] + '...'
        expected = 'http://myrepublica.com/portal/i...'
        msg = 'Extracted SOURCEURL error'
        self.assertEqual(received, expected, msg)

    def test_callable(self):
        """Invoke the geoutils.Gdelt object instance.
        """
        data = DATA['gdelt_001']

        gdelt = geoutils.Gdelt(data)

        received = gdelt()
        expected = GDELT_SCHEMA
        msg = 'Callable geoutils.Gdelt return value error'
        self.assertDictEqual(received, expected, msg)

    def tearDown(self):
        self._gdelt = None
        del self._gdelt

    @classmethod
    def tearDownClass(cls):
        pass
