# pylint: disable=R0904,C0103
""":class:`geoutils.model.Gdelt` tests.

"""
import unittest2
import os
import tempfile

import geoutils
import geolib_mock
from geoutils.model.tests.results.gdelt_01 import GDELT_MODEL
from geosutils.files import (copy_file,
                             remove_files)


class TestModelGdelt(unittest2.TestCase):
    """:class:`geoutils.model.Gdelt` test cases.
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

        cls._gdelt_table_name = 'gdelt_spatial_index'

        # Prepare a GDELT daemon for data ingest.
        cls._conf = geoutils.GdeltConfig()
        cls._conf.threads = 1
        cls._conf.inbound_dir = tempfile.mkdtemp()

        # The sample GDELT ingest file
        cls._gdelt_dir = os.path.join('geoutils',
                                      'daemon',
                                      'tests',
                                      'files')
        cls._gdelt_file = '20141124.export.CSV.zip'

    def setUp(self):
        self._ds = geoutils.Datastore()
        self._gdelt = geoutils.model.Gdelt(connection=self._ds.connect(),
                                           name=self._gdelt_table_name)

        self._gdeltd = geoutils.GdeltDaemon(pidfile=None, conf=self._conf)

    def test_init(self):
        """Initialise a :class:`geoutils.model.Gdelt` object.
        """
        msg = 'Object is not a geoutils.model.Gdelt'
        self.assertIsInstance(self._gdelt, geoutils.model.Gdelt, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._gdelt.name, 'gdelt_spatial_index', msg)

    def test_query_gdelt_no_data(self):
        """Query the GDELT component from an empty datastore.
        """
        self._ds.init_table(self._gdelt_table_name)

        received = self._gdelt.query_gdelt(key='i_3001a')
        expected = {}
        msg = 'Scan across empty GDELT table should return 0 cells'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._gdelt_table_name)

    def test_query_gdelt_with_data(self):
        """Attempt to query the GDELT component from the datastore.
        """
        self._ds.init_table(self._gdelt_table_name)

        # Load the sample GDELT data.
        target_file = os.path.join(self._conf.inbound_dir,
                                   self._gdelt_file)
        copy_file(os.path.join(self._gdelt_dir, self._gdelt_file),
                  target_file)
        self._gdeltd.filename = target_file
        self._gdeltd.delete = True
        self._gdeltd._start(self._gdeltd.exit_event)

        key = '0000_dqcjqbxu3u0u_09221955249654775807_324845985'
        received = self._gdelt.query_gdelt(key)
        expected = GDELT_MODEL['gdelt_01']
        msg = 'Scan across GDELT table should return cells'
        self.assertDictEqual(received, expected, msg)

        # ... and query missing row_id.
        received = self._gdelt.query_gdelt(key='dodgy')
        expected = {}
        msg = 'Scan across GDELT table against dodgy row_id error'
        self.assertDictEqual(received, expected, msg)

        # Uncomment the sleep statement to block and query some
        # sample data.
        # import time
        # time.sleep(1000)

        # Clean up.
        self._gdeltd.exit_event.clear()
        self._ds.delete_table(self._gdelt_table_name)

    def test_query_points(self):
        """Scan the GDELT spatial index table.
        """
        self._ds.init_table(self._gdelt_table_name)

        # Load the sample GDELT data.
        target_file = os.path.join(self._conf.inbound_dir,
                                   self._gdelt_file)
        copy_file(os.path.join(self._gdelt_dir, self._gdelt_file),
                  target_file)
        self._gdeltd.filename = target_file
        self._gdeltd.delete = True
        self._gdeltd._start(self._gdeltd.exit_event)

        points = [(-9.46, 147.19), (14.41, 121.03)]
        received = self._gdelt.query_points(points)
        expected = {
            'center_point_match': [
                '0000_rq2djmn8j2rf_09221955249654775807_324845987',
                '0000_wdw1fpbkwpnk_09221955249654775807_324846123',
            ]
        }
        msg = 'GDELT query points error'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._gdeltd.exit_event.clear()
        self._ds.delete_table(self._gdelt_table_name)

    def tearDown(self):
        del self._gdelt
        del self._ds
        del self._gdeltd

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

        del cls._gdelt_table_name
        del cls._gdelt_dir
        del cls._gdelt_file

        os.removedirs(cls._conf.inbound_dir)
        del cls._conf
