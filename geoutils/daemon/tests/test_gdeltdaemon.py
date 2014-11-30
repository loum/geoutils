# pylint: disable=R0904,C0103
""":class:`geoutils.GdeltConfig` tests.

"""
import unittest2
import tempfile
import os

import geoutils
from geosutils.files import (copy_file,
                             remove_files)
import geolib_mock


class TestGdeltDaemon(unittest2.TestCase):
    """:class:`geoutils.GdeltDaemon` test cases.
    """
    @classmethod
    def setUpClass(cls):
        conf = os.path.join('geoutils',
                            'tests',
                            'files',
                            'proxy.properties')
        cls._mock = geolib_mock.MockServer(conf)
        cls._mock.start()

        cls._conf = geoutils.GdeltConfig()
        cls._conf.threads = 1
        cls._conf.inbound_dir = tempfile.mkdtemp()

        cls._gdelt_spatial_index_table_name = 'gdelt_spatial_index'
        cls._audit_table_name = 'audit'

    def setUp(self):
        self._gdeltd = geoutils.GdeltDaemon(pidfile=None, conf=self._conf)

        self._ds = geoutils.Datastore()
        self._ds.host = 'localhost'
        self._ds.port = 42425
        self._ds.user = 'root'

    def test_init(self):
        """Initialise a GdeltDaemon object.
        """
        msg = 'Object is not a geoutils.GdeltDaemon'
        self.assertIsInstance(self._gdeltd, geoutils.GdeltDaemon, msg)

    def test_start_dry_run(self):
        """GdeltDaemon dry run.
        """
        self._ds.connect()
        self._ds.init_table(self._gdelt_spatial_index_table_name)
        self._ds.init_table(self._audit_table_name)

        gdelt_dir = os.path.join('geoutils', 'daemon', 'tests', 'files')
        gdelt_file = '20141124.export.CSV.zip'
        target_file = os.path.join(self._conf.inbound_dir, gdelt_file)
        copy_file(os.path.join(gdelt_dir, gdelt_file), target_file)

        old_dry = self._gdeltd.dry

        self._gdeltd.dry = True
        self._gdeltd._start(self._gdeltd.exit_event)

        # Clean up.
        self._gdeltd.dry = old_dry
        self._gdeltd.exit_event.clear()
        remove_files(target_file)
        self._ds.delete_table(self._gdelt_spatial_index_table_name)
        self._ds.delete_table(self._audit_table_name)

    def tearDown(self):
        del self._gdeltd
        del self._ds

    @classmethod
    def tearDownClass(cls):
        cls._mock.stop()

        del cls._gdelt_spatial_index_table_name
        del cls._audit_table_name

        os.removedirs(cls._conf.inbound_dir)
        del cls._conf
