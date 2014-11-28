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

    def test_source_file_no_file(self):
        """Check inbound directory for NITF file: empty directory.
        """
        msg = 'Source file in empty directory should return None'
        self.assertIsNone(self._gdeltd.source_file(), msg)

    def test_source_file_valid_nitf_file(self):
        """Check inbound directory for NITF file: NITF file exists.
        """
        source_file = os.path.join('geoutils',
                                   'tests',
                                   'files',
                                   'i_3001a.ntf')
        target_file = os.path.join(self._gdeltd.conf.inbound_dir,
                                   os.path.basename(source_file))
        copy_file(source_file, target_file)

        received = self._gdeltd.source_file()
        expected = target_file
        msg = 'Source NITF file should return file path'
        self.assertEqual(received, expected, msg)

        # Clean up.
        remove_files(target_file)

    def test_initialise_with_nitf_file(self):
        """Initialise a GdeltDaemon with a file.
        """
        source_file = os.path.join('geoutils',
                                   'tests',
                                   'files',
                                   'i_3001a.ntf')
        target_file = os.path.join(self._gdeltd.conf.inbound_dir,
                                   os.path.basename(source_file))
        copy_file(source_file, target_file)

        ingestd = geoutils.GdeltDaemon(pidfile=None,
                                        filename=source_file,
                                        conf=self._conf)
        received = ingestd.source_file()
        expected = target_file
        msg = 'Initialised GdeltDaemon should return file path'
        self.assertEqual(received, expected, msg)

        # ... and the batch should be set.
        msg = 'Initialised GdeltDaemon batch should be True'
        self.assertTrue(ingestd.batch, msg)

        # Clean up.
        remove_files(target_file)

    def tearDown(self):
        self._gdeltd = None
        del self._gdeltd

        self._ds = None
        del self._ds

    @classmethod
    def tearDownClass(cls):
        cls._mock.stop()

        del cls._gdelt_spatial_index_table_name

        os.removedirs(cls._conf.inbound_dir)
        cls._conf = None
        del cls._conf
