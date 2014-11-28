# pylint: disable=R0904,C0103
""":class:`geoutils.IngestDaemon` tests.

"""
import unittest2
import tempfile
import os

import geoutils
from geosutils.files import (copy_file,
                             remove_files)
import geolib_mock


class TestIngestDaemon(unittest2.TestCase):
    """:class:`geoutils.IngestDaemon` test cases.
    """
    @classmethod
    def setUpClass(cls):
        conf = os.path.join('geoutils',
                            'tests',
                            'files',
                            'proxy.properties')
        cls._mock = geolib_mock.MockServer(conf)
        cls._mock.start()

        cls._conf = geoutils.IngestConfig()
        cls._conf.threads = 1
        cls._conf.inbound_dir = tempfile.mkdtemp()

        cls._meta_table_name = 'meta_library'
        cls._image_spatial_index_table_name = 'image_spatial_index'
        cls._metasearch_table_name = 'meta_search'
        cls._image_table_name = 'image_library'
        cls._thumb_table_name = 'thumb_library'
        cls._audit_table_name = 'audit'

    def setUp(self):
        self._ingestd = geoutils.IngestDaemon(pidfile=None,
                                              conf=self._conf)

        self._ds = geoutils.Datastore()
        self._ds.host = 'localhost'
        self._ds.port = 42425
        self._ds.user = 'root'

    def test_init(self):
        """Initialise a IngestDaemon object.
        """
        msg = 'Object is not a geoutils.IngestDaemon'
        self.assertIsInstance(self._ingestd, geoutils.IngestDaemon, msg)

    def test_start_dry_run(self):
        """IngestDaemon dry run.
        """
        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._image_spatial_index_table_name)
        self._ds.init_table(self._metasearch_table_name)
        self._ds.init_table(self._image_table_name)
        self._ds.init_table(self._thumb_table_name)
        self._ds.init_table(self._audit_table_name)

        ntf_dir = os.path.join('geoutils', 'tests', 'files')
        ntf_file = 'i_3001a.ntf'
        target_file = os.path.join(self._conf.inbound_dir, ntf_file)
        copy_file(os.path.join(ntf_dir, ntf_file), target_file)

        old_dry = self._ingestd.dry

        self._ingestd.dry = True
        self._ingestd._start(self._ingestd.exit_event)

        # Clean up.
        self._ingestd.dry = old_dry
        self._ingestd.exit_event.clear()
        remove_files(target_file)
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._image_spatial_index_table_name)
        self._ds.delete_table(self._metasearch_table_name)
        self._ds.delete_table(self._image_table_name)
        self._ds.delete_table(self._thumb_table_name)
        self._ds.delete_table(self._audit_table_name)

    def test_source_file_no_file(self):
        """Check inbound directory for NITF file: empty directory.
        """
        msg = 'Source file in empty directory should return None'
        self.assertIsNone(self._ingestd.source_file(), msg)

    def test_source_file_valid_nitf_file(self):
        """Check inbound directory for NITF file: NITF file exists.
        """
        source_file = os.path.join('geoutils',
                                   'tests',
                                   'files',
                                   'i_3001a.ntf')
        target_file = os.path.join(self._ingestd.conf.inbound_dir,
                                   os.path.basename(source_file))
        copy_file(source_file, target_file)

        received = self._ingestd.source_file()
        expected = target_file
        msg = 'Source NITF file should return file path'
        self.assertEqual(received, expected, msg)

        # Clean up.
        remove_files(target_file)

    def test_initialise_with_nitf_file(self):
        """Initialise a IngestDaemon with a file.
        """
        source_file = os.path.join('geoutils',
                                   'tests',
                                   'files',
                                   'i_3001a.ntf')
        target_file = os.path.join(self._ingestd.conf.inbound_dir,
                                   os.path.basename(source_file))
        copy_file(source_file, target_file)

        ingestd = geoutils.IngestDaemon(pidfile=None,
                                        filename=source_file,
                                        conf=self._conf)
        received = ingestd.source_file()
        expected = target_file
        msg = 'Initialised IngestDaemon should return file path'
        self.assertEqual(received, expected, msg)

        # ... and the batch should be set.
        msg = 'Initialised IngestDaemon batch should be True'
        self.assertTrue(ingestd.batch, msg)

        # Clean up.
        remove_files(target_file)

    def tearDown(self):
        self._ingestd = None
        del self._ingestd

        self._ds = None
        del self._ds

    @classmethod
    def tearDownClass(cls):
        cls._mock.stop()

        del cls._meta_table_name
        del cls._image_spatial_index_table_name
        del cls._metasearch_table_name
        del cls._image_table_name
        del cls._thumb_table_name
        del cls._audit_table_name

        os.removedirs(cls._conf.inbound_dir)
        cls._conf = None
        del cls._conf
