# pylint: disable=R0904,C0103
""":class:`geoutils.IngestConfig` tests.

"""
import unittest2
import tempfile
import os

import geoutils
from geosutils.files import (copy_file,
                             remove_files)


class TestIngestDaemon(unittest2.TestCase):
    """:class:`geoutils.IngestDaemon` test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls._conf = geoutils.IngestConfig()
        cls._conf.inbound_dir = tempfile.mkdtemp()

    def setUp(self):
        self._ingestd = geoutils.IngestDaemon(pidfile=None,
                                              conf=self._conf)

    def test_init(self):
        """Initialise a IngestDaemon object.
        """
        msg = 'Object is not a geoutils.IngestDaemon'
        self.assertIsInstance(self._ingestd, geoutils.IngestDaemon, msg)

#    def test_start_dry_run(self):
#        """IngestDaemon dry run.
#        """
#        old_dry = self._ingestd.dry
#
#        self._ingestd.dry = True
#        self._ingestd._start(self._ingestd.exit_event)
#
#        # Clean up.
#        self._ingestd.dry = old_dry
#        self._ingestd.exit_event.clear()

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
        msg = 'Source NITF file should return file path'
        received = self._ingestd.source_file()
        expected = target_file
        self.assertEqual(received, expected, msg)

        # Clean up.
        remove_files(target_file)

    def tearDown(self):
        self._ingestd = None
        del self._ingestd

    @classmethod
    def tearDownClass(cls):
        os.removedirs(cls._conf.inbound_dir)
        cls._conf = None
        del cls._conf
