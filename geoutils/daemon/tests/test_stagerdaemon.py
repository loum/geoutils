# pylint: disable=R0904,C0103,W0212
""":class:`geoutils.StagerConfig` tests.

"""
import unittest2
import tempfile
import os

import geoutils
from geosutils.files import (copy_file,
                             remove_files,
                             get_directory_files_list)


class TestStagerDaemon(unittest2.TestCase):
    """:class:`geoutils.StagerDaemon` test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls._conf = geoutils.StagerConfig()
        cls._conf.inbound_dir = tempfile.mkdtemp()

    def setUp(self):
        self._stagerd = geoutils.StagerDaemon(pidfile=None,
                                              conf=self._conf)
        self._stagerd.regen_sleep = 0.01

    def test_init(self):
        """Initialise a StagerDaemon object.
        """
        msg = 'Object is not a geoutils.StagerDaemon'
        self.assertIsInstance(self._stagerd, geoutils.StagerDaemon, msg)

    def test_start_dry_run_undefined_fixture_dir(self):
        """StagerDaemon dry run: undefined fixture directory.
        """
        old_dry = self._stagerd.dry

        self._stagerd.dry = True
        self._stagerd._start(self._stagerd.exit_event)

        received = self._stagerd.files_to_process
        expected = []
        msg = 'Dry run: undefined fixture directory should return []'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._stagerd.dry = old_dry
        self._stagerd.exit_event.clear()

    def test_start_dry_run(self):
        """StagerDaemon dry run.
        """
        fixture_dir = tempfile.mkdtemp()

        test_dir = os.path.join('geoutils', 'tests', 'files')
        test_files = ['i_3001a.ntf', 'i_6130e.ntf']
        for test_file in test_files:
            copy_file(os.path.join(test_dir, test_file),
                      os.path.join(fixture_dir, test_file))

        old_dry = self._stagerd.dry
        old_fixture_dir = self._stagerd.fixture_dir
        self._stagerd.fixture_dir = fixture_dir

        self._stagerd.dry = True
        self._stagerd._start(self._stagerd.exit_event)

        received = self._stagerd.files_to_process
        expected = [os.path.join(fixture_dir, x) for x in test_files]
        msg = 'Dry pass: fixture file list error'
        self.assertListEqual(sorted(received), sorted(expected), msg)

        # Clean up.
        self._stagerd.dry = old_dry
        self._stagerd.exit_event.clear()
        self._stagerd.fixture_dir = old_fixture_dir
        remove_files(get_directory_files_list(fixture_dir))
        os.removedirs(fixture_dir)

    def test_start(self):
        """StagerDaemon run.
        """
        fixture_dir = tempfile.mkdtemp()

        test_dir = os.path.join('geoutils', 'tests', 'files')
        test_files = ['i_3001a.ntf', 'i_6130e.ntf']
        for test_file in test_files:
            copy_file(os.path.join(test_dir, test_file),
                      os.path.join(fixture_dir, test_file))

        old_dry = self._stagerd.dry
        old_fixture_dir = self._stagerd.fixture_dir
        self._stagerd.fixture_dir = fixture_dir

        self._stagerd.dry = False
        self._stagerd._start(self._stagerd.exit_event)

        # Fixture files found.
        received = self._stagerd.files_to_process
        expected = [os.path.join(fixture_dir, x) for x in test_files]
        msg = 'Dry pass: fixture file list error'
        self.assertListEqual(sorted(received), sorted(expected), msg)

        # Generated file count.
        received = len(get_directory_files_list(self._stagerd.conf.inbound_dir))
        expected = 10
        msg = 'Generated file count error'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._stagerd.dry = old_dry
        self._stagerd.exit_event.clear()
        self._stagerd.fixture_dir = old_fixture_dir
        remove_files(get_directory_files_list(self._stagerd.conf.inbound_dir))
        remove_files(get_directory_files_list(fixture_dir))
        os.removedirs(fixture_dir)

    def test_source_files_no_directory(self):
        """Source NITF file: empty directory.
        """
        received = self._stagerd.source_files()
        expected = []
        msg = 'Source files: directory not defined should return []'
        self.assertListEqual(received, expected, msg)

    def test_source_files_dodge_directory(self):
        """Source NITF file: dodge directory.
        """
        received = self._stagerd.source_files()
        expected = []
        msg = 'Source files: dodge directory should return []'
        self.assertListEqual(received, expected, msg)

    def test_source_files_empty_directory(self):
        """Source NITF file: empty directory.
        """
        fixture_dir = tempfile.mkdtemp()

        old_fixture_dir = self._stagerd.fixture_dir
        self._stagerd.fixture_dir = fixture_dir

        received = self._stagerd.source_files()
        expected = []
        msg = 'Source files: empty directory should return []'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._stagerd.fixture_dir = old_fixture_dir
        os.removedirs(fixture_dir)

    def test_source_files(self):
        """inbound directory for NITF file: empty directory.
        """
        fixture_dir = tempfile.mkdtemp()

        test_dir = os.path.join('geoutils', 'tests', 'files')
        test_files = ['i_3001a.ntf', 'i_6130e.ntf']
        for test_file in test_files:
            copy_file(os.path.join(test_dir, test_file),
                      os.path.join(fixture_dir, test_file))

        old_fixture_dir = self._stagerd.fixture_dir
        self._stagerd.fixture_dir = fixture_dir

        received = self._stagerd.source_files()
        expected = [os.path.join(fixture_dir, x) for x in test_files]
        msg = 'Source files list error'
        self.assertListEqual(sorted(received), sorted(expected), msg)

        # Clean up.
        self._stagerd.fixture_dir = old_fixture_dir
        remove_files(get_directory_files_list(fixture_dir))
        os.removedirs(fixture_dir)

    def tearDown(self):
        self._stagerd = None
        del self._stagerd

    @classmethod
    def tearDownClass(cls):
        os.removedirs(cls._conf.inbound_dir)
        cls._conf = None
        del cls._conf
