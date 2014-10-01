# pylint: disable=R0904,C0103,W0142,W0212
""":class:`geoutils.Standard` tests.

"""
import unittest2
import os
import tempfile
from osgeo import gdal

import geoutils
from geosutils.files import (get_directory_files_list,
                             remove_files,
                             copy_file)


class TestStandard(unittest2.TestCase):
    """:class:`geoutils.Standard` test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None
        cls._file = os.path.join('geoutils',
                                 'tests',
                                 'files',
                                 'i_3001a.ntf')
        cls._file_no_geogcs = os.path.join('geoutils',
                                           'tests',
                                           'files',
                                           'i_6130e.ntf')

    @classmethod
    def setUp(cls):
        cls._standard = geoutils.Standard()
        cls._standard.image_model.hdfs_namenode = None
        cls._standard.image_model.hdfs_namenode_port = 50070
        cls._standard.image_model.hdfs_namenode_user = None

    def test_init(self):
        """Initialise a geoutils.Standard object.
        """
        msg = 'Object is not a geoutils.Standard'
        self.assertIsInstance(self._standard, geoutils.Standard, msg)

    def test_open_no_file_given(self):
        """Attempt to open a GDALDataset stream -- no file.
        """
        received = self._standard.open()
        msg = 'Open against None should return None'
        self.assertIsNone(received, msg)
        received = None

    def test_open_file_given(self):
        """Attempt to open a GDALDataset stream: valid NITF file.
        """
        self._standard.filename = self._file
        self._standard.open()
        received = self._standard.dataset
        msg = 'NITF open should set geoutils.Standard.dataset attribute'
        self.assertIsInstance(received, gdal.Dataset, msg)

        self._standard.close()
        msg = 'Dataset stream object after close is not None'
        self.assertIsNone(self._standard.dataset, msg)

    def test_callable_image_file_uri(self):
        """Invoke the geoutils.Standard object instance: image file URI.
        """
        # Note: this is not really a test but more as a visual
        # aid that allows you to dump the ingest data structure
        # (but we do check the generated Row ID ...)
        target_dir = tempfile.mkdtemp()
        source_dir = tempfile.mkdtemp()
        source_file = os.path.join(source_dir, os.path.basename(self._file))
        copy_file(self._file, source_file)

        self._standard.filename = source_file
        self._standard.open()
        received = self._standard(target_path=target_dir)
        expected = 'i_3001a'
        msg = 'Callable Row ID error'
        self.assertEqual(received.get('row_id'), expected, msg)
        #print(received)

        # Clean up.
        remove_files(get_directory_files_list(target_dir))
        os.removedirs(target_dir)
        os.removedirs(source_dir)

    def test_callable_image_file_uri_proc_file(self):
        """Invoke the geoutils.Standard object instance: proc file.
        """
        target_dir = tempfile.mkdtemp()
        source_dir = tempfile.mkdtemp()
        source_file = os.path.join(source_dir,
                                   os.path.basename(self._file + '.proc'))
        copy_file(self._file, source_file)

        self._standard.filename = source_file
        self._standard.open()
        received = self._standard(target_path=target_dir)
        expected = 'i_3001a'
        msg = 'Callable (proc file) Row ID error'
        self.assertEqual(received.get('row_id'), expected, msg)

        # Clean up.
        remove_files(get_directory_files_list(target_dir))
        os.removedirs(target_dir)
        os.removedirs(source_dir)

    def test_get_shard(self):
        """Generate a shard.
        """
        source = 'i_3001a'
        received = self._standard.get_shard(source)
        expected = 's01'
        msg = 'Generated shard (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

        source = 'i_6130e'
        received = self._standard.get_shard(source)
        expected = 's03'
        msg = 'Generated shard (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

    def test_get_stripe_token(self):
        """Generate a stripe_token.
        """
        # Set a new stripe value.
        old_spatial_stripes = self._standard.spatial_stripes
        self._standard.spatial_stripes = 100

        source = 'i_3001a'
        received = self._standard.get_stripe_token(source)
        expected = '0021'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

        source = 'i_6130e'
        received = self._standard.get_stripe_token(source)
        expected = '0031'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

        # Restore the default.
        self._standard.spatial_stripes = old_spatial_stripes

        source = 'i_3001a'
        received = self._standard.get_stripe_token(source)
        expected = '0000'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

        source = 'i_6130e'
        received = self._standard.get_stripe_token(source)
        expected = '0000'
        msg = 'Generated stripe_token (%s) incorrect' % source
        self.assertEqual(expected, received, msg)

    def test_get_reverse_timestamp(self):
        """Generate reverse timestamp.
        """
        received = self._standard.get_reverse_timestamp('19961217102630')
        expected = '09222521218464775807'
        msg = 'Reverse timestamp error'
        self.assertEqual(received, expected, msg)

        newer = self._standard.get_reverse_timestamp('19971217102630')
        msg = 'Reverse timestamp of newer date should be less'
        self.assertLess(int(newer), int(received), msg)

    def test_get_reverse_timestamp_invalid_source_utc_length(self):
        """Generate reverse timestamp: invalid source UTC length.
        """
        received = self._standard.get_reverse_timestamp('199612171026')
        msg = 'Reverse timestamp error: invalid UTC length'
        self.assertIsNone(received, msg)

    def test_get_reverse_timestamp_invalid_source_unknown_delimeter(self):
        """Generate reverse timestamp: invalid source (unknown delimiter).
        """
        received = self._standard.get_reverse_timestamp('1996121710--')
        msg = 'Reverse timestamp error: invalid source (unknown delimiter)'
        self.assertIsNone(received, msg)

    @classmethod
    def tearDown(cls):
        cls._standard = None
        del cls._standard

    @classmethod
    def tearDownClass(cls):
        del cls._file
        del cls._file_no_geogcs
