# pylint: disable=R0904,C0103,W0212
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

    def test_build_meta_data_structure(self):
        """Build the metadata ingest data structure.
        """
        self._standard.filename = self._file
        self._standard.open()

        received = self._standard._build_meta_data_structure()

        from geoutils.tests.files.ingest_data_01 import DATA
        expected = DATA['tables']['meta_library']['cf']
        msg = 'Metadata data structure result error'
        self.assertDictEqual(received, expected, msg)

    def test_build_meta_data_structure_missing_geogcs(self):
        """Build the metadata ingest data structure: missing GEOGCS.
        """
        self._standard.filename = self._file_no_geogcs
        self._standard.open()

        received = self._standard._build_meta_data_structure()

        from geoutils.tests.files.ingest_data_02 import DATA
        expected = DATA['tables']['meta_library']['cf']
        msg = 'Metadata data structure result error'
        self.assertDictEqual(received, expected, msg)

    def test_image_data_structure(self):
        """Build the image ingest data structure.
        """
        self._standard.filename = self._file
        self._standard.open()

        image_data = self._standard._build_image_data_structure()
        received = image_data['val']['image']
        msg = 'Image data structure result error'
        self.assertTrue(callable(received), msg)

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

    def test_build_image_uri_no_hdfs_host(self):
        """Store and build image URI: no HDFS host.
        """
        source_dir = tempfile.mkdtemp()
        hdfs_dir = tempfile.mkdtemp()
        source_test_file = os.path.join('geoutils',
                                        'tests',
                                        'files',
                                        'i_3001a.ntf')
        test_file = os.path.join(source_dir,
                                 os.path.basename(source_test_file))
        copy_file(source_test_file, test_file)

        self._standard.filename = test_file
        received = self._standard._build_image_uri(target_path=hdfs_dir)
        expected = 'file://%s/%s' % (hdfs_dir, os.path.basename(test_file))
        msg = 'Image library image URI schema error: no HDFS host'
        self.assertEqual(received, expected, msg)

        # Clean up.
        remove_files(get_directory_files_list(hdfs_dir))
        os.removedirs(hdfs_dir)
        os.removedirs(source_dir)

    def test_build_image_uri_with_hdfs_host(self):
        """Store and build image URI: with HDFS host.
        """
        # Use this test to verify a write to a functional
        # HDFS filesystem.  To do so, set dry to False.  Otherwise the
        # file creation will be bypassed.
        dry = True
        test_file = os.path.join('geoutils',
                                  'tests',
                                  'files',
                                  'i_3001a.ntf')
        self._standard.image_model.hdfs_namenode = 'jp2044lm-hdfs-nn01'
        self._standard.image_model.hdfs_namenode_port = '50070'
        self._standard.image_model.hdfs_namenode_user = None

        self._standard.filename =  test_file
        received = self._standard._build_image_uri(target_path='tmp',
                                                   dry=dry)
        expected = 'hdfs://jp2044lm-hdfs-nn01/tmp/i_3001a.ntf'
        msg = 'Image library image URI schema error: valid HDFS host'
        self.assertEqual(received, expected, msg)

    def test_build_image_uri_with_dodgy_hdfs_host(self):
        """Store and build image URI: with dodgy HDFS host.
        """
        test_file = os.path.join('geoutils',
                                  'tests',
                                  'files',
                                  'i_3001a.ntf')
        self._standard.image_model.hdfs_namenode = 'dodge'
        self._standard.image_model.hdfs_namenode_port = '50070'
        self._standard.image_model.hdfs_namenode_user = None

        self._standard.filename =  test_file
        received = self._standard._build_image_uri(target_path='tmp')
        msg = 'Image library image URI schema error: dodgy HDFS host'
        self.assertIsNone(received, msg)

    @classmethod
    def tearDown(cls):
        cls._standard = None
        del cls._standard

    @classmethod
    def tearDownClass(cls):
        del cls._file
        del cls._file_no_geogcs
