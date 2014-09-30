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

    def test_metasearch_data_structure(self):
        """Build the metasearch ingest data structure.
        """
        row_id = 'i_3001a'
        shard_id = 's01'
        token_set = set(['geocentric',
                         'huachuca',
                         'i_3001a',
                         'image',
                         'jitc'])
        args = (row_id, shard_id, token_set)
        received = self._standard._build_metasearch_data_struct(*args)
        expected = {'cq': {
                       'geocentric': row_id,
                       'huachuca': row_id,
                       'i_3001a': row_id,
                       'image': row_id,
                       'jitc': row_id},
                    'val': {
                       'e': shard_id}}
        msg = 'Meta search structure result error'
        self.assertDictEqual(received, expected, msg)

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

    def test_build_document_map(self):
        """Build a document map.
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        meta_dict = DATA['tables']['meta_library']['cf']['cq']

        received = self._standard.build_document_map(meta_dict)
        expected = ['00000',
                    '1024x1024',
                    '19961217102630',
                    '19971217102630',
                    '5458',
                    'airfield',
                    'base',
                    'bf01',
                    'checks',
                    'data',
                    'fort',
                    'geocentric',
                    'huachuca',
                    'i_3001a',
                    'image',
                    'jitc',
                    'missing',
                    'mono',
                    'nitf02',
                    'uncompressed',
                    'unknown',
                    'with']
        msg = 'Metadata document map error'
        self.assertListEqual(sorted(list(received)), expected, msg)

    def test_build_document_map_10_or_more_characters(self):
        """Build a document map: 10 or more characters.
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        meta_dict = DATA['tables']['meta_library']['cf']['cq']

        received = self._standard.build_document_map(meta_dict, length=9)
        expected = ['19961217102630',
                    '19971217102630',
                    'geocentric',
                    'uncompressed']
        msg = 'Metadata document map error: 10 or more character length'
        self.assertListEqual(sorted(list(received)), expected, msg)

    def test_build_document_map_alternate_token(self):
        """Build a document map: alternate_token
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        meta_dict = DATA['tables']['meta_library']['cf']['cq']

        received = self._standard.build_document_map(meta_dict,
                                                     token='fil')
        expected = ['i_3001a']
        msg = 'Metadata document map error: alternate token'
        self.assertListEqual(sorted(list(received)), expected, msg)

    def test_build_document_map_empty_source(self):
        """Build a document map: empty source
        """
        meta_dict = {}

        received = self._standard.build_document_map(meta_dict)
        msg = 'Metadata document map error: empty source'
        self.assertFalse(len(received), msg)

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
