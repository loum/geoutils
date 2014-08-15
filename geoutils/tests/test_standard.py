# pylint: disable=R0904,C0103,W0212
""":class:`geoutils.Standard` tests.

"""
import unittest2
import os
from osgeo import gdal

import geoutils


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

    @classmethod
    def setUp(cls):
        cls._standard = geoutils.Standard()

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
        expected = DATA['tables']['meta_test']['cf']
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

    def test_callable(self):
        """Invoke the geoutils.Standard object instance.
        """
        # Note: this is not really a test but more as a visual
        # aid that allows you to dump the ingest data structure
        # (but we do check the generated Row ID ...)
        self._standard.filename = self._file
        self._standard.open()
        received = self._standard()
        expected = 'i_3001a'
        msg = 'Callable Row ID error'
        self.assertEqual(received.get('row_id'), expected, msg)
        #print(received)

    @classmethod
    def tearDown(cls):
        cls._standard = None
        del cls._standard

    @classmethod
    def tearDownClass(cls):
        del cls._file
