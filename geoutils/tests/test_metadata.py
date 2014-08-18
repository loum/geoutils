# pylint: disable=R0904,C0103
""":class:`geoutils.Metadata` tests.

"""
import unittest2
import os
from osgeo import gdal

import geoutils


class TestMetadata(unittest2.TestCase):
    """:class:`geoutils.Metadata` test cases.
    """
    @classmethod
    def setUpClass(cls):
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
        cls._meta = geoutils.Metadata()

    def test_init(self):
        """Initialise a geoutils.Metadata object.
        """
        msg = 'Object is not a geoutils.Metadata'
        self.assertIsInstance(self._meta, geoutils.Metadata, msg)

    def test_extract_meta(self):
        """Verify the x_coord_size attribute.
        """
        nitf = geoutils.NITF(source_filename=self._file)
        nitf.open()

        received = self._meta.extract_meta(dataset=nitf.dataset)
        msg = 'Successful metadata extraction should return True'
        self.assertTrue(received, msg)

        # Driver.
        received = self._meta.driver
        msg = 'Driver error'
        self.assertIsInstance(received, gdal.Driver, msg)

        # File.
        received = self._meta.file
        expected = os.path.basename(self._file)
        msg = 'Source file name error'
        self.assertEqual(received, expected, msg)

        # X coord size.
        received = self._meta.x_coord_size
        expected = 1024
        msg = 'Extracted x-cord size error'
        self.assertEqual(received, expected, msg)

        # Y coord size.
        received = self._meta.y_coord_size
        expected = 1024
        msg = 'Extracted y-cord size error'
        self.assertEqual(received, expected, msg)

        # GEOGCS - Spatial Reference System
        received = self._meta.geogcs
        results_file = os.path.join('geoutils',
                                    'tests',
                                    'results',
                                    'geogcs.txt')
        results_fh = open(results_file)
        expected = results_fh.readline().rstrip()
        msg = 'Extracted geogcs error'
        self.assertEqual(received, expected, msg)

        # GeoTransform.
        received = self._meta.geoxform
        expected = [84.999999864233729,
                    2.7153252959988272e-07,
                    0.0,
                    32.983333469099598,
                    0.0,
                    -2.7153252959294383e-07]
        msg = 'GeoTransform error'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        nitf = None
        del nitf

    def test_calculate_extents(self):
        """Verify the x_coord_size attribute.
        """
        nitf = geoutils.NITF(source_filename=self._file)
        nitf.open()
        nitf.meta.extract_meta(nitf.dataset)
        received = nitf.meta.calculate_extents()
        expected = [[84.999999864233729, 32.983333469099598],
                    [84.999999864233729, 32.983055419789295],
                    [85.000277913544039, 32.983055419789295],
                    [85.000277913544039, 32.983333469099598]]
        msg = 'Extent calculation error'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        nitf = None
        del nitf

    def test_calculate_extents_missing_geogcs(self):
        """Verify the x_coord_size attribute: missing GEOGCS.
        """
        nitf = geoutils.NITF(source_filename=self._file_no_geogcs)
        nitf.open()
        nitf.meta.extract_meta(nitf.dataset)
        received = nitf.meta.calculate_extents()
        expected = [[0.0, 0.0],
                    [0.0, 1280.0],
                    [1152.0, 1280.0],
                    [1152.0, 0.0]]
        msg = 'Extent calculation error: missing GEOGCS'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        nitf = None
        del nitf

    def test_reproject_coords(self):
        """Reproject a set of X-Y coordinates.
        """
        xy_coords = [[84.999999864233729, 32.983333469099598],
                     [84.999999864233729, 32.983055419789295],
                     [85.000277913544039, 32.983055419789295],
                     [85.000277913544039, 32.983333469099598]]

        from geoutils.tests.files.ingest_data_01 import DATA
        geogcs = DATA['tables']['meta_test']['cf']['cq']['geogcs']
        self._meta.geogcs = geogcs

        received = self._meta.reproject_coords(extents=xy_coords)
        expected = [[84.999999864233729, 32.983333469099598],
                    [84.999999864233729, 32.983055419789295],
                    [85.000277913544039, 32.983055419789295],
                    [85.000277913544039, 32.983333469099598]]
        msg = 'X-Y coord re-projection error'
        self.assertListEqual(received, expected, msg)

    def test_reproject_coords_missing_geogcs(self):
        """Reproject a set of X-Y coordinates: missing GEOGCS.
        """
        xy_coords = [[0.0, 0.0],
                     [0.0, 1280.0],
                     [1152.0, 1280.0],
                     [1152.0, 0.0]]

        received = self._meta.reproject_coords(extents=xy_coords)
        expected = []
        msg = 'X-Y coord re-projection error: missing GEOGCS'
        self.assertListEqual(received, expected, msg)

    @classmethod
    def tearDown(cls):
        cls._meta = None
        del cls._meta

    @classmethod
    def tearDownClass(cls):
        del cls._file
