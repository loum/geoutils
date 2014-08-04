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

    def test_init(self):
        """Initialise a :class:`geoutils.Metadata` object.
        """
        nitf = geoutils.Metadata()
        msg = 'Object is not a geoutils.Metadata'
        self.assertIsInstance(nitf, geoutils.Metadata, msg)

    def test_extract_meta(self):
        """Verify the x_coord_size attribute.
        """
        nitf = geoutils.NITF(source_filename=self._file)
        nitf.open()

        meta = geoutils.Metadata()
        meta.dataset = nitf.dataset
        meta.extract_meta()

        # Driver.
        received = meta.driver
        msg = 'Driver error'
        self.assertIsInstance(received, gdal.Driver, msg)

        # Files.
        received = meta.files
        expected = [self._file]
        msg = 'Source file list error'
        self.assertListEqual(received, expected, msg)

        # X coord size.
        received = meta.x_coord_size
        expected = 1024
        msg = 'Extracted x-cord size error'
        self.assertEqual(received, expected, msg)

        # Y coord size.
        received = meta.y_coord_size
        expected = 1024
        msg = 'Extracted y-cord size error'
        self.assertEqual(received, expected, msg)

        # GEOGCS - Spatial Reference System
        received = meta.geogcs
        results_file = os.path.join('geoutils',
                                    'tests',
                                    'results',
                                    'geogcs.txt')
        results_fh = open(results_file)
        expected = results_fh.readline().rstrip()
        msg = 'Extracted geogcs error'
        self.assertEqual(received, expected, msg)

        # GeoTransform.
        received = meta.geoxform
        expected = [84.999999864233729,
                    2.7153252959988272e-07,
                    0.0,
                    32.983333469099598,
                    0.0,
                    -2.7153252959294383e-07]
        msg = 'GeoTransform error'
        self.assertListEqual(received, expected, msg)

        del meta
        del nitf

    @classmethod
    def tearDownMethod(cls):
        del cls._file
