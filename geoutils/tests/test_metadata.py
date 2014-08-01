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
                                 '03-ALPSMB110862965-O1B1___B_rp.ntf')

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
        expected = 4928
        msg = 'Extracted x-cord size error'
        self.assertEqual(received, expected, msg)

        # Y coord size.
        received = meta.y_coord_size
        expected = 16000
        msg = 'Extracted y-cord size error'
        self.assertEqual(received, expected, msg)

        # GEOGCS - Spatial Reference System
        received = meta.geogcs
        expected = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9108"]],AUTHORITY["EPSG","4326"]]'
        msg = 'Extracted geogcs error'
        self.assertEqual(received, expected, msg)

        # GeoTransform.
        received = meta.geoxform
        expected = [132.26734943904776,
                    2.9542430597838272e-05,
                    -6.198304060667497e-06,
                    34.511402854353108,
                    -5.6378684347034569e-06,
                    -2.2293059982916727e-05]
        msg = 'GeoTransform error'
        self.assertListEqual(received, expected, msg)

        del meta
        del nitf

    @classmethod
    def tearDownMethod(cls):
        del cls._file
