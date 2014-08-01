# pylint: disable=R0904,C0103
""":class:`geoutils.NITF` tests.

"""
import unittest2
import os
from osgeo import gdal

import geoutils


class TestNITF(unittest2.TestCase):
    """:class:`geoutils.NITF` test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'tests',
                                 'files',
                                 '03-ALPSMB110862965-O1B1___B_rp.ntf')

    def test_init(self):
        """Initialise a geoutils.NITF object.
        """
        nitf = geoutils.NITF()
        msg = 'Object is not a geoutils.NITF'
        self.assertIsInstance(nitf, geoutils.NITF, msg)

    def test_open_no_file_given(self):
        """Attempt to open a GDALDataset stream -- no file.
        """
        nitf = geoutils.NITF()
        received = nitf.open()
        msg = 'Open against None should return None'
        self.assertIsNone(received, msg)
        received = None
        del nitf

    def test_open_file_given(self):
        """Attempt to open a GDALDataset stream -- valid NITF file.
        """
        nitf = geoutils.NITF(source_filename=self._file)
        nitf.open()
        received = nitf.dataset
        msg = 'Open against NITF file should set geoutils.NITFdataset attr'
        self.assertIsInstance(received, gdal.Dataset, msg)
        del nitf

    @classmethod
    def tearDownClass(cls):
        del cls._file
