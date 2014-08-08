# pylint: disable=R0904,C0103
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
        """Attempt to open a GDALDataset stream -- valid NITF file.
        """
        self._standard.filename = self._file
        self._standard.open()
        received = self._standard.dataset
        msg = 'NITF open should set geoutils.Standard.dataset attribute'
        self.assertIsInstance(received, gdal.Dataset, msg)

    @classmethod
    def tearDown(cls):
        cls._standard = None
        del cls._standard

    @classmethod
    def tearDownClass(cls):
        del cls._file
