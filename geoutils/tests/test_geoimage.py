# pylint: disable=R0904,C0103
""":class:`geoutils.Metadata` tests.

"""
import unittest2
import os

import geoutils


class TestGeoImage(unittest2.TestCase):
    """:class:`geoutils.GeoImage test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'tests',
                                 'files',
                                 'i_3001a.ntf')

    @classmethod
    def setUp(cls):
        cls._image = geoutils.GeoImage()

    def test_init(self):
        """Initialise a geoutils.GeoImage object.
        """
        msg = 'Object is not a geoutils.GeoImage'
        self.assertIsInstance(self._image, geoutils.GeoImage, msg)

    def test_extract_image_no_dataset(self):
        """Extract the image from the NITF file: no dataset.
        """
        self._image.filename = self._file
        received = self._image.extract_image(None)
        msg = 'Unsuccessful image extraction should return None'
        self.assertIsNone(received, msg)

    def test_extract_image(self):
        """Extract the image from the NITF file.
        """
        nitf = geoutils.NITF(source_filename=self._file)
        nitf.open()

        received = self._image.extract_image(dataset=nitf.dataset)
        msg = 'Successful image extraction should not return None'
        self.assertIsNotNone(received, msg)

        nitf = None
        del nitf

    def test_scale(self):
        """Scale calulations.
        """
        received = self._image.scale((2048, 1024), 300)
        expected = (300, 150)
        msg = 'Scale to 300 pixels error'
        self.assertTupleEqual(received, expected, msg)

    def test_scale_larger_scale(self):
        """Scale calulations: larger scale.
        """
        received = self._image.scale((1024, 1024), 2048)
        expected = (1024, 1024)
        msg = 'Scale to larger 2048 pixels error'
        self.assertTupleEqual(received, expected, msg)

    @classmethod
    def tearDown(cls):
        cls._image = None
        del cls._image

    @classmethod
    def tearDownClass(cls):
        del cls._file
