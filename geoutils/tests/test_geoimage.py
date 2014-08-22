# pylint: disable=R0904,C0103
""":class:`geoutils.Metadata` tests.

"""
import unittest2
import os
import tempfile
import hashlib

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
        received = self._image.extract_image(None)()
        msg = 'Unsuccessful image extraction should return None'
        self.assertIsNone(received, msg)

    def test_extract_image(self):
        """Extract the image from the NITF file.
        """
        nitf = geoutils.NITF(source_filename=self._file)
        nitf.open()

        handle = self._image.extract_image(dataset=nitf.dataset)
        received = handle()
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

    def test_reconstruct_image_300x300_PNG(self):
        """Reconstruct a 1D image stream to a 2D structure: 300x300 PNG.
        """
        image_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         '300x300_stream.out')
        image_stream_fh = open(image_stream_file, 'rb')
        png_image = tempfile.NamedTemporaryFile('wb')
        dimensions = (300, 300)
        self._image.reconstruct_image(image_stream_fh.read,
                                      dimensions).save(png_image, "PNG")

        expected_file = os.path.join('geoutils',
                                     'tests',
                                     'files',
                                     'image300x300.png')
        expected = hashlib.md5(open(expected_file).read()).hexdigest()
        received = hashlib.md5(open(png_image.name).read()).hexdigest()
        msg = 'Original 300x300 PNG differs from reconstructed version'
        self.assertEqual(received, expected, msg)

        # Clean up.
        image_stream_fh.close()

    def test_reconstruct_image_50x50_PNG(self):
        """Reconstruct a 1D image stream to a 2D structure: 50x50 PNG.
        """
        image_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         '50x50_stream.out')
        image_stream_fh = open(image_stream_file, 'rb')
        png_image = tempfile.NamedTemporaryFile('wb')
        dimensions = (50, 50)
        self._image.reconstruct_image(image_stream_fh.read,
                                      dimensions).save(png_image, "PNG")

        expected_file = os.path.join('geoutils',
                                     'tests',
                                     'files',
                                     'image50x50.png')
        expected = hashlib.md5(open(expected_file).read()).hexdigest()
        received = hashlib.md5(open(png_image.name).read()).hexdigest()
        msg = 'Original 50x50 PNG differs from reconstructed version'
        self.assertEqual(received, expected, msg)

        # Clean up.
        image_stream_fh.close()

    @classmethod
    def tearDown(cls):
        cls._image = None
        del cls._image

    @classmethod
    def tearDownClass(cls):
        del cls._file
