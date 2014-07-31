""":class:`geoutils.Image` tests
"""
import unittest2

import geoutils


class TestImage(unittest2.TestCase):
    """:class:`geoutils.Image`
    """
    @classmethod
    def setUpClass(cls):
        cls._image = geoutils.Image()

    def test_init(self):
        """Initialise a :class:`geoutils.Image` object.
        """
        msg = 'Object is not an geoutils.Image'
        self.assertIsInstance(self._image, geoutils.Image, msg)

    @classmethod
    def tearDownClass(cls):
        del cls._image
