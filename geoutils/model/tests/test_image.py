# pylint: disable=R0904,C0103
""":class:`geoutils.model.Image` tests.

"""
import unittest2

import geoutils


class TestModelImage(unittest2.TestCase):
    """:class:`geoutils.model.Image` test cases.
    """
    @classmethod
    def setUp(cls):
        cls._meta = geoutils.model.Image()

    def test_init(self):
        """Initialise a :class:`geoutils.model.Image` object.
        """
        msg = 'Object is not a geoutils.model.Image'
        self.assertIsInstance(self._meta, geoutils.model.Image, msg)

    def test_name(self):
        """Chekc the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._meta.name, 'image_library', msg)

    @classmethod
    def tearDown(cls):
        cls._meta = None
        del cls._meta
