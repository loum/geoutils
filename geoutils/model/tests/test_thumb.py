# pylint: disable=R0904,C0103
""":class:`geoutils.model.Thumb` tests.

"""
import unittest2

import geoutils


class TestModelThumb(unittest2.TestCase):
    """:class:`geoutils.model.Thumb` test cases.
    """
    @classmethod
    def setUp(cls):
        cls._meta = geoutils.model.Thumb()

    def test_init(self):
        """Initialise a :class:`geoutils.model.Thumb` object.
        """
        msg = 'Object is not a geoutils.model.Thumb'
        self.assertIsInstance(self._meta, geoutils.model.Thumb, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._meta.name, 'thumb_library', msg)

    @classmethod
    def tearDown(cls):
        cls._meta = None
        del cls._meta
