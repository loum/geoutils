# pylint: disable=R0904,C0103
""":class:`geoutils.model.Metadata` tests.

"""
import unittest2

import geoutils


class TestModelMetadata(unittest2.TestCase):
    """:class:`geoutils.model.Metadata` test cases.
    """
    @classmethod
    def setUp(cls):
        cls._meta = geoutils.model.Metadata()

    def test_init(self):
        """Initialise a :class:`geoutils.model.Metadata` object.
        """
        msg = 'Object is not a geoutils.model.Metadata'
        self.assertIsInstance(self._meta, geoutils.model.Metadata, msg)

    def test_name(self):
        """Chekc the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._meta.name, 'meta_library', msg)

    @classmethod
    def tearDown(cls):
        cls._meta = None
        del cls._meta
