""":class:`geoutils.ModelBase` tests.

"""
import unittest2

import geoutils


class TestModelBase(unittest2.TestCase):
    """:class:`geoutils.ModelBase` test cases.
    """
    @classmethod
    def setUp(cls):
        cls._base = geoutils.ModelBase()

    def test_init(self):
        """Initialise a :class:`geoutils.ModelBase` object.
        """
        msg = 'Object is not a geoutils.ModelBase'
        self.assertIsInstance(self._base, geoutils.ModelBase, msg)

    @classmethod
    def tearDown(cls):
        cls._base = None
        del cls._base
