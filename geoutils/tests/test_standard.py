# pylint: disable=R0904
""":class:`geoutils.Standard` tests.

"""
import unittest2

import geoutils


class TestStandard(unittest2.TestCase):
    """:class:`geoutils.Standard` test cases.
    """
    def test_init(self):
        """Initialise a geoutils.Standard object.
        """
        standard = geoutils.Standard()
        msg = 'Object is not a geoutils.Standard'
        self.assertIsInstance(standard, geoutils.Standard, msg)
