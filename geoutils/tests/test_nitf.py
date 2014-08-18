# pylint: disable=R0904,C0103
""":class:`geoutils.NITF` tests.

"""
import unittest2
import os

import geoutils


class TestNITF(unittest2.TestCase):
    """:class:`geoutils.NITF` test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'tests',
                                 'files',
                                 'i_3001a.ntf')

    @classmethod
    def setUp(cls):
        cls._nitf = geoutils.NITF()

    def test_init(self):
        """Initialise a geoutils.NITF object.
        """
        msg = 'Object is not a geoutils.NITF'
        self.assertIsInstance(self._nitf, geoutils.NITF, msg)

    def test_extract_nitf_meta(self):
        """Extract NITF metadata.
        """
        self._nitf.filename = self._file
        self._nitf.open()
        self._nitf.meta.extract_meta(self._nitf.dataset)

    @classmethod
    def tearDown(cls):
        cls._nitf = None
        del cls._nitf

    @classmethod
    def tearDownClass(cls):
        del cls._file
