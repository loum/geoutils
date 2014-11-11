# pylint: disable=R0904,C0103
""":class:`geoutils.Auditer` tests.

"""
import unittest2

import geoutils
from geoutils.auditer import audit


class TestAuditer(unittest2.TestCase):
    """:class:`geoutils.Auditer` test case.
    """
    @classmethod
    def setUp(cls):
        cls._auditer = geoutils.Auditer()

    def test_init(self):
        """Initialise a :class:`geoutils.Datastore` object.
        """
        msg = 'Object is not a geoutils.Datastore'
        self.assertIsInstance(self._auditer, geoutils.Auditer, msg)

    def test_shared_state_via_object(self):
        """Check the shared data state: via object.
        """
        obj_1 = geoutils.Auditer()
        obj_2 = geoutils.Auditer()

        obj_1.data = {'name_1': 'value_1'}
        obj_2.data = {'name_2': 'value_2'}

        received = obj_1.data
        expected = {'tables': {
                        'audit': {
                            'cf': {
                                'cq': {'name_1': 'value_1',
                                       'name_2': 'value_2'}}}}}
        msg = 'Shared data (object) state error'
        self.assertDictEqual(received, expected, msg)

        # ... and reset.
        obj_1.reset()

        received = obj_1()
        expected = {}
        msg = 'Shared data (object) state error: after reset'
        self.assertDictEqual(received, expected, msg)

    def test_shared_state_via_global(self):
        """Check the shared data state: via global.
        """
        audit.data = {'name_1': 'value_1'}
        audit.data = {'name_2': 'value_2'}

        received = audit()
        expected = {'tables': {
                        'audit': {
                            'cf': {
                                'cq': {'name_1': 'value_1',
                                       'name_2': 'value_2'}}}}}
        msg = 'Shared data (global) state error'
        self.assertDictEqual(received, expected, msg)

        # ... and reset.
        audit.reset()

        received = audit()
        expected = {}
        msg = 'Shared data (global) state error: after reset'
        self.assertDictEqual(received, expected, msg)

    @classmethod
    def tearDown(cls):
        cls._auditer = None
        del cls._auditer
