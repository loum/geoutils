# pylint: disable=R0904,C0103,W0142
""":class:`geoutils.model.Audit` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock
from geoutils.model.tests.files.audit_ingest_01 import (AUDIT_01,
                                                        AUDIT_02,
                                                        AUDIT_03,
                                                        AUDIT_04)



class TestModelAudit(unittest2.TestCase):
    """:class:`geoutils.model.Audit` test cases.
    """
    @classmethod
    def setUpClass(cls):
        """Attempt to start the Accumulo mock proxy server.
        """
        cls.maxDiff = None

        conf = os.path.join('geoutils',
                            'tests',
                            'files',
                            'proxy.properties')
        cls._mock = geolib_mock.MockServer(conf)
        cls._mock.start()

        cls._audit_table_name = 'audit'

    def setUp(self):
        self._ds = geoutils.Datastore()
        self._audit = geoutils.model.Audit(connection=self._ds.connect(),
                                           name=self._audit_table_name)


    def test_init(self):
        """Initialise a :class:`geoutils.model.Audit` object.
        """
        msg = 'Object is not a geoutils.model.Audit'
        self.assertIsInstance(self._audit, geoutils.model.Audit, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._audit.name, 'audit', msg)

    def test_query_auditer_no_data(self):
        """Query the auditer: no data.
        """
        self._ds.init_table(self._audit_table_name)

        received = self._audit.query_recent_audit(depth=2)
        expected = {'audits': []}
        msg = 'Audit scan across empty table should return no records'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._audit_table_name)

    def test_query_auditer_full_search(self):
        """Query the auditer: full search.
        """
        self._ds.init_table(self._audit_table_name)

        self._ds.ingest(AUDIT_01)
        self._ds.ingest(AUDIT_02)
        self._ds.ingest(AUDIT_03)
        self._ds.ingest(AUDIT_04)

        # Get all audits back.  Order is important.
        received = self._audit.query_recent_audit()
        expected = {'audits': ['09221956375225656441_dummy',
                               '09221956437718808093_ingest_daemon',
                               '09221956447616086912_ingest_daemon',
                               '09221956447764569211_ingest_daemon']}
        msg = 'Scan across auditer table should return sorted records'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._audit_table_name)

    def test_query_auditer_control_depth(self):
        """Query the auditer: control depth.
        """
        self._ds.init_table(self._audit_table_name)

        self._ds.ingest(AUDIT_01)
        self._ds.ingest(AUDIT_02)
        self._ds.ingest(AUDIT_03)
        self._ds.ingest(AUDIT_04)

        # Control depth of records returned.  Again, order is important.
        received = self._audit.query_recent_audit(depth=2)
        expected = {'audits': ['09221956375225656441_dummy',
                               '09221956437718808093_ingest_daemon']}
        msg = 'Regex audit scan should return filtered records'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._audit_table_name)

    def test_query_auditer_regex(self):
        """Query the auditer: regex.
        """
        self._ds.init_table(self._audit_table_name)

        self._ds.ingest(AUDIT_01)
        self._ds.ingest(AUDIT_02)
        self._ds.ingest(AUDIT_03)
        self._ds.ingest(AUDIT_04)

        # Control depth of records returned.  Again, order is important.
        received = self._audit.query_recent_audit(key='.*ingest_daemon')
        expected = {'audits': ['09221956437718808093_ingest_daemon',
                               '09221956447616086912_ingest_daemon',
                               '09221956447764569211_ingest_daemon']}
        msg = 'Depth audit scan should return two records'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._audit_table_name)

    def test_query_audit_no_data(self):
        """Query the audit component from an empty datastore.
        """
        self._ds.init_table(self._audit_table_name)

        kwargs = {'key': '09221956447764569211_ingest_daemon'}
        received = self._audit.query_audit(**kwargs)
        expected = {}
        msg = 'Scan across empty audit table should return 0 cells'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._audit_table_name)

    def test_query_audit_with_data(self):
        """Query the audit table with explicit row_id.
        """
        self._ds.init_table(self._audit_table_name)

        self._ds.ingest(AUDIT_01)
        self._ds.ingest(AUDIT_02)
        self._ds.ingest(AUDIT_03)
        self._ds.ingest(AUDIT_04)

        kwargs = {'key': '09221956447764569211_ingest_daemon'}
        received = self._audit.query_audit(**kwargs)
        expected = {'09221956447764569211_ingest_daemon': {
                       'ingest_daemon|finish': '1415589090.21',
                       'ingest_daemon|start': '1415589090.12'}}
        msg = 'Audit table search should return audit structure'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._audit_table_name)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

        del cls._audit_table_name

    def tearDown(self):
        self._audit = None
        del self._audit
        self._ds = None
        del self._ds
