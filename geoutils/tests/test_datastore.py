# pylint: disable=R0904,C0103
""":class:`geoutils.Datastore` tests.

"""
import unittest2
import os

import geoutils


class TestDatastore(unittest2.TestCase):
    """:class:`geoutils.Datastore` test cases.
    """
    @classmethod
    def setUpClass(cls):
        """Attempt to start the Accumulo mock proxy server.
        """
        conf = os.path.join('geoutils',
                            'tests',
                            'files',
                            'proxy.properties')
        cls._mock = geoutils.MockServer(conf)
        cls._mock.start()

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()
        cls._ds.image_table_name = 'image_library_test'

    def test_init(self):
        """Initialise a :class:`geoutils.Datastore` object.
        """
        msg = 'Object is not a geoutils.Datastore'
        self.assertIsInstance(self._ds, geoutils.Datastore, msg)

    def test_connect(self):
        """Attempt a connection to an Accumulo datastore.
        """
        self._ds.connect()
        received = self._ds.connection
        msg = 'geoutils.Datastore.connection attribute should be set'
        self.assertIsNotNone(received, msg)

    def test_connect_bad_credentials(self):
        """Attempt a connection to an Accumulo datastore: bad creds.
        """
        self._ds.password = 'banana'
        self._ds.connect()
        received = self._ds.connection
        msg = 'geoutils.Datastore.connection attribute SHOULD NOT be set'
        self.assertIsNone(received, msg)

    def test_init_table_no_connection_state(self):
        """Initialise the image library table: no connection state.
        """
        received = self._ds.init_table()
        msg = 'Table initialisation (no connection) should return False'
        self.assertFalse(received, msg)

    def test_table_management_with_connection_state(self):
        """Initialise/delete the image library table: with connection state.
        """
        self._ds.connect()
        received = self._ds.init_table()
        msg = 'Table initialisation should return True'
        self.assertTrue(received, msg)

        # Try again.  An existing table should fail.
        received = self._ds.init_table()
        msg = 'Table initialisation (existing table) should return False'
        self.assertFalse(received, msg)

        # Now delete.
        received = self._ds.delete_table()
        msg = 'Table deletion (existing table) should return True'
        self.assertTrue(received, msg)

        # Try delete again.  Missing table should fail.
        received = self._ds.delete_table()
        msg = 'Table deletion (missing table) should return False'
        self.assertFalse(received, msg)

    def test_ingest(self):
        """Attempt to ingest the metadata component into the datastore.
        """
        from geoutils.tests.files.i_3001a_dict import i_3001a
        image_stream_file =  os.path.join('geoutils',
                                          'tests',
                                          'files',
                                          'image_stream.out')
        image_stream_fh = open(image_stream_file, 'rb')

        self._ds.connect()
        self._ds.init_table()

        self._ds.ingest(i_3001a, image_stream_fh.read)

        # Clean up.
        image_stream_fh.close()
        self._ds.delete_table()

    def test_query_metadata_no_data(self):
        """Query the metadata component from an empty datastore.
        """
        self._ds.connect()
        self._ds.init_table()

        received = self._ds.query_metadata(key='i_3001a')
        expected = 0
        msg = 'Scan across empty table should return 0 cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table()

    def test_query_metadata_with_data(self):
        """Attempt to query the metadata component from the datastore.
        """
        from geoutils.tests.files.i_3001a_dict import i_3001a

        self._ds.connect()
        self._ds.init_table()

        self._ds.ingest(i_3001a)

        received = self._ds.query_metadata(key='i_3001a', display=False)
        expected = 73
        msg = 'Scan across table should return cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table()

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

    @classmethod
    def tearDown(cls):
        cls._ds = None
        del cls._ds
