# pylint: disable=R0904,C0103
""":class:`geoutils.Datastore` tests.

"""
import unittest2
import os

import geoutils
import geolib_mock


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
        cls._mock = geolib_mock.MockServer(conf)
        cls._mock.start()

        cls._meta_table_name = 'meta_library'
        cls._image_spatial_index_table_name = 'image_spatial_index'
        cls._metasearch_table_name = 'meta_search'
        cls._image_table_name = 'image_library'
        cls._thumb_table_name = 'thumb_library'

    def setUp(self):
        self._ds = geoutils.Datastore()
        self._ds.host = 'localhost'
        self._ds.port = 42425
        self._ds.user = 'root'

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
        received = self._ds.init_table(self._image_table_name)
        msg = 'Table initialisation (no connection) should return False'
        self.assertFalse(received, msg)

    def test_table_management_with_connection_state(self):
        """Initialise/delete image library table: with connection state.
        """
        self._ds.connect()
        received = self._ds.init_table(self._image_table_name)
        msg = 'Table initialisation should return True'
        self.assertTrue(received, msg)

        # Try again.  An existing table should fail.
        received = self._ds.init_table(self._image_table_name)
        msg = 'Table initialisation (existing table) should return False'
        self.assertFalse(received, msg)

        # Now delete.
        received = self._ds.delete_table(self._image_table_name)
        msg = 'Table deletion (existing table) should return True'
        self.assertTrue(received, msg)

        # Try delete again.  Missing table should fail.
        received = self._ds.delete_table(self._image_table_name)
        msg = 'Table deletion (missing table) should return False'
        self.assertFalse(received, msg)

    def test_create_write_no_connection(self):
        """Create an Accumulo writer object: no connection.
        """
        received = self._ds._create_writer(table='dodgy')
        msg = 'Accumulo writer object (no connection) not None'
        self.assertIsNone(received, msg)

    def test_ingest_no_connection(self):
        """Accumulo ingest: no connection.
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        received = self._ds.ingest(DATA)
        msg = 'Accumulo ingest (no connection) not False'
        self.assertFalse(received, msg)

    def test_ingest_no_row_id(self):
        """Ingest attempt with no datastore connection.
        """
        data = {}
        received = self._ds.ingest(data)
        msg = 'Ingest status with no row_id not False'
        self.assertFalse(received, msg)

    def test_ingest(self):
        """Ingest the metadata component into the datastore.
        """
        image_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         'image_stream.out')
        image_fh = open(image_stream_file, 'rb')

        thumb_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         '300x300_stream.out')
        thumb_fh = open(thumb_stream_file, 'rb')

        from geoutils.tests.files.ingest_data_01 import DATA
        image_tbl = self._image_table_name
        thumb_tbl = self._thumb_table_name
        table = DATA['tables']
        table[image_tbl] = {'cf': {'cq': {'x_coord_size': '1024',
                                          'y_coord_size': '1024'},
                                   'val': {'image': image_fh.read}}}
        table[thumb_tbl] = {'cf': {'cq': {'x_coord_size': '300',
                                          'y_coord_size': '300'},
                                   'val': {'image': thumb_fh.read}}}

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._image_table_name)
        self._ds.init_table(self._thumb_table_name)

        received = self._ds.ingest(DATA)
        msg = 'Ingest status with datastore connection not True'
        self.assertTrue(received, msg)

        # Clean up.
        DATA['tables'].pop(self._image_table_name, None)
        DATA['tables'].pop(self._thumb_table_name, None)
        image_fh.close()
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._image_table_name)
        self._ds.delete_table(self._thumb_table_name)

    def test_ingest_spatial_index(self):
        """Ingest the metadata component: spatial index.
        """
        from geoutils.tests.files.ingest_data_01 import DATA

        self._ds.connect()
        self._ds.init_table(self._image_spatial_index_table_name)

        received = self._ds.ingest(DATA)
        msg = 'Ingest status with datastore connection not True'
        self.assertTrue(received, msg)

        # Clean up.
        self._ds.delete_table(self._image_spatial_index_table_name)

    def test_ingest_with_metasearch(self):
        """Ingest the metadata component: with metasearch.
        """
        image_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         'image_stream.out')
        image_fh = open(image_stream_file, 'rb')

        thumb_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         '300x300_stream.out')
        thumb_fh = open(thumb_stream_file, 'rb')

        from geoutils.tests.files.ingest_data_02 import DATA
        image_tbl = self._image_table_name
        thumb_tbl = self._thumb_table_name
        table = DATA['tables']
        table[image_tbl] = {'cf': {'cq': {'x_coord_size': '1024',
                                          'y_coord_size': '1024'},
                                   'val': {'image': image_fh.read}}}
        table[thumb_tbl] = {'cf': {'cq': {'x_coord_size': '300',
                                          'y_coord_size': '300'},
                                   'val': {'image': thumb_fh.read}}}

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._metasearch_table_name)
        self._ds.init_table(self._image_table_name)
        self._ds.init_table(self._thumb_table_name)

        received = self._ds.ingest(DATA)
        msg = 'Ingest status with datastore connection not True'
        self.assertTrue(received, msg)

        # Clean up.
        DATA['tables'].pop(self._image_table_name, None)
        DATA['tables'].pop(self._thumb_table_name, None)
        image_fh.close()
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._metasearch_table_name)
        self._ds.delete_table(self._image_table_name)
        self._ds.delete_table(self._thumb_table_name)

    def test_ingest_val_column_not_callable(self):
        """Ingest: value column not a callable.
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        thumb_tbl = self._thumb_table_name
        table = DATA['tables']
        table[thumb_tbl] = {'cf': {'cq': {'x_coord_size': '300',
                                          'y_coord_size': '300'},
                                   'val': {'image': 'just a scalar'}}}

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._thumb_table_name)

        received = self._ds.ingest(DATA)
        msg = 'Ingest with value column as a scalar not True'
        self.assertTrue(received, msg)

        # Clean up.
        DATA['tables'].pop(self._thumb_table_name, None)
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._thumb_table_name)

    def test_ingest_from_file(self):
        """Attempt to ingest from NITF file.
        """
        ntf_file = os.path.join('geoutils',
                                'tests',
                                'files',
                                'i_3001a.ntf')

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._image_table_name)
        self._ds.init_table(self._thumb_table_name)

        standard = geoutils.Standard(source_filename=ntf_file)
        standard.open()
        self._ds.ingest(standard(dry=True))

        # If you want to ingest some sample data into the proxy server
        # and block (so that you can connect via the client) then
        # uncomment the following two lines.
        # import time
        # time.sleep(1000)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._image_table_name)
        self._ds.delete_table(self._thumb_table_name)

    def test_ingest_from_file_document_partioned_index(self):
        """Attempt to ingest from NITF file.
        """
        ntf_file = os.path.join('geoutils',
                                'tests',
                                'files',
                                'i_3001a.ntf')

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._metasearch_table_name)
        self._ds.init_table(self._image_table_name)
        self._ds.init_table(self._thumb_table_name)

        standard = geoutils.Standard(source_filename=ntf_file)
        standard.open()
        self._ds.ingest(standard(dry=True))

        # If you want to ingest some sample data into the proxy server
        # and block (so that you can connect via the client) then
        # uncomment the following two lines.
        # import time
        # time.sleep(1000)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._metasearch_table_name)
        self._ds.delete_table(self._image_table_name)
        self._ds.delete_table(self._thumb_table_name)

    def test_ingest_from_file_spatial_index(self):
        """Attempt to ingest from NITF file.
        """
        ntf_file = os.path.join('geoutils',
                                'tests',
                                'files',
                                'i_3001a.ntf')

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._image_spatial_index_table_name)
        self._ds.init_table(self._metasearch_table_name)
        self._ds.init_table(self._image_table_name)
        self._ds.init_table(self._thumb_table_name)

        standard = geoutils.Standard(source_filename=ntf_file)
        standard.open()
        self._ds.ingest(standard(dry=True))

        # If you want to ingest some sample data into the proxy server
        # and block (so that you can connect via the client) then
        # uncomment the following two lines.
        # import time
        # time.sleep(1000)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._image_spatial_index_table_name)
        self._ds.delete_table(self._metasearch_table_name)
        self._ds.delete_table(self._image_table_name)
        self._ds.delete_table(self._thumb_table_name)

    def test_delete_table_no_connection(self):
        """Delete an Accumulo table: no connection.
        """
        received = self._ds.delete_table(name='dodge')
        msg = 'Accumulo table deletion (no connection) not False'
        self.assertFalse(received, msg)

    def test_exists_table_no_connection(self):
        """Table exists check: no connection.
        """
        received = self._ds.exists_table(name='dodge')
        msg = 'Table exists check (no connection) is not False'
        self.assertFalse(received, msg)

    def test_exists_table_no_table(self):
        """Table exists check: no table.
        """
        self._ds.connect()

        received = self._ds.exists_table(name='dodge')
        msg = 'Table exists check (no table) is not False'
        self.assertFalse(received, msg)

    def test_exists_table(self):
        """Table exists check.
        """
        self._ds.connect()
        self._ds.init_table(self._meta_table_name)

        received = self._ds.exists_table(name=self._meta_table_name)
        msg = 'Table exists check is not True'
        self.assertTrue(received, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_index(self):
        """Index creating mutation.
        """
        pass

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

        del cls._meta_table_name
        del cls._image_spatial_index_table_name
        del cls._metasearch_table_name
        del cls._image_table_name
        del cls._thumb_table_name

    def tearDown(self):
        self._ds = None
        del self._ds
