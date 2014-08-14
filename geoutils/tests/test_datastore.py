# pylint: disable=R0904,C0103
""":class:`geoutils.Datastore` tests.

"""
import unittest2
import os
import tempfile
import hashlib

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
        cls._ds.meta_table_name = 'meta_test'
        cls._ds.image_table_name = 'image_test'
        cls._ds.thumb_table_name = 'thumb_test'

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
        received = self._ds.init_table('image_test')
        msg = 'Table initialisation (no connection) should return False'
        self.assertFalse(received, msg)

    def test_table_management_with_connection_state(self):
        """Initialise/delete image library table: with connection state.
        """
        self._ds.connect()
        received = self._ds.init_table('image_test')
        msg = 'Table initialisation should return True'
        self.assertTrue(received, msg)

        # Try again.  An existing table should fail.
        received = self._ds.init_table('image_test')
        msg = 'Table initialisation (existing table) should return False'
        self.assertFalse(received, msg)

        # Now delete.
        received = self._ds.delete_table('image_test')
        msg = 'Table deletion (existing table) should return True'
        self.assertTrue(received, msg)

        # Try delete again.  Missing table should fail.
        received = self._ds.delete_table('image_test')
        msg = 'Table deletion (missing table) should return False'
        self.assertFalse(received, msg)

    def test_ingest(self):
        """Attempt to ingest the metadata component into the datastore.
        """
        image_stream_file =  os.path.join('geoutils',
                                          'tests',
                                          'files',
                                          'image_stream.out')
        image_fh = open(image_stream_file, 'rb')

        from geoutils.tests.files.ingest_data_01 import DATA
        table = DATA['tables']
        table['image_test'] = {'cf': {'cv': {'image': image_fh.read}}}

        self._ds.connect()
        self._ds.init_table(self._ds.meta_table_name)
        self._ds.init_table(self._ds.image_table_name)
        self._ds.init_table(self._ds.thumb_table_name)

        self._ds.ingest(DATA)

        # Clean up.
        DATA['tables'].pop('image_test', None)
        image_fh.close()
        self._ds.delete_table(self._ds.meta_table_name)
        self._ds.delete_table(self._ds.image_table_name)
        self._ds.delete_table(self._ds.thumb_table_name)

    def test_query_metadata_no_data(self):
        """Query the metadata component from an empty datastore.
        """
        self._ds.connect()
        self._ds.init_table('image_test')

        received = self._ds.query_metadata(table='image_test',
                                           key='i_3001a')
        expected = 0
        msg = 'Scan across empty table should return 0 cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table('image_test')

    def test_query_metadata_with_data(self):
        """Attempt to query the metadata component from the datastore.
        """
        from geoutils.tests.files.ingest_data_01 import DATA

        self._ds.connect()
        self._ds.init_table('meta_test')

        self._ds.ingest(DATA)

        received = self._ds.query_metadata(table='meta_test',
                                           key='i_3001a',
                                           display=False)
        expected = 73
        msg = 'Scan across table should return cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table('meta_test')

    def test_reconstruct_image_300x300_PNG(self):
        """Reconstruct a 1D image stream to a 2D structure: 300x300 PNG.
        """
        image_stream_file =  os.path.join('geoutils',
                                          'tests',
                                          'files',
                                          '300x300.out')
        image_stream_fh = open(image_stream_file, 'rb')
        png_image = tempfile.NamedTemporaryFile('wb')
        dimensions = (300, 300)
        self._ds.reconstruct_image(image_stream_fh.read,
                                   dimensions).save(png_image, "PNG")

        expected_file = os.path.join('geoutils',
                                     'tests',
                                     'files',
                                     'image300x300.png')
        expected = hashlib.md5(open(expected_file).read()).hexdigest()
        received = hashlib.md5(open(png_image.name).read()).hexdigest()
        msg = 'Original 300x300 PNG differs from reconstructed version'
        self.assertEqual(received, expected, msg)

        # Clean up.
        image_stream_fh.close()

    def test_reconstruct_image_50x50_PNG(self):
        """Reconstruct a 1D image stream to a 2D structure: 50x50 PNG.
        """
        image_stream_file =  os.path.join('geoutils',
                                          'tests',
                                          'files',
                                          '50x50.out')
        image_stream_fh = open(image_stream_file, 'rb')
        png_image = tempfile.NamedTemporaryFile('wb')
        dimensions = (50, 50)
        self._ds.reconstruct_image(image_stream_fh.read,
                                   dimensions).save(png_image, "PNG")

        expected_file = os.path.join('geoutils',
                                     'tests',
                                     'files',
                                     'image50x50.png')
        expected = hashlib.md5(open(expected_file).read()).hexdigest()
        received = hashlib.md5(open(png_image.name).read()).hexdigest()
        msg = 'Original 50x50 PNG differs from reconstructed version'
        self.assertEqual(received, expected, msg)

        # Clean up.
        image_stream_fh.close()

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        cls._mock.stop()

    @classmethod
    def tearDown(cls):
        cls._ds = None
        del cls._ds
