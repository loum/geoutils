# pylint: disable=R0904,C0103
""":class:`geoutils.Datastore` tests.

"""
import unittest2
import os
import tempfile
import hashlib

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
        cls._image_table_name = 'image_library'
        cls._thumb_table_name = 'thumb_library'

    @classmethod
    def setUp(cls):
        cls._ds = geoutils.Datastore()

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

    def test_ingest(self):
        """Attempt to ingest the metadata component into the datastore.
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
                                   'cv': {'image': image_fh.read}}}
        table[thumb_tbl] = {'cf': {'cq': {'x_coord_size': '300',
                                          'y_coord_size': '300'},
                                   'cv': {'image': thumb_fh.read}}}

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)
        self._ds.init_table(self._image_table_name)
        self._ds.init_table(self._thumb_table_name)

        self._ds.ingest(DATA)

        # Clean up.
        DATA['tables'].pop(self._image_table_name, None)
        DATA['tables'].pop(self._thumb_table_name, None)
        image_fh.close()
        self._ds.delete_table(self._meta_table_name)
        self._ds.delete_table(self._image_table_name)
        self._ds.delete_table(self._thumb_table_name)

    def test_query_metadata_no_data(self):
        """Query the metadata component from an empty datastore.
        """
        self._ds.connect()
        self._ds.init_table(self._image_table_name)

        received = self._ds.query_metadata(table=self._image_table_name,
                                           key='i_3001a')
        expected = 0
        msg = 'Scan across empty table should return 0 cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._image_table_name)

    def test_query_metadata_with_data(self):
        """Attempt to query the metadata component from the datastore.
        """
        from geoutils.tests.files.ingest_data_01 import DATA

        self._ds.connect()
        self._ds.init_table(self._meta_table_name)

        self._ds.ingest(DATA)

        received = self._ds.query_metadata(table=self._meta_table_name,
                                           key='i_3001a',
                                           display=False)
        expected = 77
        msg = 'Scan across table should return cells'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_coords(self):
        """Scan the metadata datastore table.
        """
        self._ds.connect()
        self._ds.init_table(self._meta_table_name)

        from geoutils.tests.files.ingest_data_01 import DATA
        self._ds.ingest(DATA)

        received = self._ds.query_coords(table=self._meta_table_name,
                                         jsonify=True)
        results_file = os.path.join('geoutils',
                                    'tests',
                                    'results',
                                    'coords01.txt')
        results_fh = open(results_file)
        expected = results_fh.readline().rstrip()
        msg = 'Image coordinates scan should return results'
        self.assertEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_coords_missing_geogcs(self):
        """Scan the metadata datastore table: missing GEOGCS.
        """
        self._ds.connect()
        self._ds.init_table(self._meta_table_name)

        from geoutils.tests.files.ingest_data_02 import DATA
        self._ds.ingest(DATA)

        received = self._ds.query_coords(table=self._meta_table_name,
                                         jsonify=False)
        expected = []
        msg = 'Image coordinates scan should return no results: no GEOGCS'
        self.assertListEqual(received, expected, msg)

        # Clean up.
        self._ds.delete_table(self._meta_table_name)

    def test_query_image(self):
        """Attempt to query the image component from the datastore.
        """
        image_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         'image_stream.out')
        image_fh = open(image_stream_file, 'rb')

        data = {'row_id': 'i_3001a'}
        data['tables'] = {self._image_table_name: {
                          'cf': {
                              'cq': {
                                  'x_coord_size': '1024',
                                  'y_coord_size': '1024'},
                              'val': {
                                  'image': image_fh.read}}}}

        self._ds.connect()
        self._ds.init_table(self._image_table_name)

        self._ds.ingest(data)

        results = self._ds.query_image(table=self._image_table_name,
                                       key='i_3001a')
        received = None
        for cell in results:
            if cell.cf == 'image':
                received = hashlib.md5(cell.val).hexdigest()
                break

        image_fh.seek(0)
        expected = hashlib.md5(image_fh.read()).hexdigest()
        msg = 'Ingested image stream differs from query result'
        self.assertEqual(received, expected, msg)

        # Clean up.
        image_fh.close()
        self._ds.delete_table(self._image_table_name)

    def test_query_thumb(self):
        """Attempt to query the thumb component from the datastore.
        """
        thumb_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         '300x300_stream.out')
        thumb_fh = open(thumb_stream_file, 'rb')

        data = {'row_id': 'i_3001a'}
        data['tables'] = {self._thumb_table_name: {
                          'cf': {
                              'cq': {
                                  'x_coord_size': '300',
                                  'y_coord_size': '300'},
                              'val': {
                                  'thumb': thumb_fh.read}}}}

        self._ds.connect()
        self._ds.init_table(self._thumb_table_name)

        self._ds.ingest(data)

        results = self._ds.query_image(table=self._thumb_table_name,
                                       key='i_3001a')
        received = None
        for cell in results:
            if cell.cf == 'thumb':
                received = hashlib.md5(cell.val).hexdigest()
                break

        thumb_fh.seek(0)
        expected = hashlib.md5(thumb_fh.read()).hexdigest()
        msg = 'Ingested thumb stream differs from query result'
        self.assertEqual(received, expected, msg)

        # Clean up.
        thumb_fh.close()
        self._ds.delete_table(self._thumb_table_name)

    def test_reconstruct_image_300x300_PNG(self):
        """Reconstruct a 1D image stream to a 2D structure: 300x300 PNG.
        """
        image_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         '300x300_stream.out')
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
        image_stream_file = os.path.join('geoutils',
                                         'tests',
                                         'files',
                                         '50x50_stream.out')
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

        del cls._meta_table_name
        del cls._image_table_name
        del cls._thumb_table_name

    @classmethod
    def tearDown(cls):
        cls._ds = None
        del cls._ds
