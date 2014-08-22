# pylint: disable=R0904,C0103
""":class:`geoutils.model.Image` tests.

"""
import unittest2
import os
import hashlib

import geoutils
import geolib_mock


class TestModelImage(unittest2.TestCase):
    """:class:`geoutils.model.Image` test cases.
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
        cls._image = geoutils.model.Image(cls._ds.connect())

    def test_init(self):
        """Initialise a :class:`geoutils.model.Image` object.
        """
        msg = 'Object is not a geoutils.model.Image'
        self.assertIsInstance(self._image, geoutils.model.Image, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._image.name, 'image_library', msg)

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

        self._ds.init_table(self._image_table_name)
        self._ds.ingest(data)

        expected_file = os.path.join('geoutils',
                                     'tests',
                                     'results',
                                     'i_3001a_1024x1024.jpg')
        expected = hashlib.md5(open(expected_file).read()).hexdigest()
        image_jpg_stream = self._image.query_image(key='i_3001a')
        received = hashlib.md5(image_jpg_stream.read()).hexdigest()
        msg = 'Ingested image stream differs from query result'
        self.assertEqual(received, expected, msg)

        # Clean up.
        image_fh.close()
        self._ds.delete_table(self._image_table_name)

    def test_query_image_png(self):
        """Attempt to query the image component from the datastore: PNG
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

        self._ds.init_table(self._image_table_name)
        self._ds.ingest(data)

        expected_file = os.path.join('geoutils',
                                     'tests',
                                     'results',
                                     'i_3001a_1024x1024.png')
        expected = hashlib.md5(open(expected_file).read()).hexdigest()
        image_jpg_stream = self._image.query_image(key='i_3001a',
                                                   img_format='PNG')
        received = hashlib.md5(image_jpg_stream.read()).hexdigest()
        msg = 'Ingested image stream differs from query result'
        self.assertEqual(received, expected, msg)

        # Clean up.
        image_fh.close()
        self._ds.delete_table(self._image_table_name)

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
        cls._image = None
        del cls._image
        cls._ds = None
        del cls._ds
