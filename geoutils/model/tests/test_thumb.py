# pylint: disable=R0904,C0103
""":class:`geoutils.model.Thumb` tests.

"""
import unittest2
import os
import hashlib

import geoutils
import geolib_mock


class TestModelThumb(unittest2.TestCase):
    """:class:`geoutils.model.Thumb` test cases.
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
        cls._thumb = geoutils.model.Thumb(cls._ds.connect())

    def test_init(self):
        """Initialise a :class:`geoutils.model.Thumb` object.
        """
        msg = 'Object is not a geoutils.model.Thumb'
        self.assertIsInstance(self._thumb, geoutils.model.Thumb, msg)

    def test_name(self):
        """Check the default table name.
        """
        msg = 'Default table name error'
        self.assertEqual(self._thumb.name, 'thumb_library', msg)

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

        self._ds.init_table(self._thumb_table_name)
        self._ds.ingest(data)

        expected_file = os.path.join('geoutils',
                                     'tests',
                                     'results',
                                     'i_3001a_300x300.jpg')
        expected = hashlib.md5(open(expected_file).read()).hexdigest()
        image_jpg_stream = self._thumb.query_thumb(key='i_3001a')
        received = hashlib.md5(image_jpg_stream.read()).hexdigest()
        msg = 'Ingested thumb stream differs from query result'
        self.assertEqual(received, expected, msg)

        # Clean up.
        thumb_fh.close()
        self._ds.delete_table(self._thumb_table_name)

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
        cls._thumb = None
        del cls._thumb
        cls._ds = None
        del cls._ds
