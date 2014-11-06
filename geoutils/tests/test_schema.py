# pylint: disable=R0904,C0103,W0142,W0212
""":class:`geoutils.Schema` tests.

"""
import unittest2
import os

import geoutils


class TestSchema(unittest2.TestCase):
    """:class:`geoutils.Schema` test cases.
    """
    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'tests',
                                 'files',
                                 'i_3001a.ntf')
        cls._meta = geoutils.Metadata()
        nitf = geoutils.NITF(source_filename=cls._file)
        nitf.open()
        cls._meta.extract_meta(dataset=nitf.dataset)

    def setUp(self):
        self._schema = geoutils.Schema()

    def test_init(self):
        """Initialise a geoutils.Schema object.
        """
        msg = 'Object is not a geoutils.Schema'
        self.assertIsInstance(self._schema, geoutils.Schema, msg)

    def test_callable(self):
        """Invoke the geoutils.Schema object instance.
        """
        self._schema.source_id = 'i_3001a'
        self._schema.shard_id = 's01'

        received = self._schema()
        expected = {'row_id': 'i_3001a',
                    'shard_id': 's01',
                    'tables': {}}
        msg = 'geoutils.Schema callable error'
        self.assertDictEqual(received, expected, msg)

    def test_build_meta_data_structure(self):
        """Build the metadata ingest data structure.
        """
        self._schema.build_meta('meta_library', self._meta)
        received = self._schema.data['tables']['meta_library']['cf']

        from geoutils.tests.files.ingest_data_01 import DATA
        expected = DATA['tables']['meta_library']['cf']
        msg = 'Metadata data structure result error'
        self.assertDictEqual(received, expected, msg)

    def test_build_meta_data_structure_missing_geogcs(self):
        """Build the metadata ingest data structure: missing GEOGCS.
        """
        file_no_geogcs = os.path.join('geoutils',
                                      'tests',
                                      'files',
                                      'i_6130e.ntf')
        nitf = geoutils.NITF(source_filename=file_no_geogcs)
        nitf.open()
        meta = geoutils.Metadata()
        meta.extract_meta(dataset=nitf.dataset)

        self._schema.build_meta('meta_library', meta)
        received = self._schema.data['tables']['meta_library']['cf']

        from geoutils.tests.files.ingest_data_02 import DATA
        expected = DATA['tables']['meta_library']['cf']
        msg = 'Metadata data structure result error'
        self.assertDictEqual(received, expected, msg)

    def test_build_document_map(self):
        """Build a document map.
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        meta_dict = DATA['tables']['meta_library']['cf']['cq']

        received = self._schema.build_document_map(meta_dict)
        expected = ['00000',
                    '1024x1024',
                    '19961217102630',
                    '19971217102630',
                    '5458',
                    'airfield',
                    'base',
                    'bf01',
                    'checks',
                    'data',
                    'fort',
                    'geocentric',
                    'huachuca',
                    'i_3001a',
                    'image',
                    'jitc',
                    'missing',
                    'mono',
                    'nitf02',
                    'uncompressed',
                    'unknown',
                    'with']
        msg = 'Metadata document map error'
        self.assertListEqual(sorted(list(received)), expected, msg)

    def test_build_document_map_10_or_more_characters(self):
        """Build a document map: 10 or more characters.
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        meta_dict = DATA['tables']['meta_library']['cf']['cq']

        received = self._schema.build_document_map(meta_dict, length=9)
        expected = ['19961217102630',
                    '19971217102630',
                    'geocentric',
                    'uncompressed']
        msg = 'Metadata document map error: 10 or more character length'
        self.assertListEqual(sorted(list(received)), expected, msg)

    def test_build_document_map_alternate_token(self):
        """Build a document map: alternate_token
        """
        from geoutils.tests.files.ingest_data_01 import DATA
        meta_dict = DATA['tables']['meta_library']['cf']['cq']

        received = self._schema.build_document_map(meta_dict,
                                                   token='fil')
        expected = ['i_3001a']
        msg = 'Metadata document map error: alternate token'
        self.assertListEqual(sorted(list(received)), expected, msg)

    def test_build_document_map_empty_source(self):
        """Build a document map: empty source
        """
        meta_dict = {}

        received = self._schema.build_document_map(meta_dict)
        msg = 'Metadata document map error: empty source'
        self.assertFalse(len(received), msg)

    def test_build_metasearch(self):
        """Build the metasearch ingest data structure.
        """
        row_id = 'i_3001a'
        shard_id = 's01'
        token_set = set(['geocentric',
                         'huachuca',
                         'i_3001a',
                         'image',
                         'jitc'])
        self._schema.source_id = row_id
        self._schema.shard_id = shard_id
        self._schema.build_metasearch(token_set)
        received = self._schema.data['tables']['meta_search']
        expected = {'row_id': 's01',
                    'cf': {
                        'cq': {
                            'geocentric': row_id,
                            'huachuca': row_id,
                            'i_3001a': row_id,
                            'image': row_id,
                            'jitc': row_id},
                        'val': {
                             'e': shard_id}}}
        msg = 'Meta search structure result error'
        self.assertDictEqual(received, expected, msg)

    def test_build_meta_spatial_index(self):
        """Build the meta spatial index ingest data structure.
        """
        self._schema.source_id = 'i_3001a'

        kwargs = {'index_table': 'image_spatial_index',
                  'point': '32.9831944444,85.0001388889',
                  'source_date': '19961217102630'}
        self._schema.build_spatial_index(**kwargs)
        received = self._schema.get_table('image_spatial_index')
        expected = {'row_id': '0000_tvu7whrjnc16_09222521218464775807',
                    'cf': {
                        'cq': {'file': 'i_3001a'}}}
        msg = 'Image spatial index structure result error'
        self.assertDictEqual(received, expected, msg)

    def test_build_meta_spatial_index_current_time(self):
        """Build the meta spatial index ingest data structure.
        """
        self._schema.source_id = 'i_3001a'

        kwargs = {'index_table': 'image_spatial_index',
                  'point': '32.9831944444,85.0001388889',
                  'source_date': None}
        self._schema.build_spatial_index(**kwargs)
        received = self._schema.get_table('image_spatial_index')
        received = received.get('row_id')
        expected = '0000_tvu7whrjnc16_'
        msg = 'Spatial index (current time) structure result error'
        self.assertRegexpMatches(received, expected, msg)

    def tearDown(self):
        self._schema = None
        del self._schema

    @classmethod
    def tearDownClass(cls):
        cls._meta = None
        del cls._meta
