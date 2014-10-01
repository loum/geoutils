# Add error codes here so that pylint doesn't complain.
# pylint: disable=E1101,C0111
"""The :class:`geoutils.Schema` manages the data structure that can be
consumed by an Accumulo ingest.

"""
__all__ = ["Schema"]

import re

from geosutils.log import log


class Schema(object):
    """:class:`geoutils.Standard`

    """
    _source_id = None
    _shard_id = None
    _data = {}

    def __init__(self, source_id=None, shard_id=None):
        self.source_id = source_id
        self.shard_id = shard_id
        self.data['tables'] = {}

    def __call__(self):
        self.data['row_id'] = self.source_id
        self.data['shard_id'] = self.shard_id

        return self.data

    @property
    def source_id(self):
        return self._source_id

    @source_id.setter
    def source_id(self, value):
        self._source_id = value

    @property
    def shard_id(self):
        return self._shard_id

    @shard_id.setter
    def shard_id(self, value):
        self._shard_id = value

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

    def build_meta(self, meta_table, meta, image_uri=None):
        """Build the Image Library metadata schema.

        **Args:**
            *meta_table*: name of the Accumulo metadata table

            *meta*: :class:`geoutils.Metadata` object instance

        **Returns:**
            dictionary structure representing the metadata

        """
        log.info('Building ingest metadata component ...')

        self.data['tables'][meta_table] = {}
        data = self.data['tables'][meta_table]['cf'] = {}
        data['cq'] = {'file': meta.file,
                      'x_coord_size': str(meta.x_coord_size),
                      'y_coord_size': str(meta.y_coord_size),
                      'geogcs': meta.geogcs}

        count = 0
        for geoxform in meta.geoxform:
            data['cq']['geoxform=%d' % count] = repr(geoxform)
            count += 1

        count = 0
        raw_image_boundaries = meta.calculate_extents()
        image_boundaries = meta.reproject_coords(raw_image_boundaries)
        for image_boundary in sorted(image_boundaries):
            data['cq']['coord=%d' % count] = ('%s,%s' %
                                              (image_boundary[1],
                                               image_boundary[0]))
            count += 1

        raw_image_centroid = meta.calculate_centroid(lat_long=True)
        image_centroid = meta.reproject_coords([raw_image_centroid])
        if len(image_centroid):
            data['cq']['center'] = ('%s,%s' % (image_centroid[0][1],
                                            image_centroid[0][0]))

        for meta_key in meta.metadata:
            meta_value = meta.metadata[meta_key]
            data['cq']['metadata=%s' % meta_key] = meta_value

        if image_uri is not None:
            data['cq']['image'] = image_uri

        token_set = self.build_document_map(data['cq'])
        self.build_metasearch(token_set)

        log.info('Ingest metadata structure build done')

    def build_image(self,
                    image_table,
                    image_extract_ref,
                    downsample=None,
                    thumb=False):
        """Create a reference to an image extraction process that is
        associated with the image library's schema image/thumb component.

        Requires an active :attr:`dataset`.

        **Kwargs:**
            *downsample*: an integer value that represents the
            reduced horizontal image pixel count.  The vertical
            pixel count will be scaled automatically.

            *thumb*: boolean flag that distinuguishes processing
            as a reduced image thumb.  Importantly, this will set the
            column family value to ``thumb``

        **Returns:**
            dictionary structure that represents an Accumulo
            family/value structure in the form::

                {'val: {'<thumb|image>': <method>}

        """
        msg = 'Building ingest image component'
        if downsample is not None:
            msg = '%s: downsample %s columns' % (msg, downsample)
        log.info('%s ...' % msg)

        key = 'image'
        if thumb:
            key = 'thumb'

        self.data['tables'][image_table] = {}
        data = self.data['tables'][image_table]['cf'] = {}
        data['val'] = {key: image_extract_ref}

        data['cq'] = {'x_coord_size': downsample,
                      'y_coord_size': downsample}

        log.info('Ingest image structure build done')

    def build_document_map(self,
                           source_meta,
                           token='metadata=',
                           length=3):
        """Extract the original metadata components and prepare a
        unique collection of words larger than *length* characters.
        Typically, these are the keys within the schema that start with
        ``metadata=*``.  However, token can be overriden with *token*.

        **Args:**
            *source*: dictionary of schema metadata components

            *token*: the dictionary key delimiter that identifies
            the original metadata components (less the programmatically
            created components)

            *length*: ignores document map elements less than or equal
            to this number

        **Returns:**
            a Python ``set`` of unique words extracted from the
            metadata values

        """
        log.debug('Creating metadata document map ...')

        # Strip out all of the metdata specific keys.
        meta_keys = [k for k in source_meta.keys() if k.startswith(token)]
        meta = dict((re.sub('^%s' % token, '', k),
                    source_meta[k]) for k in meta_keys)


        # Strip out all short values and split on sequences of
        # alphanumeric characters.
        ok_content = {}
        for meta_key, value in meta.iteritems():
            if len(value) > length:
                ok_content[meta_key] = re.split('[^\w]+', value.lower())

        doc_set = None
        content = ok_content.values()
        doc_set = set([v for vals in content for v in vals if len(v) > length])
        log.debug('Metadata document map done')

        return doc_set

    def build_metasearch(self, token_set):
        """Creates a Document Partitioned Indexed structure for
        the metadata component defined by the *token_set* document map.

        **Args:**
            *token_set*: a Python ``set`` of words

        **Returns:**
            dictionary structure that represents an Accumulo
            family/value structure in the form::

                {'cq': {<token_01>: <row_id>,
                        <token_02>: <row_id>,
                        ...,},
                 'val': {'e': <row_id>}}

        """
        log.info('Building ingest metasearch component ...')

        self.data['tables']['meta_search'] = {}
        data = self.data['tables']['meta_search']['cf'] = {}
        data['val'] = {'e': self.shard_id}

        data['cq'] = {}
        for token in token_set:
            data['cq'][token] = self.source_id

        log.info('Ingest metasearch structure build done')
