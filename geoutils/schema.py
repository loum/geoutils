# Add error codes here so that pylint doesn't complain.
# pylint: disable=E1101,C0111
"""The :class:`geoutils.Schema` manages the data structure that can be
consumed by an Accumulo ingest.

"""
__all__ = ["Schema"]

import re

import geoutils.index
from geosutils.log import log
from geosutils.utils import get_reverse_timestamp


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

    def get_table(self, table_name):
        """Returns the tables schema defined by *table_name*
        """
        return self.data.get('tables').get(table_name)

    def build_meta(self, meta_table, meta, image_uri=None):
        """Build the Image Library metadata schema.

        As with all the ``geoutils.Schema.build*` methods, builds and
        persists the schema data structure within the object instance.

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

        # New work as part of GDT-239 to build the image with a
        # reference to a HDFS URI.
        token_set = self.build_document_map(data['cq'])
        self.build_metasearch(token_set)

        # New work as part of GDT-392 to build the spatial index.
        point = data.get('cq').get('center')
        image_date = data.get('cq').get('metadata=NITF_IDATIM')
        self.build_spatial_index('image_spatial_index',
                                 point,
                                 image_date)

        log.info('Ingest metadata structure build done')

    def build_image(self,
                    image_table,
                    image_extract_ref,
                    downsample=None,
                    image_type='MONO',
                    thumb=False):
        """Create a reference to an image extraction process that is
        associated with the image library's schema image/thumb component.

        Requires an active :attr:`dataset`.

        As with all the ``geoutils.Schema.build*` methods, builds and
        persists the schema data structure within the object instance.

        **Kwargs:**
            *downsample*: an integer value that represents the
            reduced horizontal image pixel count.  The vertical
            pixel count will be scaled automatically.

            *image_type*: general kind of image represented by the data.
            This is typically taken from the NITF IREP metadata field.
            Currently supported values are MONO (default) and RGB

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

        data['cq'] = {'x_coord_size': str(downsample[0]),
                      'y_coord_size': str(downsample[1]),
                      'irep': image_type}

        log.info('Ingest image structure build done')

    def build_document_map(self,
                           source_meta,
                           token='metadata=',
                           length=3):
        """Extract the original metadata components and prepare a
        unique collection of words larger than *length* characters.
        Typically, these are the keys within the schema that start with
        ``metadata=*``.  However, token can be overriden with *token*.

        As with all the ``geoutils.Schema.build*` methods, builds and
        persists the schema data structure within the object instance.

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

        As with all the ``geoutils.Schema.build*` methods, builds and
        persists the schema data structure within the object instance.

        **Args:**
            *token_set*: a Python ``set`` of words

        **Returns:**
            dictionary structure that represents an Accumulo
            family/value structure in the form::

                {'cq': {<token_01>: <row_id>,
                        <token_02>: <row_id>,
                        ...,},
                 'val': {'e': <shard_id>}}

        """
        log.info('Building ingest metasearch component ...')

        self.data['tables']['meta_search'] = {}

        # Override the row_id.
        self.data['tables']['meta_search']['row_id'] = self.shard_id

        data = self.data['tables']['meta_search']['cf'] = {}

        data['val'] = {'e': self.shard_id}

        data['cq'] = {}
        for token in token_set:
            data['cq'][token] = self.source_id

        log.info('Ingest metasearch structure build done')

    def build_spatial_index(self, index_table, point, source_date=None):
        """Build the spatial index schema for an Accumulo ingest.

        As with all the ``geoutils.Schema.build*` methods, builds and
        persists the schema data structure within the object instance.

        **Args:**
            *index_table*: the name of the spatial index table

            *point*: comma separated string representation of a
            latitude and longitude in the form ``'<latitude>,<longitude>'``.
            For example::

                ``'32.9831944444,85.0001388889'``

            *source_date*: the date to use for indexing purposes.
            Refer to the
            :meth:`geosutils.utils.get_reverse_timestamp`
            for a definition of the supported time formats.

        """
        log.info('Building ingest meta spatial index component ...')

        # Initialise the table construct if missing.
        if self.data['tables'].get(index_table) is None:
            self.data['tables'][index_table] = {}

        data = self.data['tables'][index_table]

        index = geoutils.index.Spatial()

        stripe_token = index.get_stripe_token(self.source_id)

        if point is None:
            log.error('Lat/long point not defined: spatial index skipped')
        else:
            (latitude, longitude) = point.split(',')
            log.debug('Generating geohash from lat/long: %s/%s' %
                      (str(latitude), str(longitude)))
            geohash = index.gen_geohash(float(latitude), float(longitude))

            timestamp = get_reverse_timestamp(source_date)

            # Override the row_id.
            row_id = '%s_%s_%s' % (stripe_token, geohash, timestamp)

            # Build the schema component.
            data['row_id'] = row_id
            data['cf'] = {}
            data['cf']['cq'] = {'file': self.source_id}

        log.info('Ingest meta spatial index structure build done')

    def build_gdelt_spatial_index(self,
                                  index_table,
                                  gdelt):
        """Build the GDELT spatial index schema for an Accumulo ingest.

        As with all the ``geoutils.Schema.build*` methods, builds and
        persists the schema data structure within the object instance.

        .. note::

            A valid geohash is required to generate the appropriate
            ingest ``row_id``.  Otherwise, the schema build is skipped.

        **Args:**
            *index_table*: the name of the GDELT spatial index table

            *gdelt*: the GDELT ingest schema dictionary structure

        """
        log.info('Building ingest GDELT spatial index component ...')

        # Initialise the table construct if missing.
        if self.data['tables'].get(index_table) is None:
            self.data['tables'][index_table] = {}

        data = self.data['tables'][index_table]

        index = geoutils.index.Spatial()

        stripe_token = index.get_stripe_token(self.source_id)

        log.debug('Generating geohash from lat/long: %s/%s' %
                  (str(gdelt.latitude), str(gdelt.longitude)))
        geohash = index.gen_geohash(gdelt.latitude, gdelt.longitude)

        if gdelt.date_added is not None:
            timestamp = get_reverse_timestamp(gdelt.date_added.ljust(14, '0'))

        # Override the row_id.
        if geohash is not None:
            self.source_id = ('%s_%s_%s_%s' % (stripe_token,
                                               geohash,
                                               timestamp,
                                               gdelt.event_id))

            data['cf'] = {
                'cq': {
                    'GlobalEventID': gdelt.event_id,
                    'Day': gdelt.event_day,
                    'Actor1Geo_Type': gdelt.type,
                    'Actor1Geo_Fullname': gdelt.fullname,
                    'Actor1Geo_CountryCode': gdelt.country_code,
                    'Actor1Geo_ADM1Code': gdelt.adm1_code,
                    'Actor1Geo_Lat': gdelt.latitude,
                    'Actor1Geo_Long': gdelt.longitude,
                    'Actor1Geo_FeatureID': gdelt.feature_id,
                    'DATEADDED': gdelt.date_added,
                    'SOURCEURL': gdelt.source_url
                }
            }
        else:
            log.error('')

        log.info('Ingest GDELT spatial index structure build done')
