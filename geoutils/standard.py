# Add error codes here so that pylint doesn't complain.
# pylint: disable=E1101,C0111
"""The :class:`geoutils.Standard` is the base class for all standards that
relate to the exchange, storage and transmission of digital-imagery
products and image-related products.

"""
__all__ = ["Standard"]

import os
import re
import sys
from osgeo import gdal
import time
import calendar

import geoutils
import geoutils.model
from geosutils.log import log
from geosutils.utils import hashcode


class Standard(object):
    """:class:`geoutils.Standard`

    .. attribute: filename

    """
    _filename = None
    _dataset = None
    _meta = geoutils.Metadata()
    _image = geoutils.GeoImage()
    _meta_model = geoutils.model.Metadata(None)
    _image_model = geoutils.model.Image(None)
    _thumb_model = geoutils.model.Thumb(None)
    _meta_shards = 4
    _spatial_stripes = 1

    def __init__(self, source_filename=None):
        self._filename = source_filename

    def __call__(self, target_path=None, dry=False):
        """The object instance callable is a quick handle to the
        meta/image extaction process.  It will also construct a dictionary
        like construct that can be fed directly into a
        :class:`geoutils.Datastore` instance.

        **Kwargs:**
            *target_path*: destination HDFS path where the original
            :attr:`filename` will be written to

            *dry*: if ``True`` only simulate, do not execute

        **Returns:**
            a dictionary structure that can be fed into a
        :class:`geoutils.Datastore` ingest

        .. todo::

            Make the thumb image dimension settings configuratble.
            Currently, they are hard-wired to 300 columns

        """
        log.info('Building ingest data structure ...')

        data = {}
        file_basename = os.path.basename(self._filename)
        if file_basename.endswith('.proc'):
            file_basename = os.path.splitext(file_basename)[0]
        row_id = os.path.splitext(file_basename)[0]
        data['row_id'] = row_id
        shard_id = self.get_shard(row_id)
        data['shard_id'] = shard_id

        data['tables'] = {}

        meta_structure = self._build_meta_data_structure()
        data['tables'][self.meta_model.name] = {'cf': meta_structure}

        # New work as part of GDT-239 to build the image with a
        # reference to a HDFS URI.
        image_uri = self._build_image_uri(target_path, dry)
        data['tables'][self.meta_model.name]['cf']['cq']['image'] = image_uri

        # Suppress image data ingest as part of GDT-281.
        # Not removing until final solution has been defined.
        #image_structure = self._build_image_data_structure()
        #data['tables'][self.image_model.name] = {'cf': image_structure}
        #dimensions = {'x_coord_size': str(self.meta.x_coord_size),
        #              'y_coord_size': str(self.meta.y_coord_size)}
        #data['tables'][self.image_model.name]['cf']['cq'] = dimensions

        thumb_structure = self._build_image_data_structure(downsample=300,
                                                           thumb=True)
        data['tables'][self.thumb_model.name] = {'cf': thumb_structure}
        thumb_dimensions = {'x_coord_size': '300',
                            'y_coord_size': '300'}
        data['tables'][self.thumb_model.name]['cf']['cq'] = thumb_dimensions
        # Document Partitioned Indexing set as part of GDT-384.
        token_set = self.build_document_map(meta_structure['cq'])
        metasearch = self._build_metasearch_data_struct(row_id,
                                                        shard_id,
                                                        token_set)
        data['tables']['meta_search']= {'cf':  metasearch}

        log.info('Ingest data structure build done')

        return data

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, value):
        self._filename = value

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, value):
        self._dataset = value

    @property
    def meta(self):
        return self._meta

    @property
    def image(self):
        return self._image

    @property
    def meta_model(self):
        return self._meta_model

    @property
    def image_model(self):
        return self._image_model

    @property
    def thumb_model(self):
        return self._thumb_model

    @property
    def meta_shards(self):
        return self._meta_shards

    @meta_shards.setter
    def meta_shards(self, value):
        self._meta_shards = value

    @property
    def spatial_stripes(self):
        return self._spatial_stripes

    @spatial_stripes.setter
    def spatial_stripes(self, value):
        self._spatial_stripes = value

    def _build_meta_data_structure(self):
        """TODO

        """
        log.info('Building ingest metadata component ...')

        self.meta.extract_meta(self.dataset)

        data = {}
        data['cq'] = {'file': self.meta.file,
                      'x_coord_size': str(self.meta.x_coord_size),
                      'y_coord_size': str(self.meta.y_coord_size),
                      'geogcs': self.meta.geogcs}

        count = 0
        for geoxform in self.meta.geoxform:
            data['cq']['geoxform=%d' % count] = repr(geoxform)
            count += 1

        count = 0
        raw_image_boundaries = self.meta.calculate_extents()
        image_boundaries = self.meta.reproject_coords(raw_image_boundaries)
        for image_boundary in sorted(image_boundaries):
            data['cq']['coord=%d' % count] = ('%s,%s' %
                                              (image_boundary[1],
                                               image_boundary[0]))
            count += 1

        raw_image_centroid = self.meta.calculate_centroid(lat_long=True)
        image_centroid = self.meta.reproject_coords([raw_image_centroid])
        if len(image_centroid):
            data['cq']['center'] = ('%s,%s' % (image_centroid[0][1],
                                            image_centroid[0][0]))

        for meta_key in self.meta.metadata:
            meta_value = self.meta.metadata[meta_key]
            data['cq']['metadata=%s' % meta_key] = meta_value

        log.info('Ingest metadata structure build done')

        return data

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

    def _build_image_data_structure(self, downsample=None, thumb=False):
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
            msg = '%s: downsample %d columns' % (msg, downsample)
        log.info('%s ...' % msg)

        key = 'image'
        if thumb:
            key = 'thumb'

        data = {}
        data['val'] = {key: self.image.extract_image(self.dataset,
                                                     downsample)}

        log.info('Ingest image structure build done')

        return data

    def _build_metasearch_data_struct(self, row_id, shard_id, token_set):
        """Creates a Document Partitioned Indexed structure for
        the metadata component defined by the *token_set* document map.

        **Args:**
            *row_id*: the Accumulo row_id

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

        data = {}
        data['val'] = {'e': shard_id}

        data['cq'] = {}
        for token in token_set:
            data['cq'][token] = row_id

        log.info('Ingest metasearch structure build done')

        return data

    def get_shard(self, source):
        code = hashcode(source)
        shard = "s%02d" % ((code & 0x0ffffffff) % self.meta_shards)

        log.debug('Shard for source "%s": "%s"' % (source, shard))

        return shard

    def get_stripe_token(self, source):
        code = hashcode(source)
        stripe_token = ((code & 0x0ffffffff) % self.spatial_stripes)
        stripe_token = str(stripe_token).zfill(4)

        log.debug('Stripe token for source "%s": "%s"' %
                  (source, stripe_token))

        return stripe_token

    def get_reverse_timestamp(self, utc_time):
        """Converts a string representation of time denoted by *utc_time*
        into a reverse timestamp.

        Supported *utc_time* construct is ``CCYYMMDDhhmmss``, where:

        * **CC** is the century (00 to 99)
        * **YY** is the last two digits of the year (00 to 99)
        * **MM** is the month (01 to 12)
        * **DD** is the day (01 to 31)
        * **hh** is the hour (00 to 23)
        * **mm** is the minute (00 to 59)
        * **ss** is the second (00 to 59)

        Timezone designator is assumed UTC (Zulu).

        **Args:**
            *utc_time*: string representation of time as per the
            definition within the National Imagery Transmission Format
            Version 2.1.  For example, ``19961217102630``

            .. note::

                The assumption here is a well-formed *utc_time* value
                of 14 digits without the hyphens denoting unknown
                or not expressed values.  Malformed *utc_time*
                will not be converted.

        **Returns:**
            String of uniform length 20 character representing the
            reverse timestamp of the the given *utc_time* or ``None``
            if the conversion fails

        """
        log.debug('Generating reverse timestamp for UTC string "%s"' %
                   utc_time)
        reverse_timestamp = None

        if len(utc_time) == 14 and '-' not in utc_time:
            utc_struct_time = time.strptime(utc_time, '%Y%m%d%H%M%S')
            sec_since_epoch = calendar.timegm(utc_struct_time)

            reverse_timestamp = sys.maxint - int(sec_since_epoch * 10**6)
            reverse_timestamp = str(reverse_timestamp).zfill(20)
        else:
            log.error('Unsupported UTC time: "%s"' % utc_time)

        log.info('Source UTC|Reverse timestring: "%s|%s"' %
                 (utc_time, reverse_timestamp))

        return reverse_timestamp

    def _build_image_uri(self, target_path=None, dry=False):
        """Stores :attr:`filename` into a HDFS datastore
        and builds the resultant URI into the image library schema's
        ``image`` component.

        Requires an active :attr:`dataset`.

        **Kwargs:**
            *target_path*: directory structure that can be prepended to
            the destination file path.  Defaults to ``None`` which means
            current directory of target device.

            *dry*: if ``True`` only simulate, do not execute

        **Returns:**
            dictionary structure that represents an Accumulo
            column family/qualifier structure in the form::

                hdfs://jp2044lm-hdfs-nn01/tmp/i_3001a.ntf

        """
        uri = self.image_model.hdfs_write(self.filename, target_path, dry)

        return uri

    def open(self):
        """Attempts to open :attr:`filename` as a raster file as a
        GDALDataset.

        **Returns:**
            a :mod:`osgeo.gdal.Dataset` stream object

        """
        try:
            log.debug('Attempting to open file "%s"' % self.filename)
            self.dataset = gdal.Open(self.filename, gdal.GA_ReadOnly)
        except ValueError as err:
            log.error('Unable to open "%s": %s' % (self.filename, err))

    def close(self):
        """Close the :mod:`osgeo.gdal.Dataset` stream object

        """
        self._dataset = None
