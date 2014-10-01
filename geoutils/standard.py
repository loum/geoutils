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

        file_basename = os.path.basename(self._filename)
        if file_basename.endswith('.proc'):
            file_basename = os.path.splitext(file_basename)[0]
        row_id = os.path.splitext(file_basename)[0]
        shard_id = self.get_shard(row_id)

        self.meta.extract_meta(self.dataset)

        schema = geoutils.Schema(row_id, shard_id)

        # New work as part of GDT-239 to build the image with a
        # reference to a HDFS URI.
        image_uri = self.image_model.hdfs_write(self.filename,
                                                target_path,
                                                dry)
        schema.build_meta(self.meta_model.name, self.meta, image_uri)

        image_extract_ref = self.image.extract_image(self.dataset, 300)
        schema.build_image(self.thumb_model.name,
                           image_extract_ref,
                           downsample='300',
                           thumb=True)

        log.info('Ingest data structure build done')

        return schema()

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
