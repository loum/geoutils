# Add error codes here so that pylint doesn't complain.
# pylint: disable=E1101,C0111
"""The :class:`geoutils.Standard` is the base class for all standards that
relate to the exchange, storage and transmission of digital-imagery
products and image-related products.

"""
__all__ = ["Standard"]

import os
from osgeo import gdal

import geoutils
from oct.utils.log import log


class Standard(object):
    """:class:`geoutils.Standard`

    .. attribute: filename

    .. attribute:: *meta_table_name*
        Accumulo table name of the image metadata library (defaults to
        ``meta_test``)

    .. attribute:: *image_table_name*
        Accumulo table name of the image library (defaults to
        ``image_test``)

    .. attribute:: *thumb_table_name*
        Accumulo table name of the thumb library (defaults to
        ``thumb_test``)

    """
    _filename = None
    _dataset = None
    _meta = geoutils.Metadata()
    _image = geoutils.GeoImage()
    _meta_table_name = 'meta_test'
    _image_table_name = 'image_test'
    _thumb_table_name = 'thumb_test'

    def __init__(self, source_filename=None):
        self._filename = source_filename

    def __call__(self):
        """The object instance callable is a quick handle to the
        meta/image extaction process.  It will also construct a dictionary
        like construct that can be fed directly into a
        :class:`geoutils.Datastore` instance.

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
        data['row_id'] = os.path.splitext(file_basename)[0]

        data['tables'] = {}

        meta_structure = self._build_meta_data_structure()
        data['tables'][self._meta_table_name] = {'cf': meta_structure}

        image_structure = self._build_image_data_structure()
        data['tables'][self._image_table_name] = {'cf': image_structure}
        dimensions = {'x_coord_size': str(self.meta.x_coord_size),
                      'y_coord_size': str(self.meta.y_coord_size)}
        data['tables'][self._image_table_name]['cf']['cq'] = dimensions

        thumb_structure = self._build_image_data_structure(downsample=300)
        data['tables'][self._thumb_table_name] = {'cf': thumb_structure}
        thumb_dimensions = {'x_coord_size': '300',
                            'y_coord_size': '300'}
        data['tables'][self._thumb_table_name]['cf']['cq'] = thumb_dimensions

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
    def meta_table_name(self):
        return self._meta_table_name

    @meta_table_name.setter
    def meta_table_name(self, value):
        self._meta_table_name = value

    @property
    def image_table_name(self):
        return self._image_table_name

    @image_table_name.setter
    def image_table_name(self, value):
        self._image_table_name = value

    @property
    def thumb_table_name(self):
        return self._thumb_table_name

    @thumb_table_name.setter
    def thumb_table_name(self, value):
        self._thumb_table_name = value

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

        for meta_key in self.meta.metadata:
            meta_value = self.meta.metadata[meta_key]
            data['cq']['metadata=%s' % meta_key] = meta_value

        log.info('Ingest metadata structure build done')

        return data

    def _build_image_data_structure(self, downsample=None):
        """TODO

        """
        msg = 'Building ingest image component'
        if downsample is not None:
            msg = '%s: downsample %d columns' % (msg, downsample)
        log.info('%s ...' % msg)

        data = {}

        data['val'] = {'image': self.image.extract_image(self.dataset,
                                                         downsample)}

        log.info('Ingest image structure build done')

        return data

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
            log.error('Unable top open "%s": %s' % (self.filename, err))

    def close(self):
        """Close the :mod:`osgeo.gdal.Dataset` stream object

        """
        self._dataset = None
