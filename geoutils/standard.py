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
import geoutils.model
from oct.utils.log import log


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

    def __init__(self, source_filename=None):
        self._filename = source_filename

    def __call__(self, target_path=None, dry=False):
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
            family/value structure of the form::

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

    def _build_image_uri(self, target_path=None, dry=False):
        """Stores :attr:`filename` into a HDFS datastore
        and builds the resultant URI into the image library schema's
        ``image`` component.

        Requires an active :attr:`dataset`.

        **Kwargs:**
            *target_path*: directory structure that can be prepended to
            the destination file path.  Defaults to ``None`` which means
            current directory of target device.

            *dry*: only report, do not execute


        **Returns:**
            dictionary structure that represents an Accumulo
            column family/qualifier structure of the form::

                hdfs://jp2044lm-hdfs-nn01/tmp/i_3001a.ntf

        """
        uri = self.image_model.hdfs_write(self.filename,
                                          target_path,
                                          dry)

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
