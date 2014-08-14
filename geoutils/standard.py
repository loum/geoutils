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

    """
    _filename = None
    _dataset = None
    _metadata = geoutils.Metadata()
    _image = geoutils.GeoImage()
    _meta_table_name = 'meta_test'

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

        """
        data = {}

        self.metadata.extract_meta(self.dataset)

        file_basename = os.path.basename(self._filename)
        data['row_id'] = os.path.splitext(file_basename)

        data['tables'] = {}

        # Metadata table.
        meta = data['tables'][self._meta_table_name] = {}
        meta['cf'] = {'cq': {}}
        meta['cf']['cq'] = {'file': self.metadata.file,
                            'x_coord_size': str(self.metadata.x_coord_size),
                            'y_coord_size': str(self.metadata.y_coord_size),
                            'geogcs': self.metadata.geogcs}

        count = 0
        for geoxform in self._metadata.geoxform:
            meta['cf']['cq']['geoxform=%d' % count] = repr(geoxform)
            count += 1

        for meta_key in self._metadata.metadata:
            meta_value = self._metadata.metadata[meta_key]
            meta['cf']['cq']['metadata=%s' % meta_key] = meta_value

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
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    @property
    def image(self):
        return self._image

    @image.setter
    def image(self, value):
        self._image = value

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
