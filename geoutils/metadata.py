# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Metadata` class is a component of a
:class:`geoutils.Standard`: each standard presents a metadata component
and an image.

"""
__all__ = ["Metadata"]

import os

from oct.utils.log import log


class Metadata(object):
    """:class:`geoutils.Metadata`

    .. attribute:: *driver*
        Based in the input file, Python GDAL will register the correct
        driver to access the dataset

    .. file:: *file*
        Source file name that is undergoing processing

    .. attribute:: *bands*
        Number of raster bands contained within the dataset.
        In general, a dataset is an assembly of raster bands and some
        general information common to them all

    .. attribute:: *x_coord_size*
        Raster width in pixels

    .. attribute:: *y_coord_size*
        Raster height in pixels

    .. attribute:: *geogcs*
        Information on the spatial reference system.
        This is returned as a string in Well Known Text (WKT) format.
        WKT is defined by the Open Geospace Consortium and is a common
        format used to exchange spatial information

    .. attribute:: *geoxform*
        Returns a 6-item tuple that represents the affine transformation
        coefficients.  The coefficients can be used for transforming
        between pixel/line (P,L) raster space, and projection coordinates
        (Xp,Yp) space

    .. metadata::
        Dictionary of key/value pairs that provide details about the
        image data

    """
    _driver = None
    _file = None
    _x_coord_size = 0
    _y_coord_size = 0
    _geogcs = None
    _geoxform = []
    _metadata = {}

    @property
    def driver(self):
        return self._driver

    @driver.setter
    def driver(self, value):
        self._driver = value

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, value):
        self._file = value

    @property
    def x_coord_size(self):
        return self._x_coord_size

    @x_coord_size.setter
    def x_coord_size(self, value):
        self._x_coord_size = value

    @property
    def y_coord_size(self):
        return self._y_coord_size

    @y_coord_size.setter
    def y_coord_size(self, value):
        self._y_coord_size = value

    @property
    def geogcs(self):
        return self._geogcs

    @geogcs.setter
    def geogcs(self, value):
        self._geogcs = value

    @property
    def geoxform(self):
        return self._geoxform

    @geoxform.setter
    def geoxform(self, values):
        del self._geoxform[:]
        self._geoxform = []
        self._geoxform.extend(values)

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, values):
        self._metadata.clear()
        self._metadata = values

    def __call__(self):
        return {'file': self.file}

    def extract_meta(self, dataset):
        """Attempts to extract the metadata from the
        :attr:`geoutils.Standard.dataset` *dataset*

        """
        if dataset is not None:
            self.driver = dataset.GetDriver()
            log.debug('Driver: %s' % str(self.driver))

            self.file = os.path.basename(dataset.GetFileList()[0])
            log.debug('File: %s' % str(self.file))

            self.x_coord_size = dataset.RasterXSize
            log.debug('X-cord size: %d' % self.x_coord_size)

            self.y_coord_size = dataset.RasterYSize
            log.debug('Y-cord size: %d' % self.y_coord_size)

            self.geogcs = dataset.GetProjection()
            log.debug('Spatial Reference System: %s' % self.geogcs)

            self.geoxform = dataset.GetGeoTransform()
            log.debug('GeoTransform: %s' % self.geoxform)

            self.metadata = dataset.GetMetadata_Dict()
            log.debug('Metadata dict: %s' % self.metadata)
        else:
            log.warn('Extraction failed: dataset stream not provided')
