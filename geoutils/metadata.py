# pylint: disable=C0111,R0902
"""The :class:`geoutils.Metadata` class is a component of a
:class:`geoutils.Standard`: each standard presents a metadata component
and an image.  This class focuses on the metadata.

"""
__all__ = ["Metadata"]

from osgeo import osr, gdal
import os
import shapely.geometry

from geosutils.log import log


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
        (Xp,Yp) space.  The items in the tuple represent:

        * geoxform[0]: Top left X
        * geoxform[1]: West-east pixel resolution
        * geoxform[2]: Rotation (0 if image is "north up"
        * geoxform[3]: Top left Y
        * geoxform[4]: Rotation (0 if image is "north up")
        * geoxform[5]: North-south pixel resolution

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

    def extract_meta(self, dataset):
        """Attempts to extract the metadata from the
        :attr:`geoutils.Standard.dataset` *dataset*

        **Args:**
            *dataset*: a :class:`gdal.Dataset` object generally
            obtained via a :func:`gdal.Open` operation

        **Returns:**
             Boolean ``True`` if the image extraction was successful
             Boolean ``False`` otherwise

        """
        status = False

        if dataset is None:
            log.warn('Extraction failed: dataset stream not provided')
        else:
            self.driver = dataset.GetDriver()
            log.debug('Driver: %s' % str(self.driver))

            self.file = os.path.basename(dataset.GetFileList()[0])
            # Strip off the trailing ".proc"
            if self.file.endswith('.proc'):
                self.file = os.path.splitext(self.file)[0]
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

            status = True

        return status

    def calculate_extents(self):
        """Calculate the corner coordinates from a geotransform.

        **Returns:**
            4 x 2 dimensional list of coordinates taken from the
            :attr:`geoutils.Metadata.geoxform` attribute that represent
            the corner boundaries of a geographic image

        """
        log.debug('Calculating corner coordinates from geotransform: %s' %
                  str(self.geoxform))
        extents = []
        x_points = [0, self.x_coord_size]
        y_points = [0, self.y_coord_size]
        log.debug('Image (X, Y) size: (%d, %d)' % (self.x_coord_size,
                                                   self.y_coord_size))

        for x_point in x_points:
            for y_point in y_points:
                corner = self.point_to_lat_long((x_point, y_point))
                extents.append(corner)
                log.debug('(X, Y) extents: (%.16f, %.16f)' %
                          (corner[0], corner[1]))
            y_points.reverse()

        return extents

    def calculate_centroid(self, lat_long=False):
        """Calculate the center coordinates of the image defined
        by its :attr:`geoutils.Metadata.x_coord_size` and
         :attr:`geoutils.Metadata.y_coord_size` coordinates.

        Treats the image extents as a polygon and reduces the problem
        domain to a simple geometric evaluation.

        Centroid value is returned as a (X, Y) point unless
        *lat_long* is set in which case it will be converted to a
        longitude, latitude set.

        **Kwargs:**
            *lat_long*: transform the centroid from (X, Y) point to
            longitude, latitude

        **Returns:**
            A tuple structure representing the image's centroid

        """
        log.debug('Calculating center coordinate from geotransform: %s' %
                  str(self.geoxform))

        polygon_points = ((0, 0),
                          (self.x_coord_size, 0),
                          (self.x_coord_size, self.y_coord_size),
                          (0, self.y_coord_size))
        poly = shapely.geometry.Polygon(polygon_points)
        centroid_point = (poly.centroid.x, poly.centroid.y)
        log.info('Centroid point (X, Y): %s' % str(centroid_point))

        if lat_long:
            centroid_point = tuple(self.point_to_lat_long(centroid_point))
            log.info('Centroid to lat/long (X, Y): %s' %
                     str(centroid_point))

        return centroid_point

    def point_to_lat_long(self, point):
        """Convert raster positions (in pixel/line coordinates) provided
        by *point* to georeferenced coordinates based on an affine
        transform.

        The affine transform consists of six coefficients provided by
        :attr:`geoutils.Metadata.geoxform` which maps pixel/line
        coordinates into georeferenced space.  The algorithm
        is detailed `here <http://www.gdal.org/gdal_datamodel.html>`_

        **Args:**
            *point*: tuple that represents the pixel/line coordinates
            of the point to transform.  For example::

                (512.0, 512.0)

        **Returns:**
            longitude/latitude translation as a iterator (list) structure

        """
        longitude = (self.geoxform[0] +
                     (point[0] * self.geoxform[1]) +
                     (point[1] * self.geoxform[2]))

        latitude = (self.geoxform[3] +
                    (point[0] * self.geoxform[4]) +
                    (point[1] * self.geoxform[5]))

        return [longitude, latitude]

    def reproject_coords(self, extents):
        """Reproject a list of X, Y coordinates provided by *extents*.
        Typically, *extents* will be a list of 4 X, Y coordinates
        (themselves a 2-element list construct) similar to the following::

        >>> import pprint
        >>> pprint.pprint(extents)
        [[84.999999864233729, 32.983333469099598],
         [84.999999864233729, 32.983055419789295],
         [85.000277913544039, 32.983055419789295],
         [85.000277913544039, 32.983333469099598]]

        This method will only be useful if we need to re-project the
        coordinates from one projection to another.  For example, UTM to
        WGS 84.  However, currently we are only supporting the
        World Geodetic System Geographic Coordinate System (aka WGS 84,
        WGS 1984, EPSG:4326).  As such, this method in its current
        form is a simple WGS 84 validator.

        **Args:**
            *extents*: 4 x 2 dimensional list of XY coordinates that
            represent the corner boundaries of a geogrpahic image

        **Returns:**
            4 x 2 dimensional list of WGS 84-based coordinates
            that represent the corner boundaries of a geographic image

        """
        log.debug('Projection: "%s"' % self.geogcs)

        # Get some info about the source Spatial Reference System.
        spatial_ref_sys = osr.SpatialReference(wkt=self.geogcs)
        log.debug('Source SRS IsProjected?: %s' %
                  (spatial_ref_sys.IsProjected() == 1))
        geogcs = spatial_ref_sys.GetAttrValue('geogcs')
        log.debug('Geographic coordinate system: %s' % geogcs)

        trans_coords = []
        if spatial_ref_sys.GetAttrValue('geogcs') != 'WGS 84':
            log.error('Unsupported GEOGCS "%s": skipped re-projection' %
                      geogcs)
        else:
            target_spatial_ref_sys = spatial_ref_sys.CloneGeogCS()
            transform = osr.CoordinateTransformation(spatial_ref_sys,
                                                     target_spatial_ref_sys)
            for x_coord, y_coord in extents:
                x_coord, y_coord, z_coord = transform.TransformPoint(x_coord,
                                                                     y_coord)
                trans_coords.append([x_coord, y_coord])

        log.debug('Re-projected coords: "%s"' % trans_coords)

        return trans_coords
