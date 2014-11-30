# pylint: disable=R0903,C0111,R0902,W0142
"""The :class:`geoutils.model.Gdelt` abstracts an Accumulo GDELT
table schema.

"""
__all__ = ["Gdelt"]

import geohash
import shapely

import geoutils
from geosutils.log import log


class Gdelt(geoutils.ModelBase):
    """:class:`geoutils.model.Gdelt` Accumulo datastore model.

    """
    _gdelt_table_name = 'gdelt_spatial_index'

    def __init__(self, connection, name=None):
        """:class:`geoutils.model.Gdelt` initialisation.

        **Kwargs:**
            *name*: override the name of the GDELT table

        """
        super(Gdelt, self).__init__(connection, name)

    @property
    def gdelt_table_name(self):
        return self._gdelt_table_name

    @gdelt_table_name.setter
    def gdelt_table_name(self, value):
        self._gdelt_table_name = value

    def query_gdelt(self, key=None):
        """Query the GDELT component from the datastore.

        **Kwargs:**
            *key*: at this time, *key* relates to the GDELT
            Accumulo table row_id.

        **Returns:**
            the GDELT component of *key* as a Python dictionary
            structure to the following::

            {'0000_dqcjqbxu3u0u_09221955249654775807_324845985': {
                'Actor1Geo_ADM1Code': 'USDC',
                ...
                'Actor1Geo_FeatureID': '531871'}}

        """
        msg = 'Query GDELT table "%s"' % self.name
        if key is not None:
            msg = '%s against key "%s"' % (msg, key)
        log.info('%s ...' % msg)

        gdelt = {}

        results = self.query(self.name, key)
        for cell in results:
            if gdelt.get(cell.row) is None:
                gdelt[cell.row] = {}

            gdelt[cell.row][cell.cf] = cell.cq

        log.info('Query key "%s" complete' % key)

        return gdelt

    def query_points(self, points, precision=5):
        """Scan the GDELT spatial index table that match a given list
        of *points* represented as a latitude/longitude tuple.

        A precision of 5 equates to a geohash grid size of around 10KM.

        **Args:**
            *points*: list iterable objects (list or tuple) representing
            the latitude and longitude of the point of interest

            *precision*: geohash grid size.  The precision in the
            Accumulo spatial index is 12 (less than 1 meter grid)
            so the value needs to be 12 or less.  Defaults to 5

        **Returns:**
            Dictionary structure representing all matching points
            contained with the search grid.  For of dictionary
            structure is::

                {'center_point_match': ['i_3001a', ...]}

        """
        gdelts = {'center_point_match': []}

        # Generate the geohashes.
        hashcodes = []
        for point in points:
            (latitude, longitude) = point

            hashcode = geohash.encode(latitude, longitude, precision)
            log.debug('Point "%s" geohash is: "%s"' % (point, hashcode))
            hashcodes.append(hashcode)

        results = self.regex_row_scan(self.gdelt_table_name, hashcodes)

        for cell in results:
            if cell.row not in gdelts['center_point_match']:
                gdelts['center_point_match'].append(cell.row)

        return gdelts

    def query_bbox_points(self, bbox, precision=4):
        """Scan the metadata spatial index table that match a given
        *bbox* boundary box range.

        A precision of 4 equates to a geohash grid size of around 500KM.

        **Args:**
            *bbox*: iterable object (list or tuple) representing
            the left (longitude), bottom (latitude), right (longitude)
            and top (latitude) of the bounding box.

            *precision*: geohash grid size.  The precision in the
            Accumulo spatial index is 12 (less than 1 meter grid)
            so the value needs to be 12 or less.  Defaults to 5

        **Returns:**
            Dictionary structure representing all matching points
            contained with the search grid.  For of dictionary
            structure is::

                {'center_point_match': ['i_3001a', ...]}

        """
        box = shapely.geometry.box(*bbox)
        centroid_point = (box.centroid.x, box.centroid.y)
        log.info('BBox centroid point (X, Y): %s' % str(centroid_point))

        return self.query_points(centroid_point, precision)
