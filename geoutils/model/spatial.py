# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Spatial` abstracts an Accumulo geohash-based
table schema.

"""
__all__ = ["Spatial"]

import json
import re
import geohash

import geoutils
from geosutils.log import log


class Spatial(geoutils.ModelBase):
    """Spatial Accumulo datastore model.

    """
    _name = 'spatial'

    def __init__(self, connection, name=None):
        """Spatial model initialisation.

        **Kwargs:**
            *name*: override the name of the Spatial table

        """
        super(Spatial, self).__init__(connection, name)

    def gen_geohash(self,
                    latitude,
                    longitude,
                    precision=12):
        """Convert a lat/long point into a geohash.

        The geohash algorithm is well documented
        `here <http://en.wikipedia.org/wiki/Geohash>`_

        **Args:**
            *latitude*: point latitude

            *longitude*: point longitude

            *precision*: number of characters to use in the geohash.
            The more characters, the smaller the geohash grid which inturn
            improves the point precision

        **Returns:**
            the geohash value

        """
        return geohash.encode(latitude, longitude, precision=precision)
