# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.index.Spatial` provides a spatial/temporal
indexing mechanism.

"""
__all__ = ['Spatial']

import geohash

from geosutils.log import log
from geosutils.utils import hashcode


class Spatial(object):
    """Spatial Accumulo datastore index.

    """
    _stripes = 1

    @property
    def stripes(self):
        return self._stripes

    @stripes.setter
    def stripes(self, value):
        self._stripes = value

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
        hash = None

        if latitude is not None and longitude is not None:
            if ((isinstance(latitude, str) and len(latitude)) and
               (isinstance(longitude, str) and len(longitude))):
                latitude = float(latitude)
                longitude = float(longitude)

            if (isinstance(latitude, float) and
               isinstance(longitude, float)):
                hash = geohash.encode(latitude,
                                      longitude,
                                      precision=precision)

        log.info('Geohash for lat/long "%s/%s" produced "%s"' %
                 (latitude, longitude, hash))

        return hash

    def get_stripe_token(self, source):
        code = hashcode(source)
        stripe_token = ((code & 0x0ffffffff) % self.stripes)
        stripe_token = str(stripe_token).zfill(4)

        log.debug('Stripe token for source "%s": "%s"' %
                  (source, stripe_token))

        return stripe_token
