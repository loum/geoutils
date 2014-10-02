# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.index.Spatial` provides a spatial/temporal
indexing mechanism.

"""
__all__ = ["Spatial"]

import geohash
import sys
import time
import calendar

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
        return geohash.encode(latitude, longitude, precision=precision)

    def get_stripe_token(self, source):
        code = hashcode(source)
        stripe_token = ((code & 0x0ffffffff) % self.stripes)
        stripe_token = str(stripe_token).zfill(4)

        log.debug('Stripe token for source "%s": "%s"' %
                  (source, stripe_token))

        return stripe_token

    def get_reverse_timestamp(self, utc_time=None):
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

                If *utc_time* is ``None`` then the current time is used.

        **Returns:**
            String of uniform length 20 character representing the
            reverse timestamp of the the given *utc_time* or ``None``
            if the conversion fails

        """
        log.debug('Generating reverse timestamp for UTC string "%s"' %
                   utc_time)
        reverse_ts = None
        secs_since_epoch = None

        if utc_time is None:
            secs_since_epoch = time.time()
        elif len(utc_time) == 14 and '-' not in utc_time:
            utc_struct_time = time.strptime(utc_time, '%Y%m%d%H%M%S')
            secs_since_epoch = calendar.timegm(utc_struct_time)
        else:
            log.error('Unsupported UTC time: "%s"' % utc_time)

        if secs_since_epoch is not None:
            reverse_ts = sys.maxint - int(secs_since_epoch * 10 ** 6)
            reverse_ts = str(reverse_ts).zfill(20)

        log.info('Source UTC|Reverse timestring: "%s|%s"' %
                 (utc_time, reverse_ts))

        return reverse_ts
