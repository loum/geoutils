# pylint: disable=C0111,R0902
"""The :class:`geoutils.Gdelt` class models the data associated with
GDELT project.

Used primarily as a proof of concept around bulk latitude/longitude
ingests and geospatial queries.

"""
__all__ = ["Gdelt"]

import geoutils

from geosutils.log import log


class Gdelt(object):
    """:class:`geoutils.Gdelt`

    .. file:: *file*
        Source file name that is undergoing processing

    """
    _type = None
    _fullname = None
    _country_code = None
    _adm1_code = None
    _latitude = None
    _longitude = None
    _feature_id = None
    _date_added = None
    _source_url = None

    def __init__(self, data=None):
        """Accept a line of *data* from a GDELT source file and extract
        all relevant values.

        """
        if data is not None:
            self.extract_gdelt(data)

    def __call__(self):
        """The object instance callable is a quick handle to the
        GDELT event ingest schema object.

        **Returns:**
            a dictionary structure that can be fed into a
            :class:`geoutils.Datastore` ingest

        """
        schema = geoutils.Schema()

        schema.build_gdelt_spatial_index('gdelt_spatial_index', self)

        return schema()

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def fullname(self):
        return self._fullname

    @fullname.setter
    def fullname(self, value):
        self._fullname = value

    @property
    def country_code(self):
        return self._country_code

    @country_code.setter
    def country_code(self, value):
        self._country_code = value

    @property
    def adm1_code(self):
        return self._adm1_code

    @adm1_code.setter
    def adm1_code(self, value):
        self._adm1_code = value

    @property
    def latitude(self):
        return self._latitude

    @latitude.setter
    def latitude(self, value):
        self._latitude = value

    @property
    def longitude(self):
        return self._longitude

    @longitude.setter
    def longitude(self, value):
        self._longitude = value

    @property
    def feature_id(self):
        return self._feature_id

    @feature_id.setter
    def feature_id(self, value):
        self._feature_id = value

    @property
    def date_added(self):
        return self._date_added

    @date_added.setter
    def date_added(self, value):
        self._date_added = value

    @property
    def source_url(self):
        return self._source_url

    @source_url.setter
    def source_url(self, value):
        self._source_url = value

    def extract_gdelt(self, data):
        """Attempts to extract the GDELT data from the based on *data*

        **Args:**
            *dataset*: a :class:`gdal.Dataset` object generally
            obtained via a :func:`gdal.Open` operation

        **Returns:**
             Boolean ``True`` if the image extraction was successful
             Boolean ``False`` otherwise

        """
        log.debug('Received: %s' % data)

        tmp_data = data.split('\t')

        self.type = tmp_data[0][0]
        self.fullname = tmp_data[0][1:]
        self.country_code = tmp_data[1]
        self.adm1_code = tmp_data[2]
        self.latitude = tmp_data[3]
        self.longitude = tmp_data[4]
        self.feature_id = tmp_data[5]
        self.date_added = tmp_data[6]

        if len(tmp_data) > 7:
            self.source_url = tmp_data[7]
