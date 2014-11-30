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
    _event_id = None
    _event_day = None
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
    def event_id(self):
        return self._event_id

    @event_id.setter
    def event_id(self, value):
        self._event_id = value

    @property
    def event_day(self):
        return self._event_day

    @event_day.setter
    def event_day(self, value):
        self._event_day = value

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

        Only interested in the EVENTID AND DATE ATTRIBUTES (column 1) and
        DATA MANAGEMENT FIELDS (index 5) columns

        **Args:**
            *dataset*: a :class:`gdal.Dataset` object generally
            obtained via a :func:`gdal.Open` operation

        **Returns:**
             Boolean ``True`` if the image extraction was successful
             Boolean ``False`` otherwise

        """
        event_columns = data.split('\t')

        event_id = event_columns[0]
        event_day = event_columns[1]
        log.debug('Processing GlobalEventID|Day: "%s|%s"' %
                  (event_id, event_day))
        self.event_id = event_id
        self.event_day = event_day

        self.type = event_columns[49]
        self.fullname = event_columns[50]
        self.country_code = event_columns[51]
        self.adm1_code = event_columns[52]
        self.latitude = event_columns[53]
        self.longitude = event_columns[54]
        self.feature_id = event_columns[55]
        self.date_added = event_columns[56]

        if len(event_columns) == 58:
                self.source_url = event_columns[57]
