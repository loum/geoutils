# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Auditer` is the auditing mechanism based on the
Borg monostate idiom -- only one instance of the class is ever created.

"""
__all__ = ['Auditer',
           'audit']

import geoutils.index
from geosutils.log import log


class Auditer(object):
    """:class:`geoutils.Auditer`

    """
    _state = {}
    _state['data'] = {}

    _source_id = None
    _table_name = 'audit'

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls, *args, **kwargs)
        obj.__dict__ = cls._state

        return obj

    def __init__(self, source_id=None):
        """:class:`geoutils.Auditer` initialisation.

        We use the schema construct that is consistent with
        :mod:`geoutils.Schema`.  As we share state amongst multiple
        invocations, we only need to initialise the data structure once.

        """
        if self.data.get('tables') is None:
            self._state['data']['tables'] = {}
            self._state['data']['tables'][self.table_name] = {}
            self._state['data']['tables'][self.table_name]['cf'] = {}
            self._state['data']['tables'][self.table_name]['cf']['cq'] = {}

    def __call__(self):
        self._state['data']['row_id'] = self.source_id
        log.debug('Audit call: %s' % self._state['data'])

        return self._state['data']

    @property
    def source_id(self):
        return self._source_id

    @source_id.setter
    def source_id(self, value):
        self._source_id = value

    @property
    def data(self):
        return self._state['data']

    @data.setter
    def data(self, value):
        data = self._state['data']['tables'][self.table_name]['cf']['cq']

        for name, action in value.iteritems():
            data.update({name: action})

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    def reset(self):
        """Clears the current audit information.
        """
        log.debug('Clearing audit info')
        self._state['data'] = {}

audit = Auditer()
