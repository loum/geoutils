# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Auditer` is the auditing mechanism based on the
Borg monostate idiom -- only one instance of the class is ever created.

"""
__all__ = ['Auditer',
           'audit']

import os

from geosutils.log import log


class Auditer(object):
    """:class:`geoutils.Auditer`

    """
    _state = {}

    _source_id = None
    _table_name = 'audit'

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls, *args, **kwargs)
        obj.__dict__ = cls._state

        return obj

    def __init__(self, pid=None):
        """:class:`geoutils.Auditer` initialisation.

        We use the schema construct that is consistent with
        :mod:`geoutils.Schema`.  As we share state amongst multiple
        invocations, we only need to initialise the data structure once.

        """
        if pid is None:
            pid = os.getpid()

        if self._state.get(pid) is None:
            log.debug('Setting audit state against PID: %d' % pid)
            self._state[pid] = {}
            self._state[pid]['tables'] = {}
            self._state[pid]['tables'][self.table_name] = {}
            self._state[pid]['tables'][self.table_name]['cf'] = {}
            self._state[pid]['tables'][self.table_name]['cf']['cq'] = {}
        else:
            log.debug('Audit state PID %d exists' % pid)

    def __call__(self):
        pid = os.getpid()

        state = {}

        try:
            if self.source_id is not None:
                self._state[pid]['row_id'] = self.source_id
            state = self._state[pid]
            log.debug('Audit call against PID %d: %s' %
                      (pid, self._state[pid]))
        except KeyError as error:
            log.error('PID state not defined: %s' % error)

        return state

    @property
    def source_id(self):
        return self._source_id

    @source_id.setter
    def source_id(self, value):
        self._source_id = value

    @property
    def data(self):
        return self._state[os.getpid()]

    @data.setter
    def data(self, value):
        pid = os.getpid()
        if self._state.get(pid) is None:
            log.debug('state PID is None')
            self._state[pid] = {}
            self._state[pid]['tables'] = {}
            self._state[pid]['tables'][self.table_name] = {}
            self._state[pid]['tables'][self.table_name]['cf'] = {}
            self._state[pid]['tables'][self.table_name]['cf']['cq'] = {}

        try:
            data = self._state[pid]['tables'][self.table_name]['cf']['cq']

            for name, action in value.iteritems():
                data.update({name: action})
        except KeyError as error:
            log.error('Unable to set data: %s' % error)

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    def reset(self):
        """Clears the current audit information.
        """
        pid = os.getpid()
        log.debug('Clearing audit info for PID: %d' % pid)
        self._state.pop(pid, None)

audit = Auditer()
