# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.ModelBase` is the base construct for the
Accumulo models.

"""
__all__ = ["ModelBase"]

import pyaccumulo

from geosutils.log import log


class ModelBase(object):
    """:class:`geoutils.ModelBase` is intended to be a generalisation
    of a Accumulo datastore table.

    """
    _name = str()
    _connection = None

    def __init__(self, connection, name=None):
        self._connection = connection
        if name is not None:
            self._name = name

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    def query(self, table, key=None, cols=None):
        """Base method for a Accumulo table scan.

        **Args:**
            *table*: name of the table to scan

        **Kwargs:**
            *key*: optionally provide a value that will filter the scan
            against the table Row ID

            *cols*: limit the extract to the record's column family and
            qualifier identifiers.  The structure is a family/qualifier
            combination in list format.  For example::

            >>> cols=['family', 'qualifier']

            or

            >>> cols=['family']

        **Returns:**
            Generator object that can be iterated over to display
            the record's cell data

        """
        if cols is None:
            cols = []

        scan_range = None
        if key is not None:
            log.info('Table "%s" scan ...' % table)
            scan_range = pyaccumulo.Range(srow=key, erow=key)
        else:
            log.info('Querying table "%s" against key: "%s" ...' %
                     (table, key))

        if scan_range is not None:
            results = self.connection.scan(table=table,
                                           scanrange=scan_range,
                                           cols=cols)
        else:
            results = self.connection.scan(table=table, cols=cols)

        return results
