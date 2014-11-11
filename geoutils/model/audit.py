# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Audit` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Audit"]

import geoutils
from geosutils.log import log


class Audit(geoutils.ModelBase):
    """Audit Accumulo datastore model.

    """
    _name = 'audit'

    def __init__(self, connection, name=None):
        """Audit model initialisation.

        **Kwargs:**
            *name*: override the name of the Audit table

        """
        super(Audit, self).__init__(connection, name)

    def query_recent_audit(self, key=None, depth=1):
        """Query the auditer component from the datastore.

        **Kwargs:**
            *key*: acts as a rugular expression filter to be used
            against the record row

            *depth*: limits the number of recent records returns

        **Returns:**
            the most recent audit row_id's as a Python dictionary
            structure

                {'audits': [
                    '09221956437718808093_ingest_daemon',
                    ...
                    '09221956447616086912_ingest_daemon',]}

        """
        msg = 'Query audit table "%s"' % self.name
        if key is not None:
            msg = '%s against key "%s"' % (msg, key)
        log.info('%s ...' % msg)

        audits = {'audits': []}

        if key is not None:
            results = self.regex_query(self.name, regexs=key)
        else:
            results = self.query(self.name)

        for result in results:
            if result.row not in audits['audits']:
                audits['audits'].append(result.row)

            if len(audits['audits']) >= depth:
                log.debug('Audit search depth of %d reached' % depth)
                break

        log.info('Query key "%s" complete' % key)

        return audits

    def query_audit(self, key=None):
        """Query the audit component from the datastore.

        **Kwargs:**
            *key*: *key* relates to the audit table row_id.  For example::

                09221956447764569211_ingest_daemon

        **Returns:**
            the audit information of *key* as a Python dictionary
            structure.  Structure is similar to the following"::

                {'09221956447764569211_ingest_daemon': {
                    'ingest_daemon|start': '1415589090.12',
                    ...
                    'ingest_daemon|finish': '1415589090.21',}}}

        """
        msg = 'Query audit table "%s"' % self.name
        if key is not None:
            msg = '%s against key "%s"' % (msg, key)
        log.info('%s ...' % msg)

        audits = {}
        results = self.query(self.name, key)

        for cell in results:
            if audits.get(cell.row) is None:
                audits[cell.row] = {}

            audits[cell.row][cell.cf] = cell.cq

        log.info('Query key "%s" complete' % key)

        return audits
