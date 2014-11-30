# pylint: disable=R0903,C0111,R0902,W0142
"""The :class:`geoutils.ModelBase` is the base construct for the
Accumulo models.

"""
__all__ = ["ModelBase"]

import collections
import pyaccumulo
import pyaccumulo.iterators

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

    def query(self, table, key=None, cols=None):
        """Base method for a Accumulo table scan.

        **Args:**
            *table*: name of the table to scan

        **Kwargs:**
            *key*: optionally provide a value that will filter the scan
            against the table Row ID

            *cols*: limit the extract to the record's column family and
            qualifier identifiers.  The structure is a family/qualifier
            combination in a list of lists format.  For example::

            >>> cols=[['family', 'qualifier'], ...]

            or

            >>> cols=[['family'], ...]

            .. note::

                Family and qualifier identifier values must be exact.
                Only the cell that matches the family/qualifier identifier
                combination will be returned

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

    def doc_query(self, table, search_terms):
        """Base method for a Accumulo table document based batch scan.

        **Args:**
            *table*: name of the table to scan

        **Kwargs:**
            *search_terms*: keys to use in the search

        **Returns:**
            Generator object that can be iterated over to display
            the record's cell data

        """
        # This range aligns with the shard code.
        scan_ranges = [pyaccumulo.Range(srow='s', erow='t')]
        kwargs = {'priority': 21,
                  'terms': search_terms}
        iterators = [pyaccumulo.iterators.IntersectingIterator(**kwargs)]
        log.info('Querying table "%s" against search terms: "%s" ...' %
                 (table, search_terms))

        results = []
        for record in self.connection.batch_scan(table=table,
                                                 scanranges=scan_ranges,
                                                 iterators=iterators):
            results.append(record.cq)

        return results

    def regex_query(self, table, regexs, cols=None):
        """A simple implementation of an Accumulo regular expression
        filter iterator.

        .. note::

            Although *regexs* accepts a list of values that can chained
            together, only the first index is used at this time.

        **Kwargs:**
            *table*: the name of the table to search.

            *regex*: list of Java based regular expression constructs.
            In the context of the image spatial index search::

                ['.*tvu7whrjnc16.*']

        """
        log.info('Querying table "%s" against regexs: "%s" ...' %
                 (table, regexs))

        if isinstance(regexs, basestring):
            regexs = [regexs]

        iterators = [pyaccumulo.iterators.RegExFilter(row_regex=regexs[0])]

        results = self.connection.batch_scan(table=table,
                                             iterators=iterators,
                                             cols=cols)

        return results

    def regex_scan(self, table, family, qualifiers):
        """Scan *table* for a record with row *family* identifier
        that matches the given list of *qualifers*.

        **Args:**
            *table*: name of the table to scan

            *family*: column family identifier to scan

            *qualifers*: list of column qualifer identifiers to scan

        **Returns:**
            lisf of Accumulo cells that match the search term criteria

        """
        iterators = []
        priority = 21
        count = 1
        for qualifier in qualifiers:
            kwargs = {'priority': priority,
                      'cf_regex': family,
                      'cq_regex': '.*%s.*' % qualifier,
                      'match_substring': True,
                      'name': 'regex%d' % count}
            iterators.append(pyaccumulo.iterators.RegExFilter(**kwargs))
            log.debug('RegExIterator kwargs: %s' % kwargs)
            priority += 1
            count += 1

        results = self.connection.scan(table=table, iterators=iterators)
        return results

    def regex_row_scan(self, table, qualifiers):
        """Scan *table* for a record with row *family* identifier
        that matches the given list of *qualifers*.

        **Args:**
            *table*: name of the table to scan

            *qualifers*: list of row column qualifer identifiers to scan

        **Returns:**
            lisf of Accumulo cells that match the search term criteria

        """
        results = []
        for qualifier in qualifiers:
            iterators = []
            kwargs = {'row_regex': '.*%s.*' % qualifier,
                      'match_substring': True,
                      'name': 'regex_row_scan'}
            iterators.append(pyaccumulo.iterators.RegExFilter(**kwargs))
            log.debug('RegExIterator kwargs: %s' % kwargs)

            results.extend(self.connection.scan(table=table,
                                                iterators=iterators))

        return results

    def batch_regex_scan(self, table, search_terms):
        """Combine the results of multiple
        :meth:`geoutils.ModelBase.regex_scan` calls into a single
        list construct.

        .. note::
            Given that potentially multiple scans are performed, ordering
            cannot be guaranteed

        **Args:**
            *table*: the name of the table to search.

            *search_terms*: dictionary structure of key value pairs
            that represent the family/qualifier identifiers as
            required by the :meth:`geoutils.ModelBase.regex_scan`
            parameter list

        **Returns:**
            list of unique

        """
        log.info('Regex scanning table "%s" against search terms: "%s" ...' %
                 (table, search_terms))

        batch_results = []
        for family, qualifiers in search_terms.iteritems():
            cells = collections.OrderedDict()

            results = self.regex_scan(table, family, qualifiers)
            for result in results:
                cells[result.row] = True

            batch_results.append(set(cells.keys()))

            # Provide some optimisation.  No need continuing scans if
            # the most recent one returns no results.
            if not cells.keys():
                log.debug('Sub-scan returned empty set: ending scans')
                break

        intersects = set.intersection(*batch_results)

        log.debug('Regex scanning result count: %d' % len(intersects))

        return list(intersects)
