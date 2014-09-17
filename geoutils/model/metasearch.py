# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Metasearch` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Metasearch"]

import json
import re

import geoutils
from geosutils.log import log


class Metasearch(geoutils.ModelBase):
    """Metasearch Accumulo datastore model.

    """
    _name = 'meta_search'

    def __init__(self, connection, name=None):
        """Metasearch model initialisation.

        **Kwargs:**
            *name*: override the name of the Metasearch table

        """
        super(Metasearch, self).__init__(connection, name)

    def query_metadata(self, search_terms):
        """Query the metadata component from the datastore.

        **Kwargs:**
            *search_terms*:

        **Returns:**
            the metadata component of *key* as a Python dictionary
            structure

                {'i_3001a': {
                    'coord=3': '32.9833334691,85.0002779135',
                    'coord=1': '32.9833334691,84.9999998642',
                    ...
                    'metadata': {
                        'NITF_IDATIM': '19961217102630',
                        'NITF_FSCLAS': 'U',
                        'NITF_ISDGDT': '',
                        ...}}}

        """
        terms = [term.lower() for term in search_terms if len(term) > 3]
        results = self.doc_query(self.name, terms)

        for result in results:
            log.info('result: %s' % result)

        return results
