# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Metadata` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Metadata"]

import json
import re

import geoutils
from geosutils.log import log


class Metadata(geoutils.ModelBase):
    """Metadata Accumulo datastore model.

    """
    _name = 'meta_library'
    _coord_cols = [['coord=0'], ['coord=1'], ['coord=2'], ['coord=3']]

    def __init__(self, connection, name=None):
        """Metadata model initialisation.

        **Kwargs:**
            *name*: override the name of the Metadata table

        """
        super(Metadata, self).__init__(connection, name)

    @property
    def coord_cols(self):
        return self._coord_cols

    @coord_cols.setter
    def coord_cols(self, value):
        del self._coord_cols[:]
        self._coord_cols = []
        self._coord_cols = value

    def query_metadata(self, key=None, jsonify=False):
        """Query the metadata component from the datastore.

        **Kwargs:**
            *jsonify*: return as a JSON string

            *key*: at this time, *key* relates to the NITF file name
            (less the ``.ntf`` extension) that is used in the current
            schema as the Row ID component of the row key.

            *display*: write the results to STDOUT (default ``True``)

        **Returns:**
            the metadata component of *key* as a Python dictionary
            structure or as a JSON string if *jsonify* argument is set.
            Structure is similar to the following"::

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
        msg = 'Query metadata table "%s"' % self.name
        if key is not None:
            msg = '%s against key "%s"' % (msg, key)
        log.info('%s ...' % msg)

        metas = {}
        results = self.query(self.name, key)

        pattern = re.compile('metadata=')
        for cell in results:
            if metas.get(cell.row) is None:
                metas[cell.row] = {'metadata': {}}

            # Strip off the leading 'metadata=' token.
            family = cell.cf
            (family, subs) = pattern.subn('', family, count=1)
            if subs == 1:
                metas[cell.row]['metadata'][family] = cell.cq
            else:
                metas[cell.row][family] = cell.cq

        log.info('Query key "%s" complete' % key)

        if jsonify:
            metas = json.dumps(metas)

        return metas

    def query_coords(self, jsonify=False):
        """Scan the metadata table for all family columns
        that match :attr:`geoutils.Datastore.coord_cols`.  Typically
        the family columns are of the form ``coord=?``.

        **Kwargs:**
            *jsonify*: return as a JSON string

        **Returns:**
            a list of 4 sets of (lists) of float lat/long values that
            represent the boundary coordinates of the image.  List
            construct is similar to the following::

                {
                    'i_3001a': [
                        [32.983055419800003, 84.999999864200007],
                        [32.983055419800003, 85.0002779135],
                        [32.983333469100003, 84.999999864200007],
                        [32.983333469100003, 85.0002779135]
                    ]
                }

        """
        log.debug('Scanning for image boundary coordinates ...')

        results = self.query(table=self.name,
                             cols=self.coord_cols)

        coords = {}
        for cell in results:
            (lat, lng) = cell.cq.split(',')
            if coords.get(cell.row) is not None:
                coords[cell.row].append([float(lat), float(lng)])
            else:
                coords[cell.row] = []
                coords[cell.row].append([float(lat), float(lng)])

        if jsonify:
            coords = json.dumps(coords)

        log.info('Image boundary coordinates scan complete')

        return coords
