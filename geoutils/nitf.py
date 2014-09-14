# pylint: disable=R0903
"""The :class:`geoutils.NITF` class is a specialisation of an
 :class:`geoutils.Standard` that can process NITF based files.

The most recent NITF specification can be obtained from
`<http://www.gwg.nga.mil/ntb/baseline/docs/2500c/index.html>`_

"""
__all__ = ["NITF"]

import geoutils


class NITF(geoutils.Standard):
    """:class:`geoutils.NITF`

    """
    def __init__(self, source_filename=None):
        super(NITF, self).__init__(source_filename=source_filename)
