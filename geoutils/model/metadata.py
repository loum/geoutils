# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Metadata` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Metadata"]

import geoutils


class Metadata(geoutils.ModelBase):
    """

    """
    _name = 'meta_library'

    def __init__(self, name=None):
        """TODO

        """
        geoutils.ModelBase.__init__(self, name)
