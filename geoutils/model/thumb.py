# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Thumb` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Thumb"]

import geoutils


class Thumb(geoutils.ModelBase):
    """

    """
    _name = 'thumb_library'

    def __init__(self, name=None):
        """TODO

        """
        geoutils.ModelBase.__init__(self, name)
