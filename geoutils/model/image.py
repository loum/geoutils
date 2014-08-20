# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Image` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Image"]

import geoutils


class Image(geoutils.ModelBase):
    """

    """
    _name = 'image_library'

    def __init__(self, name=None):
        """TODO

        """
        geoutils.ModelBase.__init__(self, name)
