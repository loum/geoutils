# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.ModelBase` is the base construct for the
Accumulo models.

"""
__all__ = ["ModelBase"]


class ModelBase(object):
    """TODO

    """
    _name = str()

    def __init__(self, name=None):
        if name is not None:
            self._name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
