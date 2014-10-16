# pylint: disable=R0903,C0111,R0902,W0142
"""The :class:`geoutils.StagerConfig` is the configuration parser for
the GeoUtils ingest staging facility.

"""
__all__ = ["StagerConfig"]


import geoutils
from geosutils.setter import set_scalar


class StagerConfig(geoutils.Config):
    """:class:`geoutils.StagerConfig` class.

    """
    _inbound_dir = None

    def __init__(self, config_file=None):
        """:class:`geoutils.StagerConfig` initialisation.

        """
        geoutils.Config.__init__(self, config_file)

    @property
    def inbound_dir(self):
        return self._inbound_dir

    @set_scalar
    def set_inbound_dir(self, value):
        pass

    def parse_config(self):
        """Read config items from the configuration file.

        """
        geoutils.Config.parse_config(self)

        kwargs = [{'section': 'ingest',
                   'option': 'inbound_dir'}]

        for kwarg in kwargs:
            self.parse_scalar_config(**kwarg)
