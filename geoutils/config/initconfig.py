# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.InitConfig` is the configuration parser for
the GeoUtils ingest facility.

"""
__all__ = ["InitConfig"]


import geoutils
from geosutils.config import Config
from geosutils.setter import set_scalar


class InitConfig(Config):
    """:class:`geoutils.InitConfig` class.

    """
    _accumulo_host = 'localhost'
    _accumulo_port = 42425
    _accumulo_user = 'root'
    _accumulo_password = str()

    def __init__(self, config_file=None):
        """:class:`geoutils.InitConfig` initialisation.

        """
        Config.__init__(self, config_file)

    @property
    def accumulo_host(self):
        return self._accumulo_host

    @set_scalar
    def set_accumulo_host(self, value):
        pass

    @property
    def accumulo_port(self):
        return self._accumulo_port

    @set_scalar
    def set_accumulo_port(self, value):
        pass

    @property
    def accumulo_user(self):
        return self._accumulo_user

    @set_scalar
    def set_accumulo_user(self, value):
        pass

    @property
    def accumulo_password(self):
        return self._accumulo_password

    @set_scalar
    def set_accumulo_password(self, value):
        pass

    def parse_config(self):
        """Read config items from the configuration file.

        """
        Config.parse_config(self)

        kwargs = [{'section': 'accumulo_proxy_server',
                   'option': 'host',
                   'var': 'accumulo_host'},
                  {'section': 'accumulo_proxy_server',
                   'option': 'port',
                   'var': 'accumulo_port',
                   'cast_type': 'int'},
                  {'section': 'accumulo_proxy_server',
                   'option': 'user',
                   'var': 'accumulo_user'},
                  {'section': 'accumulo_proxy_server',
                   'option': 'password',
                   'var': 'accumulo_password'}]

        for kw in kwargs:
            self.parse_scalar_config(**kw)
