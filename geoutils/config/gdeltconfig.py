# pylint: disable=R0902,R0903,R0904,C0111,W0142
"""The :class:`geoutils.GdeltConfig` is the configuration parser for
the GeoUtils GDELT ingest facility.

"""
__all__ = ["GdeltConfig"]


from geosutils.config import Config
from geosutils.setter import (set_scalar,
                              set_list)


class GdeltConfig(Config):
    """:class:`geoutils.GdeltConfig` class.

    """
    _accumulo_host = 'localhost'
    _accumulo_port = 42425
    _accumulo_user = 'root'
    _accumulo_password = str()
    _threads = 5
    _inbound_dir = None
    _archive_dir = None
    _thread_sleep = 2.0
    _spatial_order = ['stripe', 'geohash', 'reverse_time']
    _spatial_stripes = 1
    _stripes = 1

    def __init__(self, config_file=None):
        """:class:`geoutils.GdeltConfig` initialisation.

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

    @property
    def threads(self):
        return self._threads

    @set_scalar
    def set_threads(self, value):
        pass

    @property
    def inbound_dir(self):
        return self._inbound_dir

    @set_scalar
    def set_inbound_dir(self, value):
        pass

    @property
    def archive_dir(self):
        return self._archive_dir

    @set_scalar
    def set_archive_dir(self, value):
        pass

    @property
    def thread_sleep(self):
        return self._thread_sleep

    @set_scalar
    def set_thread_sleep(self, value):
        pass

    @property
    def spatial_order(self):
        return self._spatial_order

    @set_list
    def set_spatial_order(self, values=None):
        pass

    @property
    def spatial_stripes(self):
        return self._spatial_stripes

    @set_scalar
    def set_spatial_stripes(self, value):
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
                   'var': 'accumulo_password'},
                  {'section': 'gdelt',
                   'option': 'threads',
                   'var': 'threads',
                   'cast_type': 'int'},
                  {'section': 'gdelt',
                   'option': 'inbound_dir',
                   'var': 'inbound_dir'},
                  {'section': 'gdelt',
                   'option': 'archive_dir',
                   'var': 'archive_dir'},
                  {'section': 'gdelt',
                   'option': 'thread_sleep',
                   'var': 'thread_sleep',
                   'cast_type': 'float'},
                  {'section': 'spatial',
                   'option': 'order',
                   'var': 'spatial_order',
                   'is_list': True},
                  {'section': 'spatial',
                   'option': 'stripes',
                   'var': 'spatial_stripes',
                   'cast_type': 'int'}]

        for kwarg in kwargs:
            self.parse_scalar_config(**kwarg)
