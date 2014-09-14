# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.IngestConfig` is the configuration parser for
the GeoUtils ingest facility.

"""
__all__ = ["IngestConfig"]


import geoutils
from geosutils.setter import set_scalar


class IngestConfig(geoutils.Config):
    """:class:`geoutils.IngestConfig` class.

    """
    _accumulo_host = 'localhost'
    _accumulo_port = 42425
    _accumulo_user = 'root'
    _accumulo_password = str()
    _namenode_host = 'localhost'
    _namenode_port = 50070
    _namenode_user = None
    _namenode_target_path = str()

    def __init__(self, config_file=None):
        """:class:`geoutils.IngestConfig` initialisation.

        """
        geoutils.Config.__init__(self, config_file)

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
    def namenode_host(self):
        return self._namenode_host

    @set_scalar
    def set_namenode_host(self, value):
        pass

    @property
    def namenode_port(self):
        return self._namenode_port

    @set_scalar
    def set_namenode_port(self, value):
        pass

    @property
    def namenode_user(self):
        return self._namenode_user

    @set_scalar
    def set_namenode_user(self, value):
        pass

    @property
    def namenode_target_path(self):
        return self._namenode_target_path

    @set_scalar
    def set_namenode_target_path(self, value):
        pass

    def parse_config(self):
        """Read config items from the configuration file.

        """
        geoutils.Config.parse_config(self)

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
                  {'section': 'hdfs_namenode',
                   'option': 'host',
                   'var': 'namenode_host'},
                  {'section': 'hdfs_namenode',
                   'option': 'port',
                   'var': 'namenode_port',
                   'cast_type': 'int'},
                  {'section': 'hdfs_namenode',
                   'option': 'user',
                   'var': 'namenode_user'},
                  {'section': 'hdfs_namenode',
                   'option': 'target_path',
                   'var': 'namenode_target_path'}]

        for kw in kwargs:
            self.parse_scalar_config(**kw)