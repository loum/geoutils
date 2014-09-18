# pylint: disable=R0902,R0903,R0904,C0111,W0142
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
    _namenode_host = None
    _namenode_port = 50070
    _namenode_user = None
    _namenode_target_path = None
    _threads = 5
    _inbound_dir = None
    _archive_dir = None
    _thread_sleep = 2.0
    _shards = 4

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
    def shards(self):
        return self._shards

    @set_scalar
    def set_shards(self, value):
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
                   'var': 'namenode_target_path'},
                  {'section': 'ingest',
                   'option': 'threads',
                   'var': 'threads',
                   'cast_type': 'int'},
                  {'section': 'ingest',
                   'option': 'inbound_dir',
                   'var': 'inbound_dir'},
                  {'section': 'ingest',
                   'option': 'archive_dir',
                   'var': 'archive_dir'},
                  {'section': 'ingest',
                   'option': 'thread_sleep',
                   'var': 'thread_sleep',
                   'cast_type': 'float'},
                  {'section': 'ingest',
                   'option': 'shards',
                   'var': 'shards',
                   'cast_type': 'int'}]

        for kwarg in kwargs:
            self.parse_scalar_config(**kwarg)
