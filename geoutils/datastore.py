# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Datastore` abstracts an Accumulo interface.

"""
__all__ = ["Datastore"]

import pyaccumulo
from thrift.transport.TTransport import TTransportException
from pyaccumulo.proxy.AccumuloProxy import AccumuloSecurityException

from oct.utils.log import log


class Datastore(object):
    """:class:`geoutils.Metadata`

    .. attribute:: *connection*
        Handles the Accumulo connection state.  Upon successful
        connection, will hold a :class:`pyaccumulo.Accumulo` object
        (``None`` otherwise)

    .. attribute:: *host*
        Hostname of the Accumulo proxy host to connect to (default
        ``localhost``)

    .. attribute:: *port*
        Port of the Accumulo proxy host to connect to (default 42425)

    .. attribute:: *user*
        Username credential of the Accumulo proxy host connection
        (default ``root``)

    .. attribute:: *password*
        Password credential of the Accumulo proxy host conneciton
        (defaults to the empty string)

    .. attribute:: *image_table_name*
        Accumulo table name of the image library (defaults to
        ``image_library``)

    """
    _connection = None
    _host = 'localhost'
    _port = 42425
    _user = 'root'
    _password = ''
    _image_table_name = 'image_library'

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value):
        self._port = value

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        self._password = value

    @property
    def image_table_name(self):
        return self._image_table_name

    @image_table_name.setter
    def image_table_name(self, value):
        self._image_table_name = value

    def __del__(self):
        self.close()

    def connect(self):
        """Connect to the Accumulo datastore.

        Will use the :attr:`geoutils.Datastore.user`,
        :attr:`geoutils.Datastore.password`,
        :attr:`geoutils.Datastore.host` and
        :attr:`geoutils.Datastore.password` attributes to attempt a
        connection.

        On successful connection to the Accumulo proxy, will set
        the :attr:`geoutils.Datastore.connection` attribute with
        the connection state (a :class:`pyaccumulo.Accumulo` object)

        """
        log.info('Attempting connection to Accumulo proxy ...')
        log.debug('Connection args: "%s:%s@%s:%s"' %
                  #(self.user, '********', self.host, self.port))
                  (self.user, self.password, self.host, self.port))
        try:
            self.connection = pyaccumulo.Accumulo(host=self.host,
                                                  port=self.port,
                                                  user=self.user,
                                                  password=self.password)
        except (TTransportException,
                AccumuloSecurityException) as err:
            log.error('Connection error: "%s"' % err)

    def init_table(self, name=None):
        """Initialise the datastore table.
        """
        if name is not None:
            self.image_table_name = name

        status = False
        log.info('Initialising the image library table: "%s" ...' %
                 self.image_table_name)

        if self.connection is not None:
            if self.connection.table_exists(self.image_table_name):
                log.error('Image table "%s" already exists!' %
                        self.image_table_name)
            else:
                # Finally, create the table.
                self.connection.create_table(self.image_table_name)
                status = True
        else:
            log.error('Connection state not detected. Table not created')

        log.info('Image library table "%s" creation status: "%s"' %
                 (self.image_table_name, status))

        return status

    def close(self):
        """Attempt to close the :attr:`geoutils.Datastore.connection`
        state.

        """
        if self.connection is not None:
            log.info('Closing datastore connection ...')
            self.connection.close()
            log.info('Datastore connection closed.')
