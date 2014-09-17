# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Datastore` abstracts an Accumulo interface.

"""
__all__ = ["Datastore"]

import pyaccumulo
from thrift.transport.TTransport import TTransportException
from pyaccumulo.proxy.AccumuloProxy import AccumuloSecurityException

import geoutils.model
from geosutils.log import log


class Datastore(object):
    """:class:`geoutils.Datastore`

    .. note::

        The :meth:`geoutils.Datastore.ingest_metadata` method
        hardwires schema information into the mutations.  This is
        probably not a good idea.  Ideally, this should be abstracted
        into a special schema module.

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
        Password credential of the Accumulo proxy host connection
        (defaults to the empty string)

    .. attribute:: *coord_cols*
        List of family column names to extract during the
        metadata table scan for the image coordinate boundaries

    .. attribute:: *base*
        an object instance of a :class:`geoutils.model.Base`

    """
    _connection = None
    _host = 'localhost'
    _port = 42425
    _user = 'root'
    _password = ''
    _meta = geoutils.model.Metadata(None)
    _image = geoutils.model.Image(None)
    _thumb = geoutils.model.Thumb(None)

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
    def meta(self):
        return self._meta

    @property
    def image(self):
        return self._image

    @property
    def thumb(self):
        return self._thumb

    def __del__(self):
        self.close()

    def _create_writer(self, table):
        writer = None

        if self.connection is None:
            log.error('Writer error: Accumulo connection not detected.')
        else:
            log.debug('Creating writer for table "%s"' % table)
            writer = self.connection.create_batch_writer(table)

        return writer

    def connect(self):
        """Connect to the Accumulo datastore via a proxy client.

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
                  (self.user, '********', self.host, self.port))
        try:
            self.connection = pyaccumulo.Accumulo(host=self.host,
                                                  port=self.port,
                                                  user=self.user,
                                                  password=self.password)
            self.meta.connection = self.connection
            self.image.connection = self.connection
            self.thumb.connection = self.connection
        except (TTransportException,
                AccumuloSecurityException) as err:
            log.error('Connection error: "%s"' % err)

        return self.connection

    def init_table(self, name):
        """Initialise the datastore table.

        **Kwargs:**
            *name*: override the name of the image table to delete

        """
        status = False
        log.info('Initialising the image library table: "%s" ...' % name)

        if self.connection is not None:
            if self.exists_table(name):
                log.error('Image table "%s" already exists!' % name)
            else:
                # Finally, create the table.
                self.connection.create_table(name)
                status = True
        else:
            log.error('Connection state not detected. Table not created')

        log.info('Image library table "%s" creation status: "%s"' %
                 (name, status))

        return status

    def delete_table(self, name):
        """Remove an existing datastore table.

        .. note::

            Use with caution!

        **Kwargs:**
            *name*: override the name of the image table to delete

        """
        status = False
        log.info('Deleting the image library table: "%s" ...' % name)

        if self.connection is not None:
            if self.exists_table(name):
                self.connection.delete_table(name)
                status = True
            else:
                log.error('Image table "%s" does not exist!' % name)
        else:
            log.error('Connection state not detected. Table not deleted')

        log.info('Image library table "%s" deletion status: "%s"' %
                 (name, status))

        return status

    def exists_table(self, name):
        """Check if table *name* exists in the datastore.

        **Kwargs:**
            *name*: override the name of the image table to delete

        """
        log.info('Checking image library table: "%s" ...' % name)
        status = False

        if self.connection is not None:
            if self.connection.table_exists(name):
                status = True
        else:
            log.error('Connection not detected: table state undefined ')

        log.info('Image library table "%s" exists?: "%s"' %
                 (name, status))

        return status

    def close(self):
        """Attempt to close the :attr:`geoutils.Datastore.connection`
        state.

        """
        if self.connection is not None:
            log.info('Closing proxy client connection ...')
            self.connection.close()
            log.info('Proxy client connection closed')

    def ingest(self, data, dry=False):
        """Write a record to the Accumulo datastore.

        **Args:**
            *data*: dictionary object of the data to ingest

        **Kwargs:**
            *dry*: if ``True`` only simulate, do not execute

        **Returns:**
            Boolean ``True`` on successful record creation.  Boolean
            ``False`` otherwise

        """
        log.info('Ingesting data ...')
        ingest_status = False

        row_id = data.get('row_id')
        if row_id is None:
            log.error('Ingest error: no "row_id" defined')
        else:
            for table, value in data.get('tables').iteritems():
                log.info('Processing ingest for table: "%s"' % table)
                if not self.exists_table(table):
                    log.info('Ingest skipped')
                    continue

                writer = self._create_writer(table)
                if writer is None:
                    break

                log.info('Creating mutation for Row ID: "%s"' % row_id)
                mutation = pyaccumulo.Mutation(row_id)

                family_qualifiers = value.get('cf').get('cq')
                self._ingest_family_qualifiers(family_qualifiers,
                                               mutation)

                family_values = value.get('cf').get('val')
                self._ingest_family_values(family_values, mutation)

                if not dry:
                    writer.add_mutation(mutation)
                else:
                    log.info('Dry pass: mutation skipped')
                writer.close()

                ingest_status = True

        log.info('Data ingestion complete')

        return ingest_status

    def _ingest_family_qualifiers(self, family_qualifiers, mutation):
        if family_qualifiers is not None:
            log.debug('Processing family|qualifiers ...')
            for key, val in family_qualifiers.iteritems():
                log.debug('Mutation: cf|cq: %s|%s' % (key, val))
                mutation.put(cf=key, cq=val)
            log.debug('family|qualifiers ingest component done')

    def _ingest_family_values(self, family_values, mutation):
        if family_values is not None:
            log.debug('Processing family|value ...')
            for key, val in family_values.iteritems():
                log.debug('Mutation: cf|val: %s|%s' % (key, val))
                if callable(val):
                    mutation.put(cf=key, val=val())
                else:
                    mutation.put(cf=key, val=val)
            log.debug('family|value ingest component done')
