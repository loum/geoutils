# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Datastore` abstracts an Accumulo interface.

"""
__all__ = ["Datastore"]

import Image

import pyaccumulo
from thrift.transport.TTransport import TTransportException
from pyaccumulo.proxy.AccumuloProxy import AccumuloSecurityException

from oct.utils.log import log


class Datastore(object):
    """:class:`geoutils.Metadata`

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

    .. attribute:: *meta_table_name*
        Accumulo table name of the image metadat library (defaults to
        ``meta_library``)

    .. attribute:: *image_table_name*
        Accumulo table name of the image library (defaults to
        ``image_library``)

    """
    _connection = None
    _host = 'localhost'
    _port = 42425
    _user = 'root'
    _password = ''
    _meta_table_name = 'meta_library'
    _image_table_name = 'image_library'
    _thumb_table_name = 'thumb_library'

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
    def meta_table_name(self):
        return self._meta_table_name

    @meta_table_name.setter
    def meta_table_name(self, value):
        self._meta_table_name = value

    @property
    def image_table_name(self):
        return self._image_table_name

    @image_table_name.setter
    def image_table_name(self, value):
        self._image_table_name = value

    @property
    def thumb_table_name(self):
        return self._thumb_table_name

    @thumb_table_name.setter
    def thumb_table_name(self, value):
        self._thumb_table_name = value

    def __del__(self):
        self.close()

    def _create_writer(self, table):
        writer = None

        if self.connection is None:
            log.error('Writer error: Accumulo connection not detected.')
        else:
            log.debug('Creating writer for table "%s"' %table)
            writer = self.connection.create_batch_writer(table)

        return writer

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

    def init_table(self, name):
        """Initialise the datastore table.

        **Kwargs:**
            *name*: override the name of the image table to delete

        """
        status = False
        log.info('Initialising the image library table: "%s" ...' %
                 self.image_table_name)

        if self.connection is not None:
            if self.connection.table_exists(name):
                log.error('Image table "%s" already exists!' %
                        self.image_table_name)
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
            if self.connection.table_exists(name):
                self.connection.delete_table(name)
                status = True
            else:
                log.error('Image table "%s" does not exist!' % name)
        else:
            log.error('Connection state not detected. Table not deleted')

        log.info('Image library table "%s" deletion status: "%s"' %
                 (name, status))

        return status

    def close(self):
        """Attempt to close the :attr:`geoutils.Datastore.connection`
        state.

        """
        if self.connection is not None:
            log.info('Closing datastore connection ...')
            self.connection.close()
            log.info('Datastore connection closed.')

    def ingest(self, data):
        """TODO

        """
        log.info('Ingesting data ...')

        row_id = data.get('row_id')
        if row_id is None:
            log.error('Ingest error: no "row_id" defined')
        else:
            for table, value in data.get('tables').iteritems():
                log.debug('Processing ingest for table: "%s"' % table)
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

                writer.add_mutation(mutation)
                writer.close()

        log.info('Data ingestion complete')

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

    def query_metadata(self, table, key, display=True):
        """Query the metadata component from the datastore.

        **Args:**
            *table*: TODO

            *key*: at this time, *key* relates to the NITF file name
            (less the ``.ntf`` extension) that is used in the current
            schema as the Row ID component of the row key.

        **Kwargs:**
            *display*: write the results to STDOUT (default ``True``)

        **Returns:**
            the metadata component of *key*

        """
        log.info('Querying datastore table "%s" against key: "%s" ...' %
                 (self.image_table_name, key))

        scan_range = pyaccumulo.Range(srow=key, erow=key)
        results = self.connection.scan(table=table,
                                       scanrange=scan_range,
                                       cols=[])
        results_count = 0
        for cell in results:
            results_count += 1
            if display:
                print(cell)

        log.info('Query key "%s" complete' % key)

        return results_count

    def query(self, table, key):
        """TODO

        """

        log.info('Querying datastore table "%s" against key: "%s" ...' %
                 (self.image_table_name, key))

        scan_range = pyaccumulo.Range(srow=key, erow=key)
        results = self.connection.scan(table=table,
                                       scanrange=scan_range,
                                       cols=[])

        return results

    def query_image(self, table, key):
        """Query the metadata component from the datastore.

        **Args:**
            *table*: TODO

            *key*: at this time, *key* relates to the NITF file name
            (less the ``.ntf`` extension) that is used in the current
            schema as the Row ID component of the row key.

        **Returns:**
            the image component of *key*

        """
        return self.query(table, key)

    def reconstruct_image(self, image_stream, dimensions):
        """Reconstruct a 1D stream to an image file.

        **Args:**
            *image_stream*: ``string`` type stream of raw binary
            floating point data that is a 1D representation of an
            image file.  This is typically the format that is stored
            in the Accumulo image library

            *dimensions*: tuple structure that represents the rows
            and columns of the 1D image.  This information is used
            to recreate the original 2D structure of the image

        **Returns:**
            A :mod:`Image` image in memory from pixel data provided
            by the *image_stream*

        """
        log.debug('Reconstructing image with dimensions "%s"' %
                  str(dimensions))

        return Image.frombuffer('L',
                                dimensions,
                                image_stream(),
                                'raw',
                                'L', 0, 1)
