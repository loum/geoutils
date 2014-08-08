# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Datastore` abstracts an Accumulo interface.

"""
__all__ = ["Datastore"]

import os

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

    def _create_writer(self):
        writer = None

        if self.connection is None:
            log.error('Writer error: Accumulo connection not detected.')
        else:
            writer = self.connection.create_batch_writer(self.image_table_name)

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

    def init_table(self, name=None):
        """Initialise the datastore table.

        **Kwargs:**
            *name*: override the name of the image table to delete

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

    def delete_table(self, name=None):
        """Remove an existing datastore table.

        .. note::

            Use with caution!

        **Kwargs:**
            *name*: override the name of the image table to delete

        """
        if name is not None:
            self.image_table_name = name

        status = False
        log.info('Deleting the image library table: "%s" ...' %
                 self.image_table_name)

        if self.connection is not None:
            if self.connection.table_exists(self.image_table_name):
                self.connection.delete_table(self.image_table_name)
                status = True
            else:
                log.error('Image table "%s" does not exist!' %
                          self.image_table_name)
        else:
            log.error('Connection state not detected. Table not deleted')

        log.info('Image library table "%s" deletion status: "%s"' %
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

    def ingest(self,
               metadata,
               image_stream=None,
               thumb_image_stream=None):
        """Ingest the metadata component into the datastore.

        **Args:**
            *metadata*: dictionary structure that represents the
            metadata component to ingest

            *image_stream*:
            *thumb_image_stream*: referenct to a
            :func:`osgeo.gdal.Dataset.GetRasterBand.ReadRaster` stream
            typically provided by :meth:`geoutils.GeoImage.extract_image`

        """
        row_id = None

        writer = self._create_writer()
        if writer is not None:
            row_id = metadata.get('file')
            if row_id is None:
                log.error('Unable to generate Row ID from source data')
            else:
                row_id = os.path.splitext(row_id)[0]
                log.debug('row id: %s' % row_id)

                mutation = pyaccumulo.Mutation(row_id)
                mutation.put(cf='file', cq=metadata.get('file'))
                mutation.put(cf='x_coord_size',
                             cq=metadata.get('x_coord_size'))
                mutation.put(cf='y_coord_size',
                             cq=metadata.get('y_coord_size'))
                mutation.put(cf='geogcs',
                             cq=metadata.get('geogcs'))

                # geoxform
                index = 0
                for value in metadata.get('geoxform'):
                    mutation.put(cf='geoxform=%d' % index, cq=repr(value))
                    index += 1

                # metadata
                for key, value in metadata.get('metadata').iteritems():
                    mutation.put(cf='metadata=%s' % key, cq=value)

                # Image.
                if image_stream is not None:
                    mutation.put(cf='image', val=image_stream())
                # Image thumb.
                if thumb_image_stream is not None:
                    mutation.put(cf='image', val=thumb_image_stream())

                writer.add_mutation(mutation)

            writer.close()

    def query_metadata(self, key, display=True):
        """Query the metadata component from the datastore.

        **Args:**
            *key*: at this time, *key* relates to the NITF file name
            (less the ``.ntf`` extension) that is used in the current
            schema as the Row ID component of the row key.

        **Kwargs:**
            *display*: write the results to STDOUT (default ``True``)

        **Returns:**
            the metadata component of *key*

        """
        log.info('Querying datastore against key: "%s" ...' % key)

        scan_range = pyaccumulo.Range(srow=key, erow=key)
        results = self.connection.scan(table=self.image_table_name,
                                       scanrange=scan_range,
                                       cols=[])
        results_count = 0
        for cell in results:
            results_count += 1
            if display:
                print(cell)

        log.info('Query key "%s" complete' % key)

        return results_count
