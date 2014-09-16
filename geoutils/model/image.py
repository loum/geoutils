# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.model.Image` abstracts an Accumulo metadata
table schema.

"""
__all__ = ["Image"]

import tempfile
import os
import requests
from pywebhdfs.webhdfs import PyWebHdfsClient

import geoutils
from geosutils.log import log
from geosutils.files import move_file


class Image(geoutils.ModelBase):
    """Image Accumulo datastore model.

    .. attribute:: hdfs_namenode
        HDFS NameNode hostname

    .. attribute:: hdfs_namenode_port
        HDFS NameNode port (defaults to 50070)

    .. attribute:: hdfs
        object reference to :class:`pywebhdfs.PyWebHdfsClient` instance

    """
    _name = 'image_library'
    _hdfs_namenode = None
    _hdfs_namenode_port = 50070
    _hdfs_namenode_user = None
    _hdfs = None

    def __init__(self, connection, name=None):
        """:class:`geoutils.model.Image models the Accumulo
        ``image_library`` table.

        """
        super(Image, self).__init__(connection, name)

    @property
    def hdfs_namenode(self):
        return self._hdfs_namenode

    @hdfs_namenode.setter
    def hdfs_namenode(self, value):
        self._hdfs_namenode = value

    @property
    def hdfs_namenode_port(self):
        return self._hdfs_namenode_port

    @hdfs_namenode_port.setter
    def hdfs_namenode_port(self, value):
        self._hdfs_namenode_port = value

    @property
    def hdfs_namenode_user(self):
        return self._hdfs_namenode_user

    @hdfs_namenode_user.setter
    def hdfs_namenode_user(self, value):
        self._hdfs_namenode_user = value

    @property
    def hdfs(self):
        if self._hdfs is None:
            log.info('Creating HDFS connection to "%s:%s"' %
                     (self.hdfs_namenode, self.hdfs_namenode_port))
            hdfs = PyWebHdfsClient(host=self.hdfs_namenode,
                                   port=str(self.hdfs_namenode_port),
                                   user_name=self.hdfs_namenode_user)
            self._hdfs = hdfs

        return self._hdfs

    def query_image(self, key, img_format='JPEG'):
        """Query the metadata component from the datastore.

        **Args:**
            *key*: at this time, *key* relates to the NITF file name
            (less the ``.ntf`` extension) that is used in the current
            schema as the Row ID component of the row key.

        **Returns:**
            the image component of *key*

        """
        log.info('Retrieving image for row_id "%s" ...' % key)

        cells = self.query(self.name, key)

        image_fh = tempfile.NamedTemporaryFile('w+b')
        x_coord = y_coord = None
        for cell in cells:
            if cell.cf == 'x_coord_size':
                x_coord = cell.cq
            elif cell.cf == 'y_coord_size':
                y_coord = cell.cq
            elif cell.cf == 'image':
                image_fh.write(cell.val)

        log.debug('X|Y: %s|%s' % (x_coord, y_coord))
        dimensions = (int(x_coord), int(y_coord))

        suffix = 'jpg'
        if img_format == 'PNG':
            suffix = 'png'

        temp_obj = tempfile.NamedTemporaryFile(mode='w+b',
                                               suffix=suffix)
        image_fh.seek(0)
        log.debug('Reconstructing image to format "%s"' % img_format)
        geoutils.GeoImage.reconstruct_image(image_fh.read,
                                            dimensions).save(temp_obj,
                                                             img_format)

        temp_obj.seek(0)

        return temp_obj

    def hdfs_write(self, filename, target_path=None, dry=False):
        """Wrapper around the :mod:`pywebhdfs` module which is a Python
        wrapper for the Hadoop WebHDFS REST API.

        Takes the local *file* and writes it to the HDFS-based
        filesystem defined by *:attr:`host`*:*:attr:`port`*.

        **Args:**
            *filename*: full path to the source file on the local
            filesystem that will be created on the target HDFS filesystem

        **Kwargs:**
            *target_path*: directory structure that can be prepended to
            the destination file path.  Defaults to ``None`` which means
            current directory of target device.

            *dry*: only report, do not execute

        **Returns:**
            The URI scheme of the written file on success or ``None``
            otherwise.  Some successful examples include::

                file:///tmp/i_3001a.ntf
                hdfs://jp2044lm-hdfs-nn01/tmp/i_3001a.ntf

        """
        status = False
        cleansed_file = filename

        # Strip off the trailing ".proc"
        if filename.endswith('.proc'):
            cleansed_file = os.path.splitext(filename)[0]

        # Work out our target path.
        target = None
        if target_path is None:
            target = os.path.basename(cleansed_file)
            # Strip the leading filesystem separator as this format
            # is required by Hadoop WebHDFS REST API.
            target = target.strip(os.sep)
        else:
            target = os.path.join(target_path,
                                  os.path.basename(cleansed_file))
        log.debug('Target path: "%s"' % target)

        # OK, perform the write.
        uri_scheme = None
        if self.hdfs_namenode is not None:
            log.info('Writing "%s" to HDFS ...' % filename)
            with open(filename) as file_data:
                try:
                    uri_scheme = 'hdfs'
                    if not dry:
                        status = self.hdfs.create_file(target, file_data)
                    else:
                        # Simulate the write.
                        status = True
                except requests.ConnectionError as err:
                    log.error('HDFS connection error: %s' % err)
        else:
            log.info('Writing "%s" to local filesystem' % cleansed_file)
            uri_scheme = 'file'
            status = move_file(filename, target, dry=dry)

        uri = None
        if status:
            if self.hdfs_namenode is not None:
                uri = ('%(scheme)s:%(sep)s%(sep)s%(host)s%(sep)s' %
                       {'scheme': uri_scheme,
                        'sep': os.sep,
                        'host': self.hdfs_namenode})
                uri = '%s%s' % (uri, target)
            else:
                uri = ('%(scheme)s:%(sep)s%(sep)s%(target)s' %
                       {'scheme': uri_scheme,
                        'sep': os.sep,
                        'target': target})

        return uri
