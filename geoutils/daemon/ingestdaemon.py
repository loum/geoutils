# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Datastore` abstracts an Accumulo interface.

"""
__all__ = ["IngestDaemon"]

import sys
import signal
import time

import geoutils
import daemoniser
from geosutils.files import get_directory_files
from geosutils.log import log


class IngestDaemon(daemoniser.Daemon):
    """:class:`IngestDaemon`

    """
    dry = False
    batch = False
    conf = None
    loop = 5

    def __init__(self,
                 pidfile,
                 filename=None,
                 dry=False,
                 batch=False,
                 conf=None):
        """:class:`IngestDaemon` initialisation.

        """
        super(IngestDaemon, self).__init__(pidfile=pidfile)

        self.filename = filename
        self.dry = dry
        self.batch = batch
        self.conf = conf

        # If a file is provided on the command line, we want to
        # force a single iteration.
        if self.filename is not None:
            self.batch = True

    def _start(self, event):
        """Override the :meth:daemoniser.Daemon._start` method.

        """
        signal.signal(signal.SIGTERM, self._exit_handler)

        self.accumulo = self.accumulo_connect()

        file_to_process = None
        if self.filename is not None:
            file_to_process = self.filename

        while not event.isSet():
            if file_to_process is not None:
                self.ingest(file_to_process, dry=self.dry)

            if self.dry:
                print('Dry run iteration complete -- aborting')
                event.set()
            elif self.batch:
                print('Batch run iteration complete -- aborting')
                event.set()
            else:
                time.sleep(self.loop)

    def accumulo_connect(self):
        """Create a connection to the Accumulo datastore defined
        within the environment's setting.py file.

        """
        datastore = geoutils.Datastore()
        datastore.host = self.conf.accumulo_host
        datastore.port = self.conf.accumulo_port
        datastore.user = self.conf.accumulo_user
        datastore.password = self.conf.accumulo_password

        datastore.connect()

        return datastore

    def ingest(self, filename, dry=False):
        """Ingest *filename* into the Accumulo datatore.

        **Args:**
            *filename*: absolute path to the file to ingest

        **Kwargs:**
            *dry*: if ``True`` only simulate, do not execute

        """
        nitf = geoutils.NITF(source_filename=filename)
        nitf.image_model.hdfs_namenode = self.conf.namenode_user
        nitf.image_model.hdfs_namenode_port = self.conf.namenode_port
        nitf.image_model.hdfs_namenode_user = self.conf.namenode_user
        nitf.open()
        hdfs_target_path = self.conf.namenode_target_path
        self.accumulo.ingest(nitf(target_path=hdfs_target_path, dry=dry),
                             dry=dry)

    def source_file(self):
        """Checks inbound directory (defined by the
        :attr:`geoutils.IngestConfig.inbound_dir` config option) for valid
        NITF files to be processed.

        **Returns:**
            if a NITF file has been detected, the absolute path to the
            file.  Otherwise ``None``

        """
        file_to_process = None

        log.debug('Looking for files at: %s ...' % self.conf.inbound_dir)
        for file_match in get_directory_files(self.conf.inbound_dir,
                                              file_filter='.*\.ntf$'):
            file_to_process = file_match
            log.info('File to process: "%s"' % file_to_process)
            break

        return file_to_process
