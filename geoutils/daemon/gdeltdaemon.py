# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.GdeltDaemon` supports the daemonisation
facility for the geogdelt

"""
__all__ = ["GdeltDaemon"]

import os
import signal
import time
import sys
from multiprocessing import Process
import zipfile

import geoutils
import daemoniser
from geosutils.files import (get_directory_files,
                             move_file)
from geosutils.log import log
from geosutils.utils import get_reverse_timestamp
from geoutils.auditer import audit


class GdeltDaemon(daemoniser.Daemon):
    """:class:`GdeltDaemon`

    """
    dry = False
    batch = False
    conf = None
    delete = False
    accumulo = None

    def __init__(self,
                 pidfile,
                 filename=None,
                 dry=False,
                 batch=False,
                 conf=None,
                 delete=False):
        """:class:`GdeltDaemon` initialisation.

        """
        super(GdeltDaemon, self).__init__(pidfile=pidfile)

        self.filename = filename
        self.dry = dry
        self.batch = batch
        self.conf = conf
        self.delete = delete

        # If a file is provided on the command line, we want to
        # force a single iteration.
        if self.filename is not None:
            self.batch = True

    def _start(self, event):
        """Override the :meth:daemoniser.Daemon._start` method.

        """
        signal.signal(signal.SIGTERM, self._exit_handler)

        file_to_process = None
        if self.filename is not None:
            file_to_process = self.filename
            if os.path.exists(file_to_process):
                self.process(event, file_to_process)
            else:
                log.warn('Source "%s" does not exist' % file_to_process)
        else:
            child_pids = []
            for thread_count in range(self.conf.threads):
                log.debug('Starting child thread %d of %d' %
                          (thread_count + 1, self.conf.threads))
                proc = Process(target=self.process, args=(event, ))
                proc.start()

                if self.dry:
                    # Block until the thread completes.
                    proc.join()

                child_pids.append(proc.pid)

            if not self.dry and not self.batch:
                while not event.isSet():
                    time.sleep(1)

                for proc in child_pids:
                    os.kill(proc, signal.SIGTERM)

    def process(self, event, file_to_process=None):
        """Imgest thread wrapper.  Each call to this method is
        effectively a GDELT ingest process.

        **Args:**
            *event*: a :mod:`threading.Event` based internal semaphore
            that can be set via the :mod:`signal.signal.SIGTERM` signal
            event to perform a function within the running proess.

        **Kwargs:**
            *file_to_process* override the file to process (will bypass
            a file system search)

        """
        self.accumulo = self.accumulo_connect()
        if self.accumulo.connection is None:
            log.fatal('Datastore connection not detected: aborting')
            sys.exit(1)

        while not event.isSet():
            skip_sleep = False
            if file_to_process is None:
                file_to_process = self.source_file()

            if file_to_process is not None:
                # Don't sleep if there are files to process.
                skip_sleep = True

                if (self.ingest(file_to_process, dry=self.dry) and
                    self.delete and not self.dry):
                    log.info('Deleting file: %s' %
                             file_to_process + '.proc')
                    try:
                        os.remove(file_to_process + '.proc')
                    except OSError as error:
                        log.error(error)

            if self.dry:
                print('Dry run iteration complete')
                event.set()
            elif self.batch:
                print('Batch run iteration complete')
                event.set()
            else:
                file_to_process = None
                if not skip_sleep:
                    time.sleep(self.conf.thread_sleep)

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

        **Returns:**
            Boolean ``True`` on successful record creation.  Boolean
            ``False`` otherwise

        """
        status = False

        # First, move the file into a "processing" state.
        proc_file = filename + '.proc'
        if move_file(filename, proc_file):
            gdelt_zip = zipfile.ZipFile(proc_file, 'r')
            for zip_filename in gdelt_zip.namelist():
                log.debug ('Processing GDELT file "%s"' % zip_filename)
                audit.data = {'gdelt_daemon|start': str(time.time())}

                file_h = gdelt_zip.open(zip_filename, 'r')
                for line in file_h:
                    event_data = line.strip()

                    # Only interested in the EVENT GEOGRAPHY
                    event_data = event_data.split(',')
                    if len(event_data) > 4:
                        data = geoutils.Gdelt(event_data[4])
                        log.debug('GDELT: %s' % data.latitude)
                    else:
                        continue

                    status = self.accumulo.ingest(data, dry=dry)

                    audit.data = {'gdelt_daemon|finish': str(time.time())}
                    audit.data = {'gdelt_daemon|row_id': data['row_id']}
                    audit.source_id = ('%s_gdelt_daemon' %
                                        get_reverse_timestamp())
                    self.accumulo.ingest(audit())
                    audit.reset()

            # In dry mode we need to restore the file.
            if dry:
                move_file(proc_file, filename)

        return status

    def source_file(self):
        """Checks inbound directory (defined by the
        :attr:`geoutils.GdeltConfig.inbound_dir` config option) for valid
        gzipped GDELT files to be processed.

        **Returns:**
            if a GDELT file has been detected, the absolute path to the
            file.  Otherwise ``None``

        """
        file_to_process = None

        log.debug('Sourcing files at: %s' % self.conf.inbound_dir)
        gdelt_filter = '.*\.export.CSV.zip'
        for file_match in get_directory_files(self.conf.inbound_dir,
                                              file_filter=gdelt_filter):
            file_to_process = file_match
            log.info('File to process: "%s"' % file_to_process)
            break

        return file_to_process
