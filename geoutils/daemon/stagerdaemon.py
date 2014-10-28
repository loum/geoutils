# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.StagerDaemon` supports the daemonisation
facility for the geostager.

"""
__all__ = ["StagerDaemon"]

import signal
import daemoniser
import os
import time
import random
import string

from geosutils.log import log
from geosutils.files import (copy_file,
                             get_directory_files_list)


class StagerDaemon(daemoniser.Daemon):
    """:class:`StagerDaemon`

    """
    dry = False
    batch = False
    conf = None
    fixture_dir = None
    files_to_process = []
    regen_count = 10
    regen_sleep = 1.0

    def __init__(self,
                 pidfile,
                 dry=False,
                 conf=None,
                 fixture_dir=None,
                 regen_count=10,
                 regen_sleep=1):
        """:class:`StagerDaemon` initialisation.

        """
        super(StagerDaemon, self).__init__(pidfile=pidfile)

        self.dry = dry
        self.conf = conf
        self.fixture_dir = fixture_dir
        self.regen_count = regen_count
        self.regen_sleep = regen_sleep

    def _start(self, event):
        """Override the :meth:daemoniser.Daemon._start` method.

        """
        signal.signal(signal.SIGTERM, self._exit_handler)

        self.source_files()

        self.process(event)

    def process(self, event):
        """Main processing thread for the file stager process.

        Will cycle through the
        :attr:`geoutils.StagerDaemon.files_to_process`
        :attr:`geoutils.StagerDaemon.regen_count` times and recreate
        a new file for ingestion.

        **Args:**
            *event*: a :mod:`threading.Event` based internal semaphore
            that can be set via the :mod:`signal.signal.SIGTERM` signal
            event to perform a function within the running proess.


        """
        fixture_file_count = len(self.files_to_process)
        for regen_count in range(self.regen_count):
            if fixture_file_count <= 0:
                break

            file_index = regen_count % fixture_file_count
            regen_file = self.files_to_process[file_index]

            filename, extension = os.path.splitext(regen_file)
            regen_file_rand = ('%s_%s%s' % (filename,
                                            self.random_string(),
                                            extension))

            target_file =  os.path.join(self.conf.inbound_dir,
                                        os.path.basename(regen_file_rand))
            log.info('Regen file %d:%s' % ((regen_count + 1), target_file))

            if not self.dry:
                copy_file(regen_file, target_file)

            if event.isSet():
                break

            time.sleep(self.regen_sleep)

    def source_files(self):
        """Checks inbound directory (defined by the
        :attr:`geoutils.StagerDaemon.fixture_dir` attribute) for valid
        NITF files to be regenerated with a different filename.

        **Returns:**
            list of absolute path names to the files found

        """
        if (self.fixture_dir is None or
            not os.path.isdir(self.fixture_dir)):
            log.error('Fixture directory "%s" undefined or missing' %
                      self.fixture_dir)
        else:
            log.debug('Sourcing files at: %s' % self.fixture_dir)
            self.files_to_process = get_directory_files_list(self.fixture_dir)

        return self.files_to_process

    @staticmethod
    def random_string(length=10):
        rand = ''.join(random.choice(string.lowercase) for i in range(length))

        return rand
