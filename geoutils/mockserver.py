# pylint: disable=E1101,E0102,C0111
"""The :class:`geoutils.MockServer` presents a Accumulo mockup of
a proxy server.

"""
__all__ = ["MockServer"]

import subprocess
import signal
import os
import time

from oct.utils.log import log


class MockServer(object):
    """:class:`geoutils.MockServer`

    .. attribute:: *is_active*
        Boolean flag that denotes if the mock server state is enabled

    .. attribute:: *accumulo_exe*
        Path to the ``accumulo`` executable

    .. attribute:: *properties_file*
        Path to the proxy server properties file

    .. attribute:: *proc*
        Handle to a :class:`subprocess.Popen` object

    """
    _is_active = False
    _accumulo_exe = os.path.join(os.sep, 'usr', 'bin', 'accumulo')
    _properties_file = None
    _proc = None

    def __init__(self, conf=None):
        if conf is not None:
            self._properties_file = conf

    @property
    def is_active(self):
        return self._is_active
    
    @is_active.setter
    def is_active(self, value):
        self._is_active = value

    @property
    def accumulo_exe(self):
        return self._accumulo_exe
    
    @accumulo_exe.setter
    def accumulo_exe(self, value):
        self._accumulo_exe = value

    @property
    def properties_file(self):
        return self._properties_file
    
    @properties_file.setter
    def properties_file(self, value):
        self._properties_file = value

    @property
    def proc(self):
        return self._proc
    
    @proc.setter
    def proc(self, value):
        self._proc = value

    def start(self):
        """Attempt to start the Mock Server.

        """
        log.info('Starting the Accumulo Mock Server ...')
        if (os.path.isfile(self.accumulo_exe) and
            os.access(self.accumulo_exe, os.X_OK)):
            args = [self.accumulo_exe,
                    'proxy',
                    '-p',
                    self.properties_file]
            self.proc = subprocess.Popen(args)
            time.sleep(1)
            self.is_active = True

        log.info('Accumulo Mock Server is_active status: %s' %
                 self.is_active)

    def stop(self):
        """Attempt to stop the Accumulo Mock Server.

        """
        if self._is_active:
            log.info('Stopping the Accumulo Mock Server ...')
            self.proc.send_signal(signal.SIGINT)
            self.proc = None
            self.is_active = False
            log.info('Accumulo Mock Server stopped')
