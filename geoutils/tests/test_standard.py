# pylint: disable=R0904,C0103
""":class:`geoutils.Standard` tests.

"""
import unittest2
import subprocess
import os
import signal

import geoutils
from oct.utils.log import log


class TestStandard(unittest2.TestCase):
    """:class:`geoutils.Standard` test cases.
    """
    @classmethod
    def setUpClass(cls):
        """Attempt to start the Accumulo mock proxy server.
        """
        cls._accumulo_server_exists = False
        accumulo_exe = os.path.join(os.sep, 'usr', 'bin', 'accumulo')
        if (os.path.isfile(accumulo_exe) and
            os.access(accumulo_exe, os.X_OK)):
            cls._accumulo_server_exists = True

            conf = os.path.join('geoutils',
                                'tests',
                                'files',
                                'proxy.properties')
            args = [accumulo_exe, 'proxy', '-p', conf]
            cls._proc = subprocess.Popen(args)

            log.debug('The PID of the accumulo mock is: %d' % cls._proc.pid)

    def test_init(self):
        """Initialise a geoutils.Standard object.
        """
        standard = geoutils.Standard()
        msg = 'Object is not a geoutils.Standard'
        self.assertIsInstance(standard, geoutils.Standard, msg)

    @classmethod
    def tearDownClass(cls):
        """Shutdown the Accumulo mock proxy server (if enabled)
        """
        if cls._accumulo_server_exists:
            cls._proc.send_signal(signal.SIGINT)
