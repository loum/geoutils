# pylint: disable=R0904,C0103
""":class:`geoutils.Config` tests.

"""
import unittest2
import os

import geoutils
from geosutils.setter import (set_scalar,
                              set_list,
                              set_dict)


class DummyConfig(geoutils.Config):
    _dummy_key = None
    _int_key = None
    _empty_key = None
    _dummy_list = []
    _dummy_dict_section = {}
    _dummy_dict_int = {}
    _dummy_dict_key_as_int = {}
    _dummy_dict_key_as_upper = {}
    _dummy_dict_key_as_lower = {}
    _dummy_dict_as_list = {}

    def __init__(self, config_file):
        geoutils.Config.__init__(self, config_file)

    @property
    def dummy_key(self):
        return self._dummy_key

    @set_scalar
    def set_dummy_key(self, value):
        pass

    @property
    def int_key(self):
        return self._int_key

    @set_scalar
    def set_int_key(self, value):
        pass

    @property
    def empty_key(self):
        return self._empty_key

    @set_scalar
    def set_empty_key(self, value):
        pass

    @property
    def dummy_list(self):
        return self._dummy_list

    @set_list
    def set_dummy_list(self, value):
        pass

    @property
    def dummy_dict_section(self):
        return self._dummy_dict_section

    @set_dict
    def set_dummy_dict_section(self, value):
        pass

    @property
    def dummy_dict_int(self):
        return self._dummy_dict_int

    @set_dict
    def set_dummy_dict_int(self, value):
        pass

    @property
    def dummy_dict_key_as_int(self):
        return self._dummy_dict_key_as_int

    @set_dict
    def set_dummy_dict_key_as_int(self, value):
        pass

    @property
    def dummy_dict_key_as_upper(self):
        return self._dummy_dict_key_as_upper

    @set_dict
    def set_dummy_dict_key_as_upper(self, value):
        pass

    @property
    def dummy_dict_key_as_lower(self):
        return self._dummy_dict_key_as_lower

    @set_dict
    def set_dummy_dict_key_as_lower(self, value):
        pass

    @property
    def dummy_dict_as_list(self):
        return self._dummy_dict_as_list

    @set_dict
    def set_dummy_dict_as_list(self, value):
        pass


class TestConfig(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._file = os.path.join('geoutils',
                                 'config',
                                 'tests',
                                 'files',
                                 'geoutils.conf')

    def setUp(self):
        self._conf = geoutils.Config()

    def test_init(self):
        """Initialise a Config object.
        """
        msg = 'Object is not a geoutils.Config'
        self.assertIsInstance(self._conf, geoutils.Config, msg)

    def test_parse_config_no_file(self):
        """Read config with no file provided.
        """
        received = self._conf.parse_config()
        msg = 'Valid config read did not return True'
        self.assertFalse(received, msg)

    def test_parse_config(self):
        """Read valid config.
        """
        old_config = self._conf.config_file

        self._conf.set_config_file(self._file)
        received = self._conf.parse_config()
        msg = 'Valid config read did not return True'
        self.assertTrue(received, msg)

        # Clean up.
        self._conf.set_config_file(old_config)

    def test_parse_scalar_config(self):
        """Parse a scalar from the configuration file.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_section'
        option = 'dummy_key'
        var = 'dummy_key'

        received = conf.parse_scalar_config(section, option, var)
        expected = 'dummy_value'
        msg = 'Parsed config scalar error'
        self.assertEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_key
        msg = 'Parsed config scalar: set variable error'
        self.assertEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_scalar_config_is_required(self):
        """Parse a required scalar from the configuration file.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        # Missing option.
        section = 'dummy_section'
        option = 'missing_option'

        kwargs = {'section': section,
                  'option': option,
                  'is_required': True}
        self.assertRaises(SystemExit, conf.parse_scalar_config, **kwargs)

        # Missing section.
        section = 'missing_section'
        option = str()

        kwargs = {'section': section,
                  'option': option,
                  'is_required': True}
        self.assertRaises(SystemExit, conf.parse_scalar_config, **kwargs)

        # Clean up.
        conf = None
        del conf

    def test_parse_scalar_config_as_int(self):
        """Parse a scalar from the configuration file: cast to int.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'int_section'
        option = 'int_key'

        received = conf.parse_scalar_config(section,
                                            option,
                                            cast_type='int')
        expected = 1234
        msg = 'Parsed config scalar error: cast to int'
        self.assertEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.int_key
        msg = 'Parsed config scalar (cast to int): set variable error'
        self.assertEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_scalar_config_no_value_found(self):
        """Parse a scalar from the configuration file: no value found.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_setion'
        option = 'empty_key'
        var = 'empty_key'

        received = conf.parse_scalar_config(section, option, var)
        msg = 'Parsed config scalar error: no value found/no var'
        self.assertIsNone(received, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_scalar_config_as_list(self):
        """Parse a scalar from the configuration file -- as list.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_section'
        option = 'dummy_list'
        var = 'dummy_list'

        received = conf.parse_scalar_config(section,
                                            option,
                                            var,
                                            is_list=True)
        expected = ['list 1', 'list 2']
        msg = 'Parsed config scalar error: lists'
        self.assertListEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_list
        msg = 'Parsed config scalar (list) error'
        self.assertEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_dict_config(self):
        """Parse a dict (section) from the configuration file.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_dict_section'
        received = conf.parse_dict_config(section)
        expected = {'dict_1': 'dict 1 value', 'dict_2': 'dict 2 value'}
        msg = 'Parsed config dict error'
        self.assertDictEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_dict_section
        msg = 'Parsed config dict set variable error'
        self.assertEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_dict_config_is_required(self):
        """Parse a required dict (section) from the configuration file.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        kwargs = {'section': 'missing_dict_section',
                  'is_required': True}
        self.assertRaises(SystemExit, conf.parse_dict_config, **kwargs)

        # Clean up.
        conf = None
        del conf

    def test_parse_dict_config_as_int(self):
        """Parse a dict (section) from the configuration file (as int).
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_dict_int'
        received = conf.parse_dict_config(section, cast_type='int')
        expected = {'dict_1': 1234}
        msg = 'Parsed config dict as int error'
        self.assertDictEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_dict_int
        msg = 'Parsed config dict as int set variable error'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_dict_config_key_as_int(self):
        """Parse a dict (section) from the configuration file (key as int).
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_dict_key_as_int'
        received = conf.parse_dict_config(section, key_cast_type='int')
        expected = {1234: 'int_key_value'}
        msg = 'Parsed config dict with key as int error'
        self.assertDictEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_dict_key_as_int
        msg = 'Parsed config dict set variable error (key as int)'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_dict_config_key_upper_case(self):
        """Parse a dict (section) from the configuration file (key upper).
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_dict_key_as_upper'
        received = conf.parse_dict_config(section, key_case='upper')
        expected = {'ABC': 'upper_key_value'}
        msg = 'Parsed config dict (key upper case) error'
        self.assertDictEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_dict_key_as_upper
        msg = 'Parsed config dict set variable error (key as upper case)'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_dict_config_key_lower_case(self):
        """Parse a dict (section) from the configuration file (key lower).
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_dict_key_as_lower'
        received = conf.parse_dict_config(section, key_case='lower')
        expected = {'abc': 'lower_key_value'}
        msg = 'Parsed config dict (key lower case) error'
        self.assertDictEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_dict_key_as_lower
        msg = 'Parsed config dict set variable error (key as lower case)'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def test_parse_dict_config_list_values(self):
        """Parse a dict (section) from the configuration file -- as list.
        """
        conf = DummyConfig(self._file)
        conf.parse_config()

        section = 'dummy_dict_as_list'
        received = conf.parse_dict_config(section, is_list=True)
        expected = {'dict_1': ['list item 1', 'list item 2'],
                    'dict_2': ['list item 3', 'list item 4']}
        msg = 'Parsed config dict error (as list)'
        self.assertDictEqual(received, expected, msg)

        # ... and check that the variable is set.
        received = conf.dummy_dict_as_list
        msg = 'Parsed config dict set variable error (as list)'
        self.assertDictEqual(received, expected, msg)

        # Clean up.
        conf = None
        del conf

    def tearDown(self):
        self._conf = None
        del self._conf

    @classmethod
    def tearDownClass(cls):
        cls._file = None
        del cls._file
