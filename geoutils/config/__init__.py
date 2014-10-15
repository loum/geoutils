# pylint: disable=R0903,C0111,R0902
"""The :class:`geoutils.Config` acts as a base for the abstraction of the
GeoUtils configuration items.

"""
__all__ = ["Config"]

import sys
import __builtin__
import os
import ConfigParser

from geosutils.log import log


class Config(ConfigParser.SafeConfigParser):
    """:class:`geosutils.Config` class.

    .. attribute:: *config_file*

        path to the configuration file to parse

    .. note::

        The :class:`Config` class inherits from the old-style
        :class:`ConfigParser.SafeConfigParser` and does not support
        property getters and setters.

    """
    _config_file = None
    _facility = None

    def __init__(self, config_file=None):
        """:class:`geosutils.Config` initialisation.
        """
        self._config_file = config_file
        self._facility = self.__class__.__name__

        ConfigParser.SafeConfigParser.__init__(self)

    @property
    def config_file(self):
        return self._config_file

    def set_config_file(self, value):
        self._config_file = value

    @property
    def facility(self):
        return self._facility

    def parse_config(self):
        """Attempt to read the contents of the :attr:`geoutils.Config`
        (unless ``None``).

        File contents should be as per :mod:`ConfigParser` format.

        **Returns:**
            Boolean ``True`` upon success.  Boolean ``False`` otherwise.

        """
        log.debug('Parsing config file: "%s"' % self.config_file)
        config_parse_status = False

        if (self.config_file is None or
           not os.path.exists(self.config_file)):
            log.error('Invalid config file: "%s"' % self.config_file)
        else:
            self.read(self.config_file)
            config_parse_status = True

        return config_parse_status

    def parse_scalar_config(self,
                            section,
                            option,
                            var=None,
                            cast_type=None,
                            is_list=False,
                            is_required=False):
        """Helper method that can parse a scalar value based on
        *section* and *option* in the :mod:`ConfigParser` based
        configuration file and set *var* attribute with the value parsed.

        If *is_required* is set and configuration section/option is missing
        then the process will exit.

        **Args:**
            *section*: the configuration file section.  For example
            ``[environment]``

            *option*: the configuration file section's options to
            parse.

        **Kwargs:**
            *var*: the target attribute name.  This can be omitted if
            the target attribute name is the same as *option*

            *cast_type*: cast the value parsed as *cast_type*.  If
            ``None`` is specified, then parse as a string

            *is_list*: boolean flag to indicate whether to parse the
            option values as a list (default ``False``)

            *is_required*: boolean flag to indicate whether the
            value is required.

        **Returns:**
            the value of the scalar option value parsed

        """
        value = None

        if var is None:
            var = option

        try:
            value = self.get(section, option)
            if cast_type is not None:
                caster = getattr(__builtin__, cast_type)
                value = caster(value)
            if is_list:
                value = value.split(',')
            setter = getattr(self, 'set_%s' % var)
            setter(value)
        except (ConfigParser.NoOptionError,
                ConfigParser.NoSectionError), err:
            if is_required:
                log.critical('Missing required config: %s' % err)
                sys.exit(1)

            try:
                getter = getattr(self, var)
                msg = ('%s %s.%s not defined. Using' %
                       (self.facility, section, option))
                if isinstance(getter, (int, long, float, complex)):
                    log.debug('%s %d' % (msg, getter))
                else:
                    log.debug('%s "%s"' % (msg, getter))
            except AttributeError, err:
                log.debug('%s %s.%s not defined: %s.' %
                          (self.facility, section, option, err))

        return value

    def parse_dict_config(self,
                          section,
                          var=None,
                          cast_type=None,
                          key_cast_type=None,
                          key_case=None,
                          is_list=False,
                          is_required=False):
        """Helper method that can parse a :mod:`ConfigParser` *section*
        and set the *var* attribute with the value parsed.

        :mod:`ConfigParser` sections will produce a dictionary structure.
        If *is_list* is ``True`` the section's options values will be
        treated as a list.  This will produce a dictionary of lists.

        **Args:**
            *section*: the configuration file section.  For example
            ``[comms_delivery_partners]``

        **Kwargs:**
            *var*: the target attribute name.  This can be omitted if
            the target attribute name is the same as *option*

            *cast_type*: cast the value parsed as *cast_type*.  If
            ``None`` is specified, then parse as a string

            *key_cast_type*: cast the option parsed as *cast_type*

            *key_case*: for strings, change the case of the option value

            *is_list*: boolean flag to indicate whether to parse the
            option values as a list (default ``False``)

            *is_required*: boolean flag to indicate whether the
            value is required.

        **Returns:**
            the value of the :mod:`ConfigParser` section as a dict
            structure

        """
        value = None

        if var is None:
            var = section

        try:
            key_caster = None
            if key_cast_type is not None:
                key_caster = getattr(__builtin__, key_cast_type)

            tmp_value = {}
            for k, v in dict(self.items(section)).iteritems():
                # Cast the dictionay key if required.
                key = k
                if key_caster is not None:
                    key = key_caster(k)

                if key_case == 'upper':
                    tmp_value[key.upper()] = v
                elif key_case == 'lower':
                    tmp_value[key.lower()] = v
                else:
                    tmp_value[key] = v

            # Now, take care of list values.
            if is_list:
                for k, v in tmp_value.iteritems():
                    tmp_value[k] = v.split(',')

            # Finally, cast the value if required.
            if cast_type is not None:
                caster = getattr(__builtin__, cast_type)
                for k, v in tmp_value.iteritems():
                    if isinstance(v, (list)):
                        tmp_value[k] = [caster(x) for x in v]
                    else:
                        tmp_value[k] = caster(v)

            value = tmp_value
            setter = getattr(self, 'set_%s' % var)
            setter(value)
        except (ConfigParser.NoOptionError,
                ConfigParser.NoSectionError), err:
            if is_required:
                log.critical('Missing required config: %s' % err)
                sys.exit(1)

            try:
                getter = getattr(self, var)
                msg = ('%s %s not defined.  Using' %
                       (self.facility, section))
                if isinstance(getter, (int, long, float, complex)):
                    log.debug('%s %d' % (msg, getter))
                else:
                    log.debug('%s "%s"' % (msg, getter))
            except AttributeError, err:
                log.debug('%s %s not defined: %s.' %
                          (self.facility, section, err))

        return value
