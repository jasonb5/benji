#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
import operator
import os
import re
from copy import deepcopy
from functools import reduce
from os.path import expanduser
from pathlib import Path
from typing import List, Callable, Union, Dict, Any, Optional, Sequence

from cerberus import Validator, SchemaError
from pkg_resources import resource_filename
from ruamel.yaml import YAML

from benji.exception import ConfigurationError, InternalError
from benji.logging import logger


class _ConfigDict(dict):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.full_name: Optional[str] = None


class _ConfigList(list):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.full_name: Optional[str] = None


class Config:
    _CONFIG_DIRS = ['/etc', '/etc/benji']
    _CONFIG_FILE = 'benji.yaml'
    _CONFIGURATION_VERSION_KEY = 'configurationVersion'
    _CONFIGURATION_VERSION_REGEX = '\d+\.\d+\.\d+'
    _PARENTS_KEY = 'parents'
    _SCHEMA_VERSIONS = ['1.0.0']
    _YAML_SUFFIX = '.yaml'

    _schema_registry: Dict[str, Dict] = {}

    @staticmethod
    def _schema_name(module: str, version: str) -> str:
        return '{}-{}'.format(module, version)

    @classmethod
    def add_schema(cls, *, module: str, version: str, file: str) -> None:
        yaml = YAML(typ='safe', pure=True)
        name = cls._schema_name(module, version)
        try:
            schema = yaml.load(Path(file))
            cls._schema_registry[name] = schema
        except FileNotFoundError:
            raise InternalError('Schema {} not found or not accessible.'.format(file))
        except SchemaError as exception:
            raise InternalError('Schema {} is invalid.'.format(file)) from exception

    def _merge_dicts(self, result, parent):
        if isinstance(result, dict) and isinstance(parent, dict):
            for k, v in parent.items():
                if k not in result:
                    result[k] = deepcopy(v)
                else:
                    result[k] = self._merge_dicts(result[k], v)
        return result

    def _resolve_schema(self, *, name: str) -> Dict:
        try:
            child = self._schema_registry[name]
        except KeyError:
            raise InternalError('Schema for module {} is missing.'.format(name))
        result: Dict = {}
        if self._PARENTS_KEY in child:
            parent_names = child[self._PARENTS_KEY]
            for parent_name in parent_names:
                parent = self._resolve_schema(name=parent_name)
                self._merge_dicts(result, parent)
        result = self._merge_dicts(result, child)
        if self._PARENTS_KEY in result:
            del result[self._PARENTS_KEY]
        logger.debug('Resolved schema for {}: {}.'.format(name, result))
        return result

    def _get_validator(self, *, module: str, version: str) -> Validator:
        name = self._schema_name(module, version)
        schema = self._resolve_schema(name=name)
        try:
            validator = Validator(schema)
        except SchemaError as exception:
            logger.error('Schema {} validation errors:'.format(name))
            self._output_validation_errors(exception.args[0])
            raise InternalError('Schema {} is invalid.'.format(name)) from exception
        return validator

    @staticmethod
    def _output_validation_errors(errors) -> None:

        def traverse(cursor, path=''):
            if isinstance(cursor, dict):
                for key, value in cursor.items():
                    traverse(value, path + ('.' if path else '') + str(key))
            elif isinstance(cursor, list):
                for value in cursor:
                    if isinstance(value, dict):
                        traverse(value, path)
                    else:
                        logger.error('  {}: {}'.format(path, value))

        traverse(errors)

    def validate(self, *, module: str, version: str = None, config: Union[Dict, _ConfigDict]) -> Dict:
        validator = self._get_validator(module=module, version=self._config_version if version is None else version)
        if not validator.validate({'configuration': config if config is not None else {}}):
            logger.error('Configuration validation errors:')
            self._output_validation_errors(validator.errors)
            raise ConfigurationError('Configuration for module {} is invalid.'.format(module))

        config_validated = validator.document['configuration']
        logger.debug('Configuration for module {}: {}.'.format(module, config_validated))
        return config_validated

    def __init__(self, ad_hoc_config: Dict = None, sources: Sequence[str] = None) -> None:
        yaml = YAML(typ='safe', pure=True)

        if ad_hoc_config is None:
            if not sources:
                sources = self._get_sources()

            config = None
            for source in sources:
                if os.path.isfile(source):
                    try:
                        config = yaml.load(Path(source))
                    except Exception as exception:
                        raise ConfigurationError('Configuration file {} is invalid.'.format(source)) from exception
                    if config is None:
                        raise ConfigurationError('Configuration file {} is empty.'.format(source))
                    break

            if not config:
                raise ConfigurationError('No configuration file found in the default places ({}).'.format(
                    ', '.join(sources)))
        else:
            config = yaml.load(ad_hoc_config)
            if config is None:
                raise ConfigurationError('Configuration string is empty.')

        if self._CONFIGURATION_VERSION_KEY not in config:
            raise ConfigurationError('Configuration is missing required key "{}".'.format(self._CONFIGURATION_VERSION_KEY))

        version = config[self._CONFIGURATION_VERSION_KEY]
        if not re.fullmatch(self._CONFIGURATION_VERSION_REGEX, version):
            raise ConfigurationError('Configuration has invalid version of "{}".'.format(version))

        if version not in self._SCHEMA_VERSIONS:
            raise ConfigurationError('Configuration has unsupported version of "{}".'.format(version))

        self._config_version = version
        self._config = _ConfigDict(self.validate(module=__name__, config=config))
        logger.debug('Loaded configuration: {}'.format(self._config))

    def _get_sources(self) -> List[str]:
        sources = []
        for directory in self._CONFIG_DIRS:
            sources.append('{directory}/{file}'.format(directory=directory, file=self._CONFIG_FILE))
        sources.append(expanduser('~/.{file}'.format(file=self._CONFIG_FILE)))
        sources.append(expanduser('~/{file}'.format(file=self._CONFIG_FILE)))
        return sources

    @staticmethod
    def _get(root,
             name: str,
             *args,
             types: Any = None,
             check_func: Callable[[object], bool] = None,
             check_message: str = None,
             full_name_override: str = None,
             index: int = None) -> object:
        if full_name_override is not None:
            full_name = full_name_override
        elif hasattr(root, 'full_name') and root.full_name:
            full_name = root.full_name
        else:
            full_name = ''

        if index is not None:
            full_name = '{}{}{}'.format(full_name, '.' if full_name else '', index)

        full_name = '{}{}{}'.format(full_name, '.' if full_name else '', name)

        if len(args) > 1:
            raise InternalError('Called with more than two arguments for key {}.'.format(full_name))

        try:
            value = reduce(operator.getitem, name.split('.'), root)
            if types is not None and not isinstance(value, types):
                raise TypeError('Config value {} has wrong type {}, expected {}.'.format(full_name, type(value), types))
            if check_func is not None and not check_func(value):
                if check_message is None:
                    raise ConfigurationError(
                        'Config option {} has the right type but the supplied value is invalid.'.format(full_name))
                else:
                    raise ConfigurationError('Config option {} is invalid: {}.'.format(full_name, check_message))
            if isinstance(value, dict):
                value = _ConfigDict(value)
                value.full_name = full_name
            elif isinstance(value, list):
                value = _ConfigList(value)
                value.full_name = full_name
            return value
        except KeyError:
            if len(args) == 1:
                return args[0]
            else:
                if types and isinstance({}, types):
                    raise KeyError('Config section {} is missing.'.format(full_name)) from None
                else:
                    raise KeyError('Config option {} is missing.'.format(full_name)) from None

    def get(self, name: str, *args, **kwargs) -> Any:
        return Config._get(self._config, name, *args, **kwargs)

    @staticmethod
    def get_from_dict(dict_: _ConfigDict, name: str, *args, **kwargs) -> Any:
        return Config._get(dict_, name, *args, **kwargs)


for version in Config._SCHEMA_VERSIONS:
    schema_base_path = os.path.join(resource_filename(__name__, 'schemas'), version)
    for filename in os.listdir(schema_base_path):
        full_path = os.path.join(schema_base_path, filename)
        if not os.path.isfile(full_path) or not full_path.endswith(Config._YAML_SUFFIX):
            continue
        module = filename[0:len(filename) - len(Config._YAML_SUFFIX)]
        logger.debug('Loading  schema {} for module {}, version {}.'.format(full_path, module, version))
        Config.add_schema(module=module, version=version, file=full_path)
