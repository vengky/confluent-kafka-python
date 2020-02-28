#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import warnings

from requests import utils

from confluent_kafka.schema_registry import SchemaRegistryClient as _RegistryClient

try:
    # Python 2 considers int an instance of str
    string_type = basestring  # noqa
except NameError:
    string_type = str


warnings.warn(
    "CachedSchemaRegistryClient has been repackaged under confluent_kafka.schema_registry"
    " and renamed SchemaRegistryClient. This package will be removed in a future version",
    category=DeprecationWarning, stacklevel=2)

__all__ = ['CachedSchemaRegistryClient']


VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO', 'SASL_INHERIT']


class CachedSchemaRegistryClient(object):
    """
    CachedSchemaRegistryClient shim to maintain compatibility during transitional period.
    """
    def __new__(cls, url, max_schemas_per_subject=1000,
                ca_location=None,
                cert_location=None, key_location=None):
        conf = url
        if not isinstance(url, dict):
            conf = {
                'url': url,
                'ssl.ca.location': ca_location,
                'ssl.certificate.location': cert_location,
                'ssl.key.location': key_location
            }
            warnings.warn(
                "CachedSchemaRegistry constructor is being deprecated. "
                "Use CachedSchemaRegistryClient(dict: config) instead. "
                "Existing params ca_location, cert_location and key_location will be replaced with their "
                "librdkafka equivalents as keys in the conf dict: `ssl.ca.location`, `ssl.certificate.location` and "
                "`ssl.key.location` respectively",
                category=DeprecationWarning, stacklevel=2)

        url = conf.pop('url', '')
        if not isinstance(url, string_type):
            raise TypeError("URL must be of type str")

        ca_bundle = conf.pop('ssl.ca.location', ca_location)

        cert = cls._configure_client_tls(conf)
        auth = cls._configure_basic_auth(url, conf)

        return _RegistryClient(url=url, ca_bundle=ca_bundle, cert=cert, auth=auth)

    @staticmethod
    def _configure_client_tls(conf):
        cert = conf.pop('ssl.certificate.location', None), conf.pop('ssl.key.location', None)
        # Both values can be None or no values can be None
        if bool(cert[0]) != bool(cert[1]):
            raise ValueError(
                "Both schema.registry.ssl.certificate.location and "
                "schema.registry.ssl.key.location must be set")

        return cert

    @staticmethod
    def _configure_basic_auth(url, conf):
        auth_provider = conf.pop('basic.auth.credentials.source', 'URL').upper()
        if auth_provider not in VALID_AUTH_PROVIDERS:
            raise ValueError("schema.registry.basic.auth.credentials.source must be one of {}"
                             .format(VALID_AUTH_PROVIDERS))

        if auth_provider == 'SASL_INHERIT':
            if conf.pop('sasl.mechanism', '').upper() is ['GSSAPI']:
                raise ValueError("SASL_INHERIT does not support SASL mechanisms GSSAPI")
            auth = (conf.pop('sasl.username', ''), conf.pop('sasl.password', ''))
        elif auth_provider == 'USER_INFO':
            auth = tuple(conf.pop('basic.auth.user.info', '').split(':'))
        else:
            auth = utils.get_auth_from_url(url)

        if len(conf) > 0:
            raise ValueError("Unrecognized configuration properties: {}".format(conf.keys()))

        return auth
