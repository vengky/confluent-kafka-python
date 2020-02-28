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
import json
import logging

from requests import Session

from .schema import Schema
from .error import ClientError

log = logging.getLogger(__name__)

VALID_LEVELS = ['NONE',
                'FULL', 'FULL_TRANSITIVE',
                'FORWARD', 'FORWARD_TRANSITIVE',
                'BACKWARD', 'BACKWARD_TRANSITIVE']

VALID_METHODS = ['GET', 'POST', 'PUT', 'DELETE']

# Common accept header sent
ACCEPT_HDR = "application/vnd.schemaregistry.v1+json," \
             " application/vnd.schemaregistry+json," \
             " application/json"


class RegistrationEncoder(json.JSONEncoder):
    def default(self, obj):
        """
        Encodes Schema object to the Schema Registry request body format

        :param Schema obj: Schema object
        :returns: Schema Registry request body
        :rtype: str
        """
        if obj.schema_type == 'AVRO':
            return {'schema': str(obj)}
        else:
            return {'schema': str(obj), 'schemaType': obj.schema_type}


class RegistrationDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        """
        Decodes Schema Registry response body

        :param obj: Schema Registry response body
        :returns: Schema Registry Response
        :rtype: int|str|tuple
        """
        response = []
        schema_id = obj.get('id', None)
        version = obj.pop('version', -1)

        if schema_id:
            response.append(schema_id)

        if 'schema' in obj:
            subject = obj.pop('subject', None)
            schema = Schema(**obj)

            if subject:
                schema.subjects.add(subject)

            response.append(schema)

        if version >= 0:
            response.append(version)

        if len(response) > 1:
            return tuple(response)

        return response[0]


class SchemaRegistryClient(object):
    """
    A client that talks to a Schema Registry over HTTP

    See http://confluent.io/docs/current/schema-registry/docs/intro.html for more information.

    Errors communicating to the server will result in a ClientError being raised.
    """

    def __init__(self, url, auth=None,
                 ca_bundle=None, cert=None):
        """

        :param str url: Schema Registry listener address
        :param tuple(str, str) auth: username, password tuple used to authenticate
        :param str ca_bundle: path to CA bundle file
        :param tuple(str, str) cert: client certificate, private key tuple
        """
        if not str(url).startswith('http'):
            raise ValueError("Invalid URL provided for Schema Registry: {}".format(url))

        self.url = url.rstrip('/')
        self.id_to_schema = {}
        self._session = Session()

        if cert:
            self._session.cert = cert

        if ca_bundle:
            self._session.verify = ca_bundle

        if auth:
            self._session.auth = auth

    def __del__(self):
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._session:
            self._session.close()

    def user_auth(self, auth):
        """
        Sets HTTP Basic auth credentials.

        :param tuple(str, str) auth: Schema Registry credentials

        """
        self._session.auth = auth

    def ca_location(self, ca_bundle):
        """
        Set CA bundle path for verifying Schema Registry certificate.

        :param ca_bundle: location of certificate bundle file

        """
        self._session.verify = ca_bundle

    def client_certificate(self, cert):
        """
        Set client certificate, private key.

        :param tuple(str, str) cert: Client certificate and private key

        """
        self._session.cert = cert

    def _send_request(self, url, method='GET', body=None, headers={}):
        """
        Send request to Schema Registry

        :param str url: Request URL
        :param str method: HTTP method
        :param object body: Optional HTTP request body
        :param dict headers: HTTP request headers
        :returns: HTTP response
        """
        if method not in VALID_METHODS:
            raise ClientError(
                "Method {} is invalid; valid methods include {}".format(method, VALID_METHODS))

        _headers = {'Accept': ACCEPT_HDR}

        if body:
            body = json.dumps(body, cls=RegistrationEncoder)
            _headers["Content-Length"] = str(len(body))
            _headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
        _headers.update(headers)

        response = self._session.request(method, url, headers=_headers,
                                         data=body)

        # Returned by Jetty not SR so the payload is not json encoded
        try:
            if 200 <= response.status_code <= 299:
                return response.json(cls=RegistrationDecoder)
            raise ClientError(response.json().get('message'), response.status_code)
        except ValueError:
            return ClientError(response.content, response.status_code)

    def _cache_schema(self, schema):
        """
        Cache schema id association

        :param Schema schema: parsed schema

        """

        # don't overwrite anything
        if schema.id not in self.id_to_schema:
            self.id_to_schema[schema.id] = schema

    def register(self, subject, schema):
        """
        POST /subjects/(string: subject)/versions

        Register a schema with the registry under the given subject
        and receive a schema id.

        Multiple instances of the same schema will result in cache misses.

        :param str subject: subject name
        :param Schema schema: schema to be registered
        :returns: schema_id
        :rtype: int
        """

        if schema is None:
            return None

        # If a schema is already registered with a subject it must have an id
        if subject in schema.subjects:
            return schema.id

        # send it up
        url = '/'.join([self.url, 'subjects', subject, 'versions'])

        try:
            schema.id = self._send_request(url, method='POST', body=schema)
            schema.subjects.add(subject)
            return schema.id
        except ClientError as e:
            raise ClientError("Failed to register schema {}: {}".format(e.http_code, e.message))

    def check_registration(self, subject, schema):
        """
        POST /subjects/(string: subject)
        Check if a schema has already been registered under the specified subject.
        If so, returns the schema id. Otherwise, raises a ClientError.

        avro_schema must be a parsed schema from the python avro library

        Multiple instances of the same schema will result in inconsistencies.

        :param str subject: subject name
        :param schema schema: schema to be checked
        :returns: schema_id
        :rtype: int
        """

        # If a schema is already registered with a subject it must have an id
        if subject in schema.subjects:
            return schema.id

        # send it up
        url = '/'.join([self.url, 'subjects', subject])
        # body is { schema : json_string }

        body = {'schema': str(schema)}
        result, code = self._send_request(url, method='POST', body=body)
        if code == 401 or code == 403:
            raise ClientError("Unauthorized access. Error code:" + str(code))
        elif code == 404:
            raise ClientError("Schema or subject not found:" + str(code))
        elif not 200 <= code <= 299:
            raise ClientError("Unable to check schema registration. Error code:" + str(code))
        # result is a dict
        schema.id = result['id']
        schema.subjects.add(subject)
        # cache it
        self._cache_schema(schema)
        return schema.id

    def delete_subject(self, subject):
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be recycled or in development environments.
        :param subject: subject name
        :returns: version of the schema deleted under this subject
        :rtype: (int)
        """

        url = '/'.join([self.url, 'subjects', subject])

        try:
            return self._send_request(url, method="DELETE")
        except ClientError as e:
            raise ClientError('Unable to delete subject {}: {}'.format(e.http_code, e.http_code))

    def get_by_id(self, schema_id):
        """
        GET /schemas/ids/{int: id}

        Retrieve a parsed Schema by id or None if not found

        :param int schema_id: int value
        :returns: Schema if found
        :rtype: schema
        """

        schema = self.id_to_schema.get(schema_id, None)
        if schema is None:
            # fetch from the registry
            url = '/'.join([self.url, 'schemas', 'ids', str(schema_id)])

            try:
                schema = self._send_request(url)
                schema.id = schema_id

                self._cache_schema(schema)
            except ClientError as e:
                log.error("Failed to fetch schema {}: {}".format(e.http_code, e.message))
        return schema

    def get_latest_schema(self, subject):
        """
        GET /subjects/(string: subject)/versions/(versionId: version)

        Return the latest 3-tuple of:
        (the schema id, the parsed schema, the schema version)
        for a particular subject.

        This call always contacts the registry.

        If the subject is not found, (None,None,None) is returned.
        :param str subject: subject name
        :returns: (schema_id, schema, version)
        :rtype: (int, schema, int)
        """
        url = '/'.join([self.url, 'subjects', subject, 'versions', 'latest'])

        try:
            schema_id, schema, version = self._send_request(url)
            schema.subjects.add(subject)

            self._cache_schema(schema)
            return schema.id, schema, version
        except ClientError as e:
            log.error("Failed to fetch latest schema {}: {}".format(e.http_code, e.message))
            return None, None, None

    def get_version(self, subject, schema):
        """
        POST /subjects/(string: subject)

        Get the version of a schema for a given subject.
        Returns None if not found.

        :param str subject: subject name
        :param schema schema: schema
        :returns: version
        :rtype: int
        """

        url = '/'.join([self.url, 'subjects', subject])
        body = {'schema': str(schema)}

        try:
            schema_id, schema, version = self._send_request(url, method='POST', body=body)
            self._cache_schema(schema)
            return version
        except ClientError as e:
            log.error("Failed to fetch schema version {}: {}".format(e.http_code, e.message))

    def test_compatibility(self, subject, schema, version='latest'):
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)

        Test the compatibility of a candidate parsed schema for a given subject.

        By default the latest version is checked against.
        :param: str subject: subject name
        :param: Schema schema: Schema to test
        :return: True if compatible, False if not compatible
        :rtype: bool
        """

        url = '/'.join([self.url, 'compatibility', 'subjects',
                        subject, 'versions', str(version)])
        body = {'schema': str(schema)}
        try:
            return self._send_request(url, method='POST', body=body).get('is_compatible')
        except ClientError as e:
            log.error("Failed to test compatibility {}".format(e.message), e.http_code)
            return False

    def update_compatibility(self, level, subject=None):
        """
        PUT /config/(string: subject)

        Update the compatibility level for a subject.  Level must be one of:

        :param str level: ex: 'NONE','FULL','FORWARD', or 'BACKWARD'
        :param str subject: Subject name
        """

        if level not in VALID_LEVELS:
            raise ClientError("Invalid level specified: {}".format(level))

        url = '/'.join([self.url, 'config'])
        if subject:
            url += '/' + subject

        body = {"compatibility": level}
        try:
            return self._send_request(url, method='PUT', body=body).get('compatibility')
        except ClientError as e:
            raise ClientError("Unable to update level {}: {}".format(e.http_code, e.message))

    def get_compatibility(self, subject=None):
        """
        GET /config
        Get the current compatibility level for a subject.  Result will be one of:

        :param str subject: subject name
        :raises ClientError: if the request was unsuccessful or an invalid compatibility level was returned
        :returns: one of 'NONE','FULL','FORWARD', or 'BACKWARD'
        :rtype: bool
        """
        url = '/'.join([self.url, 'config'])
        if subject:
            url = '/'.join([url, subject])

        try:
            return self._send_request(url).get('compatibilityLevel', None)
        except ClientError as e:
            raise ClientError(
                'Failed to fetch compatibility level {}: {}'.format(e.http_code, e.message))


class SubjectNameStrategy(object):

    def __call__(self, schema, ctx):
        """
        Provides the subject name to register a schema under.

        :param SerializationContext ctx:
        :param Schema schema: unused

        :returns: subject name
        :rtype: str
        """
        raise NotImplementedError("Subject name strategy hook must be callable")


class TopicNameStrategy(SubjectNameStrategy):
    """

    :param SerializationContext ctx: metadata about the serialization operation
    :param Schema schema: parsed schema

    :return: subject name
    :rtype: str
    """

    def __call__(self, schema, ctx):
        return ctx.topic + "-" + str(ctx.field)


class TopicRecordNameStrategy(SubjectNameStrategy):
    """

    :param SerializationContext ctx: metadata about the serialization operation
    :param Schema schema: parsed schema

    :return: subject name
    :rtype: str
    """

    def __call__(self, schema, ctx):
        return ctx.topic + "-" + schema.name


class RecordNameStrategy(SubjectNameStrategy):
    """

    :param SerializationContext ctx: metadata about the serialization operation
    :param Schema schema: parsed schema

    :return: subject name
    :rtype: str
    """

    def __call__(self, schema, ctx):
        return schema.name
