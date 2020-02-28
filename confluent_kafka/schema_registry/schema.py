#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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

from fastavro._schema_common import PRIMITIVES
from fastavro.schema import parse_schema, schema_name


class Schema(object):
    """
    Schema objects encapsulate serialization metadata used by serializers
    to register, encode and decode objects.

    Args:
        schema (:dict:`str`): Schema definition
        id (int, optional): Schema Registration ID. Defaults to -1(unregistered)

    Keyword Arguments:
        schema_type (str, optional): Schema type to return. Defaults to AVRO

    raises: SchemaParseException if the schema can't be parsed.
    """

    def __new__(cls, schema, id=-1, *args, **kwargs):
        schema_type = kwargs.pop('schemaType', None)
        if schema_type is None or schema_type == 'AVRO':
            return super(Schema, cls).__new__(AvroSchema, *args, **kwargs)
        else:
            raise TypeError("Unrecognized schema_type")


class AvroSchema(Schema):
    schema_type = "AVRO"

    def __new__(cls, *args, **kwargs):
        raise TypeError("AvroSchema is a non user-instantiable class")

    def __init__(self, schema, id=-1):
        """
        Avro Schema definitions and accompanying Schema Registry metadata.

        :param str|dict schema: Parsed Schema to be used when serializing objects

        :raises: SchemaParseException if schema can't be parsed
        """
        self.id = id
        self.subjects = set()

        if isinstance(schema, dict):
            self.schema = parse_schema(schema)
        else:
            # FastAvro does not support parsing schemas in their canonical form
            if schema.lstrip()[0] != "{":
                schema = '{{"type": {} }}'.format(schema)

            self.schema = parse_schema(json.loads(schema))

        if self.schema['type'] in PRIMITIVES:
            self.name = self.schema["type"]
        else:
            self.name = schema_name(self.schema, None)[1]

        # schema should be treated as an immutable so there is no need to recalculate the hash.
        self._hash = hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)

    def __str__(self):
        return json.dumps(
            {key: value for key, value in self.schema.items()
             if key != "__fastavro_parsed"})

    # dicts aren't hashable
    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def load(schema_avsc):
        """
        Create Schema Object from an avsc file

        :param schema_avsc: Path to schema file to avsc file

        :raises: SchemaParseException if schema can't be parsed

        :returns: Parsed Avro Schema
        :rtype: Schema
        """
        with open(schema_avsc) as fd:
            schema = json.load(fd)

        return Schema(schema)

    @staticmethod
    def loads(schema_def):
        """
        Create Schema object from a string

        This method exists for backward compatibility reasons only.
        An instance of AvroSchema can be instantiated from a schema's raw
        string representation.

        :param str schema_def: Schema string to be parsed

        :raises: SchemaParseException if schema can't be parsed

        :returns: Parsed Avro Schema
        :rtype: AvroSchema
        """
        if schema_def.lstrip()[0] != "{":
            schema_def = '{{"type": {} }}'.format(schema_def)

        return Schema(json.loads(schema_def))
