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

import io
import struct

from fastavro import schemaless_writer, schemaless_reader

from .error import SerializerError
from .serializers import SerializerBase
from confluent_kafka.schema_registry.schema_registry_client import TopicNameStrategy

MAGIC_BYTE = 0


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class AvroSerializer(SerializerBase):
    """
    AvroSerializer encodes objects into the Avro binary format in accordance
    with the schema provided at instantiation.

    Unless configured otherwise the AvroSerializer will automatically register
    its schema with the provided registry instance.

    By default all Avro records must be converted from their instance representation
    to a dict prior to calling serialize. Likewise the deserialize function will
    always returns records as type dict. A custom Converter implementation may
    be provided to handle this conversion within the serializer.

    Arguments:
        registry (SchemaRegistryclient): Confluent Schema Registry client
        schema (Schema): Parsed Avro Schema object

    Keyword Arguments:
        reader_schema (Schema, optional): Optional parsed schema to project
            decoded record on. See Schema Resolution for more details. Defaults
            to None.
        name_strategy (SubjectNameStrategy, optional): Subject naming strategy.
            Defaults to TopicNameStrategy.
        converter (Converter, optional): Converter instance to handle class
            instance dict conversions. Defaults to None.

    .. _Schema Registry specification
        https://avro.apache.org/docs/current/spec.html for more details.

    .. _Schema Resolution
        https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution
    """
    def __init__(self, registry, schema,
                 reader_schema=None,
                 name_strategy=TopicNameStrategy, auto_register=True,
                 converter=None):

        self.registry = registry
        self.schema = schema
        self.reader_schema = reader_schema
        self.id_func = registry.register if auto_register else registry.check_registration
        self.name_func = name_strategy()
        self.converter = converter

    def __hash__(self):
        return hash(self.schema)

    def serialize(self, datum, ctx):
        """
        Encode datum to Avro binary format.

        Arguments:
            datum (object): Avro object to encode
            ctx (SerializationContext): Serialization context object.

        Raises:
            SerializerError if any error occurs encoding datum with the writer
            schema.

        Returns:
            bytes: Encoded Record|Primitive

        """
        if datum is None:
            return None

        if self.converter:
            datum = self.converter.to_dict(datum)

        with ContextStringIO() as fo:
            self._encode(fo, self.schema, datum, ctx)

            return fo.getvalue()

    def _encode(self, fo, schema, datum, ctx):
        """
        Encode datum on buffer.

        Arguments:
            fo (IOBytes): buffer encode datum on.
            schema (Schema): Schema definition to use when encoding the datum.
            datum (object): Record|Primitive to encode.
            ctx (SerializationContext): Serialization context object.

        Raises:
            SerializerError if an error occurs when encoding datum.

        """

        if schema is None:
            raise SerializerError("Missing schema.")

        subject = self.name_func(schema, ctx)
        self.registry.register(subject, schema)

        # Write the magic byte and schema ID in network byte order (big endian)
        fo.write(struct.pack('>bI', MAGIC_BYTE, schema.id))
        # write the record to the rest of the buffer
        schemaless_writer(fo, schema.schema, datum)

        return

    def deserialize(self, data, ctx):
        """
        Decode Avro binary to object.

        Arguments:
            data (bytes): Avro binary encoded bytes
            ctx (SerializationContext): Serialization context object.

        Raises:
            SerializerError if an error occurs ready data.

        Returns:
            object: Decoded object

        """
        if data is None:
            return None

        record = self._decode(data, self.reader_schema)

        if self.converter:
            return self.converter.from_dict(record)

        return record

    def _decode(self, data, reader_schema=None):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        Arguments:
            data (bytes | str): Avro binary to be decoded.
            reader_schema (Schema, optional): Schema to project on.

        Returns:
            object: Decoded object

        """

        if len(data) <= 5:
            raise SerializerError("message is too small to decode {}".format(data))

        with ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")

            schema = self.registry.get_by_id(schema_id)

            if reader_schema:
                return schemaless_reader(payload, schema.schema, reader_schema.schema)
            return schemaless_reader(payload, schema.schema)


class Converter(object):
    """
    Converts class instance objects to and from dict instances to enable more
    flexible Avro Record serialization/deserialization. By default the serializer
    expects all records to represented as dicts. Extending the Converter base
    enables developers to provide the serializer with instructions on how to
    convert their objects to and from dicts.

    """
    def to_dict(self, record_obj):
        """
        Converts class instance objects to dict for serialization.

        Arguments:
            record_obj (object): Instance to be converted to dict

        Returns:
            dict: dictionary representation of an Avro Record.

        """

        raise NotImplementedError

    def from_dict(self, record_dict):
        """
        Converts an Avro Record from its dict for to class instance.

        Arguments:
            record_dict (dict): dictionary representation of an Avro Record.

        Returns:
            object: A new instance of a class.

        """

        raise NotImplementedError
