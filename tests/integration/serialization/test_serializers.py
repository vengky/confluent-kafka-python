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

import random

import pytest

from confluent_kafka import \
    DoubleSerializer, IntegerSerializer, LongSerializer, \
    ShortSerializer, StringSerializer, FloatSerializer, AvroSerializer


@pytest.mark.parametrize("serializer, data",
                         [(DoubleSerializer(), random.uniform(-32768.0, 32768.0)),
                          (LongSerializer(), random.getrandbits(63)),
                          (IntegerSerializer(), random.randint(-32768, 32768)),
                          (ShortSerializer(), random.randint(-32768, 32768)),
                          (DoubleSerializer(), None),
                          (LongSerializer(), None),
                          (IntegerSerializer(), None),
                          (ShortSerializer(), None)])
def test_numeric_serialization(kafka_cluster, serializer, data):
    """
    Tests basic serialization/deserialization of numeric types.

    :param KafkaClusterFixture kafka_cluster: cluster fixture
    :param Serializer serializer: serializer to test
    :param object data: input data

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = kafka_cluster.create_topic("serialization-numeric")

    producer = kafka_cluster.producer(value_serializer=serializer)
    producer.produce(topic, data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_serializer=serializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.value() == data

    consumer.close()


@pytest.mark.parametrize("data",
                         [random.uniform(-1.0, 1.0),
                          None])
def test_float_serialization(kafka_cluster, data):
    """
    Tests basic float serialization/deserialization functionality.

    This test must be separated from the standard numeric types due
    to the nature of floats in Python. Some precision loss occurs
    when converting python's double-precision float to the expected
    single-precision float value. As such we only test that the
    single-precision approximated values is reasonably close to the original.

    :param KafkaClusterFixture kafka_cluster: cluster fixture
    :param float data: test data

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = kafka_cluster.create_topic("serialization-float")

    producer = kafka_cluster.producer(key_serializer=FloatSerializer())

    data = random.uniform(-1.0, 1.0)
    producer.produce(topic, key=data)
    producer.flush()

    consumer = kafka_cluster.consumer(key_serializer=FloatSerializer())
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert data == pytest.approx(msg.key())

    consumer.close()


@pytest.mark.parametrize("data, codec",
                         [(u'Jämtland', 'utf_8'),
                          (u'Härjedalen', 'utf_16'),
                          (None, 'utf_32')])
def test_string_serialization(kafka_cluster, data, codec):
    """
    Tests basic unicode serialization/deserialization functionality

    :param KafkaClusterFixture kafka_cluster: cluster fixture
    :param unicode data: input data
    :param str codec: encoding type

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = kafka_cluster.create_topic("serialization-string")

    producer = kafka_cluster.producer(value_serializer=StringSerializer(codec))

    producer.produce(topic, value=data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_serializer=StringSerializer(codec))

    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.value() == data

    consumer.close()


@pytest.mark.parametrize("key_serializer, value_serializer, key, value",
                         [(DoubleSerializer(), StringSerializer('utf_8'),
                           random.uniform(-32768.0, 32768.0), u'Jämtland'),
                          (StringSerializer('utf_32'), LongSerializer(),
                           u'Härjedalen', random.getrandbits(63)),
                          (IntegerSerializer(), ShortSerializer(),
                           random.randint(-32768, 32768), random.randint(-32768, 32768))])
def test_mixed_serialization(kafka_cluster, key_serializer, value_serializer, key, value):
    """pyte
    Tests basic mixed serializer/deserializer functionality.

    :param KafkaClusterFixture kafka_cluster: cluster fixture
    :param Serializer key_serializer: key serializer
    :param Serializer value_serializer: value serializer
    :param object key: key data
    :param object value: value data

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = kafka_cluster.create_topic("serialization-numeric")

    producer = kafka_cluster.producer(key_serializer=key_serializer,
                                      value_serializer=value_serializer)
    producer.produce(topic, key=key, value=value)
    producer.flush()

    consumer = kafka_cluster.consumer(key_serializer=key_serializer,
                                      value_serializer=value_serializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.key() == key
    assert msg.value() == value

    consumer.close()


def test_batch_deserialization(kafka_cluster):
    """
    Tests basic SerializedConsumer consume functionality with the DoubleSerializer

    :param KafkaClusterFixture kafka_cluster: cluster fixture

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = kafka_cluster.create_topic("deserialization-batch")

    value_serializer = DoubleSerializer()
    producer = kafka_cluster.producer(value_serializer=value_serializer)

    num_msg = 1000
    data_in = [random.uniform(-32768.0, 32768.0) for x in range(num_msg)]

    for data in data_in:
        producer.produce(topic, value=data)

    consumer = kafka_cluster.consumer(value_serializer=value_serializer)
    consumer.subscribe([topic])

    data_out = []
    for msg in consumer.consume(num_messages=num_msg):
        data_out.append(msg.value())

    assert data_in.sort() == data_out.sort()


@pytest.mark.parametrize("avsc, data",
                         [('basic_schema.avsc', {'name': 'abc'}),
                          ('primitive_string.avsc', u'Jämtland'),
                          ('primitive_bool.avsc', True),
                          ('primitive_float.avsc', random.uniform(-32768.0, 32768.0)),
                          ('primitive_double.avsc', random.uniform(-32768.0, 32768.0))])
def test_avro_record_serialization(kafka_cluster, load_avsc, avsc, data):
    """
    Tests basic Avro serializer functionality

    :param KafkaClusterFixture kafka_cluster: cluster fixture
    :param callable(str) load_avsc: Avro file reader
    :param str avsc: Avro schema file
    :param object data: data to be serialized

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = kafka_cluster.create_topic("serialization-avro")
    schema = load_avsc(avsc)

    value_serializer = AvroSerializer(kafka_cluster.schema_registry(),
                                      schema=load_avsc(avsc))

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    producer.produce(topic, data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_serializer=value_serializer)
    consumer.subscribe([topic])
    msg = consumer.poll()

    actual = msg.value()
    # schema may include default which need not exist in the original
    schema_type = schema.schema['type']
    if schema_type == "record":
        assert [v == actual[k] for k, v in data.items()]
    elif schema_type == 'float':
        print("{} - {}".format(actual, data))
        assert data == pytest.approx(actual)
    else:
        assert actual == data


@pytest.mark.parametrize("avsc, data",
                         [('basic_schema.avsc', {'name': 'abc'}),
                          ('primitive_string.avsc', u'Jämtland'),
                          ('primitive_bool.avsc', True),
                          ('primitive_float.avsc', random.uniform(-32768.0, 32768.0)),
                          ('primitive_double.avsc', random.uniform(-32768.0, 32768.0))])
def test_delivery_report_serialization(kafka_cluster, load_avsc, avsc, data):
    """
    Tests basic Avro serializer functionality

    :param KafkaClusterFixture kafka_cluster: cluster fixture
    :param callable(str) load_avsc: Avro file reader
    :param str avsc: Avro schema file
    :param object data: data to be serialized

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """

    def assert_cb(err, msg):
        actual = msg.value()

        assert type(actual) == type(data)

        schema_type = schema.schema['type']
        if schema_type == "record":
            assert [v == actual[k] for k, v in data.items()]
        elif schema_type == 'float':
            print("{} - {}".format(actual, data))
            assert data == pytest.approx(actual)
        else:
            assert actual == data

    topic = kafka_cluster.create_topic("serialization-avro")
    schema = load_avsc(avsc)

    value_serializer = AvroSerializer(kafka_cluster.schema_registry(),
                                      schema=load_avsc(avsc))

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    producer.produce(topic, data, on_delivery=assert_cb)
    producer.flush()

    consumer = kafka_cluster.consumer(value_serializer=value_serializer)
    consumer.subscribe([topic])
    msg = consumer.poll()

    actual = msg.value()
    # schema may include default which need not exist in the original
    schema_type = schema.schema['type']
    if schema_type == "record":
        assert [v == actual[k] for k, v in data.items()]
    elif schema_type == 'float':
        print("{} - {}".format(actual, data))
        assert data == pytest.approx(actual)
    else:
        assert actual == data
