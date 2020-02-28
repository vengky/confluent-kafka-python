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
import sys
from uuid import uuid1

import pytest

from confluent_kafka.serialization import DoubleSerializer, IntegerSerializer, LongSerializer, \
    ShortSerializer, StringSerializer, FloatSerializer


@pytest.mark.parametrize("serializer, deserializer, data",
                         [(DoubleSerializer, "DoubleDeserializer",
                           random.uniform(-32768.0, 32768.0)),
                          (FloatSerializer, "FloatDeserializer", random.uniform(-32768.0, 32768.0)),
                          (LongSerializer, "LongDeserializer", random.getrandbits(63)),
                          (IntegerSerializer, "IntegerDeserializer", random.randint(-32768, 32768)),
                          (ShortSerializer, "ShortDeserializer", random.randint(-32768, 32768)),
                          (StringSerializer('utf_8'), "StringDeserializer", u'Jämtland')])
def test_python_2_java_serialization(kafka_cluster, serializer, deserializer, data):
    topic = kafka_cluster.create_topic("serialization-numeric{}".format(str(uuid1())),
                                       {'num_partitions': 1})

    producer = kafka_cluster.producer(value_serializer=serializer)
    producer.produce(topic, data)
    producer.flush()

    consumer = kafka_cluster.java_consumer(value_serializer=deserializer)
    consumer.subscribe([topic])

    msg = consumer.poll(10.0)

    if deserializer == "FloatDeserializer":
        assert data == pytest.approx(float(msg.value()))
    elif deserializer == "DoubleDeserializer":
        assert data == float(msg.value())
    elif deserializer == "StringDeserializer":
        if sys.version_info >= (3, 5):
            assert data == msg.value()
        else:
            assert data.encode('utf_8') == msg.value()
    else:
        assert data == int(msg.value())

    consumer.close()


@pytest.mark.parametrize("serializer, deserializer, data",
                         [("DoubleSerializer", DoubleSerializer, random.uniform(-32768.0, 32768.0)),
                          ("FloatSerializer", FloatSerializer, random.uniform(-32768.0, 32768.0)),
                          ("LongSerializer", LongSerializer, random.getrandbits(63)),
                          ("IntegerSerializer", IntegerSerializer, random.randint(-32768, 32768)),
                          ("ShortSerializer", ShortSerializer, random.randint(-32768, 32768)),
                          ("StringSerializer", StringSerializer('utf_8'), u"Härjedalen")])
def test_java_2_python_serialization(kafka_cluster, serializer, deserializer, data):
    topic = kafka_cluster.create_topic("deserialization-numeric")

    producer = kafka_cluster.java_producer(value_serializer=serializer)

    producer.produce(topic, repr(data))
    producer.flush(10.0)

    consumer = kafka_cluster.consumer(value_serializer=deserializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    if serializer == "FloatSerializer":
        assert data == pytest.approx(msg.value())
    elif serializer == "StringSerializer":
        if sys.version_info >= (3, 5):
            assert data == msg.value()
        else:
            assert data.encode('utf_8') == msg.value()
    else:
        assert data == msg.value()

    consumer.close()
