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
from copy import deepcopy

from .cimpl import Producer as _cProducer, KafkaError
from .serialization import SerializationContext, MessageField, SerializerError


class Producer(_cProducer):
    """
    A Kafka client that publishes records to the Kafka cluster.

    The Producer is thread safe and sharing a single Producer instance across
    threads will generally be faster than having multiple instances.

    The following Producer example send records with strings
    containing sequential numbers as the key/value pairs.

    .. code-block:: python
        :linenos:

        -*- coding: utf-8 -*-
        from confluent_kafka import Producer, \\
            StringSerializer, IntegerSerializer


        def delivery_report(err, msg):
            if err is not None:
                print('Message delivery failed: {}'.format(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(),
                                                            msg.partition()))

        # key_serializer, value_serializer require v1.4.0+
        p = Producer({'bootstrap.servers': 'mybroker1,mybroker2',
                      'message.timeout.ms': 500},
                     key_serializer=StringSerializer(),
                     value_serializer=IntegerSerializer())

        for i in range(0, 100):
            p.produce('mytopic', key=str(i), value=i, on_delivery=delivery_report)
            p.poll(0.0)
        p.flush()

    The produce() method is asynchronous. When called it adds the record to a
    buffer of pending records and immediately returns. This allows the Producer
    to batch together individual records for efficiency.

    The Producer will automatically retry failed produce requests up to
    `retries` or `message.timeout.ms` (whichever comes first).

    .. versionadded:: 1.0.0

        Setting `enable.idempotence: True` enables Producer Idempotence which
        provides guaranteed ordering and exactly-once producing.

    .. versionadded:: 1.4.0

        The Transactional Producer allows an application to send messages to
        multiple partitions (and topics) atomically.

        Setting `transactional.id=<str transaction id>` will enable the
        Transactional Producer. If `transactional.id` is set, Producer
        Idempotence is automatically enabled.

        The `key_serializer` and `value_serializer` classes instruct the producer
        on how to convert the message payload to bytes.

    See Also:
        Detailed Producer configuration parameters can be found at
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    Keyword Arguments:
        conf (dict): Producer configuration
        key_serializer (Serializer, optional): The serializer for key that implements
            Serializer. Defaults to None
        value_serializer (Serializer, optional): The serializer for value that
            implements Serializer. Defaults to None
    """

    def __new__(cls, *args, **kwargs):
        """
        A Kafka client that publishes records to the Kafka cluster.

        """
        if 'key_serializer' in kwargs or 'value_serializer' in kwargs:
            return super(Producer, cls).__new__(SerializingProducer,
                                                *args, **kwargs)

        return super(Producer, cls).__new__(cls, *args, **kwargs)


def _serializing_dr_cb(fn, key_serializer=None, value_serializer=None):
    def dr_decorator(err, msg):
        if err:
            return fn(err, None)
        try:
            if value_serializer:
                msg.set_value(value_serializer.deserialize(msg.value(), None))
        except SerializerError:
            return fn(KafkaError._KEY_DESERIALIZATION, None)

        try:
            if key_serializer:
                msg.set_key(key_serializer.deserialize(msg.key(), None))
        except SerializerError:
            return fn(KafkaError._VALUE_DESERIALIZATION, None)

        return fn(None, msg)
    return dr_decorator


class SerializingProducer(Producer):
    __slots__ = ['_key_serializer', '_value_serializer']

    def __new__(cls, conf, key_serializer=None, value_serializer=None,
                error_cb=None, log_cb=None, stats_cb=None, throttle_cb=None):

        raise TypeError("SerializingProducer is a non user-instantiable class")

    def __init__(self, conf, key_serializer=None, value_serializer=None,
                 error_cb=None, log_cb=None, stats_cb=None, throttle_cb=None):
        """

        :param conf:
        :param key_serializer:
        :param value_serializer:
        """

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

        producer_config = deepcopy(conf)
        producer_config.update({
            'error_cb': error_cb,
            'log_cb': log_cb,
            'throttle_cb': throttle_cb,
            'stats_cb': stats_cb
        })

        super(SerializingProducer, self).__init__(producer_config)

    def produce(self, topic, value=None, key=None, partition=-1,
                on_delivery=None, timestamp=0, headers=None):

        ctx = SerializationContext(topic, MessageField.KEY)
        if self._key_serializer is not None:
            key = self._key_serializer.serialize(key, ctx)
        if self._value_serializer:
            value = self._value_serializer.serialize(value, ctx)

        if on_delivery:
            on_delivery = _serializing_dr_cb(on_delivery,
                                             key_serializer=self._key_serializer,
                                             value_serializer=self._value_serializer)

        super(SerializingProducer, self).produce(topic, value, key,
                                                 headers=headers,
                                                 partition=partition,
                                                 timestamp=timestamp,
                                                 on_delivery=on_delivery)
