#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2019 Confluent Inc.
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


# This is a simple example of a `read-process-write` application
# using Kafka's transaction API.
#
# See the following blog for additional information.
# https://www.confluent.io/blog/transactions-apache-kafka/
#


"""
The following example demonstrates how to perform a consume-transform-produce loop with exactly-once semantics.

In order to achieve exactly-once semantics we use the idempotent producer with transactions enabled and a single
transaction aware consumer.

The following assumptions apply to the source data(input_topic below):
    1. The input source, input_topic below, was populated using a transactional producer
    2. There are no duplicates in the input topic

##  A quick note about exactly-once-processing guarantees and Kafka. ##

Exactly once, and idempotence, guarantees start after the producer has been provided a record. There is no way for a
producer to identify a record as a duplicate in isolation. Instead it is the application's job to ensure that only a
single copy of any record is passed to the producer.

Special care needs to be taken when expanding the consumer group to multiple members.
Review KIP-447 for complete details.
"""

from base64 import b64encode

from confluent_kafka import Producer, Consumer, KafkaError


def process_input(msg):
    """
    Base64 encodes msg key/value contents
    :param msg:
    :returns: transformed key, value
    :rtype: tuple
    """

    key, value = None, None
    if msg.key() is not None:
        key = b64encode(msg.key())
    if msg.value() is not None:
        value = b64encode(msg.value())

    return key, value


def delivery_report(err, msg):
    """
    Reports message delivery status; success or failure
    :param KafkaError err: reason for delivery failure
    :param Message msg:
    :returns: None
    """
    if err:
        print('Message delivery failed ({} [{}]): {}'.format(msg.topic(), str(msg.partition()), err))
    else:
        print('Message delivered to {} [{}] at offset [{}]: {} | {}'.format(msg.topic(), msg.partition(),
                                                                            msg.offset(), msg.key(), msg.value()))


if __name__ == "__main__":
    brokers = "PLAINTEXT://mybroker:9092"

    group_id = "eos-example-consumer"
    consumer = Consumer({
        'bootstrap.servers': brokers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': True,
    })

    consumer.subscribe(['input_topic'])

    producer = Producer({
        'bootstrap.servers': brokers,
        'transactional.id': 'example_transactional_id',
    })

    # Initialize producer transaction.
    producer.init_transactions()
    # Start producer transaction.
    producer.begin_transaction()

    eof = {}
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof[(msg.topic(), msg.partition())] = True
                if len(eof) == len(consumer.assignment()):
                    break
            continue

        processed_value, processed_key = process_input(msg)

        producer.produce("output_topic", processed_value, processed_key, on_delivery=delivery_report)
        producer.poll()

    # commit processed messages to the transaction
    producer.send_offsets_to_transaction(group_id, consumer.position(consumer.assignment()))
    # commit transaction
    producer.commit_transaction()

    consumer.close()
