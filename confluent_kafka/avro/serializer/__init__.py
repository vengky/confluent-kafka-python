#!/usr/bin/env python
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

from confluent_kafka.serialization.error import \
    SerializerError, KeySerializerError, ValueSerializerError

__all__ = ['SerializerError', 'KeySerializerError', 'ValueSerializerError']


warnings.warn(
    "Exceptions SerializerError, KeySerializerError and ValueSerializerError "
    "have been repackaged under confluent_kafka.serialization. "
    "This package will be removed in a future version",
    category=DeprecationWarning, stacklevel=2)
