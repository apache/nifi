# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib

from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.recordtransform import RecordTransform, RecordTransformResult
from nifiapi import recordpath


class HashRecordField(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Hashes a record field using SHA-256.'
        tags = ['record', 'hash', 'security']

    def __init__(self, **kwargs):
        super().__init__()
        self.record_path = PropertyDescriptor(
            name='Record Path',
            description='The RecordPath identifying the field that should be hashed.',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )
        self.descriptors = [self.record_path]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, record, schema, attributemap):
        record_path_value = context.getProperty(self.record_path.name).getValue()
        if not record_path_value:
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        path_result = recordpath.evaluate(record, schema, record_path_value)
        if path_result.is_empty():
            self.logger.warn('Record Path {} did not resolve to a value; record left unchanged'.format(record_path_value))
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        for field in path_result:
            value = field.get_value()
            if value is None:
                continue
            if isinstance(value, (dict, list)):
                self.logger.warn('Record Path {} resolved to a non-scalar value; skipping hash operation'.format(record_path_value))
                continue
            try:
                value_as_string = value if isinstance(value, str) else str(value)
                hashed = hashlib.sha256(value_as_string.encode('utf-8')).hexdigest()
            except Exception as exc:
                self.logger.error('Failed to hash value at {}: {}'.format(record_path_value, exc))
                continue
            field.set_value(hashed)

        path_result.apply()

        return RecordTransformResult(record=record, schema=schema, relationship='success')
