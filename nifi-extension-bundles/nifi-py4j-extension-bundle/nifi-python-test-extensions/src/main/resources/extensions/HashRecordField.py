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

from glom import assign, glom
from glom.core import GlomError

from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.recordtransform import RecordTransform, RecordTransformResult


_MISSING = object()


class HashRecordField(RecordTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Hashes a record field using SHA-256.'
        tags = ['record', 'hash', 'security']
        dependencies = ['glom == 24.11.0']

    def __init__(self, **kwargs):
        super().__init__()
        self.record_path = PropertyDescriptor(
            name='Record Path',
            description='Glom path identifying the field that should be hashed (for example "customer.address.street").',
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

        path_spec = record_path_value.strip()
        if not path_spec:
            self.logger.error('Record Path {} is empty after trimming whitespace'.format(record_path_value))
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        try:
            current_value = glom(record, path_spec, default=_MISSING)
        except GlomError as exc:
            self.logger.error('Failed to resolve path {}: {}'.format(path_spec, exc))
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        if current_value is _MISSING:
            self.logger.warn('Record Path {} did not resolve to a value; record left unchanged'.format(path_spec))
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        if current_value is None:
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        if isinstance(current_value, (dict, list)):
            self.logger.warn('Record Path {} resolved to a non-scalar value; skipping hash operation'.format(path_spec))
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        try:
            value_as_string = current_value if isinstance(current_value, str) else str(current_value)
            hashed = hashlib.sha256(value_as_string.encode('utf-8')).hexdigest()
        except Exception as exc:
            self.logger.error('Failed to hash value at {}: {}'.format(path_spec, exc))
            return RecordTransformResult(record=record, schema=schema, relationship='success')

        try:
            assign(record, path_spec, hashed, missing=lambda: None)
        except GlomError as exc:
            self.logger.error('Failed to assign hashed value at {}: {}'.format(path_spec, exc))

        return RecordTransformResult(record=record, schema=schema, relationship='success')
