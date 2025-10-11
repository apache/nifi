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

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import (
    PropertyDescriptor,
    PropertyDependency,
    StandardValidators,
    ExpressionLanguageScope,
)


class ConditionalProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Demonstrates property dependencies by switching between text and JSON output modes.'
        tags = ['dependency', 'demo', 'python']

    def __init__(self, **kwargs):
        super().__init__()

        output_mode = PropertyDescriptor(
            name='Output Mode',
            description='Determines the format of the emitted FlowFile.',
            allowable_values=['text', 'json'],
            default_value='text',
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        uppercase = PropertyDescriptor(
            name='Uppercase',
            description='If set true the text payload is uppercased (text mode only).',
            allowable_values=['true', 'false'],
            default_value='false',
            dependencies=[PropertyDependency(output_mode, 'text')],
            validators=[StandardValidators.BOOLEAN_VALIDATOR]
        )

        json_field = PropertyDescriptor(
            name='JSON Field Name',
            description='Field name inserted into the JSON payload (json mode only).',
            required=True,
            default_value='message',
            dependencies=[PropertyDependency(output_mode, 'json')],
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        payload_text = PropertyDescriptor(
            name='Payload Text',
            description='Message written to the payload. Supports Expression Language.',
            default_value='Hello from NiFi!',
            expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
            validators=[StandardValidators.ALWAYS_VALID]
        )

        self.output_mode = output_mode
        self.uppercase = uppercase
        self.json_field = json_field
        self.payload_text = payload_text

        self.descriptors = [output_mode, uppercase, json_field, payload_text]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        message = context.getProperty(self.payload_text.name).evaluateAttributeExpressions(flowfile).getValue()
        mode = context.getProperty(self.output_mode.name).getValue()

        if mode == 'json':
            field_name = context.getProperty(self.json_field.name).getValue()
            contents = f'{{"{field_name}": "{message}"}}'
        else:
            uppercase = context.getProperty(self.uppercase.name).getValue().lower() == 'true'
            contents = message.upper() if uppercase else message

        attributes = {'output.mode': mode}
        return FlowFileTransformResult(relationship='success', attributes=attributes, contents=contents)
