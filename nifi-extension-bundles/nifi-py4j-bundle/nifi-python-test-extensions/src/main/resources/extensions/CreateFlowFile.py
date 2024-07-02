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

from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, PropertyDependency
from nifiapi.relationship import Relationship

class CreateFlowFile(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = '''A Python processor that creates FlowFiles with given contents.'''
        tags = ['text', 'test', 'python', 'source']

    FF_CONTENTS = PropertyDescriptor(
        name='FlowFile Contents',
        description='''The contents of the FlowFile.''',
        required=True,
        default_value='Hello World!'
    )

    property_descriptors = [FF_CONTENTS]

    REL_MULTILINE = Relationship(name='multiline', description='FlowFiles that contain multiline text.')

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def getRelationships(self):
        return [self.REL_MULTILINE]

    def create(self, context):
        contents = context.getProperty(self.FF_CONTENTS).getValue()

        if contents is not None and isinstance(contents, str):
            contents_str = str.encode(contents)
            if b'\n' in contents_str:
                return FlowFileSourceResult(relationship='multiline', attributes=None, contents=contents_str)

        return FlowFileSourceResult(relationship='success', attributes=None, contents=contents)
