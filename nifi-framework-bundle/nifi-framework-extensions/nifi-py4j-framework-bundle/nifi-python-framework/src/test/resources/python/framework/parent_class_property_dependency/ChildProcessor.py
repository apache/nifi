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

"""
A child processor that uses multiple inheritance with a Python parent class.

This fixture tests the pattern that caused failures in NiFi 2.1.0+:
  - ParentProcessorClass is imported as a class (not its individual properties)
  - CHILD_ONLY_PROPERTY references ParentProcessorClass.PARENT_ENABLE_FEATURE
    via PropertyDependency(ParentProcessorClass.PARENT_ENABLE_FEATURE, "true")
  - The AST for that argument is ast.Attribute (not ast.Name), which previously
    caused AttributeError: 'Attribute' object has no attribute 'id'
  - After the crash fix, the property still could not be resolved, causing a
    warning and the dependency being silently dropped
"""

from ParentProcessorClass import ParentProcessorClass

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, PropertyDependency, StandardValidators


class ChildProcessor(ParentProcessorClass, FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Test processor for parent-class PropertyDependency resolution'
        tags = ['test', 'inheritance', 'dependency']

    # This property depends on a property defined in ParentProcessorClass.
    # The dependency is written as ParentProcessorClass.PARENT_ENABLE_FEATURE,
    # which produces an ast.Attribute node — the pattern that caused the crash.
    CHILD_ONLY_PROPERTY = PropertyDescriptor(
        name="Child Only Setting",
        description="A setting only visible when Enable Feature is true",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        dependencies=[PropertyDependency(ParentProcessorClass.PARENT_ENABLE_FEATURE, "true")]
    )

    property_descriptors = [
        ParentProcessorClass.PARENT_ENABLE_FEATURE,
        CHILD_ONLY_PROPERTY,
    ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        return FlowFileTransformResult(relationship='success')
