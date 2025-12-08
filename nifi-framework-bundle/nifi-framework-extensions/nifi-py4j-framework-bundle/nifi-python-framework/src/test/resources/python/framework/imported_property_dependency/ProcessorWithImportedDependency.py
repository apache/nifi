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
This processor demonstrates the bug described in NIFI-14233:
When a PropertyDescriptor is imported from another module and used
as a PropertyDependency, NiFi fails to load the processor with a KeyError.

The issue is that the AST-based ProcessorInspection only discovers
PropertyDescriptors defined within the current class, not imported ones.
"""

from SharedProperties import SHARED_OUTPUT_FORMAT, SHARED_FEATURE_ENABLED
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, PropertyDependency, StandardValidators, ExpressionLanguageScope


class ProcessorWithImportedDependency(FlowFileTransform):
    """
    A test processor that imports properties from a shared module
    and uses them as dependencies for other properties.
    """
    
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Test processor for NIFI-14233 - uses imported properties as dependencies'
        tags = ['test', 'dependency', 'import']

    # This property depends on an IMPORTED property (SHARED_OUTPUT_FORMAT)
    # This is where the bug manifests - the AST inspection can't find
    # SHARED_OUTPUT_FORMAT in discovered_property_descriptors because
    # it's defined in a different module
    JSON_PRETTY_PRINT = PropertyDescriptor(
        name="Pretty Print JSON",
        description="Whether to pretty-print JSON output (only applies when Output Format is 'json')",
        allowable_values=["true", "false"],
        default_value="false",
        required=False,
        dependencies=[PropertyDependency(SHARED_OUTPUT_FORMAT, "json")],
        validators=[StandardValidators.BOOLEAN_VALIDATOR]
    )

    # Another property that depends on the imported SHARED_FEATURE_ENABLED
    FEATURE_CONFIG = PropertyDescriptor(
        name="Feature Configuration",
        description="Configuration for the optional feature (only shown when Feature Enabled is 'true')",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        dependencies=[PropertyDependency(SHARED_FEATURE_ENABLED, "true")],
        validators=[StandardValidators.NON_EMPTY_VALIDATOR]
    )

    def __init__(self, **kwargs):
        super().__init__()
        self.descriptors = [
            SHARED_OUTPUT_FORMAT,
            SHARED_FEATURE_ENABLED,
            self.JSON_PRETTY_PRINT,
            self.FEATURE_CONFIG
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        output_format = context.getProperty(SHARED_OUTPUT_FORMAT.name).getValue()
        contents = f"Output format: {output_format}"
        return FlowFileTransformResult(relationship='success', contents=contents)

