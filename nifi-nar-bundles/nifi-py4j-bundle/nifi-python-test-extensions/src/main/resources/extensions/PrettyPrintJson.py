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

import json
from nifiapi.properties import PropertyDescriptor
from nifiapi.properties import StandardValidators
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class PrettyPrintJson(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'


    def __init__(self, **kwargs):
        # Build Property Descriptors
        self.indentation = PropertyDescriptor(
            name="Indentation",
            description="Number of spaces",
            required = True,
            default_value="4",
            validators = [StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR]
        )
        self.descriptors = [self.indentation]

    def transform(self, context, flowFile):
        spaces = context.getProperty(self.indentation.name).asInteger()
        parsed = json.loads(flowFile.getContentsAsBytes())
        pretty = json.dumps(parsed, indent=spaces)

        return FlowFileTransformResult(relationship = "success", contents = str.encode(pretty))


    def getPropertyDescriptors(self):
        return self.descriptors
