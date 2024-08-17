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
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class LookupAddress(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, jvm, **kwargs):
        self.jvm = jvm

        # Build Property Descriptors
        self.lookupServiceDescriptor = PropertyDescriptor(
            name = "Lookup",
            description = "The Controller Service to use for looking up values",
            required = True,
            controller_service_definition = 'StringLookupService'
        )
        self.descriptors = [self.lookupServiceDescriptor]

    def transform(self, context, flowFile):
        service = context.getProperty(self.lookupServiceDescriptor.name).asControllerService()
        coordinates = self.jvm.java.util.HashMap()

        parsed = json.loads(flowFile.getContentsAsBytes())

        if parsed['name']:
            coordinates.put('key', parsed['name'])
            optional_result = service.lookup(coordinates)
            if optional_result.isPresent():
                parsed['address'] = optional_result.get()

        enriched = json.dumps(parsed)

        return FlowFileTransformResult(relationship = "success", contents = str.encode(enriched))


    def getPropertyDescriptors(self):
        return self.descriptors
