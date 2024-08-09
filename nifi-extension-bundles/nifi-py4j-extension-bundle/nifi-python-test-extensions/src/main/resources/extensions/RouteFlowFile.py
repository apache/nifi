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
from nifiapi.relationship import Relationship

class RouteFlowFile(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = "Routes a FlowFile to 'small' or 'large' based on whether the size of the flowfile exceeds 50 KB"

    REL_SMALL = Relationship(name="small", description="FlowFiles smaller than 50 KB")
    REL_LARGE = Relationship(name="large", description="FlowFiles larger than 50 KB")

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        size = flowFile.getSize()
        if size > 50000:
            return FlowFileTransformResult(relationship = "large")
        else:
            return FlowFileTransformResult(relationship = "small")

    def getRelationships(self):
        return [self.REL_SMALL, self.REL_LARGE]
