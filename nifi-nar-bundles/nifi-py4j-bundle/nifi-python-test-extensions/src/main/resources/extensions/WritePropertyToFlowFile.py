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
from nifiapi.properties import PropertyDescriptor
from nifiapi.properties import StandardValidators
from nifiapi.properties import ExpressionLanguageScope

class WritePropertyToFlowFile(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, jvm):
        self.message = PropertyDescriptor(
            name = 'Message',
            description = 'What to write to FlowFile',
            expression_language_scope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
            validators = [StandardValidators.ALWAYS_VALID]
        )
        self.properties = [self.message]

    def transform(self, context, flowFile):
        msg = context.getProperty(self.message.name).evaluateAttributeExpressions(flowFile).getValue()
        return FlowFileTransformResult(relationship = "success", contents = str.encode(msg))

    def getPropertyDescriptors(self):
        return self.properties