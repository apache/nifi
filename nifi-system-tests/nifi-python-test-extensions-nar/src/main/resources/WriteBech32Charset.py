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

import bech32
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult


# A simple processor that demonstrates the ability to import and use a third-party library that is not defined as a dependency
# but that is bundled with the nar file. This processor uses the bech32 library to return the bech32.CHARSET value.
class WriteBech32Charset(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        return FlowFileTransformResult(relationship = "success", contents = bech32.CHARSET)
