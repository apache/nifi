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
import os
import subprocess

class ExitAfterFourInvocations(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Transfers the first 4 FlowFiles to success, then kills the process on subsequent invocations by invoking system command kill -9 <pid>'
        dependencies = ['pandas'] # Just to have some dependency that must be downloaded to test functionality on restart

    invocations = 0

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        self.invocations += 1
        if self.invocations > 4:
            # Issue a `kill -9` to the current process
            pid = os.getpid()
            subprocess.run(['kill', '-9', str(pid)])

        return FlowFileTransformResult(relationship = "success")


    def getPropertyDescriptors(self):
        return []