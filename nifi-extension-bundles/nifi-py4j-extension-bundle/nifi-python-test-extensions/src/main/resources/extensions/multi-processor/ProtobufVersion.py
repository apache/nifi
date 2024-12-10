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

from google.protobuf.runtime_version import OSS_MAJOR, OSS_MINOR, OSS_PATCH
from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult

class ProtobufVersion(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        description = "Test processor which outputs the version of the protobuf library"
        version = '0.0.1-SNAPSHOT'
        tags = ['test', 'protobuf', 'version']
        dependencies = ['protobuf==5.29.1']

    def __init__(self, jvm):
        pass

    def create(self, context):
        version = f"{OSS_MAJOR}.{OSS_MINOR}.{OSS_PATCH}"
        return FlowFileSourceResult('success', contents=version)
