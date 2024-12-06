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

from unstructured.partition.text import partition_text
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class UnstructuredProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        description = "This processor depends on both google-cloud-vision and unstructured"
        version = '0.0.1-SNAPSHOT'
        tags = ['cloud', 'vision', 'unstructured']
        dependencies = ['unstructured==0.16.5']

    def __init__(self, jvm):
        pass

    def transform(self, context, flow_file):
        partitioned_json = partition_text(text="sample text")
        partitioned_text = partitioned_json[0].to_dict()['text']
        return FlowFileTransformResult('success', contents=partitioned_text)
