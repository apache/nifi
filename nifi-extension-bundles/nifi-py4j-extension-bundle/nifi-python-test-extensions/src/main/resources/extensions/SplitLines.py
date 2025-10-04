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


class SplitLines(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Splits the incoming FlowFile into one FlowFile per line of text.'
        tags = ['split', 'line', 'test', 'python']

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        contents = flowFile.getContentsAsBytes().decode('utf-8')
        if not contents:
            return []

        results = []
        for index, line in enumerate(contents.splitlines()):
            attributes = {
                'split.index': str(index),
                'split.line.length': str(len(line))
            }
            results.append(FlowFileTransformResult(relationship='success', attributes=attributes, contents=line))
        return results
