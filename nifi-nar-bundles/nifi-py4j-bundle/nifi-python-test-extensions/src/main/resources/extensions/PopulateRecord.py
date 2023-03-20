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

from nifiapi.recordtransform import RecordTransformResult
from nifiapi.recordtransform import RecordCodec
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class PopulateRecord(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'


    def __init__(self, **kwargs):
        self.descriptors = []


    def transform(self, context, record, attributemap):
        rec = RecordCodec().record_to_dict(record)
        rec['int'] = 4
        rec['str'] = 'Hello there'
        rec['child'] = { 'child1': 4, 'child2': 'hi' }
        rec['array'] = ['abc', 'xyz']
        result = RecordCodec().dict_to_map(rec)

        return RecordTransformResult(record=result, relationship ='success')


    def getPropertyDescriptors(self):
        return self.descriptors
