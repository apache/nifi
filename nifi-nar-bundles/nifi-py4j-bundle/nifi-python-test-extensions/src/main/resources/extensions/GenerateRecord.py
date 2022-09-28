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
from faker import Faker
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class GenerateRecord(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        dependencies = ['faker']
        version = '0.0.1-SNAPSHOT'

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        faker = Faker()
        contents = {
            'name': faker.name(),
            'ssn': faker.ssn(),
            'address': faker.address()
        }

        json_string = json.dumps(contents)
        return FlowFileTransformResult(relationship = "success", contents = str.encode(json_string))


    def getPropertyDescriptors(self):
        return []