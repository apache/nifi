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
from abc import ABC, abstractmethod
from nifiapi.properties import ProcessContext
from nifiapi.__jvm__ import JvmHolder

class RecordTransform(ABC):
    # These will be set by the PythonProcessorAdapter when the component is created
    identifier = None
    logger = None

    def __init__(self):
        self.arrayList = JvmHolder.jvm.java.util.ArrayList

    def setContext(self, context):
        self.process_context = ProcessContext(context)

    def transformRecord(self, jsonarray, schema, attributemap):
        parsed_array = json.loads(jsonarray)
        results = self.arrayList()
        caching_attribute_map = CachingAttributeMap(attributemap)

        for record in parsed_array:
            result = self.transform(self.process_context, record, schema, caching_attribute_map)
            result_record = result.getRecord()
            resultjson = None if result_record is None else json.dumps(result_record)
            results.add(__RecordTransformResult__(result, resultjson))

        return results


    @abstractmethod
    def transform(self, context, record, schema, attributemap):
        pass


class CachingAttributeMap:
    cache = None

    def __init__(self, delegate):
        self.delegate = delegate

    def getAttribute(self, attributeName):
        # Lazily initialize cache
        if self.cache is None:
            self.cache = {}
        if attributeName in self.cache:
            return self.cache[attributeName]

        value = self.delegate.getAttribute(attributeName)
        self.cache[attributeName] = value
        return value

    def getAttributes(self):
        return self.delegate.getAttributes()


class __RecordTransformResult__:
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransformResult']

    def __init__(self, processor_result, recordJson):
        self.processor_result = processor_result
        self.recordJson = recordJson

    def getRecordJson(self):
        return self.recordJson

    def getSchema(self):
        return self.processor_result.schema

    def getRelationship(self):
        return self.processor_result.relationship

    def getPartition(self):
        if self.processor_result.partition is None:
            return None

        map = JvmHolder.jvm.java.util.HashMap()
        for key, value in self.processor_result.partition.items():
            map.put(key, value)

        return map


class RecordTransformResult:

    def __init__(self, record=None, schema=None, relationship="success", partition=None):
        self.record = record
        self.schema = schema
        self.relationship = relationship
        self.partition = partition

    def getRecord(self):
        return self.record

    def getSchema(self):
        return self.schema

    def getRelationship(self):
        return self.relationship

    def getPartition(self):
        return self.partition
