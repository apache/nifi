import json
from abc import ABC, abstractmethod
from nifiapi.properties import ProcessContext
from nifiapi.__jvm__ import JvmHolder

class RecordTransform(ABC):
    def __init__(self, **kwargs):
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
        return self.processor_result.partition



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
