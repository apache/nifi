from nifiapi.recordtransform import RecordTransformResult
from nifiapi.recordtransform import RecordCodec
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class PopulateRecord(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.RecordTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'


    def __init__(self, jvm, **kwargs):
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
