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