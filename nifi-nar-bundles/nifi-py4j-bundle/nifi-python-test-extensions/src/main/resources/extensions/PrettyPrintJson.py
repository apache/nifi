import json
from nifiapi.properties import PropertyDescriptor
from nifiapi.properties import StandardValidators
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class PrettyPrintJson(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'


    def __init__(self, **kwargs):
        # Build Property Descriptors
        self.indentation = PropertyDescriptor(
            name="Indentation",
            description="Number of spaces",
            required = True,
            default_value="4",
            validators = [StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR]
        )
        self.descriptors = [self.indentation]

    def transform(self, context, flowFile):
        spaces = context.getProperty(self.indentation.name).asInteger()
        parsed = json.loads(flowFile.getContentsAsBytes())
        pretty = json.dumps(parsed, indent=spaces)

        return FlowFileTransformResult(relationship = "success", contents = str.encode(pretty))


    def getPropertyDescriptors(self):
        return self.descriptors
