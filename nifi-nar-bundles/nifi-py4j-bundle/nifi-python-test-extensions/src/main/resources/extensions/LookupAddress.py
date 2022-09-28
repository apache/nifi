import json
from nifiapi.properties import PropertyDescriptor
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class LookupAddress(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, jvm, **kwargs):
        self.jvm = jvm

        # Build Property Descriptors
        self.lookupServiceDescriptor = PropertyDescriptor(
            name = "Lookup",
            description = "The Controller Service to use for looking up values",
            required = True,
            controller_service_definition = 'StringLookupService'
        )
        self.descriptors = [self.lookupServiceDescriptor]

    def transform(self, context, flowFile):
        service = context.getProperty(self.lookupServiceDescriptor.name).asControllerService()
        coordinates = self.jvm.java.util.HashMap()

        parsed = json.loads(flowFile.getContentsAsBytes())

        if parsed['name']:
            coordinates.put('key', parsed['name'])
            optional_result = service.lookup(coordinates)
            if optional_result.isPresent():
                parsed['address'] = optional_result.get()

        enriched = json.dumps(parsed)

        return FlowFileTransformResult(relationship = "success", contents = str.encode(enriched))


    def getPropertyDescriptors(self):
        return self.descriptors
