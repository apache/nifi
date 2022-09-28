from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

MESSAGE = "Hello, World 2"

class WriteMessage(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.2-SNAPSHOT'

    def __init__(self, jvm):
        pass

    def transform(self, context, flowFile):
        return FlowFileTransformResult(relationship = "success", contents = str.encode(MESSAGE))


    def getPropertyDescriptors(self):
        return []