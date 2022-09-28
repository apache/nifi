from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class LogContents(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        contents = flowFile.getContentsAsBytes().decode("utf-8")
        self.logger.info(contents)
        return FlowFileTransformResult(relationship = "success")


    def getPropertyDescriptors(self):
        return []