from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import numpy as np

class WriteNumpyVersion(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        dependencies = ['numpy==1.20.0']
        version = '0.0.1-SNAPSHOT'

    def __init__(self, jvm):
        pass

    def transform(self, context, flowFile):
        version = np.version.version
        return FlowFileTransformResult(relationship = "success", contents = str.encode(version))
