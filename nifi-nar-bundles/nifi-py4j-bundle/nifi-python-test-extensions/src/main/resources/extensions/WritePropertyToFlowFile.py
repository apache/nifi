from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor
from nifiapi.properties import StandardValidators
from nifiapi.properties import ExpressionLanguageScope

class WritePropertyToFlowFile(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'

    def __init__(self, jvm):
        self.message = PropertyDescriptor(
            name = 'Message',
            description = 'What to write to FlowFile',
            expression_language_scope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
            validators = [StandardValidators.ALWAYS_VALID]
        )
        self.properties = [self.message]

    def transform(self, context, flowFile):
        msg = context.getProperty(self.message.name).evaluateAttributeExpressions(flowFile).getValue()
        return FlowFileTransformResult(relationship = "success", contents = str.encode(msg))

    def getPropertyDescriptors(self):
        return self.properties