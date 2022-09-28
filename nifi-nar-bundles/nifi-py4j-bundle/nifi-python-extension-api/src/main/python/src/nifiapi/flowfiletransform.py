from abc import ABC, abstractmethod
from nifiapi.__jvm__ import JvmHolder
from nifiapi.properties import ProcessContext


class FlowFileTransform(ABC):
    def __init__(self, **kwargs):
        self.arrayList = JvmHolder.jvm.java.util.ArrayList

    def setContext(self, context):
        self.process_context = ProcessContext(context)

    def transformFlowFile(self, flowfile):
        return self.transform(self.process_context, flowfile)

    @abstractmethod
    def transform(self, context, flowFile):
        pass


class FlowFileTransformResult:
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransformResult']

    def __init__(self, relationship, attributes = None, contents = None):
        self.relationship = relationship
        self.attributes = attributes
        if contents is not None and isinstance(contents, str):
            self.contents = str.encode(contents)
        else:
            self.contents = contents

    def getRelationship(self):
        return self.relationship

    def getContents(self):
        return self.contents

    def getAttributes(self):
        if self.attributes is None:
            return None

        map = JvmHolder.jvm.java.util.HashMap()
        for key, value in self.attributes.items():
            map.put(key, value)

        return map