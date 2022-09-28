
# PythonProcessorAdapter is responsible for receiving method invocations from Java side and delegating to the appropriate
# method for a Processor. We use this adapter instead of calling directly into the Processor because it allows us to be more
# flexible on the Python side, by allowing us to avoid implementing things like customValidate, etc.
class PythonProcessorAdapter:
    class Java:
        implements = ['org.apache.nifi.python.processor.PythonProcessorAdapter']

    def __init__(self, gateway, processor, extension_manager, controller_service_type_lookup):
        self.processor = processor
        self.gateway = gateway
        self.hasCustomValidate = self.hasMethod(processor, 'customValidate')
        self.extension_manager = extension_manager
        self.controller_service_type_lookup = controller_service_type_lookup
        self.has_properties = self.hasMethod(processor, 'getPropertyDescriptors')
        self.supportsDynamicProperties = self.hasMethod(processor, 'getDynamicPropertyDescriptor')

        if self.hasMethod(processor, 'getRelationships'):
            self.relationships = None
        else:
            self.relationships = gateway.jvm.java.util.HashSet()
            success = gateway.jvm.org.apache.nifi.processor.Relationship.Builder() \
                .name("success") \
                .description("All FlowFiles will go to this relationship") \
                .build()
            self.relationships.add(success)


    def hasMethod(self, processor, methodName):
        # Get the attribute from the given Processor with the provided method name, returning None if method is not present
        attr = getattr(processor, methodName, None)

        # Return True if the attribute is present and is a method (i.e., is callable).
        return callable(attr)

    def customValidate(self, context):
        # If no customValidate method, just return
        if not self.hasCustomValidate:
            return None

        return self.processor.customValidate(context)

    def getRelationships(self):
        # If self.relationships is None, it means that the Processor has implemented the method, and we need
        # to call the Processor's implementation. This allows for dynamically change the Relationships based on
        # configuration, etc.
        if self.relationships is None:
            return self.processor.getRelationships()
        else:
            return self.relationships

    def getSupportedPropertyDescriptors(self):
        descriptors = self.processor.getPropertyDescriptors() if self.has_properties else []
        descriptorList = self.gateway.jvm.java.util.ArrayList()
        for descriptor in descriptors:
            descriptorList.add(descriptor.to_java_descriptor(self.gateway, self.controller_service_type_lookup))

        return descriptorList

    def getProcessor(self):
        return self.processor

    def isDynamicPropertySupported(self):
        return self.supportsDynamicProperties

    def getSupportedDynamicPropertyDescriptor(self, propertyName):
        if not self.supportsDynamicProperties:
            return None
        descriptor = self.processor.getDynamicPropertyDescriptor(propertyName)
        return None if descriptor is None else descriptor.to_java_descriptor(gateway = self.gateway, cs_type_lookup=self.controller_service_type_lookup)

    def onScheduled(self, context):
        if self.hasMethod(self.processor, 'onScheduled'):
            self.processor.onScheduled(context)

    def onStopped(self, context):
        if self.hasMethod(self.processor, 'onStopped'):
            self.processor.onStopped(context)

    def initialize(self, context):
        self.processor.logger = context.getLogger()
        self.processor.identifier = context.getIdentifier()
