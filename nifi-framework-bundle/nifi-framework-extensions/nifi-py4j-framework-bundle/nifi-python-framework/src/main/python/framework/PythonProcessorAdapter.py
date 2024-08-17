# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from nifiapi.properties import ProcessContext, ValidationContext


def is_method_defined(processor, method_name):
    # Get the attribute from the given Processor with the provided method name, returning None if method is not present
    attr = getattr(processor, method_name, None)

    # Return True if the attribute is present and is a method (i.e., is callable).
    return callable(attr)


# PythonProcessorAdapter is responsible for receiving method invocations from Java side and delegating to the appropriate
# method for a Processor. We use this adapter instead of calling directly into the Processor because it allows us to be more
# flexible on the Python side, by allowing us to avoid implementing things like customValidate, etc.
class PythonProcessorAdapter:
    class Java:
        implements = ['org.apache.nifi.python.processor.PythonProcessorAdapter']


    def __init__(self, gateway, processor, extension_manager, controller_service_type_lookup):
        self.processor = processor
        self.gateway = gateway
        self.hasCustomValidate = is_method_defined(processor, 'customValidate')
        self.extension_manager = extension_manager
        self.controller_service_type_lookup = controller_service_type_lookup
        self.has_properties = is_method_defined(processor, 'getPropertyDescriptors')
        self.supportsDynamicProperties = is_method_defined(processor, 'getDynamicPropertyDescriptor')

        if is_method_defined(processor, 'getRelationships'):
            self.relationships = None
            self.cached_relationships = ([], None)
        else:
            self.relationships = gateway.jvm.java.util.HashSet()
            success = gateway.jvm.org.apache.nifi.processor.Relationship.Builder() \
                .name("success") \
                .description("All FlowFiles will go to this relationship after being successfully processed") \
                .build()
            self.relationships.add(success)


    def customValidate(self, context):
        # If no customValidate method, just return
        if not self.hasCustomValidate:
            return None

        validation_results = self.processor.customValidate(ValidationContext(context))

        result_list = self.gateway.jvm.java.util.ArrayList()
        for result in validation_results:
            result_list.add(result.to_java_validation_result())
        return result_list

    def getRelationships(self):
        # If self.relationships is None, it means that the Processor has implemented the method, and we need
        # to call the Processor's implementation. This allows for dynamically changing the Relationships based on
        # configuration, etc.
        if self.relationships is None:
            processor_rels = self.processor.getRelationships()

            # If the relationships haven't changed, return the cached set
            # This is to avoid creating a new HashSet and Java Relationship objects every time getRelationships is called, which is very expensive
            if processor_rels == self.cached_relationships[0]:
                return self.cached_relationships[1]

            hash_set = self.gateway.jvm.java.util.HashSet()
            for rel in processor_rels:
                hash_set.add(rel.to_java_relationship(self.gateway))

            # Cache and return the results
            self.cached_relationships = (processor_rels, hash_set)
            return hash_set
        else:
            return self.relationships

    def getSupportedPropertyDescriptors(self):
        descriptors = self.processor.getPropertyDescriptors() if self.has_properties else []
        descriptor_list = self.gateway.jvm.java.util.ArrayList()
        for descriptor in descriptors:
            descriptor_list.add(descriptor.to_java_descriptor(self.gateway, self.controller_service_type_lookup))

        return descriptor_list

    def getProcessor(self):
        return self.processor

    def isDynamicPropertySupported(self):
        return self.supportsDynamicProperties

    def getSupportedDynamicPropertyDescriptor(self, property_name):
        if not self.supportsDynamicProperties:
            return None
        descriptor = self.processor.getDynamicPropertyDescriptor(property_name)
        return None if descriptor is None else descriptor.to_java_descriptor(gateway=self.gateway, cs_type_lookup=self.controller_service_type_lookup)

    def onScheduled(self, context):
        if is_method_defined(self.processor, 'onScheduled'):
            self.processor.onScheduled(ProcessContext(context))

    def onStopped(self, context):
        if is_method_defined(self.processor, 'onStopped'):
            self.processor.onStopped(ProcessContext(context))

    def initialize(self, context):
        self.processor.logger = context.getLogger()
        self.processor.identifier = context.getIdentifier()
