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

from enum import Enum
from nifiapi.componentstate import StateManager
from nifiapi.__jvm__ import JvmHolder
import re

EMPTY_STRING_ARRAY = JvmHolder.gateway.new_array(JvmHolder.jvm.java.lang.String, 0)
EMPTY_ALLOWABLE_VALUE_ARRAY = JvmHolder.gateway.new_array(JvmHolder.jvm.org.apache.nifi.components.AllowableValue, 0)

class ExpressionLanguageScope(Enum):
    NONE = 1
    ENVIRONMENT = 2
    FLOWFILE_ATTRIBUTES = 3


class StandardValidators:
    _standard_validators = JvmHolder.jvm.org.apache.nifi.processor.util.StandardValidators

    ALWAYS_VALID = JvmHolder.jvm.org.apache.nifi.components.Validator.VALID
    NON_EMPTY_VALIDATOR = _standard_validators.NON_EMPTY_VALIDATOR
    INTEGER_VALIDATOR = _standard_validators.INTEGER_VALIDATOR
    POSITIVE_INTEGER_VALIDATOR = _standard_validators.POSITIVE_INTEGER_VALIDATOR
    POSITIVE_LONG_VALIDATOR = _standard_validators.POSITIVE_LONG_VALIDATOR
    NON_NEGATIVE_INTEGER_VALIDATOR = _standard_validators.NON_NEGATIVE_INTEGER_VALIDATOR
    NUMBER_VALIDATOR = _standard_validators.NUMBER_VALIDATOR
    LONG_VALIDATOR = _standard_validators.LONG_VALIDATOR
    PORT_VALIDATOR = _standard_validators.PORT_VALIDATOR
    NON_EMPTY_EL_VALIDATOR = _standard_validators.NON_EMPTY_EL_VALIDATOR
    HOSTNAME_PORT_LIST_VALIDATOR = _standard_validators.HOSTNAME_PORT_LIST_VALIDATOR
    BOOLEAN_VALIDATOR = _standard_validators.BOOLEAN_VALIDATOR
    URL_VALIDATOR = _standard_validators.URL_VALIDATOR
    URI_VALIDATOR = _standard_validators.URI_VALIDATOR
    REGULAR_EXPRESSION_VALIDATOR = _standard_validators.REGULAR_EXPRESSION_VALIDATOR
    REGULAR_EXPRESSION_WITH_EL_VALIDATOR = _standard_validators.REGULAR_EXPRESSION_WITH_EL_VALIDATOR
    TIME_PERIOD_VALIDATOR = _standard_validators.TIME_PERIOD_VALIDATOR
    DATA_SIZE_VALIDATOR = _standard_validators.DATA_SIZE_VALIDATOR
    FILE_EXISTS_VALIDATOR = _standard_validators.FILE_EXISTS_VALIDATOR



class PropertyDependency:
    def __init__(self, property_descriptor, *dependent_values):
        if dependent_values is None:
            dependent_values = []

        self.property_descriptor = property_descriptor
        self.dependent_values = dependent_values

    @staticmethod
    def from_java_dependency(java_dependencies):
        if java_dependencies is None or len(java_dependencies) == 0:
            return None

        dependencies = []
        for dependency in java_dependencies:
            dependencies.append(PropertyDependency(dependency.getPropertyName(), dependency.getDependentValues()))

        return dependencies


class ResourceDefinition:
    def __init__(self, allow_multiple=False, allow_file=True, allow_url=False, allow_directory=False, allow_text=False):
        self.allow_multiple = allow_multiple
        self.allow_file = allow_file
        self.allow_url = allow_url
        self.allow_directory = allow_directory
        self.allow_text = allow_text

    @staticmethod
    def from_java_definition(java_definition):
        if java_definition is None:
            return None

        allow_multiple = java_definition.getCardinality().name() == "MULTIPLE"
        resource_types = java_definition.getResourceTypes()
        allow_file = False
        allow_url = False
        allow_directory = False
        allow_text = False
        for type in resource_types:
            name = type.name()
            if name == "FILE":
                allow_file = True
            elif name == "DIRECTORY":
                allow_directory = True
            elif name == "TEXT":
                allow_text = True
            elif name == "URL":
                allow_url = True

        return ResourceDefinition(allow_multiple, allow_file, allow_url, allow_directory, allow_text)


class PropertyDescriptor:
    def __init__(self, name, description, required=False, sensitive=False,
                 display_name=None, default_value=None, allowable_values=None,
                 dependencies=None, expression_language_scope=ExpressionLanguageScope.NONE,
                 dynamic=False, validators=None,
                 resource_definition=None, controller_service_definition=None):
        """
        :param name: the name of the property
        :param description: a description of the property
        :param required: a boolean indicating whether or not the property is required. Defaults to False.
        :param sensitive: a boolean indicating whether or not the property is sensitive. Defaults to False.
        :param display_name: Once a Processor has been released, its properties' configuration are stored as key/value pairs where the key is the name of the property.
                             Because of that, subsequent versions of the Processor should not change the name of a property. However, there are times when renaming a property
                             would be advantageous. For example, one might find a typo in the name of a property, or users may find a property name confusing. While the name of
                             the property should not be changed, a display_name may be added. Doing this results in the Display Name being used in the NiFi UI but still maintains
                             the original name as the key. Generally, this value should be left unspecified. If unspecified (or a value of `None`), the display name will default
                             to whatever the name is. However, the display_name may be changed at any time between versions without adverse effects.
        :param default_value: a default value for the property. If not specified, the initial value will be unset. If specified, any time the value is removed, it is reset to this
                              default_value. That is to say, if a default value is specified, the property cannot be unset.
        :param allowable_values: a list of string values that are allowed. If specified, the UI will present the options as a drop-down instead of a freeform text. If any other value
                                 is specified for the property, the Processor will be invalid.
        :param dependencies: a list of dependencies for this property. By default, all properties are always configurable. However, sometimes we want to expose a property that only makes
                             sense in certain circumstances. In this situation, we can say that Property A depends on Property B. Now, Property A will only be shown if a value is selected
                             for Property B. Additionally, we may say that Property A depends on Property B being set to some explicit value, say "foo." Now, Property A will only be shown
                             in the UI if Property B is set to a value of "foo." If a Property is not shown in the UI, its value will also not be validated. For example, if we indicate that
                             Property A is required and depends on Property B, then Property A is only required if Property B is set.
        :param expression_language_scope: documents the scope in which Expression Language is valid. This value must be specified as one of the enum values
                                          in `nifiapi.properties.ExpressionLanguageScope`. A value of `NONE` indicates that Expression Language will not be evaluated for this property.
                                          This is the default. A value of `FLOWFILE_ATTRIBUTES` indicates that FlowFile attributes may be referenced when configuring the property value.
                                          A value of `ENVIRONMENT` indicates that Expression Language may be used and may reference environment variables but may not reference FlowFile attributes.
                                          For example, a value of `${now()}` might be used to reference the current date and time, or `${hostname(true)}` might be used to specify the hostname.
                                          Or a value of `${ENV_VAR}` could be used to reference an environment variable named `ENV_VAR`.
        :param dynamic: whether or not this Property Descriptor represents a dynamic (aka user-defined) property. This is not necessary to specify, as the framework can determine this.
                        However, it is available if there is a desire to explicitly set it for completeness' sake.
        :param validators: A list of property validators that can be used to ensure that the user-supplied value is valid. The standard validators can be referenced using the
                           members of the `nifiapi.properties.StandardValidators` class.
        :param resource_definition: an instance of `nifiapi.properties.ResourceDefinition`. This may be used to convey that the property references a file, directory, or URL, or a set of them.
        :param controller_service_definition: if this Processor is to make use of a Controller Service, this indicates the type of Controller Service. This will always be a fully-qualified
                                              classname of a Java interface that extends from `ControllerService`.
        """
        if validators is None:
            validators = [StandardValidators.ALWAYS_VALID]

        self.name = name
        self.description = description
        self.required = required
        self.sensitive = sensitive
        self.displayName = display_name
        self.defaultValue = None if default_value is None else str(default_value)
        self.allowableValues = allowable_values
        self.dependencies = dependencies
        self.expressionLanguageScope = expression_language_scope
        self.dynamic = dynamic
        self.validators = validators
        self.resourceDefinition = resource_definition
        self.controllerServiceDefinition = controller_service_definition

    @staticmethod
    def from_java_descriptor(java_descriptor):
        # Build the dependencies
        dependencies = PropertyDependency.from_java_dependency(java_descriptor.getDependencies())

        # Build the allowable values
        allowable = java_descriptor.getAllowableValues()
        if allowable is None or len(allowable) == 0:
            allowable_values = None
        else:
            allowable_values = []
            for value in allowable:
                allowable_values.append(value.getValue())

        # Build the resource definition
        resource_definition = ResourceDefinition.from_java_definition(java_descriptor.getResourceDefinition())

        el_scope = java_descriptor.getExpressionLanguageScope()
        el_scope_name = None if el_scope is None else el_scope.name()

        return PropertyDescriptor(java_descriptor.getName(), java_descriptor.getDescription(),
                                  required = java_descriptor.isRequired(),
                                  sensitive = java_descriptor.isSensitive(),
                                  display_name = java_descriptor.getDisplayName(),
                                  default_value = java_descriptor.getDefaultValue(),
                                  allowable_values = allowable_values,
                                  expression_language_scope = el_scope_name,
                                  dynamic = java_descriptor.isDynamic(),
                                  validators = java_descriptor.getValidators(),
                                  controller_service_definition = java_descriptor.getControllerServiceDefinition(),
                                  dependencies = dependencies,
                                  resource_definition = resource_definition)


    def to_java_descriptor(self, gateway, cs_type_lookup):
        el_scope = gateway.jvm.org.apache.nifi.expression.ExpressionLanguageScope.valueOf(self.expressionLanguageScope.name)

        builder = gateway.jvm.org.apache.nifi.components.PropertyDescriptor.Builder() \
            .name(self.name) \
            .displayName(self.displayName) \
            .description(self.description) \
            .required(self.required) \
            .sensitive(self.sensitive) \
            .defaultValue(self.defaultValue) \
            .expressionLanguageSupported(el_scope) \
            .dynamic(self.dynamic)

        if self.resourceDefinition is not None:
            self.__add_resource_definition(gateway, self.resourceDefinition, builder)

        if self.controllerServiceDefinition is not None:
            cs_type = cs_type_lookup.lookup(self.controllerServiceDefinition)
            builder.identifiesControllerService(cs_type)

        if self.allowableValues is not None:
            builder.allowableValues(self.__get_allowable_values(gateway))

        if self.dependencies is not None:
            for dependency in self.dependencies:
                dependent_property = dependency.property_descriptor.to_java_descriptor(gateway, cs_type_lookup)

                # The PropertyDescriptor.dependsOn method uses varargs to provide the dependent values. This makes things a bit complicated on the Python side.
                # If there are no dependent values provided, we must pass the property descriptor and an empty array of AllowableValues.
                # Otherwise, we must pass the first dependent value as a String followed by an empty array of Strings. We call this out as a special case so that
                # we can avoid creating an empty String array, as doing so would mean another call to the Java side, which is expensive.
                # Finally, we can consider the case where there are 2 or more dependent values. In that case, we must create an array of Strings that contains all but
                # the first dependent value. We then pass the property descriptor, the first dependent value, and the array of remaining dependent values.
                if dependency.dependent_values:
                    if len(dependency.dependent_values) == 1:
                        builder.dependsOn(dependent_property, dependency.dependent_values[0], EMPTY_STRING_ARRAY)
                    else:
                        additional_args = JvmHolder.gateway.new_array(JvmHolder.jvm.java.lang.String, len(dependency.dependent_values) - 1)

                        # copy all but first element of dependency.dependent_values into additional_args
                        for i, arg in enumerate(dependency.dependent_values):
                            if i == 0:
                                continue
                            additional_args[i - 1] = arg

                        builder.dependsOn(dependent_property, dependency.dependent_values[0], additional_args)
                else:
                    builder.dependsOn(dependent_property, EMPTY_ALLOWABLE_VALUE_ARRAY)

        for validator in self.validators:
            builder.addValidator(validator)

        return builder.build()


    def __get_allowable_values(self, gateway):
        if self.allowableValues is None:
            return None
        values = gateway.jvm.java.util.LinkedHashSet()
        for value in self.allowableValues:
            values.add(value)

        return values

    def __add_resource_definition(self, gateway, resource_definition, builder):
        allowed_types = 0
        if resource_definition.allow_file:
            allowed_types += 1
        if resource_definition.allow_directory:
            allowed_types += 1
        if resource_definition.allow_url:
            allowed_types += 1
        if resource_definition.allow_text:
            allowed_types += 1

        array_type = gateway.jvm.org.apache.nifi.components.resource.ResourceType
        types = gateway.new_array(array_type, allowed_types)
        index = 0
        if resource_definition.allow_file:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.FILE
            index += 1
        if resource_definition.allow_directory:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.DIRECTORY
            index += 1
        if resource_definition.allow_url:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.URL
            index += 1
        if resource_definition.allow_text:
            types[index] = gateway.jvm.org.apache.nifi.components.resource.ResourceType.TEXT
            index += 1

        cardinality = gateway.jvm.org.apache.nifi.components.resource.ResourceCardinality.MULTIPLE if resource_definition.allow_multiple else \
            gateway.jvm.org.apache.nifi.components.resource.ResourceCardinality.SINGLE

        builder.identifiesExternalResource(cardinality, types[0], types[1:])


class PropertyContext:
    __trivial_attribute_reference__ = re.compile(r"\$\{([^${}\[\],:;/*\' \t\r\n\\d][^${}\[\],:;/*\' \t\r\n]*)}")
    __escaped_attribute_reference__ = re.compile(r"\$\{'([^${}\[\],:;/*\' \t\r\n\\d][^${}\[\],:;/*\'\t\r\n]*)'}")

    def create_python_property_value(self, el_supported, java_property_value, string_value):
        el_present = java_property_value.isExpressionLanguagePresent()
        referenced_attribute = None
        if el_present:
            trivial_match = self.__trivial_attribute_reference__.match(string_value)
            if trivial_match is not None:
                referenced_attribute = trivial_match.group(1)
            else:
                escaped_match = self.__escaped_attribute_reference__.match(string_value)
                if escaped_match is not None:
                    referenced_attribute = escaped_match.group(1)

        return PythonPropertyValue(java_property_value, string_value, el_supported, el_present, referenced_attribute)


    def getProperty(self, descriptor):
        property_name = descriptor if isinstance(descriptor, str) else descriptor.name
        return self.property_values.get(property_name)

    def getProperties(self):
        return self.descriptor_value_map

    def newPropertyValue(self, value):
        java_property_value = self.java_context.newPropertyValue(value)
        return self.create_python_property_value(True, java_property_value, value)


class ProcessContext(PropertyContext):

    def __init__(self, java_context):
        self.java_context = java_context

        descriptors = java_context.getProperties().keySet()
        self.name = java_context.getName()
        self.property_values = {}
        self.descriptor_value_map = {}

        for descriptor in descriptors:
            property_value = java_context.getProperty(descriptor.getName())
            string_value = property_value.getValue()

            property_value = self.create_python_property_value(descriptor.isExpressionLanguageSupported(), property_value, string_value)
            self.property_values[descriptor.getName()] = property_value

            python_descriptor = PropertyDescriptor.from_java_descriptor(descriptor)
            self.descriptor_value_map[python_descriptor] = string_value

    def getName(self):
        return self.name

    def yield_resources(self):
        JvmHolder.java_gateway.get_method(self.java_context, "yield")()

    def getStateManager(self):
        return StateManager(self.java_context.getStateManager())


class ValidationContext(PropertyContext):

    def __init__(self, java_context):
        self.java_context = java_context

        descriptors = java_context.getProperties().keySet()
        self.property_values = {}
        self.descriptor_value_map = {}

        for descriptor in descriptors:
            property_value = java_context.getProperty(descriptor)
            string_value = property_value.getValue()

            property_value = self.create_python_property_value(descriptor.isExpressionLanguageSupported(), property_value, string_value)
            self.property_values[descriptor.getName()] = property_value

            python_descriptor = PropertyDescriptor.from_java_descriptor(descriptor)
            self.descriptor_value_map[python_descriptor] = string_value


class TimeUnit(Enum):
    NANOSECONDS = "NANOSECONDS",
    MICROSECONDS = "MICROSECONDS",
    MILLISECONDS = "MILLISECONDS",
    SECONDS = "SECONDS",
    MINUTES = "MINUTES",
    HOURS = "HOURS",
    DAYS = "DAYS"


class DataUnit(Enum):
    B = "B",
    KB = "KB",
    MB = "MB",
    GB = "GB",
    TB = "TB"


class PythonPropertyValue:
    def __init__(self, property_value, string_value, el_supported, el_present, referenced_attribute):
        self.value = string_value
        self.property_value = property_value
        self.el_supported = el_supported
        self.el_present = el_present
        self.referenced_attribute = referenced_attribute

    def getValue(self):
        return self.value

    def isSet(self):
        return self.value is not None

    def isExpressionLanguagePresent(self):
        return self.el_present

    def asInteger(self):
        if self.value is None:
            return None
        return int(self.value)

    def asBoolean(self):
        if self.value is None:
            return None
        return self.value.lower() == 'true'

    def asFloat(self):
        if self.value is None:
            return None
        return float(self.value)

    def asTimePeriod(self, time_unit):
        javaTimeUnit = JvmHolder.jvm.java.util.concurrent.TimeUnit.valueOf(time_unit._name_)
        return self.property_value.asTimePeriod(javaTimeUnit)

    def asDataSize(self, data_unit):
        javaDataUnit = JvmHolder.jvm.org.apache.nifi.processor.DataUnit.valueOf(data_unit._name_)
        return self.property_value.asDataSize(javaDataUnit)

    def asControllerService(self):
        return self.property_value.asControllerService()

    def asResource(self):
        return self.property_value.asResource()

    def asResources(self):
        return self.property_value.asResources()

    def evaluateAttributeExpressions(self, attributeMap=None):
        # If Expression Language is supported and present, evaluate it and return a new PropertyValue.
        # Otherwise just return self, in order to avoid the cost of making the call to Java for evaluateAttributeExpressions
        if self.el_supported and self.el_present:
            new_property_value = None
            if self.referenced_attribute is not None:
                attribute_value = attributeMap.getAttribute(self.referenced_attribute)
                new_property_value = self.property_value
                if attribute_value is None:
                    new_string_value = ""
                else:
                    new_string_value = attribute_value
            else:
                # TODO: Consider having property_value wrapped in another class that delegates to the existing property_value but allows evaluateAttributeExpressions to take in an AttributeMap.
                #       This way we can avoid the call to getAttributes() here, which is quite expensive
                new_property_value = self.property_value.evaluateAttributeExpressions(attributeMap.getAttributes())
                new_string_value = new_property_value.getValue()

            return PythonPropertyValue(new_property_value, new_string_value, self.el_supported, False, None)

        return self


class ValidationResult:
    def __init__(self, subject="", explanation="", valid=False, input=None):
        self.subject = subject
        self.explanation = explanation
        self.valid = valid
        self.input = input

    def to_java_validation_result(self):
            return JvmHolder.gateway.jvm.org.apache.nifi.components.ValidationResult.Builder() \
                .subject(self.subject) \
                .explanation(self.explanation) \
                .valid(self.valid) \
                .input(self.input) \
                .build()
