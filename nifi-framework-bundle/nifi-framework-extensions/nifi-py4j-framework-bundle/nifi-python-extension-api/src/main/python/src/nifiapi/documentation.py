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

from nifiapi.__jvm__ import ArrayList

class ProcessorConfiguration:
    class Java:
        implements = ['org.apache.nifi.python.processor.documentation.ProcessorConfigurationDetails']

    def __init__(self, processor_type: str, configuration: str):
        self.processor_type = processor_type
        self.configuration = configuration

    def getProcessorType(self):
        return self.processor_type

    def getConfiguration(self):
        return self.configuration


def use_case(description: str, configuration: str = None, notes: str = None, keywords: list[str] = None):
    """Decorator to explain how to perform a specific use case with a given processor"""
    def decorator(func):
        return func
    return decorator


def multi_processor_use_case(description: str, configurations: list[ProcessorConfiguration], notes: str = None, keywords: list[str] = None):
    """Decorator to explain how to perform a specific use case that involves the decorated Processor, in addition to additional Processors"""
    def decorator(func):
        return func
    return decorator


class UseCaseDetails:
    class Java:
        implements = ['org.apache.nifi.python.processor.documentation.UseCaseDetails']

    def __init__(self, description: str, notes: str, keywords: list[str], configuration: str):
        self.description = description
        self.notes = notes
        self.keywords = keywords
        self.configuration = configuration

    def getDescription(self):
        return self.description

    def getNotes(self):
        return self.notes

    def getKeywords(self):
        return ArrayList(self.keywords)

    def getConfiguration(self):
        return self.configuration

    def __str__(self):
        return f"UseCaseDetails[description={self.description}]"


class MultiProcessorUseCaseDetails:
    class Java:
        implements = ['org.apache.nifi.python.processor.documentation.MultiProcessorUseCaseDetails']

    def __init__(self, description: str, notes: str, keywords: list[str], configurations: list[ProcessorConfiguration]):
        self.description = description
        self.notes = notes
        self.keywords = keywords
        self.configurations = configurations

    def getDescription(self):
        return self.description

    def getNotes(self):
        return self.notes

    def getKeywords(self):
        return ArrayList(self.keywords)

    def getConfigurations(self):
        return ArrayList(self.configurations)

    def __str__(self):
        return f"MultiProcessorUseCaseDetails[description={self.description}]"

class PropertyDependency:
    class Java:
            implements = ['org.apache.nifi.python.processor.documentation.PropertyDependency']

    def __init__(self,
                      name: str,
                      display_name: str,
                      dependent_values: list[str]):
             self.name = name
             self.display_name = display_name
             self.dependent_values = dependent_values

    def getName(self):
        return self.name

    def getDisplayName(self):
        return self.display_name

    def getDependentValues(self):
        return ArrayList(self.dependent_values)

class PropertyDescription:
    class Java:
        implements = ['org.apache.nifi.python.processor.documentation.PropertyDescription']

    def __init__(self,
                 name: str,
                 description: str,
                 display_name: str = None,
                 required: bool = False,
                 sensitive: bool = True,
                 default_value: str = None,
                 expression_language_scope: str = None,
                 controller_service_definition: str = None,
                 allowable_values: list[str] = None,
                 dependencies: list[PropertyDependency] = None):
        self.name = name
        self.description = description
        self.display_name = display_name
        self.required = required
        self.sensitive = sensitive
        self.default_value = default_value
        self.expression_language_scope = expression_language_scope
        self.controller_service_definition = controller_service_definition
        self.allowable_values = allowable_values if allowable_values is not None else []
        self.dependencies = dependencies if dependencies is not None else []

    def getName(self):
        return self.name

    def getDescription(self):
        return self.description

    def getDisplayName(self):
        return self.display_name

    def isRequired(self):
        return self.required

    def isSensitive(self):
        return self.sensitive

    def getDefaultValue(self):
        return self.default_value

    def getExpressionLanguageScope(self):
        return self.expression_language_scope

    def getControllerServiceDefinition(self):
        return self.controller_service_definition

    def getAllowableValues(self):
        return ArrayList(self.allowable_values)

    def getDependencies(self):
        return ArrayList(self.dependencies)
