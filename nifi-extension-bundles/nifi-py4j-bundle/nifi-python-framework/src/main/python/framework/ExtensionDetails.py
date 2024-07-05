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

import BundleCoordinate

class ExtensionDetails:
    class Java:
        implements = ['org.apache.nifi.python.PythonProcessorDetails']

    def __init__(self, type, interfaces,
                 version='Unknown',
                 dependencies=None,
                 source_location=None,
                 description=None,
                 tags=None,
                 use_cases=None,
                 multi_processor_use_cases=None,
                 extension_home=None,
                 dependencies_bundled=False,
                 property_descriptions=None,
                 bundle_coordinate=BundleCoordinate.BundleCoordinate):

        self.type = type
        self.interfaces = interfaces if interfaces else []
        self.dependencies = dependencies if dependencies else []
        self.tags = tags if tags else []
        self.version = version
        self.source_location = source_location
        self.extension_home = extension_home
        self.description = description
        self.use_cases = use_cases if use_cases else {}
        self.multi_processor_use_cases = multi_processor_use_cases if multi_processor_use_cases else {}
        self.property_descriptions = property_descriptions if property_descriptions else {}
        self.dependencies_bundled = dependencies_bundled
        self.bundle_coordinate = bundle_coordinate

    def getProcessorType(self):
        return self.type

    def getProcessorVersion(self):
        return self.version

    def getSourceLocation(self):
        return self.source_location

    def getExtensionHome(self):
        return self.extension_home

    def getDependencies(self):
        return ArrayList(self.dependencies)

    def getCapabilityDescription(self):
        return self.description

    def getTags(self):
        return ArrayList(self.tags)

    def getUseCases(self):
        return ArrayList(self.use_cases)

    def getMultiProcessorUseCases(self):
        return ArrayList(self.multi_processor_use_cases)

    def getPropertyDescriptions(self):
        return ArrayList(self.property_descriptions)

    def isBundledWithDependencies(self):
        return self.dependencies_bundled

    def getInterface(self):
        if len(self.interfaces) == 0:
            return None
        return self.interfaces[0]

    def getBundleCoordinate(self):
        return self.bundle_coordinate