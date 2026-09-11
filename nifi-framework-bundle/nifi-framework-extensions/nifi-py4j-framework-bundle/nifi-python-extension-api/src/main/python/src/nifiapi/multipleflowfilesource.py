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

from abc import ABC, abstractmethod
from nifiapi.__jvm__ import JvmHolder
from nifiapi.properties import ProcessContext
from nifiapi.flowfilesourceresult import FlowFileSourceResult


class MultipleFlowFileSource(ABC):
    # These will be set by the PythonProcessorAdapter when the component is created
    identifier = None
    logger = None

    def __init__(self):
        self.arrayList = JvmHolder.jvm.java.util.ArrayList

    def setContext(self, context):
        self.process_context = ProcessContext(context)

    def createFlowFiles(self):
        results = self.arrayList()
        for (result) in self.create(self.process_context):
            results.add(result)
        return results

    @abstractmethod
    def create(self, context):
        pass
