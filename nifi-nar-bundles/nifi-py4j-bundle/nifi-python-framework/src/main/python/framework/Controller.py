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

import os
import logging
import ExtensionManager
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters

import PythonProcessorAdapter

# Initialize logging
logger = logging.getLogger("org.apache.nifi.py4j.Controller")
logger.setLevel(logging.INFO)

logging.getLogger("py4j").setLevel(logging.WARN)

logsDir = os.getenv('LOGS_DIR')
logging.basicConfig(filename=logsDir + '/python.log',
                    format='%(asctime)s %(levelname)s %(name)s %(message)s',
                    encoding='utf-8',
                    level=logging.INFO)


class Controller:

    def ping(self):
        return "pong"

    def getProcessorTypes(self):
        types = self.extensionManager.getProcessorTypes()
        typesList = self.gateway.jvm.java.util.ArrayList()
        for type in types:
            typesList.add(type)
        return typesList

    def discoverExtensions(self, dirs, work_dir):
        self.extensionManager.discoverExtensions(dirs, work_dir)

    def createProcessor(self, processorType, version, work_dir):
        processorClass = self.extensionManager.getProcessorClass(processorType, version, work_dir)
        processor = processorClass(jvm=self.gateway.jvm)
        adapter = PythonProcessorAdapter.PythonProcessorAdapter(self.gateway, processor, self.extensionManager, self.controllerServiceTypeLookup)
        return adapter

    def reloadProcessor(self, processorType, version, workDirectory):
        self.extensionManager.reload_processor(processorType, version, workDirectory)

    def getModuleFile(self, processorType, version):
        module_file = self.extensionManager.get_module_file(processorType, version)
        return module_file

    def getProcessorDependencies(self, processorType, version):
        deps = self.extensionManager.__get_dependencies_for_extension_type__(processorType, version)
        dependencyList = self.gateway.jvm.java.util.ArrayList()
        for dep in deps:
            dependencyList.add(dep)

        return dependencyList

    def setGateway(self, gateway):
        self.gateway = gateway
        self.extensionManager = ExtensionManager.ExtensionManager(gateway)

    def setControllerServiceTypeLookup(self, typeLookup):
        self.controllerServiceTypeLookup = typeLookup

    class Java:
        implements = ["org.apache.nifi.py4j.PythonController"]


# Create the Controller
controller = Controller()

# Create the Java Gateway for communicating with NiFi Java process
java_port = int(os.getenv('JAVA_PORT'))
gateway = JavaGateway(
   callback_server_parameters=CallbackServerParameters(port=0),
   gateway_parameters=GatewayParameters(port=java_port, read_timeout=None, enable_memory_management=True),
   auto_convert=True,
   python_server_entry_point=controller)

controller.setGateway(gateway)
python_port = gateway.get_callback_server().get_listening_port()
logger.info("Listening for requests from Java side using Python Port {}, communicating with Java on port {}".format(python_port, java_port) )

# Notify the Java side of the port that Python is listening on
gateway.java_gateway_server.resetCallbackClient(
    gateway.java_gateway_server.getCallbackClient().getAddress(),
    python_port)
