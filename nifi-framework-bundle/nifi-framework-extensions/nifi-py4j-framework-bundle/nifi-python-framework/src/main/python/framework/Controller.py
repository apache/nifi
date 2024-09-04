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

import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from py4j import java_gateway
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters

import ExtensionManager

# We do not use ThreadPoolExecutor or ProcessPoolExecutor, but they must be kept here. Python introduced a bug in 3.9 that causes Exceptions to be raised
# incorrectly in multi-threaded applications (https://bugs.python.org/issue42647). This works around the bug.
# What is actually necessary is to import ThreadPoolExecutor and ProcessPoolExecutor.
# Unfortunately, IntelliJ often likes to cleanup the unused import. So we assign a bogus variable just so
# that we have some reference to ThreadPoolExecutor and ProcessPoolExecutor in order to prevent the IDE from cleaning up the import
threadpool_attrs = dir(ThreadPoolExecutor)
processpool_attrs = dir(ProcessPoolExecutor)

# Set log format with level number and separator as expected in PythonProcessReaderCommand
logging.basicConfig(stream=sys.stderr,
                    format='PY4JLOG %(levelno)s %(name)s:%(message)s',
                    encoding='utf-8',
                    level=logging.INFO)

logger = logging.getLogger("org.apache.nifi.py4j.Controller")


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

    def getProcessorDetails(self, type, version):
        processor_details = self.extensionManager.get_processor_details(type, version)
        if processor_details is None:
            raise ValueError(f"Invalid Processor Type/Version: {type}/{version}")

        return processor_details

    def downloadDependencies(self, type, version, work_dir):
        processor_details = self.extensionManager.get_processor_details(type, version)
        if processor_details is None:
            raise ValueError(f"Invalid Processor Type/Version: {type}/{version}")

        self.extensionManager.import_external_dependencies(processor_details, work_dir)

    def createProcessor(self, processorType, version, work_dir):
        processorClass = self.extensionManager.getProcessorClass(processorType, version, work_dir)
        processor = processorClass(jvm=self.gateway.jvm)
        adapter = PythonProcessorAdapter.PythonProcessorAdapter(self.gateway, processor, self.extensionManager, self.controllerServiceTypeLookup)
        return adapter

    def reloadProcessor(self, processorType, version, workDirectory):
        self.extensionManager.reload_processor(processorType, version, workDirectory)

    def removeProcessorType(self, processorType, version):
        self.extensionManager.remove_processor_type(processorType, version)

    def getModuleFile(self, processorType, version):
        module_file = self.extensionManager.get_module_file(processorType, version)
        return module_file

    def setGateway(self, gateway):
        self.gateway = gateway
        self.extensionManager = ExtensionManager.ExtensionManager(gateway)

    def setControllerServiceTypeLookup(self, typeLookup):
        self.controllerServiceTypeLookup = typeLookup

    def setLoggerLevel(self, loggerName, level):
        logging.getLogger(loggerName).setLevel(level)

    class Java:
        implements = ["org.apache.nifi.py4j.PythonController"]


if __name__ == "__main__":
    # Create the Controller
    controller = Controller()

    # Create the Java Gateway for communicating with NiFi Java process
    java_port = int(os.getenv('JAVA_PORT'))
    auth_token = os.getenv('AUTH_TOKEN')

    gateway = JavaGateway(
       callback_server_parameters=CallbackServerParameters(port=0, auth_token=auth_token),
       gateway_parameters=GatewayParameters(port=java_port, read_timeout=None, enable_memory_management=True, auth_token=auth_token),
       auto_convert=True,
       python_server_entry_point=controller)

    controller.setGateway(gateway)
    python_port = gateway.get_callback_server().get_listening_port()
    logger.info("Listening for requests from Java side using Python Port {}, communicating with Java on port {}".format(python_port, java_port) )

    # Initialize the JvmHolder class with the gateway jvm.
    # This must be done before executing the module to ensure that the nifiapi module
    # is able to access the JvmHolder.jvm variable. This enables the nifiapi.properties.StandardValidators, etc. to be used
    # However, we have to delay the import until this point, rather than adding it to the top of the ExtensionManager class
    # because we need to ensure that we've fetched the appropriate dependencies for the pyenv environment for the extension point.
    from nifiapi.__jvm__ import JvmHolder
    JvmHolder.jvm = gateway.jvm
    JvmHolder.gateway = gateway
    JvmHolder.java_gateway = java_gateway

    # We need to import PythonProcessorAdapter but cannot import it at the top of the class because we must first initialize the Gateway,
    # since there are statically defined objects in the file that contains PythonProcessorAdapter, and those statically defined objects require the Gateway.
    import PythonProcessorAdapter

    # Notify the Java side of the port that Python is listening on
    gateway.java_gateway_server.resetCallbackClient(
        gateway.java_gateway_server.getCallbackClient().getAddress(),
        python_port)

    # Join main thread to non-daemon threads in order to avoid RuntimeError on Python 3.12 blocking new thread creation in Py4J
    import threading
    for thread in threading.enumerate():
        if thread.daemon or thread is threading.current_thread():
            continue
        thread.join()
