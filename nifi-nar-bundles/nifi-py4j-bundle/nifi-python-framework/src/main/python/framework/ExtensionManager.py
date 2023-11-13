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

import ast
import importlib
import importlib.util  # Note requires Python 3.4+
import inspect
import logging
import os
import pkgutil
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger("org.apache.nifi.py4j.ExtensionManager")


# A simple wrapper class to encompass a processor type and its version
class ExtensionId:
    def __init__(self, classname=None, version=None):
        self.classname = classname
        self.version = version

    def __hash__(self):
        return hash((self.classname, self.version))

    def __eq__(self, other):
        return (self.classname, self.version) == (other.classname, other.version)


class ExtensionDetails:
    class Java:
        implements = ['org.apache.nifi.python.PythonProcessorDetails']

    def __init__(self, gateway, type, interfaces,
                 version='Unknown',
                 dependencies=None,
                 source_location=None,
                 description=None,
                 tags=None):

        self.gateway = gateway
        self.type = type

        if interfaces is None:
            interfaces = []
        self.interfaces = interfaces

        if dependencies is None:
            dependencies = []
        if tags is None:
            tags = []

        self.version = version
        self.dependencies = dependencies
        self.source_location = source_location
        self.description = description
        self.tags = tags

    def getProcessorType(self):
        return self.type

    def getProcessorVersion(self):
        return self.version

    def getSourceLocation(self):
        return self.source_location

    def getDependencies(self):
        list = self.gateway.jvm.java.util.ArrayList()
        for dep in self.dependencies:
            list.add(dep)

        return list

    def getCapabilityDescription(self):
        return self.description

    def getTags(self):
        list = self.gateway.jvm.java.util.ArrayList()
        for tag in self.tags:
            list.add(tag)

        return list

    def getInterface(self):
        if len(self.interfaces) == 0:
            return None
        return self.interfaces[0]



class ExtensionManager:
    """
    ExtensionManager is responsible for discovery of extensions types and the lifecycle management of those extension types.
    Discovery of extension types includes finding what extension types are available
    (e.g., which Processor types exist on the system), as well as information about those extension types, such as
    the extension's documentation (tags and capability description).

    Lifecycle management includes determining the third-party dependencies that an extension has and ensuring that those
    third-party dependencies have been imported.
    """

    processorInterfaces = ['org.apache.nifi.python.processor.FlowFileTransform', 'org.apache.nifi.python.processor.RecordTransform']
    processor_details = {}
    processor_class_by_name = {}
    module_files_by_extension_type = {}
    dependency_directories = {}

    def __init__(self, gateway):
        self.gateway = gateway


    def get_processor_details(self, classname, version):
        extension_id = ExtensionId(classname=classname, version=version)
        return self.processor_details.get(extension_id)


    def getProcessorTypes(self):
        """
        :return: a list of Processor types that have been discovered by the #discoverExtensions method
        """
        return self.processor_details.values()

    def getProcessorClass(self, type, version, work_dir):
        """
        Returns the Python class that can be used to instantiate a processor of the given type.
        Additionally, it ensures that the required third-party dependencies are on the system path in order to ensure that
        the necessary libraries are available to the Processor so that it can be instantiated and used.

        :param type: the type of Processor
        :param version: the version of the Processor
        :param work_dir: the working directory for extensions
        :return: the Python class that can be used to instantiate a Processor of the given type and version

        :raises ValueError: if there is no known Processor with the given type and version
        """
        id = ExtensionId(classname=type, version=version)
        if id in self.processor_class_by_name:
            return self.processor_class_by_name[id]

        if id not in self.module_files_by_extension_type:
            raise ValueError('Invalid Processor Type: No module is known to contain Processor of type ' + type + ' version ' + version)
        module_file = self.module_files_by_extension_type[id]

        if id in self.processor_details:
            extension_working_dir = os.path.join(work_dir, 'extensions', type, version)
            sys.path.insert(0, extension_working_dir)

        details = self.processor_details[id]
        processor_class = self.__load_extension_module(module_file, details.local_dependencies)
        self.processor_class_by_name[id] = processor_class
        return processor_class


    def reload_processor(self, processor_type, version, work_dir):
        """
        Reloads the class definition for the given processor type. This is used in order to ensure that any changes that have
        been made to the Processor are reloaded and will take effect.

        :param processor_type: the type of the processor whose class definition should be reloaded
        :param version: the version of the processor whose class definition should be reloaded
        :param work_dir: the working directory
        :return: the new class definition
        """
        id = ExtensionId(classname=processor_type, version=version)

        # get the python module file that contains the specified processor
        module_file = self.get_module_file(processor_type, version)

        # Delete the file that tells us that the dependencies have been downloaded. We do this only when reloading a processor
        # because we want to ensure that download any new dependencies
        details = self.processor_details[id]
        completion_marker_file = self.__get_download_complete_marker_file(work_dir, details)
        if os.path.exists(completion_marker_file):
            os.remove(completion_marker_file)

        # Call load_extension to ensure that we load all necessary dependencies, in case they have changed
        self.__gather_extension_details(module_file, work_dir)

        # Reload the processor class itself
        processor_class = self.__load_extension_module(module_file, details.local_dependencies)

        # Update our cache so that when the processor is created again, the new class will be used
        self.processor_class_by_name[id] = processor_class


    def get_module_file(self, processor_type, version):
        """
        Returns the module file that contains the source for the given Processor type and version
        :param processor_type: the Processor type
        :param version: the version of the Processor
        :return: the file that contains the source for the given Processor

        :raises ValueError: if no Processor type is known for the given type and version
        """
        id = ExtensionId(processor_type, version)
        if id not in self.module_files_by_extension_type:
            raise ValueError('Invalid Processor Type: No module is known to contain Processor of type ' + processor_type + ' version ' + version)

        return self.module_files_by_extension_type[id]


    # Discover extensions using the 'prefix' method described in
    # https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/
    def discoverExtensions(self, dirs, work_dir):
        """
        Discovers any extensions that are available in any of the given directories, as well as any extensions that are available
        from PyPi repositories

        :param dirs: the directories to check for any local extensions
        :param work_dir: the working directory
        """
        self.__discover_local_extensions(dirs, work_dir)
        self.__discover_extensions_from_pypi(work_dir)

    def __discover_extensions_from_pypi(self, work_dir):
        self.__discover_extensions_from_paths(None, work_dir, True)

    def __discover_local_extensions(self, dirs, work_dir):
        self.__discover_extensions_from_paths(dirs, work_dir, False)


    def __discover_extensions_from_paths(self, paths, work_dir, require_nifi_prefix):
        if paths is None:
            paths = []

        for path in paths:
            for finder, name, ispkg in pkgutil.iter_modules([path]):
                if not require_nifi_prefix or name.startswith('nifi_'):
                    module_file = '<Unknown Module File>'
                    try:
                        module = finder.find_module(name)
                        module_file = module.path
                        logger.info('Discovered extension %s' % module_file)

                        self.__gather_extension_details(module_file, work_dir)
                    except Exception:
                        logger.error("Failed to load Python extensions from module file {0}. This module will be ignored.".format(module_file), exc_info=True)


    def __gather_extension_details(self, module_file, work_dir, local_dependencies=None):
        path = Path(module_file)
        basename = os.path.basename(module_file)

        # If no local_dependencies have been provided, check to see if there are any.
        # We consider any Python module in the same directory as a local dependency
        if local_dependencies is None:
            local_dependencies = []

            if basename == '__init__.py':
                dir = path.parent
                for filename in os.listdir(dir):
                    if not filename.endswith('.py'):
                        continue
                    if filename == '__init__.py':
                        continue

                    child_module_file = os.path.join(dir, filename)
                    local_dependencies.append(child_module_file)

        # If the module file is an __init__.py file, we check all module files in the same directory.
        if basename == '__init__.py':
            dir = path.parent
            for filename in os.listdir(dir):
                if not filename.endswith('.py'):
                    continue
                if filename == '__init__.py':
                    continue

                child_module_file = os.path.join(dir, filename)
                self.__gather_extension_details(child_module_file, work_dir, local_dependencies=local_dependencies)

        classes_and_details = self.__get_processor_classes_and_details(module_file)
        for classname, details in classes_and_details.items():
            id = ExtensionId(classname, details.version)
            logger.info(f"For {classname} found local dependencies {local_dependencies}")

            details.local_dependencies = local_dependencies

            # Add class name to processor types only if not there already
            if id not in self.processor_details:
                self.processor_details[id] = details

            self.module_files_by_extension_type[id] = module_file


    def __get_download_complete_marker_file(self, work_dir, processor_details):
        version = processor_details.version
        return os.path.join(work_dir, 'extensions', processor_details.type, version, 'dependency-download.complete')


    def __get_dependencies_for_extension_type(self, extension_type, version):
        id = ExtensionId(extension_type, version)
        return self.processor_details[id].dependencies

    def __get_processor_classes_and_details(self, module_file):
        # Parse the python file
        with open(module_file) as file:
            root_node = ast.parse(file.read())

        details_by_class = {}

        # Get top-level class nodes (e.g., MyProcessor)
        class_nodes = self.__get_class_nodes(root_node)

        for class_node in class_nodes:
            logger.debug(f"Checking if class {class_node.name} is a processor")
            if self.__is_processor_class_node(class_node):
                logger.info(f"Discovered Processor class {class_node.name} in module {module_file}")
                details = self.__get_processor_details(class_node, module_file)
                details_by_class[class_node.name] = details

        return details_by_class


    def __is_processor_class_node(self, class_node):
        """
        Checks if the Abstract Syntax Tree (AST) Node represents a Processor class.
        We are looking for any classes within the given module file that look like:

        class MyProcessor:
            ...
            class Java:
                implements = ['org.apache.nifi.python.processor.FlowFileTransform']

        :param class_node: the abstract syntax tree (AST) node
        :return: True if the AST Node represents a Python Class that is a Processor, False otherwise
        """

        # Look for a 'Java' sub-class
        interfaces = self.__get_java_interfaces(class_node)
        return len(interfaces) > 0


    def __get_java_interfaces(self, class_node):
        # Get all class definition nodes
        child_class_nodes = self.__get_class_nodes(class_node)

        interfaces = []
        for child_class_node in child_class_nodes:
            # Look for a 'Java' sub-class
            if child_class_node.name != 'Java':
                continue

            # Look for an assignment that assigns values to the `implements` keyword
            assignment_nodes = self.__get_assignment_nodes(child_class_node)
            for assignment_node in assignment_nodes:
                targets = assignment_node.targets
                if (len(targets) == 1 and targets[0].id == 'implements'):
                    assigned_values = assignment_node.value.elts
                    for assigned_value in assigned_values:
                        if assigned_value.value in self.processorInterfaces:
                            interfaces.append(assigned_value.value)

        return interfaces


    def __get_processor_details(self, class_node, module_file):
        # Look for a 'ProcessorDetails' class
        child_class_nodes = self.__get_class_nodes(class_node)

        # Get the Java interfaces that it implements
        interfaces = self.__get_java_interfaces(class_node)

        for child_class_node in child_class_nodes:
            if child_class_node.name == 'ProcessorDetails':
                logger.debug(f"Found ProcessorDetails class in {class_node.name}")
                version = self.__get_processor_version(child_class_node, class_node.name)
                dependencies = self.__get_processor_dependencies(child_class_node, class_node.name)
                description = self.__get_processor_description(child_class_node, class_node.name)
                tags = self.__get_processor_tags(child_class_node, class_node.name)

                return ExtensionDetails(gateway=self.gateway,
                                        interfaces=interfaces,
                                        type=class_node.name,
                                        version=version,
                                        dependencies=dependencies,
                                        source_location=module_file,
                                        description=description,
                                        tags=tags)

        return ExtensionDetails(gateway=self.gateway,
                                interfaces=interfaces,
                                type=class_node.name,
                                version='Unknown',
                                dependencies=[],
                                source_location=module_file)


    def __get_processor_version(self, details_node, class_name):
        assignment_nodes = self.__get_assignment_nodes(details_node)
        for assignment_node in assignment_nodes:
            targets = assignment_node.targets
            if (len(targets) == 1 and targets[0].id == 'version'):
                assigned_values = assignment_node.value.value
                logger.info("Found version of {0} to be {1}".format(class_name, assigned_values))
                return assigned_values

        # No dependencies found
        logger.info("Found no version information for {0}".format(class_name))
        return 'Unknown'


    def __get_processor_dependencies(self, details_node, class_name):
        deps = self.__get_assigned_list(details_node, class_name, 'dependencies')
        if len(deps) == 0:
            logger.info("Found no external dependencies that are required for class %s" % class_name)
        else:
            logger.info("Found the following external dependencies that are required for class {0}: {1}".format(class_name, deps))

        return deps


    def __get_processor_tags(self, details_node, class_name):
        return self.__get_assigned_list(details_node, class_name, 'tags')


    def __get_assigned_list(self, details_node, class_name, element_name):
        assignment_nodes = self.__get_assignment_nodes(details_node)
        for assignment_node in assignment_nodes:
            targets = assignment_node.targets
            if (len(targets) == 1 and targets[0].id == element_name):
                assigned_values = assignment_node.value.elts
                declared_dependencies = []
                for assigned_value in assigned_values:
                    declared_dependencies.append(assigned_value.value)

                return declared_dependencies

        # No values found
        return []


    def __get_processor_description(self, details_node, class_name):
        assignment_nodes = self.__get_assignment_nodes(details_node)
        for assignment_node in assignment_nodes:
            targets = assignment_node.targets
            if (len(targets) == 1 and targets[0].id == 'description'):
                return assignment_node.value.value

        # No description found
        logger.debug("Found no description for class %s" % class_name)
        return None



    def __get_class_nodes(self, node):
        class_nodes = [n for n in node.body if isinstance(n, ast.ClassDef)]
        return class_nodes


    def __get_assignment_nodes(self, node):
        assignment_nodes = [n for n in node.body if isinstance(n, ast.Assign)]
        return assignment_nodes


    def import_external_dependencies(self, processor_details, work_dir):
        class_name = processor_details.getProcessorType()

        completion_marker_file = self.__get_download_complete_marker_file(work_dir, processor_details)
        target_dir = os.path.dirname(completion_marker_file)

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        if os.path.exists(completion_marker_file):
            logger.info("All dependencies have already been imported for {0}".format(class_name))
            return True

        python_cmd = os.getenv("PYTHON_CMD")
        if processor_details.source_location is not None:
            package_dir = os.path.dirname(processor_details.source_location)
            requirements_file = os.path.join(package_dir, 'requirements.txt')
            if os.path.exists(requirements_file):
                args = [python_cmd, '-m', 'pip', 'install', '--no-cache-dir', '--target', target_dir, '-r', requirements_file]

                logger.info(f"Importing dependencies from requirements file for package {package_dir} to {target_dir} using command {args}")
                result = subprocess.run(args)

                if result.returncode == 0:
                    logger.info(f"Successfully imported requirements for package {package_dir} to {target_dir}")
                else:
                    raise RuntimeError(f"Failed to import requirements for package {package_dir} from requirements.txt file: process exited with status code {result}")

        dependencies = processor_details.getDependencies()
        if len(dependencies) > 0:
            python_cmd = os.getenv("PYTHON_CMD")
            args = [python_cmd, '-m', 'pip', 'install', '--no-cache-dir', '--target', target_dir]
            for dep in dependencies:
                args.append(dep)

            logger.info(f"Importing dependencies {dependencies} for {class_name} to {target_dir} using command {args}")
            result = subprocess.run(args)

            if result.returncode == 0:
                logger.info(f"Successfully imported requirements for {class_name} to {target_dir}")
            else:
                raise RuntimeError(f"Failed to import requirements for {class_name}: process exited with status code {result}")
        else:
            logger.info(f"No dependencies to import for {class_name}")

        # Write a completion Marker File
        with open(completion_marker_file, "w") as file:
            file.write("True")


    def __load_extension_module(self, file, local_dependencies):
        # If there are any local dependencies (i.e., other python files in the same directory), load those modules first
        if local_dependencies is not None and len(local_dependencies) > 0:
            to_load = [dep for dep in local_dependencies]
            if file in to_load:
                to_load.remove(file)

            # There is almost certainly a better way to do this. But we need to load all modules that are 'local dependencies'. I.e., all
            # modules in the same directory/package. But Python does not appear to give us a simple way to do this. We could have a situation in which
            # we have:
            # Module A depends on B
            # Module C depends on B
            # Module B has no dependencies
            # But we don't know the order of the dependencies so if we attempt to import Module A or C first, we get an ImportError because Module B hasn't
            # been imported. To address this, we create a queue of dependencies. If we attempt to import one and it fails, we insert it at the front of the queue
            # so that it will be tried again after trying all dependencies. After we attempt to load a dependency 10 times, we give up and re-throw the error.
            attempts = {}
            for dep in to_load:
                attempts[dep] = 0

            while len(to_load) > 0:
                local_dependency = to_load.pop()

                try:
                    logger.debug(f"Loading local dependency {local_dependency} before loading {file}")
                    self.__load_extension_module(local_dependency, None)
                except:
                    previous_attempts = attempts[local_dependency]
                    if previous_attempts >= 10:
                        raise

                    attempts[local_dependency] = previous_attempts + 1
                    logger.debug(f"Failed to load local dependency {local_dependency}. Will try again after all have been attempted", exc_info=True)
                    to_load.insert(0, local_dependency)

        # Determine the module name
        moduleName = Path(file).name.split('.py')[0]

        # Create the module specification
        moduleSpec = importlib.util.spec_from_file_location(moduleName, file)
        logger.debug(f"Module Spec: {moduleSpec}")

        # Create the module from the specification
        module = importlib.util.module_from_spec(moduleSpec)
        logger.debug(f"Module: {module}")

        # Load the module
        sys.modules[moduleName] = module
        moduleSpec.loader.exec_module(module)
        logger.info(f"Loaded module {moduleName}")

        # Find the Processor class and return it
        for name, member in inspect.getmembers(module):
            if inspect.isclass(member):
                logger.debug(f"Found class: {member}")
                if self.__is_processor_class(member):
                    logger.debug(f"Found Processor: {member}")
                    return member

        return None


    def __is_processor_class(self, potentialProcessorClass):
        # Go through all members of the given class and see if it has an inner class named Java
        for name, member in inspect.getmembers(potentialProcessorClass):
            if name == 'Java' and inspect.isclass(member):
                # Instantiate the Java class
                instance = member()

                # Check if the instance has a method named 'implements'
                hasImplements = False
                for attr in dir(instance):
                    if attr == 'implements':
                        hasImplements = True
                        break

                # If not, move to the next member
                if not hasImplements:
                    continue

                # The class implements something. Check if it implements Processor
                for interface in instance.implements:
                    if interface in self.processorInterfaces:
                        logger.debug(f"{potentialProcessorClass} implements Processor")
                        return True
        return False