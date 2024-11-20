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

import importlib
import importlib.util
import inspect
import logging
import os
import pkgutil
import subprocess
import sys
from pathlib import Path

import ProcessorInspection

logger = logging.getLogger("python.ExtensionManager")


# A simple wrapper class to encompass a processor type and its version
class ExtensionId:
    def __init__(self, classname=None, version=None):
        self.classname = classname
        self.version = version

    def __hash__(self):
        return hash((self.classname, self.version))

    def __eq__(self, other):
        return (self.classname, self.version) == (other.classname, other.version)



class ExtensionManager:
    """
    ExtensionManager is responsible for discovery of extensions types and the lifecycle management of those extension types.
    Discovery of extension types includes finding what extension types are available
    (e.g., which Processor types exist on the system), as well as information about those extension types, such as
    the extension's documentation (tags and capability description).

    Lifecycle management includes determining the third-party dependencies that an extension has and ensuring that those
    third-party dependencies have been imported.
    """

    processor_interfaces = ['org.apache.nifi.python.processor.FlowFileTransform',
                            'org.apache.nifi.python.processor.RecordTransform',
                            'org.apache.nifi.python.processor.FlowFileSource']
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
        dependencies_bundled = details.isBundledWithDependencies()
        extension_home = details.getExtensionHome()
        self.__gather_extension_details(module_file, extension_home, dependencies_bundled, work_dir)

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
            # If the path has a child directory named NAR-INF, we note that it has dependencies bundled with it
            nar_inf_dir = os.path.join(path, 'NAR-INF')
            dependencies_bundled = os.path.exists(nar_inf_dir)

            for finder, name, ispkg in pkgutil.iter_modules([path]):
                if not require_nifi_prefix or name.startswith('nifi_'):
                    module_file = '<Unknown Module File>'
                    try:
                        module = finder.find_spec(name)
                        module_file = module.origin

                        # Ignore any packaged dependencies
                        if 'NAR-INF/bundled-dependencies' in module_file:
                            continue

                        logger.debug('Discovered extension %s' % module_file)

                        self.__gather_extension_details(module_file, path, dependencies_bundled, work_dir)
                    except Exception:
                        logger.error("Failed to load Python extensions from module file {0}. This module will be ignored.".format(module_file), exc_info=True)


    def __gather_extension_details(self, module_file, extension_home, dependencies_bundled, work_dir, local_dependencies=None):
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
                self.__gather_extension_details(child_module_file, extension_home, dependencies_bundled, work_dir, local_dependencies=local_dependencies)

        classes_and_details = self.__get_processor_classes_and_details(module_file, extension_home, dependencies_bundled)
        for classname, details in classes_and_details.items():
            id = ExtensionId(classname, details.version)
            logger.debug(f"For {classname} found local dependencies {local_dependencies}")

            details.local_dependencies = local_dependencies

            # Add class name to processor types only if not there already
            if id not in self.processor_details:
                logger.debug(f"Storing processor details for {classname} - {details.version}")
                self.processor_details[id] = details

            self.module_files_by_extension_type[id] = module_file


    def __get_download_complete_marker_file(self, work_dir, processor_details):
        version = processor_details.version
        return os.path.join(work_dir, 'extensions', processor_details.type, version, 'dependency-download.complete')


    def __get_dependencies_for_extension_type(self, extension_type, version):
        id = ExtensionId(extension_type, version)
        return self.processor_details[id].dependencies

    def __get_processor_classes_and_details(self, module_file, extension_home, dependencies_bundled):
        class_nodes = ProcessorInspection.get_processor_class_nodes(module_file)
        details_by_class = {}

        for class_node in class_nodes:
            logger.debug(f"Discovered Processor class {class_node.name} in module {module_file} with home {extension_home}")
            details = ProcessorInspection.get_processor_details(class_node, module_file, extension_home, dependencies_bundled=dependencies_bundled)
            details_by_class[class_node.name] = details

        return details_by_class


    def import_external_dependencies(self, processor_details, work_dir):
        class_name = processor_details.getProcessorType()

        completion_marker_file = self.__get_download_complete_marker_file(work_dir, processor_details)
        target_dir = os.path.dirname(completion_marker_file)

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        if os.path.exists(completion_marker_file):
            logger.info("All dependencies have already been imported for {0}".format(class_name))
            return True

        dependency_references = []

        if processor_details.source_location is not None:
            package_dir = os.path.dirname(processor_details.source_location)
            requirements_file = os.path.join(package_dir, 'requirements.txt')
            if os.path.exists(requirements_file):
                dependency_references.append('-r')
                dependency_references.append(requirements_file)

        inline_dependencies = processor_details.getDependencies()
        for dependency in inline_dependencies:
            dependency_references.append(dependency)

        if len(dependency_references) > 0:
            python_cmd = os.getenv("PYTHON_CMD")
            args = [python_cmd, '-m', 'pip', 'install', '--no-cache-dir', '--target', target_dir] + dependency_references
            logger.info(f"Installing dependencies {dependency_references} for {class_name} to {target_dir} using command {args}")
            result = subprocess.run(args)

            if result.returncode == 0:
                logger.info(f"Successfully installed requirements for {class_name} to {target_dir}")
            else:
                raise RuntimeError(f"Failed to install requirements for {class_name}: process exited with status code {result}")
        else:
            logger.info(f"No dependencies to install for {class_name}")

        # Write a completion Marker File
        with open(completion_marker_file, "w") as file:
            file.write("True")


    def __load_extension_module(self, file, local_dependencies):
        # If there are any local dependencies (i.e., other python files in the same directory), load those modules first
        if local_dependencies:
            to_load = [dep for dep in local_dependencies if dep != file and not self.__is_processor_module(dep)]

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
        module_name = Path(file).name.split('.py')[0]

        # Create the module specification
        module_spec = importlib.util.spec_from_file_location(module_name, file)
        logger.debug(f"Module Spec: {module_spec}")

        # Create the module from the specification
        module = importlib.util.module_from_spec(module_spec)
        logger.debug(f"Module: {module}")

        # Load the module
        sys.modules[module_name] = module
        module_spec.loader.exec_module(module)
        logger.info(f"Loaded module {module_name}")

        # Find the Processor class and return it
        for name, member in inspect.getmembers(module):
            if inspect.isclass(member):
                logger.debug(f"Found class: {member}")
                if self.__is_processor_class(member):
                    logger.debug(f"Found Processor: {member}")
                    return member

        return None


    def __is_processor_module(self, module_file):
        return len(ProcessorInspection.get_processor_class_nodes(module_file)) > 0


    def __is_processor_class(self, potential_processor_class):
        # Go through all members of the given class and see if it has an inner class named Java
        for name, member in inspect.getmembers(potential_processor_class):
            if name == 'Java' and inspect.isclass(member):
                # Instantiate the Java class
                instance = member()

                # Check if the instance has a method named 'implements'
                has_implements = False
                for attr in dir(instance):
                    if attr == 'implements':
                        has_implements = True
                        break

                # If not, move to the next member
                if not has_implements:
                    continue

                # The class implements something. Check if it implements Processor
                for interface in instance.implements:
                    if interface in self.processor_interfaces:
                        logger.debug(f"{potential_processor_class} implements Processor")
                        return True
        return False

    def remove_processor_type(self, processorType, version):
        logger.debug(f"Removing processor {processorType} - {version}")
        extension_id = ExtensionId(processorType, version)
        self.processor_details.pop(extension_id, None)
        self.processor_class_by_name.pop(extension_id, None)
        self.module_files_by_extension_type.pop(extension_id, None)