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
import logging
import textwrap
import os
import BundleCoordinate
from nifiapi.documentation import UseCaseDetails, MultiProcessorUseCaseDetails, ProcessorConfiguration, PropertyDescription, PropertyDependency

import ExtensionDetails

PROCESSOR_INTERFACES = ['org.apache.nifi.python.processor.FlowFileTransform',
                        'org.apache.nifi.python.processor.RecordTransform',
                        'org.apache.nifi.python.processor.FlowFileSource']

logger = logging.getLogger("python.ProcessorInspection")


class StringConstantVisitor(ast.NodeVisitor):
    def __init__(self):
        self.string_assignments = {}

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            string_value = node.value.value
            for target in node.targets:
                if isinstance(target, ast.Name):
                    variable_name = target.id
                    self.string_assignments[variable_name] = string_value
                elif isinstance(target, ast.Tuple):
                    for element in target.elts:
                        if isinstance(element, ast.Name):
                            variable_name = element.id
                            self.string_assignments.append[variable_name] = string_value
        self.generic_visit(node)

class CollectPropertyDescriptorVisitors(ast.NodeVisitor):

    def __init__(self, module_string_constants, processor_name):
        self.module_string_constants = module_string_constants
        self.discovered_property_descriptors = {}
        self.processor_name = processor_name
        self.logger = logging.getLogger("python.CollectPropertyDescriptorVisitors")

    def resolve_dependencies(self, node: ast.AST):
        resolved_dependencies = []
        for dependency in node.elts:
            variable_name = dependency.args[0].id
            if not self.discovered_property_descriptors[variable_name]:
                self.logger.error(f"Not able to find actual property descriptor for {variable_name}, so not able to resolve property dependencies in {self.processor_name}.")
            else:
                actual_property = self.discovered_property_descriptors[variable_name]
                dependent_values = []
                for dependent_value in dependency.args[1:]:
                    dependent_values.append(get_constant_values(dependent_value, self.module_string_constants))
                resolved_dependencies.append(PropertyDependency(name = actual_property.name,
                                                                display_name = actual_property.display_name,
                                                                dependent_values = dependent_values))
        return resolved_dependencies

    def resolve_property_descriptor_name_in_code(self, node: ast.AST):
        if isinstance(node.targets[0], ast.Name):
            return node.targets[0].id
        elif isinstance(node.targets[0], ast.Attribute):
            return node.targets[0].attr
        else:
            raise Exception("Unable to determine name from source code")

    def visit_Assign(self, node: ast.AST):
        if self.assignment_is_property_descriton(node):
            property_descriptor_name_in_code = self.resolve_property_descriptor_name_in_code(node)
            self.logger.debug(f"Found PropertyDescriptor in the following assignment {property_descriptor_name_in_code}")
            if not node.value.keywords:
                self.logger.error(f"Not able to parse {property_descriptor_name_in_code} PropertyDescriptor as no keywords assignments used.")
            else:
                descriptor_info = {}
                for keyword in node.value.keywords:
                    key = keyword.arg
                    if key == 'dependencies':
                        self.logger.debug(f"Resolving dependencies for {property_descriptor_name_in_code}.")
                        value = self.resolve_dependencies(keyword.value)
                    else:
                        value = get_constant_values(keyword.value, self.module_string_constants)
                    descriptor_info[key] = value

                self.discovered_property_descriptors[property_descriptor_name_in_code] = PropertyDescription(name=descriptor_info.get('name'),
                                                                                        description=descriptor_info.get('description'),
                                                                                         display_name=replace_null(descriptor_info.get('display_name'), descriptor_info.get('name')),
                                                                                         required=replace_null(descriptor_info.get('required'), False),
                                                                                         sensitive=replace_null(descriptor_info.get('sensitive'), False),
                                                                                         default_value=descriptor_info.get('default_value'),
                                                                                         expression_language_scope=replace_null(descriptor_info.get('expression_language_scope'), 'NONE'),
                                                                                            controller_service_definition=descriptor_info.get('controller_service_definition'),
                                                                                          allowable_values = descriptor_info.get('allowable_values'),
                                                                                          dependencies = descriptor_info.get('dependencies'))
        self.generic_visit(node)


    def assignment_is_property_descriton(self, node: ast.AST):
        return isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == 'PropertyDescriptor'


def get_module_string_constants(module_file: str) -> dict:
    with open(module_file) as file:
        root_node = ast.parse(file.read())
    visitor = StringConstantVisitor()
    visitor.visit(root_node)
    return visitor.string_assignments


def get_processor_class_nodes(module_file: str) -> list:
    with open(module_file) as file:
        root_node = ast.parse(file.read())

    processor_class_nodes = []
    class_nodes = get_class_nodes(root_node)
    for class_node in class_nodes:
        if is_processor_class_node(class_node):
            processor_class_nodes.append(class_node)

    return processor_class_nodes


def get_processor_details(class_node, module_file, extension_home, dependencies_bundled):
    # Look for a 'ProcessorDetails' class
    child_class_nodes = get_class_nodes(class_node)
    module_string_constants = get_module_string_constants(module_file)

    # Get the Java interfaces that it implements
    interfaces = get_java_interfaces(class_node)

    for child_class_node in child_class_nodes:
        if child_class_node.name == 'ProcessorDetails':
            logger.debug(f"Found ProcessorDetails class in {class_node.name}")
            version = __get_processor_version(child_class_node)
            dependencies = __get_processor_dependencies(child_class_node, class_node.name)
            description = __get_processor_description(child_class_node)
            tags = __get_processor_tags(child_class_node)
            use_cases = get_use_cases(class_node)
            multi_processor_use_cases = get_multi_processor_use_cases(class_node)
            property_descriptions = get_property_descriptions(class_node, module_string_constants)
            bundle_coordinate = __get_bundle_coordinate(extension_home)

            return ExtensionDetails.ExtensionDetails(interfaces=interfaces,
                                                     type=class_node.name,
                                                     version=version,
                                                     dependencies=dependencies,
                                                     source_location=module_file,
                                                     extension_home=extension_home,
                                                     dependencies_bundled=dependencies_bundled,
                                                     description=description,
                                                     tags=tags,
                                                     use_cases=use_cases,
                                                     multi_processor_use_cases=multi_processor_use_cases,
                                                     property_descriptions=property_descriptions,
                                                     bundle_coordinate=bundle_coordinate)

    return ExtensionDetails.ExtensionDetails(interfaces=interfaces,
                            type=class_node.name,
                            version='Unknown',
                            dependencies=[],
                            source_location=module_file,
                            extension_home=extension_home,
                            dependencies_bundled=dependencies_bundled)


def __get_processor_version(details_node):
    return get_assigned_value(details_node, 'version', 'Unknown')


def __get_processor_dependencies(details_node, class_name):
    deps = get_assigned_value(details_node, 'dependencies', [])
    if len(deps) == 0:
        logger.debug("Found no external dependencies that are required for class %s" % class_name)
    else:
        logger.debug("Found the following external dependencies that are required for class {0}: {1}".format(class_name, deps))

    return deps


def __get_processor_tags(details_node):
    return get_assigned_value(details_node, 'tags', [])


def get_use_cases(class_node) -> list[UseCaseDetails]:
    decorators = class_node.decorator_list
    if not decorators:
        return []

    use_cases = []
    for decorator in decorators:
        if decorator.func.id != 'use_case' or not decorator.keywords:
            continue

        kv_pairs = {}
        for keyword in decorator.keywords:
            keyword_name = keyword.arg
            keyword_value = get_constant_values(keyword.value)
            if keyword_value is not None and isinstance(keyword_value, str):
                keyword_value = textwrap.dedent(keyword_value).strip()
            kv_pairs[keyword_name] = keyword_value

        use_case = UseCaseDetails(
            description=dedent(kv_pairs.get('description')),
            notes=dedent(kv_pairs.get('notes')),
            keywords=kv_pairs.get('keywords'),
            configuration=dedent(kv_pairs.get('configuration'))
        )
        use_cases.append(use_case)

    return use_cases

def dedent(val: str) -> str:
    if not val:
        return ""
    return textwrap.dedent(val)

def get_multi_processor_use_cases(class_node) -> list[MultiProcessorUseCaseDetails]:
    decorators = class_node.decorator_list
    if not decorators:
        return []

    use_cases = []
    for decorator in decorators:
        if decorator.func.id != 'multi_processor_use_case' or not decorator.keywords:
            continue

        kv_pairs = {}
        for keyword in decorator.keywords:
            keyword_name = keyword.arg
            if keyword_name == 'configurations':
                keyword_value = get_processor_configurations(keyword.value)
            else:
                keyword_value = get_constant_values(keyword.value)

            if keyword_value is not None and isinstance(keyword_value, str):
                keyword_value = textwrap.dedent(keyword_value).strip()
            kv_pairs[keyword_name] = keyword_value

        use_case = MultiProcessorUseCaseDetails(
            description=dedent(kv_pairs.get('description')),
            notes=dedent(kv_pairs.get('notes')),
            keywords=kv_pairs.get('keywords'),
            configurations=kv_pairs.get('configurations')
        )
        use_cases.append(use_case)

    return use_cases


def get_processor_configurations(constructor_calls: ast.List) -> list:
    configurations = []
    for constructor_call in constructor_calls.elts:
        if not isinstance(constructor_call, ast.Call) or constructor_call.func.id != 'ProcessorConfiguration':
            continue

        kv_pairs = {}
        for keyword in constructor_call.keywords:
            keyword_name = keyword.arg
            keyword_value = get_constant_values(keyword.value)
            if keyword_value is not None and isinstance(keyword_value, str):
                keyword_value = textwrap.dedent(keyword_value).strip()

            kv_pairs[keyword_name] = keyword_value

        processor_config = ProcessorConfiguration(processor_type=kv_pairs.get('processor_type'),
                                                  configuration=kv_pairs.get('configuration'))
        configurations.append(processor_config)

    return configurations


def get_property_descriptions(class_node, module_string_constants):
    visitor = CollectPropertyDescriptorVisitors(module_string_constants, class_node.name)
    visitor.visit(class_node)
    return visitor.discovered_property_descriptors.values()


def replace_null(val: any, replacement: any):
    return val if val else replacement

def get_assigned_value(class_node, assignment_id, default_value=None):
    assignments = get_assignment_nodes(class_node)
    for assignment in assignments:
        targets = assignment.targets
        if len(targets) != 1 or targets[0].id != assignment_id:
            continue
        assigned_value = assignment.value
        return get_constant_values(assigned_value)

    return default_value

def get_constant_values(val, string_constants: dict = {}):
    if isinstance(val, ast.Constant):
        return val.value
    if isinstance(val, ast.Name):
        return string_constants.get(val.id)
    if isinstance(val, ast.List):
        return [get_constant_values(v, string_constants) for v in val.elts]
    if isinstance(val, ast.Dict):
        keys = val.keys
        values = val.values
        key_values = [get_constant_values(v, string_constants).strip() for v in keys]
        value_values = [get_constant_values(v, string_constants).strip() for v in values]
        return dict(zip(key_values, value_values))
    if isinstance(val, ast.Attribute):
        return val.attr
    if isinstance(val, ast.BinOp) and isinstance(val.op, ast.Add):
        left = get_constant_values(val.left, string_constants)
        right = get_constant_values(val.right, string_constants)
        if left and right:
            return left + right
        if left and not right:
            return left
        if right and not left:
            return right

    return None


def __get_processor_description(details_node):
    return get_assigned_value(details_node, 'description')


def is_processor_class_node(class_node):
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
    interfaces = get_java_interfaces(class_node)
    return len(interfaces) > 0

def get_java_interfaces(class_node):
    # Get all class definition nodes
    child_class_nodes = get_class_nodes(class_node)

    interfaces = []
    for child_class_node in child_class_nodes:
        # Look for a 'Java' sub-class
        if child_class_node.name != 'Java':
            continue

        implemented = get_assigned_value(child_class_node, 'implements')
        interfaces.extend([ifc_name for ifc_name in implemented if ifc_name in PROCESSOR_INTERFACES])

    return interfaces

def get_class_nodes(node) -> list:
    return [n for n in node.body if isinstance(n, ast.ClassDef)]

def get_assignment_nodes(node) -> list:
    return [n for n in node.body if isinstance(n, ast.Assign)]

def __get_bundle_coordinate(extension_home):
    group = 'unknown'
    id = 'unknown'
    version = 'unknown'

    manifest_file = os.path.join(extension_home, 'META-INF/MANIFEST.MF')
    if os.path.exists(manifest_file):
        with open(manifest_file) as file:
            for line in file:
                stripped_line = line.strip()
                if stripped_line.startswith('Nar-Group'):
                    group = __get_manifest_value(stripped_line)
                if stripped_line.startswith('Nar-Id'):
                    id = __get_manifest_value(stripped_line)
                if stripped_line.startswith('Nar-Version'):
                    version = __get_manifest_value(stripped_line)

    logger.debug(f"Bundle coordinate from {manifest_file} is [{group} - {id} - {version}]")
    return BundleCoordinate.BundleCoordinate(group=group, id=id, version=version)

def __get_manifest_value(line):
    parts = line.split(':')
    if len(parts) > 1:
        return parts[1].strip()
