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

"""
Tests for ProcessorInspection module.

This test module includes tests for NIFI-14233: Python Processor can not use
imported properties as PropertyDependency.

The issue is that when a PropertyDescriptor is imported from another module
and used as a PropertyDependency, the AST-based inspection fails with a KeyError
because it can only discover PropertyDescriptors defined within the current class.
"""

import os
import unittest

import ProcessorInspection
from testutils import set_up_env, get_processor_details


# Use absolute path resolution based on the script location
# _SCRIPT_DIR: .../nifi-python-framework/src/test/python/framework
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# _TEST_RESOURCES_DIR: .../nifi-python-framework/src/test/resources/python/framework
_TEST_RESOURCES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(_SCRIPT_DIR)),  # Go up from python/framework/ to test/
    'resources/python/framework'
)

# Path to the test processor that uses imported properties as dependencies
IMPORTED_DEPENDENCY_TEST_DIR = os.path.join(_TEST_RESOURCES_DIR, 'imported_property_dependency')
IMPORTED_DEPENDENCY_TEST_FILE = os.path.join(IMPORTED_DEPENDENCY_TEST_DIR, 'ProcessorWithImportedDependency.py')

# Path to the existing ConditionalProcessor which uses local dependencies (should work)
# Navigate from test/python/framework up to nifi root
# _SCRIPT_DIR is .../nifi-python-framework/src/test/python/framework
# We need to go up 8 levels to reach the nifi root:
# framework -> python -> test -> src -> nifi-python-framework -> nifi-py4j-framework-bundle -> 
# nifi-framework-extensions -> nifi-framework-bundle -> nifi
_NIFI_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR))))
))))
CONDITIONAL_PROCESSOR_FILE = os.path.join(
    _NIFI_ROOT,
    'nifi-extension-bundles/nifi-py4j-extension-bundle/nifi-python-test-extensions/src/main/resources/extensions/ConditionalProcessor.py'
)


class TestProcessorInspection(unittest.TestCase):
    """Tests for the ProcessorInspection module."""

    def setUp(self):
        set_up_env()

    def test_get_processor_class_nodes_finds_processor(self):
        """Test that get_processor_class_nodes correctly identifies processor classes."""
        class_nodes = ProcessorInspection.get_processor_class_nodes(IMPORTED_DEPENDENCY_TEST_FILE)
        self.assertIsNotNone(class_nodes)
        self.assertEqual(len(class_nodes), 1)
        self.assertEqual(class_nodes[0].name, 'ProcessorWithImportedDependency')

    def test_local_property_dependency_works(self):
        """
        Test that PropertyDependency with locally-defined properties works correctly.
        
        This test uses ConditionalProcessor which defines all properties locally
        and uses them as dependencies. This should work without issues.
        """
        # Skip if the file doesn't exist (might be in a different test environment)
        if not os.path.exists(CONDITIONAL_PROCESSOR_FILE):
            self.skipTest(f"ConditionalProcessor.py not found at {CONDITIONAL_PROCESSOR_FILE}")

        class_nodes = ProcessorInspection.get_processor_class_nodes(CONDITIONAL_PROCESSOR_FILE)
        self.assertIsNotNone(class_nodes)
        self.assertEqual(len(class_nodes), 1)
        
        class_node = class_nodes[0]
        self.assertEqual(class_node.name, 'ConditionalProcessor')
        
        # This should work without raising an exception
        details = ProcessorInspection.get_processor_details(
            class_node, 
            CONDITIONAL_PROCESSOR_FILE, 
            '/extensions/conditional', 
            False
        )
        self.assertIsNotNone(details)

    def test_imported_property_dependency_does_not_raise_key_error(self):
        """
        Test that verifies the fix for NIFI-14233: PropertyDependency with imported 
        properties should NOT cause a KeyError during processor inspection.
        
        After the fix:
        1. ProcessorInspection resolves imported PropertyDescriptors from source modules
        2. The resolve_dependencies() method uses .get() to avoid KeyError
        3. Imported properties are correctly resolved and dependencies work
        """
        class_nodes = ProcessorInspection.get_processor_class_nodes(IMPORTED_DEPENDENCY_TEST_FILE)
        self.assertIsNotNone(class_nodes)
        self.assertEqual(len(class_nodes), 1)
        
        class_node = class_nodes[0]
        self.assertEqual(class_node.name, 'ProcessorWithImportedDependency')
        
        # This should NOT raise a KeyError after the fix
        # Instead, it should successfully process the imported property dependencies
        try:
            details = ProcessorInspection.get_processor_details(
                class_node, 
                IMPORTED_DEPENDENCY_TEST_FILE, 
                IMPORTED_DEPENDENCY_TEST_DIR, 
                False
            )
            self.assertIsNotNone(details)
        except KeyError as e:
            self.fail(f"KeyError should not be raised after NIFI-14233 fix: {e}")

    def test_imported_property_dependency_works_correctly(self):
        """
        Test that imported property dependencies work correctly after the NIFI-14233 fix.
        
        This test verifies that:
        1. No exception is raised during processor inspection
        2. Property descriptions are correctly extracted
        3. Dependencies on imported properties are properly resolved
        """
        class_nodes = ProcessorInspection.get_processor_class_nodes(IMPORTED_DEPENDENCY_TEST_FILE)
        self.assertIsNotNone(class_nodes)
        self.assertEqual(len(class_nodes), 1)
        
        class_node = class_nodes[0]
        
        # Get processor details - this should work without raising any exception
        details = ProcessorInspection.get_processor_details(
            class_node, 
            IMPORTED_DEPENDENCY_TEST_FILE, 
            IMPORTED_DEPENDENCY_TEST_DIR, 
            False
        )
        
        # Verify basic details
        self.assertIsNotNone(details)
        self.assertEqual(details.type, 'ProcessorWithImportedDependency')
        self.assertEqual(details.version, '0.0.1-SNAPSHOT')
        
        # Verify property descriptions were extracted
        property_descriptions = list(details.property_descriptions) if details.property_descriptions else []
        
        # We should have at least the two properties with dependencies
        # (JSON_PRETTY_PRINT and FEATURE_CONFIG)
        property_names = [p.name for p in property_descriptions]
        self.assertIn('Pretty Print JSON', property_names)
        self.assertIn('Feature Configuration', property_names)
        
        # Verify that dependencies were correctly resolved
        for prop in property_descriptions:
            if prop.name == 'Pretty Print JSON':
                self.assertIsNotNone(prop.dependencies)
                self.assertTrue(len(prop.dependencies) > 0)
                # The dependency should reference "Output Format" (the name of SHARED_OUTPUT_FORMAT)
                dep_names = [d.name for d in prop.dependencies]
                self.assertIn('Output Format', dep_names)


class TestPropertyDependencyResolution(unittest.TestCase):
    """
    Focused tests for property dependency resolution in ProcessorInspection.
    
    These tests specifically target the resolve_dependencies() method and
    the CollectPropertyDescriptorVisitors class.
    """

    def setUp(self):
        set_up_env()

    def test_resolve_dependencies_with_missing_property_handles_gracefully(self):
        """
        Test that resolve_dependencies handles missing properties gracefully
        without raising KeyError.
        
        After the fix for NIFI-14233, when a dependent property is not found,
        the code uses .get() which returns None instead of raising KeyError.
        The dependency is logged as a warning and skipped.
        """
        import ast
        
        # Create a minimal AST node that simulates a dependency list
        # This simulates: [PropertyDependency(MISSING_PROPERTY, "value")]
        code = '[PropertyDependency(MISSING_PROPERTY, "value")]'
        tree = ast.parse(code, mode='eval')
        dependency_list_node = tree.body  # This is the List node
        
        module_string_constants = {}
        visitor = ProcessorInspection.CollectPropertyDescriptorVisitors(
            module_string_constants, 
            'TestProcessor'
        )
        
        # The visitor has no discovered_property_descriptors, so MISSING_PROPERTY won't be found
        # After the fix, this should NOT raise KeyError - it should return an empty list
        # and log a warning instead
        result = visitor.resolve_dependencies(dependency_list_node)
        
        # The result should be an empty list since the property couldn't be resolved
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()

