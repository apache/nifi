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
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from ExtensionManager import ExtensionManager
from nifiapi.__jvm__ import JvmHolder
import ProcessorInspection

TEST_PROCESSOR_FILE = 'src/test/resources/python/framework/processor_with_dependencies/ProcessorWithDependencies.py'

class FakeJvm:
    def __init__(self):
        self.java = FakeJava()

class FakeJava:
    def __init__(self):
        self.util = FakeJavaUtil()

class FakeJavaUtil:
    def ArrayList(self):
        return FakeArrayList([])

class FakeArrayList:
    def __init__(self, my_list):
        self.my_list = my_list

    def __len__(self):
        return len(self.my_list)

    def __iter__(self):
        return iter(self.my_list)

    def add(self, element):
        self.my_list.append(element)

class ReturncodeMocker:
    def __init__(self):
        self.returncode = 0

@patch('subprocess.run')
class TestDownloadDependencies(unittest.TestCase):
    def test_import_external_dependencies(self, mock_subprocess_run):
        class_nodes = ProcessorInspection.get_processor_class_nodes(TEST_PROCESSOR_FILE)
        self.assertIsNotNone(class_nodes)
        self.assertEqual(len(class_nodes), 1)
        class_node = class_nodes[0]
        self.assertEqual(class_node.name, 'ProcessorWithDependencies')

        details = ProcessorInspection.get_processor_details(class_node, TEST_PROCESSOR_FILE, '/extensions/processor_with_dependencies', False)
        self.assertIsNotNone(details)

        JvmHolder.jvm = FakeJvm()
        extension_manager = ExtensionManager(None)
        mock_subprocess_run.return_value = ReturncodeMocker()

        python_command = sys.executable
        os.environ["PYTHON_CMD"] = python_command

        with tempfile.TemporaryDirectory() as temp_dir:
            packages_dir = os.path.join(temp_dir, 'packages')
            extension_manager.import_external_dependencies(details, packages_dir)

        mock_subprocess_run.assert_called_once()

if __name__ == '__main__':
    unittest.main()
