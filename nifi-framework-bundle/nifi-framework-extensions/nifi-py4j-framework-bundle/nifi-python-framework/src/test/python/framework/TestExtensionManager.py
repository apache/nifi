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
import tempfile
import unittest
from unittest.mock import patch

from ExtensionManager import ExtensionManager
from testutils import set_up_env, get_processor_details

PROCESSOR_WITH_DEPENDENCIES_TEST_FILE = 'src/test/resources/python/framework/processor_with_dependencies/ProcessorWithDependencies.py'

class ReturncodeMocker:
    def __init__(self, return_code):
        self.returncode = return_code

class TestExtensionManager(unittest.TestCase):
    def setUp(self):
        set_up_env()
        self.extension_manager = ExtensionManager(None)

    @patch('subprocess.run')
    def test_import_external_dependencies(self, mock_subprocess_run):
        details = get_processor_details(self, 'ProcessorWithDependencies', PROCESSOR_WITH_DEPENDENCIES_TEST_FILE, '/extensions/processor_with_dependencies')
        self.assertIsNotNone(details)

        mock_subprocess_run.return_value = ReturncodeMocker(0)

        with tempfile.TemporaryDirectory() as temp_dir:
            packages_dir = os.path.join(temp_dir, 'packages')
            self.extension_manager.import_external_dependencies(details, packages_dir)

        mock_subprocess_run.assert_called_once()

if __name__ == '__main__':
    unittest.main()
