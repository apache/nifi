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

import unittest
from testutils import get_processor_details

DUMMY_PROCESSOR_FILE = 'src/test/resources/python/framework/dummy_processor/DummyProcessor.py'

class DetectProcessorUseCase(unittest.TestCase):
    def test_get_processor_details(self):
        details = get_processor_details(self, 'DummyProcessor', DUMMY_PROCESSOR_FILE, '/extensions/dummy_processor')
        self.assertIsNotNone(details)
        self.assertEqual(details.description, 'Fake Processor')
        self.assertEqual(details.tags, ['tag1', 'tag2'])
        self.assertEqual(details.extension_home, '/extensions/dummy_processor')
        self.assertEqual(len(details.use_cases), 2)
        self.assertEqual(details.use_cases[0].description, 'First Use Case')
        self.assertEqual(details.use_cases[1].description, 'Second Use Case')
        self.assertEqual(details.use_cases[0].notes, 'First Note')
        self.assertEqual(details.use_cases[1].notes, 'Another Note')
        self.assertEqual(details.use_cases[0].configuration, 'This Processor has no configuration.')
        self.assertEqual(details.use_cases[0].keywords, ['A', 'B'])

        self.assertEqual(len(details.multi_processor_use_cases), 1)
        self.assertEqual(details.multi_processor_use_cases[0].description, 'Multi Processor Use Case')
        self.assertEqual(details.multi_processor_use_cases[0].notes, 'Note #1')
        self.assertEqual(details.multi_processor_use_cases[0].keywords, ['D', 'E'])
        self.assertEqual(len(details.multi_processor_use_cases[0].configurations), 2)
        self.assertEqual(details.multi_processor_use_cases[0].configurations[0].processor_type, 'OtherProcessor')
        self.assertEqual(details.multi_processor_use_cases[0].configurations[0].configuration, 'No config necessary.')
        self.assertEqual(details.multi_processor_use_cases[0].configurations[1].processor_type, 'DummyProcessor')
        self.assertEqual(details.multi_processor_use_cases[0].configurations[1].configuration, 'None.')


if __name__ == '__main__':
    unittest.main()
