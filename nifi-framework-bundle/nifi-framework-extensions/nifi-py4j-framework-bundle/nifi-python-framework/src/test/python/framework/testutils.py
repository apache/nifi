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
import sys
from nifiapi.__jvm__ import JvmHolder
import ProcessorInspection

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

def set_up_env():
    python_command = sys.executable
    os.environ["PYTHON_CMD"] = python_command
    JvmHolder.jvm = FakeJvm()

def get_processor_details(test_fixture, processor_name, processor_file, extension_home):
    class_nodes = ProcessorInspection.get_processor_class_nodes(processor_file)
    test_fixture.assertIsNotNone(class_nodes)
    test_fixture.assertEqual(len(class_nodes), 1)
    class_node = class_nodes[0]
    test_fixture.assertEqual(class_node.name, processor_name)

    return ProcessorInspection.get_processor_details(class_node, processor_file, extension_home, False)
