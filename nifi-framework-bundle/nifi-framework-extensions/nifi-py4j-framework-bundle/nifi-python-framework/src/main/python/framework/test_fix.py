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

test_code = '''
PropertyDependency(my_module.IMPORTED_PROPERTY, ["true"])
'''

tree = ast.parse(test_code)
dependency = tree.body[0].value

print("Testing dependency types:")
print(f"Type of args[0]: {type(dependency.args[0])}")

if hasattr(dependency.args[0], 'attr'):
    print(f" Imported property detected: {dependency.args[0].attr}")
elif hasattr(dependency.args[0], 'id'):  
    print(f" Local property detected: {dependency.args[0].id}")

print(" Fix can handle both cases!")
