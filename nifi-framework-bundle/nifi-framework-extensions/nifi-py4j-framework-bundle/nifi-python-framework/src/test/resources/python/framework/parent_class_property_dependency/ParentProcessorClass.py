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
A parent class that defines a PropertyDescriptor as a class-level attribute.

This is the pattern that caused a crash in NiFi 2.1.0+:
  - The parent class is imported as a class (not its individual properties)
  - Child processors reference the property as ParentClass.MY_PROP in PropertyDependency
  - ProcessorInspection must resolve that attribute-style reference correctly
"""

from nifiapi.properties import PropertyDescriptor, StandardValidators


class ParentProcessorClass:

    PARENT_ENABLE_FEATURE = PropertyDescriptor(
        name="Enable Feature",
        description="Whether to enable the optional feature",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
        validators=[StandardValidators.BOOLEAN_VALIDATOR]
    )
