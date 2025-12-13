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
This module defines shared PropertyDescriptors that can be imported
by multiple processors. This is a common pattern for reusing property
definitions across processors.
"""

from nifiapi.properties import PropertyDescriptor, StandardValidators

# A shared property that controls the output format
# This property is intended to be imported and used by other processors
SHARED_OUTPUT_FORMAT = PropertyDescriptor(
    name="Output Format",
    description="The format of the output data",
    allowable_values=["json", "xml", "csv"],
    default_value="json",
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR]
)

# Another shared property for enabling/disabling features
SHARED_FEATURE_ENABLED = PropertyDescriptor(
    name="Feature Enabled",
    description="Whether to enable the optional feature",
    allowable_values=["true", "false"],
    default_value="false",
    required=True,
    validators=[StandardValidators.BOOLEAN_VALIDATOR]
)

