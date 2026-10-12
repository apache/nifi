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

from testutils import set_up_env

set_up_env()

from nifiapi.properties import PropertyContext


class FakeJavaPropertyValue:
    def __init__(self, expression_language_present):
        self.expression_language_present = expression_language_present

    def isExpressionLanguagePresent(self):
        return self.expression_language_present


class RecognizeTrivialAttributeReferenceUseCase(unittest.TestCase):
    """PropertyContext.create_python_property_value looks for a property value that is
    *entirely* a single attribute reference, such as "${attr}" or "${'attr with spaces'}", so
    that PythonPropertyValue.evaluateAttributeExpressions can take a fast path and look the
    attribute up directly instead of invoking the full expression language engine.

    A value that merely *contains* such a reference, with literal text before or after it,
    must not take that fast path: the literal text has to survive evaluation too."""

    def setUp(self):
        self.context = PropertyContext()

    def matched_attribute(self, string_value):
        property_value = self.context.create_python_property_value(True, FakeJavaPropertyValue(True), string_value)
        return property_value.referenced_attribute

    def test_bare_reference_is_recognized(self):
        self.assertEqual('my_attribute', self.matched_attribute('${my_attribute}'))

    def test_bare_escaped_reference_is_recognized(self):
        self.assertEqual('my attribute', self.matched_attribute("${'my attribute'}"))

    def test_reference_with_trailing_text_is_not_trivial(self):
        self.assertIsNone(self.matched_attribute('${my_attribute}_foo_bar'))

    def test_reference_with_leading_text_is_not_trivial(self):
        self.assertIsNone(self.matched_attribute('foo_bar_${my_attribute}'))

    def test_escaped_reference_with_trailing_text_is_not_trivial(self):
        self.assertIsNone(self.matched_attribute("${'my attribute'}_foo_bar"))

    def test_two_references_are_not_trivial(self):
        self.assertIsNone(self.matched_attribute('${first}${second}'))

    def test_no_expression_language_present_is_not_trivial(self):
        property_value = self.context.create_python_property_value(True, FakeJavaPropertyValue(False), '${my_attribute}')
        self.assertIsNone(property_value.referenced_attribute)


if __name__ == '__main__':
    unittest.main()
