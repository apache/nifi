/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processor;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.flowfile.FlowFile;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestStandardPropertyValue {

    private final ControllerServiceLookup lookup = new TestControllerServiceLookup();

    @Test
    public void testSubstituteAttributesWithOneMatchingArg() {
        final PropertyValue value = new StandardPropertyValue("Hello, ${audience}!", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("audience", "World");
        assertEquals("Hello, World!", value.evaluateAttributeExpressions(createFlowFile(attributes)).getValue());
    }

    @Test
    public void testMissingEndBraceEvaluatesToStringLiteral() {
        final PropertyValue value = new StandardPropertyValue("Hello, ${audience!", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("audience", "World");
        assertEquals("Hello, ${audience!", value.evaluateAttributeExpressions(createFlowFile(attributes)).getValue());
    }

    @Test
    public void testEscaped() {
        final PropertyValue value = new StandardPropertyValue("Hello, $${audience}!", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("audience", "World");
        assertEquals("Hello, ${audience}!", value.evaluateAttributeExpressions(createFlowFile(attributes)).getValue());
    }

    @Test
    public void testSubstituteAttributesWithMultipleMatchingArgs() {
        final PropertyValue value = new StandardPropertyValue("Hello, ${audience}${comma}${question}!", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("audience", "World");
        attributes.put("comma", ",");
        attributes.put("question", " how are you?");
        assertEquals("Hello, World, how are you?!", value.evaluateAttributeExpressions(createFlowFile(attributes)).getValue());
    }

    @Test
    public void testSubstituteAttributesWithNoMatch() {
        final PropertyValue value = new StandardPropertyValue("Hello, ${audience}${comma}${question:replaceNull('')}!", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        assertEquals("Hello, !", value.evaluateAttributeExpressions(createFlowFile(attributes)).getValue());
    }

    @Test
    public void testSubstituteAttributesRecursively() {
        final PropertyValue value = new StandardPropertyValue("Hello, ${'${a}${b}'}!", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "b");
        attributes.put("b", "World");
        attributes.put("bWorld", "World");
        assertEquals("Hello, World!", value.evaluateAttributeExpressions(createFlowFile(attributes)).getValue());
    }

    @Test
    public void testGetValueAsIntegerAfterSubstitute() {
        final PropertyValue value = new StandardPropertyValue("1${value}", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "39");
        assertEquals(139, value.evaluateAttributeExpressions(createFlowFile(attributes)).asInteger().intValue());
    }

    @Test()
    public void testGetValueAsIntegerAfterSubstitutingWithNonInteger() {
        final PropertyValue value = new StandardPropertyValue("1${value}", lookup, ParameterLookup.EMPTY);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "Yes");
        final PropertyValue substituted = value.evaluateAttributeExpressions(createFlowFile(attributes));

        assertThrows(NumberFormatException.class, substituted::asInteger);
    }

    @Test
    public void testGetValueAsAllowableValue() {
        for (ExampleDescribedValueEnum enumValue : ExampleDescribedValueEnum.values()) {
            final PropertyValue value = new StandardPropertyValue(enumValue.getValue(), lookup, ParameterLookup.EMPTY);
            assertEquals(enumValue, value.asAllowableValue(ExampleDescribedValueEnum.class));
        }

        final PropertyValue nullDescribedValue = new StandardPropertyValue(null, lookup, ParameterLookup.EMPTY);
        assertNull(nullDescribedValue.asAllowableValue(ExampleDescribedValueEnum.class));

        IllegalArgumentException describedValueException = assertThrows(IllegalArgumentException.class, () -> {
            final PropertyValue invalidValue = new StandardPropertyValue("FOO", lookup, ParameterLookup.EMPTY);
            invalidValue.asAllowableValue(ExampleDescribedValueEnum.class);
        });
        assertEquals("ExampleDescribedValueEnum does not have an entry with value FOO", describedValueException.getMessage());

        for (ExampleNonDescribedValueEnum enumValue : ExampleNonDescribedValueEnum.values()) {
            final PropertyValue value = new StandardPropertyValue(enumValue.name(), lookup, ParameterLookup.EMPTY);
            assertEquals(enumValue, value.asAllowableValue(ExampleNonDescribedValueEnum.class));
        }

        final PropertyValue nullValue = new StandardPropertyValue(null, lookup, ParameterLookup.EMPTY);
        assertNull(nullValue.asAllowableValue(ExampleNonDescribedValueEnum.class));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            final PropertyValue invalidValue = new StandardPropertyValue("FOO", lookup, ParameterLookup.EMPTY);
            invalidValue.asAllowableValue(ExampleNonDescribedValueEnum.class);
        });
        assertEquals("ExampleNonDescribedValueEnum does not have an entry with value FOO", exception.getMessage());
    }

    @Test
    public void testFileSize() {
        final PropertyValue value = new StandardPropertyValue("${fileSize}", lookup, ParameterLookup.EMPTY);
        final FlowFile flowFile = new StandardFlowFileRecord.Builder().size(1024 * 1024L).build();
        final long val = value.evaluateAttributeExpressions(flowFile).asLong().longValue();
        assertEquals(1024 * 1024L, val);
    }

    @Test
    public void testFlowFileEntryYear() {
        final Calendar now = Calendar.getInstance();
        final int year = now.get(Calendar.YEAR);
        final PropertyValue value = new StandardPropertyValue("${entryDate:toNumber():toDate():format('yyyy')}", lookup, ParameterLookup.EMPTY);
        final FlowFile flowFile = new StandardFlowFileRecord.Builder().entryDate(now.getTimeInMillis()).build();
        final int val = value.evaluateAttributeExpressions(flowFile).asInteger().intValue();
        assertEquals(year, val);
    }

    @Test
    public void testisExpressionLanguagePresentShouldHandleNPE() {
        // Arrange
        final PropertyValue value = new StandardPropertyValue(null, lookup, ParameterLookup.EMPTY);

        // Act
        boolean elPresent = value.isExpressionLanguagePresent();

        // Assert
        assertFalse(elPresent);
    }

    private FlowFile createFlowFile(final Map<String, String> attributes) {
        return new StandardFlowFileRecord.Builder().addAttributes(attributes).build();
    }

    private static class TestControllerServiceLookup implements ControllerServiceLookup {

        private final Map<String, ControllerService> map = new HashMap<>();

        @Override
        public ControllerService getControllerService(final String serviceIdentifier) {
            return map.get(serviceIdentifier);
        }

        @Override
        public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
            return null;
        }

        @Override
        public boolean isControllerServiceEnabled(final String serviceIdentifier) {
            return true;
        }

        @Override
        public boolean isControllerServiceEnabled(final ControllerService service) {
            return true;
        }

        @Override
        public String getControllerServiceName(final String serviceIdentifier) {
            return null;
        }

        @Override
        public boolean isControllerServiceEnabling(final String serviceIdentifier) {
            return false;
        }

    }

    private enum ExampleDescribedValueEnum implements DescribedValue {
        ONE("One Value", "One Display", "One Description"),
        OTHER("Other Value", "Other Display", "Other Description"),
        ANOTHER("Another Value", "Another Display", "Another Description");

        private final String value;
        private final String displayName;
        private final String description;

        ExampleDescribedValueEnum(final String value, final String displayName, final String description) {
            this.value = value;
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return this.value;
        }

        @Override
        public String getDisplayName() {
            return this.displayName;
        }

        @Override
        public String getDescription() {
            return this.description;
        }
    }

    private enum ExampleNonDescribedValueEnum { ONE, TWO, THREE, FOUR }
}
