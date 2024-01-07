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
package org.apache.nifi.components;

import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

public class TestPropertyDescriptor {

    private static final String DEFAULT_VALUE = "Default Value";
    private static final String DEPENDENT_PROPERTY_NAME = "dependentProperty";

    @Nested
    class RegardingDefaultValue {
        @Test
        void supportsStringValues() {
            final PropertyDescriptor descriptor = builder().defaultValue(DEFAULT_VALUE).build();

            assertEquals(DEFAULT_VALUE, descriptor.getDefaultValue());
        }

        @Test
        void supportsDescribedValuesValues() {
            final PropertyDescriptor descriptor = builder().defaultValue(EnumDescribedValue.GREEN).build();

            assertEquals(EnumDescribedValue.GREEN.getValue(), descriptor.getDefaultValue());
        }

        /**
         * Regression test for issue NIFI-49, to ensure that if a Processor's Property's
         * Default Value is not allowed, the Exception thrown should indicate what the default value is
         */
        @Test
        void throwsIllegalStateExceptionWhenDefaultValueNotInAllowableValues() {
            IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
                builder().allowableValues("NOT DEFAULT", "OTHER NOT DEFAULT").defaultValue(DEFAULT_VALUE).build();
            });
            assertTrue(exception.getMessage().contains("[" + DEFAULT_VALUE + "]"));
        }

        @Test
        void canBeCleared() {
            final PropertyDescriptor descriptorWithDefault = builder().defaultValue(DEFAULT_VALUE).build();
            final PropertyDescriptor resetDescriptor = builder(descriptorWithDefault).clearDefaultValue().build();

            assertNull(resetDescriptor.getDefaultValue());
        }
    }

    @Nested
    class RegardingAllowableValues {

        private static final Comparator<AllowableValue> allowableValueComparator = Comparator.comparing(AllowableValue::getValue);
        private final List<AllowableValue> expectedMinimalAllowableValues =
                List.of(new AllowableValue("GREEN"), new AllowableValue("RED"), new AllowableValue("BLUE"));
        private final List<AllowableValue> expectedAllowableValuesWithDescription =
                Arrays.stream(EnumDescribedValue.values()).map(AllowableValue::fromDescribedValue).toList();

        @Test
        void supportsStringVarArgValues() {
            final List<AllowableValue> expected = expectedMinimalAllowableValues;

            final PropertyDescriptor descriptor = builder().allowableValues("GREEN", "RED", "BLUE").build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        void supportsStringSetValues() {
            final List<AllowableValue> expected = sort(expectedMinimalAllowableValues);

            final PropertyDescriptor descriptor = builder().allowableValues(Set.of("GREEN", "RED", "BLUE")).build();
            // the iteration order of sets is not guaranteed by all implementations, thus we unify the order here
            final List<AllowableValue> actual = sort(descriptor.getAllowableValues());

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        void supportsEnumArrayValues() {
            final List<AllowableValue> expected = expectedMinimalAllowableValues;

            final PropertyDescriptor descriptor = builder().allowableValues(EnumNotDescribedValue.values()).build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        @SuppressWarnings({"rawtypes", "unchecked"})
        void supportsDescribedValueEnumArrayValues() {
            final List<AllowableValue> expected = expectedAllowableValuesWithDescription;

            final Enum[] enumArray = EnumDescribedValue.values();
            final PropertyDescriptor descriptor = builder().allowableValues(enumArray).build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        void supportsEnumClassValues() {
            final List<AllowableValue> expected = expectedMinimalAllowableValues;

            final PropertyDescriptor descriptor = builder().allowableValues(EnumNotDescribedValue.class).build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        void supportsDescribedValueEnumClassValues() {
            final List<AllowableValue> expected = expectedAllowableValuesWithDescription;

            final PropertyDescriptor descriptor = builder().allowableValues(EnumDescribedValue.class).build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        void supportsEnumSetValues() {
            final List<AllowableValue> expected = expectedMinimalAllowableValues;

            final PropertyDescriptor descriptor = builder().allowableValues(EnumSet.allOf(EnumNotDescribedValue.class)).build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        void supportsDescribedValueEnumSetValues() {
            final List<AllowableValue> expected = expectedAllowableValuesWithDescription;

            final PropertyDescriptor descriptor = builder().allowableValues(EnumSet.allOf(EnumDescribedValue.class)).build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        @Test
        void supportsDescribedValueVarArgValues() {
            final List<AllowableValue> expected = expectedAllowableValuesWithDescription;

            final PropertyDescriptor descriptor = builder()
                    .allowableValues(EnumDescribedValue.GREEN, EnumDescribedValue.RED, EnumDescribedValue.BLUE).build();
            final List<AllowableValue> actual = descriptor.getAllowableValues();

            assertEquals(expected, actual); // equals only compares getValue()
            assertEquals(displayNamesOf(expected), displayNamesOf(actual));
            assertEquals(descriptionsOf(expected), descriptionsOf(actual));
        }

        private List<AllowableValue> sort(final List<AllowableValue> allowableValues) {
            return allowableValues.stream().sorted(allowableValueComparator).toList();
        }

        private List<String> displayNamesOf(final List<AllowableValue> allowableValues) {
            return allowableValues.stream().map(AllowableValue::getDisplayName).toList();
        }

        private List<String> descriptionsOf(final List<AllowableValue> allowableValues) {
            return allowableValues.stream().map(AllowableValue::getDescription).toList();
        }
    }

    @Test
    void testDependsOnWithEnumValue() {
        final PropertyDescriptor dependentPropertyDescriptor = new PropertyDescriptor.Builder()
                .name(DEPENDENT_PROPERTY_NAME)
                .build();

        final PropertyDescriptor propertyDescriptor = new PropertyDescriptor.Builder()
                .name("enumDependsOnDescriptor")
                .dependsOn(dependentPropertyDescriptor, EnumDescribedValue.RED)
                .build();

        assertNotNull(propertyDescriptor);

        final Set<PropertyDependency> dependencies = propertyDescriptor.getDependencies();
        assertEquals(1, dependencies.size());
        final PropertyDependency dependency = dependencies.iterator().next();
        assertEquals(DEPENDENT_PROPERTY_NAME, dependency.getPropertyName());
        final Set<String> dependentValues = dependency.getDependentValues();
        assertEquals(1, dependentValues.size());
        final String dependentValue = dependentValues.iterator().next();
        assertEquals(EnumDescribedValue.RED.getValue(), dependentValue);
    }

    @Test
    void testExternalResourceIgnoredIfELWithAttributesPresent() {
        final PropertyDescriptor descriptor = new PropertyDescriptor.Builder()
                .name("dir")
                .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .required(false)
                .build();

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        Mockito.when(validationContext.isExpressionLanguagePresent(anyString())).thenReturn(true);
        Mockito.when(validationContext.isExpressionLanguageSupported(anyString())).thenReturn(true);
        Mockito.when(validationContext.newPropertyValue(anyString())).thenAnswer(invocation -> {
            final String inputArg = invocation.getArgument(0);
            return inputArg.replace("${TestPropertyDescriptor.Var1}", "__my_var__").replaceAll("\\$\\{.*}", "");
        });

        assertTrue(descriptor.validate("${TestPropertyDescriptor.Var1}", validationContext).isValid());
    }

    @Test
    void testExternalResourceConsideredIfELVarRegistryPresent() {
        final PropertyDescriptor descriptor = new PropertyDescriptor.Builder()
                .name("dir")
                .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.DIRECTORY)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .required(false)
                .build();

        final AtomicReference<String> variable = new AtomicReference<>("__my_var__");
        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        Mockito.when(validationContext.isExpressionLanguagePresent(anyString())).thenReturn(true);
        Mockito.when(validationContext.isExpressionLanguageSupported(anyString())).thenReturn(true);
        Mockito.when(validationContext.newPropertyValue(anyString())).thenAnswer(invocation -> {
            final String inputArg = invocation.getArgument(0);
            final String evaluatedValue = inputArg.replace("${TestPropertyDescriptor.Var1}", variable.get().replaceAll("\\$\\{.*}", ""));

            final PropertyValue propertyValue = Mockito.mock(PropertyValue.class);
            Mockito.when(propertyValue.getValue()).thenReturn(evaluatedValue);
            Mockito.when(propertyValue.evaluateAttributeExpressions()).thenReturn(propertyValue);
            return propertyValue;
        });

        // Should not be valid because Expression Language scope is ENVIRONMENT, so the ${TestPropertyDescriptor.Var1} will be replaced with
        // __my_var__, and __my_var__ does not exist.
        assertFalse(descriptor.validate("${TestPropertyDescriptor.Var1}", validationContext).isValid());

        // Will now be valid because variable changed to 'target', which does exist.
        variable.set("target");
        assertTrue(descriptor.validate("${TestPropertyDescriptor.Var1}", validationContext).isValid());

        // Consider if Expression Language is not supported.
        Mockito.when(validationContext.isExpressionLanguageSupported(anyString())).thenReturn(false);
        final PropertyDescriptor withElNotAllowed = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(descriptor)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .build();

        // Expression will not be evaluated, so the directory being looked at will literally be ${TestPropertyDescriptor.Var1}
        assertFalse(withElNotAllowed.validate("${TestPropertyDescriptor.Var1}", validationContext).isValid());

        // Test the literal value 'target'
        assertTrue(withElNotAllowed.validate("target", validationContext).isValid());
    }

    @Test
    void testClearingValues() {
        final PropertyDescriptor dep1 = new PropertyDescriptor.Builder()
                .name("dep1")
                .allowableValues("delVal1")
                .build();

        final PropertyDescriptor pd1 = new PropertyDescriptor.Builder()
                .name("test")
                .addValidator(Validator.VALID)
                .allowableValues("val1")
                .dependsOn(dep1, "depVal1")
                .build();

        final PropertyDescriptor pd2 = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(pd1)
                .clearValidators()
                .clearAllowableValues()
                .clearDependsOn()
                .build();

        assertEquals("test", pd1.getName());
        assertFalse(pd1.getValidators().isEmpty());
        assertFalse(pd1.getDependencies().isEmpty());
        assertNotNull(pd1.getAllowableValues());

        assertEquals("test", pd2.getName());
        assertTrue(pd2.getValidators().isEmpty());
        assertTrue(pd2.getDependencies().isEmpty());
        assertNull(pd2.getAllowableValues());
    }

    private Builder builder() {
        return new PropertyDescriptor.Builder().name("propertyName");
    }

    private Builder builder(final PropertyDescriptor propertyDescriptor) {
        return new PropertyDescriptor.Builder().fromPropertyDescriptor(propertyDescriptor);
    }
}
