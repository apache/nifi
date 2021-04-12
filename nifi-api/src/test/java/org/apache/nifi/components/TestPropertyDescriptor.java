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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Regression test for issue NIFI-49, to ensure that if a Processor's Property's
 * Default Value is not allowed, the Exception thrown should indicate what the
 * default value is
 */
public class TestPropertyDescriptor {

    private static Builder invalidDescriptorBuilder;
    private static Builder validDescriptorBuilder;
    private static String DEFAULT_VALUE = "Default Value";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        validDescriptorBuilder = new PropertyDescriptor.Builder().name("").allowableValues("Allowable Value", "Another Allowable Value").defaultValue("Allowable Value");
        invalidDescriptorBuilder = new PropertyDescriptor.Builder().name("").allowableValues("Allowable Value", "Another Allowable Value").defaultValue(DEFAULT_VALUE);
    }

    @Test
    public void testExceptionThrownByDescriptorWithInvalidDefaultValue() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("[" + DEFAULT_VALUE + "]");

        invalidDescriptorBuilder.build();
    }

    @Test
    public void testNoExceptionThrownByPropertyDescriptorWithValidDefaultValue() {
        assertNotNull(validDescriptorBuilder.build());
    }

    @Test
    public void testExternalResourceIgnoredIfELWithAttributesPresent() {
        final PropertyDescriptor descriptor = new PropertyDescriptor.Builder()
            .name("dir")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        Mockito.when(validationContext.isExpressionLanguagePresent(anyString())).thenReturn(true);
        Mockito.when(validationContext.isExpressionLanguageSupported(anyString())).thenReturn(true);
        Mockito.when(validationContext.newPropertyValue(anyString())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final String inputArg = invocation.getArgument(0);
                return inputArg.replace("${TestPropertyDescriptor.Var1}", "__my_var__").replaceAll("\\$\\{.*}", "");
            }
        });

        assertTrue(descriptor.validate("${TestPropertyDescriptor.Var1}", validationContext).isValid());
    }

    @Test
    public void testExternalResourceConsideredIfELVarRegistryPresent() {
        final PropertyDescriptor descriptor = new PropertyDescriptor.Builder()
            .name("dir")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();

        final AtomicReference<String> variable = new AtomicReference<>("__my_var__");
        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        Mockito.when(validationContext.isExpressionLanguagePresent(anyString())).thenReturn(true);
        Mockito.when(validationContext.isExpressionLanguageSupported(anyString())).thenReturn(true);
        Mockito.when(validationContext.newPropertyValue(anyString())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) {
                final String inputArg = invocation.getArgument(0);
                final String evaluatedValue = inputArg.replace("${TestPropertyDescriptor.Var1}", variable.get().replaceAll("\\$\\{.*}", ""));

                final PropertyValue propertyValue = Mockito.mock(PropertyValue.class);
                Mockito.when(propertyValue.getValue()).thenReturn(evaluatedValue);
                Mockito.when(propertyValue.evaluateAttributeExpressions()).thenReturn(propertyValue);
                return propertyValue;
            }
        });

        // Should not be valid because Expression Language scope is VARIABLE_REGISTRY, so the ${TestPropertyDescriptor.Var1} will be replaced with
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
}
