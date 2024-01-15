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
package org.apache.nifi.schemaregistry.services;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.JsonSchemaRegistryComponent;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.nifi.util.TestRunners;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandardJsonSchemaRegistry {
    private static final String SCHEMA_NAME = "fooSchema";
    private static final PropertyDescriptor SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR =  new PropertyDescriptor.Builder()
            .name(SCHEMA_NAME)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    @Mock
    private ValidationContext validationContext;
    @Mock
    private PropertyValue propertyValue;
    private Map<PropertyDescriptor, String> properties;
    private StandardJsonSchemaRegistry delegate;

    @BeforeEach
    void setUp() throws InitializationException {
        properties = new HashMap<>();
        delegate = new StandardJsonSchemaRegistry();
        TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("jsonSchemaRegistry", delegate);
    }

    @Test
    void testCustomValidateWithoutAnySchemaSpecified() {
        when(validationContext.getProperties()).thenReturn(properties);

        final Collection<ValidationResult> results = delegate.customValidate(validationContext);
        assertEquals(1, results.size());
        final ValidationResult result = results.iterator().next();
        assertFalse(result.isValid());
        assertTrue(result.getExplanation().contains("at least one JSON schema specified"));
    }

    @ParameterizedTest(name = "{3}")
    @MethodSource("dynamicProperties")
    void testCustomValidateWithSchemaRegistrationFromDynamicProperties(PropertyDescriptor propertyDescriptor, String schema, int numValidationErrors) {
        when(validationContext.getProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION)).thenReturn(propertyValue);
        when(propertyValue.getValue()).thenReturn(JsonSchemaRegistryComponent.SCHEMA_VERSION.getDefaultValue());
        when(validationContext.getProperties()).thenReturn(properties);
        properties.put(propertyDescriptor, schema);
        delegate.getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDisplayName()));

        assertEquals(numValidationErrors, delegate.customValidate(validationContext).size());
    }

    @ParameterizedTest(name = "{3}")
    @MethodSource("dynamicProperties")
    void testSchemaRetrieval(PropertyDescriptor propertyDescriptor, String schema, int numValidationErrors) throws SchemaNotFoundException {
        delegate.onPropertyModified(propertyDescriptor, null, schema);
        boolean validSchema = numValidationErrors == 0;

        if(validSchema) {
            assertDoesNotThrow(() -> delegate.retrieveSchema(SCHEMA_NAME));
            assertNotNull(delegate.retrieveSchema(SCHEMA_NAME));
        } else {
            assertThrows(SchemaNotFoundException.class, () -> delegate.retrieveSchema(SCHEMA_NAME));
        }
    }

    private static Stream<Arguments> dynamicProperties() {
        return Stream.of(
                Arguments.of(SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR, "{}", 0, "empty object schema"),
                Arguments.of(SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR, "[]", 0, "empty array schema"),
                Arguments.of(SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR, "not a schema", 1, "non whitespace")
        );
    }
}
