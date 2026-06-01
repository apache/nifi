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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAvroSchemaRegistry {
    private static final String SCHEMA_NAME = "fooSchema";
    private static final PropertyDescriptor SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name(SCHEMA_NAME)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final String NON_PERIOD_NAMESPACE_SEPARATOR = """
                                {"namespace": "example-avro", "type": "record", "name": "User",
                                "fields": [ {"name": "name", "type": ["string", "null"]},
                                {"name": "favorite_number",  "type": ["int", "null"]},
                                {"name": "foo",  "type": ["int", "null"]},
                                {"name": "favorite_color", "type": ["string", "null"]} ]}""";

    private static final String ILLEGAL_CHARACTER_IN_RECORD_NAME = """
                                {"namespace": "example.avro", "type": "record", "name": "$User",
                                "fields": [ {"name": "name", "type": ["string", "null"]},
                                {"name": "favorite_number",  "type": ["int", "null"]},
                                {"name": "foo",  "type": ["int", "null"]},
                                {"name": "favorite_color", "type": ["string", "null"]} ]}""";

    private static final String ILLEGAL_CHARACTER_IN_FIELD_NAME = """
                                {"namespace": "example.avro", "type": "record", "name": "User",
                                "fields": [ {"name": "@name", "type": ["string", "null"]},
                                {"name": "favorite_number",  "type": ["int", "null"]},
                                {"name": "foo",  "type": ["int", "null"]},
                                {"name": "favorite_color", "type": ["string", "null"]} ]}""";

    private static final String NON_MATCHING_DEFAULT_TYPE = """
                                {"namespace": "example.avro", "type": "record", "name": "User",
                                "fields": [ {"name": "name", "type": ["string", "null"]},
                                {"name": "favorite_number",  "type": ["int", "null"]},
                                {"name": "foo",  "type": "int", "default": "NAN"},
                                {"name": "favorite_color", "type": ["string", "null"]} ]}""";

    private static final String NON_MATCHING_UNION_DEFAULT_TYPE = """
                                {"namespace": "example.avro", "type": "record", "name": "User",
                                "fields": [ {"name": "name", "type": ["string", "null"]},
                                {"name": "favorite_number",  "type": ["int", "null"]},
                                {"name": "foo",  "type": ["int", "null"], "default": "NAN"},
                                {"name": "favorite_color", "type": ["string", "null"]} ]}""";

    private TestRunner runner;
    private AvroSchemaRegistry avroSchemaRegistry;

    @BeforeEach
    void setup() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        avroSchemaRegistry = new AvroSchemaRegistry();
        runner.addControllerService("avroSchemaRegistry", avroSchemaRegistry);
    }

    @Test
    public void testRetrievalOfNonExistentSchema() {
        runner.assertValid(avroSchemaRegistry);
        runner.enableControllerService(avroSchemaRegistry);
        final SchemaIdentifier nonExistentSchemaIdentifier = SchemaIdentifier.builder().name("barSchema").build();

        assertThrows(SchemaNotFoundException.class, () -> avroSchemaRegistry.retrieveSchema(nonExistentSchemaIdentifier));
    }

    @Test
    public void validateSchemaRegistration() throws Exception {
        String schemaName = "fooSchema";
        String fooSchemaText = """
                {"namespace": "example.avro", "type": "record", "name": "User",
                "fields": [ {"name": "name", "type": ["string", "null"]},
                {"name": "favorite_number",  "type": ["int", "null"]},
                {"name": "foo",  "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]} ]}""";

        runner.setProperty(avroSchemaRegistry, SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR, fooSchemaText);
        runner.assertValid(avroSchemaRegistry);
        runner.enableControllerService(avroSchemaRegistry);

        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name(schemaName).build();
        final RecordSchema locatedSchema = avroSchemaRegistry.retrieveSchema(schemaIdentifier);
        assertTrue(locatedSchema.getSchemaText().isPresent());
        assertEquals(fooSchemaText, locatedSchema.getSchemaText().get());
    }

    @ParameterizedTest
    @MethodSource("invalidSchemasForValidation")
    public void testWhereSchemasValidated(String schema, List<String> keyWordsInExceptionMessage) {
        runner.setProperty(avroSchemaRegistry, SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR, schema);
        runner.setProperty(avroSchemaRegistry, AvroSchemaRegistry.VALIDATION_STRATEGY, ValidationStrategy.VALIDATE.getValue());

        final AssertionFailedError assertionFailedError = assertThrows(AssertionFailedError.class, () -> runner.assertValid(avroSchemaRegistry));
        keyWordsInExceptionMessage.forEach(keyword -> assertTrue(assertionFailedError.getMessage().contains(keyword)));
    }

    @ParameterizedTest
    @MethodSource("invalidSchemasForNoValidation")
    public void testWhereSchemasNotValidated(String schema) {
        runner.setProperty(avroSchemaRegistry, SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR, schema);
        runner.setProperty(avroSchemaRegistry, AvroSchemaRegistry.VALIDATION_STRATEGY, ValidationStrategy.NONE.getValue());

        runner.assertValid(avroSchemaRegistry);
    }

    @ParameterizedTest
    @EnumSource(ValidationStrategy.class)
    public void testDuplicateFieldsInvalidRegardlessOfValidation(ValidationStrategy validationStrategy) {
        final String schema = """
                {"namespace": "example.avro", "type": "record", "name": "User",
                "fields": [ {"name": "name", "type": ["string", "null"]},
                {"name": "favorite_number",  "type": ["int", "null"]},
                {"name": "foo",  "type": ["int", "null"]},
                {"name": "foo",  "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]} ]}""";

        runner.setProperty(avroSchemaRegistry, SUPPORTED_DYNAMIC_PROPERTY_DESCRIPTOR, schema);
        runner.setProperty(avroSchemaRegistry, AvroSchemaRegistry.VALIDATION_STRATEGY, validationStrategy.getValue());

        runner.assertNotValid(avroSchemaRegistry);
    }

    @ParameterizedTest
    @MethodSource("migrationConfigurations")
    void testMigrateProperties(MockPropertyConfiguration configuration) {
        final Set<String> expectedRemoved = new HashSet<>(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES);
        final Set<String> expectedUpdated = Set.of(AvroSchemaRegistry.VALIDATION_STRATEGY.getName());

        final AvroSchemaRegistry service = new AvroSchemaRegistry();
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Set<String> propertiesRemoved = result.getPropertiesRemoved();
        assertEquals(expectedRemoved, propertiesRemoved);
        assertEquals(expectedUpdated, result.getPropertiesUpdated());
    }

    private static Stream<Arguments> invalidSchemasForValidation() {
        return Stream.of(
                Arguments.argumentSet("Namespace separator other than a period", NON_PERIOD_NAMESPACE_SEPARATOR, List.of("Namespace", "Illegal character")),
                Arguments.argumentSet("Illegal character(s) in record name", ILLEGAL_CHARACTER_IN_RECORD_NAME, List.of("Illegal initial character")),
                Arguments.argumentSet("Illegal character(s) in field name", ILLEGAL_CHARACTER_IN_FIELD_NAME, List.of("Illegal initial character")),
                Arguments.argumentSet("Non matching default type", NON_MATCHING_DEFAULT_TYPE, List.of("Invalid default")),
                Arguments.argumentSet("Non matching union default type", NON_MATCHING_UNION_DEFAULT_TYPE, List.of("Invalid default"))
        );
    }

    private static Stream<Arguments> invalidSchemasForNoValidation() {
        return Stream.of(
                Arguments.argumentSet("Namespace separator other than a period", NON_PERIOD_NAMESPACE_SEPARATOR),
                Arguments.argumentSet("Illegal character(s) in record name", ILLEGAL_CHARACTER_IN_RECORD_NAME),
                Arguments.argumentSet("Illegal character(s) in field name", ILLEGAL_CHARACTER_IN_FIELD_NAME),
                Arguments.argumentSet("Non matching default type", NON_MATCHING_DEFAULT_TYPE),
                Arguments.argumentSet("Non matching union default type", NON_MATCHING_UNION_DEFAULT_TYPE)
        );
    }

    private static Stream<Arguments> migrationConfigurations() {
        return Stream.of(
                Arguments.argumentSet(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.getFirst() + " Validate",
                        new MockPropertyConfiguration(Map.of(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.getFirst(), Boolean.TRUE.toString()))),
                Arguments.argumentSet(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.getFirst() + " Do Not Validate",
                        new MockPropertyConfiguration(Map.of(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.getFirst(), Boolean.FALSE.toString()))),
                Arguments.argumentSet(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.get(1) + " Validate",
                        new MockPropertyConfiguration(Map.of(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.get(1), Boolean.TRUE.toString()))),
                Arguments.argumentSet(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.get(1) + " Do Not Validate",
                        new MockPropertyConfiguration(Map.of(AvroSchemaRegistry.OBSOLETE_VALIDATE_FIELD_NAMES.get(1), Boolean.FALSE.toString())))
        );
    }
}
