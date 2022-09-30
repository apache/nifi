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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestAvroSchemaDirectoryRegistry {
    private static final boolean DEBUG = Boolean.getBoolean("debug");

    @SuppressWarnings("unused")
    @TempDir
    private File registryDirectory;

    private AvroSchemaDirectoryRegistry registry;

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        if(DEBUG) {
            System.out.println("Running " + testInfo.getDisplayName());
        }

        registry = new AvroSchemaDirectoryRegistry();
        ControllerServiceInitializationContext initializationContext =
                new MockControllerServiceInitializationContext(registry, "avroRegistry");
        registry.initialize(initializationContext);
    }

    @Test
    void testExistingSchemaWithBaseNameNamingStrategy() throws Exception {
        writeSchemaTextToDisk(getAvroCompliantSchemaText());
        Map<PropertyDescriptor, String> properties = getProperties();
        properties.put(AvroSchemaDirectoryRegistry.SCHEMA_NAMING_STRATEGY,
                AvroSchemaDirectoryRegistry.NamingStrategy.BASE_NAME.name());

        registry.onEnabled(new MockConfigurationContext(properties, null));

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name(getSchemaBaseName()).build();
        assertSchemaFound(schemaIdentifier);
    }

    private File writeSchemaTextToDisk(String schemaText) throws Exception {
        return writeSchemaTextToDisk(schemaText, getSchemaBaseName(), getJsonSchemaExtension());
    }

    private String getSchemaBaseName() {
        return "someSchema";
    }

    private String getJsonSchemaExtension() {
        return ".json";
    }

    private File writeSchemaTextToDisk(String schemaText, String baseName, String extension) throws Exception {
        File schema = new File(registryDirectory, baseName + extension);
        FileUtils.writeStringToFile(schema, schemaText, StandardCharsets.UTF_8);
        return schema;
    }

    private void assertSchemaFound(SchemaIdentifier schemaIdentifier) throws Exception {
        RecordSchema locatedSchema = registry.retrieveSchema(schemaIdentifier);
        assertEquals(getAvroCompliantSchemaText(), locatedSchema.getSchemaText().orElse(""));
    }

    @Test
    void testExistingSchemaWithFileNameNamingStrategy() throws Exception {
        File schema = writeSchemaTextToDisk(getAvroCompliantSchemaText());
        Map<PropertyDescriptor, String> properties = getProperties();
        properties.put(AvroSchemaDirectoryRegistry.SCHEMA_NAMING_STRATEGY,
                AvroSchemaDirectoryRegistry.NamingStrategy.FILE_NAME.name());

        registry.onEnabled(new MockConfigurationContext(properties, null));

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name(schema.getName()).build();
        assertSchemaFound(schemaIdentifier);
    }

    @Test
    void testExistingSchemaWithFullPathNamingStrategy() throws Exception {
        File schema = writeSchemaTextToDisk(getAvroCompliantSchemaText());
        Map<PropertyDescriptor, String> properties = getProperties();
        properties.put(AvroSchemaDirectoryRegistry.SCHEMA_NAMING_STRATEGY,
                AvroSchemaDirectoryRegistry.NamingStrategy.FULL_PATH.name());

        registry.onEnabled(new MockConfigurationContext(properties, null));

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name(schema.getAbsolutePath()).build();
        assertSchemaFound(schemaIdentifier);
    }

    private Map<PropertyDescriptor, String> getProperties() {
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(AvroSchemaDirectoryRegistry.SCHEMA_DIRECTORY, registryDirectory.getAbsolutePath());

        return properties;
    }

    private String getAvroCompliantSchemaText() {
        return "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
                + "\"fields\": [ " + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]}, "
                + "{\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}, "
                + "{\"name\": \"foo\",  \"type\": [\"int\", \"null\"]}, "
                + "{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} " + "]" + "}";
    }

    @Test
    void testNonExistentSchemaRegistration() {
        registry.onEnabled(new MockConfigurationContext(getProperties(), null));

        assertThrows(SchemaNotFoundException.class,
                () -> registry.retrieveSchema(SchemaIdentifier.builder().name("someSchema").build()));
    }

    @Test
    void testSchemaWithJsonExtensionFiltering() throws Exception {
        File schemaWithJsonExtension = writeSchemaTextToDisk(getAvroCompliantSchemaText());
        File schemaWithTxtExtension =
                writeSchemaTextToDisk(getAvroCompliantSchemaText(), "textSchema", ".txt");
        Map<PropertyDescriptor, String> properties = getProperties();
        properties.put(AvroSchemaDirectoryRegistry.SCHEMA_NAMING_STRATEGY,
                AvroSchemaDirectoryRegistry.NamingStrategy.FILE_NAME.name());

        registry.onEnabled(new MockConfigurationContext(properties, null));

        assertSchemaFound(SchemaIdentifier.builder().name(schemaWithJsonExtension.getName()).build());
        assertThrows(SchemaNotFoundException.class,
                () -> registry.retrieveSchema(SchemaIdentifier.builder().name(schemaWithTxtExtension.getName()).build()));
    }

    @Test
    void testValidateWithEmptyDirectory() {
        ValidationContext validationContext = getValidationContext(true);
        Collection<ValidationResult> results = registry.customValidate(validationContext);
        assertEquals(1, results.size(),
                "Expected to have one validation result instead had " + results.size());
        results.forEach(result -> assertFalse(result.isValid()));
    }

    private ValidationContext getValidationContext(boolean strict) {
        ValidationContext validationContext = mock(ValidationContext.class);
        PropertyValue mockValidate = mock(PropertyValue.class);
        when(validationContext.getProperty(AvroSchemaDirectoryRegistry.VALIDATE_FIELD_NAMES)).thenReturn(mockValidate);
        PropertyValue mockDirectory = mock(PropertyValue.class);
        when(validationContext.getProperty(AvroSchemaDirectoryRegistry.SCHEMA_DIRECTORY)).thenReturn(mockDirectory);

        when(mockValidate.asBoolean()).thenReturn(strict);
        when(mockDirectory.getValue()).thenReturn(registryDirectory.getAbsolutePath());

        return validationContext;
    }

    @Test
    void testStrictValidate() throws Exception {
        writeSchemaTextToDisk(getNonAvroCompliantSchemaText());
        ValidationContext validationContext = getValidationContext(true);
        Collection<ValidationResult> results = registry.customValidate(validationContext);

        results.forEach(result -> assertFalse(result.isValid()));
    }

    /**
     * <b>NOTE: </b>name of record and name of first field are not Avro-compliant
     * @return Non Avro compliant schema text
     */
    private String getNonAvroCompliantSchemaText() {
       return "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"$User\", "
                + "\"fields\": [ " + "{\"name\": \"@name\", \"type\": [\"string\", \"null\"]}, "
                + "{\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}, "
                + "{\"name\": \"foo\",  \"type\": [\"int\", \"null\"]}, "
                + "{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} " + "]" + "}";
    }

    @Test
    void testNonStrictValidate() throws Exception {
        writeSchemaTextToDisk(getNonAvroCompliantSchemaText());
        ValidationContext validationContext = getValidationContext(false);
        Collection<ValidationResult> results = registry.customValidate(validationContext);

        results.forEach(result -> assertTrue(result.isValid()));
    }
}
