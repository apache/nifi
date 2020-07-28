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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroSchemaRegistry {

    @Test
    public void validateSchemaRegistrationFromrDynamicProperties() throws Exception {
        String schemaName = "fooSchema";

        PropertyDescriptor fooSchema = new PropertyDescriptor.Builder()
            .name(schemaName)
            .dynamic(true)
            .build();
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
            + "\"fields\": [ " + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]}, "
            + "{\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}, "
            + "{\"name\": \"foo\",  \"type\": [\"int\", \"null\"]}, "
            + "{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} " + "]" + "}";
        PropertyDescriptor barSchema = new PropertyDescriptor.Builder()
            .name("barSchema")
            .dynamic(false)
            .build();

        AvroSchemaRegistry delegate = new AvroSchemaRegistry();
        delegate.onPropertyModified(fooSchema, null, fooSchemaText);
        delegate.onPropertyModified(barSchema, null, "");

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name(schemaName).build();
        RecordSchema locatedSchema = delegate.retrieveSchema(schemaIdentifier);
        assertEquals(fooSchemaText, locatedSchema.getSchemaText().get());
        try {
            delegate.retrieveSchema(SchemaIdentifier.builder().name("barSchema").build());
            Assert.fail("Expected a SchemaNotFoundException to be thrown but it was not");
        } catch (final SchemaNotFoundException expected) {
        }

    }

    @Test
    public void validateStrictAndNonStrictSchemaRegistrationFromDynamicProperties() throws Exception {
        String schemaName = "fooSchema";
        ConfigurationContext configContext = mock(ConfigurationContext.class);
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        PropertyDescriptor fooSchema = new PropertyDescriptor.Builder()
                .name(schemaName)
                .dynamic(true)
                .build();
        // NOTE: name of record and name of first field are not Avro-compliant, verified below
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"$User\", "
                + "\"fields\": [ " + "{\"name\": \"@name\", \"type\": [\"string\", \"null\"]}, "
                + "{\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}, "
                + "{\"name\": \"foo\",  \"type\": [\"int\", \"null\"]}, "
                + "{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} " + "]" + "}";
        PropertyDescriptor barSchema = new PropertyDescriptor.Builder()
                .name("barSchema")
                .dynamic(false)
                .build();
        properties.put(fooSchema, fooSchemaText);
        properties.put(barSchema, "");
        AvroSchemaRegistry delegate = new AvroSchemaRegistry();
        delegate.getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDisplayName()));
        when(configContext.getProperties()).thenReturn(properties);

        ValidationContext validationContext = mock(ValidationContext.class);
        when(validationContext.getProperties()).thenReturn(properties);
        PropertyValue propertyValue = mock(PropertyValue.class);
        when(validationContext.getProperty(AvroSchemaRegistry.VALIDATE_FIELD_NAMES)).thenReturn(propertyValue);

        // Strict parsing
        when(propertyValue.asBoolean()).thenReturn(true);
        Collection<ValidationResult> results = delegate.customValidate(validationContext);
        assertTrue(results.stream().anyMatch(result -> !result.isValid()));

        // Non-strict parsing
        when(propertyValue.asBoolean()).thenReturn(false);
        results = delegate.customValidate(validationContext);
        results.forEach(result -> assertTrue(result.isValid()));
    }
}
