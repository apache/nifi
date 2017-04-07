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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;

public class TestAvroSchemaRegistry {

    @Test
    public void validateSchemaRegistrationFromrDynamicProperties() throws Exception {
        String schemaName = "fooSchema";
        ConfigurationContext configContext = mock(ConfigurationContext.class);
        Map<PropertyDescriptor, String> properties = new HashMap<>();
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
        properties.put(fooSchema, fooSchemaText);
        properties.put(barSchema, "");
        when(configContext.getProperties()).thenReturn(properties);
        SchemaRegistry delegate = new AvroSchemaRegistry();
        ((AvroSchemaRegistry) delegate).enable(configContext);

        String locatedSchemaText = delegate.retrieveSchemaText(schemaName);
        assertEquals(fooSchemaText, locatedSchemaText);
        try {
            locatedSchemaText = delegate.retrieveSchemaText("barSchema");
            fail();
        } catch (Exception e) {
            // ignore
        }
        delegate.close();
    }


    @Test
    public void validateRecordSchemaRetrieval() throws Exception {
        String schemaName = "fooSchema";
        ConfigurationContext configContext = mock(ConfigurationContext.class);
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        PropertyDescriptor fooSchema = new PropertyDescriptor.Builder()
            .name(schemaName)
            .dynamic(true)
            .build();
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
            + "\"fields\": [ " + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]}, "
            + "{\"name\": \"favorite_number\",  \"type\": \"int\"}, "
            + "{\"name\": \"foo\",  \"type\": \"boolean\"}, "
            + "{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} " + "]" + "}";
        PropertyDescriptor barSchema = new PropertyDescriptor.Builder()
            .name("barSchema")
            .dynamic(false)
            .build();
        properties.put(fooSchema, fooSchemaText);
        properties.put(barSchema, "");
        when(configContext.getProperties()).thenReturn(properties);
        SchemaRegistry delegate = new AvroSchemaRegistry();
        ((AvroSchemaRegistry) delegate).enable(configContext);

        RecordSchema locatedSchema = delegate.retrieveSchema(schemaName);
        List<RecordField> recordFields = locatedSchema.getFields();
        assertEquals(4, recordFields.size());
        assertEquals(RecordFieldType.STRING.getDataType(), recordFields.get(0).getDataType());
        assertEquals("name", recordFields.get(0).getFieldName());
        assertEquals(RecordFieldType.INT.getDataType(), recordFields.get(1).getDataType());
        assertEquals("favorite_number", recordFields.get(1).getFieldName());
        assertEquals(RecordFieldType.BOOLEAN.getDataType(), recordFields.get(2).getDataType());
        assertEquals("foo", recordFields.get(2).getFieldName());
        assertEquals(RecordFieldType.STRING.getDataType(), recordFields.get(3).getDataType());
        assertEquals("favorite_color", recordFields.get(3).getFieldName());
        delegate.close();
    }

}
