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
package org.apache.nifi.schema.access;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.when;

public class TestSchemaNamePropertyStrategy extends AbstractSchemaAccessStrategyTest {

    @Test
    public void testNameOnly() throws SchemaNotFoundException, IOException {
        final PropertyValue nameValue = new MockPropertyValue("person");
        final PropertyValue branchValue = new MockPropertyValue(null);
        final PropertyValue versionValue = new MockPropertyValue(null);

        final SchemaNamePropertyStrategy schemaNamePropertyStrategy = new SchemaNamePropertyStrategy(
                schemaRegistry, nameValue, branchValue, versionValue);

        final SchemaIdentifier expectedSchemaIdentifier = SchemaIdentifier.builder()
                .name(nameValue.getValue())
                .build();

        when(schemaRegistry.retrieveSchema(argThat(new SchemaIdentifierMatcher(expectedSchemaIdentifier))))
                .thenReturn(recordSchema);

        final RecordSchema retrievedSchema = schemaNamePropertyStrategy.getSchema(Collections.emptyMap(), null, recordSchema);
        assertNotNull(retrievedSchema);
    }

    @Test
    public void testNameAndVersion() throws SchemaNotFoundException, IOException {
        final PropertyValue nameValue = new MockPropertyValue("person");
        final PropertyValue branchValue = new MockPropertyValue(null);
        final PropertyValue versionValue = new MockPropertyValue("1");

        final SchemaNamePropertyStrategy schemaNamePropertyStrategy = new SchemaNamePropertyStrategy(
                schemaRegistry, nameValue, branchValue, versionValue);

        final SchemaIdentifier expectedSchemaIdentifier = SchemaIdentifier.builder()
                .name(nameValue.getValue())
                .version(versionValue.asInteger())
                .build();

        when(schemaRegistry.retrieveSchema(argThat(new SchemaIdentifierMatcher(expectedSchemaIdentifier))))
                .thenReturn(recordSchema);

        final RecordSchema retrievedSchema = schemaNamePropertyStrategy.getSchema(Collections.emptyMap(), null, recordSchema);
        assertNotNull(retrievedSchema);
    }

    @Test
    public void testNameAndBlankVersion() throws SchemaNotFoundException, IOException {
        final PropertyValue nameValue = new MockPropertyValue("person");
        final PropertyValue branchValue = new MockPropertyValue(null);
        final PropertyValue versionValue = new MockPropertyValue("   ");

        final SchemaNamePropertyStrategy schemaNamePropertyStrategy = new SchemaNamePropertyStrategy(
                schemaRegistry, nameValue, branchValue, versionValue);

        final SchemaIdentifier expectedSchemaIdentifier = SchemaIdentifier.builder()
                .name(nameValue.getValue())
                .build();

        when(schemaRegistry.retrieveSchema(argThat(new SchemaIdentifierMatcher(expectedSchemaIdentifier))))
                .thenReturn(recordSchema);

        final RecordSchema retrievedSchema = schemaNamePropertyStrategy.getSchema(Collections.emptyMap(), null, recordSchema);
        assertNotNull(retrievedSchema);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testNameAndNonNumericVersion() throws SchemaNotFoundException, IOException {
        final PropertyValue nameValue = new MockPropertyValue("person");
        final PropertyValue branchValue = new MockPropertyValue(null);
        final PropertyValue versionValue = new MockPropertyValue("XYZ");

        final SchemaNamePropertyStrategy schemaNamePropertyStrategy = new SchemaNamePropertyStrategy(
                schemaRegistry, nameValue, branchValue, versionValue);

        schemaNamePropertyStrategy.getSchema(Collections.emptyMap(), null, recordSchema);
    }

    @Test
    public void testNameAndBranch() throws SchemaNotFoundException, IOException {
        final PropertyValue nameValue = new MockPropertyValue("person");
        final PropertyValue branchValue = new MockPropertyValue("test");
        final PropertyValue versionValue = new MockPropertyValue(null);

        final SchemaNamePropertyStrategy schemaNamePropertyStrategy = new SchemaNamePropertyStrategy(
                schemaRegistry, nameValue, branchValue, versionValue);

        final SchemaIdentifier expectedSchemaIdentifier = SchemaIdentifier.builder()
                .name(nameValue.getValue())
                .branch(branchValue.getValue())
                .build();

        when(schemaRegistry.retrieveSchema(argThat(new SchemaIdentifierMatcher(expectedSchemaIdentifier))))
                .thenReturn(recordSchema);

        final RecordSchema retrievedSchema = schemaNamePropertyStrategy.getSchema(Collections.emptyMap(), null, recordSchema);
        assertNotNull(retrievedSchema);
    }

    @Test
    public void testNameAndBlankBranch() throws SchemaNotFoundException, IOException {
        final PropertyValue nameValue = new MockPropertyValue("person");
        final PropertyValue branchValue = new MockPropertyValue("  ");
        final PropertyValue versionValue = new MockPropertyValue(null);

        final SchemaNamePropertyStrategy schemaNamePropertyStrategy = new SchemaNamePropertyStrategy(
                schemaRegistry, nameValue, branchValue, versionValue);

        final SchemaIdentifier expectedSchemaIdentifier = SchemaIdentifier.builder()
                .name(nameValue.getValue())
                .build();

        when(schemaRegistry.retrieveSchema(argThat(new SchemaIdentifierMatcher(expectedSchemaIdentifier))))
                .thenReturn(recordSchema);

        final RecordSchema retrievedSchema = schemaNamePropertyStrategy.getSchema(Collections.emptyMap(), null, recordSchema);
        assertNotNull(retrievedSchema);
    }

}
