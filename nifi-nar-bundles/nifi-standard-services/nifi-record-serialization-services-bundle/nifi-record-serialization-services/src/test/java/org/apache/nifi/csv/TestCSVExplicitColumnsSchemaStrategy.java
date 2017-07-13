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

package org.apache.nifi.csv;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestCSVExplicitColumnsSchemaStrategy {

    @Test
    public void testHappyPath() throws SchemaNotFoundException, IOException {

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(CSVUtils.EXPLICIT_COLUMNS, "a,b, c");

        final ConfigurationContext context = new MockConfigurationContext(properties, null);
        final CSVExplicitColumnsSchemaStrategy test = new CSVExplicitColumnsSchemaStrategy(context.getProperty(CSVUtils.EXPLICIT_COLUMNS));

        RecordSchema result = test.getSchema(null, null, null);

        Assert.assertEquals(3, result.getFields().size());
        Assert.assertTrue(result.getField("a").isPresent());
        Assert.assertTrue(result.getField("b").isPresent());
        Assert.assertTrue(result.getField("c").isPresent());

    }

    @Test(expected = SchemaNotFoundException.class)
    public void testMissingField() throws SchemaNotFoundException, IOException {

        final Map<PropertyDescriptor, String> properties = new HashMap<>();

        final ConfigurationContext context = new MockConfigurationContext(properties, null);
        final CSVExplicitColumnsSchemaStrategy test = new CSVExplicitColumnsSchemaStrategy(context.getProperty(CSVUtils.EXPLICIT_COLUMNS));

        test.getSchema(null, null, null);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testEmptyField() throws SchemaNotFoundException, IOException {

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(CSVUtils.EXPLICIT_COLUMNS, "");

        final ConfigurationContext context = new MockConfigurationContext(properties, null);
        final CSVExplicitColumnsSchemaStrategy test = new CSVExplicitColumnsSchemaStrategy(context.getProperty(CSVUtils.EXPLICIT_COLUMNS));

        test.getSchema(null, null, null);
    }

}
