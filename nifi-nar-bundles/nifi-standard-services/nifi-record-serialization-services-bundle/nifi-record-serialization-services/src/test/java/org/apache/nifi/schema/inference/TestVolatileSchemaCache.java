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
package org.apache.nifi.schema.inference;

import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestVolatileSchemaCache {

    @Test
    public void testEqualSchemasSameIdentifier() throws InitializationException {
        final List<RecordField> fields = new ArrayList<>();
        for (int i=0; i < 3; i++) {
            fields.add(new RecordField(String.valueOf(i), RecordFieldType.STRING.getDataType()));
        }

        final ConfigurationContext configContext = new MockConfigurationContext(Collections.singletonMap(VolatileSchemaCache.MAX_SIZE, "100"), null);
        final VolatileSchemaCache cache = new VolatileSchemaCache();
        cache.initialize(new MockControllerServiceInitializationContext(cache, "id"));
        cache.setup(configContext);

        final String firstId = cache.cacheSchema(new SimpleRecordSchema(fields));
        final String secondId = cache.cacheSchema(new SimpleRecordSchema(fields));
        assertEquals(firstId, secondId);
    }

    @Test
    public void testDifferentSchemasDifferentIdentifier() throws InitializationException {
        final List<RecordField> stringFields = new ArrayList<>();
        final List<RecordField> intFields = new ArrayList<>();

        for (int i=0; i < 3; i++) {
            stringFields.add(new RecordField(String.valueOf(i), RecordFieldType.STRING.getDataType()));
            intFields.add(new RecordField(String.valueOf(i), RecordFieldType.INT.getDataType()));
        }

        final ConfigurationContext configContext = new MockConfigurationContext(Collections.singletonMap(VolatileSchemaCache.MAX_SIZE, "100"), null);
        final VolatileSchemaCache cache = new VolatileSchemaCache();
        cache.initialize(new MockControllerServiceInitializationContext(cache, "id"));
        cache.setup(configContext);

        final String firstId = cache.cacheSchema(new SimpleRecordSchema(stringFields));
        final String secondId = cache.cacheSchema(new SimpleRecordSchema(intFields));
        assertNotEquals(firstId, secondId);
    }

    @Test
    public void testIdentifierCollission() throws InitializationException {
        final List<RecordField> stringFields = new ArrayList<>();
        final List<RecordField> intFields = new ArrayList<>();

        for (int i=0; i < 3; i++) {
            stringFields.add(new RecordField(String.valueOf(i), RecordFieldType.STRING.getDataType()));
            intFields.add(new RecordField(String.valueOf(i), RecordFieldType.INT.getDataType()));
        }

        final ConfigurationContext configContext = new MockConfigurationContext(Collections.singletonMap(VolatileSchemaCache.MAX_SIZE, "100"), null);
        final VolatileSchemaCache cache = new VolatileSchemaCache() {
            @Override
            protected String createIdentifier(final RecordSchema schema) {
                return "identifier";
            }
        };

        cache.initialize(new MockControllerServiceInitializationContext(cache, "id"));
        cache.setup(configContext);

        final String firstId = cache.cacheSchema(new SimpleRecordSchema(stringFields));
        final String secondId = cache.cacheSchema(new SimpleRecordSchema(intFields));
        assertNotEquals(firstId, secondId);

        assertEquals(new SimpleRecordSchema(stringFields), cache.getSchema(firstId).get());
        assertEquals(new SimpleRecordSchema(intFields), cache.getSchema(secondId).get());
    }
}
